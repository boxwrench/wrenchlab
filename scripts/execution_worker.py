"""Linux CPU worker: durable admission, detached supervisor and verified receipts.

Original implementation. Not a sandbox for hostile commands. No model calls.
"""
import base64
import ctypes
import json
import os
from pathlib import Path
import platform
import resource
import signal
import stat
import subprocess
import sys
import time

from execution_protocol import (VERSION, MAX_BUNDLE, MAX_ARTIFACT, MAX_TOTAL, MAX_WIRE,
    canonical, digest, identifier, locked, now, read, save, safe_file, validate_request)

MAX_GPU_EXEC_FILE = 512 * 1024 * 1024
MAX_PROGRESS = 32 * 1024


def boot():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def proc_info(pid):
    try:
        values = Path(f'/proc/{int(pid)}/stat').read_text().rsplit(') ', 1)[1].split()
        return {'pid': int(pid), 'state': values[0], 'ppid': int(values[1]),
                'pgid': int(values[2]), 'ticks': values[19], 'boot_id': boot()}
    except (ValueError, OSError, IndexError):
        return None


def alive(identity):
    current = proc_info(identity.get('pid', 0)) if identity else None
    return bool(current and current['state'] != 'Z' and all(current[k] == identity.get(k) for k in ('pid', 'ticks', 'boot_id')))


def processes():
    result = []
    for p in Path('/proc').iterdir():
        if p.name.isdigit():
            info = proc_info(p.name)
            if info:
                result.append(info)
    return result


def owned(pgid=None):
    records = processes()
    descendants = {os.getpid()}
    while True:
        expanded = descendants | {p['pid'] for p in records if p['ppid'] in descendants}
        if expanded == descendants:
            break
        descendants = expanded
    return [p for p in records if p['state'] != 'Z' and p['pid'] != os.getpid()
            and (p['pid'] in descendants or (pgid is not None and p['pgid'] == pgid))]


def clean_processes(command):
    pgid = command.pid if command else None
    # The direct child is not reaped until group cleanup, preventing PGID reuse.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if command:
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                pass
        for p in owned(pgid):
            if alive(p):
                try:
                    os.kill(p['pid'], sig)
                except ProcessLookupError:
                    pass
        end = time.monotonic() + 1.5
        while owned(pgid) and time.monotonic() < end:
            time.sleep(.03)
    if command:
        command.wait(timeout=2)
    while True:
        try:
            if os.waitpid(-1, os.WNOHANG)[0] == 0:
                break
        except ChildProcessError:
            break
    remaining = owned(pgid)
    if remaining:
        raise RuntimeError('owned processes survived termination')


def state_path(root):
    return root / 'state.json'


def reconcile(root, state):
    active = state.get('active_job')
    if active:
        job = state['jobs'][active]
        if job['state'] in ('RUNNING', 'CLEANING') and not alive(job.get('runner')):
            state['status'] = 'QUARANTINED'
            state['reason'] = 'supervisor disappeared; cleanup cannot be proven'
            job['cleanup'] = 'unknown'
        elif job['state'] == 'QUEUED' and now() - job['accepted_at'] > 5000:
            state['status'] = 'RECOVERING'
    return state


def git_run(args, **kwargs):
    return subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', *args],
                          check=True, timeout=30, **kwargs)


def file_bytes(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_size > MAX_ARTIFACT:
            raise ValueError('artifact must be a bounded regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(MAX_ARTIFACT + 1)
        if len(data) > MAX_ARTIFACT:
            raise ValueError('artifact exceeds limit')
        return data
    finally:
        os.close(fd)


def result_for(root, state, jid):
    job = state['jobs'][jid]
    result = root / 'jobs' / jid / 'result.json'
    progress = progress_for(root, job)
    return {'worker_status': state['status'], 'reason': state.get('reason'),
            'job': job, 'progress': progress, 'result': read(result) if result.exists() else None}


def progress_for(root, job):
    """Read one bounded, worker-produced progress snapshot without trusting paths."""
    request = job.get('request', {})
    spec = request.get('progress')
    if not spec:
        return None
    try:
        path = safe_file(root / 'jobs' / request['job_id'] / 'work', spec['path'])
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            data = os.read(fd, MAX_PROGRESS + 1)
        finally:
            os.close(fd)
        if len(data) > MAX_PROGRESS:
            return {'state': 'INVALID', 'error': 'progress snapshot exceeds limit'}
        snapshot = json.loads(data)
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get('state'), str):
            return {'state': 'INVALID', 'error': 'progress snapshot must be an object with state'}
        snapshot['_bytes'] = len(data)
        return snapshot
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {'state': 'INVALID', 'error': f'{type(exc).__name__}: {exc}'}


def supervise(root, jid):
    folder = root / 'jobs' / jid
    try:
        with locked(folder / 'runner.lock', nonblocking=True):
            execute(root, jid)
    except BlockingIOError:
        return


def execute(root, jid):
    # Reparent double-forked descendants to this supervisor so cleanup can see them.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError('Linux child subreaper unavailable')
    folder = root / 'jobs' / jid
    with locked(root / 'state.lock'):
        state = read(state_path(root))
        job = state['jobs'][jid]
        if job['state'] != 'QUEUED' or state['status'] == 'QUARANTINED':
            return
        job.update(state='RUNNING', runner=proc_info(os.getpid()), started_at=now())
        state['status'] = 'BUSY'
        save(state_path(root), state)  # Persist ownership before any work.
        request = job['request']
        request_hash = job['request_hash']
        worker_id = state['worker_id']
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, stop)
    # Avoid forwarding login/session secrets to either git or the command.
    env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'HOME': str(folder), 'LANG': 'C.UTF-8',
           'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null'}
    # GPU visibility is governed by the resource reservation (exclusive
    # acquire + owner observation), not by device-hiding variables. An
    # empty HIP_VISIBLE_DEVICES actively hides every AMD device from
    # ROCm torch, so it must never be set, even to ''.
    os.environ.clear()
    os.environ.update(env)
    started = job['started_at']
    end_mono = time.monotonic() + min(request['timeout_ms'], request['deadline'] - now()) / 1000
    command = None
    outcome, cleanup, error = 'failed', 'unknown', None
    exit_code = None
    work = folder / 'work'
    evidence_errors = []
    artifacts = []
    host = read(root / 'host.json') if request['schema_version'] == 2 else None
    guard = None
    resource_receipt = None
    command_identity = None
    try:
        def expired(*_):
            raise TimeoutError('job deadline expired')
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(.001, end_mono - time.monotonic()))
        if host:
            from execution_hosts import admit
            admit(host, request)
            if 'gpu' in request['requirements']['resources']:
                from execution_resources import ResourceManager
                guard = ResourceManager(host)
                resource_receipt = guard.acquire(request, job['runner'])
                with locked(root / 'state.lock'):
                    state = read(state_path(root))
                    state['jobs'][jid]['resource'] = resource_receipt
                    save(state_path(root), state)
                env.update(host['resources']['gpu']['environment'])
                env.update(WRENCHLAB_RESERVATION_ID=resource_receipt['reservation_id'],
                           WRENCHLAB_RESOURCE_ID=resource_receipt['resource_id'], WRENCHLAB_HOST_ID=host['host_id'],
                           WRENCHLAB_FLOW_ID=request['flow_id'])
        if digest((folder / 'input.bundle').read_bytes()) != request['source']['bundle_sha256']:
            raise ValueError('persisted source bundle hash mismatch')
        # Preparation uses bounded local tools; the outer watchdog covers it too.
        # SIGALRM raises in synchronous git setup; children are cleaned in finally.
        def expired(*_):
            raise TimeoutError('job deadline expired')
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(.001, end_mono - time.monotonic()))
        with (folder / 'setup.log').open('wb') as log:
            git_run(['init', '--bare', str(folder / 'source.git')], stdout=log, stderr=log, env=env)
            git_run(['-C', str(folder / 'source.git'), 'fetch', '--no-tags', str(folder / 'input.bundle'),
                     'refs/heads/wrenchlab-input'], stdout=log, stderr=log, env=env)
            actual = git_run(['-C', str(folder / 'source.git'), 'rev-parse', 'FETCH_HEAD^{commit}'],
                             capture_output=True, env=env).stdout.decode().strip()
            if actual != request['source']['commit']:
                raise ValueError('bundle commit mismatch')
            git_run(['-C', str(folder / 'source.git'), 'worktree', 'add', '--detach', str(work), actual],
                    stdout=log, stderr=log, env=env)
        if stopped or (folder / 'cancel').exists():
            outcome = 'cancelled'
        elif time.monotonic() >= end_mono or now() >= request['deadline']:
            outcome = 'timeout'
        else:
            # Cap individual output files, core dumps, and CPU threads by convention.
            # ROCm/LLVM JIT caches can exceed the CPU artifact limit. Keep
            # returned artifacts capped at MAX_ARTIFACT, but give a governed
            # GPU process a bounded private execution-file allowance.
            execution_file_limit = (MAX_GPU_EXEC_FILE if host and
                                     'gpu' in request['requirements']['resources'] else MAX_ARTIFACT)
            resource.setrlimit(resource.RLIMIT_FSIZE, (execution_file_limit, execution_file_limit))
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', WRENCHLAB_JOB_ID=jid)
            with (folder / 'stdout.log').open('wb') as out, (folder / 'stderr.log').open('wb') as err:
                if guard:
                    guard.launched(None)  # Persist launch uncertainty BEFORE fork.
                command = subprocess.Popen(request['command'], cwd=work, env=env, stdout=out, stderr=err,
                                           stdin=subprocess.DEVNULL, start_new_session=True)
                command_identity = proc_info(command.pid)
                if guard:
                    guard.launched(command_identity)
                with locked(root / 'state.lock'):
                    state = read(state_path(root))
                    state['jobs'][jid]['command_process'] = command_identity
                    save(state_path(root), state)
                next_resource_probe = time.monotonic()
                while True:
                    if stopped or (folder / 'cancel').exists():
                        outcome = 'cancelled'; break
                    if time.monotonic() >= end_mono or now() >= request['deadline']:
                        outcome = 'timeout'; break
                    status = os.waitid(os.P_PID, command.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                    if status:
                        exit_code = status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
                        outcome = 'success' if exit_code == 0 else 'failed'
                        break
                    if guard and time.monotonic() >= next_resource_probe:
                        guard.observe(owned(command.pid))
                        next_resource_probe = time.monotonic() + 1
                    time.sleep(.03)
    except TimeoutError as exc:
        outcome, error = 'timeout', str(exc)
    except BaseException as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)
        with locked(root / 'state.lock'):
            state = read(state_path(root))
            state['jobs'][jid]['state'] = 'CLEANING'
            state['status'] = 'RECOVERING' if state['status'] != 'QUARANTINED' else 'QUARANTINED'
            save(state_path(root), state)
        try:
            clean_processes(command)
            cleanup = 'verified'
        except BaseException as exc:
            cleanup = 'failed'
            error = (error or '') + f'; cleanup: {exc}'
        if guard and guard.owns:
            # A rejected second reservation does not own the current receipt.
            resource_receipt = guard.release(cleanup)
        # Freeze evidence only after writers are gone. Failed cleanup is never ingestible.
        total = 0
        if cleanup == 'verified':
            expected = [{'path': a, 'required': True, 'kind': 'log'} for a in ('stdout.log', 'stderr.log', 'setup.log')]
            expected += [dict(a, kind='artifact') for a in request['artifacts']]
            for spec in expected:
                try:
                    source = safe_file(work if spec['kind'] == 'artifact' else folder, spec['path'])
                    if not source.exists():
                        if spec['required']:
                            evidence_errors.append('missing: ' + spec['path'])
                        continue
                    data = file_bytes(source)
                    total += len(data)
                    if total > MAX_TOTAL:
                        raise ValueError('total artifact limit exceeded')
                    actual_hash = digest(data)
                    if spec.get('sha256') and spec['sha256'] != actual_hash:
                        raise ValueError('expected artifact hash mismatch')
                    relative = spec['kind'] + '/' + spec['path']
                    destination = safe_file(folder / 'evidence', relative)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(data)
                    artifacts.append({'path': relative, 'bytes': len(data), 'sha256': actual_hash})
                except (ValueError, OSError) as exc:
                    evidence_errors.append(spec['path'] + ': ' + str(exc))
        result = {'schema_version': VERSION, 'job_id': jid, 'flow_id': request['flow_id'],
                  'request_hash': request_hash, 'source': request['source'], 'command': request['command'],
                  'worker': {'worker_id': worker_id, 'hostname': platform.node(), 'boot_id': boot(),
                             'root': str(root), 'implementation_sha256': digest(Path(__file__).read_bytes())},
                  'environment': {'system': platform.system(), 'release': platform.release(),
                                  'machine': platform.machine(), 'python': platform.python_version(),
                                  'git': subprocess.check_output(['git', '--version'], text=True).strip(),
                                  'inherited_environment': False},
                  'started_at': started, 'ended_at': now(), 'execution': outcome, 'exit_code': exit_code,
                  'cleanup': cleanup, 'artifacts': artifacts, 'evidence_errors': evidence_errors, 'error': error}
        if request.get('progress'):
            result['progress'] = progress_for(root, job)
        if host:
            result.update(schema_version=2, placement={'executor': host['host_id'], 'transport': host['transport'],
                          'capabilities': host['capabilities'], 'capability_evidence': host['capability_evidence'],
                          'host_config_hash': request['host_config_hash']},
                          command_process=command_identity, resource=resource_receipt)
            result['environment']['implementation_hashes'] = {name: digest((root / name).read_bytes()) for name in
                ('execution_worker.py', 'execution_protocol.py', 'execution_hosts.py', 'execution_resources.py', 'execution_probe.py', 'gpu_owner_probe.py')}
            result['resource_admission'] = ('acquired' if resource_receipt and resource_receipt['acquire'] == 'verified'
                                            else 'rejected' if 'gpu' in request['requirements']['resources'] else 'not_requested')
        save(folder / 'result.json', result)
        with locked(root / 'state.lock'):
            state = read(state_path(root))
            state['jobs'][jid].update(state='TERMINAL', cleanup=cleanup, execution=outcome)
            if cleanup == 'verified' and state['status'] != 'QUARANTINED':
                state.update(status='AVAILABLE', active_job=None)
            else:
                state.update(status='QUARANTINED', reason=error or 'cleanup unknown')
            save(state_path(root), state)


def handle(root, payload):
    op = payload['op']
    if op in ('resource-status', 'resource-reset'):
        from execution_resources import ResourceManager
        return ResourceManager(read(root / 'host.json')).reconcile(reset=op == 'resource-reset')
    with locked(root / 'state.lock'):
        state = reconcile(root, read(state_path(root)))
        save(state_path(root), state)
        if op == 'ping':
            return {'schema_version': VERSION, 'worker_id': state['worker_id'], 'status': state['status'],
                    'hostname': platform.node(), 'boot_id': boot(), 'root': str(root)}
        jid = identifier(payload.get('job_id') or payload.get('request', {}).get('job_id'))
        folder = root / 'jobs' / jid
        if op == 'submit':
            req = validate_request(payload['request'])
            if req['schema_version'] == 2:
                from execution_hosts import admit
                admit(read(root / 'host.json'), req)
            req_hash = digest(canonical(req))
            if req['worker_id'] != state['worker_id']:
                raise ValueError('wrong worker identity')
            existing = state['jobs'].get(jid)
            if existing:
                if existing['request_hash'] != req_hash:
                    raise ValueError('CONFLICT: job ID already owns a different immutable request')
                if existing['state'] != 'QUEUED' or state['status'] == 'QUARANTINED':
                    return result_for(root, state, jid)
            else:
                if state['status'] != 'AVAILABLE':
                    raise ValueError('worker unavailable: ' + state['status'])
                if now() >= req['deadline']:
                    raise ValueError('deadline expired before admission')
                bundle = base64.b64decode(payload['bundle'], validate=True)
                if len(bundle) > MAX_BUNDLE or digest(bundle) != req['source']['bundle_sha256']:
                    raise ValueError('invalid source bundle')
                folder.mkdir(parents=True, exist_ok=True)
                with (folder / 'input.bundle').open('wb') as f:
                    f.write(bundle); f.flush(); os.fsync(f.fileno())
                state['jobs'][jid] = {'request': req, 'request_hash': req_hash, 'state': 'QUEUED',
                                      'cleanup': 'unknown', 'accepted_at': now()}
                state.update(status='BUSY', active_job=jid)
                save(state_path(root), state)
            with (folder / 'runner.log').open('ab') as log:
                subprocess.Popen([sys.executable, str(Path(__file__).resolve()), str(root), '_run', jid],
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
            return result_for(root, state, jid)
        if jid not in state['jobs']:
            raise ValueError('unknown job ID')
        if op == 'cancel':
            with (folder / 'cancel').open('ab') as f:
                f.flush(); os.fsync(f.fileno())
        elif op == 'collect':
            if state['jobs'][jid]['state'] != 'TERMINAL':
                raise ValueError('job is not terminal')
            result = read(folder / 'result.json')
            files = {}
            for a in result['artifacts']:
                data = file_bytes(safe_file(folder / 'evidence', a['path']))
                if digest(data) != a['sha256']:
                    raise ValueError('stored artifact changed after completion')
                files[a['path']] = base64.b64encode(data).decode()
            return {'result': result, 'files': files, 'worker_status': state['status']}
        elif op != 'inspect':
            raise ValueError('unsupported operation')
        return result_for(root, state, jid)


def main():
    os.umask(0o077)
    root = Path(sys.argv[1]).resolve()
    if len(sys.argv) > 2 and sys.argv[2] == '_run':
        supervise(root, identifier(sys.argv[3])); return
    raw = sys.stdin.buffer.read(MAX_WIRE + 1)
    if len(raw) > MAX_WIRE:
        raise ValueError('request too large')
    import json
    print(json.dumps(handle(root, json.loads(raw))))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        import json
        print(json.dumps({'error': f'{type(exc).__name__}: {exc}'}))
        sys.exit(1)
