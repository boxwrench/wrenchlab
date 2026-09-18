"""Campaign-local deterministic job broker; no model runtime dependency."""
import argparse
import base64
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time

from execution_protocol import (VERSION, MAX_BUNDLE, MAX_WIRE, canonical, digest, fields,
    identifier, integer, locked, now, read, save, validate_config, validate_request)

HERE = Path(__file__).resolve().parent


class SSHTransport:
    """SSH is a wire choice, not a different job lifecycle."""
    def __init__(self, config):
        self.config = validate_config(config)

    def invoke(self, argv, payload):
        command = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                   '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=5',
                   '-o', 'ServerAliveCountMax=2', self.config['host'], shlex.join(argv)]
        last = None
        for attempt in range(3):
            try:
                p = subprocess.run(command, input=canonical(payload), capture_output=True,
                                   timeout=120 if payload.get('op') == 'resource-reset' else 20)
                if len(p.stdout) > MAX_WIRE:
                    raise ValueError('worker response exceeds protocol limit')
                if p.returncode == 255:
                    raise ConnectionError('SSH transport failed; reconcile by stable ID')
                response = json.loads(p.stdout)
                if p.returncode or set(response) == {'error'}:
                    raise ValueError(response.get('error', 'worker request failed'))
                return response
            except (subprocess.TimeoutExpired, ConnectionError, json.JSONDecodeError) as exc:
                last = exc
                if attempt < 2:
                    time.sleep(.1 * (attempt + 1))
        raise ConnectionError('SSH outcome unknown; request persisted, retry same job ID') from last

    def call(self, payload):
        return self.invoke(['python3', self.config['root'] + '/execution_worker.py', self.config['root']], payload)

    def deploy(self):
        # Bundle travels over stdin, never on the SSH command line.
        package = deployment(self.config)
        manifest = canonical(package['manifest'])
        command = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                   '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=5',
                   '-o', 'ServerAliveCountMax=2', self.config['host'],
                   shlex.join(['python3', '-c', DEPLOY, manifest.decode()])]
        last = None
        for attempt in range(2):
            try:
                p = subprocess.run(command, input=package['bundle'], capture_output=True, timeout=120)
                if p.returncode == 255:
                    raise ConnectionError('SSH transport failed; reconcile by stable ID')
                response = json.loads(p.stdout)
                if p.returncode or set(response) == {'error'}:
                    raise ValueError(response.get('error', 'worker deploy failed'))
                return response
            except (subprocess.TimeoutExpired, ConnectionError, json.JSONDecodeError, ValueError) as exc:
                last = exc
                if attempt < 1:
                    time.sleep(.5)
        raise ConnectionError('worker deploy failed: ' + str(last))


class LocalTransport(SSHTransport):
    """Same installed worker, isolation, ledger and JSON protocol without SSH."""
    def invoke(self, argv, payload):
        p = subprocess.run(argv, input=canonical(payload), capture_output=True,
                           timeout=120 if payload.get('op') == 'resource-reset' else 20)
        if len(p.stdout) > MAX_WIRE:
            raise ValueError('worker response exceeds protocol limit')
        result = json.loads(p.stdout)
        if p.returncode or set(result) == {'error'}:
            raise ValueError(result.get('error', p.stderr.decode(errors='replace')))
        return result

    def deploy(self):
        # Local install uses the same manifest + stdin tar bundle, no SSH.
        package = deployment(self.config)
        manifest = canonical(package['manifest'])
        p = subprocess.run([sys.executable, '-c', DEPLOY, manifest.decode()],
                           input=package['bundle'], capture_output=True, timeout=120)
        if p.returncode:
            raise ValueError(p.stderr.decode(errors='replace') or 'worker deploy failed')
        result = json.loads(p.stdout)
        if set(result) == {'error'}:
            raise ValueError(result.get('error', 'worker deploy failed'))
        return result


# Install only in a new dedicated root, or verify identical installed bytes.
# Never overwrite an existing executor while it may own processes.
# The installer reads a small JSON manifest on argv/stdin and the file bytes
# as a tar stream on a second stdin pass: no source payload travels on the
# SSH command line. Staging is verified by content hash before promotion,
# and failed staging is removed without touching the live worker root.
DEPLOY = '''import sys,json,os,hashlib,tempfile,tarfile,io
from pathlib import Path
manifest=json.loads(sys.argv[1]); data=sys.stdin.buffer.read()
root=Path(manifest['root']); os.umask(0o077)
if root.is_symlink() or root.resolve()!=root: raise ValueError('worker root must not traverse symlinks')
if root.exists() and not (root/'state.json').exists():
    if any(root.iterdir()): raise ValueError('worker root is not empty')
if hashlib.sha256(data).hexdigest()!=manifest['bundle_sha256']:
    raise ValueError('deploy bundle integrity mismatch')
stage=Path(tempfile.mkdtemp(prefix='wrenchlab-deploy-',dir=root.parent if root.parent.exists() else '/tmp'))
try:
    with tarfile.open(fileobj=io.BytesIO(data),mode='r') as t:
        members=t.getmembers()
        if sorted(m.name for m in members)!=sorted(manifest['files']):
            raise ValueError('deploy bundle file list mismatch')
        for m in members:
            if not m.isfile() or m.size>1048576 or '/' in m.name or m.name.startswith('.'):
                raise ValueError('deploy bundle member rejected: '+m.name)
        t.extractall(stage,filter='data')
    for name,expected in manifest['files'].items():
        content=(stage/name).read_text()
        if hashlib.sha256(content.encode()).hexdigest()!=expected:
            raise ValueError('deploy file hash mismatch: '+name)
    root.mkdir(parents=True,exist_ok=True)
    for name in manifest['files']:
        p=root/name
        content=(stage/name).read_text()
        if p.exists() and (p.is_symlink() or p.read_text()!=content):
            raise ValueError('installed executor differs; explicit upgrade required')
        if not p.exists():
            with p.open('x') as f: f.write(content); f.flush(); os.fsync(f.fileno())
    sys.path.insert(0,str(root))
    from execution_protocol import locked,save,read
    with locked(root/'state.lock'):
        if (root/'state.json').exists():
            if read(root/'state.json')['worker_id']!=manifest['worker_id']: raise ValueError('worker identity mismatch')
        else: save(root/'state.json',{'schema_version':1,'worker_id':manifest['worker_id'],'status':'AVAILABLE','active_job':None,'jobs':{}})
    if manifest.get('host_json'):
        (root/'host.json').write_text(manifest['host_json'])
finally:
    import shutil
    shutil.rmtree(stage,ignore_errors=True)
print(json.dumps({'worker_id':manifest['worker_id'],'root':str(root),'installed':True,'commit':manifest.get('commit')}))
'''


def deployment(config):
    files = {name: (HERE / name).read_text() for name in
             ('execution_protocol.py', 'execution_worker.py', 'execution_hosts.py',
              'execution_resources.py', 'execution_probe.py', 'gpu_owner_probe.py')}
    import hashlib as _hashlib
    import io as _io
    import tarfile as _tarfile
    hashes = {name: _hashlib.sha256(content.encode()).hexdigest() for name, content in files.items()}
    buf = _io.BytesIO()
    with _tarfile.open(fileobj=buf, mode='w') as t:
        for name in sorted(files):
            raw = files[name].encode()
            info = _tarfile.TarInfo(name)
            info.size = len(raw)
            info.mode = 0o600
            t.addfile(info, _io.BytesIO(raw))
    bundle = buf.getvalue()
    manifest = {'root': config['root'], 'worker_id': config['worker_id'], 'files': hashes,
                'bundle_sha256': _hashlib.sha256(bundle).hexdigest()}
    if config['schema_version'] == 2:
        manifest['host_json'] = canonical(config).decode() + '\n'
    try:
        commit = subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'],
                                         stderr=subprocess.DEVNULL, timeout=10).decode().strip()
        manifest['commit'] = commit
    except Exception:
        pass
    return {'manifest': manifest, 'bundle': bundle}


def bundle_source(repo, revision):
    repo = Path(repo).resolve(strict=True)
    if not isinstance(revision, str) or revision.startswith('-'):
        raise ValueError('invalid source revision')
    env = dict(os.environ, GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null')
    def git(*args):
        return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null', *args],
                                       stderr=subprocess.PIPE, timeout=30, env=env)
    commit = git('-C', str(repo), 'rev-parse', '--verify', revision + '^{commit}').decode().strip()
    tree = git('-C', str(repo), 'ls-tree', '-r', commit)
    if any(line.startswith(b'160000 ') for line in tree.splitlines()):
        raise ValueError('submodule inputs are not supported in protocol v1')
    with tempfile.TemporaryDirectory(prefix='wrenchlab-job-source-') as tmp:
        stage = str(Path(tmp) / 'source.git')
        output = str(Path(tmp) / 'input.bundle')
        git('init', '--bare', stage)
        git('-C', stage, 'fetch', '--no-tags', str(repo), commit)
        git('-C', stage, 'update-ref', 'refs/heads/wrenchlab-input', commit)
        git('-C', stage, 'bundle', 'create', output, 'refs/heads/wrenchlab-input')
        if Path(output).stat().st_size > MAX_BUNDLE:
            raise ValueError('source bundle exceeds 16 MiB prototype limit')
        data = Path(output).read_bytes()
    return {'commit': commit, 'bundle_sha256': digest(data)}, data


class Broker:
    def __init__(self, campaign, transport=None):
        self.campaign = Path(campaign).resolve()
        self.root = self.campaign / 'execution'
        self.state_file = self.root / 'state.json'
        self.transport_override = transport

    def transport(self, state):
        cls = LocalTransport if state['worker'].get('transport') == 'local' else SSHTransport
        return self.transport_override or cls(state['worker'])

    def create(self, config, deadline, owner, objective, max_jobs=8, author_host=None):
        validate_config(config); integer(deadline); integer(max_jobs, 1, 100)
        if deadline <= now() or not owner.strip() or not objective.strip():
            raise ValueError('future deadline, owner and objective are required')
        cid = identifier(self.campaign.name)
        with locked(self.root / 'state.lock'):
            if self.state_file.exists():
                raise ValueError('flow already exists; deadline and policy are immutable')
            state = {'schema_version': VERSION, 'flow_id': cid, 'campaign_id': cid,
                     'created_at': now(), 'deadline': deadline, 'owner': owner, 'objective': objective,
                     'state': 'OPEN', 'max_jobs': max_jobs, 'worker': config, 'jobs': {}}
            if author_host is not None:
                state['author_host'] = author_host
            save(self.state_file, state)
        return state

    def prepare(self, spec):
        fields(spec, ['job_id', 'repo', 'revision', 'command', 'timeout_ms', 'artifacts'], ['requirements', 'placement', 'progress'])
        jid = identifier(spec['job_id'])
        with locked(self.root / 'state.lock'):
            state = read(self.state_file)
            if jid in state['jobs']:
                raise ValueError('job already prepared; use submit with its stable ID')
            if state['state'] != 'OPEN' or now() >= state['deadline']:
                raise ValueError('flow closed or deadline expired')
            if len(state['jobs']) >= state['max_jobs']:
                raise ValueError('flow job allowance exhausted')
            source, data = bundle_source(spec['repo'], spec['revision'])
            request = {'schema_version': VERSION, 'job_id': jid, 'flow_id': state['flow_id'],
                       'worker_id': state['worker']['worker_id'], 'source': source, 'command': spec['command'],
                       'timeout_ms': spec['timeout_ms'], 'deadline': state['deadline'], 'resource': 'cpu',
                       'artifacts': spec['artifacts'], 'created_at': now()}
            if state['worker']['schema_version'] == 2:
                from execution_hosts import admit
                request.pop('resource')
                request.update(schema_version=2,
                               requirements=spec.get('requirements', {'capabilities': ['cpu'], 'resources': {}}),
                               placement=spec.get('placement', {'executor': state['worker']['host_id']}),
                               host_config_hash=digest(canonical(state['worker'])))
                if 'progress' in spec:
                    request['progress'] = spec['progress']
                admit(state['worker'], request)
            elif 'requirements' in spec or 'placement' in spec:
                raise ValueError('requirements/placement need a version 2 host configuration')
            validate_request(request)
            folder = self.root / 'jobs' / jid
            folder.mkdir(parents=True, exist_ok=True)
            with (folder / 'input.bundle').open('wb') as f:
                f.write(data); f.flush(); os.fsync(f.fileno())
            save(folder / 'request.json', request)
            state['jobs'][jid] = {'request_hash': digest(canonical(request)), 'state': 'PREPARED'}
            save(self.state_file, state)
        return request

    def action(self, op, jid):
        identifier(jid)
        with locked(self.root / 'state.lock'):
            state = read(self.state_file)
            if jid not in state['jobs']:
                raise ValueError('job not registered with this flow')
            folder = self.root / 'jobs' / jid
            request = validate_request(read(folder / 'request.json'))
            if digest(canonical(request)) != state['jobs'][jid]['request_hash']:
                raise ValueError('local immutable request changed')
            payload = {'op': op, 'job_id': jid}
            if op == 'submit':
                if state['state'] != 'OPEN' and state['jobs'][jid]['state'] == 'PREPARED':
                    raise ValueError('flow closed to new submission')
                if state['jobs'][jid]['state'] == 'PREPARED' and now() >= state['deadline']:
                    raise ValueError('flow deadline expired')
                data = (folder / 'input.bundle').read_bytes()
                if digest(data) != request['source']['bundle_sha256']:
                    raise ValueError('local source bundle changed')
                payload.update(request=request, bundle=base64.b64encode(data).decode())
                state['jobs'][jid]['state'] = 'SUBMITTING'
                save(self.state_file, state)  # Before all network IO, including retries.
        try:
            response = self.transport(state).call(payload)
            remote_request = response.get('job', {}).get('request')
            if remote_request is not None and canonical(remote_request) != canonical(request):
                raise ValueError('remote job request does not match local ownership')
            if response.get('result') and response['result']['request_hash'] != digest(canonical(request)):
                raise ValueError('remote result ownership mismatch')
            with locked(self.root / 'state.lock'):
                current = read(self.state_file)
                current['jobs'][jid]['last_observation'] = response.get('job', {})
                current['jobs'][jid]['state'] = response.get('job', {}).get('state', current['jobs'][jid]['state'])
                current['jobs'][jid]['worker_status'] = response.get('worker_status', 'QUARANTINED')
                current['jobs'][jid].pop('last_error', None)
                save(self.state_file, current)
            if op == 'collect':
                save(folder / 'transfer.json', response)
                # Ingest through the same JS evidence module used by campaign export.
                subprocess.run(['node', str(HERE / 'ingest-job.mjs'), str(self.campaign), str(folder)],
                               check=True, timeout=20, stdout=subprocess.DEVNULL)
                response = dict(response, files=list(response['files']))
            return response
        except Exception as exc:
            with locked(self.root / 'state.lock'):
                current = read(self.state_file)
                current['jobs'][jid]['last_error'] = str(exc)
                current['jobs'][jid]['observation'] = 'unknown'
                save(self.state_file, current)
            raise

    def reconcile(self):
        state = read(self.state_file)
        observations = {}
        for jid, job in state['jobs'].items():
            if job['state'] == 'PREPARED':
                continue
            try:
                observations[jid] = self.action('inspect', jid)
            except Exception as exc:
                observations[jid] = {'observation': 'unknown', 'error': str(exc)}
        return observations


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign', required=True)
    sub = p.add_subparsers(dest='operation', required=True)
    s = sub.add_parser('init')
    s.add_argument('--config', required=True); s.add_argument('--deadline', type=int, required=True)
    s.add_argument('--owner', required=True); s.add_argument('--objective', required=True)
    s.add_argument('--max-jobs', type=int, default=8)
    sub.add_parser('deploy'); sub.add_parser('reconcile'); sub.add_parser('status')
    sub.add_parser('resource-status'); sub.add_parser('resource-reset')
    s = sub.add_parser('prepare'); s.add_argument('--spec', required=True)
    for name in ('submit', 'inspect', 'cancel', 'collect'):
        s = sub.add_parser(name); s.add_argument('job_id')
    a = p.parse_args()
    broker = Broker(a.campaign)
    if a.operation == 'init':
        result = broker.create(read(a.config), a.deadline, a.owner, a.objective, a.max_jobs)
    elif a.operation == 'prepare':
        result = broker.prepare(read(a.spec))
    elif a.operation == 'deploy':
        state = read(broker.state_file)
        result = broker.transport(state).deploy()
    elif a.operation == 'reconcile':
        result = broker.reconcile()
    elif a.operation == 'status':
        result = read(broker.state_file)
    elif a.operation in ('resource-status', 'resource-reset'):
        result = broker.transport(read(broker.state_file)).call({'op': a.operation})
    else:
        result = broker.action(a.operation, a.job_id)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'error': f'{type(exc).__name__}: {exc}'}))
        sys.exit(1)
