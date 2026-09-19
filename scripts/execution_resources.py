"""Deterministic cooperative resource guard. No model decisions, no force-clear.

The host ledger is shared across worker roots for the same configured host.
Only configured systemd user services may be stopped; unknown owners are never killed.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import time
from execution_protocol import canonical, digest, locked, now, read, save
from execution_hosts import admit, validate_host


def live(identity):
    from execution_worker import alive
    return alive(identity)


class SystemAdapter:
    def __init__(self, host):
        self.host = host
        self.resource = host['resources']['gpu']
        self.services = {k: v for k, v in host['managed_services'].items()
                         if self.resource['resource_id'] in v['conflicts_with']}
        # Explicit user bus, not inherited credentials. Job HOME is never used here.
        import pwd
        self.env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
                    'HOME': pwd.getpwuid(os.getuid()).pw_dir,
                    'XDG_RUNTIME_DIR': f'/run/user/{os.getuid()}',
                    'DBUS_SESSION_BUS_ADDRESS': f'unix:path=/run/user/{os.getuid()}/bus'}
        # The configured GPU selection must reach the probe; otherwise the probe
        # inspects unmasked hardware and admission disagrees with qualification.
        for key, value in self.resource.get('environment', {}).items():
            if not isinstance(value, str) or key not in (
                    'HIP_VISIBLE_DEVICES', 'ROCR_VISIBLE_DEVICES', 'LD_LIBRARY_PATH'):
                raise ValueError('unsupported GPU environment key: ' + str(key))
            self.env[key] = value

    def run(self, argv, check=True, timeout=15):
        helper = Path(__file__).with_name('execution_probe.py')
        # Each helper has its own group/subreaper and local deadline. A parent
        # timeout cannot turn a health probe into an unbounded orphan.
        wire = subprocess.run([sys.executable, str(helper), str(timeout), *argv], env=self.env,
                              capture_output=True, text=True, timeout=timeout + 5, start_new_session=True)
        if wire.returncode:
            detail = (wire.stderr or wire.stdout or '').strip().replace('\n', ' ')[:500]
            raise RuntimeError('configured helper failed/cleanup unverified: ' + argv[0] + (': ' + detail if detail else ''))
        data = json.loads(wire.stdout)
        p = subprocess.CompletedProcess(argv, data['returncode'], data['stdout'], data['stderr'])
        if check and p.returncode:
            raise RuntimeError('configured probe/service command failed: ' + argv[0])
        if len(p.stdout) > 1024 * 1024:
            raise ValueError('probe response too large')
        return p

    def unit(self, sid):
        p = self.run(['/usr/bin/systemctl', '--user', 'show', self.services[sid]['unit'],
                      '--property=LoadState,ActiveState,SubState,ControlGroup,MainPID,UnitFileState,FragmentPath'])
        values = dict(line.split('=', 1) for line in p.stdout.splitlines() if '=' in line)
        if values.get('LoadState') != 'loaded' or values.get('ActiveState') not in ('active', 'inactive'):
            raise RuntimeError('service state unknown or transitional: ' + sid)
        active = values['ActiveState'] == 'active'
        from execution_worker import proc_info
        identity = proc_info(int(values.get('MainPID', 0)))
        if active and not identity:
            raise RuntimeError('active service process identity unavailable: ' + sid)
        fragment = values.get('FragmentPath', '')
        group = values.get('ControlGroup', '')
        return {'active': active, 'healthy': active and self.run(self.services[sid]['health'], check=False).returncode == 0,
                'cgroup': values.get('ControlGroup', ''), 'main_pid': int(values.get('MainPID', 0)),
                'process_identity': identity,
                'cgroup_inode': (Path('/sys/fs/cgroup') / group.lstrip('/')).stat().st_ino if group else None,
                'unit': self.services[sid]['unit'], 'sub_state': values.get('SubState'),
                'unit_file_state': values.get('UnitFileState'), 'fragment_path': fragment,
                'fragment_sha256': digest(Path(fragment).read_bytes()) if fragment else None}

    def _snapshot_once(self):
        data = json.loads(self.run(self.resource['probe']).stdout)
        if not isinstance(data.get('owners'), list) or data.get('complete') is not True:
            raise RuntimeError('resource owner inspection incomplete')
        if not set(self.resource['capabilities']) <= set(data.get('capabilities', [])):
            raise RuntimeError('runtime resource capabilities do not match configured host')
        services = {sid: self.unit(sid) for sid in self.services}
        if any(s['active'] and not live(s['process_identity']) for s in services.values()):
            raise RuntimeError('service identity changed during health inspection')
        from execution_worker import proc_info
        for owner in data['owners']:
            actual = proc_info(owner['pid'])
            if not actual or any(actual[k] != owner.get(k) for k in ('pid', 'ticks', 'boot_id')):
                raise RuntimeError('resource owner changed during inspection')
            owner.pop('service', None)
            group = Path(f"/proc/{owner['pid']}/cgroup").read_text()
            owner['cgroup'] = group
            for sid, service in services.items():
                cg = service['cgroup']
                if service['active'] and cg and any(line.split(':', 2)[-1] == cg or
                        line.split(':', 2)[-1].startswith(cg + '/') for line in group.splitlines()):
                    owner['service'] = sid
        return {'owners': data['owners'], 'services': services, 'capabilities': data['capabilities'],
                'environment': data.get('environment', {}), 'timestamp': now()}

    def snapshot(self):
        # Single-experiment reliability: a transient probe/helper failure
        # under GPU load (rocminfo contention while the workload holds the
        # device) must not kill the run. Re-sample bounded times; persistent
        # disagreement stays fail-closed and is quarantined by the caller.
        # Unknown owners, capability mismatch, and incomplete inspection are
        # never retried here.
        for attempt in range(3):
            try:
                return self._snapshot_once()
            except RuntimeError as exc:
                msg = str(exc)
                transient = ('resource owner changed during inspection' in msg
                             or 'configured probe/service command failed' in msg
                             or 'configured helper failed/cleanup unverified' in msg)
                if not transient or attempt == 2:
                    raise
                time.sleep(.15 * (attempt + 1))

    def stop(self, sid):
        self.run(['/usr/bin/systemctl', '--user', 'stop', self.services[sid]['unit']], timeout=30)

    def start(self, sid):
        self.run(['/usr/bin/systemctl', '--user', 'start', self.services[sid]['unit']], timeout=30)


class ResourceManager:
    def __init__(self, host, adapter=None):
        self.host = validate_host(host)
        self.resource = host['resources']['gpu']
        root = Path(host['resource_root'])
        if root.resolve() != root:
            raise ValueError('resource root may not traverse symlinks')
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root = root
        self.path = root / (self.resource['resource_id'].replace(':', '-') + '.json')
        self.lock = self.path.with_suffix('.lock')
        self.adapter = adapter or SystemAdapter(host)
        self.receipt = None
        self.owns = False
        self.owned_id = None
        # The same hardware must not acquire divergent service policies under one ledger.
        policy = {k: host[k] for k in ('host_id', 'resources', 'managed_services', 'capabilities')}
        with locked(root / 'host.lock'):
            p = root / 'host.json'
            if p.exists() and read(p) != policy:
                raise ValueError('shared host resource policy conflict')
            if not p.exists():
                save(p, policy)

    def persist(self):
        self.receipt['updated_at'] = now()
        # Reservation history remains available after a later reservation starts.
        save(self.root / 'receipts' / (self.receipt['reservation_id'] + '.json'), self.receipt)
        save(self.path, self.receipt)

    def load(self):
        self.receipt = read(self.path) if self.path.exists() else None
        return self.receipt

    def load_owned(self):
        self.load()
        if not self.owned_id or not self.receipt or self.receipt['reservation_id'] != self.owned_id:
            raise RuntimeError('reservation ownership changed; refusing operation')

    def quarantine(self, error):
        self.receipt.update(state='QUARANTINED', cleanup='unknown', error=str(error))
        self.persist()

    def _known(self, snap):
        services = snap['services']
        for owner in snap['owners']:
            sid = owner.get('service')
            if sid not in services or not services[sid]['active']:
                raise RuntimeError('CONFLICT: unknown resource owner; left untouched')

    def _healthy(self, snap, original):
        self._known(snap)
        if set(snap['services']) != set(original['services']):
            raise RuntimeError('managed service set changed')
        for sid, before in original['services'].items():
            after = snap['services'][sid]
            if after['active'] != before['active'] or (before['active'] and not after['healthy']):
                raise RuntimeError('service restoration/health unverified: ' + sid)
            if any(after.get(k) != before.get(k) for k in ('unit', 'unit_file_state', 'fragment_path', 'fragment_sha256')):
                raise RuntimeError('managed service configuration changed: ' + sid)
            expected = self.host['managed_services'][sid]['expects_owner']
            if before['active'] and expected and not any(o.get('service') == sid for o in snap['owners']):
                raise RuntimeError('expected service GPU ownership absent: ' + sid)

    def acquire(self, request, runner_identity):
        admit(self.host, request)
        rid = digest(canonical({'host': self.host['host_id'], 'resource': self.resource['resource_id'],
                               'flow': request['flow_id'], 'job': request['job_id'],
                               'request': digest(canonical(request))}))
        with locked(self.lock, nonblocking=True):
            previous = self.load()
            if previous and previous['state'] != 'AVAILABLE':
                if not live(previous.get('runner')) and previous['state'] == 'BUSY':
                    self.quarantine('reservation supervisor disappeared')
                raise RuntimeError('resource unavailable: ' + self.receipt['state'])
            if (self.root / 'receipts' / (rid + '.json')).exists():
                raise ValueError('reservation already executed; reconcile original job')
            self.receipt = {'schema_version': 1, 'reservation_id': rid, 'host_id': self.host['host_id'],
                            'resource_id': self.resource['resource_id'], 'flow_id': request['flow_id'],
                            'job_id': request['job_id'], 'request_hash': digest(canonical(request)),
                            'requirements': request['requirements'], 'runner': runner_identity,
                            'state': 'RECOVERING', 'acquire': 'pending', 'release': 'pending',
                            'cleanup': 'unknown', 'created_at': now(), 'before': None, 'after': None,
                            'during': [], 'services_touched': [], 'execution_started': False,
                            'process_cleanup': 'not_started'}
            self.owns = True
            self.owned_id = rid
            self.persist()
            try:
                before = self.adapter.snapshot()
                self.receipt['before'] = before
                self.persist()  # Original state and identities durable BEFORE any stop.
                self._healthy(before, before)
                for sid, status in before['services'].items():
                    if status['active']:
                        self.receipt['services_touched'].append(sid)
                        self.persist()  # A crash inside stop still requires restoration.
                        self.adapter.stop(sid)
                free = self.adapter.snapshot()
                if free['owners'] or any(s['active'] for s in free['services'].values()):
                    raise RuntimeError('resource not released by managed services')
                self.receipt.update(state='BUSY', acquire='verified', acquired=free)
                self.persist()
                return self.receipt
            except BaseException as exc:
                self.receipt['acquire'] = 'failed'
                self.receipt['error'] = str(exc)
                self._release('verified')
                raise

    def launched(self, identity):
        with locked(self.lock):
            self.load_owned()
            if self.receipt['state'] != 'BUSY':
                raise RuntimeError('resource reservation is not active')
            self.receipt.update(execution_started=True, process_cleanup='unknown', command_process=identity)
            self.persist()

    def observe(self, owned_process_identities):
        with locked(self.lock):
            self.load_owned()
            snap = self.adapter.snapshot()
            allowed = {(o['pid'], o['ticks'], o['boot_id']) for o in owned_process_identities}
            if any((o['pid'], o['ticks'], o['boot_id']) not in allowed for o in snap['owners']):
                self.quarantine('unknown resource owner appeared during execution')
                raise RuntimeError('unknown GPU owner during job; left untouched')
            if snap['owners']:
                self.receipt['during'] = snap['owners']
            self.receipt['last_observed_at'] = now()
            self.persist()

    def release(self, process_cleanup='verified'):
        with locked(self.lock):
            self.load_owned()
            return self._release(process_cleanup)

    def _release(self, process_cleanup):
        r = self.receipt
        r.update(state='RECOVERING', process_cleanup=process_cleanup)
        self.persist()
        try:
            if process_cleanup != 'verified':
                raise RuntimeError('owned process cleanup unverified')
            if not r.get('before'):
                raise RuntimeError('original state unavailable; operator inspection required')
            snap = self.adapter.snapshot()
            self._known(snap)
            # Failed preflight/unknown owner never authorizes a service mutation.
            for sid in r['services_touched']:
                original = r['before']['services'][sid]
                current = snap['services'][sid]
                if original['active'] and not current['active']:
                    self.adapter.start(sid)
            # Model loading may need time; never use active alone as health proof.
            deadline = time.monotonic() + (60 if isinstance(self.adapter, SystemAdapter) else 0)
            while True:
                after = self.adapter.snapshot()
                r['after'] = after
                self.persist()
                try:
                    self._healthy(after, r['before'])
                    break
                except RuntimeError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(1)
            r.update(state='AVAILABLE', release='verified', cleanup='verified')
            r.pop('error', None)
            self.persist()
        except BaseException as exc:
            r['release'] = 'failed'
            self.quarantine(exc)
        return r

    def reconcile(self, reset=False):
        with locked(self.lock):
            r = self.load()
            if not r:
                return {'state': 'AVAILABLE', 'resource_id': self.resource['resource_id']}
            if r['state'] == 'AVAILABLE':
                return r  # No stale receipt replay; next acquire performs fresh inspection.
            if live(r.get('runner')) and r['state'] != 'AVAILABLE':
                if reset:
                    raise RuntimeError('reservation still has a live supervisor')
                return r
            if r['state'] not in ('AVAILABLE', 'QUARANTINED') and not reset:
                self.quarantine('orphaned/unfinished reservation; explicit reconciliation required')
            if reset:
                if r['execution_started'] and r['process_cleanup'] != 'verified':
                    self.quarantine('orphaned execution cannot prove descendant cleanup')
                    return r
                if live(r.get('command_process')):
                    self.quarantine('owned command still alive; reset prohibited')
                    return r
                return self._release('verified')
            return r
