"""Real local worker processes with substituted transport; never claims SSH qualification."""
import base64
import concurrent.futures
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from execution_broker import Broker, DEPLOY, deployment
from execution_protocol import canonical, digest, now, read, save, validate_request
from execution_worker import alive, proc_info


class LocalTransport:
    def __init__(self, config):
        self.config = config
        self.lose_ack = False

    def deploy(self):
        p = subprocess.run([sys.executable, '-c', DEPLOY], input=json.dumps(deployment(self.config)),
                           text=True, capture_output=True, timeout=10)
        if p.returncode:
            raise RuntimeError(p.stderr)
        return json.loads(p.stdout)

    def call(self, request):
        root = self.config['root']
        p = subprocess.run([sys.executable, root + '/execution_worker.py', root],
                           input=json.dumps(request), text=True, capture_output=True, timeout=10)
        response = json.loads(p.stdout)
        if p.returncode or 'error' in response:
            raise ValueError(response.get('error', p.stderr))
        if request['op'] == 'submit' and self.lose_ack:
            self.lose_ack = False
            raise ConnectionError('fixture dropped acknowledgement AFTER remote acceptance')
        return response


TASK = '''import json, pathlib, subprocess, sys, time
mode=sys.argv[1]
with open('count.txt','a') as f: f.write('executed\\n')
print('fixture stdout',flush=True)
print('fixture stderr',file=sys.stderr,flush=True)
if mode in ('tree','detached'):
    child=subprocess.Popen([sys.executable,'-c',"import subprocess,time; subprocess.Popen(['sleep','30']); time.sleep(30)"],start_new_session=(mode=='detached'))
    pathlib.Path('child.json').write_text(json.dumps({'pid':child.pid}))
if mode in ('tree','sleep'): time.sleep(30)
if mode=='brief': time.sleep(1)
if mode=='symlink': pathlib.Path('result.json').symlink_to('/etc/passwd')
else: pathlib.Path('result.json').write_text('{"answer":42}\\n')
if mode=='fail': raise SystemExit(7)
'''


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='wrenchlab-execution-test-')
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        (self.source / 'task.py').write_text(TASK)
        def git(*args):
            return subprocess.check_output(['git', '-C', str(self.source), *args], stderr=subprocess.DEVNULL).decode().strip()
        git('init'); git('add', 'task.py')
        git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-m', 'CPU fixture')
        self.commit = git('rev-parse', 'HEAD')
        self.config = {'schema_version': 1, 'worker_id': 'cpu', 'host': 'fixture', 'root': str(self.root / 'worker')}
        self.transport = LocalTransport(self.config)
        self.transport.deploy()
        self.campaign = self.root / 'campaign-fixture'
        self.broker = Broker(self.campaign, self.transport)
        self.broker.create(self.config, now() + 60_000, 'test-owner', 'deterministic qualification', max_jobs=8)

    def tearDown(self):
        p = Path(self.config['root']) / 'state.json'
        if p.exists():
            for jid, job in read(p)['jobs'].items():
                try:
                    self.transport.call({'op': 'cancel', 'job_id': jid})
                    end = time.monotonic() + 4
                    while alive(job.get('runner')) and time.monotonic() < end:
                        time.sleep(.03)
                    for key in ('command_process', 'runner'):
                        identity = job.get(key)
                        if alive(identity):
                            if key == 'command_process':
                                os.killpg(identity['pid'], signal.SIGKILL)
                            else:
                                os.kill(identity['pid'], signal.SIGKILL)
                except (OSError, ValueError):
                    pass
        self.tmp.cleanup()

    def prepare(self, mode='success', jid='job-a', timeout=10_000, artifacts=None):
        return self.broker.prepare({'job_id': jid, 'repo': str(self.source), 'revision': self.commit,
           'command': ['python3', 'task.py', mode], 'timeout_ms': timeout,
           'artifacts': artifacts if artifacts is not None else [{'path': 'result.json', 'required': True}, {'path': 'count.txt', 'required': True}]})

    def wait(self, jid='job-a', command=False):
        end = time.monotonic() + 12
        last = None
        while time.monotonic() < end:
            try:
                last = self.broker.action('inspect', jid)
            except ValueError as exc:
                if command and 'unknown job ID' in str(exc):
                    time.sleep(.03); continue
                raise
            if (command and last['job'].get('command_process')) or (not command and last['job']['state'] == 'TERMINAL'):
                return last
            if last['worker_status'] == 'QUARANTINED':
                self.fail(str(last))
            time.sleep(.04)
        self.fail('worker did not settle: ' + str(last))

    def test_A_lost_acknowledgement(self):
        self.prepare()
        self.transport.lose_ack = True
        with self.assertRaises(ConnectionError):
            self.broker.action('submit', 'job-a')
        restarted = Broker(self.campaign, LocalTransport(self.config))
        restarted.action('submit', 'job-a')
        done = self.wait()
        self.assertEqual(done['result']['execution'], 'success')
        restarted.action('collect', 'job-a')
        count = self.campaign / 'execution/jobs/job-a/evidence/artifact/count.txt'
        self.assertEqual(count.read_text(), 'executed\n')

    def test_B_controller_restart(self):
        self.prepare('brief')
        # Kill an actual state-owning controller process after remote acceptance.
        program = "import sys,time;sys.path.insert(0,sys.argv[1]);from execution_fixture import LocalTransport,Broker;import json; b=Broker(sys.argv[2],LocalTransport(json.loads(sys.argv[3])));b.action('submit','job-a');time.sleep(30)"
        controller = subprocess.Popen([sys.executable, '-c', program, str(ROOT / 'tests'), str(self.campaign), json.dumps(self.config)])
        try:
            running = self.wait(command=True)
            controller.kill(); controller.wait(timeout=5)
            restarted = Broker(self.campaign, LocalTransport(self.config))
            observed = restarted.reconcile()['job-a']
            self.assertEqual(observed['job']['request_hash'], running['job']['request_hash'])
            self.assertEqual(read(restarted.state_file)['deadline'], running['job']['request']['deadline'])
            self.assertEqual(self.wait()['result']['execution'], 'success')
            restarted.action('collect', 'job-a')
            self.assertEqual((self.campaign / 'execution/jobs/job-a/evidence/artifact/count.txt').read_text(), 'executed\n')
        finally:
            if controller.poll() is None:
                controller.kill(); controller.wait(timeout=5)

    def test_C_command_failure(self):
        self.prepare('fail'); self.broker.action('submit', 'job-a')
        done = self.wait()
        self.assertEqual((done['result']['execution'], done['result']['exit_code'], done['result']['cleanup'], done['worker_status']), ('failed', 7, 'verified', 'AVAILABLE'))
        self.broker.action('collect', 'job-a')
        self.assertIn('fixture stderr', (self.campaign / 'execution/jobs/job-a/evidence/log/stderr.log').read_text())
        self.prepare(jid='job-b'); self.broker.action('submit', 'job-b')
        self.assertEqual(self.wait('job-b')['result']['execution'], 'success')

    def test_D_timeout_tree(self):
        self.prepare('tree', timeout=1200); self.broker.action('submit', 'job-a')
        running = self.wait(command=True)
        done = self.wait()
        self.assertEqual((done['result']['execution'], done['result']['cleanup'], done['worker_status']), ('timeout', 'verified', 'AVAILABLE'))
        self.assertFalse(alive(running['job']['command_process']))
        child = read(Path(self.config['root']) / 'jobs/job-a/work/child.json')
        info = proc_info(child['pid'])
        self.assertTrue(info is None or info['state'] == 'Z')

    def test_E_cancellation_tree(self):
        self.prepare('tree'); self.broker.action('submit', 'job-a')
        running = self.wait(command=True)
        time.sleep(.1)
        self.broker.action('cancel', 'job-a')
        done = self.wait()
        self.assertEqual((done['result']['execution'], done['result']['cleanup']), ('cancelled', 'verified'))
        self.assertFalse(alive(running['job']['command_process']))

    def test_F_corrupted_transfer(self):
        self.prepare(); self.broker.action('submit', 'job-a'); self.wait()
        transfer = self.transport.call({'op': 'collect', 'job_id': 'job-a'})
        transfer['files']['artifact/result.json'] = base64.b64encode(b'truncated').decode()
        folder = self.campaign / 'execution/jobs/job-a'
        save(folder / 'transfer.json', transfer)
        p = subprocess.run(['node', str(ROOT / 'scripts/ingest-job.mjs'), str(self.campaign), str(folder)], capture_output=True, text=True)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn('hash verification failed', p.stderr)
        self.assertFalse((folder / 'evidence').exists())

    def test_G_conflicting_retry(self):
        request = self.prepare(); self.broker.action('submit', 'job-a'); self.wait()
        for change in ('command', 'source', 'deadline'):
            altered = copy.deepcopy(request)
            if change == 'command': altered['command'] = ['true']
            elif change == 'source': altered['source']['commit'] = 'a' * 40
            else: altered['deadline'] += 1
            with self.assertRaisesRegex(ValueError, 'CONFLICT'):
                self.transport.call({'op': 'submit', 'request': altered, 'bundle': ''})

    def test_H_orphan_quarantine(self):
        self.prepare('sleep'); self.broker.action('submit', 'job-a')
        running = self.wait(command=True)
        os.kill(running['job']['runner']['pid'], signal.SIGKILL)
        end = time.monotonic() + 3
        while alive(running['job']['runner']) and time.monotonic() < end: time.sleep(.02)
        response = self.broker.action('inspect', 'job-a')
        self.assertEqual(response['worker_status'], 'QUARANTINED')
        self.assertEqual(response['job']['cleanup'], 'unknown')
        self.prepare(jid='job-b')
        with self.assertRaisesRegex(ValueError, 'QUARANTINED'):
            self.broker.action('submit', 'job-b')
        os.killpg(running['job']['command_process']['pid'], signal.SIGKILL)
        # Absence of that group alone does not erase the quarantine.
        self.assertEqual(self.broker.action('inspect', 'job-a')['worker_status'], 'QUARANTINED')

    def test_concurrent_duplicate_submission(self):
        self.prepare()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.broker.action('submit', 'job-a'), range(4)))
        self.wait(); self.broker.action('collect', 'job-a')
        self.assertEqual((self.campaign / 'execution/jobs/job-a/evidence/artifact/count.txt').read_text(), 'executed\n')

    def test_detached_descendant_cleanup(self):
        self.prepare('detached'); self.broker.action('submit', 'job-a'); done = self.wait()
        self.assertEqual(done['result']['cleanup'], 'verified')
        child = read(Path(self.config['root']) / 'jobs/job-a/work/child.json')
        info = proc_info(child['pid'])
        self.assertTrue(info is None or info['state'] == 'Z')

    def test_path_traversal_and_symlink(self):
        request = self.prepare('symlink')
        for bad in ('../outside', '/etc/passwd', 'a/../../b', 'a//b', '.git/config'):
            changed = copy.deepcopy(request); changed['artifacts'][0]['path'] = bad
            with self.assertRaises(ValueError): validate_request(changed)
        self.broker.action('submit', 'job-a'); done = self.wait()
        self.assertTrue(any('symlinks' in e for e in done['result']['evidence_errors']))
        with self.assertRaises(subprocess.CalledProcessError): self.broker.action('collect', 'job-a')

    def test_exact_source_and_idempotent_ingestion(self):
        self.prepare()
        # Dirty source edits after preparation must not reach the worker.
        (self.source / 'task.py').write_text('raise SystemExit(99)\n')
        self.broker.action('submit', 'job-a'); done = self.wait()
        self.assertEqual(done['result']['source']['commit'], self.commit)
        self.assertEqual(done['result']['execution'], 'success')
        self.broker.action('collect', 'job-a'); self.broker.action('collect', 'job-a')
        folder = self.campaign / 'execution/jobs/job-a/evidence'
        self.assertEqual((folder / 'artifact/result.json').read_bytes(), b'{"answer":42}\n')
        script = "import {exportCampaign} from './scripts/export-campaign.mjs';import fs from 'node:fs';const c=process.argv[1];const m=exportCampaign({campaignDir:c,sessionHome:c+'/no-sessions',startedMs:0,endedMs:Date.now(),status:0});if(m.executionEvidence.length!==1||m.validatedWorkerResultCount!==0)process.exit(2);"
        subprocess.run(['node', '--input-type=module', '-e', script, str(self.campaign)], cwd=ROOT, check=True)
        inspect = subprocess.check_output(['node', str(ROOT / 'scripts/inspect-state.mjs'), '--campaign', str(self.campaign)], cwd=ROOT)
        self.assertEqual(json.loads(inspect)['executionEvidence'][0]['integrity'], 'verified')
        (folder / 'artifact/result.json').write_text('corrupt')
        inspect = subprocess.check_output(['node', str(ROOT / 'scripts/inspect-state.mjs'), '--campaign', str(self.campaign)], cwd=ROOT)
        self.assertEqual(json.loads(inspect)['executionEvidence'][0]['integrity'], 'invalid')

    def test_limits_and_immutable_flow(self):
        self.prepare()
        state = read(self.broker.state_file); state['max_jobs'] = 1; save(self.broker.state_file, state)
        with self.assertRaisesRegex(ValueError, 'allowance'): self.prepare(jid='job-b')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.broker.create(self.config, now()+100_000, 'test', 'test')
        request = read(self.campaign / 'execution/jobs/job-a/request.json')
        request['command'] = ['false']; save(self.campaign / 'execution/jobs/job-a/request.json', request)
        with self.assertRaisesRegex(ValueError, 'immutable'): self.broker.action('submit', 'job-a')

    def test_worker_busy_and_expired_admission(self):
        self.prepare('sleep'); self.broker.action('submit', 'job-a'); self.wait(command=True)
        other = self.prepare(jid='job-b')
        with self.assertRaisesRegex(ValueError, 'BUSY'): self.broker.action('submit', 'job-b')
        self.broker.action('cancel', 'job-a'); self.wait()
        expired = copy.deepcopy(other); expired['created_at'] = now()-200; expired['deadline'] = now()-100
        with self.assertRaisesRegex(ValueError, 'deadline expired'):
            self.transport.call({'op':'submit','request':expired,'bundle':''})

    def test_recovery_of_queued_admission(self):
        # Simulate admission persisted before worker launch: safe retry has no prior RUNNING transition.
        request = self.prepare(); folder = Path(self.config['root']) / 'jobs/job-a'; folder.mkdir(parents=True)
        (folder/'input.bundle').write_bytes((self.campaign/'execution/jobs/job-a/input.bundle').read_bytes())
        state = read(Path(self.config['root'])/'state.json')
        state.update(status='BUSY', active_job='job-a')
        state['jobs']['job-a']={'request':request,'request_hash':digest(canonical(request)),'state':'QUEUED','cleanup':'unknown','accepted_at':now()-6000}
        save(Path(self.config['root'])/'state.json',state)
        self.assertEqual(self.transport.call({'op':'inspect','job_id':'job-a'})['worker_status'],'RECOVERING')
        self.broker.action('submit','job-a'); self.wait(); self.broker.action('collect','job-a')
        self.assertEqual((self.campaign/'execution/jobs/job-a/evidence/artifact/count.txt').read_text(),'executed\n')


if __name__ == '__main__':
    unittest.main()
