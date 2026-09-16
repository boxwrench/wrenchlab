"""Deterministic experimenter/session tests over the local v2 executor."""
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / 'scripts' / 'run-experimenter.py'
BROKER = ROOT / 'scripts' / 'execution_broker.py'


class ExperimenterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='wrenchlab-experimenter-test-')
        root = Path(self.tmp.name)
        self.campaign = root / 'campaign'
        # Keep the fixture independent of the public checkout's commit state.
        # The experiment still records an exact, attributable source revision.
        self.source = root / 'source'
        self.source.mkdir()
        (self.source / 'README.md').write_text('deterministic experiment source\n')
        subprocess.run(['git', '-C', str(self.source), 'init'], check=True,
                       capture_output=True, text=True)
        subprocess.run(['git', '-C', str(self.source), 'add', 'README.md'], check=True,
                       capture_output=True, text=True)
        subprocess.run(['git', '-C', str(self.source), '-c', 'user.name=Fixture',
                        '-c', 'user.email=fixture@example.invalid', 'commit', '-m',
                        'fixture source'], check=True, capture_output=True, text=True)
        self.source_revision = subprocess.check_output(
            ['git', '-C', str(self.source), 'rev-parse', 'HEAD'], text=True).strip()
        self.host_file = root / 'executor.json'
        self.author_file = root / 'author.json'
        self.host = {
            'schema_version': 2, 'worker_id': 'exp-fixture-worker', 'host_id': 'fixture-executor',
            'transport': 'local', 'root': str(root / 'worker'), 'resource_root': str(root / 'resources'),
            'capabilities': ['cpu'],
            'capability_evidence': {'verified_at': 1, 'description': 'deterministic fixture host'},
            'resources': {}, 'managed_services': {},
            'runtimes': {'python': {'launcher': ['/usr/bin/python3']}},
        }
        self.author = {'host_id': 'fixture-author', 'transport': 'local',
                       'capabilities': ['cpu'], 'description': 'independent author placement'}
        self.host_file.write_text(json.dumps(self.host)); self.author_file.write_text(json.dumps(self.author))
        self.deadline = int(time.time() * 1000) + 120_000

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args, check=True):
        p = subprocess.run(['python3', str(RUNNER), '--campaign', str(self.campaign), *args],
                           capture_output=True, text=True, timeout=45)
        if check and p.returncode:
            self.fail(p.stdout + p.stderr)
        return json.loads(p.stdout)

    def init_and_deploy(self, *, steps=4, delay=200):
        self.run_cli('init', '--author-host', str(self.author_file), '--executor-host', str(self.host_file),
                     '--repo', str(self.source), '--revision', self.source_revision,
                     '--runtime', 'python', '--mode', 'cpu',
                     '--steps', str(steps), '--delay-ms', str(delay), '--timeout-ms', '30_000',
                     '--deadline', str(self.deadline), '--owner', 'fixture', '--objective', 'bounded test')
        p = subprocess.run(['python3', str(BROKER), '--campaign', str(self.campaign), 'deploy'],
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)

    def test_independent_hosts_progress_and_evidence(self):
        self.init_and_deploy(steps=5, delay=250)
        started = self.run_cli('start')
        self.assertEqual(started['author_host']['host_id'], 'fixture-author')
        self.assertEqual(started['executor_host']['host_id'], 'fixture-executor')
        state = started
        end = time.monotonic() + 30
        while state['state'] not in ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT') and time.monotonic() < end:
            time.sleep(.15); state = self.run_cli('status')
        self.assertEqual(state['state'], 'SUCCEEDED', state)
        self.assertEqual(state['progress']['state'], 'SUCCEEDED')
        self.assertGreaterEqual(state['progress']['step'], 5)
        self.assertIsNotNone(state['last_progress_at'])
        self.assertEqual(state['evidence_integrity'], 'verified')
        packet = json.loads(Path(state['evidence_packet']).read_text())
        self.assertEqual(packet['kind'], 'execution-evidence')
        self.assertEqual(packet['result']['execution'], 'success')
        self.assertEqual(packet['result']['cleanup'], 'verified')
        self.assertIn('artifact/progress.json', state['artifact_hashes'])
        self.assertIn('artifact/experiment-result.json', state['artifact_hashes'])
        self.assertEqual([x['to'] for x in state['transitions']], ['READY', 'QUEUED', 'RUNNING', 'SUCCEEDED'])
        inspected = subprocess.run(['node', str(ROOT / 'scripts' / 'inspect-state.mjs'), '--campaign', str(self.campaign)],
                                   capture_output=True, text=True, timeout=20)
        self.assertEqual(inspected.returncode, 0, inspected.stdout + inspected.stderr)
        self.assertEqual(json.loads(inspected.stdout)['experimenter']['state'], 'SUCCEEDED')

    def test_one_interruption_is_authoritative_and_partial(self):
        self.init_and_deploy(steps=40, delay=250)
        self.run_cli('start')
        running = None
        end = time.monotonic() + 15
        while time.monotonic() < end:
            running = self.run_cli('status')
            if running['state'] == 'RUNNING' and (running.get('progress') or {}).get('step', 0) >= 1:
                break
            time.sleep(.15)
        self.assertEqual(running['state'], 'RUNNING', running)
        final = self.run_cli('cancel', '--wait-seconds', '20')
        self.assertEqual(final['state'], 'CANCELLED', final)
        self.assertEqual(final['result_summary']['execution'], 'cancelled')
        self.assertEqual(final['result_summary']['cleanup'], 'verified')
        self.assertTrue(final['partial_artifacts'])
        self.assertEqual(final['worker_status'], 'AVAILABLE')
        self.assertEqual(final['evidence_integrity'], 'verified')
        self.assertEqual(final['progress']['state'], 'RUNNING')
        self.assertEqual([x['to'] for x in final['transitions']], ['READY', 'QUEUED', 'RUNNING', 'CANCELLED'])


if __name__ == '__main__':
    unittest.main()
