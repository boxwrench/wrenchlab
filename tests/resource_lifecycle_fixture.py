"""End-to-end resource protocol fixtures. No real GPU/service operations."""
import copy
import json
import os
from pathlib import Path
import signal
import sys
import time
import unittest
from execution_fixture import ExecutionTests, ROOT
from execution_broker import Broker, LocalTransport
from execution_protocol import now, read, save
from execution_worker import alive, proc_info


class ResourceLifecycle(ExecutionTests):
    def setUp(self):
        super().setUp()
        self.probe_state = self.root / 'probe.json'
        save(self.probe_state, {'complete': True, 'owners': [], 'capabilities': ['rocm', 'gpu']})
        self.config = {'schema_version': 2, 'worker_id': 'lab-worker', 'host_id': 'fixture-lab',
            'transport': 'local', 'root': str(self.root / 'local-worker'),
            'capabilities': ['cpu', 'rocm', 'gpu'],
            'capability_evidence': {'verified_at': now(), 'description': 'deterministic fake GPU; no hardware qualification'},
            'resource_root': str(self.root / 'host-resources'),
            'resources': {'gpu': {'resource_id': 'gpu:primary', 'capabilities': ['rocm', 'gpu'],
                'probe': ['/usr/bin/cat', str(self.probe_state)], 'environment': {'HIP_VISIBLE_DEVICES': '0', 'ROCR_VISIBLE_DEVICES': '0'}}},
            'managed_services': {}}
        self.transport = LocalTransport(self.config)
        self.transport.deploy()
        self.campaign = self.root / 'resource-campaign'
        self.broker = Broker(self.campaign)
        self.broker.create(self.config, now() + 60_000, 'fixture', 'end-to-end resource lifecycle')

    def prepare(self, mode='success', jid='job-a', timeout=10_000, artifacts=None, gpu=True):
        return self.broker.prepare({'job_id': jid, 'repo': str(self.source), 'revision': self.commit,
            'command': ['python3', 'task.py', mode], 'timeout_ms': timeout,
            'artifacts': artifacts if artifacts is not None else [{'path': 'result.json', 'required': True}],
            'requirements': {'capabilities': ['rocm', 'gpu'] if gpu else ['cpu'],
                             'resources': {'gpu': {'exclusive': True}} if gpu else {}}})

    def packet(self, jid='job-a'):
        self.broker.action('collect', jid)
        return read(self.campaign / 'execution/jobs' / jid / 'evidence/packet.json')

    def test_local_resource_roundtrip(self):
        request = self.prepare()
        self.broker.action('submit', 'job-a')
        result = self.wait()['result']
        self.assertEqual(result['execution'], 'success')
        self.assertEqual(result['resource']['state'], 'AVAILABLE')
        packet = self.packet()
        self.assertEqual(packet['result']['placement']['transport'], 'local')
        self.assertEqual(packet['request']['source']['commit'], self.commit)
        self.assertEqual(packet['result']['resource']['requirements'], request['requirements'])

    def test_local_failure_timeout_cancel_restore(self):
        for mode, outcome in [('fail', 'failed'), ('tree', 'timeout'), ('sleep', 'cancelled')]:
            jid = 'job-' + mode
            self.prepare(mode, jid, timeout=1200 if mode == 'tree' else 10_000)
            self.broker.action('submit', jid)
            if mode == 'sleep':
                self.wait(jid, command=True)
                self.broker.action('cancel', jid)
            result = self.wait(jid)['result']
            self.assertEqual(result['execution'], outcome)
            self.assertEqual(result['cleanup'], 'verified')
            self.assertEqual(result['resource']['cleanup'], 'verified')
            self.assertEqual(result['resource']['state'], 'AVAILABLE')
            self.packet(jid)

    def test_local_controller_restart_and_lost_ack(self):
        import subprocess
        self.prepare('brief')
        program = "import sys,time;sys.path.insert(0,sys.argv[1]);from execution_broker import Broker;b=Broker(sys.argv[2]);b.action('submit','job-a');time.sleep(30)"
        controller = subprocess.Popen([sys.executable, '-c', program, str(ROOT / 'scripts'), str(self.campaign)])
        try:
            running = self.wait(command=True)
            controller.kill(); controller.wait(timeout=5)
            self.broker = Broker(self.campaign)
            observed = self.broker.reconcile()['job-a']
            self.assertEqual(observed['job']['request_hash'], running['job']['request_hash'])
            # Lost submit response is safe to retry through the real local transport.
            self.broker.action('submit', 'job-a')
            result = self.wait()['result']
            self.assertEqual(result['resource']['state'], 'AVAILABLE')
            self.assertEqual((Path(self.config['root']) / 'jobs/job-a/work/count.txt').read_text(), 'executed\n')
            self.packet()
        finally:
            if controller.poll() is None:
                controller.kill(); controller.wait(timeout=5)

    def test_resource_quarantine_allows_cpu(self):
        owner = proc_info(os.getpid())
        save(self.probe_state, {'complete': True, 'owners': [owner], 'capabilities': ['rocm', 'gpu']})
        self.prepare(); self.broker.action('submit', 'job-a')
        result = self.wait()['result']
        self.assertEqual(result['execution'], 'failed')
        self.assertEqual(result['resource']['state'], 'QUARANTINED')
        self.assertEqual(result['cleanup'], 'verified')
        self.assertTrue(alive(owner))
        self.packet()  # Hazard receipt is visible; verified bytes != safe resource.
        self.prepare(jid='cpu', gpu=False); self.broker.action('submit', 'cpu')
        self.assertEqual(self.wait('cpu')['result']['execution'], 'success')
        self.packet('cpu')
        save(self.probe_state, {'complete': True, 'owners': [], 'capabilities': ['rocm', 'gpu']})
        end = time.monotonic() + 3
        while time.monotonic() < end:
            try:
                reset = self.transport.call({'op': 'resource-reset'})
                break
            except ValueError:
                time.sleep(.05)
        self.assertEqual(reset['state'], 'AVAILABLE')

    def test_resource_evidence_tamper_rejected(self):
        self.prepare(); self.broker.action('submit', 'job-a'); self.wait(); self.packet()
        p = self.campaign / 'execution/jobs/job-a/transfer.json'
        data = read(p); data['result']['resource']['reservation_id'] = '0' * 64; save(p, data)
        import subprocess
        check = subprocess.run(['node', str(ROOT / 'scripts/ingest-job.mjs'), str(self.campaign), str(p.parent)], capture_output=True)
        self.assertNotEqual(check.returncode, 0)

    def test_bounded_helper_cleans_detached_descendant(self):
        import subprocess
        marker = self.root / 'helper-child.json'
        program = ("import subprocess,json,time; from pathlib import Path; "
                   "p=subprocess.Popen(['sleep','30'],start_new_session=True); "
                   "Path(" + repr(str(marker)) + ").write_text(json.dumps({'pid':p.pid})); time.sleep(30)")
        p = subprocess.run([sys.executable, str(ROOT / 'scripts/execution_probe.py'), '.4', sys.executable, '-c', program],
                           capture_output=True, timeout=8)
        self.assertNotEqual(p.returncode, 0)
        child = proc_info(read(marker)['pid'])
        self.assertFalse(child and child['state'] != 'Z')

    def test_concrete_gpu_target_parser(self):
        from unittest.mock import patch
        import subprocess
        from gpu_owner_probe import probe
        output = '  Name: gfx0000\n  Name: amdgcn-amd-amdhsa--generic\n'
        # Reach owner inspection (not the target ambiguity exception) without a GPU.
        with patch('gpu_owner_probe.subprocess.run', return_value=subprocess.CompletedProcess([], 0, output, '')):
            with patch('gpu_owner_probe.Path.is_dir', return_value=False):
                with self.assertRaisesRegex(RuntimeError, 'KFD owner inspection unavailable'):
                    probe('/fake/rocminfo', 'gfx0000')
        output += '  Name: gfx1201\n'
        with patch('gpu_owner_probe.subprocess.run', return_value=subprocess.CompletedProcess([], 0, output, '')):
            with self.assertRaisesRegex(RuntimeError, 'unambiguous'):
                    probe('/fake/rocminfo', 'gfx0000')

    def test_selected_gpu_owner_filter(self):
        # Per-GPU ownership: only queues on the selected topology node block.
        from types import SimpleNamespace
        from unittest.mock import patch
        import subprocess
        from gpu_owner_probe import probe
        output = '  Name: gfx1201\n'
        run = subprocess.CompletedProcess([], 0, output, '')
        # Topology: node 7 is gfx1201 (gpuid 2277), node 9 is gfx1100 (gpuid 23276).
        topo = {'7': 120001, '9': 110000}
        gpuids = {'7': 2277, '9': 23276}
        def identity(pid):
            return {'pid': pid, 'state': 'S', 'ppid': 1, 'pgid': 1, 'ticks': '1', 'boot_id': 'b'}
        def entries(*args):
            return [SimpleNamespace(name='101', exists=lambda: True),
                    SimpleNamespace(name='202', exists=lambda: True)]
        def ctx(queue_side_effect):
            return (patch('gpu_owner_probe.subprocess.run', return_value=run),
                    patch('gpu_owner_probe._topology_target_map', return_value=dict(topo)),
                    patch('gpu_owner_probe._node_gpuid', side_effect=lambda n: gpuids[n]),
                    patch('gpu_owner_probe.proc_info', side_effect=identity),
                    patch('gpu_owner_probe._proc_entries', side_effect=entries),
                    patch('pathlib.Path.read_text', return_value='x'),
                    patch('pathlib.Path.readlink', return_value='/bin/true'),
                    patch('gpu_owner_probe._owner_queue_gpuids', side_effect=queue_side_effect))
        patches = ctx(lambda pid: {2277} if pid == 101 else {23276})
        for p in patches:
            p.start()
        try:
            result = probe('/fake/rocminfo', 'gfx1201')
        finally:
            for p in patches:
                p.stop()
        self.assertEqual([o['pid'] for o in result['owners']], [101])
        self.assertEqual([o['pid'] for o in result['other_gpu_owners']], [202])
        self.assertEqual(result['environment']['selected_gpuid'], 2277)
        # Owner on both GPUs still blocks.
        patches = ctx(lambda pid: {2277, 23276})
        for p in patches:
            p.start()
        try:
            result = probe('/fake/rocminfo', 'gfx1201')
        finally:
            for p in patches:
                p.stop()
        self.assertEqual(sorted(o['pid'] for o in result['owners']), [101, 202])
        self.assertEqual(result['other_gpu_owners'], [])
        # No owners passes with empty lists.
        patches = ctx(lambda pid: {2277})
        for p in patches:
            p.start()
        try:
            with patch('gpu_owner_probe._proc_entries', return_value=[]):
                result = probe('/fake/rocminfo', 'gfx1201')
        finally:
            for p in patches:
                p.stop()
        self.assertEqual(result['owners'], [])
        self.assertEqual(result['other_gpu_owners'], [])
        # Ambiguous topology mapping fails closed.
        with patch('gpu_owner_probe.subprocess.run', return_value=run), \
             patch('gpu_owner_probe._topology_target_map', return_value={'7': 120001, '8': 120001}):
            with self.assertRaisesRegex(RuntimeError, 'ambiguously'):
                probe('/fake/rocminfo', 'gfx1201')
        # Unreadable queue data fails closed, never reads as absent.
        patches = ctx(RuntimeError('KFD queue data incomplete for owner: 101'))
        for p in patches:
            if 'queue_gpuids' not in str(p):
                p.start()
        started = [p for p in patches if 'queue_gpuids' not in str(p)]
        try:
            with patch('gpu_owner_probe._owner_queue_gpuids',
                       side_effect=RuntimeError('KFD queue data incomplete for owner: 101')):
                with self.assertRaisesRegex(RuntimeError, 'queue data'):
                    probe('/fake/rocminfo', 'gfx1201')
        finally:
            for p in started:
                p.stop()

    def test_cross_worker_exclusive_admission(self):
        self.prepare('sleep'); self.broker.action('submit', 'job-a'); self.wait(command=True)
        config = copy.deepcopy(self.config)
        config.update(worker_id='second-worker', root=str(self.root / 'second-worker'))
        second = Broker(self.root / 'second-campaign')
        second.create(config, now() + 60_000, 'fixture', 'conflicting reservation')
        transport = second.transport(read(second.state_file)); transport.deploy()
        second.prepare({'job_id': 'conflict', 'repo': str(self.source), 'revision': self.commit,
            'command': ['python3', 'task.py', 'success'], 'timeout_ms': 5000, 'artifacts': [],
            'requirements': {'capabilities': ['rocm'], 'resources': {'gpu': {'exclusive': True}}}})
        second.action('submit', 'conflict')
        end = time.monotonic() + 5
        while time.monotonic() < end:
            result = second.action('inspect', 'conflict')['result']
            if result:
                break
            time.sleep(.05)
        self.assertEqual(result['execution'], 'failed')
        self.assertIsNone(result['command_process'])
        self.assertEqual(result['resource_admission'], 'rejected')
        self.assertEqual(self.transport.call({'op': 'resource-status'})['job_id'], 'job-a')
        self.assertEqual(self.transport.call({'op': 'resource-status'})['state'], 'BUSY')
        second.action('collect', 'conflict')
        self.broker.action('cancel', 'job-a'); self.wait()

    def test_dead_supervisor_quarantines_resource(self):
        self.prepare('sleep'); self.broker.action('submit', 'job-a')
        observation = self.wait(command=True)
        runner = observation['job']['runner']
        command = observation['job']['command_process']
        os.kill(runner['pid'], signal.SIGKILL)
        end = time.monotonic() + 3
        while alive(runner) and time.monotonic() < end:
            time.sleep(.02)
        self.assertEqual(self.transport.call({'op': 'resource-status'})['state'], 'QUARANTINED')
        self.assertEqual(self.transport.call({'op': 'resource-reset'})['state'], 'QUARANTINED')
        self.assertTrue(alive(command))  # Reset cannot guess ownership or kill an unproven tree.
        self.assertEqual(self.broker.action('inspect', 'job-a')['worker_status'], 'QUARANTINED')


if __name__ == '__main__':
    unittest.main()
