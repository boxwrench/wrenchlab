"""Deterministic resource/host lifecycle tests; no real services or GPU required."""
import copy
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from execution_hosts import admit, validate_host, validate_requirements
from execution_resources import ResourceManager


def identity(pid=101, ticks=7, boot_id="fixture-boot", cgroup="job-a"):
    return {"pid": pid, "ticks": ticks, "boot_id": boot_id, "cgroup": cgroup}


def live_identity(cgroup="job-a"):
    fields = Path("/proc/self/stat").read_text().split()
    return identity(os.getpid(), fields[21], Path("/proc/sys/kernel/random/boot_id").read_text().strip(), cgroup)


class FakeAdapter:
    def __init__(self, *, owners=None, active=True, healthy=True):
        self.service_owner = identity(202, 12, cgroup="managed-service")
        self.owners = copy.deepcopy(owners if owners is not None else
                                    ([dict(self.service_owner, service="managed-service")] if active else []))
        self.services = {"managed-service": {"active": active, "healthy": healthy,
                                   "cgroup": "user@fixture/managed-service"}}
        self.stop_calls = []
        self.start_calls = []
        self.fail_stop = False
        self.fail_after_stop = False
        self.fail_start = False
        self.fail_health = False
        self.fail_snapshot = False
        self.capabilities = ["cpu", "rocm", "gpu"]

    def snapshot(self):
        if self.fail_snapshot:
            raise RuntimeError("configured probe/service command failed: /usr/bin/python3")
        services = copy.deepcopy(self.services)
        if self.fail_health and "managed-service" in services:
            services["managed-service"]["healthy"] = False
        return {"owners": copy.deepcopy(self.owners), "services": services,
                "capabilities": list(self.capabilities)}

    def stop(self, service_id):
        if self.fail_stop:
            raise RuntimeError("injected stop failure")
        self.stop_calls.append(service_id)
        self.owners = [o for o in self.owners if o.get("service") != service_id]
        self.services[service_id]["active"] = False
        self.services[service_id]["healthy"] = False
        if self.fail_after_stop:
            raise RuntimeError("injected post-stop failure")

    def start(self, service_id):
        if self.fail_start:
            raise RuntimeError("injected start failure")
        self.start_calls.append(service_id)
        self.services[service_id]["active"] = True
        self.services[service_id]["healthy"] = not self.fail_health
        if not any(o.get("service") == service_id for o in self.owners):
            self.owners.append(dict(self.service_owner, service=service_id))


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wrenchlab-resource-test-")
        root = Path(self.tmp.name)
        self.host = {
            "schema_version": 2, "worker_id": "fixture-worker", "host_id": "fixture",
            "transport": "local", "root": str(root / "worker"),
            "resource_root": str(root / "resources"),
            "capabilities": ["cpu", "rocm", "gpu"],
            "capability_evidence": {"verified_at": 1, "description": "fixture"},
            "resources": {"gpu": {"resource_id": "gpu:primary",
                "capabilities": ["rocm", "gpu"], "probe": ["/bin/true"],
                "environment": {"HIP_VISIBLE_DEVICES": "0"}}},
            "managed_services": {"managed-service": {"unit": "managed.service",
                "conflicts_with": ["gpu:primary"], "health": ["/bin/true"],
                "expects_owner": True}},
        }
        self.request = {"job_id": "job-a", "flow_id": "flow-a",
                        "requirements": {"capabilities": ["rocm", "gpu"],
                                          "resources": {"gpu": {"exclusive": True}}},
                        "placement": {"executor": "fixture"},
                        "host_config_hash": ""}
        # admit() checks the immutable host digest; the manager receives the host directly.
        from execution_protocol import canonical, digest
        self.request["host_config_hash"] = digest(canonical(self.host))

    def tearDown(self):
        self.tmp.cleanup()

    def manager(self, adapter=None):
        return ResourceManager(self.host, adapter=adapter or FakeAdapter())

    def acquire(self, manager, request=None):
        return manager.acquire(request or self.request, identity())

    def test_host_capabilities_and_requirements(self):
        validate_host(self.host)
        validate_requirements(self.request["requirements"])
        admit(self.host, self.request)
        bad = copy.deepcopy(self.request)
        bad["requirements"]["capabilities"] = ["gfx1201"]
        with self.assertRaises(ValueError):
            admit(self.host, bad)

    def test_reservation_identity_and_exclusive_conflict(self):
        m = self.manager()
        receipt = self.acquire(m)
        self.assertEqual(receipt["state"], "BUSY")
        self.assertTrue(receipt["reservation_id"])
        with self.assertRaises((ValueError, RuntimeError)):
            self.acquire(m, {**self.request, "job_id": "job-b", "flow_id": "flow-b"})
        self.assertEqual(m.receipt["reservation_id"], receipt["reservation_id"])

    def test_unknown_owner_is_untouched(self):
        owner = identity(222, 9, cgroup="unrelated")
        adapter = FakeAdapter(owners=[owner])
        m = self.manager(adapter)
        with self.assertRaises((ValueError, RuntimeError)):
            self.acquire(m)
        self.assertEqual(adapter.owners, [owner])
        self.assertEqual(m.receipt["state"], "QUARANTINED")
        self.assertEqual(adapter.stop_calls, [])
        self.assertEqual(adapter.start_calls, [])

    def test_initially_off_service_stays_off(self):
        adapter = FakeAdapter(active=False, healthy=False)
        m = self.manager(adapter)
        self.acquire(m)
        m.release()
        self.assertEqual(adapter.stop_calls, [])
        self.assertEqual(adapter.start_calls, [])
        self.assertEqual(m.receipt["state"], "AVAILABLE")

    def test_partial_acquire_failure_restores_prior_active(self):
        adapter = FakeAdapter()
        adapter.fail_after_stop = True
        m = self.manager(adapter)
        with self.assertRaises((ValueError, RuntimeError)):
            self.acquire(m)
        self.assertTrue(adapter.services["managed-service"]["active"])
        self.assertEqual(m.receipt["state"], "AVAILABLE")
        self.assertEqual(adapter.start_calls, ["managed-service"])

    def test_release_after_failure_timeout_and_cancel(self):
        for index, outcome in enumerate(("failed", "timeout", "cancelled")):
            adapter = FakeAdapter()
            m = self.manager(adapter)
            self.acquire(m, {**self.request, "job_id": f"job-{outcome}-{index}"})
            result = m.release(process_cleanup="verified")
            self.assertEqual(result["cleanup"], "verified")
            self.assertEqual(result["state"], "AVAILABLE")

    def test_failed_health_restoration_quarantines(self):
        adapter = FakeAdapter()
        m = self.manager(adapter)
        self.acquire(m)
        adapter.fail_health = True
        result = m.release(process_cleanup="verified")
        self.assertIn(result["state"], ("RECOVERING", "QUARANTINED"))
        self.assertEqual(result["state"], "QUARANTINED")
        adapter.fail_health = False
        adapter.services["managed-service"]["healthy"] = True
        self.assertEqual(m.reconcile(reset=True)["state"], "AVAILABLE")

    def test_release_start_failure_quarantines_then_recovers(self):
        adapter = FakeAdapter()
        m = self.manager(adapter)
        self.acquire(m)
        adapter.fail_start = True
        result = m.release(process_cleanup="verified")
        self.assertEqual(result["state"], "QUARANTINED")
        adapter.fail_start = False
        self.assertEqual(m.reconcile(reset=True)["state"], "AVAILABLE")

    def test_safe_reset_after_healthy_reconciliation(self):
        adapter = FakeAdapter()
        m = self.manager(adapter)
        self.acquire(m)
        m.release(process_cleanup="verified")
        self.assertEqual(m.reconcile(reset=True)["state"], "AVAILABLE")

    def test_restart_active_runner_remains_busy(self):
        adapter = FakeAdapter()
        m = self.manager(adapter)
        self.acquire(m, {**self.request, "job_id": "job-restart"})
        m.receipt["runner"] = live_identity()
        m.persist()
        restarted = ResourceManager(self.host, adapter=adapter)
        state = restarted.reconcile()
        self.assertEqual(state["state"], "BUSY")
        self.assertEqual(state["reservation_id"], m.receipt["reservation_id"])

    def test_orphan_without_cleanup_cannot_reset(self):
        adapter = FakeAdapter()
        m = self.manager(adapter)
        self.acquire(m)
        m.launched(identity(303, 11, cgroup="owned-job"))
        state = m.reconcile()
        self.assertIn(state["state"], ("RECOVERING", "QUARANTINED"))
        self.assertEqual(m.reconcile(reset=True)["state"], "QUARANTINED")

    def test_old_manager_cannot_release_newer_reservation(self):
        adapter = FakeAdapter()
        old = self.manager(adapter)
        self.acquire(old, {**self.request, "job_id": "job-old"})
        old.release()
        newer = self.manager(adapter)
        self.acquire(newer, {**self.request, "job_id": "job-new"})
        with self.assertRaises(RuntimeError):
            old.release()
        self.assertEqual(newer.receipt["state"], "BUSY")

    def test_malformed_requirements_rejected(self):
        with self.assertRaises(ValueError):
            validate_requirements({"capabilities": ["rocm"], "resources": {"gpu": {"exclusive": False}}})
        with self.assertRaises(ValueError):
            validate_requirements({"capabilities": [], "resources": {"fpga": {"exclusive": True}}})
        missing = copy.deepcopy(self.request)
        missing["requirements"]["capabilities"] = ["missing"]
        with self.assertRaises(ValueError):
            admit(self.host, missing)

    def test_probe_failure_quarantines_before_mutation(self):
        adapter = FakeAdapter()
        adapter.fail_snapshot = True
        m = self.manager(adapter)
        with self.assertRaises(RuntimeError):
            self.acquire(m)
        self.assertEqual(m.receipt["state"], "QUARANTINED")
        self.assertEqual(adapter.stop_calls, [])
        self.assertEqual(adapter.start_calls, [])

    def test_snapshot_retries_transient_probe_failure(self):
        # Workload-reliable, safety intact: a transient probe/helper
        # failure under load re-samples (3 attempts); persistent failure
        # still raises.
        from execution_resources import SystemAdapter
        calls = {'n': 0}
        class Stub:
            def _snapshot_once(self):
                calls['n'] += 1
                if calls['n'] < 3:
                    raise RuntimeError('configured probe/service command failed: /usr/bin/python3')
                return {'ok': True}
        result = SystemAdapter.snapshot(Stub())
        self.assertEqual(result, {'ok': True})
        self.assertEqual(calls['n'], 3)
        # Persistent transient-class failure still raises after 3 tries.
        class AlwaysBad:
            def _snapshot_once(self):
                calls['n'] += 1
                raise RuntimeError('configured helper failed/cleanup unverified: /usr/bin/python3')
        before = calls['n']
        with self.assertRaisesRegex(RuntimeError, 'configured helper failed'):
            SystemAdapter.snapshot(AlwaysBad())
        self.assertEqual(calls['n'] - before, 3)

    def test_snapshot_never_retries_unknown_owner(self):
        # Unknown owners and incomplete inspection quarantine immediately,
        # single attempt, no re-sample.
        from execution_resources import SystemAdapter
        for msg in ('unknown GPU owner during job; left untouched',
                    'resource owner inspection incomplete',
                    'CONFLICT: unknown resource owner; left untouched'):
            calls = {'n': 0}
            class Stub:
                def _snapshot_once(self):
                    calls['n'] += 1
                    raise RuntimeError(msg)
            with self.assertRaises(RuntimeError):
                SystemAdapter.snapshot(Stub())
            self.assertEqual(calls['n'], 1)

    def test_observe_skips_transient_probe_tick(self):
        # Workload-reliable: one failed 1s observe tick skips and counts;
        # the next tick re-checks. Unknown owners still quarantine.
        m = self.manager()
        self.acquire(m)
        before = m.receipt.get('last_observed_at')
        m.adapter.fail_snapshot = True
        m.observe([])
        self.assertEqual(m.receipt.get('skipped_observations'), 1)
        self.assertNotEqual(m.receipt.get('last_observed_at'), before)
        self.assertEqual(m.receipt['state'], 'BUSY')
        m.adapter.fail_snapshot = False
        m.observe([])
        self.assertEqual(m.receipt['state'], 'BUSY')

    def test_observe_never_skips_unknown_owner(self):
        # Unknown owners quarantine on the observe path, never skip.
        owner = identity(222, 9, cgroup="unrelated")
        m = self.manager(FakeAdapter(owners=[owner]))
        with self.assertRaises((ValueError, RuntimeError)):
            self.acquire(m)
        self.assertEqual(m.receipt["state"], "QUARANTINED")

    def test_adapter_inherits_configured_gpu_environment(self):
        # The sanitized probe env must carry the declared GPU selection;
        # otherwise admission inspects unmasked hardware and disagrees
        # with qualification. Unsupported keys fail closed at construction.
        from execution_resources import SystemAdapter
        adapter = SystemAdapter(self.host)
        self.assertEqual(adapter.env.get("HIP_VISIBLE_DEVICES"), "0")
        self.assertEqual(adapter.env["PATH"], "/usr/local/bin:/usr/bin:/bin")
        bad = copy.deepcopy(self.host)
        bad["resources"]["gpu"]["environment"] = {"CUDA_VISIBLE_DEVICES": "0"}
        with self.assertRaisesRegex(ValueError, "unsupported GPU environment key"):
            SystemAdapter(bad)


if __name__ == "__main__":
    unittest.main()
