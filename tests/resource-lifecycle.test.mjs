import test from 'node:test'
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
for (const name of ['local_resource_roundtrip', 'local_failure_timeout_cancel_restore',
  'local_controller_restart_and_lost_ack', 'resource_quarantine_allows_cpu', 'resource_evidence_tamper_rejected',
  'bounded_helper_cleans_detached_descendant', 'concrete_gpu_target_parser',
  'cross_worker_exclusive_admission', 'dead_supervisor_quarantines_resource']) {
  test(`Resource end-to-end: ${name}`, { timeout: 30000 }, () => {
    const p = spawnSync('python3', ['tests/resource_lifecycle_fixture.py', `ResourceLifecycle.test_${name}`], { encoding: 'utf8', timeout: 28000 })
    assert.equal(p.status, 0, p.stdout + p.stderr)
  })
}
