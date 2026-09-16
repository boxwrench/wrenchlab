import test from 'node:test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
const fixture = fileURLToPath(new URL('./execution_fixture.py', import.meta.url))
const cases = [
  'A_lost_acknowledgement', 'B_controller_restart', 'C_command_failure',
  'D_timeout_tree', 'E_cancellation_tree', 'F_corrupted_transfer',
  'G_conflicting_retry', 'H_orphan_quarantine', 'concurrent_duplicate_submission',
  'detached_descendant_cleanup', 'path_traversal_and_symlink',
  'exact_source_and_idempotent_ingestion', 'limits_and_immutable_flow',
  'worker_busy_and_expired_admission', 'recovery_of_queued_admission',
]
for (const name of cases) {
  test(`CPU execution: ${name}`, { timeout: 25000 }, () => {
    try {
      execFileSync('python3', [fixture, `ExecutionTests.test_${name}`], { timeout: 23000, encoding: 'utf8', stdio: 'pipe' })
    } catch (error) {
      throw new Error(`${name}\n${error.stdout ?? ''}\n${error.stderr ?? ''}`, { cause: error })
    }
  })
}
