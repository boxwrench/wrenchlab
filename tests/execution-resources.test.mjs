import test from 'node:test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const fixture = fileURLToPath(new URL('./resource_fixture.py', import.meta.url))
const cases = [
  'host_capabilities_and_requirements',
  'reservation_identity_and_exclusive_conflict',
  'unknown_owner_is_untouched',
  'initially_off_service_stays_off',
  'partial_acquire_failure_restores_prior_active',
  'release_after_failure_timeout_and_cancel',
  'failed_health_restoration_quarantines',
  'release_start_failure_quarantines_then_recovers',
  'safe_reset_after_healthy_reconciliation',
  'restart_active_runner_remains_busy',
  'orphan_without_cleanup_cannot_reset',
  'old_manager_cannot_release_newer_reservation',
  'malformed_requirements_rejected',
  'probe_failure_quarantines_before_mutation',
]

for (const name of cases) {
  test(`resource lifecycle: ${name}`, { timeout: 15000 }, () => {
    try {
      execFileSync('python3', [fixture, `ResourceTests.test_${name}`], {
        timeout: 12000, encoding: 'utf8', stdio: 'pipe'
      })
    } catch (error) {
      throw new Error(`${name}\n${error.stdout ?? ''}\n${error.stderr ?? ''}`, { cause: error })
    }
  })
}
