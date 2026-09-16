import test from 'node:test'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const fixture = fileURLToPath(new URL('./experimenter_fixture.py', import.meta.url))
for (const name of ['independent_hosts_progress_and_evidence', 'one_interruption_is_authoritative_and_partial']) {
  test(`long-running experimenter: ${name}`, { timeout: 60000 }, () => {
    try {
      execFileSync('python3', [fixture, `ExperimenterTests.test_${name}`], {
        timeout: 55000, encoding: 'utf8', stdio: 'pipe'
      })
    } catch (error) {
      throw new Error(`${name}\n${error.stdout ?? ''}\n${error.stderr ?? ''}`, { cause: error })
    }
  })
}
