import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import { readSessionLog } from '../src/session-log.mjs'
import { listJobEvidence } from '../src/job-evidence.mjs'

const sessionHome = path.resolve(process.env.WRENCHLAB_SESSION_HOME ?? path.join(process.cwd(), '.wrenchlab'))
const sessions = path.join(sessionHome, 'sessions')

function walk(directory) {
  if (!fs.existsSync(directory)) return []
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(directory, entry.name)
    return entry.isDirectory() ? walk(full) : [full]
  })
}

const files = walk(sessions).filter(file => file.endsWith('.jsonl') || file.endsWith('.jsonl.zstd')).sort()
const sessionOutput = files.map(file => {
  const first = readSessionLog(file).split('\n').find(line => line.trim()) ?? '{}'
  let event = {}
  try { event = JSON.parse(first) } catch { /* report the file, not arbitrary text */ }
  const stat = fs.statSync(file)
  return {
    file: path.relative(sessionHome, file),
    bytes: stat.size,
    modifiedAt: stat.mtime.toISOString(),
    firstEventKeys: Object.keys(event).sort(),
    id: event.id ?? event.sessionId ?? event.session_id ?? null,
  }
})

const campaignFlag = process.argv.indexOf('--campaign')
const execution = campaignFlag >= 0 ? (() => {
  const campaign = path.resolve(process.argv[campaignFlag + 1])
  const stateFile = path.join(campaign, 'experimenter', 'state.json')
  let experimenter = null
  if (fs.existsSync(stateFile)) {
    try {
      const state = JSON.parse(fs.readFileSync(stateFile, 'utf8'))
      experimenter = {
        stateFile: path.relative(campaign, stateFile), experimentId: state.experiment_id,
        sessionId: state.session_id, state: state.state, authorHost: state.author_host,
        executorHost: state.executor_host, jobId: state.job_id,
        evidencePacket: state.evidence_packet ? path.relative(campaign, state.evidence_packet) : null,
        evidenceIntegrity: state.evidence_integrity, progress: state.progress,
        resultSummary: state.result_summary ?? null,
      }
    } catch { experimenter = { stateFile: 'experimenter/state.json', state: 'invalid' } }
  }
  return { executionEvidence: listJobEvidence(campaign), ...(experimenter ? { experimenter } : {}) }
})() : {}

process.stdout.write(JSON.stringify({
  sessionHome,
  sessionCount: sessionOutput.length,
  sessions: sessionOutput,
  ...execution,
}, null, 2) + '\n')
