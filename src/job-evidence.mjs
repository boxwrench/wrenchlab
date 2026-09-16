// Deterministic CPU receipts join campaign evidence; never impersonate research roles.
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'

const MAX_FILE = 4 * 1024 * 1024
const MAX_TOTAL = 8 * 1024 * 1024
export const hashBytes = bytes => crypto.createHash('sha256').update(bytes).digest('hex')
export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value !== null && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`
  return JSON.stringify(value)
}
function assert(ok, message) { if (!ok) throw new Error(message) }
function safe(root, relative) {
  assert(typeof relative === 'string' && relative.length <= 250 && !/[\\\x00-\x1f]/.test(relative), 'invalid evidence path')
  assert(!path.isAbsolute(relative) && relative.split('/').every(p => p && !['.', '..', '.git'].includes(p)), 'unsafe evidence path')
  let current = root
  for (const part of relative.split('/')) {
    current = path.join(current, part)
    assert(!fs.existsSync(current) || !fs.lstatSync(current).isSymbolicLink(), 'symlink in evidence path')
  }
  return current
}
function load(file) { return JSON.parse(fs.readFileSync(file, 'utf8')) }
function identity(campaign, jobDir) {
  const state = load(path.join(campaign, 'execution/state.json'))
  const request = load(path.join(jobDir, 'request.json'))
  assert(request.flow_id === state.flow_id && state.campaign_id === path.basename(campaign), 'campaign/flow ownership mismatch')
  assert(path.basename(jobDir) === request.job_id && /^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$/.test(request.job_id), 'job ownership mismatch')
  assert(state.jobs[request.job_id]?.request_hash === hashBytes(canonical(request)), 'immutable request hash mismatch')
  assert(request.worker_id === state.worker.worker_id, 'worker identity mismatch')
  if (request.schema_version === 2) assert(request.host_config_hash === hashBytes(canonical(state.worker)), 'host configuration changed')
  return request
}
function validateResult(request, r) {
  assert(r?.schema_version === request.schema_version && [1, 2].includes(r.schema_version) && r.job_id === request.job_id && r.flow_id === request.flow_id, 'result identity mismatch')
  assert(r.request_hash === hashBytes(canonical(request)), 'result request hash mismatch')
  assert(canonical(r.source) === canonical(request.source) && canonical(r.command) === canonical(request.command), 'result source/command mismatch')
  assert(r.worker?.worker_id === request.worker_id && typeof r.worker.hostname === 'string' && typeof r.worker.boot_id === 'string', 'missing worker provenance')
  assert(/^[a-f0-9]{64}$/.test(r.worker.implementation_sha256) && typeof r.worker.root === 'string', 'missing worker implementation provenance')
  assert(r.environment?.system === 'Linux' && typeof r.environment.git === 'string' && typeof r.environment.python === 'string', 'missing execution environment')
  assert(Number.isSafeInteger(r.started_at) && Number.isSafeInteger(r.ended_at) && r.started_at <= r.ended_at, 'invalid timestamps')
  assert(['success', 'failed', 'timeout', 'cancelled'].includes(r.execution), 'unknown execution outcome')
  assert(r.exit_code === null || Number.isSafeInteger(r.exit_code), 'invalid exit code')
  assert(r.execution !== 'success' || r.exit_code === 0, 'success requires exit code zero')
  assert(['verified', 'failed', 'unknown'].includes(r.cleanup), 'unknown cleanup state')
  assert(r.cleanup === 'verified', 'cleanup is not verified; receipt requires operator inspection')
  if (request.schema_version === 2) {
    assert(r.placement?.executor === request.placement.executor && r.placement.host_config_hash === request.host_config_hash, 'placement provenance mismatch')
    assert(['local', 'ssh'].includes(r.placement.transport) && Array.isArray(r.placement.capabilities), 'missing host provenance')
    assert(request.requirements.capabilities.every(c => r.placement.capabilities.includes(c)), 'capability mismatch')
    if (request.requirements.resources.gpu) {
      assert(['acquired', 'rejected'].includes(r.resource_admission), 'missing resource admission')
      assert(r.resource_admission !== 'rejected' || r.execution !== 'success', 'rejected resource cannot execute successfully')
      if (r.resource) {
        const g = r.resource
        assert(g.host_id === request.placement.executor && g.flow_id === request.flow_id && g.job_id === request.job_id && g.request_hash === r.request_hash, 'resource ownership mismatch')
        assert(canonical(g.requirements) === canonical(request.requirements) && /^gpu:[A-Za-z0-9_-]+$/.test(g.resource_id), 'resource requirements mismatch')
        const reservation = hashBytes(canonical({ host: g.host_id, resource: g.resource_id, flow: g.flow_id, job: g.job_id, request: r.request_hash }))
        assert(g.reservation_id === reservation, 'reservation identity mismatch')
        assert(['AVAILABLE', 'QUARANTINED'].includes(g.state) && ['verified', 'unknown', 'failed'].includes(g.cleanup), 'resource release not terminal')
        assert(g.state !== 'AVAILABLE' || (g.release === 'verified' && g.cleanup === 'verified' && g.process_cleanup === 'verified' && g.after), 'unsafe resource availability')
        assert(r.resource_admission !== 'acquired' || (g.acquire === 'verified' && g.before && Array.isArray(g.during)), 'missing resource receipt')
      } else assert(r.resource_admission === 'rejected', 'missing reservation')
    } else assert(r.resource === null && r.resource_admission === 'not_requested', 'unexpected resource reservation')
  }
  assert(Array.isArray(r.artifacts) && Array.isArray(r.evidence_errors), 'missing artifact manifest')
  const expectations = new Map(request.artifacts.map(a => [`artifact/${a.path}`, a]))
  const allowed = new Set([...expectations.keys(), 'log/stdout.log', 'log/stderr.log', 'log/setup.log'])
  const names = new Set()
  let bytes = 0
  for (const a of r.artifacts) {
    assert(allowed.has(a.path) && !names.has(a.path), 'unexpected or duplicate artifact')
    assert(Number.isSafeInteger(a.bytes) && a.bytes >= 0 && a.bytes <= MAX_FILE && /^[a-f0-9]{64}$/.test(a.sha256), 'invalid artifact hash/size')
    if (expectations.get(a.path)?.sha256) assert(a.sha256 === expectations.get(a.path).sha256, 'expected artifact hash mismatch')
    bytes += a.bytes; names.add(a.path)
  }
  assert(bytes <= MAX_TOTAL, 'artifact packet exceeds limit')
  const missing = [...expectations].filter(([p, a]) => a.required && !names.has(p)).map(([p]) => p)
  if (r.execution === 'success') assert(missing.length === 0 && r.evidence_errors.length === 0, 'successful job has incomplete/invalid evidence')
  return missing.length || r.evidence_errors.length ? 'incomplete' : 'complete'
}
function durableFile(file, bytes) {
  const fd = fs.openSync(file, 'wx', 0o600)
  try { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd) } finally { fs.closeSync(fd) }
}

export function ingestJob(campaignDir, jobDirectory) {
  const campaign = path.resolve(campaignDir)
  const jobDir = path.resolve(jobDirectory)
  assert(path.dirname(jobDir) === path.join(campaign, 'execution/jobs'), 'job must belong to campaign execution directory')
  const request = identity(campaign, jobDir)
  const transfer = load(path.join(jobDir, 'transfer.json'))
  const completeness = validateResult(request, transfer.result)
  assert(transfer.worker_status !== 'QUARANTINED', 'worker quarantined; do not ingest as verified evidence')
  const files = transfer.files
  assert(files && typeof files === 'object' && Object.keys(files).length === transfer.result.artifacts.length, 'transfer manifest differs')
  // Validate every byte before publishing any packet.
  const verified = transfer.result.artifacts.map(a => {
    assert(typeof files[a.path] === 'string', 'artifact payload missing')
    const bytes = Buffer.from(files[a.path], 'base64')
    assert(bytes.toString('base64') === files[a.path], 'invalid base64 transfer')
    assert(bytes.length === a.bytes && hashBytes(bytes) === a.sha256, 'artifact hash verification failed')
    safe(jobDir, a.path)
    return { a, bytes }
  })
  const packet = { schema_version: 1, kind: 'execution-evidence', integrity: 'verified', completeness, request, result: transfer.result }
  const dest = path.join(jobDir, 'evidence')
  if (fs.existsSync(dest)) {
    assert(canonical(load(path.join(dest, 'packet.json'))) === canonical(packet), 'immutable evidence packet conflict')
    verifyStored(campaign, jobDir)
    return packet
  }
  const stage = fs.mkdtempSync(path.join(jobDir, '.ingest-'))
  for (const { a, bytes } of verified) {
    const file = safe(stage, a.path)
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 })
    durableFile(file, bytes)
  }
  durableFile(path.join(stage, 'packet.json'), JSON.stringify(packet, null, 2) + '\n')
  durableFile(path.join(stage, 'README.md'), `# Execution evidence: ${request.job_id}\n\nFlow: ${request.flow_id}\n\nExecution: ${packet.result.execution}; process cleanup: ${packet.result.cleanup}; resource cleanup: ${packet.result.resource?.cleanup ?? 'not acquired'}; evidence: ${completeness}.\n\nSource: ${request.source.commit}\n\nRequest SHA-256: ${packet.result.request_hash}\n\nSee packet.json for placement, resource restoration, commands, environment and raw log/artifact hashes. Verified byte integrity does not imply successful resource restoration or an accepted engineering finding.\n`)
  // Another collector may have won publication. Never overwrite its packet.
  try { fs.renameSync(stage, dest) } catch (error) {
    if (!fs.existsSync(dest)) throw error
    assert(canonical(load(path.join(dest, 'packet.json'))) === canonical(packet), 'concurrent packet conflict')
    verifyStored(campaign, jobDir)
    fs.rmSync(stage, { recursive: true })
  }
  const fd = fs.openSync(jobDir, 'r')
  try { fs.fsyncSync(fd) } finally { fs.closeSync(fd) }
  return packet
}

function verifyStored(campaign, jobDir) {
  const request = identity(campaign, jobDir)
  const root = path.join(jobDir, 'evidence')
  assert(!fs.lstatSync(root).isSymbolicLink(), 'evidence directory is a symlink')
  const packet = load(safe(root, 'packet.json'))
  assert(packet.kind === 'execution-evidence' && packet.integrity === 'verified' && canonical(packet.request) === canonical(request), 'invalid stored packet')
  assert(packet.completeness === validateResult(request, packet.result), 'evidence completeness mismatch')
  for (const a of packet.result.artifacts) {
    const file = safe(root, a.path)
    assert(fs.statSync(file).isFile() && fs.statSync(file).size === a.bytes, 'stored artifact size mismatch')
    assert(hashBytes(fs.readFileSync(file)) === a.sha256, 'stored artifact hash mismatch')
  }
  return packet
}

export function listJobEvidence(campaignDir) {
  const campaign = path.resolve(campaignDir)
  const statePath = path.join(campaign, 'execution/state.json')
  if (!fs.existsSync(statePath)) return []
  return Object.keys(load(statePath).jobs).sort().flatMap(jobId => {
    assert(/^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$/.test(jobId), 'invalid registered job ID')
    const folder = path.join(campaign, 'execution/jobs', jobId)
    const relative = `execution/jobs/${jobId}/evidence/packet.json`
    if (!fs.existsSync(path.join(campaign, relative))) return []
    try {
      const packet = verifyStored(campaign, folder)
      return [{ jobId, flowId: packet.request.flow_id, packetFile: relative, integrity: 'verified',
                completeness: packet.completeness, execution: packet.result.execution, cleanup: packet.result.cleanup,
                ...(packet.request.schema_version === 2 ? { executor: packet.result.placement.executor,
                  transport: packet.result.placement.transport, resourceCleanup: packet.result.resource?.cleanup ?? 'not_acquired',
                  resourceState: packet.result.resource?.state ?? null } : {}),
                requestHash: packet.result.request_hash }]
    } catch (error) {
      return [{ jobId, packetFile: relative, integrity: 'invalid', error: error.message }]
    }
  })
}
