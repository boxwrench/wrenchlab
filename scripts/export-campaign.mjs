import fs from 'node:fs'
import path from 'node:path'
import { listJobEvidence } from '../src/job-evidence.mjs'

// Public, provider-neutral campaign export.  Native model/session logs remain
// an integration concern; deterministic execution evidence is exported here
// so callers can inspect one authoritative campaign view without another store.
export function exportCampaign({ campaignDir, startedMs = 0, endedMs = Date.now(), status = 0 }) {
  const campaign = path.resolve(campaignDir)
  const executionEvidence = listJobEvidence(campaign)
  const evidenceLines = executionEvidence.map(receipt => JSON.stringify({
    type: 'execution-evidence',
    ...receipt,
  }))
  fs.writeFileSync(path.join(campaign, 'evidence.jsonl'), `${evidenceLines.join('\n')}${evidenceLines.length ? '\n' : ''}`)

  const manifest = {
    schemaVersion: 1,
    campaignId: path.basename(campaign),
    startedAt: new Date(Number(startedMs)).toISOString(),
    endedAt: new Date(Number(endedMs)).toISOString(),
    status: Number(status),
    validatedWorkerResultCount: 0,
    executionEvidence,
  }
  fs.writeFileSync(path.join(campaign, 'campaign.json'), `${JSON.stringify(manifest, null, 2)}\n`)
  fs.writeFileSync(path.join(campaign, 'worker-results.json'), '[]\n')
  return manifest
}
