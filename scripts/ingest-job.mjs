import { ingestJob } from '../src/job-evidence.mjs'
try {
  if (process.argv.length !== 4) throw new Error('usage: node scripts/ingest-job.mjs CAMPAIGN JOB_DIRECTORY')
  const packet = ingestJob(process.argv[2], process.argv[3])
  console.log(JSON.stringify({ jobId: packet.request.job_id, integrity: packet.integrity, completeness: packet.completeness }))
} catch (error) {
  console.error(`Execution evidence rejected: ${error.message}`)
  process.exitCode = 1
}
