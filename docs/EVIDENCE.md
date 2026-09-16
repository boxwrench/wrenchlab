# Evidence and provenance

Every collected job has one immutable evidence packet containing:

* request and source revision hashes;
* command, host, transport and relevant capability provenance;
* start/end timestamps and execution outcome;
* resource reservation/acquire/release/cleanup receipt;
* bounded logs and artifact manifest with SHA-256 hashes;
* progress/final state and errors.

The broker verifies exact bytes before ingestion. A valid packet proves what the
executor reported and what bytes were returned; it does not by itself accept a
research claim. Experiment outcome and cleanup/resource outcome remain separate.

Long-running manifests point to this normal packet and retain the latest bounded
progress snapshot, terminal state and artifact hashes. Raw environments,
credentials and private machine evidence are not public evidence.
