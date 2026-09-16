# Reference implementation comparison

WrenchLab's execution contracts were informed by the public
[Agent Lab template](https://github.com/ciru-ai/agent-lab-template), inspected
as a reference for lifecycle semantics. No Agent Lab source is vendored here.
Its GPL-3.0-only license and notices must be reviewed before any direct code
reuse; this repository uses an independent implementation.

| Mechanism | Reference concept | WrenchLab v0 treatment | Deferred boundary |
|---|---|---|---|
| Stable submission ID | Persist request and reject changed retries | Immutable canonical request hash and stable job identity | No recovery after erased ledgers |
| Remote execution | Structured SSH transport | Local and SSH transports share one broker contract | No worker pool or failover |
| Exact source | Isolated execution input | Git bundle and detached per-job worktree | Patches/submodules remain limited |
| Deadlines | Worker-side wall-clock bound | Absolute deadline plus monotonic timeout | No dynamic budget scheduler |
| Cancellation | Explicit marker and owned process cleanup | Process-group/subreaper cleanup with verified result | Not hostile-code containment |
| Ownership | Job and process identity | PID/start ticks/boot identity and durable receipts | No multi-tenant isolation |
| Orphan recovery | Inspect before resuming | Ambiguous ownership quarantines the worker/resource | No blind force-clear |
| Atomic state | Durable controller/worker records | Atomic JSON state under campaign locks | No second database |
| Artifacts | Transfer and integrity checks | Bounded files, hashes and normal evidence ingestion | No large resumable transfer |
| Resource guards | Cooperative lock/hooks | Typed capability/resource/service adapters | No automatic placement policy |
| Long-running author | Resident author/session loop | Bounded experimenter manifest over the broker | Native author attachment is future work |

The reference influenced concepts, not the public data boundary: live machine
names, paths, service environments and qualification packets are intentionally
absent from this repository.
