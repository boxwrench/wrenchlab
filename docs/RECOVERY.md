# Recovery

Recovery is deterministic and independent of a reasoning model.

* **Success/failure:** record the exit outcome, preserve available logs/artifacts,
  and verify cleanup.
* **Timeout/cancellation:** terminate the owned process group and descendants;
  distinguish timeout, cancellation and ordinary failure.
* **Lost acknowledgement:** retry the same immutable job ID; never create a
  second execution for the same request.
* **Controller restart:** reload durable flow/job state and inspect the worker;
  do not infer safety from a missing local process.
* **Stale or unknown ownership:** do not kill an unproven process. Mark the
  resource/worker `RECOVERING` or `QUARANTINED` and block new use.
* **Service restoration failure:** do not declare the resource available until
  health and ownership are verified.

Reset is a reconciliation operation requiring proof of no stale owned process,
understood service state and no unresolved receipt. There is no normal blind
force-clear path.
