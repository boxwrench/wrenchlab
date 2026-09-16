# Bounded long-running experimenter

`scripts/run-experimenter.py` is a thin durable session/controller over the existing
`execution_broker.Broker`. It does not submit work outside the broker and does not
introduce a second job ledger. A future native author session can attach its
session ID to this manifest.

## Placement

The experiment manifest stores `author_host` separately from `executor_host`. The
author host is a small placement identity (`host_id`, `transport`, optional
capabilities); executor details are the validated v2 host contract in the broker
flow. The experiment asks for capabilities/resources and a runtime name. The
executor host maps that runtime name to a configured launcher. Experiment logic
contains no systemd, provider, username, filesystem, or device assumptions.

## Lifecycle and observation

The durable manifest is `campaign/experimenter/state.json` and is updated under the
existing execution lock. Its states are `CREATED → READY → QUEUED → RUNNING →
SUCCEEDED|FAILED|CANCELLED|TIMED_OUT`; every transition has an epoch-millisecond
timestamp and note. `status`/`watch` call Broker `inspect`, which reads a bounded
atomic `progress.json` snapshot from the executor and persists the latest progress,
executor identity, worker status and resource receipt.

The worker accepts an optional relative `progress.path`, reads at most 32 KiB, and
never treats malformed progress as execution truth. The experiment workload writes
progress atomically and writes a result artifact only on success. Cancellation
therefore leaves a clearly marked partial artifact set.

## Evidence

`collect` uses the existing `ingest-job.mjs` path. The manifest points at the
normal verified packet under `execution/jobs/<job_id>/evidence/packet.json` and
copies its artifact hashes and result summary. Source commit, request hash,
placement/capabilities, resource reservation/release receipt, process provenance,
timestamps and cleanup remain in that packet.

## Commands

```text
run-experimenter.py --campaign C init ...
run-experimenter.py --campaign C start
run-experimenter.py --campaign C status
run-experimenter.py --campaign C watch --wait-seconds 300
run-experimenter.py --campaign C cancel --wait-seconds 30
```

The controller may exit after `start`; the worker and its durable ledger continue.
Re-running `status` or `watch` reconciles the same stable job ID. No retry creates a
new job.
