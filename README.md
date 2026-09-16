# WrenchLab

WrenchLab is a portable, model-agnostic research and experiment execution
system for coordinating workers, compute resources, long-running experiments
and reproducible evidence.

This v0 foundation extracts the deterministic execution behavior proven in an
internal prototype. It provides stable job identity, exact source inputs,
local/SSH transport, deadlines, cancellation, process ownership, resource
receipts, artifact hashes and bounded experiment-session progress. It is a
foundation, not a general scheduler, sandbox or autonomous engineering author.

## Quick start

Requirements: Node.js 22+ and Python 3. Run the deterministic suite:

```text
npm test
```

The default suite uses temporary local fixtures; it does not require GPU
hardware, provider credentials or metered APIs. Public-safe placement examples
are in `config/examples/`.

## How it fits together

```text
experiment/session
        ↓
worker/provider boundary
        ↓
deterministic broker
        ↓
host and resource ownership
        ↓
runtime/service adapters
        ↓
execution and verified evidence
```

An author host and executor host are independent configuration fields. Providers
such as local models or hosted APIs are replaceable inputs to future worker
adapters; no provider is required by the execution core.

## Maturity and limits

The deterministic contracts and fixtures are public. Hardware qualification and
long-running lifecycle behavior were proven privately before this bootstrap and
are summarized without live machine evidence. Physical cross-host qualification,
automatic scheduling, native author-session attachment, multi-tenant isolation
and broader environment backends are intentionally not claimed.

Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and
[`docs/HOW_IT_WORKS.md`](docs/HOW_IT_WORKS.md) before extending the system.
The public bootstrap classification is in
[`reports/BOOTSTRAP.md`](reports/BOOTSTRAP.md), with accepted private-foundation
results summarized in [`reports/QUALIFICATION_SUMMARY.md`](reports/QUALIFICATION_SUMMARY.md).
