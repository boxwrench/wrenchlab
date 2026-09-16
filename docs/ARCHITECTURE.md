# Architecture

WrenchLab separates the question being studied from the machine that executes
it. A bounded experiment/session records objective, source, requirements,
placement, state and evidence references.

```text
experiment / research session
        ↓
worker or provider adapter
        ↓
campaign-local deterministic broker
        ↓
execution host + resource manager
        ↓
runtime and managed-service adapters
        ↓
owned process tree
        ↓
receipts, artifacts, hashes and verdict
```

The broker persists immutable requests before transport I/O. A stable job ID
makes acknowledgement loss safe: retrying the same request discovers the
existing job and rejects changed contents. The worker enforces its own deadline,
contains its process group, and reports execution and cleanup separately.

Hosts advertise capabilities and resources. A resource manager records original
state, admission, owners, release and recovery. If ownership or cleanup cannot
be proven, the affected worker/resource is quarantined. Service adapters expose
start/stop/status/health/restore semantics without making one service manager a
core architectural dependency.

Evidence is ingested through one normal packet path. Raw operational state is
not a research finding; acceptance remains a human/research policy decision.

The current implementation is intentionally small: one campaign-local broker,
one worker contract and static host configuration. Scheduling pools, automatic
placement and hostile-code isolation are future designs, not hidden features.
