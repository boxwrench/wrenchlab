# WrenchLab v0 bootstrap classification

Source reviewed: private frozen DSH foundation at
`9e6e30795692c6684062aa36f96c411ad84b0783` (`pre-wrenchlab-foundation`). The
source history and bundle remain private and were not copied into this Git
repository.

## PUBLIC_SAFE

* Deterministic protocol, host, broker, worker, resource and evidence modules.
* Sanitized unit/lifecycle fixtures and public-safe schemas.
* Generic experimenter session/progress implementation.
* Architecture, operating, recovery, portability and provenance documentation.

## SANITIZE

* Host/resource examples: replaced live names, paths and probes with neutral
  `workstation`, `worker-01`, `/home/user/...` and generic capabilities.
* Agent Lab comparison: retained concepts and public license reference, removed
  private DSH/machine/service details.
* Qualification history: reduced to human-readable summaries and pointers; no
  raw process environments or campaign blobs.
* Tests: changed synthetic service/device labels to generic fixture values.

## LOCAL_ONLY

* Real `.env` files, provider/service environment files, live service configuration, raw campaign
  directories, private evidence packets, host mappings, process/cgroup dumps and
  credentials.
* The private DSH Git history and its preservation tag.

## DISCARD

* Provider-specific routing overlays, model prompts, private runtime tuning,
  unrelated model worktrees, dashboards and speculative scheduler/agent designs.

No private source material was copied solely for completeness. See `SECURITY.md`
and `.gitignore` for the publication boundary.
