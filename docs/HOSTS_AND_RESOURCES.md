# Hosts and resources

A host is an execution location; a resource is a scarce capability requiring
ownership. Host identity and transport are separate from capabilities. A job
declares requirements and may optionally constrain an executor host.

```yaml
author_host: workstation
executor_host: accelerator-worker
requirements:
  capabilities: [gpu]
  resources:
    gpu: {exclusive: true}
```

Static configuration is sufficient for v0. The resource manager persists a
reservation ID, owner (`flow_id`, `job_id`, `host_id`, `resource_id`), original
state, during/after observations, release result and recovery state. Exclusive
admission rejects a conflicting governed request. Unknown owners are not killed;
ambiguous state is quarantined.

Public examples use neutral IDs. Live host mappings and raw probes belong in
ignored local configuration.
