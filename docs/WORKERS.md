# Workers and providers

A worker/provider is the replaceable component that reasons, proposes work or
interprets results. The deterministic broker does not ask a model to decide job
identity, retries, deadlines, process ownership, hashing or quarantine.

Use a provider-neutral boundary, for example:

```yaml
worker:
  capability: bounded_synthesis
  provider: example-provider
```

Provider credentials belong in the local environment and are never represented
in public examples or evidence. Local models, hosted APIs and future agent
workers can implement this boundary without changing execution contracts.
The public v0 tree does not include a provider marketplace or metered API
integration.
