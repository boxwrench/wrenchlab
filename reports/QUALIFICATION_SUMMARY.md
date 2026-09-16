# Qualification summary

This public summary records the accepted results of the private DSH foundation
without publishing raw machine evidence, service configuration, or campaign
directories.

## Execution and resource foundation

Verdict: `EXECUTION_RESOURCE_FOUNDATION_QUALIFIED`

The deterministic execution/resource path demonstrated the following bounded
lifecycle on a real ROCm accelerator host:

```text
record managed-service state
→ reserve the required accelerator
→ execute attributable GPU work
→ handle success, timeout, cancellation and controlled failure
→ clean owned processes and resource state
→ release the reservation
→ restore the managed service
→ verify service health
```

Receipts preserve request identity, source revision, host/resource placement,
process ownership, artifact hashes, cleanup state and restoration state. Unknown
ownership fails closed and is quarantined rather than guessed safe.

## Long-running experimenter

Verdict: `LONG_RUNNING_EXPERIMENTER_QUALIFIED`

One bounded experimenter session demonstrated durable state, independent author
and executor configuration, atomic progress snapshots, normal completion,
explicit cancellation, evidence collection and cleanup. The experiment logic
requested capabilities/resources; host-specific service and runtime behavior
remained behind configuration/adapters.

The frozen private regression state was `104/104 passing` at preservation time.

## Deliberately unqualified

This summary does not claim physical workstation-to-remote-accelerator
execution. Native author-session attachment, broad multi-host scheduling and
continuous ownership monitoring remain outside the qualified v0 boundary.

Raw qualification packets remain in the private DSH preservation materials.
Only this sanitized summary is published here.
