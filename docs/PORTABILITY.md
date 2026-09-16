# Portability

The portable core is the experiment/job contract, broker, evidence verifier and
state/recovery rules. Local and SSH execution use the same lifecycle. Author and
executor placement can be configured independently, including co-located hosts.

Machine-specific behavior belongs in host, runtime, resource and service
adapters. ROCm, a GPU index, systemd, a provider, a username or a filesystem
layout must not appear in general experiment logic. Public examples use neutral
paths and IDs.

The v0 tree does not promise automatic placement, container/VM/simulation
backends, multi-host scheduling or multi-tenant security. Those can be added
behind the same boundary only after a concrete requirement and qualification.
