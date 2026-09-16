# Managed services

Some resources are also used by a service. The service abstraction is limited to
start, stop, status, health and restore operations. A host-specific adapter may
implement those operations with systemd user units, another supervisor or a
custom API.

Before a destructive acquire, the adapter records whether the service was active,
its identity and health. It stops only configured conflicts, verifies release,
then restores the original state after the experiment. Failed health or
ambiguous ownership prevents an `AVAILABLE` verdict and leads to recovery or
quarantine.

Systemd is an adapter used by the private qualification environment, not a
WrenchLab core assumption. Public configuration contains no live service
commands, credentials or machine paths.
