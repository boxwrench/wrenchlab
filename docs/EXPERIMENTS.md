# Experiments

An experiment is a bounded question or deterministic task, not an unrestricted
agent instruction. Define:

* objective and owner;
* exact source revision and immutable request identity;
* command/runtime and expected artifacts;
* capability/resource requirements and optional host constraint;
* timeout/deadline and success criterion;
* progress format and evidence expectations.

Keep experiment logic portable. Ask for capabilities such as `gpu` or `rocm`,
not a username, device index or service name. Reruns use a new experiment/job ID
when the question or inputs differ.

The default tests use safe temporary CPU fixtures. Hardware qualification should
be a separately authorized, clearly labelled operation and must not be required
for the public regression suite.
