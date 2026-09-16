# How one experiment works

1. An author creates a bounded experiment with an objective, exact source
   revision, executor requirements and independent author/executor placement.
2. The broker validates the host capabilities and writes an immutable request
   and stable job ID before contacting the worker.
3. The worker receives the source bundle, creates a detached worktree, and
   records `QUEUED` then `RUNNING` ownership with a deadline.
4. A workload writes a small atomic progress snapshot. The author can call
   `inspect`/`status` without opening the executor manually.
5. The worker terminates only its owned process tree on completion, timeout or
   cancellation. Resource and managed-service adapters release or restore state
   and independently verify cleanup.
6. The broker collects bounded logs/artifacts, verifies bytes and hashes, and
   ingests one immutable evidence packet.
7. The session manifest records the terminal verdict, timestamps, progress,
   executor identity, resource receipt, artifact hashes and evidence path.

The same lifecycle works with local or SSH transport. Transport is a placement
choice, not a second experiment model.
