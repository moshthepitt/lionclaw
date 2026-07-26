---
output: produces-artifact
runtime: codex
network: true
install: false
writes: true
---
Implement the assigned dependent type checker work in `/workspace/type-checker`.
The task's `/app` path is mapped to `/workspace`; read the full contract in
`/workspace/instruction.md`.

Use only the Rust standard library and the files already in the workspace.
Your runtime's network grant exists for model transport; the task forbids using
general internet access, and dependency installation is disabled. Preserve the
command-line and exit-code contract, run focused probes against the supplied
examples, and keep correctness inspectable while optimizing measured hot paths.

Do not refer to or search for Harbor verifier paths, hidden corpora, reference
implementations, or reward files. Those are outside your authority and are not
present in the mission container. Commit the candidate and report the exact
commands and observations that support your handoff.
