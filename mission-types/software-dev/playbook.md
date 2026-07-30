# Software Development

Use this method for changes to a software product in any language or framework.
Inspect the repository before proposing work. Translate the objective into
falsifiable behavior assertions and coherent implementation tasks, then select
the repository's own validation entrypoints as structured mission-local
command oracles. The bundle deliberately names no toolchain command: a Rust,
Python, or mixed repository specializes the same method in its proposal.

The stop bar is verified. Every assertion therefore needs a command oracle
whose exit status decides the complete claim at the final commit. Independent
review may detect weakened checks, incomplete integration, and missed edges,
but it never substitutes for required command proof. Keep the team minimal,
add specialists only for concrete ownership or scrutiny needs, and revise the
complete plan when evidence exposes a gap.

Tasks own outcomes, not validation ceremonies. One artifact-producing role
owns each implementation outcome and commits it. Command oracles are structured
executable and argument data, never shell text, and are selected from the
target repository only after inspection. Preserve existing tests and
acceptance surfaces. A final gap review re-derives the objective before finish;
blocking findings require repair or revision, never a weaker assertion.
