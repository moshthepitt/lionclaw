# FrontierSWE Dependent Type Checker

This mission type is for the pinned FrontierSWE dependent-type-checker task.
The Harbor task instruction is present at `instruction.md`; Harbor's `/app`
path maps to `/workspace` for every role. The required deliverable is therefore
`/workspace/type-checker`.

The official Harbor verifier is deliberately unavailable during the mission.
It is uploaded and run only after LionClaw returns the candidate to Harbor.
Plans must classify the implementation and performance requirements as
reviewer-checkable. Do not invent an oracle binding or claim that local examples
stand in for the hidden correctness and throughput corpus.

The task forbids network access and external crates. Keep the implementation
within the Rust standard library, use the supplied examples for local probes,
and preserve the scaffold's CLI contract. A useful plan separates semantic
correctness from profiling and optimization so that performance work does not
erase inspectable correctness.

Role network authority exists only because the confined agent CLI must reach
its model API. The current kernel cannot express the official benchmark's
API-domain-only allowlist, so general network use remains forbidden by the task
contract and makes this bring-up non-publishable. Installation authority is
disabled.

Close at the `attested` bar only after independent judges and the terminal gap
review find no blocking gap in the candidate. That attestation is a LionClaw
workflow outcome, not a benchmark pass. Harbor's post-agent verifier is the
only benchmark outcome authority.
