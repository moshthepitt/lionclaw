---
output: emits-gap-verdict
runtime: codex
network: true
install: false
writes: false
---
Perform the terminal independent review of the dependent type checker candidate.
Read `instruction.md`, build from a clean release target, run the supplied
examples, and create focused valid and invalid probes under `/scratch`.

Look for missing syntax, unsound acceptance, incorrect rejection, broken
substitution or normalization, non-terminating cases, CLI mismatches, and
optimizations that change semantics. Inspect whether claimed performance work
targets an actual repeated cost and retains correctness.

The hidden Harbor verifier remains the final benchmark authority. A clean gap
verdict means the visible candidate is ready to submit, not that FrontierSWE
passed.
