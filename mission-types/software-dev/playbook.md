# Software-dev playbook

This is the method for a software-development mission. The orchestrator (the
host harness driving the engine) uses it to author a plan: a **contract of
falsifiable assertions** plus a **task DAG** that produces the change and
proves it.

## The shape of a good plan

1. **State the contract first.** Each assertion is a falsifiable claim about
   the finished code, with an uppercase id (`^[A-Z][A-Z0-9-]+$`) and prose the
   reviewer and oracles can judge against. This mission type stops at **verified**, so
   **every** contract assertion must bind an **oracle** — the engine rejects an
   oracle-less assertion at submit. A claim no command can check (design,
   readability, intent) does not belong in the contract here; capture it as a
   reviewer's remit on an oracle-bound assertion (point 3), never as an
   assertion of its own.

2. **One work task per assertion.** Exactly one active `work` task must cover
   each assertion (the engine enforces this). A work task is dispatched to
   the `implementer`, runs in a writable clone, and commits its result.

3. **Add validators for depth on top of the oracle.** An oracle proves a claim
   is *green*; a `validate` task dispatched to the `reviewer` adds the judgement
   an oracle can't — "the fix is correct, not a test weakened to pass." A
   validator targets an assertion that already binds an oracle; its verdict is
   **advisory** — it routes and ranks, it never marks the mission verified.

4. **Add a gate to aggregate advisory verdicts.** A `gate` task depending on
   the validators AND-aggregates their verdicts over its targets. A cleared
   gate still pauses for a human checkpoint; a blocked gate raises attention.

## Binding assertions to checks (dynamically)

The engine does not know your domain. You choose, per assertion, whether it
is proven by:

- an **oracle** (`cargo-test`, `cargo-clippy`, `fmt-check`, `build-release`) —
  an engine-run command, exit 0 = pass, the strong authoritative check; every
  contract assertion binds one; and, optionally,
- a **reviewer** — an agent's advisory judgement layered *on top* of the
  oracle, for depth a command can't capture (was the change made honestly?).

Every assertion is oracle-bound; a reviewer only adds scrutiny, never stands in
for the oracle.

## The honesty bar

This mission type stops at **verified**: the engine declares a verified finish only
when every contract assertion has a fresh authoritative (oracle) pass at the
final commit. Because of that, the `verified` bar rejects an oracle-less
assertion at submit — a reviewer can never stand in for an oracle here; it only
adds depth on top of one. Do not weaken a test to make an oracle pass; the
reviewer is instructed to catch exactly that.
