# Software-dev playbook

This is the method for a software-development mission. The orchestrator (the
host harness driving the engine) uses it to author a plan: a **contract of
falsifiable assertions** plus a **task DAG** that produces the change and
proves it.

## The shape of a good plan

1. **State the contract first.** Each assertion is a falsifiable claim about
   the finished code, with an uppercase id (`^[A-Z][A-Z0-9-]+$`) and prose
   the reviewer and oracles can judge against. Bind every assertion that a
   command can check to an **oracle** — that is what lets the mission finish
   *verified* rather than merely *reviewed*. Assertions no command can check
   (design, readability, intent) are left oracle-less and covered by a
   reviewer instead.

2. **One work task per assertion.** Exactly one active `work` task must cover
   each assertion (the engine enforces this). A work task is dispatched to
   the `implementer`, runs in a writable clone, and commits its result.

3. **Add validators where judgement is needed.** For assertions an oracle
   cannot fully capture, add a `validate` task targeting them, dispatched to
   the `reviewer`. Validator verdicts are **advisory**: they route and rank,
   they never mark the mission verified.

4. **Add a gate to aggregate advisory verdicts.** A `gate` task depending on
   the validators AND-aggregates their verdicts over its targets. A cleared
   gate still pauses for a human checkpoint; a blocked gate raises attention.

## Binding assertions to checks (dynamically)

The engine does not know your domain. You choose, per assertion, whether it
is proven by:

- an **oracle** (`cargo-test`, `cargo-clippy`, `fmt-check`, `build-release`) —
  an engine-run command, exit 0 = pass, the strong authoritative check; or
- a **reviewer** — an agent's advisory judgement, for claims no command
  captures.

Prefer oracles. Reach for a reviewer only for the genuinely un-automatable.

## The honesty bar

This plugin stops at **verified**: the engine declares a verified finish only
when every contract assertion has a fresh authoritative (oracle) pass at the
final commit. A plan with only reviewers, or with reviewers standing in for
checkable claims, finishes *internally consistent* at best — never verified.
Do not weaken a test to make an oracle pass; the reviewer is instructed to
catch exactly that.
