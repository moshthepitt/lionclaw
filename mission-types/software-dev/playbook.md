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
   oracle-less assertion when the plan is proposed. A claim no command can
   check (design, readability, intent) does not belong in the contract here;
   capture it as a reviewer's remit on an oracle-bound assertion (point 3),
   never as an assertion of its own.

2. **Choose work boundaries by coherent outcome ownership.** Assertions are
   units of proof; work tasks own implementation outcomes. Exactly one active
   task owns each assertion (the engine enforces this), while one task
   may own several related assertions. Group behavior, tests, formatting,
   lint, and build assertions with the implementation outcome they constrain
   instead of creating work whose only purpose is to run a check. Available
   artifact-producing role instances receive a writable task-owned Git
   checkout and commit any changes they make.

3. **Assign an independent judgment panel.** An oracle proves a claim is
   *green*; the team's `judgment_assignments` bind one or more
   `emits-verdict` role instances that add the judgment an oracle cannot:
   "the fix is correct, not a test weakened to pass." Judgment lives in the
   team revision, never in authored validation tasks.

4. **Return one complete team revision.** Preserve role contracts that do not
   need to change, assign every task to one artifact-producing role instance,
   assign every assertion to its judgment panel, and retain the configured gap
   reviewer. Role instances carry their runtime, output semantics, skills,
   timeout, instructions, environment, and grants.

5. **Review the proposal before handing it off.** The default planning pass is
   deliberately one strategist turn. Build an explicit objective-to-contract
   coverage map, attack every assertion for vacuous proof or weakenable tests,
   and repair the complete proposal before writing the handoff. Add another
   planning perspective only when novelty, security impact, public API change,
   prior blocking findings, or a concrete coverage gap justifies it.

## Binding assertions to checks (dynamically)

The engine does not know your domain. You choose, per assertion, whether it
is proven by:

- an **oracle** (`cargo-test`, `cargo-clippy`, `fmt-check`, `build-release`) —
  an engine-run command, exit 0 = pass, the strong authoritative check; every
  contract assertion binds one; and, optionally,
- a **judgment panel** — assigned role instances layered on top of the oracle
  for depth a command cannot capture (was the change made honestly?).

Every assertion is oracle-bound; assigned judges add scrutiny and never stand
in for the oracle.

## The honesty bar

This mission type stops at **verified**: the engine declares a verified finish only
when every contract assertion has a fresh authoritative (oracle) pass at the
final commit. Because of that, the `verified` bar rejects an oracle-less
assertion when proposed — a reviewer can never stand in for an oracle here; it only
adds depth on top of one. Do not weaken a test to make an oracle pass; the
reviewer is instructed to catch exactly that.

## Handoff evidence

Every completed role handoff carries a bounded narrative report. LionClaw binds
that report to the exact role effect in `RoleTurnCompleted`. The report remains
visible with accepted work and gap-review outcomes. Missing, malformed, or
mismatched handoffs never gain report or verdict authority; their typed failure
evidence remains visible instead.

## The gap review

After every work task settles and every oracle verdict is fresh at the final
commit, the engine dispatches the team's assigned contract-blind **gap
reviewer**. It sees only the objective and final tree, and re-derives
the requirements from scratch. It exists to catch what the contract never
asserted — the oracle proves what you asserted; the reviewer hunts what you
missed.

Do not add a final catch-all review task to the plan, and do not write
assertions "for the reviewer" — plan the contract on its own
merits. If the gap review reports blocking gaps, revise the plan. A
revision is a complete next plan, not a patch language: retain the still-valid
requirements and assertions, add any missing falsifiable assertions and their
oracle bindings, retire completed work, and add the repair work. Remediation
automatically re-runs the review at the new commit.
