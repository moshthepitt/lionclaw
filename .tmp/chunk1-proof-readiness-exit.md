# Chunk 1 Proof Readiness Exit Note

Worktree: `.worktrees/lionclaw2-a1-runtime-chunk-1`

Branch: `lionclaw2-a1-runtime-chunk-1`

Accepted base: `33ee8a47826cf269af61a7d5051e2724f1385e11`

## Commits

- `3259fc477614dc4cabcfa2ccc468355687149fc3`
  `Unify required proof failure handling`
  - GPG good signature from `Kelvin Jayanoris <kelvin@jayanoris.com>`.
  - Added one derived proof-readiness calculation, one command/judgment
    proof-failure recovery path, immutable authoritative receipt storage,
    receipt-derived manual retry suppression, and fail-closed decision guards.
- `1ce9e8ca86884be00494924b2045b0d5a0abcc74`
  `Close Chunk 1 review gaps`
  - GPG good signature from `Kelvin Jayanoris <kelvin@jayanoris.com>`.
  - Makes proof-failure `Revise` atomically carry every current failure into
    planning, corrects the shipped operator skill, and adds this exit note.
- `d49ada653613c98df74b2084594f984ad5a6a801`
  `Preserve mixed revision evidence`
  - Preserves existing failure refinement across sequential mixed-kind
    revisions, corrects the coverage reconciliation command, and removes one
    stale symbol reference.
- `cabda894de61df95272ddb6ca831e5f36c069598`
  `Retain oracle runtime failure evidence`
  - Carries the exact typed oracle runtime failure through attention and into
    replanning instead of retaining only a generic parked-oracle summary.
- Final RoboRev coverage commit:
  - Proves that parked oracle runtime failure cannot be accepted through the
    engine API or forged away through direct event replay.
  - The commit carrying this file is GPG-signed. Its hash cannot be embedded
    here without rewriting history; verify with `git log --show-signature -1`.

## RED and GREEN

Original Slice 11 defect evidence was captured on
`bc00391af80c17c83ab6ded3136d70815b5fe5ab`: failed required judgment could
reach below-bar auto-finish or lacked a direct generic repair/replan path. The
accepted base already fixed the finish defect, so Chunk 1 did not fake a RED by
reverting that protection.

Chunk 1 characterization initially failed because `Accept` remained legal for
a failed required command verdict:

```text
accept must be rejected for failed required proof
```

The review regression
`revising_one_of_multiple_proof_failures_replans_with_every_receipt` was then
run against `3259fc47` and failed:

```text
left: AttentionNeeded
right: Planning
```

The system QA regression
`changed_judgment_evidence_offers_a_new_retry` also failed before its fix:

```text
left: [Repair, Revise]
right: [Retry, Repair, Revise]
```

This showed that judgment anti-thrash compared verdict items but omitted the
exact accepted report, unlike command proof's exact stdout/stderr comparison.

The branch-wide review regression
`mixed_failures_are_fail_closed_and_preserve_every_feedback` failed before its
evidence-preservation fix:

```text
left: 1
right: 2
```

One drained oracle batch had produced a failed command verdict and a separate
oracle runtime failure. Revising both attention items sequentially replaced the
first refinement instead of preserving both.

RoboRev then found that the mixed regression expected
`DecisionEvidence::None` for the oracle runtime failure. The strengthened
regression failed before the evidence fix:

```text
left: None
right: OracleRuntimeFailure { failure: PermanentRuntime { ... } }
```

After the fix, that same regression requires the exact typed failure and its
rendered category, code, and detail after `oracle_failures` has been cleared.

After the fix, one `Revise` on either of two failed command proofs:

- enters `MissionPhase::Planning` immediately;
- clears every failed current proof pointer while preserving both immutable
  receipts;
- stores two exact `FailureFeedback` records in deterministic attention order;
- renders both oracle receipts into the next planner input.

## Result

- One `ProofReadiness` function owns pending, failed, and satisfied proof truth.
- One `ProofFailed` attention kind replaces command-verdict and stop-bar
  failure kinds.
- `Retry` clears only the selected failed current receipt.
- `Repair` clears the selected failure, reopens owning and downstream work, and
  attaches its exact receipt evidence.
- `Revise` is mission-level: it atomically clears all current proof failures
  and passes every exact failure record through the existing complete-plan
  refinement slot.
- Every later failure-led `Revise` appends its exact feedback to that ordered
  refinement, so mixed proof, oracle-runtime, node, gate, or review failures
  cannot erase evidence accepted by an earlier revision.
- Oracle runtime failure feedback retains its typed category, code, detail,
  diagnostics, and applied runtime configuration after the failure map clears.
- Historical command and judgment receipts remain append-only.
- A first failure may expose manual `Retry`; an identical repeat suppresses it
  without a counter. Changed command diagnostics, judgment evidence, or proof
  identity re-offer it. `Repair`, `Revise`, and universal `Abort` remain.
- Required proof failure never exposes `Accept`, and forged decisions or
  `MissionFinished` events remain inert.
- The shipped LionClaw skill now distinguishes forbidden required-proof
  acceptance from separately delegated terminal-review acceptance.

## Versions and Contracts

- `SCHEMA_VERSION`: remains `33`.
  - No event-log payload changed.
- `REDUCER_VERSION`: `61 -> 65`.
  - `62` introduced receipt-ledger proof derivation and unified recovery.
  - `63` makes proof-driven replanning consume every current failure and changes
    the folded planning refinement from one feedback record to an ordered list.
  - `64` preserves that ordered list across sequential mixed-kind revisions.
  - `65` attaches exact typed oracle runtime evidence to its folded attention.
- Public folded model changes:
  - `AssertionState` stores a current authoritative receipt ID rather than a
    copied verdict.
  - `MissionState::authoritative_receipts` stores the immutable verdict ledger.
  - `ProofFailed` replaces `OracleVerdictFailed` and `ProofBarUnmet`.
  - `DecisionEvidence::AuthoritativeReceipts` replaces copied oracle-failure
    evidence.
  - `PlanningRefinement::FailureEvidence` carries an ordered list.
- CLI/report JSON changes:
  - removed required-proof waiver fields;
  - renamed proof-failure attention IDs and kinds;
  - authoritative evidence includes receipt identity and provenance;
  - planning failure refinement exposes `failures` as an ordered array.
- No compatibility layer was added. This is the pre-launch replacement line;
  old snapshots fail over to replay through the reducer-version boundary.

## Security Impact

- Determinism wall: unchanged. `lionclaw-model` gained no dependency and still
  performs no I/O, clock, RNG, async, or runtime calls.
- Honesty moat: strengthened. Failed required proof cannot be accepted, all
  finish authority still flows through `ready_to_finish`, and the fold
  revalidates legal decisions against current derived state.
- Receipt provenance: strengthened. Current assertion pointers resolve through
  immutable command and role receipt ledgers; recovery never deletes history.
- Secrets, egress, capability ceilings, confinement, runtime authentication,
  and writable-role authority: unchanged.

## Coverage Parity

Required reconciliation command:

```text
git diff --name-status -M 33ee8a47 -- \
  ':(glob)crates/*/tests/**' ':(glob)crates/*/src/**/*tests*.rs'
```

Output:

```text
M crates/lionclaw-model/tests/team_cutover.rs
M crates/lionclaw/tests/advisory_validator.rs
M crates/lionclaw/tests/environment_assignment.rs
M crates/lionclaw/tests/happy_path.rs
M crates/lionclaw/tests/operator_skill.rs
M crates/lionclaw/tests/parallel_writers.rs
M crates/lionclaw/tests/phase0_liveness.rs
M crates/lionclaw/tests/production_conversation_flow.rs
M crates/lionclaw/tests/recovery.rs
M crates/lionclaw/tests/reference_expansion.rs
M crates/lionclaw/tests/replanning.rs
M crates/lionclaw/tests/resume.rs
M crates/lionclaw/tests/terminal_review.rs
```

Deletion reconciliation command:

```text
git diff --name-status --diff-filter=D -M 33ee8a47 -- \
  ':(glob)crates/*/tests/**' ':(glob)crates/*/src/**/*tests*.rs'
```

Output: empty. No test file or prior coverage was deleted.

## Verification

Focused QA passed:

- complete `lionclaw-model` test suite;
- complete `happy_path`, `operator_skill`, `phase0_liveness`, and
  `terminal_review` integration suites;
- the two-failure exact-evidence replanning regression.
- judgment anti-thrash regressions for identical, changed-evidence, and
  changed-identity outcomes.

Full QA passed from the repository root after the final code change:

- `cargo fmt -- --check`
- `cargo check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test`
  - all runnable tests passed;
  - the two existing real-auth/network continuity tests remained explicitly
    ignored.
- `cargo test --workspace --all-features`
  - all runnable feature-enabled tests passed with the same two explicit
    real-auth/network ignores.
- `bash ./scripts/ci.sh`
  - all eight Podman self-tests passed.
- `cargo run -p lionclaw -- mission self-test`
  - all eight checks passed.
- `git diff --check`

The pre-launch branch has no top-level `README.md` or `docs/` tree. Model,
reducer, CLI, evidence, shipped-skill, and regression contracts were updated at
their code sources of truth.

## Review and Scope

The first external review reported:

1. multiple proof failures could overwrite replanning evidence;
2. the shipped skill taught obsolete below-proof-bar acceptance language;
3. no Chunk 1 exit record documented RED/GREEN, API, or security impact.

The branch-wide rereview then reported:

1. sequential revisions across mixed failure kinds could still overwrite
   earlier replanning evidence;
2. the exit-note coverage pathspec returned an empty inventory;
3. one model comment still named the removed `classify_finish` helper.

The first full-range RoboRev pass then reported that oracle runtime attention
still carried `DecisionEvidence::None`, losing its typed payload when a
revision cleared `oracle_failures`.

The second full-range RoboRev pass reported that the same `OracleFailed`
honesty boundary lacked fault-injection coverage for API rejection and inert
folding of a forged `Accept` event.

All eight findings are addressed by forward-only commits.

No Chunk 2 `Next` work, benchmark-specific behavior, remediation budget,
durable retry counter, new automatic retry, mission-type logic, confinement
change, push, merge, or PR is included.
