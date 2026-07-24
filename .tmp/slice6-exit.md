# Slice 6 Exit

Branch: `lionclaw2-a1-runtime-slice-6`
Base: `8d7855f8` (`lionclaw2`, signed Slice 5 head)
Validated product/code head: `d81d91ce0baa16b2856ddf952469bb950064790b`
Exit note: committed as a signed forward-only bookkeeping commit on top.

## Commits

- `b925029a` `Implement Slice 6 intent-preserving revision`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 25`: mission-finish/result-applied/delegation event shapes and proof-disposition plan vocabulary.
  - `REDUCER_VERSION 50`: event-driven finish replay, attested stop bars, intent-preserving revisions, host obligations, and proof-disposition scheduling.
- `d81d91ce` `Fix Slice 6 review findings`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema/reducer bump: fixes prompt/report/oracle/default creation behavior without changing replay interpretation.

## What Landed

- Replaced strengthen-only proof immortality with intent-preserving plan revision:
  explicit `requirement_changes`, assertion supersessions, exact approval matching,
  stale prior receipts, task-retirement handling, and named dogfood casualty regressions.
- Added proof dispositions:
  `confined_provable`, `reviewer_checkable`, `host_acceptance`, and `limitation`.
  Host-only proof is reported as typed host acceptance obligation and is not assigned
  as confined work.
- Added the `attested` stop bar and `FinishClass::Attested`; removed the old
  product-facing `reviewed` stop vocabulary.
- Made closure/apply event-driven with `MissionFinished` and `ResultApplied`.
  `mission finish` checks `ready_to_finish`; abort remains unconditional for every
  nonterminal mission and does not destroy retained work.
- Recorded a delegation set on mission creation. Ordinary `mission start` records
  no delegated authorities; command legality remains predicate/optimistic-head
  based, not delegation-gated.
- Added `mission-types/metric-driven`, including an attested metric mission type
  and `metric-scalar` oracle. Review fix tightened the oracle to prove a pinned
  condition from `metric.txt`, `metric.expected`, and `metric.operator`.
- Ported `crates/lionclaw/src/runner/real_runtime_continuity_tests.rs`.
  Both live-runtime continuity tests compile and remain ignored unless credentials,
  preserved runtime roots, network, and OCI image are explicitly provided.

## Review Pass

One RoboRev branch review pass was run:

- command: `roborev review --branch --base lionclaw2 --wait`
- job: `1457`
- result: four findings
  - stale `"type":"covered"` in production planner prompt and `scripts/mission-eval.sh`
  - default delegation set falsely recorded all authorities as delegated
  - `metric-scalar` accepted any numeric-looking value instead of a pinned condition
  - CLI/report derived verified reachability from oracle presence instead of proof disposition

All four findings were addressed in signed follow-up commit `d81d91ce`. No second
RoboRev pass was run, per the process diet's one-review-pass constraint.

## Gates

Final-head gates run from repository root:

- `cargo fmt -- --check` PASS
- `cargo check` PASS
- `cargo test` PASS
- `cargo clippy --workspace --all-targets -- -D warnings` PASS
- `bash ./scripts/ci.sh` PASS
  - `moat-refuses-over-privileged-judge` PASS
  - `replanning-revises-atomically-and-strengthen-only` PASS
  - `gap-review-gates-closure` PASS
  - `writable-worker-writes-land-and-resume-no-dup` PASS
  - `oracle-honesty-on-real-broken-code` PASS
  - `confinement-read-only-workspace-erofs` PASS
  - `runtime-native-skill-mount` PASS
  - `prepared-input-feeds-network-off-oracle` PASS
- `git diff --check` PASS

## Coverage Parity

Mandatory inventory command:

```text
$ git diff --name-status 8d7855f8..HEAD -- '*/tests/'
<no output>
```

Expanded test-surface inventory command, used to avoid obscuring changed test files:

```text
$ git diff --name-status 8d7855f8..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
M	crates/lionclaw-model/tests/team_cutover.rs
A	crates/lionclaw/src/runner/real_runtime_continuity_tests.rs
M	crates/lionclaw/tests/advisory_validator.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/controlled_effects.rs
M	crates/lionclaw/tests/driver_recovery.rs
M	crates/lionclaw/tests/eval_deterministic.rs
M	crates/lionclaw/tests/happy_path.rs
M	crates/lionclaw/tests/mission_type_loading.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/replanning.rs
M	crates/lionclaw/tests/resume.rs
M	crates/lionclaw/tests/skill_dispatch.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

Deletion inventory:

```text
$ git diff --name-status --diff-filter=D 8d7855f8..HEAD
<no output>

$ git diff --name-status --diff-filter=D 8d7855f8..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
<no output>
```

Test file counts:

```text
$ git ls-tree -r --name-only 8d7855f8 -- crates | rg '/tests/.*\.rs$' | wc -l
30

$ git ls-tree -r --name-only HEAD -- crates | rg '/tests/.*\.rs$' | wc -l
30
```

Integration test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 8d7855f8 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
217

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
220
```

All Rust test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 8d7855f8 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
298

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
303
```

No test files were deleted. No deletion justifications are required.

## Acceptance Mission

Fresh final-head build:

```text
CARGO_TARGET_DIR=/tmp/lionclaw-slice6-final-accept-target-20260724 cargo build -p lionclaw
```

Acceptance repository:

- path: `/tmp/lionclaw-slice6-final-accept-repo.SENdMI`
- base sha: `428c56f37cb81623974ef8f51954c03ca1384715`
- produced artifact sha: `2996297e2c842838d823c3df22d1cf91ba3d8591`

Mission type verification:

- command: `/tmp/lionclaw-slice6-final-accept-target-20260724/debug/lionclaw mission type show /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-6/mission-types/metric-driven`
- result: valid
- digest: `4574228ac6a4e83d6ea9b7dc761ca1b14c06837291cc6312d335940b86aaf0dd`
- stop: `Attested`

Mission:

- id: `m2798155cf01d`
- objective: `Final Slice 6 acceptance: correct a deliberately mistaken assertion via intent-preserving revision, satisfy the pinned metric-scalar condition, and honestly attempt the attested finish path`
- start command used `--type /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-6/mission-types/metric-driven`; no installed mission-type state was used.

Observed acceptance facts:

- Initial bad assertion `SIMULTANEOUS-CURRENT-CONVERSATIONS` was ratified at revision 1.
- Revision 2 approved `requirement_changes=["ACCEPTANCE-METRIC"]` and superseded the bad assertion with:
  - `METRIC-SCALAR-GTE-EXPECTED`
  - `MISTAKEN-ASSERTION-CORRECTED`
- Status showed default delegation as all false:
  `{"abort":false,"apply":false,"finish":false,"proof_bar_weakening":false,"ratification":false}`.
- Worker effect `1caa35ec7a9e32ec42887ca8007eaed6e596233f02f1484f26628475c9513a26` succeeded and produced commit `2996297e2c842838d823c3df22d1cf91ba3d8591`.
- Oracle `metric-scalar` passed at `2996297e2c842838d823c3df22d1cf91ba3d8591` with stdout:
  ```text
  metric.value=42
  metric.operator=gte
  metric.expected=42
  ```
- Metric judge effect `ff5fca2267b1876b6ee5a36e18f91669bc63288609d9acbbe37f51ab67098734` passed `MISTAKEN-ASSERTION-CORRECTED`.
- Event log:
  ```text
     1 mission_created
     2 team_configured
     3 proposal_recorded
     4 decision_recorded
     5 team_configured
     6 proposal_recorded
     7 decision_recorded
     8 team_configured
     9 role_turn_requested
    10 role_turn_completed
    11 role_turn_requested
    12 role_turn_completed
    13 oracle_run_requested
    14 oracle_run_completed
    15 role_turn_requested
    16 role_turn_completed
  ```

Final acceptance outcome:

- The mission parked at `gap_review_gaps:mission`.
- Terminal gap review passed the correction, intent preservation, supersession traceability, scalar condition, artifact readability, and git integrity checks.
- Terminal gap review found one blocking gap: no authoritative evidence showed that the attested finish path itself had been attempted or recorded an outcome. Because the gap is real, I did not accept/waive it and did not retry or replan.
- Final next actions: `mission decide`, `mission abort`.

This is an honest failed acceptance mission. The new Slice 6 machinery was
exercised through revision, supersession, proof-disposition scheduling, pinned
oracle proof, judged proof, attested terminal review, and gap parking.

## Required Impact Statements

Security impact:

- Tightened, not loosened, proof authority. Host-only obligations are no longer
  assigned as confined proof. Ordinary mission starts no longer falsely record
  delegated authorities. Abort remains non-destructive and unconditional for
  nonterminal missions. The new metric oracle now checks a pinned scalar
  condition rather than accepting arbitrary numeric-looking content.

API/event contract impact:

- Schema version bumped to 25 for new event/wire shapes and proof-disposition
  plan vocabulary. Reducer version bumped to 50 for event-driven finish replay
  and revised proof-disposition/attested semantics. Report JSON now uses
  `not_verified_by_oracle` for assertions that cannot reach verified closure by
  oracle proof.

Docs/product impact:

- Product-facing planner prompt vocabulary updated from `covered` to
  `confined_provable`/`reviewer_checkable`/`host_acceptance`/`limitation`.
  Added the metric-driven mission type and playbook/role docs for the pinned
  scalar oracle.
