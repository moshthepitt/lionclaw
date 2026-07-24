# Slice 9 Exit

Branch: `lionclaw2-a1-runtime-slice-9`
Base: `3caccfaa` (`lionclaw2`, signed Slice 8 head)
Validated product/code head: `08890cf33d298da0f6899031837fbaad1fdae7e1`
Exit note: committed as a signed forward-only bookkeeping commit on top.

## Commits

- `9d81d5e5` `Add parallel writer lineages`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 28`: role turn requests carry dependency candidate refs
    and execution policy carries effect capacity for Slice 9 parallel dispatch.
  - `REDUCER_VERSION 54`: replay now tracks per-task candidate lineages,
    deliverable-head freshness, and downstream repair re-owing.
- `4e003bad` `Harden fan-in writer settlement`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema bump: the event wire shape is unchanged.
  - `REDUCER_VERSION 55`: replay now rejects artifactless multi-dependency
    work outcomes and refuses to accept failed fan-in tasks without a validated
    candidate.
- `08890cf3` `Materialize fan-in dependency commits`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema bump: event and wire shapes are unchanged.
  - no reducer bump: replay semantics are unchanged; the commit fixes effect
    workspace materialization before runtime execution by fetching recorded
    fan-in dependency refs into writer checkouts.

## What Landed

- Added per-task candidate lineages and deliverable-frontier tracking so
  independent writers settle deterministically and integration tasks can merge
  the exact upstream candidate refs they were assigned.
- Rewrote deliverable-head freshness so proof, gap review, repair, status, and
  finish decisions bind to the deliverable head across parallel lineages rather
  than only to the most recent single writer.
- Added downstream repair propagation: repairing a non-sink task re-owes the
  dependent sink integration and proof at the repaired deliverable head.
- Changed dispatch to start all ready work tasks up to the mission effect
  capacity, preserving deterministic serial event append and replay.
- Plumbed integration-task dependency refs through planning validation,
  dispatch, role prompts, role-runner requests, workspace preparation, status,
  diff/report/apply consumers, and self-test surfaces.
- Added the Slice 9 regression suite in
  `crates/lionclaw/tests/parallel_writers.rs`:
  - true parallel writer dispatch with distinct workspaces and proof at the
    deliverable head;
  - repair propagation from a non-sink writer through re-owed integration and
    proof;
  - merge-conflict parking;
  - missing dependency-lineage rejection before proof;
  - failed fan-in accept without a candidate does not fabricate a head;
  - serial single-writer repair equivalence to Slice 8 dispatch/order/status;
  - completion-order fold equivalence plus stale-lineage rejection.
- Added a workspace materialization unit test proving fan-in checkouts fetch
  sibling candidate commits without moving the checkout head.
- No tests were deleted.

## Review Pass

One RoboRev branch review pass was run:

- command: `roborev review --branch --base lionclaw2 --wait`
- job: `1473`
- result: three findings
  - fan-in dependency lineage validation needed to reject missing dependency
    candidates before proof.
  - the parallel writer driver needed to drain all completed effects before
    returning a failed effect.
  - accepting a failed fan-in task without a candidate could not be allowed to
    fabricate a deliverable head.

All findings were addressed in signed follow-up commit `4e003bad`. A live
acceptance failure then exposed that integration checkouts did not fetch
sibling candidate commits; signed follow-up `08890cf3` fixed that effect
workspace materialization bug. No second RoboRev pass was run, per the process
diet's one-review-pass constraint.
`roborev list --branch lionclaw2-a1-runtime-slice-9 --open --json` returned
`null`.

## Gates

Final product/code-head gates run from repository root:

- `cargo fmt -- --check` PASS
- `cargo check` PASS
- `cargo test` PASS
  - includes `serial_single_writer_repair_flow_keeps_slice8_projection` PASS
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
$ git diff --name-status 3caccfaa..HEAD -- '*/tests/'
<no output>
```

Expanded test-surface inventory command, used to avoid obscuring changed test
files:

```text
$ git diff --name-status 3caccfaa..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/src/runner/real_runtime_continuity_tests.rs
M	crates/lionclaw/tests/controlled_effects.rs
M	crates/lionclaw/tests/fold_litmus.rs
A	crates/lionclaw/tests/parallel_writers.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/queue_bounds.rs
M	crates/lionclaw/tests/recovery.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/resume.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

Deletion inventory:

```text
$ git diff --name-status --diff-filter=D 3caccfaa..HEAD
<no output>

$ git diff --name-status --diff-filter=D 3caccfaa..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
<no output>
```

Test file counts:

```text
$ git ls-tree -r --name-only 3caccfaa -- crates | rg '/tests/.*\.rs$' | wc -l
30

$ git ls-tree -r --name-only HEAD -- crates | rg '/tests/.*\.rs$' | wc -l
31
```

Integration test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 3caccfaa -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
227

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
234
```

All Rust test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 3caccfaa -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
311

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
318
```

No test files were deleted. No deletion justifications are required.

## Acceptance Mission

Fresh final-head build:

```text
CARGO_TARGET_DIR=/tmp/lionclaw-slice9-accept-target-20260724-2255-fixed cargo build -p lionclaw
```

Acceptance repositories:

- first attempt path: `/tmp/lionclaw-slice9-accept-repo-20260724-2310`
- first attempt fixture sha: `4042facf9cf12081df6211107bd8808b1b43d105`
- retry path: `/tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed`
- retry fixture sha: `e10f75971bc2048ec5edbce8aefbe1dc303b1bab`
- both fixture seed commits are signed by
  `Kelvin Jayanoris <kelvin@jayanoris.com>`.

Mission type verification:

- command: `/tmp/lionclaw-slice9-accept-target-20260724-2255-fixed/debug/lionclaw mission type show /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-9/mission-types/software-dev --json`
- result: valid
- digest: `ec65648f6ac7bf7174ae48fd3971cf9a324dfb551575455f37f766f0d40d05d9`
- stop: `verified`
- image: `localhost/lionclaw-runtime-dev:v1`
- tmpfs ceiling: `/tmp:rw,size=2g`
- oracle resource override: `cargo-test: tmpfs=/tmp:rw,size=1536m`

First mission:

- id: `md632df60b21e`
- objective: three-task fan-in crate fix with independent `LEFT-LABEL` and
  `RIGHT-LABEL` writers plus `INTEGRATE-LABELS` sink.
- outcome: parked during integration. The integration request carried both
  dependency refs, but the integration workspace could not resolve the sibling
  right-side candidate commit. This exposed a real fan-in workspace
  materialization bug and was fixed in `08890cf3`; no acceptance waiver was
  taken.
- concurrency evidence from this attempt:
  - live `mission status --json` showed both writers active in the same sample;
  - `LEFT-LABEL`: effect
    `db09791e8437c86ca6b826dc81b64f6ff403a53c254cd7c744ab1617575ae312`,
    elapsed about `39752` ms, base `4042facf`;
  - `RIGHT-LABEL`: effect
    `a6bdf5598fb5c1e74d6cf2f8571f3c87dc93f4e59bd8e53365c5a8b3adec9689`,
    elapsed about `39752` ms, base `4042facf`;
  - writer candidates: `a27684fc96e08e6a931b574be04463b7534ce1cf` and
    `81bf9b0e005526b10422fd5610767995806fc357`.

Retry mission:

- id: `m3773003686e4`
- reason for retry: one allowed acceptance retry after the first mission
  exposed a Slice 9 fan-in checkout bug.
- objective: same three-task fan-in topology with independent writers and one
  sink integration task.
- writer workspace paths:
  - `/tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed/.lionclaw/missions/m3773003686e4/tasks/LEFT-LABEL/work`
  - `/tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed/.lionclaw/missions/m3773003686e4/tasks/RIGHT-LABEL/work`
- integration workspace path:
  - `/tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed/.lionclaw/missions/m3773003686e4/tasks/INTEGRATE-LABELS/work`
- concurrency evidence from live `mission status --json`:
  - both writer effects were active simultaneously in the same status sample;
  - `generated_at_ms`: `1784922708192`;
  - `LEFT-LABEL`: effect
    `f6d9e841bc93243e116b53537797c520486e544547da4467629d2b36a1ed650d`,
    role `left-implementer`, elapsed `34254` ms, base
    `e10f75971bc2048ec5edbce8aefbe1dc303b1bab`;
  - `RIGHT-LABEL`: effect
    `77422a164194284d6495406470133c995e4b3caac00ed5aaf0fc2724ab6f1f19`,
    role `right-implementer`, elapsed `34254` ms, base
    `e10f75971bc2048ec5edbce8aefbe1dc303b1bab`;
  - event log sequence also records two `role_turn_requested` events before
    either writer completed: events 6 and 7 precede completions 8 and 9.
- writer candidates:
  - `LEFT-LABEL`: `cc72fc2680a4be176c9c04f0672764e0397bcb70`
  - `RIGHT-LABEL`: `69009b155a547ee9a1324267e7936cf6057cec19`
- integration:
  - integration effect:
    `0eed95a745246d5f10b177db13ae04124b0698344c96f3cf62ddbd8fe58eb5c6`
  - assignment base: `cc72fc2680a4be176c9c04f0672764e0397bcb70`
  - dependency refs:
    - `LEFT-LABEL`: `cc72fc2680a4be176c9c04f0672764e0397bcb70`
    - `RIGHT-LABEL`: `69009b155a547ee9a1324267e7936cf6057cec19`
  - ordinary forward merge commit:
    `ea8a4d9997a307f59f26a0f22edfddd37ac5f717`
  - deliverable head:
    `37de9229efeb1ed3d91e73f3a8a64b70e408793b`
- final result:
  - finish: `verified`
  - deliverable head: `37de9229efeb1ed3d91e73f3a8a64b70e408793b`
  - applied branch: `lionclaw/m3773003686e4`
  - event log contains `mission_finished` and `result_applied`.

Final `mission status --json` projection for `m3773003686e4`:

```json
{
  "mission_id": "m3773003686e4",
  "phase": "done:verified",
  "disposition": "terminal",
  "finish": "verified",
  "revision": 1,
  "team_revision": 1,
  "current_sha": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
  "deliverable_head": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
  "attention_count": 0,
  "contract": [
    {"id":"BUILD-CLEAN","advisory":"pending","authoritative_pass":true},
    {"id":"CLIPPY-CLEAN","advisory":"pending","authoritative_pass":true},
    {"id":"FORMAT-CLEAN","advisory":"pending","authoritative_pass":true},
    {"id":"INTEGRATION-BEHAVIOR","advisory":"pending","authoritative_pass":true},
    {"id":"LEFT-LABEL-BEHAVIOR","advisory":"pending","authoritative_pass":true},
    {"id":"RIGHT-LABEL-BEHAVIOR","advisory":"pending","authoritative_pass":true}
  ],
  "gap_review": {
    "acknowledged": false,
    "attempts": 1,
    "failure_receipt": null,
    "fresh": true,
    "gaps": {"blocking":0,"major":0,"minor":0},
    "judged_sha": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
    "role": "gap-reviewer",
    "verdict": "clean",
    "waived": false
  }
}
```

Relevant `mission report --json` receipt fields for `m3773003686e4`:

```json
{
  "mission_id": "m3773003686e4",
  "finish": "verified",
  "current_sha": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
  "deliverable_head": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
  "gap_review": {
    "acknowledged": false,
    "attempts": 1,
    "failure_receipt": null,
    "fresh": true,
    "gaps": {"blocking":0,"major":0,"minor":0},
    "judged_sha": "37de9229efeb1ed3d91e73f3a8a64b70e408793b",
    "role": "gap-reviewer",
    "verdict": "clean",
    "waived": false
  },
  "tasks": [
    {"id":"INTEGRATE-LABELS","candidate_sha":"37de9229efeb1ed3d91e73f3a8a64b70e408793b","status":"cleared"},
    {"id":"LEFT-LABEL","candidate_sha":"cc72fc2680a4be176c9c04f0672764e0397bcb70","status":"cleared"},
    {"id":"RIGHT-LABEL","candidate_sha":"69009b155a547ee9a1324267e7936cf6057cec19","status":"cleared"}
  ]
}
```

Deliverable-head ancestry:

```text
$ git -C /tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed log --graph --oneline --decorate --max-count=12 37de9229efeb1ed3d91e73f3a8a64b70e408793b
* 37de922 (lionclaw/m3773003686e4) Integrate Slice 9 labels
*   ea8a4d9 Merge commit '69009b155a547ee9a1324267e7936cf6057cec19' into HEAD
|\
| * 69009b1 Fix right label for slice9
* | cc72fc2 Set Slice 9 left label
|/
* e10f759 (HEAD -> main) Seed Slice 9 acceptance retry fixture

$ git -C /tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed rev-list --parents -n 1 ea8a4d9
ea8a4d9997a307f59f26a0f22edfddd37ac5f717 cc72fc2680a4be176c9c04f0672764e0397bcb70 69009b155a547ee9a1324267e7936cf6057cec19

$ git -C /tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed merge-base --is-ancestor cc72fc2680a4be176c9c04f0672764e0397bcb70 37de9229efeb1ed3d91e73f3a8a64b70e408793b
<exit 0>

$ git -C /tmp/lionclaw-slice9-accept-repo-20260724-2258-fixed merge-base --is-ancestor 69009b155a547ee9a1324267e7936cf6057cec19 37de9229efeb1ed3d91e73f3a8a64b70e408793b
<exit 0>
```

Exact event log for `m3773003686e4` after apply:

```text
   1 mission_created
   2 team_configured
   3 proposal_recorded
   4 decision_recorded
   5 team_configured
   6 role_turn_requested
   7 role_turn_requested
   8 role_turn_completed
   9 role_turn_completed
  10 role_turn_requested
  11 role_turn_completed
  12 oracle_run_requested
  13 oracle_run_requested
  14 oracle_run_requested
  15 oracle_run_requested
  16 oracle_run_completed
  17 oracle_run_completed
  18 oracle_run_completed
  19 oracle_run_completed
  20 role_turn_requested
  21 role_turn_completed
  22 mission_finished
  23 result_applied
```

## Required Impact Statements

- Security impact: no new secrets access, egress authority, device access, or
  sandbox widening. Parallelism is effect execution only; event append and fold
  replay remain deterministic and serial. Integration checkouts fetch only
  explicit dependency candidate SHAs already recorded by the kernel.
- API/event contract impact: `SCHEMA_VERSION` advanced from 27 to 28 for
  `RoleTurnRequested.dependency_refs` and mission execution policy
  `effect_capacity`. `REDUCER_VERSION` advanced from 53 to 55 for per-task
  candidate lineage tracking, deliverable-head freshness, downstream repair
  re-owing, and fan-in settlement guards. No later wire change was added.
- Docs impact: no product-facing command path changed. Mission type config now
  carries explicit `effect_capacity`; everyday `lionclaw run [runtime]` usage,
  raw HTTP posture, and product docs were not changed.
