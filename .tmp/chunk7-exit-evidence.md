# Chunk 7 Child Missions Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-7-child-missions`

Branch: `lionclaw2-simple-chunk-7-child-missions`

Accepted signed base: `915af0b04df66923d32996e8944f331278526e35`

Chunk 8 is not included. No work was performed in the planning-material
worktree `.worktrees/lionclaw2`, and `main` was not used as an implementation
base.

## RED Evidence

The first focused test was added before the authoring surface and run as:

```text
cargo test -p lionclaw prompt::team_prompt_tests::planning_prompt_exposes_typed_child_mission_assignments -- --exact
```

It failed at the assertion requiring the planning contract to contain
`"type": "child_mission"`: `0 passed; 1 failed`. This demonstrated that an
ordinary `lionclaw run [runtime]` orchestrator could not author child work
through the existing typed proposal boundary. The same test passes after the
implementation.

The child crash/replay scenarios were then expressed in
`crates/lionclaw-model/tests/child_missions.rs`,
`crates/lionclaw/tests/child_missions.rs`, and the everyday-path test in
`crates/lionclaw/tests/everyday_run.rs` before their production paths were
completed.

## Implementation

- `TaskAssignment` is now a closed tagged enum with `role` and
  `child_mission` variants. A child assignment contains an ordinary complete
  `MissionConfig` and revision-zero `MissionProposal`.
- `next` deterministically projects `EffectIntent::ChildMission`; the request
  is appended before child creation. The parent effect id and child mission id
  bind the parent id, task, attempt, input artifact/dependencies, and canonical
  request digest.
- Child creation uses the existing event store, proposal approval, fold,
  `next`, engine driver, proof, recovery, and cleanup paths. There is no second
  scheduler, supervisor, prompt stack, or child-specific recovery state
  machine.
- Kernel-owned lineage reconnects an exact existing child after each crash
  boundary. A terminal receipt is folded before a separate durable cleanup
  marker; the child event log remains available as evidence.
- Successful artifact and report outputs clear an ordinary parent task.
  Failure and abort enter the existing failure, anti-thrash, retry, and replan
  choices. A changed explicit retry advances the attempt and therefore derives
  a new child id.
- The attached everyday launcher can author, approve, execute, report, finish,
  and apply a typed child assignment through normal `lionclaw run fake`
  orchestration.

## Security Impact

- Child ceilings, grants, resources, network destinations, runtime ids,
  installed skill packages, deadlines, recovery attempts, execution policy,
  depth, descendants, and effect capacity are admitted recursively against the
  parent contract using the existing authority/resource/network predicates.
- Child effect capacity reserves its whole subtree at the parent. All projected
  local and child work uses one shared capacity calculation, so recursion
  cannot multiply concurrency.
- Depth and descendant limits are checked both in proposal admission and at
  creation against kernel-owned stored lineage. Stored parent links are walked
  with cycle detection; request-provided ancestry does not exist.
- Parent abort, effect stop, and deadline expiry durably abort the bound child
  and drive its ordinary cleanup until it is terminal and has no active effect.
- Secret capability is a boolean authority only. Secret values are not fields
  of child assignments, requests, lineage, events, outputs, receipts, prompts,
  argv, or environment. Runtime credential projection remains owned by the
  existing kernel path.
- The parent receipt is derived from folded child state and binds parent id,
  parent effect id, child id, request digest, input artifact, terminal state,
  output artifact/report digest, proof-summary digests, and typed failure.
  Child proof summaries are audit-only and never enter the parent's
  authoritative receipt map or satisfy parent proof.

## API And Event Impact

This is an intentional typed wire-contract change:

- `TeamRevision.task_assignments` changes from role-id strings to tagged
  `{"type":"role","role_instance":...}` or
  `{"type":"child_mission","mission":...}` values.
- `MissionConfig` adds runtime ceilings; `ExecutionPolicy` adds maximum child
  depth and descendants. Existing profile/default constructors supply the new
  values.
- `MissionCreated` records optional kernel lineage.
- New durable events are `child_mission_requested`, `child_mission_bound`,
  `child_mission_completed`, and `child_mission_cleaned`.
- Status, report, guide, and activity projections expose active child identity,
  request/receipt truth, output/report digests, lineage, and cleanup state.
- `lionclaw team assign` continues to author the `role` variant. Child
  assignments are authored atomically through the orchestrator's complete
  proposal contract; no hidden CLI mutation was added.

## Schema And Reducer

- Event schema: `41` (was `40`).
- Reducer: `77` (was `76`).
- SQL storage shape is unchanged because child state is represented by ordinary
  versioned event payloads in the existing event store. No SQL migration is
  required.
- Old schema events continue to fail closed, and snapshots are invalidated by
  the schema/reducer stamps rather than being interpreted under the new wire
  contract.

## Verification

Passed after the final source change:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
- `bash ./scripts/ci.sh`
  - workspace check passed;
  - Clippy passed for all targets/features with warnings denied;
  - rustdoc passed with warnings denied;
  - every runnable workspace test passed;
  - all nine named Podman mission self-tests passed.
- `git diff --check`
- model child suite: 7 passed;
- engine child suite: 5 passed;
- everyday `lionclaw run` child acceptance passed as part of the 12-test suite;
- runtime product-boundary tests: 2 passed.

CodeIntel diagnostics covered all 33 changed Rust files and reported no errors:
37 known macro-input `None` false-positive warnings plus three inactive-`cfg`
hints. Cargo and Clippy reported no warnings.

Tracked product grep for `FrontierSWE` and `Harbor` returned no matches. The
secret sentinel leak probe returned no production, event, prompt, report, argv,
environment, or receipt match outside its source test.

The exact base commit has a valid signature from
`Kelvin Jayanoris <kelvin@jayanoris.com>`, and the chunk branch was confirmed to
start at that exact commit before its first signed commit.

## Residual Coverage

Four tests are ignored by the default Cargo gate:

- authenticated Codex native-session continuity;
- authenticated OpenCode native-session continuity;
- authenticated Hermes native-session continuity;
- production external-oracle OCI execution.

The three native-session tests require committed clean source and are run only
from the signed implementation commit; their fresh roots, image identity,
timings, and receipt digests are appended in the signed evidence follow-up.

The external-oracle OCI test remains blocked on this host because delegated
CPU/memory cgroup controllers are unavailable. `scripts/ci.sh` reported this
exact skip and still ran all nine mandatory Podman self-tests.

No work was pushed, merged, rebased, submitted for review, or ported to `main`.
