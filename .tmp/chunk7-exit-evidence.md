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

Post-review hardening also followed RED -> GREEN. Before changing production
code, the new focused tests demonstrated:

- a durable parent stop followed by a successful child receipt replayed as a
  cleared task instead of `control.stopped_before_settlement` (`7 passed; 2
  failed` in the model child suite with the retry-reservation RED below);
- a three-attempt leaf child was admitted under a two-descendant parent limit;
- an active child role did not observe a live parent stop within two seconds,
  and required the test to abort the child for bounded cleanup;
- a folded child failure reached the parent only as generic
  `child_mission.failed`, losing its original code, detail, and response; and
- ordinary `lionclaw mission advance <child-id> --json` exited 1 because it
  looked for a nonexistent child-local mission-type snapshot.

The retry test was extended past projection to create and settle the second
durable child. Stop, deadline, and abort are exercised against a live child;
pre-creation cancellation, terminal-before-receipt replay, and real CLI reopen
are separate regression cases.

The final review corrections also began with three focused failures:

- `report_child_handoff_reaches_an_ordinary_downstream_role` failed with
  `cleared dependency points to a missing role-attempt receipt`, proving that
  an ordinary task consumer could not read a child report;
- `report_dependency_reaches_child_through_ordinary_mission_input` failed
  because the child worker prompt did not contain `delegated task output`,
  proving that dependency identity was hashed but its content was unreachable;
- the failed-child retry scenario still advertised exact `Retry` after the
  second permitted child had settled, proving that admission and retry
  capacity used inconsistent accounting.

The earlier secret-schema test was also corrected because its sentinel was
declared only after serialization and therefore could not prove non-leakage.
The replacement tests exercise typed schema shape plus the kernel admission
boundary for authoring, child execution, and historical dependency receipts.

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
  choices while preserving the bounded folded child task/oracle failure. A
  changed explicit retry advances the attempt and therefore derives and
  actually creates a new child id.
- Ordinary task report consumers and independent judges resolve role and child
  receipts through one shared folded-task interface. Child success supplies
  task output only; its proof summary remains non-authoritative to the parent.
- Cleared dependency identity now binds producer task, producer effect,
  candidate commit, report digest, and failure digest. Child creation resolves
  the corresponding payload from folded parent truth and records it once as
  an immutable ordinary mission input before approving the child proposal.
  Planning and execution receive that input through their existing prompt
  context rather than a child-only prompt path.
- Descendant admission accounts folded historical descendants plus all legal
  planned child attempts. Creation rechecks lineage capacity before appending
  a request, and exhausted child retries are not projected, so admission
  cannot create an inflight request that runtime must refuse.
- Ordinary CLI opening walks and verifies the durable kernel-owned lineage,
  then reuses the immutable root mission-type snapshot. The loaded snapshot's
  digest is still checked against the child mission before execution.
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
  creation against kernel-owned stored lineage. Admission reserves every legal
  parent task attempt, so durable retry children cannot exhaust a budget that
  the plan was allowed to underdeclare. Stored parent links are walked with
  cycle detection; request-provided ancestry does not exist.
- Parent abort, effect stop, and deadline expiry use the same live control
  refresh and durable settlement predicate as ordinary effects. They abort and
  drain a bound active child; cancellation before creation creates only the
  deterministic terminal child needed for receipt truth and never leaves an
  active orphan.
- Secret capability remains a boolean authority, not serialized secret
  material. Teams that author or execute child work must use secret-free role
  grants, and dependency derivation rejects a historical role receipt produced
  under secret authority. This keeps child objectives, dependency payloads,
  prompts, reports, argv, environment, and receipts outside the runtime-secret
  projection path. LionClaw does not claim arbitrary-text DLP; operator-supplied
  text remains operator-owned input.
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
- `ChildMissionRequest` now carries content-bound dependency records rather
  than candidate commits alone, and `ChildMissionReceipt` records the folded
  descendant count below the child.
- `MissionInputRecorded` attaches immutable resolved ordinary-task inputs to a
  child before its proposal is approved.
- New durable events are `child_mission_requested`, `child_mission_bound`,
  `child_mission_completed`, and `child_mission_cleaned`.
- Status, report, guide, and activity projections expose active child identity,
  request/receipt truth, output/report digests, lineage, and cleanup state.
- `lionclaw team assign` continues to author the `role` variant. Child
  assignments are authored atomically through the orchestrator's complete
  proposal contract; no hidden CLI mutation was added.
- No child-specific CLI mutation or SQL shape was added. The final correction
  intentionally changes the typed request, receipt, event, and folded-state
  contracts described above.

## Schema And Reducer

- Event schema: `42` (version `41` introduced child mission facts; `42` records
  immutable task inputs inherited by a child).
- Reducer: `79` (version `78` made durable parent cancellation dominate child
  settlement; `79` folds immutable mission inputs).
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
- model child suite: 10 passed;
- engine child suite: 11 passed;
- everyday `lionclaw run` child acceptance passed as part of the 12-test suite;
- runtime product-boundary tests: 2 passed.

CodeIntel inspected the shared child dependency derivation before the final
edit and diagnostics covered all 13 changed Rust files. It reported no errors:
eight known macro-input `None` false-positive warnings and one inactive-`cfg`
hint. Cargo and Clippy reported no warnings.

The product-surface grep is restricted to implementation, bundled mission
types, skills, scripts, containers, manifests, help, and CI; audit notes and
tests are not product integration. It returned no prohibited product-name
match. Leak checks found no typed child field for secret values and no secret
grant path through child-author, child-execution, or inherited dependency
admission. The removed sentinel test is not cited as evidence.

The exact base commit has a valid signature from
`Kelvin Jayanoris <kelvin@jayanoris.com>`, and the chunk branch was confirmed to
start at that exact commit before its first signed commit.

The initial implementation and first hardening commits remain signed and
forward-only. The final task-flow, input, capacity, and secret-boundary fixes
are signed commit `dbf61d62b8284f001735ca229eff8c3d04ee6cfd`, a direct descendant
of the accepted base.

The three authenticated native-session continuity tests passed from that clean
signed source against `localhost/lionclaw-runtime:v1`, resolved before launch
to immutable image identity
`e1541b6609e7209de4087aac38b08151ad5feb9dd901cf464e088b41b32ba05d`:

- Codex: `1 passed; 0 failed`, 31.63 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-final-codex-20260801-075035`. Receipt SHA-256:
  `23436a9f198a3a6517d2d79d34b6cf3299f4116d76de12c481ac69b39adaad30`.
- OpenCode: `1 passed; 0 failed`, 20.34 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-final-opencode-20260801-075035`. Receipt SHA-256:
  `27fd9c94e3d16b206f5e73248cb01ad28e4f3f1a35ebdf0261858045c2c65bad`.
- Hermes: `1 passed; 0 failed`, 37.45 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-final-hermes-20260801-075035`. Receipt SHA-256:
  `733595bdbb502dc48a1a06604b376885cccf3dab93b38fe730286db8bf66495b`.

Each `lionclaw.runtime-continuity-proof.v2` receipt records the signed source
head `dbf61d62b8284f001735ca229eff8c3d04ee6cfd`, immutable image identity,
first observation `Reconstructed`, second
observation `Resumed`, distinct effect ids, exact effect cleanup, retained
native home and runtime state, removed credential projection, and a second
response digest equal to the hidden token digest.

## Residual Coverage

Four tests are ignored by the default Cargo gate:

- authenticated Codex native-session continuity;
- authenticated OpenCode native-session continuity;
- authenticated Hermes native-session continuity;
- production external-oracle OCI execution.

The three native-session tests require committed clean source, real auth,
network, and the OCI runtime image. All three were explicitly run and passed as
recorded above; none remains blocked.

The external-oracle OCI test remains blocked on this host because delegated
CPU/memory cgroup controllers are unavailable. `scripts/ci.sh` reported this
exact skip and still ran all nine mandatory Podman self-tests.

No work was pushed, merged, rebased, submitted for review, or ported to `main`.
