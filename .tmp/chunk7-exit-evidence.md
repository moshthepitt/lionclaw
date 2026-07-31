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
- The post-review correction adds no event variants, serialized fields, CLI
  mutation, or SQL shape. It changes admission semantics, ordinary CLI child
  snapshot resolution, and folded settlement behavior only.

## Schema And Reducer

- Event schema: `41` (was `40`).
- Reducer: `78` (was `76`; `77` introduced the child events and `78` makes
  durable parent cancellation dominate child settlement).
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
- model child suite: 9 passed;
- engine child suite: 8 passed;
- everyday `lionclaw run` child acceptance passed as part of the 12-test suite;
- runtime product-boundary tests: 2 passed.

CodeIntel diagnostics covered all eight Rust files changed by the hardening
commit and reported no errors: 26 known macro-input `None` false-positive
warnings plus three inactive-`cfg` hints. Cargo and Clippy reported no
warnings.

The product-surface grep, restricted to implementation, mission types, skills,
scripts, containers, manifests, and the README, returned no prohibited
benchmark-name match. This audit note necessarily quotes the reviewed names and
is not a product surface. The secret sentinel leak probe returned no
production, event, prompt, report, argv, environment, or receipt match outside
its defining source test.

The exact base commit has a valid signature from
`Kelvin Jayanoris <kelvin@jayanoris.com>`, and the chunk branch was confirmed to
start at that exact commit before its first signed commit.

The implementation is signed commit
`899a11aa8d8e648bce325d0f7d8f58783b3481e8`, a direct descendant of the
accepted base. The post-review production and regression fixes are signed
forward-only commit `596a6d64d72447868826938ee8767326ae489a44`.

The three authenticated native-session continuity tests passed from that clean
signed source against `localhost/lionclaw-runtime:v1`, resolved before launch
to immutable image identity
`e1541b6609e7209de4087aac38b08151ad5feb9dd901cf464e088b41b32ba05d`:

- Codex: `1 passed; 0 failed`, 29.37 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-fix-codex-20260731-191853`. Receipt SHA-256:
  `cebff30c621c6000b77944af8a614b5e4d3b8e7510f8e027abb4106624de831d`.
- OpenCode: `1 passed; 0 failed`, 22.38 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-fix-opencode-20260731-191853`. Receipt SHA-256:
  `8ccae04dbe470aa552aa0b2068ffbcbbdacb5fda365bc4eb8fee7a9fca78c5fd`.
- Hermes: `1 passed; 0 failed`, 36.72 seconds. Preserved root:
  `/tmp/lionclaw-chunk7-fix-hermes-20260731-191853`. Receipt SHA-256:
  `3f5ce3a4f022fd94c2ad31729f3e116913123516f2d5301adb64bd4d0be16d66`.

Each `lionclaw.runtime-continuity-proof.v2` receipt records the signed source
head `596a6d64d72447868826938ee8767326ae489a44`, immutable image identity,
first observation `Reconstructed`, second
observation `Resumed`, distinct effect ids, exact effect cleanup, retained
native home and runtime state, removed credential projection, and a second
response digest equal to the hidden token digest.

An earlier Codex launch failure is preserved at
`/tmp/lionclaw-chunk7-live-codex-20260731-170034`. The explicitly named
untagged image `9ed8b30de84295193f92baf66e88e6f28bd4f0f4aa77d1448e7f22220bb3a126`
contained all three agent CLIs but lacked the required
`/usr/local/bin/lionclaw` network-proxy binary, so the proxy exited before
readiness with Podman inspect code 125. The correct runtime image passed all
three tests without a source change. The failed root contains no passing
receipt and is retained as infrastructure RED evidence.

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
