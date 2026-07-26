# Slice 9.5 Exit

Branch: `lionclaw2-a1-runtime-slice-9-5`
Base: `d3f6b9cd` (`lionclaw2`, signed Slice 9 head)
Validated product/code head: `204bcfa472c8921b278142995ef3f14a60f18ff4`
Exit note: committed as a signed forward-only bookkeeping commit on top.

## Commits

- `b4df7080` `Close Slice 9.5 runtime contract gaps`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 29`: missions can durably assign a digest-pinned runtime
    environment after OCI preflight and role receipts carry prepared-input
    digest references.
  - `REDUCER_VERSION 56`: replay tracks active mission environment assignment,
    prepared-input receipt evidence, and guide/status projections from the fold.
- `03bd76d3` `Bind proof freshness to environment digest`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 30`: role/oracle effect requests and receipts carry the
    resolved environment digest they ran under.
  - `REDUCER_VERSION 57`: authoritative proof freshness is now bound to both
    judged deliverable SHA and environment digest.
- `749fd677` `Fix environment freshness race coverage`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema bump: the event and receipt wire shape stayed at Slice 9.5's
    planned schema.
  - `REDUCER_VERSION 58`: replay also binds advisory/gap-review freshness to
    the mission environment and dispatch capacity is enforced across batched
    oracle/judge effects.
- `81a4ea7d` `Bind gap review acceptance to environment`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 31`: human gap-review acceptances carry the environment
    digest they acknowledged or waived under.
  - `REDUCER_VERSION 59`: replay requires both deliverable SHA and
    environment digest before an acceptance can close a gap review.
- `f91093b8` `Update production flow version sentinel`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - sentinel-only test update for `SCHEMA_VERSION 31` / `REDUCER_VERSION 59`.
- `204bcfa4` `Update reference expansion version sentinel`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - sentinel-only test update for `SCHEMA_VERSION 31` / `REDUCER_VERSION 59`.

## What Landed

- Added `mission environment show|use` with digest-only parsing, OCI image
  preflight, driver-lock exclusion, active-effect exclusion, durable assignment,
  JSON/human projections, and tests for malformed digests, mutable tags,
  failed preflight, and inflight races.
- Added `mission guide` as a derived, non-authority projection for restarted
  leads. It includes objective, phase/disposition, deliverable head, active
  digest-pinned environment, attention, active/parked effects, conversation
  orientation, and the operator loop `mission guide` plus
  `mission status --json`.
- Bound runtime execution to the request's resolved `environment_digest` for
  both roles and oracles. The mission-level environment is sampled at dispatch;
  runners execute the immutable image recorded on the effect request rather
  than a later profile default.
- Added environment-bound freshness for authoritative verdicts, advisory
  receipts, and terminal gap-review requests. Changing the mission environment
  makes prior proofs/reviews visibly stale and demand-driven proof reruns the
  relevant oracle.
- Added environment-bound freshness for human gap-review acceptances and
  waivers, so an acknowledged blocking-gap review under image A cannot close a
  mission after `environment use` assigns image B.
- Completed the mission-level install-policy surface audit and kept the
  workspace-local prefix playbook rule visible in the shipped software-dev
  mission type.
- Completed the prepared-input projection audit and recorded prepared-input
  digests in role effect receipts/evidence.
- Completed the device grant/ceiling audit and preserved fail-closed OCI
  `--device` flag emission tests that do not require hardware.
- Added capacity regression coverage so concurrent oracle/judge dispatch never
  exceeds the configured remaining `effect_capacity`.
- No tests were deleted.

## Task 0 Audit

| Item | Verdict | Evidence and action |
| --- | --- | --- |
| (a) `mission environment show|use` | Absent at base, implemented | CLI command enum and args are in `crates/lionclaw/src/cli.rs:216` and `crates/lionclaw/src/cli.rs:595`. `show` projects the fold at `crates/lionclaw/src/cli.rs:2037`; `use` parses digest-pinned refs, rejects terminal/inflight missions, acquires the driver lock, preflights OCI, and appends assignment at `crates/lionclaw/src/cli.rs:2056`. Guards are covered by `crates/lionclaw/tests/environment_assignment.rs:153` and `crates/lionclaw/tests/environment_assignment.rs:571`. |
| (b) Install-policy port and workspace-local prefix playbook rule | Already partially present; mission/playbook rule verified | Mission authority maps role install grants to confinement install policy at `crates/lionclaw/src/authority.rs:118`; oracles default to no install authority at `crates/lionclaw/src/authority.rs:172`. The confinement policy type is in `crates/lionclaw-confinement/src/plan.rs:87`. The software-dev playbook states workspace-local/scratch-local install rules at `mission-types/software-dev/playbook.md:119`. No product rewrite was needed. |
| (c) Worker prepared-input projection and digest in EffectHeader/receipts | Partial at base; digest receipt half implemented | Role authorities carry `inputs` at `crates/lionclaw-model/src/team.rs:44`; mission-type loading/validation preserves the grant at `crates/lionclaw/src/mission_type/loader.rs:286` and `crates/lionclaw/src/mission_type/mod.rs:214`; dispatch grants only explicit inputs at `crates/lionclaw/src/engine.rs:2271`. Prepared inputs are mounted read-only and produce digest refs at `crates/lionclaw/src/runner/prepared_input.rs:69`. Receipt/event state now preserve refs through `crates/lionclaw-model/src/event.rs:514`, `crates/lionclaw-model/src/state.rs:183`, and evidence rendering at `crates/lionclaw/src/evidence.rs:210`. |
| (d) Devices grant/ceiling axis and OCI flag emission | Already present; verified no-grant/no-flag | Authority device grants compile for roles and oracles at `crates/lionclaw/src/authority.rs:136` and `crates/lionclaw/src/authority.rs:172`. OCI emits `--device` only from the compiled plan at `crates/lionclaw-confinement/src/oci.rs:390`. Compile tests cover role/oracle grants at `crates/lionclaw/src/authority.rs:499`; no-grant/no-flag OCI coverage is at `crates/lionclaw-confinement/src/oci.rs:1271`. |
| (e) Rewritten shipped skill, playbook/quality-layer content, inspectable clips | Present and inspectable | The shipped playbook includes the terminal gap-review rule and workspace-local install policy at `mission-types/software-dev/playbook.md:114` and `mission-types/software-dev/playbook.md:119`. `mission guide` and report/status JSON expose fold-derived evidence clips at `crates/lionclaw/src/cli.rs:3465`. |
| (f) Per-effect liveness attribution in recovery | Already present | The driver spawns and tracks individual inflight effects at `crates/lionclaw/src/engine.rs:878`. Recovery tests cover detached handoff/driver ownership at `crates/lionclaw/tests/driver_recovery.rs:760`, crash after accepted handoff before completion append at `crates/lionclaw/tests/driver_recovery.rs:1183`, and recovered retained-state projection at `crates/lionclaw/tests/driver_recovery.rs:1354`. |
| (g) Parallel judging lane | Already present; capacity bug fixed | Step scheduling batches judge and oracle effects up to remaining capacity at `crates/lionclaw-model/src/step.rs:300` and `crates/lionclaw-model/src/step.rs:331`; the engine executes batched effects concurrently at `crates/lionclaw/src/engine.rs:895`. Existing coverage is in `crates/lionclaw/tests/parallel_writers.rs:418` and `crates/lionclaw/tests/parallel_writers.rs:448`; the capacity regression is `crates/lionclaw-model/tests/team_cutover.rs:374`. |
| (h) Detached-driver fault injection | Already present | Handshake/driver ownership failure is covered at `crates/lionclaw/tests/driver_recovery.rs:760`; death between handoff/projection and first durable completion append is covered at `crates/lionclaw/tests/driver_recovery.rs:1183`; retained interrupted state projection is covered at `crates/lionclaw/tests/driver_recovery.rs:1354`. |
| (i) ACP usage capture | Already present from Slice 8 | ACP usage parsing is in `crates/lionclaw-runtime-acp/src/protocol.rs:143`; the runtime usage regression is `crates/lionclaw-runtime-acp/src/tests.rs:1333`; persisted/evidence rendering is in `crates/lionclaw/src/evidence.rs:210`. No action. |
| (j) `mission guide` | Absent at base, implemented | The command is wired at `crates/lionclaw/src/cli.rs:2015`. JSON guide includes phase, deliverable head, environment, next actions, active/parked effects, attention, conversations, and the recovery operator loop at `crates/lionclaw/src/cli.rs:3465`. Human guide is at `crates/lionclaw/src/cli.rs:3493`. |

## Freshness Amendment

- `AuthoritativeVerdict`, `RoleTurnProvenance`, and `ReviewAcceptance` now
  carry `environment_digest`; request/acceptance freshness compares both
  current deliverable SHA and current environment digest at
  `crates/lionclaw-model/src/state.rs:253` and
  `crates/lionclaw-model/src/state.rs:799`.
- `RoleTurnRequested` records the resolved environment digest at
  `crates/lionclaw-model/src/event.rs:647`; oracle requests carry the same
  term at `crates/lionclaw-model/src/event.rs:698`.
- Role and oracle runners execute the request-bound image at
  `crates/lionclaw/src/runner/role_runner.rs:97` and
  `crates/lionclaw/src/oracle.rs:42`.
- The named regression is
  `crates/lionclaw/tests/environment_assignment.rs:257`: an assertion is proven
  under image A, `environment use` assigns image B, the prior authoritative
  verdict is no longer verified, and the oracle reruns under image B.
- Advisory and gap-review stale-proof regressions are in
  `crates/lionclaw/tests/advisory_validator.rs:102` and
  `crates/lionclaw/tests/terminal_review.rs:254`.
- The fix-round gap-review acceptance regression is
  `crates/lionclaw/tests/terminal_review.rs:601`: a blocking gap review under
  image A is accepted, `environment use` assigns image B, the acceptance goes
  stale, finish is no longer legal, and the gap reviewer reruns under image B.

Instrument-identity audit:

- Recorded now: deliverable-head SHA, environment digest, role prompt template,
  canonical prompt hash, mission-type digest, and skill additions by digest.
  Evidence: `RoleTurnRequested.prompt_hash` at
  `crates/lionclaw-model/src/event.rs:656`, mission-type digest events at
  `crates/lionclaw-model/src/event.rs:177`, and `SkillAdded` digest events at
  `crates/lionclaw-model/src/event.rs:550`.
- Partially recorded: runtime/model identity is preserved as runtime evidence
  after outcomes, but it is not yet a pre-dispatch freshness term. Evidence
  surfaces include `crates/lionclaw-model/src/state.rs:183` and
  `crates/lionclaw/src/evidence.rs:210`.
- Not fully recorded: the resolved skill digest set per role turn and an
  explicit doctrine-floor version are not modeled as a single canonical
  instrument identity. I did not add that broader concept here because doing it
  cleanly requires one domain type and one freshness path for all instrument
  terms rather than another ad hoc predicate extension. It should be the next
  slice's explicit instrument-identity contract.

## Review Pass

One RoboRev branch review pass was run:

- command: `roborev review --branch --base lionclaw2 --wait`
- job: `1474`
- result: three findings
  - environment assignment was not synchronized with the driver, so OCI effects
    could be requested under image A and labeled with image B.
  - advisory and gap-review freshness were still SHA-only and could remain
    fresh across an environment change.
  - oracle materialization could exceed `effect_capacity` when batching more
    work than the remaining capacity.

All findings were addressed in signed follow-up commit `749fd677`. No second
RoboRev pass was run, per the process diet's one-review-pass constraint.

## Gates

Final product/code-head gates run from repository root:

- `cargo fmt -- --check` PASS
- `cargo check` PASS
- `cargo test` PASS
  - all tests passed; the two real runtime continuity tests remain ignored by
    their existing annotations.
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
$ git diff --name-status d3f6b9cd..HEAD -- '*/tests/'
<no output>
```

Expanded test-surface inventory command, used to avoid obscuring changed test
files:

```text
$ git diff --name-status d3f6b9cd..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/src/runner/real_runtime_continuity_tests.rs
M	crates/lionclaw/tests/advisory_validator.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/controlled_effects.rs
M	crates/lionclaw/tests/conversation_resource_lifecycle.rs
M	crates/lionclaw/tests/driver_recovery.rs
A	crates/lionclaw/tests/environment_assignment.rs
M	crates/lionclaw/tests/eval_deterministic.rs
M	crates/lionclaw/tests/fold_litmus.rs
M	crates/lionclaw/tests/happy_path.rs
M	crates/lionclaw/tests/message_routing.rs
M	crates/lionclaw/tests/parallel_writers.rs
M	crates/lionclaw/tests/planning.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/queue_bounds.rs
M	crates/lionclaw/tests/recovery.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/resume.rs
M	crates/lionclaw/tests/skill_dispatch.rs
M	crates/lionclaw/tests/skill_prompts.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

Deletion inventory:

```text
$ git diff --name-status --diff-filter=D d3f6b9cd..HEAD
<no output>

$ git diff --name-status --diff-filter=D d3f6b9cd..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
<no output>
```

Test file counts:

```text
$ git ls-tree -r --name-only d3f6b9cd -- crates | rg '/tests/.*\.rs$' | wc -l
31

$ git ls-tree -r --name-only HEAD -- crates | rg '/tests/.*\.rs$' | wc -l
32
```

Integration test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' d3f6b9cd -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
234

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
245
```

All Rust test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' d3f6b9cd -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
318

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
329
```

No test files were deleted. No deletion justifications are required.

Overall branch delta at final head, including this exit note:

```text
$ git diff --shortstat d3f6b9cd..HEAD
40 files changed, 2680 insertions(+), 193 deletions(-)
```

## Acceptance Mission

Fresh product/code-head build used for the successful live acceptance:

```text
CARGO_TARGET_DIR=/tmp/lionclaw-slice95-fix-target-20260726 cargo build -p lionclaw
```

Image used for `environment use`:

- digest-pinned ref:
  `localhost/lionclaw-runtime-dev@sha256:590c4259f72f669976690983cf1af2c7f2a2b2164eeeaa4a1dac72c76b490762`
- image id:
  `sha256:e2e555ab56f4a7a60f194fd7a08638480b972c88c1ced36dda5aeda03045d29a`

Mission type verification:

- command:
  `/tmp/lionclaw-slice95-fix-target-20260726/debug/lionclaw mission type show /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-9-5/mission-types/software-dev --json`
- result: valid
- digest:
  `e82ba11d695024403462fdf59b195354c6f2a94403d0ecceb08d6db09c79634a`

Successful fix-round mission:

- id: `m125f9f0f13d7`
- acceptance root:
  `/tmp/lionclaw-slice95-fix-accept-20260726b`
- fixture base SHA:
  `b520464c1891632417ad99fdfb67549e57895af2`
- delivered SHA:
  `838db8dc16ce1ec9555d45b31ba50250745247ef`
- applied branch:
  `lionclaw/m125f9f0f13d7`
- killed lead harness PID: `602514`
- event log path:
  `/tmp/lionclaw-slice95-fix-accept-20260726b/evidence/event-log.txt`
- final status path:
  `/tmp/lionclaw-slice95-fix-accept-20260726b/evidence/final-status.json`
- exact restarted guide path:
  `/tmp/lionclaw-slice95-fix-accept-20260726b/evidence/restarted-guide.txt`

Acceptance fixture repairs:

- The fake ACP writes only JSON-RPC on stdout; fixture diagnostics go to stderr.
- The first worker turn created only an uncommitted marker and no handoff,
  causing a legal `awaiting_lead` checkpoint.
- The restarted lead consumed `mission guide` plus `status --json`, sent the
  next lead message, and the second worker turn resumed with
  `workspace_preparation: preserve`, observed the marker, removed it, committed
  only `src/lib.rs`, and wrote the work handoff.

Exact guide output consumed by the restarted lead:

```text
mission m125f9f0f13d7 guide
objective: Fix the fixture library so answer() returns 42 while preserving the existing test and prove it through the cargo-test oracle.
state: running (awaiting_lead)
commit: b520464c1891
environment: sha256:e2e555ab56f4a7a60f194fd7a08638480b972c88c1ced36dda5aeda03045d29a
  assignment 1: localhost/lionclaw-runtime-dev@sha256:590c4259f72f669976690983cf1af2c7f2a2b2164eeeaa4a1dac72c76b490762 via podman
  conversation implementer: lifecycle=awaiting_lead queued=0 delivery_through=6 resume=canonical_reconstruction legal_actions=mission send
    final response: I wrote a preserved workspace marker and need lead confirmation before completing.
next: mission send | mission abort
```

Final `status --json` excerpt:

```json
{
  "mission_id": "m125f9f0f13d7",
  "disposition": "terminal",
  "phase": "done:verified",
  "finish": "verified",
  "cleanup_failure": null,
  "deliverable_head": "838db8dc16ce1ec9555d45b31ba50250745247ef",
  "environment": {
    "image_id": "sha256:e2e555ab56f4a7a60f194fd7a08638480b972c88c1ced36dda5aeda03045d29a",
    "active_assignment": {
      "image_ref": "localhost/lionclaw-runtime-dev@sha256:590c4259f72f669976690983cf1af2c7f2a2b2164eeeaa4a1dac72c76b490762",
      "image_id": "sha256:e2e555ab56f4a7a60f194fd7a08638480b972c88c1ced36dda5aeda03045d29a",
      "preflight": {
        "engine": "podman",
        "image_ref": "localhost/lionclaw-runtime-dev@sha256:590c4259f72f669976690983cf1af2c7f2a2b2164eeeaa4a1dac72c76b490762",
        "image_id": "sha256:e2e555ab56f4a7a60f194fd7a08638480b972c88c1ced36dda5aeda03045d29a"
      }
    }
  },
  "gap_review": {
    "verdict": "clean",
    "fresh": true,
    "attempts": 1,
    "judged_sha": "838db8dc16ce1ec9555d45b31ba50250745247ef",
    "gaps": {
      "blocking": 0,
      "major": 0,
      "minor": 0
    }
  },
  "contract": [
    {
      "id": "ASSERT-FIXED",
      "authoritative_pass": true,
      "advisory": "pending"
    }
  ]
}
```

Event log:

```text
   1 mission_created
   2 team_configured
   3 environment_assigned
   4 proposal_recorded
   5 decision_recorded
   6 team_configured
   7 role_turn_requested
   8 role_turn_completed
   9 message_sent
  10 role_turn_requested
  11 role_turn_completed
  12 oracle_run_requested
  13 oracle_run_completed
  14 role_turn_requested
  15 role_turn_completed
  16 mission_finished
  17 result_applied
```

Acceptance status: complete. The mission used the digest-pinned host-built
image via `environment use`, continued a preserved worker checkout after lead
restart, reached `mission_finished`, and recorded `result_applied`.

## Impact Statements

Security impact:

- `environment use` fails closed on mutable tags, malformed digests, failed OCI
  preflight, terminal missions, inflight effects, and active driver ownership.
- Execution authority is not widened. Roles/oracles still run through compiled
  confinement plans, and the OCI image used at runtime is the request-bound
  digest recorded before dispatch.

API/event contract impact:

- `SCHEMA_VERSION` is now `31` because mission environment assignments,
  role/oracle effect request/receipt shapes, and gap-review acceptances carry
  environment/prepared-input terms.
- `REDUCER_VERSION` is now `59` because replay freshness, human gap-review
  acceptance freshness, and dispatch capacity semantics changed.
- The schema changes are within the Slice 9.5 mandate.

Docs/mission-type impact:

- The software-dev playbook exposes the workspace-local prefix install rule and
  terminal gap-review quality rule.
- `mission guide` is an operator projection, not an authority surface.

## Acceptance History (restored by review lead)

The successful mission `m125f9f0f13d7` was the third live acceptance attempt.
The fix-round edit to this note replaced the earlier record rather than adding
to it; the full original text remains in history at commit `1ddde2cd`. Restated
here so this note stands alone.

Two earlier acceptance missions failed, both from defects in the acceptance
fixture, not in the product:

- `m5de04bfb790b` — `environment use` succeeded and a later turn ran under the
  digest-pinned image; the lead was killed and restarted and guide/status
  orientation was exercised. The mission then parked because the fake ACP
  committed directly inside the retained no-handoff checkout, leaving
  uncaptured commits. `role_runner` correctly refused to recreate that
  checkout. A repeated public-control decision hit the same protection and the
  attempt was stopped rather than forced.
- `m0f5adc52f6a5` — `environment use` and the forced lead-restart path were
  exercised again. The next advance failed because the fake ACP wrote its
  `git commit` summary to stdout, producing
  `invalid ACP JSON-RPC line: [detached HEAD fdbd7f5] slice95 seed acceptance marker`.
  ACP stdout carries JSON-RPC only, so the driver's rejection was correct.

Neither failure was an `environment use` or `mission guide` implementation
failure, and neither guard was weakened to accommodate the fixture: the diff
`749fd677..HEAD` over `crates/lionclaw/src/runner/`, `crates/lionclaw/src/driver*`
and `crates/lionclaw-runtime*` is empty. The fixture was repaired instead, as
recorded under "Acceptance fixture repairs".
