# Slice 7 Exit

Branch: `lionclaw2-a1-runtime-slice-7`
Base: `31b1f683` (`lionclaw2`, signed Slice 6 head)
Validated product/code head: `16c0370d10b77af3931a38b66653771950c1c55e`
Exit note: committed as a signed forward-only bookkeeping commit on top.

## Commits

- `d09aa01f` `Implement Slice 7 resource config surface`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 26`: persisted mission/team resource-ceiling, role/oracle
    resource-override, and oracle-device configuration shapes.
  - `REDUCER_VERSION 51`: replay now validates and preserves resource-only
    configuration decisions in compiled role/oracle plans.
- `16c0370d` `Address Slice 7 RoboRev findings`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema bump: the wire shape stayed at Slice 7's planned schema.
  - `REDUCER_VERSION 52`: stricter, canonical tmpfs resource parsing prevents
    resource configuration replay from accepting authority-like tmpfs flags.

## What Landed

- Added explicit bounded resource configuration:
  - mission-type resource ceilings;
  - per-role-instance tmpfs resource overrides;
  - per-oracle tmpfs resource overrides;
  - per-oracle device declarations bounded by device ceilings.
- Implemented L19 as a named regression: `cargo-test` can declare a `/tmp`
  override above the 512 MiB profile default, roles/oracles without an override
  retain the profile default, and over-ceiling or below-default declarations are
  refused at `compile_role_plan`/plan validation rather than at runtime.
- Added `ConfinementResources` and a structural `ConfinementTmpfsResource`
  parser. Tmpfs resources canonicalize to `target:rw,size=<size>` and reject
  authority-like or unknown flags such as `exec`, `suid`, or `dev`.
- Wired resource ceilings/overrides through mission-type loading, role
  frontmatter, team add/show JSON, mission type show, authority compilation,
  role runner requests, and OCI oracle requests.
- Updated the software-dev mission type to declare a `/tmp:rw,size=2g` ceiling
  and a `cargo-test` oracle override of `/tmp:rw,size=1536m`.
- Updated planning/playbook text so planners can see and author resource
  override syntax, and added the Slice 6 carried rule: objectives/assertions
  must not demand finish-attempt evidence from the terminal gap reviewer because
  finish is structurally illegal before a clean gap review.

## Review Pass

One RoboRev branch review pass was run:

- command: `roborev review --branch --base lionclaw2 --wait`
- job: `1465`
- result: three findings
  - tmpfs resource validation allowed arbitrary options such as `exec`, `suid`,
    and `dev` to pass through to confinement.
  - planning context did not expose resource ceilings or override syntax.
  - missing fault-injection tests for below-profile-default overrides and
    over-ceiling replay/configured events.

All three findings were addressed in signed follow-up commit `16c0370d`. No
second RoboRev pass was run, per the process diet's one-review-pass constraint.
`roborev list --branch lionclaw2-a1-runtime-slice-7 --open` returned
`No jobs found.`

## Gates

Final product/code-head gates run from repository root:

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
$ git diff --name-status 31b1f683..HEAD -- '*/tests/'
<no output>
```

Expanded test-surface inventory command, used to avoid obscuring changed test
files:

```text
$ git diff --name-status 31b1f683..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/src/runner/real_runtime_continuity_tests.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/mission_type_loading.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/reference_expansion.rs
```

Deletion inventory:

```text
$ git diff --name-status --diff-filter=D 31b1f683..HEAD
<no output>

$ git diff --name-status --diff-filter=D 31b1f683..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
<no output>
```

Test file counts:

```text
$ git ls-tree -r --name-only 31b1f683 -- crates | rg '/tests/.*\.rs$' | wc -l
30

$ git ls-tree -r --name-only HEAD -- crates | rg '/tests/.*\.rs$' | wc -l
30
```

Integration test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 31b1f683 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
220

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
226
```

All Rust test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' 31b1f683 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
303

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
310
```

No test files were deleted. No deletion justifications are required.

## Acceptance Mission

Fresh final-head build:

```text
CARGO_TARGET_DIR=/tmp/lionclaw-slice7-accept-target-20260724-1855 cargo build -p lionclaw
```

Acceptance repository:

- path: `/tmp/lionclaw-slice7-accept-repo-20260724`
- first mission base sha: `eb1c9eec46b86b763c4b474dfee0ca31ce9bdfc3`
- retry mission base sha: `a3f7b55eec8694042f38f34cd75410313b4fd456`
- retry fixture commit `a3f7b55` is signed by
  `Kelvin Jayanoris <kelvin@jayanoris.com>` and adds the missing `Cargo.lock`.

Mission type verification:

- command: `/tmp/lionclaw-slice7-accept-target-20260724-1855/debug/lionclaw mission type show /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-7/mission-types/software-dev`
- result: valid
- digest: `1be42c907db9`
- stop: `Verified`
- tmpfs ceiling: `/tmp:rw,size=2g`
- oracle resource override: `cargo-test: tmpfs=/tmp:rw,size=1536m`

First mission:

- id: `m213cc1ef17eb`
- objective: `Verify the existing Rust project has a cargo-test assertion that writes 640 MiB to /tmp and passes only when the cargo-test oracle receives its Slice 7 tmpfs resource override; make no product changes unless necessary.`
- start command used `--type /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-7/mission-types/software-dev`; no installed mission-type state was used.
- outcome: parked at the `cargo-test` oracle before proof because the prepared
  input key `Cargo.lock` was absent from the judged commit.

Final `mission status --json` projection for `m213cc1ef17eb`:

```json
{
  "mission_id": "m213cc1ef17eb",
  "phase": "attention_needed",
  "disposition": "parked",
  "finish": null,
  "revision": 1,
  "team_revision": 1,
  "current_sha": "eb1c9eec46b86b763c4b474dfee0ca31ce9bdfc3",
  "contract": [{"id":"TMPFS-640","advisory":"pending","authoritative_pass":null}],
  "attention": [{"id":"oracle_failed:cargo-test","kind":"oracle_failed","assertion_ids":["TMPFS-640"],"actions":["retry","revise","accept"]}],
  "gap_review": {"attempts":0,"role":"gap-reviewer","verdict":"skipped","acknowledged":false,"waived":false},
  "oracle_failures": {
    "cargo-test": {
      "type": "permanent_runtime",
      "evidence": {
        "code": "oracle.infrastructure",
        "detail": "failed to prepare mission inputs: reading prepared-input key 'Cargo.lock': No such file or directory (os error 2)"
      }
    }
  },
  "parked_effects": [{"effect_id":"28b9628ce1c33dab0597563cd3cc416ce9fae5be79f9bbaf8ac66c0cb71a7144","kind":{"kind":"oracle_run","oracle":"cargo-test"},"legal_controls":["continue"]}],
  "next_actions": ["mission continue","mission decide","mission abort"],
  "cleanup_failure": null,
  "driver_error": null
}
```

Exact event log for `m213cc1ef17eb`:

```text
   1 mission_created
   2 team_configured
   3 proposal_recorded
   4 decision_recorded
   5 team_configured
   6 role_turn_requested
   7 role_turn_completed
   8 oracle_run_requested
   9 oracle_run_completed
```

Retry mission:

- id: `m31162d3948dc`
- reason for retry: one allowed acceptance retry after the first mission parked
  on missing prepared-input fixture state. No second retry was attempted.
- objective: same as the first mission.
- start command used the same repo-path `software-dev` mission type.
- exercised Slice 7: the authoritative `cargo-test` oracle passed
  `TMPFS-640` with the mission type's declared `cargo-test` tmpfs override.
- finish did not complete: the fresh gap review found a real blocking gap in
  the temporary acceptance fixture, so I did not accept, waive, revise, or start
  another replacement mission.

Final `mission status --json` projection for `m31162d3948dc`:

```json
{
  "mission_id": "m31162d3948dc",
  "phase": "attention_needed",
  "disposition": "parked",
  "finish": null,
  "revision": 1,
  "team_revision": 1,
  "current_sha": "a3f7b55eec8694042f38f34cd75410313b4fd456",
  "contract": [{"id":"TMPFS-640","advisory":"pending","authoritative_pass":true}],
  "attention": [{"id":"gap_review_gaps:mission","kind":"gap_review_gaps","report":"Gap review found blocking gaps.","actions":["retry","revise","accept"]}],
  "gap_review": {
    "attempts": 1,
    "fresh": true,
    "gaps": {"blocking":1,"major":1,"minor":0},
    "judged_sha": "a3f7b55eec8694042f38f34cd75410313b4fd456",
    "role": "gap-reviewer",
    "verdict": "gaps",
    "acknowledged": false,
    "waived": false
  },
  "gap_review_receipt": {
    "effect_id": "ea1e362f88fb2afd34ad342c822062613623d450f5dfc7de6d60765f042f3355",
    "gaps": [
      {
        "severity": "blocking",
        "requirement": "The Cargo test must write 640 MiB to /tmp and pass only when the cargo-test oracle receives its Slice 7 tmpfs resource override.",
        "observed": "The test uses std::env::temp_dir(), so TMPDIR redirects the probe. With TMPDIR=/scratch/cargo-tmp, it passed against the 46 GiB /scratch filesystem while /tmp remained an unoverridden 512 MiB tmpfs."
      },
      {
        "severity": "major",
        "requirement": "The resource assertion should be safely rerunnable after an expected insufficient-capacity failure.",
        "observed": "When the write failed for lack of space, cleanup was never reached and a 534212608-byte file remained, filling the 512 MiB tmpfs."
      }
    ]
  },
  "oracle_failures": {},
  "parked_effects": [],
  "next_actions": ["mission decide","mission abort"],
  "cleanup_failure": null,
  "driver_error": null
}
```

Exact event log for `m31162d3948dc`:

```text
   1 mission_created
   2 team_configured
   3 proposal_recorded
   4 decision_recorded
   5 team_configured
   6 role_turn_requested
   7 role_turn_completed
   8 oracle_run_requested
   9 oracle_run_completed
  10 role_turn_requested
  11 role_turn_completed
```

Final acceptance outcome:

- The Slice 7 resource override path was exercised by a real mission and the
  authoritative `cargo-test` oracle passed on the retry mission.
- The mission did not reach `Verified`; there is no `mission_finished` event and
  no `result_applied` event.
- The carried Slice 6 finish obligation remains open for review lead
  disposition. This is an honest parked acceptance, not a waived finish.

## Required Impact Statements

Security impact:

- Resource configuration is bounded execution resource selection only. It
  cannot grant network, secrets, install authority, input access, write access,
  or devices outside ceilings. The runtime moat remains grants intersected with
  ceilings, constrained by the output-semantics floor.
- Tmpfs resources are structurally parsed and canonicalized before confinement
  arguments are built. Unknown or authority-like tmpfs flags are refused instead
  of passed through.
- Oracle device declarations are validated against explicit device ceilings.
  A mission type cannot smuggle device authority through role or oracle config.

API/event contract impact:

- Schema version bumped to 26 for persisted resource-ceiling, role/oracle
  resource-override, and oracle-device configuration shapes.
- Reducer version bumped to 52 across the slice: 51 for replaying the resource
  configuration surface, then 52 for stricter canonical tmpfs resource replay.

Docs/product impact:

- Mission-type and playbook text now document bounded resource overrides as
  resource configuration, not authority.
- Planning context now exposes resource ceilings and role resource syntax.
- Objective-authoring anti-patterns now state that terminal gap reviewers cannot
  provide finish-attempt evidence because finish is not legal before a clean gap
  review.
