# Slice 8 Exit

Branch: `lionclaw2-a1-runtime-slice-8`
Base: `f76b6672` (`lionclaw2`, signed Slice 7 head)
Validated product/code head: `02b1f0acd0c3438409e0ef398f2acd4060b8365d`
Exit note: committed as a signed forward-only bookkeeping commit on top.

## Commits

- `753a3778` `Capture ACP observation and runtime usage truth`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - `SCHEMA_VERSION 27`: persisted role outcome/failure/receipt surfaces now
    carry runtime usage, and the runtime/receipt wire shape distinguishes
    reported usage from truthful absence.
  - `REDUCER_VERSION 53`: replay now preserves runtime usage on role attempt
    receipts and failure evidence.
- `02b1f0ac` `Harden ACP configuration drift evidence`
  - signed by `Kelvin Jayanoris <kelvin@jayanoris.com>`
  - no schema bump: the event and receipt shape stayed at Slice 8's planned
    schema.
  - no reducer bump: the commit tightens validation and evidence preservation
    without changing replay semantics.

## What Landed

- Added `RuntimeUsage` as a typed runtime-result concept:
  - `not_reported` is explicit when a runtime reports no usage;
  - `reported` carries input, output, reasoning, total, context-used,
    context-window, and optional cost with an explicit scope;
  - usage is never estimated or inferred.
- Wired runtime usage through runtime API turn results, role runner requests,
  persisted role outcomes/failures, mission receipts, evidence rendering, and
  report JSON/text surfaces.
- Upgraded ACP configuration evidence from acknowledgement-only to observation
  where ACP reports it:
  - `current_mode_update` notifications are parsed and tracked;
  - `session/update` `set_model` and `set_mode` results are parsed;
  - ACP `config_option_update` current values are merged into observation
    state;
  - malformed or absent observations degrade to typed absence or typed runtime
    failure evidence, never requested-value guesses.
- Implemented L12 and L13:
  - observed applied model/mode fields are populated only from runtime-reported
    state;
  - ACP turns with no requested model preserve the runtime-advertised current
    model when the runtime reports it.
- Added post-configuration drift hardening:
  - a runtime notification that changes model or mode after successful setup
    fails closed with requested and observed evidence;
  - partial configuration failures preserve both requested values and the
    observations received before the failure.
- Ported existing role-success and role-failure tests to the new usage field.
  No tests were deleted.

## Review Pass

One RoboRev branch review pass was run:

- command: `roborev review --branch --base lionclaw2 --wait`
- job: `1472`
- result: two findings
  - mid-turn ACP configuration notifications could overwrite requested profile
    evidence without final mismatch rejection.
  - partial ACP configuration failures could lose requested/observed evidence
    if setup failed partway through.

Both findings were addressed in signed follow-up commit `02b1f0ac`. No second
RoboRev pass was run, per the process diet's one-review-pass constraint.
`roborev list --branch lionclaw2-a1-runtime-slice-8 --open --json` returned
`null`.

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
$ git diff --name-status f76b6672..HEAD -- '*/tests/'
<no output>
```

Expanded test-surface inventory command, used to avoid obscuring changed test
files:

```text
$ git diff --name-status f76b6672..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/tests/advisory_validator.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/controlled_effects.rs
M	crates/lionclaw/tests/conversation_resource_lifecycle.rs
M	crates/lionclaw/tests/driver_recovery.rs
M	crates/lionclaw/tests/eval_deterministic.rs
M	crates/lionclaw/tests/happy_path.rs
M	crates/lionclaw/tests/message_routing.rs
M	crates/lionclaw/tests/planning.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/queue_bounds.rs
M	crates/lionclaw/tests/recovery.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/skill_dispatch.rs
M	crates/lionclaw/tests/skill_prompts.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

Deletion inventory:

```text
$ git diff --name-status --diff-filter=D f76b6672..HEAD
<no output>

$ git diff --name-status --diff-filter=D f76b6672..HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*tests*.rs'
<no output>
```

Test file counts:

```text
$ git ls-tree -r --name-only f76b6672 -- crates | rg '/tests/.*\.rs$' | wc -l
30

$ git ls-tree -r --name-only HEAD -- crates | rg '/tests/.*\.rs$' | wc -l
30
```

Integration test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' f76b6672 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
226

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | wc -l
227
```

All Rust test-function counts from the same command:

```text
$ git grep -h -E '^\s*#\[(tokio::test|test)' f76b6672 -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
310

$ git grep -h -E '^\s*#\[(tokio::test|test)' HEAD -- 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' 'crates/*/src/**/*.rs' | wc -l
311
```

No test files were deleted. No deletion justifications are required.

## Acceptance Mission

Fresh final-head build:

```text
CARGO_TARGET_DIR=/tmp/lionclaw-slice8-accept-target-20260724-2030 cargo build -p lionclaw
```

Acceptance repositories:

- first attempt path: `/tmp/lionclaw-slice8-accept-repo-20260724-2035`
- first attempt fixture sha: `0d87079d27fae87a987de10cd55eb379cfb7fdad`
- retry path: `/tmp/lionclaw-slice8-accept-repo-20260724-2045`
- retry fixture sha: `61edb7b89ff95d70f69683294d90119b238a93b5`
- both fixture seed commits are signed by
  `Kelvin Jayanoris <kelvin@jayanoris.com>`.

Mission type verification:

- command: `/tmp/lionclaw-slice8-accept-target-20260724-2030/debug/lionclaw mission type show /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-8/mission-types/software-dev`
- result: valid
- digest: `1be42c907db9`
- stop: `Verified`
- image: `localhost/lionclaw-runtime-dev:v1`
- tmpfs ceiling: `/tmp:rw,size=2g`
- oracle resource override: `cargo-test: tmpfs=/tmp:rw,size=1536m`

First mission:

- id: `mf96940b82a00`
- objective: `Fix the Rust library so the existing reports_slice8_observation_label test passes by making slice8_observation_label() return slice8-observed. Keep the public API and test name intact, make only the minimal source change needed, and finish after fmt, check, test, clippy, build, and gap review are clean.`
- start command used `--type /home/mosh/mosh/misc/lionclaw/.worktrees/lionclaw2-a1-runtime-slice-8/mission-types/software-dev`; no installed mission-type state was used.
- outcome: parked before oracles because OpenCode completed the implementer
  turn, but retained native state failed post-turn admission on a non-regular
  session-control entry under `.config/opencode/node_modules/.bin/uuid`.
- live ACP evidence captured:
  - `requested_model`: `opencode/big-pickle`
  - `applied_model`: `opencode/big-pickle`
  - `model_confirmation`: `observed`
  - `requested_mode`: `build`
  - `applied_mode`: `build`
  - `mode_confirmation`: `observed`
  - `runtime_usage.status`: `reported`
  - `input_tokens`: `225`
  - `output_tokens`: `44`
  - `reasoning_tokens`: `13`
  - `total_tokens`: `10522`
  - `context_used_tokens`: `10465`
  - `context_window_tokens`: `200000`
  - `cost`: `0 USD`, `session_cumulative`

Final `mission status --json` projection for `mf96940b82a00`:

```json
{
  "mission_id": "mf96940b82a00",
  "phase": "attention_needed",
  "finish": null,
  "revision": 1,
  "team_revision": 4,
  "current_sha": "0d87079d27fae87a987de10cd55eb379cfb7fdad",
  "contract": [
    {"id":"BUILD-CHECK","advisory":"pending","authoritative_pass":null},
    {"id":"CLIPPY-CHECK","advisory":"pending","authoritative_pass":null},
    {"id":"FMT-CHECK","advisory":"pending","authoritative_pass":null},
    {"id":"LABEL-TEST","advisory":"pending","authoritative_pass":null}
  ],
  "attention": [{"id":"node_failed:fix-label","kind":"node_failed","actions":["retry","revise","accept"]}],
  "gap_review": {"attempts":0,"role":"gap-reviewer","verdict":"owed","acknowledged":false,"waived":false},
  "opencode_receipt": {
    "role": "implementer",
    "outcome": "failed",
    "failure_code": "runtime.native_state_limit",
    "effective_runtime_configuration": {
      "requested_model": "opencode/big-pickle",
      "applied_model": "opencode/big-pickle",
      "model_confirmation": "observed",
      "requested_mode": "build",
      "applied_mode": "build",
      "mode_confirmation": "observed"
    },
    "runtime_usage": {
      "status": "reported",
      "usage": {
        "input_tokens": 225,
        "output_tokens": 44,
        "reasoning_tokens": 13,
        "total_tokens": 10522,
        "context_used_tokens": 10465,
        "context_window_tokens": 200000,
        "cost": {"amount":"0","currency":"USD","scope":"session_cumulative"}
      }
    }
  }
}
```

Exact event log for `mf96940b82a00`:

```text
   1 mission_created
   2 team_configured
   3 team_configured
   4 team_configured
   5 team_configured
   6 proposal_recorded
   7 decision_recorded
   8 team_configured
   9 role_turn_requested
  10 role_turn_completed
```

Retry mission:

- id: `m64f6bfc5ae64`
- reason for retry: one allowed acceptance retry after the first mission
  parked on OpenCode retained native state.
- objective: same as the first mission.
- start command used the same repo-path `software-dev` mission type.
- live ACP exercise:
  - OpenCode implementer and gap-reviewer turns both reported observed
    model/mode and usage on the receipt/report surfaces.
  - Both OpenCode turns later failed retained native-state admission on
    non-regular entries created below `.config/opencode/node_modules/.bin`.
  - I did not accept or waive those failed outcomes.
- bounded recovery:
  - a clean Codex implementer role produced commit
    `41d47fcf7d3cd4102a9b8b4790b555c5f5c67dcb`;
  - all authoritative oracles passed at that commit;
  - the first gap review on OpenCode completed a clean review text but then
    failed retained native-state admission;
  - `gap-reviewer` was rebound to `codex`, and the failed gap-review item was
    retried once with justification recorded in the mission log;
  - the retry gap review was fresh, clean, and unwaived.
- final result:
  - finish: `verified`
  - final commit: `41d47fcf7d3cd4102a9b8b4790b555c5f5c67dcb`
  - applied branch: `lionclaw/m64f6bfc5ae64`
  - event log contains `mission_finished` and `result_applied`.

Final `mission status --json` projection for `m64f6bfc5ae64`:

```json
{
  "mission_id": "m64f6bfc5ae64",
  "phase": "done:verified",
  "finish": "verified",
  "revision": 2,
  "team_revision": 10,
  "current_sha": "41d47fcf7d3cd4102a9b8b4790b555c5f5c67dcb",
  "attention_count": 0,
  "contract": [
    {"id":"BUILD-CHECK","advisory":"pending","authoritative_pass":true},
    {"id":"CLIPPY-CHECK","advisory":"pending","authoritative_pass":true},
    {"id":"FMT-CHECK","advisory":"pending","authoritative_pass":true},
    {"id":"LABEL-TEST","advisory":"pending","authoritative_pass":true}
  ],
  "gap_review": {
    "attempts": 2,
    "fresh": true,
    "gaps": {"blocking":0,"major":0,"minor":0},
    "judged_sha": "41d47fcf7d3cd4102a9b8b4790b555c5f5c67dcb",
    "role": "gap-reviewer",
    "verdict": "clean",
    "acknowledged": false,
    "waived": false
  }
}
```

Relevant `mission report --json` receipt fields for `m64f6bfc5ae64`:

```json
{
  "mission_id": "m64f6bfc5ae64",
  "finish": "verified",
  "gap_review": {
    "acknowledged": false,
    "attempts": 2,
    "failure_receipt": null,
    "fresh": true,
    "gaps": {"blocking":0,"major":0,"minor":0},
    "judged_sha": "41d47fcf7d3cd4102a9b8b4790b555c5f5c67dcb",
    "role": "gap-reviewer",
    "verdict": "clean",
    "waived": false
  },
  "opencode_receipts": [
    {
      "role": "gap-reviewer",
      "prompt": "gap_review",
      "generation": "current",
      "outcome": "failed",
      "failure_code": "runtime.native_state_limit",
      "effective_runtime_configuration": {
        "requested_model": "opencode/big-pickle",
        "applied_model": "opencode/big-pickle",
        "model_confirmation": "observed",
        "requested_mode": "build",
        "applied_mode": "build",
        "mode_confirmation": "observed"
      },
      "runtime_usage": {
        "status": "reported",
        "usage": {
          "input_tokens": 434,
          "output_tokens": 165,
          "reasoning_tokens": 8,
          "total_tokens": 14687,
          "context_used_tokens": 14514,
          "context_window_tokens": 200000,
          "cost": {"amount":"0","currency":"USD","scope":"session_cumulative"}
        }
      }
    },
    {
      "role": "implementer",
      "prompt": "execution",
      "generation": "superseded",
      "outcome": "failed",
      "failure_code": "runtime.native_state_limit",
      "effective_runtime_configuration": {
        "requested_model": "opencode/big-pickle",
        "applied_model": "opencode/big-pickle",
        "model_confirmation": "observed",
        "requested_mode": "build",
        "applied_mode": "build",
        "mode_confirmation": "observed"
      },
      "runtime_usage": {
        "status": "reported",
        "usage": {
          "input_tokens": 125,
          "output_tokens": 39,
          "reasoning_tokens": 7,
          "total_tokens": 10475,
          "context_used_tokens": 10429,
          "context_window_tokens": 200000,
          "cost": {"amount":"0","currency":"USD","scope":"session_cumulative"}
        }
      }
    }
  ],
  "codex_success_receipts": [
    {
      "role": "gap-reviewer",
      "prompt": "gap_review",
      "outcome": "succeeded",
      "effective_runtime_configuration": {
        "requested_model": null,
        "applied_model": null,
        "model_confirmation": null,
        "requested_mode": null,
        "applied_mode": null,
        "mode_confirmation": null
      },
      "runtime_usage": {"status":"not_reported"}
    },
    {
      "role": "implementer-codex-clean",
      "prompt": "execution",
      "outcome": "succeeded",
      "effective_runtime_configuration": {
        "requested_model": null,
        "applied_model": null,
        "model_confirmation": null,
        "requested_mode": null,
        "applied_mode": null,
        "mode_confirmation": null
      },
      "runtime_usage": {"status":"not_reported"}
    }
  ]
}
```

Exact event log for `m64f6bfc5ae64`:

```text
   1 mission_created
   2 team_configured
   3 team_configured
   4 team_configured
   5 team_configured
   6 proposal_recorded
   7 decision_recorded
   8 team_configured
   9 role_turn_requested
  10 role_turn_completed
  11 team_configured
  12 decision_recorded
  13 role_turn_requested
  14 role_turn_completed
  15 team_configured
  16 team_configured
  17 decision_recorded
  18 role_turn_requested
  19 role_turn_completed
  20 decision_recorded
  21 team_configured
  22 proposal_recorded
  23 decision_recorded
  24 team_configured
  25 role_turn_requested
  26 role_turn_completed
  27 oracle_run_requested
  28 oracle_run_requested
  29 oracle_run_requested
  30 oracle_run_requested
  31 oracle_run_completed
  32 oracle_run_completed
  33 oracle_run_completed
  34 oracle_run_completed
  35 role_turn_requested
  36 role_turn_completed
  37 team_configured
  38 decision_recorded
  39 role_turn_requested
  40 role_turn_completed
  41 mission_finished
  42 result_applied
```

## Required Impact Statements

- Security impact: observation and usage reporting add no new authority, no
  secret access, no egress expansion, and no sandbox widening. ACP
  configuration drift now fails closed instead of projecting a requested value
  as observed. Usage is only runtime-reported data or explicit absence.
- API/event contract impact: `SCHEMA_VERSION` advanced from 26 to 27 for
  runtime usage in role outcomes/failures/receipts and the corresponding
  report/evidence surfaces. `REDUCER_VERSION` advanced from 52 to 53 so replay
  preserves the new usage fields. The RoboRev follow-up did not add another
  wire or replay change.
- Docs impact: no product-facing command path changed. The changed receipt and
  report surfaces are self-describing through their JSON fields and evidence
  rendering; no raw HTTP or everyday command documentation changed.
