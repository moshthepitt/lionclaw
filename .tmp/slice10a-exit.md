# Slice 10a Exit Note

Date: 2026-07-26 (Africa/Nairobi)

Branch: `lionclaw2-slice10a-frontierswe`

Base: `9b0f8a75ffd30fedafb108840be22c27cb99b638`

Fix-round commit: this signed commit.

## Status

The Slice 10a bring-up exit condition is met for the pipeline: one
FrontierSWE task ran unattended end to end through Harbor and LionClaw, and the
mission report records cost status, tokens, rounds, LionClaw outcome, and
Harbor outcome.

The completed rerun is intentionally not a publishable benchmark measurement.
Mission `m17365ad1f80d` reached terminal `done:unverified`, below the mission
type's `attested` stop bar, after visible independent judges found failures.
The Harbor hidden verifier then scored the submitted candidate and failed its
correctness gate. That bad score is retained as the honest bring-up result.

This fix round rebased onto `lionclaw2` at SCHEMA 32 / REDUCER 60 and reran
from a fresh mission. The earlier SCHEMA 31 run is obsolete. The fix consumed,
rather than rebuilt, Slice 9.5's restart property: the supervisor restarts the
external lead from `lionclaw mission guide` and `lionclaw mission status
--json`, and state changes are requested through a single action handoff that
the supervisor validates against advertised legal actions.

The plan is now authored by the benchmark lead from the objective and task
files when `mission plan propose` is advertised. It is not hardcoded in Python.
The supervisor drives legal actions generically: `mission plan propose`,
`mission decide`, `mission advance`, `mission send`, `mission continue`, and
`mission abort` are executed only when advertised by canonical status/guide.

## Task 0: Harbor and FrontierSWE

### Source and installation

The FrontierSWE benchmark is
`https://github.com/Proximal-Labs/frontier-swe`, pinned for this bundle at
commit `8ba3afe785a0f99a78d1017127b97eef60e63b3b` (2026-07-13).

The pinned repository requires Python 3.13 or newer and locks the PyPI
`harbor` package at version 0.2.0. The compatible minimal installation is:

```bash
uv tool install harbor==0.2.0
```

The locked Harbor artifacts are:

- wheel SHA-256:
  `68ad44c1940afc57c6ea8117e6e646ba811536f1a8afc4d5c1f7b47cd9ce7c31`
- source archive SHA-256:
  `cea3c91d7ae5d3faa7c685d850a958245d32612380699b090b8ba5396d20ea41`

Harbor 0.2.0's default local environment requires a working Docker CLI and
Compose provider. This host's Docker compatibility path works against the
rootless container store, but the task's resource overlay does not: the user
service delegates only the `pids` cgroup controller, so runc cannot write
`memory.swap.max`, a native CPU limit cannot write `cpu.max`, and rootless
overlay storage quotas are unavailable on this filesystem.

The bundle therefore implements Harbor's custom-environment contract with a
native Podman adapter. It preserves Harbor's start, copy, exec, mounted-log,
hidden-verifier, and `network=none` lifecycle. It accepts only an already-built
digest-pinned image and never builds inside Harbor or confinement. It normally
fails closed when resource ceilings cannot be enforced. This bring-up used the
explicit `allow_unenforced_resources=true` exception and recorded each missing
ceiling as an invalidation, so the result is non-publishable.

The prompt's `harbor-framework/frontier-bench` hypothesis was wrong. That
repository is the newer Frontier-Bench project, not the pinned FrontierSWE
benchmark. Harbor main was version 0.20.0 while scouting; integrating it would
also have been wrong because FrontierSWE pins Harbor 0.2.0.

### Real task interface

The pinned FrontierSWE tree has 17 Harbor task directories: five
implementation, three ML-research, and nine performance tasks.

A task is fetched from the pinned Git tree and passed to:

```bash
harbor run --path TASK_DIR \
  --agent-import-path lionclaw_frontierswe.agent:LionClawAgent
```

Each task directory contains `instruction.md`, `task.toml`, `environment/`,
optional `solution/`, and `tests/test.sh`. Harbor starts or builds the task
environment, sends the instruction to the selected agent, then uploads
`tests/` to `/tests` only after the agent returns. It runs `/tests/test.sh`,
reads `/logs/verifier/reward.txt`, and preserves the richer `reward.json`.

There is no task-local `submit.sh` in FrontierSWE. The worker does not submit
through an executable entry point. For `dependent-type-checker`, the candidate
is `/app/type-checker`. The custom LionClaw agent downloads only
`/app/type-checker`, `/app/examples`, and `/app/instruction.md`, runs the
mission in an isolated Git repository, and uploads only the candidate
`type-checker` directory. Hidden tests, reference code, workloads, and rewards
never enter a mission role's container.

The official sweep uses
`harbor_ext.modal_managed:ManagedModalEnvironment` with
`include_agent_domains: true`, allowing model API destinations while denying
general task internet. LionClaw currently has a binary role-network grant.
Roles need `network = true` for model transport and therefore receive broader
egress than the official job. The report records
`lionclaw_worker_network_broader_than_official_agent_domain_allowlist`.
Equivalent API-only egress requires a kernel boundary change and must exist
before 10b.

### Cheapest task

`dependent-type-checker` is the cheapest bring-up task found. It is CPU-only,
requests eight CPUs and no GPU, uses a small Rust standard-library-only public
workspace, needs no external source checkout, and has a compact verifier.
Published baseline rows also show several agents completing it in minutes,
unlike the suite's multi-hour tasks. This is an expected tooling/host-cost
choice, not a promise about model cost.

The mission type deliberately uses the `attested` stop bar. A real machine
oracle exists, but Harbor withholds it until the agent phase is over. Calling
the LionClaw mission `verified` would require exposing or duplicating hidden
grader material inside the worker boundary. Independent visible-code judges
and terminal gap review can attest submission readiness; Harbor's post-agent
verifier remains the benchmark authority.

## Leaderboard Snapshot

Snapshot time: 2026-07-26 (Africa/Nairobi).

| Rank | Model | Harness | Avg rank | Dominance |
| ---: | --- | --- | ---: | ---: |
| 1 | Claude Fable 5 | Claude Code | 2.47 | 89% |
| 2 | Grok 4.5 | Grok CLI | 4.09 | 78% |
| 3 | Claude Opus 4.8 | Claude Code | 4.82 | 73% |
| 4 | GLM-5.2 | Claude Code | 4.85 | 72% |
| 5 | GPT-5.5 | Codex | 5.21 | 70% |
| 6 | Claude Opus 4.7 | Claude Code | 6.47 | 61% |
| 7 | Claude Opus 4.6 | Claude Code | 7.59 | 53% |
| 8 | GPT-5.4 | Codex | 7.88 | 51% |
| 9 | Composer 2.5 | Cursor CLI | 9.65 | 38% |
| 10 | Gemini 3.1 Pro | Gemini CLI | 9.79 | 37% |
| 11 | GLM-5.1 | Claude Code | 11.00 | 29% |
| 12 | DeepSeek V4 Pro | Claude Code | 11.18 | 27% |
| 13 | Kimi K2.6 | Kimi CLI | 11.44 | 25% |
| 14 | Kimi K2.5 | Kimi CLI | 11.50 | 25% |
| 15 | Qwen3.6-Plus | Qwen Code | 12.06 | 21% |

Zenith is not a row on that public table. Intelligent Internet's 2026-06-29
post claims Mean@5 average rank 2.06 and 92% dominance for GPT-5.5 with Zenith,
versus 5.53 for its GPT-5.5/Codex comparison snapshot. The moving public
leaderboard now reports GPT-5.5/Codex at 5.21. Both values are retained rather
than substituting the current baseline into Zenith's historical claim.

The full source snapshot is committed as
`benchmark/frontierswe/leaderboard-2026-07-26.md`.

## Exact Scoring Rules

At the pinned commit, `SCORING.md` names
`scripts/score_from_reward.py` as the source of truth:

- implementation tasks score correctness;
- performance tasks score `0.5 * correctness` until correctness is exactly
  one, then `0.5 + 0.5 * speedup`;
- ML research tasks use raw reward, except `frogsgame-rl`, which divides the
  solved-board count by 500;
- `notebook-compression` scores speedup only when fully correct, otherwise
  zero;
- `libexpat-to-x86asm` uses the performance gate with uncapped speedup.

For `dependent-type-checker`, partial correctness is:

```text
(accept_passed + reject_passed) / (accept_total + reject_total)
```

If the correctness gate passes and raw reward is positive, correctness becomes
one and raw reward is the throughput speedup. The normal performance formula
then gives the leaderboard score.

Avg and Best are the mean and maximum gated scores across five trials.
Correctness X/5 counts trials with exactly 100% correctness, not partial
credit. A trial flagged by the post-hoc anti-cheat audit is zeroed. Global
average rank is mean task position, lower being better. Dominance is the
task-wise win probability against a randomly selected opponent.

## Composed Image

Exact reference used:

```text
localhost/lionclaw-frontierswe-dependent-type-checker@sha256:468d9bd5257518c41b2bc5bef57ea79bb31bc6aa219c888edaec55039031a429
```

Podman image ID:

```text
832d3fa86427ae0224cabfebd1f686445c3968e35cdddcfbe29c726e35de6c42
```

Digest-pinned base:

```text
localhost/lionclaw-runtime-dev@sha256:590c4259f72f669976690983cf1af2c7f2a2b2164eeeaa4a1dac72c76b490762
```

Rebuild on the host, never inside a mission:

```bash
python3 benchmark/frontierswe/fetch_task.py \
  --output .tmp/frontierswe-dependent-type-checker
benchmark/frontierswe/build-image.sh \
  .tmp/frontierswe-dependent-type-checker
```

The fetch verifies the pinned Git tree. The build uses ordinary Podman,
`--timestamp 0`, the digest-pinned base, and the public task workspace only.
Two builds produced the same digest above. `podman image inspect` reported
created time `1970-01-01 00:00:00 +0000 UTC`.

## End-to-End Recorded Output

Machine-readable report:

```text
.tmp/slice10a-e2e-rerun5-20260726/jobs/slice10a-dependent-type-checker/mission-report.json
```

Terminal command output from `run-one.sh`:

```text
Results written to .../.tmp/slice10a-e2e-rerun5-20260726/jobs/slice10a-dependent-type-checker/result.json
Agent: lionclaw
Trials: 1
Errors: 0
Mean: 0.000
Reward distribution: reward = 0.0 -> 1
```

Final recorded output:

```json
{
  "mission": {
    "mission_id": "m17365ad1f80d",
    "phase": "done:unverified",
    "disposition": "terminal",
    "finish": "unverified",
    "stop_bar": "attested",
    "deliverable_head": "aa83ee8908c768898a354ea4b5e2eb281f601612"
  },
  "cost": {
    "status": "not_reported",
    "currencies": {},
    "explanation": "No RuntimeUsageCost was reported by LionClaw role receipts or by the Codex benchmark lead JSONL stream. Downstream comparison must price the recorded token counters using the recorded model identity and the applicable published pricing snapshot."
  },
  "tokens": {
    "benchmark_lead": {
      "cache_write_input_tokens": 0,
      "cached_input_tokens": 1425152,
      "input_tokens": 2469373,
      "output_tokens": 13443,
      "reasoning_tokens": 2685
    },
    "combined": {
      "cached_input_tokens": 1425152,
      "input_tokens": 2469373,
      "output_tokens": 13443,
      "reasoning_tokens": 2685
    },
    "role_attempts": {}
  },
  "rounds": {
    "lead_attempts": 18,
    "role_attempts": 15,
    "by_role": {
      "benchmark-judge": 7,
      "gap-reviewer": 1,
      "implementer": 7
    }
  },
  "model_identity": {
    "benchmark_context": {
      "resolved_model": "gpt-5.6-sol",
      "resolved_model_source": "codex_config",
      "role_model_policy": "not requested by default because this Codex app-server build does not confirm the applied model in turn/start"
    },
    "benchmark_lead": {
      "runtime": "codex_exec",
      "models": ["gpt-5.6-sol"]
    },
    "role_attempts": {
      "benchmark-judge": {"runtime": "codex", "instrument_runtime": "codex", "requested_model": null, "applied_model": null},
      "gap-reviewer": {"runtime": "codex", "instrument_runtime": "codex", "requested_model": null, "applied_model": null},
      "implementer": {"runtime": "codex", "instrument_runtime": "codex", "requested_model": null, "applied_model": null}
    }
  },
  "harbor": {
    "status": "invalid",
    "outcome": "invalid",
    "task_outcome": "correctness_gate_failed",
    "correctness": 0.9764705882352941,
    "speedup": null,
    "gated_score": 0.48823529411764705,
    "harbor_reward": {
      "reward": 0.0
    },
    "trial_exception": null,
    "trial_name": "task__keA5oav",
    "accept_passed": 168,
    "accept_total": 174,
    "reject_passed": 81,
    "reject_total": 81,
    "correctness_gate_passed": false,
    "accept_failures": [
      "035_inductive_type1.sexp",
      "036_iota_eq.sexp",
      "144_iota_after_delta.sexp",
      "149_large_elim_unit.sexp",
      "150_large_elim_eq.sexp",
      "166_upoly_inductive.sexp"
    ],
    "reason": "Correctness gate failed: accept=0.966 (need >=0.99), reject=1.000 (need >=0.95)",
    "total_time_ms": 2838
  },
  "validity": {
    "publishable": false,
    "invalidation_reasons": [
      "cpus_limit_unenforced",
      "memory_mb_limit_unenforced",
      "storage_mb_limit_unenforced",
      "lionclaw_worker_network_broader_than_official_agent_domain_allowlist"
    ]
  }
}
```

Cost is `not_reported`, not estimated. The Codex lead JSONL stream reported
tokens but no cost. LionClaw role receipts also reported `runtime_usage:
not_reported`, so role token/cost counters are absent even though 15 role
attempts ran. The report records the resolved model identity (`gpt-5.6-sol`
from Codex config for the lead) and explicitly states that 10b must price the
recorded counters from the applicable published pricing snapshot unless the
runtime starts reporting `RuntimeUsageCost`.

The run was unattended between `run-one.sh` start and report finalization. No
human action occurred between start and terminal state.

## Verification

Required gates for this fix round:

```text
cargo fmt -- --check                                      PASS
cargo check --workspace                                   PASS
cargo test --workspace                                    PASS
cargo clippy --workspace --all-targets -- -D warnings     PASS
bash ./scripts/ci.sh                                      PASS
git diff --check                                          PASS
```

Pre-gate focused checks passed after the supervisor/report changes:

```text
python3 -m unittest discover -s benchmark/frontierswe/tests -v     PASS (12 tests)
python3 -m py_compile benchmark/frontierswe/lionclaw_frontierswe/*.py benchmark/frontierswe/*.py benchmark/frontierswe/tests/*.py     PASS
bash -n benchmark/frontierswe/run-one.sh benchmark/frontierswe/build-image.sh benchmark/frontierswe/install-harbor.sh     PASS
cargo build -p lionclaw     PASS
```

## Coverage Parity

Command:

```bash
git diff --name-status -M 28d7612e..HEAD -- \
  '*/tests/' 'crates/*/src/**/*tests*.rs'
```

Result:

```text
(no output)
```

The requested pathspec does not select the nested benchmark test file. A
broader explicit inventory:

```bash
git diff --name-status -M 28d7612e..HEAD -- \
  'benchmark/frontierswe/tests/**' \
  'crates/*/tests/**' \
  'crates/*/src/**/*tests*.rs'
```

returned:

```text
A	benchmark/frontierswe/tests/test_report.py
A	benchmark/frontierswe/tests/test_supervisor.py
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/tests/advisory_validator.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/environment_assignment.rs
M	crates/lionclaw/tests/fold_litmus.rs
M	crates/lionclaw/tests/parallel_writers.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/recovery.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/resume.rs
M	crates/lionclaw/tests/skill_dispatch.rs
M	crates/lionclaw/tests/skill_prompts.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

No test files were deleted or renamed. No Rust source or Rust tests were
changed by this fix round; the Rust test modifications above are inherited
from the rebased SCHEMA 32 / REDUCER 60 instrument-identity work. This fix
round added `benchmark/frontierswe/tests/test_supervisor.py` and extended
`benchmark/frontierswe/tests/test_report.py`. The Python tests cover report
usage aggregation, cumulative-cost handling, missing usage as `not_reported`,
Codex usage aliases, model identity capture, runtime-profile rendering,
validity propagation, outcome extraction, advertised-action handling, lead
prompt substitution, handoff validation, and no-progress feedback. Coverage
parity is preserved.

## Security and Contract Impact

No secrets or signing keys entered Harbor or confinement. Images are composed
only as a host activity. The agent does not receive the hidden verifier or
reference solution. The native Podman adapter disables task-container network,
uses SELinux private relabeling, refuses mutable images, and records every
resource-boundary exception.

No Rust, schema, reducer, kernel API, or event contract changed. The added
mission type, benchmark adapter, image recipe, supervisor, and report schema
are bundle/tooling surfaces. The known binary-egress mismatch remains explicit
kernel work and is recorded as a validity invalidation; it was not papered
over.

## Open Work

1. Provide a worker network boundary equivalent to FrontierSWE's official
   model-API-domain-only egress before any 10b measurement.
2. Make the host resource ceilings enforceable under rootless Podman or run on
   a host where CPU, memory, and storage ceilings are actually enforced before
   any publishable comparison.
3. Resolve Codex role runtime usage reporting if 10b needs exact runtime-side
   token/cost receipts. This run records lead tokens and model identity but the
   role receipts remain `not_reported`.
4. This task completed below the `attested` stop bar and failed Harbor's
   correctness gate. That is acceptable for Slice 10a bring-up, but not a
   publishable benchmark result.
