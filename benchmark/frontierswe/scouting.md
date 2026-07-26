# FrontierSWE Bring-up Snapshot

Snapshot time: 2026-07-26 (Africa/Nairobi).

## Harness and installation

The benchmark is the `Proximal-Labs/frontier-swe` repository, pinned here at
commit `8ba3afe785a0f99a78d1017127b97eef60e63b3b` (committed 2026-07-13).
Its `pyproject.toml` declares Python 3.13 or newer. Its `uv.lock` resolves the
`harbor` package from PyPI at version 0.2.0, not current Harbor main. The locked
wheel SHA-256 is
`68ad44c1940afc57c6ea8117e6e646ba811536f1a8afc4d5c1f7b47cd9ce7c31`;
the source archive SHA-256 is
`cea3c91d7ae5d3faa7c685d850a958245d32612380699b090b8ba5396d20ea41`.

The minimal compatible install used by this bundle is:

```bash
uv tool install harbor==0.2.0
```

Harbor 0.2.0 requires a working local Docker CLI and Compose provider for its
default environment. On this host `/usr/bin/docker`, `docker info`, and
`docker compose` are operational against the local rootless container store.
However, the default resource overlay fails at container start because the
user service delegates only the `pids` cgroup controller: runc cannot open
`memory.swap.max` (and a native CPU limit similarly cannot open `cpu.max`).
Rootless overlay storage quotas are also unavailable on this filesystem.

The bundle therefore uses Harbor's documented custom-environment import path
with a native Podman adapter. The adapter preserves Harbor's lifecycle,
copy/exec, mounted-log, hidden-verifier, and `network=none` contracts. It
refuses mutable images and in-harness builds. It also fails closed when
resource ceilings cannot be enforced unless this bring-up's explicit
`allow_unenforced_resources=true` exception is supplied, then records all
three unenforced ceilings as report invalidations. This is not a publishable
resource-equivalent run.

The similarly named `harbor-framework/frontier-bench` repository is not the
FrontierSWE benchmark. It is the current Frontier-Bench project. Scouting
Harbor main also showed version 0.20.0, but that is not the version locked by
the FrontierSWE snapshot and is not the integration target.

## Real task interface

FrontierSWE contains 17 Harbor task directories: five implementation, three
ML-research, and nine performance tasks. A task is fetched from the pinned Git
tree and presented to Harbor through `harbor run --path TASK_DIR`. Each task
contains `instruction.md`, `task.toml`, `environment/`, optional `solution/`,
and `tests/test.sh`.

Harbor builds or starts the task image, calls the selected agent with the task
instruction, and only after the agent returns uploads `tests/` to `/tests`.
The verifier runs `/tests/test.sh` and reads `/logs/verifier/reward.txt` (and
preserves the richer `reward.json`). The candidate for this task is
`/app/type-checker`.

There is no task-local `submit.sh` in FrontierSWE. Nothing is "submitted" by
calling an entry point from the worker. The custom LionClaw Harbor agent
downloads only `/app/type-checker`, `/app/examples`, and
`/app/instruction.md`, runs the mission against that isolated Git repository,
and uploads only the resulting `type-checker` directory. The hidden verifier,
reference implementation, corpus, workloads, and rewards are never placed in a
worker container.

The official full-sweep job uses
`harbor_ext.modal_managed:ManagedModalEnvironment` with
`include_agent_domains: true`: model API domains remain reachable while general
task internet is denied. LionClaw currently exposes a binary role-network grant,
not a destination allowlist. Its roles therefore require `network = true` to
reach the model API and receive broader egress than the official job. This
bring-up run is intentionally non-publishable; 10b must not run until an
equivalent API-only egress boundary exists. Implementing that boundary is a
kernel change and is outside this slice.

## Cheapest bring-up task

`dependent-type-checker` is the deliberate bring-up choice. It is CPU-only,
uses eight CPUs and no GPU, has a small Rust/std-only public workspace, needs
no external source checkout, and has a compact verifier bundle. Published
baseline rows also show that several agents complete it in minutes rather than
the multi-hour runs common across the suite. "Cheapest" here means expected
host/tooling cost for one full pipeline exercise, not guaranteed model cost.

The task is performance-scored but correctness-gated. LionClaw stops at
`done:attested`: a real machine verifier exists, but Harbor withholds it until
after the agent phase. Claiming `done:verified` inside LionClaw would require
exposing or duplicating hidden grader material and would violate the
worker-independent proof boundary. Harbor's post-agent reward is the benchmark
outcome recorded in the finalized mission report.

## Bring-up blocker found live

The task container, custom Harbor agent, post-agent hidden verifier, and report
finalizer ran unattended. LionClaw itself could not leave initial planning.
`crates/lionclaw-model/src/step.rs` has a planning step that dispatches the
configured `proposes-plan` role, and `scripts/mission-eval.sh` expects
`lionclaw mission advance` to drive that step. However, the CLI starts its
detached driver only for `Ready` and `CleanupBlocked`; initial planning is
reported as `AwaitingPlan`. Three `mission advance --wait` calls therefore
returned `awaiting a plan` without ever starting the planner.

Authoring or injecting a plan in the external benchmark supervisor would move
planning responsibility out of the confined mission role and hide the kernel
gap. The supervisor now fails closed when canonical `next_actions` does not
advertise `mission advance` from initial planning. Fixing the CLI dispatch
condition is a kernel change and is explicitly outside Slice 10a's allowed
lane.
