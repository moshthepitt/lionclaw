# LionClaw Architecture

LionClaw is a secure-first Claw: a small trusted kernel that turns real agent
CLIs into a persistent local assistant.

The agent does the reasoning and tool use. LionClaw gives it the local
contract around that work: sessions, channels, scheduled jobs, continuity,
runtime configuration, confinement, policy, and audit.

LionClaw currently targets Unix-like systems only. The direct `lionclaw run`
path is designed for Linux/macOS-style Unix environments. Managed daemon paths,
including service mode and channel auto-start, currently use systemd user
services; launchd support is a future portability item.

## System Shape

```text
operator / channel / scheduler
        |
        v
  LionClaw kernel
        |
        | compiles runtime plan
        v
  confined agent runtime
  /workspace  /runtime  /drafts
```

The selected runtime may be a full agent harness such as Codex or OpenCode.
Those runtimes bring their own tool loops. LionClaw controls the outer
contract: where they run, what they can see, what network they get, what
secrets are mounted, which session invoked them, and which kernel decisions are
recorded.

## Trust Boundary

LionClaw splits responsibility into three classes.

### Kernel-Owned State

The Rust kernel owns:

- sessions and turn history
- runtime profiles and execution presets
- runtime launch plans
- channel bindings, peer approval, inbound queues, outbound streams
- scheduler definitions and run records
- installed skill snapshots
- assistant-home continuity files and derived search index
- policy grants and audit events

Kernel-owned mutations are policy checked where privileged and audited where
security or operator visibility matters.

### Runtime-Owned Work

Program-backed runtimes own their internal tool loops. The selected runtime can
use its native tools inside the boundary LionClaw gives it. LionClaw does not
mediate every private step inside those harnesses.

Instead, LionClaw constrains the runtime launch:

- project root mounted at `/workspace`
- runtime-private state mounted at `/runtime`
- draft/output area mounted at `/drafts`
- network mode chosen by preset
- runtime secrets mounted only when preset allows it
- runtime auth staged into the runtime-private home
- timeout and cancellation enforced by the kernel
- OCI image identity included in compatibility decisions

### Skill-Owned Edges

Skills are installable packages of instructions, channel workers, and
integration logic. Channels are skills. Telegram, terminal UI, and future
transports stay outside the trusted Rust core and integrate through kernel
APIs.

Skill text can influence prompt context. It cannot grant permissions.

## Kernel Modules

- `kernel.sessions`: session lifecycle, history policy, and aggregate turn metadata.
- `kernel.session_turns`: durable per-turn history, recovery state, and partial assistant output.
- `kernel.skills`: installed skill registry and enable/disable state.
- `kernel.selector`: deterministic turn-time skill relevance selection.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.jobs`: scheduled job definitions, run records, and SQLite persistence.
- `kernel.capability_broker`: explicit brokered capability execution for direct runtimes and narrow kernel surfaces.
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.runtime.execution`: execution presets, plan compilation, OCI backend, and process execution.
- `kernel.scheduler`: due-job claiming, lease coordination, retry, and dispatch.
- `kernel.channel_state`: channel bindings, peer trust state, inbound logs, queued turns, outbound stream state, and transcript history.
- `kernel.continuity`: assistant-home continuity files, `ACTIVE.md` projection, daily notes, artifacts, open loops, proposals, and retrieval helpers.
- `kernel.continuity_fs`: descriptor-rooted Unix filesystem helper for assistant-home continuity.
- `kernel.session_compactions`: persisted transcript compaction summaries and ranges.
- `kernel.audit`: append-only audit event log persisted in SQLite.

## Runtime Adapter Contract

Runtime adapters implement:

- `info()`
- `session_start()`
- `turn()`
- `resolve_capability_requests()`
- `cancel()`
- `close()`

Adapters also declare a turn mode:

- `ProgramBacked`: LionClaw launches a real agent CLI inside the compiled
  execution plan. Codex and OpenCode are the first supported examples.
- `Direct`: the runtime may return explicit `RuntimeCapabilityRequest` items
  for the kernel to broker.

The distinction is central. Program-backed runtimes are the everyday product
path. Direct runtimes and brokered capabilities are useful for tests, narrow
workers, and future runtimes that do not bring a full harness.

## Program-Backed Runtime Flow

For `lionclaw run <runtime>`, channel turns, or scheduled jobs:

1. The caller submits a turn through the operator CLI, channel API, or scheduler.
2. The kernel opens or reuses a durable session.
3. The kernel renders the prompt envelope from identity, continuity, skills,
   history, and current input.
4. The execution planner resolves the runtime profile, preset, project root,
   runtime state root, drafts root, network mode, secret mount decision, image,
   timeouts, and compatibility key.
5. The kernel audits `runtime.plan.allow` or `runtime.plan.deny`.
6. The OCI backend launches the runtime in the confined layout.
7. The adapter maps runtime output into typed stream events.
8. The kernel persists canonical answer text, turn status, checkpoints, audit,
   and any continuity changes it owns.

Program-backed runtimes stream two message lanes:

- `answer`: canonical assistant reply text persisted into turn history
- `reasoning`: optional live thought/progress text that channels may render or ignore

Only `answer` is treated as the durable assistant reply.

Configured OpenCode profiles are pinned to machine-readable JSON output so
LionClaw receives typed events instead of a degraded plain-text stream. Codex
is launched through its program-backed adapter and official external-sandbox
mode inside the outer Podman boundary.

## Direct Runtime And Brokered Capability Flow

Direct runtimes may submit `RuntimeCapabilityRequest` items. Kernel flow:

1. Validate the request scope against the selected turn, job, channel, or runtime context.
2. Evaluate policy for the requested capability.
3. Execute through the kernel broker only if allowed.
4. Return a typed `RuntimeCapabilityResult`.
5. Audit request and result.

This broker is not the normal filesystem and shell path for program-backed
runtimes. It is reserved for explicit LionClaw-owned actions, direct runtimes,
narrow non-runtime surfaces, and tests.

## Execution Plan And Confined Layout

The everyday runtime layout is mount-first:

- `/workspace`: project/task root with preset-controlled read-only or read-write access
- `/runtime`: runtime-private writable state root
- `/drafts`: runtime-private draft/output area

For local `lionclaw run`, the project root defaults to the current working
directory and is mounted at `/workspace`. `LIONCLAW_HOME` remains LionClaw's
state root and is not the project tree.

The planner injects runtime-private environment defaults such as
`HOME=/runtime/home` and `LIONCLAW_DRAFTS_DIR=/drafts`, so engine-specific
caches and config stay out of assistant continuity.

Interactive program-backed turns launch a fresh confined process for each
request, but the mounted `/runtime` state root is scoped to the LionClaw
session, project root, and execution security shape. That lets the harness
resume its own conversation state across turns without sharing private runtime
state across different projects or secret/network shapes.

LionClaw keeps the canonical transcript itself. Fresh harness sessions get
replayed transcript history in the prompt envelope; resumed harness sessions
get a continuation note plus the new user input instead of the full prior
transcript on every turn.

LionClaw does not persist a separate draft registry. Draft listing scans the
shared drafts directory on demand, and explicit keep/discard actions move or
delete files from there.

## Network, Secrets, And Runtime Auth

Current runtime network policy is intentionally coarse:

- `network-mode = "on"`
- `network-mode = "none"`

`on` maps to the container engine's private network mode, not host networking.
LionClaw does not expose a fake allowlist mode before a real egress-control
plane exists.

Runtime secrets are loaded from `~/.lionclaw/config/runtime-secrets.env`.
Presets either mount that whole file or mount no runtime secrets at all with
`mount-runtime-secrets = true|false`. The Podman backend mounts it read-only
under `/run/secrets/` with a LionClaw-managed name that starts with
`lionclaw-runtime-secrets-`. LionClaw hardens the config directory to `0700`
and the runtime secret file to `0600` on Unix before loading it.

Host-only runtime auth comes from the host runtime itself. Before a confined
Codex turn, LionClaw reads the host Codex auth store, normally
`~/.codex/auth.json`, refreshes that host auth when needed, then stages
session-local copies of `auth.json` and `config.toml` under
`/runtime/home/.codex` before launch. The real host Codex home is never mounted
into the runtime container.

`lionclaw run` inherits an interactive shell's `CODEX_HOME` when set, and
`lionclaw service up` persists that same override into the managed daemon
environment for background jobs and channels.

## API Contracts

Raw HTTP is for workers, tests, and debugging. Product-facing docs should lead
with the CLI.

### Session

- `POST /v0/sessions/open`
- `GET /v0/sessions/latest`
- `POST /v0/sessions/history`
- `POST /v0/sessions/action`
- `POST /v0/sessions/turn`

### Skill

- `POST /v0/skills/install`
- `GET /v0/skills/list`
- `POST /v0/skills/enable`
- `POST /v0/skills/disable`

### Channel

- `POST /v0/channels/bind`
- `GET /v0/channels/list`
- `GET /v0/channels/peers`
- `POST /v0/channels/peers/approve`
- `POST /v0/channels/peers/block`
- `POST /v0/channels/inbound`
- `POST /v0/channels/stream/pull`
- `POST /v0/channels/stream/ack`

### Job

- `POST /v0/jobs/create`
- `GET /v0/jobs/list`
- `POST /v0/jobs/get`
- `POST /v0/jobs/pause`
- `POST /v0/jobs/resume`
- `POST /v0/jobs/run`
- `POST /v0/jobs/remove`
- `POST /v0/jobs/runs`
- `POST /v0/jobs/tick`

### Continuity

- `GET /v0/continuity/status`
- `POST /v0/continuity/get`
- `POST /v0/continuity/search`
- `POST /v0/continuity/drafts/list`
- `POST /v0/continuity/drafts/promote`
- `POST /v0/continuity/drafts/discard`
- `GET /v0/continuity/proposals`
- `POST /v0/continuity/proposals/merge`
- `POST /v0/continuity/proposals/reject`
- `GET /v0/continuity/loops`
- `POST /v0/continuity/loops/resolve`

### Policy And Audit

- `POST /v0/policy/grant`
- `POST /v0/policy/revoke`
- `GET /v0/audit/query`

### Daemon Metadata

- `GET /health`
- `GET /v0/daemon/info`

`/health` is liveness only. `/v0/daemon/info` is the typed operator-facing
metadata endpoint used to classify a listener before reusing it.

## Channel-Skill Contract

External channel skills integrate over HTTP:

1. `GET /v0/sessions/latest` restores the latest durable session snapshot for
   `(channel_id, peer_id)`.
2. `POST /v0/channels/inbound` submits normalized inbound messages. Approved
   peers queue a channel turn and receive an explicit outcome.
3. `POST /v0/sessions/action` starts `continue_last_partial`,
   `retry_last_turn`, or `reset_session` for a channel-backed session.
4. `POST /v0/channels/stream/pull` fetches typed outbound stream events for a
   consumer cursor.
5. `POST /v0/channels/stream/ack` records that a worker durably handled events
   through a sequence.
6. Peer list, approve, and block endpoints manage pairing trust.

Queued channel turns emit machine-stable status/error codes through the same
stream contract. Kernel-generated lifecycle codes include:

- `queue.queued`
- `queue.started`
- `queue.completed`
- `queue.failed`
- `runtime.started`
- `runtime.completed`
- `runtime.error`
- `runtime.timeout`

## Session Continuity

`sessions.history_policy` controls how incomplete turns are reused in future
prompts:

- `interactive`: carry forward partial assistant output with an explicit marker
- `conservative`: carry forward only a structured failure note

`session_turns` is the durable source of truth for prompt history. It records:

- `kind = normal | retry | continue`
- `status = running | completed | failed | timed_out | cancelled | interrupted`
- `display_user_text`
- `prompt_user_text`
- `assistant_text`
- `error_code`
- `error_text`
- `runtime_id`

Answer-lane text is checkpointed while a turn is still running so restart
reconciliation can preserve partial replies already emitted to the user.
Channel-backed running turns also persist the exact stream sequence through
which the durable assistant checkpoint is synchronized.

Kernel bootstrap converts stale `running` session turns into durable
`interrupted` turns before they can be reused.

## Assistant Continuity

Continuity lives under the assistant home workspace inside
`LIONCLAW_HOME/workspaces/<daemon.workspace>/`.

The assistant home workspace contains:

- `MEMORY.md`
- `continuity/ACTIVE.md`
- `continuity/daily/...`
- `continuity/open-loops/...`
- `continuity/artifacts/...`
- `continuity/proposals/memory/...`

`MEMORY.md` is prompt-loaded but human-curated in v1. `ACTIVE.md` is a
kernel-generated hot projection from deterministic state and existing
continuity files. Daily notes, artifacts, proposals, and open loops are
visible Markdown records, not hidden memory database rows.

Continuity search uses a derived SQLite FTS index in `lionclaw.db`; Markdown
files remain the canonical source of truth.

## Scheduler Model

The scheduler is kernel-owned, daemon-driven, and single-flight. A lease row
prevents duplicate or overlapping ticks.

Scheduled runs open fresh synthetic sessions:

- `channel_id = "scheduler"`
- `peer_id = "job:<job-id>"`
- `history_policy = conservative`

Scheduled jobs invoke the selected runtime with explicit job context and
optional attached skill context. Optional delivery sends the final result
through the existing channel stream/outbox path without changing the latest
interactive session for that peer.

Paused jobs are skipped by normal scheduler ticks but can still be run
manually by the operator.

## Operator Launch Model

- `launch_mode=service`: channel worker is supervised by `lionclaw service up`
  through the platform service manager. The current implementation uses systemd
  user services.
- `launch_mode=interactive`: channel worker is foreground-only and started
  with `lionclaw channel attach <id>`. If no compatible daemon is already
  running, the attach path starts the daemon through the same systemd-backed
  manager.

Worker entrypoint resolution requires `scripts/worker`.

`LIONCLAW_HOME` gets a stable machine-owned `config/home-id`. Attach and
service flows only reuse a daemon when `/v0/daemon/info` reports the same
home id, current project scope, and daemon-compat fingerprint.

## Security Posture In v0

1. Policy checks deny unless an explicit grant exists.
2. No default external channel is built into the core.
3. Runtime adapters registered by default: local `mock` only. `codex` and
   `opencode` are configured runtime profiles bound at startup.
4. Configured Codex/OpenCode profiles run through the shared execution planner
   and Podman backend, then map runtime output into kernel events.
5. Runtime idle timeout, hard timeout, and cancellation are kernel-enforced and
   audited. Local interactive runs default to a 5 minute idle timeout and a 2
   hour hard safety limit; daemon-backed work defaults to 10 minutes idle and 4
   hours hard unless env overrides are set.
6. Runtime execution policy supports per-turn working directory, idle timeout
   override, and constrained env passthrough. Configured kernel defaults are
   trusted directly; policy timeout bounds apply to explicit per-turn override
   requests.
7. Ordinary confined runtime file work stays inside mounted
   workspace/runtime/drafts paths.
8. Kernel brokers are reserved for explicit side effects and direct-runtime
   requests. `channel.send` records outbound transcript entries and appends
   typed stream events. `net.egress`, `secret.request`, and `scheduler.run`
   remain broker-gated and denied until configured.
9. Audit covers API mutations, runtime plan allow/deny, runtime
   start/finish/error/timeout, channel lifecycle events, scheduler events, and
   brokered capability decisions.
10. Channel inbound is gated by pairing approval with duplicate update
    suppression and worker-controlled polling offsets.

## Planned Hardening

- Stronger egress policy and allowlist enforcement.
- Secret broker/proxy for credentials that should not be runtime-visible.
- Skill source pinning, provenance, signatures, and update review.
- Alternative confinement backends beyond the shipped OCI path.
- Wasmtime or equivalent sandbox path for untrusted helper tools, not as the
  primary program-backed runtime path.
- Runtime image provenance and security audit reporting.

## Adding A Runtime

1. Add `kernel/runtime/adapters/<adapter>.rs` implementing `RuntimeAdapter`.
2. Choose `ProgramBacked` or `Direct` turn mode deliberately.
3. Export it from `kernel/runtime/adapters/mod.rs`.
4. Wire configured registration in `operator/runtime.rs`.
5. Only touch `kernel/runtime/builtins.rs` if the adapter is intentionally
   builtin test/kernel scaffolding.
6. Add unit tests in the adapter module plus one kernel-level integration case.
7. Update `docs/RUNTIME_MODEL.md`, this architecture doc, and manual QA notes
   if the runtime introduces new auth, state, or confinement behavior.
