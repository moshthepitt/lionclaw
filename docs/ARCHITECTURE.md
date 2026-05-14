# LionClaw Architecture

LionClaw is a secure-first Claw: a small trusted kernel that turns real agent
CLIs into a persistent local assistant.

The agent does the reasoning and tool use. LionClaw gives it the local
contract around that work: sessions, channels, scheduled jobs, continuity,
runtime configuration, confinement, policy, and audit.

LionClaw currently targets Unix-like systems only. The direct `lionclaw run`
path is designed for Linux/macOS-style Unix environments. Managed background
paths, including `lionclaw up` and channel auto-start, currently use the
systemd user manager; launchd support is a future portability item.

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
- runtime launch plans
- channel pairing, scoped grants, inbound queues, outbound streams
- scheduler definitions and run records
- policy grants and audit events
- assistant-home continuity files and derived search index
- the immutable applied runtime view loaded at startup from channel config and filesystem-installed skills

Kernel-owned mutations are policy checked where privileged and audited where
security or operator visibility matters.

### Runtime-Owned Work

Program-backed runtimes own their internal tool loops. The selected runtime can
use its native tools inside the boundary LionClaw gives it. LionClaw does not
mediate every private step inside those harnesses.

Instead, LionClaw constrains the runtime launch:

- selected work root mounted at `/workspace`
- runtime-private state mounted at `/runtime`
- draft/output area mounted at `/drafts`
- applied non-channel skill snapshots mounted read-only at `/lionclaw/skills/<alias>`
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
- `kernel.skills`: skill alias validation and installed-skill metadata helpers.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.jobs`: scheduled job definitions, run records, and SQLite persistence.
- `kernel.capability_broker`: explicit brokered capability execution for direct runtimes and narrow kernel surfaces.
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.runtime.execution`: execution presets, plan compilation, OCI backend, and process execution.
- `kernel.scheduler`: due-job claiming, lease coordination, retry, and dispatch.
- `kernel.channel_state`: channel pairing requests, scoped grants, normalized inbound event admission, queued turns, outbound stream state, and transcript history.
- `kernel.continuity`: assistant-home continuity files, `ACTIVE.md` projection, daily notes, artifacts, open loops, proposals, and retrieval helpers.
- `kernel.continuity_fs`: descriptor-rooted Unix filesystem helper for assistant-home continuity.
- `kernel.session_compactions`: persisted transcript compaction summaries and ranges.
- `kernel.audit`: append-only audit event log persisted in SQLite.

## Runtime Adapter Contract

Runtime adapters implement:

- `info()`
- `session_start()`
- `turn()`
- `program_backed_turn()`
- `runtime_control()`
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
4. The execution planner resolves the runtime profile, preset, work root,
   runtime state root, drafts root, network mode, secret mount decision, image,
   timeouts, and compatibility key.
5. The kernel audits `runtime.plan.allow` or `runtime.plan.deny`.
6. The OCI backend launches the runtime in the confined layout.
7. The adapter maps runtime output into typed stream events. Codex uses its
   native `app-server` JSON-RPC protocol over stdio inside the confined
   process; OpenCode uses its configured machine-readable run output.
8. The kernel persists canonical answer text, turn status, checkpoints, audit,
   and any continuity changes it owns.

Program-backed runtimes stream two message lanes:

- `answer`: canonical assistant reply text persisted into turn history
- `reasoning`: optional live thought/progress text that channels may render or ignore

Only `answer` is treated as the durable assistant reply.
Channel streams also emit a kernel-owned `turn_completed` event after the turn
record is finalized. Its `text` field is the canonical persisted assistant
reply and lets channel UIs reconcile live deltas against durable turn state
before the terminal `done` marker.

Configured OpenCode profiles are pinned to machine-readable JSON output so
LionClaw receives typed events instead of a degraded plain-text stream. Codex
is launched through its app-server protocol with `externalSandbox` permissions
inside the outer Podman boundary. LionClaw does not use `codex exec` as a
fallback path. Codex app-server request/notification assumptions are pinned by
checked-in protocol fixtures under `tests/fixtures/codex_app_server`, including
the target Codex CLI version and immutable source commit; update those fixtures
with the adapter when the target app-server contract changes.

## Runtime Control Commands

The first column is command space. `lionclaw run` and channel inbound routing
reserve `/lionclaw ...` for LionClaw-owned controls such as
`/lionclaw retry`, `/lionclaw reset`, and `/lionclaw exit`. Local-only controls
such as `/lionclaw exit` are acknowledged by channel routing but do not exit a
channel worker.

Other first-column slash commands are classified as runtime controls and are
persisted as `runtime_control` turns. The kernel records
`runtime.control.route`, `runtime.control.start`, `runtime.control.finish`, and
`runtime.control.outcome` audit events around those turns. Runtime adapters
decide whether a control is handled, unsupported, interactive-only, or failed.

This keeps native runtime commands such as Codex `/model`, `/rename`, and
`/compact` native to the selected runtime without teaching the kernel
runtime-specific command semantics. Leading-space slash input and path-like
slash input remain ordinary prompts.

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

- `/workspace`: selected work root with preset-controlled read-only or read-write access
- `/runtime`: runtime-private writable state root
- `/drafts`: runtime-private draft/output area
- `/lionclaw/skills/<alias>`: installed non-channel skill snapshot assets mounted read-only

For local `lionclaw run`, target resolution selects one project instance and
uses that instance's recorded work root. The work root is mounted at
`/workspace`. The instance home remains LionClaw's state root and is not the
project tree or work root.

The planner injects runtime-private environment defaults such as
`HOME=/runtime/home`, `LIONCLAW_DRAFTS_DIR=/drafts`, and
`LIONCLAW_SKILLS_DIR=/lionclaw/skills` when runtime-visible skills have mounted
assets, so engine-specific caches and config stay out of assistant continuity.

Interactive program-backed turns launch a fresh confined process for each
request, but the mounted `/runtime` state root is scoped to the LionClaw
session, work root, and execution security shape. That lets the harness
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
plane exists. On rootless hosts, `on` also requires the container engine to be
able to stand up its private network namespace. LionClaw preflights that host
capability before interactive or managed-background startup.

Runtime secrets are loaded from the selected instance home's
`config/runtime-secrets.env`.
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
`lionclaw up` persists that same override into the managed daemon environment
for background jobs and channels.

## API Contracts

Raw HTTP is for workers, tests, and debugging. Product-facing docs should lead
with the CLI.

### Session

- `POST /v0/sessions/open`
- `GET /v0/sessions/latest`
- `POST /v0/sessions/history`
- `POST /v0/sessions/action`
- `POST /v0/sessions/turn`

### Channel

- `GET /v0/channels/list`
- `GET /v0/channels/pairing` (pairing requests and current grant state)
- `POST /v0/channels/pairing/approve`
- `POST /v0/channels/pairing/block`
- `POST /v0/channels/grants/revoke`
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
   a deterministic `(channel_id, session_key)`.
2. `POST /v0/channels/inbound` submits normalized inbound facts. Approved
   grants queue a channel turn and receive an explicit outcome.
3. `POST /v0/sessions/action` starts `continue_last_partial`,
   `retry_last_turn`, or `reset_session` for a channel-backed session.
4. `POST /v0/channels/stream/pull` fetches typed outbound stream events for a
   consumer cursor.
5. `POST /v0/channels/stream/ack` records that a worker durably handled events
   through a sequence.
6. Pairing approve/block and grant revoke endpoints manage channel trust.

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

Stream events produced by actual runtime turns include `session_id` and
`turn_id`. The stream `peer_id` remains the provider-facing conversation ref for
delivery compatibility; internal session identity is carried by `session_key`
and the session row. Channel session keys are grant scoped: direct sessions
include the sender, conversation sessions include the conversation and sender,
and thread sessions include the conversation, thread, and sender.

## Session Continuity

`sessions.history_policy` controls how incomplete turns are reused in future
prompts:

- `interactive`: carry forward partial assistant output with an explicit marker
- `conservative`: carry forward only a structured failure note

`session_turns` is the durable source of truth for prompt history. It records:

- `kind = normal | retry | continue | runtime_control`
- `status = running | waiting_for_attachments | completed | failed | timed_out | cancelled | interrupted`
- `display_user_text`
- `prompt_user_text`
- `assistant_text`
- `error_code`
- `error_text`
- `runtime_id`

Answer-lane text is checkpointed while a turn is still running so restart
reconciliation can preserve partial replies already emitted to the user.
Channel-backed running turns also persist the exact stream sequence through
which the durable assistant checkpoint is synchronized. Channels v2 stores
normalized provider facts, including text and attachment descriptors, in
`channel_inbound_events`, admits work through scoped grants, and derives
deterministic session keys such as
`channel:<channel_id>:direct:<sender_ref>`. Worker-supplied runtime selection is
not part of the inbound channel contract; the kernel resolves runtime execution
from the instance/default runtime configuration.
Session-key components escape `:` and `%` so provider refs such as
`telegram:chat:-123` remain unambiguous.

Kernel bootstrap converts stale `running` session turns into durable
`interrupted` turns before they can be reused. Durable pending channel turns are
not interrupted on restart; bootstrap re-drives their channel workers so
accepted inbound work is recoverable after commit.

## Assistant Continuity

Continuity lives under the assistant home workspace inside the selected
instance home at `workspaces/<daemon.workspace>/`.

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

Scheduled jobs invoke the selected runtime with explicit job context and the
daemon's current runtime-visible skill set from applied filesystem state.
Optional delivery sends the final result through the existing channel
stream/outbox path without changing the latest interactive session for that
peer.

Paused jobs are skipped by normal scheduler ticks but can still be run
manually by the operator.

## Operator Launch Model

- Channel skills declare `lionclaw.toml` metadata: channel id, launch mode,
  worker entrypoint, and required env names. The v1 metadata contract is small
  by design and does not claim permissions LionClaw does not enforce.
- `launch=background`: the channel worker is supervised through the platform
  backend. The current implementation uses systemd user units.
- `launch=interactive`: the channel worker is foreground-only and normally
  started by `lionclaw connect <channel>` in the current terminal. The low-level
  attach path remains available for debugging.
- Required channel env is selected-instance state under `config/channels/`.
  Generated unit env may reference that private file, but generated unit env is
  not the source of truth.

Worker entrypoint resolution uses the metadata `worker` path and rejects
symlink escapes outside the skill directory.

`LIONCLAW_HOME` gets stable machine-owned `config/home-id` and managed-unit
identity state. Attach and background flows only reuse a daemon when
`/v0/daemon/info` reports the same home id, current project scope, and
daemon-compat fingerprint.
Managed systemd units are instance-scoped and carry `X-LionClaw-*` ownership
metadata so cleanup and stop operations only touch units owned by the selected
home.

## Security Posture In v0

1. Policy checks deny unless an explicit grant exists.
2. No default external channel is built into the core.
3. Runtime adapters registered by default: local `mock` only. `codex` and
   `opencode` are configured runtime profiles bound at startup.
4. Configured Codex/OpenCode profiles run through the shared execution planner
   and Podman backend, then map runtime output into kernel events.
5. Runtime idle timeout, hard timeout, and cancellation are kernel-enforced and
   audited. Local interactive runs default to a 30 minute idle timeout and a 2
   hour hard safety limit; daemon-backed work defaults to 30 minutes idle and 4
   hours hard unless env overrides are set.
6. Runtime execution policy supports per-turn working directory, idle timeout
   override, and constrained env passthrough. Configured kernel defaults are
   trusted directly; policy timeout bounds apply to explicit per-turn override
   requests.
7. Ordinary confined runtime file work stays inside mounted work-root, runtime,
   and drafts paths.
8. Kernel brokers are reserved for explicit side effects and direct-runtime
   requests. `channel.send` records metadata-only outbound entries and appends
   typed stream events. `net.egress`, `secret.request`, and `scheduler.run`
   remain broker-gated and denied until configured.
9. Audit covers API mutations, runtime plan allow/deny, runtime
   start/finish/error/timeout, channel lifecycle events, scheduler events, and
   brokered capability decisions.
10. Channel inbound is gated by scoped grants with duplicate event
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
7. Update this architecture doc and `docs/MANUAL_QA.md` if the runtime
   introduces new auth, state, or confinement behavior.
