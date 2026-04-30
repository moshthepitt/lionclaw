# LionClaw Runtime Model

Date pinned: 2026-04-17

This document defines the product and architecture model behind `lionclaw run`.
It replaces the older "binary runtime agnostic" framing with the more precise
model LionClaw is built around: real agent runtimes inside a local boundary.

## The Product Bet

Claws turn agents into assistants.

A capable agent runtime can already reason, edit files, run tools, and solve
real tasks. A Claw gives that agent persistence, channels, scheduled work,
local context, credentials, and a place to live.

LionClaw is a secure-first Claw. It does not try to become the whole assistant
stack. It runs the real agent directly and controls the environment around it.

Supported runtimes should keep their own strengths instead of being flattened
into a lowest-common-denominator tool protocol. LionClaw owns the boundary,
not every thought inside it.

## Terms

### Runtime

A runtime is the agent LionClaw launches for a turn.

Today the important runtime class is a **program-backed agent runtime**: a real
CLI with its own harness, tool loop, auth model, and resumable state. Codex and
OpenCode are the first supported program-backed runtimes.

LionClaw also keeps a **direct runtime** path for tests and future adapters
where the runtime returns explicit kernel capability requests. Direct runtimes
are useful, but they are not the everyday program-backed path.

### Harness-Native Capabilities

Harness-native capabilities are actions the selected agent already knows how
to perform inside its own tool loop: shell commands, file edits, local search,
MCP tools, web access, or future runtime-specific features.

LionClaw does not rebuild those tools in Rust. It constrains the runtime with
the execution plan: mounted workspace, runtime state, drafts directory, network
mode, secrets, timeouts, resource limits, and runtime image.

### LionClaw-Owned Capabilities

LionClaw-owned capabilities are actions that mutate LionClaw state or cross
the kernel boundary: sessions, channel delivery, scheduled jobs, runtime
profile changes, skill installation, pairing approval, audit, continuity
promotion, and any explicit brokered capability request.

These actions are policy-checked and audited by the kernel.

### Skills

Skills are installable packages of instructions, channel workers, and
integration logic. Skills can project context into a runtime or run workers
outside the core. They cannot grant permissions by prompt text.

Channels are skills. Telegram, terminal UI, and future transports stay outside
the trusted Rust core.

## Non-Negotiable Decisions

1. LionClaw is runtime-agnostic, but not runtime-flattening. Different agents
   may expose different strengths.
2. `lionclaw run [runtime]` is the canonical interactive product path.
3. Runtime selection happens at invocation or service startup, not when a
   channel is installed.
4. Identity and continuity are runtime-independent and come from assistant-home
   files plus durable LionClaw session state.
5. Security controls live in kernel policy, runtime confinement, and audit,
   never in prompt-only logic.
6. Every LionClaw-owned privileged action and every runtime boundary decision
   must be auditable.
7. Harness-native actions are bounded by the runtime execution plan. LionClaw
   does not claim to observe every private runtime step unless that runtime
   exposes it.

## Installed Layout

LionClaw uses `~/.lionclaw` as canonical state:

- `~/.lionclaw/db/lionclaw.db`
- `~/.lionclaw/config/lionclaw.toml`
- `~/.lionclaw/config/runtime-secrets.env`
- `~/.lionclaw/workspaces/<workspace-id>/`
- `~/.lionclaw/skills/<alias>/`
- `~/.lionclaw/runtime/`
- `~/.lionclaw/logs/`
- `~/.lionclaw/services/`

No normal runtime flow should depend on repository-relative paths.

## The Three Homes

LionClaw uses "home" in three distinct layers.

### 1. Instance Home

`LIONCLAW_HOME` is the installation and state root for one LionClaw instance.

It owns the database, operator config, installed skill snapshots, runtime
cache artifacts, logs, generated service files, and machine-owned
`config/home-id`.

This is infrastructure state. It is not the assistant's personality, memory,
or project workspace.

### 2. Assistant Home Workspace

`~/.lionclaw/workspaces/main/` is the default assistant home workspace.

It contains the runtime-independent identity and continuity files that shape
prompt assembly:

- `IDENTITY.md`
- `SOUL.md`
- `AGENTS.md`
- `USER.md`
- `MEMORY.md`
- `continuity/ACTIVE.md`

This is the assistant's durable life context. It is distinct from the project
root mounted into a confined runtime.

### 3. Home Channel

LionClaw should eventually have one explicit default return path for proactive
assistant output: the home channel.

Conceptually this is a `{channel_id, peer_id}` pair used as the assistant's
default delivery destination for background work. This is a product-level
anchor, not a transport implementation detail. Current delivery remains
explicit per channel interaction or per scheduled job configuration.

## Prompt Envelope

Per turn, LionClaw composes a runtime-neutral prompt envelope from:

1. kernel safety and product identity sections,
2. assistant-home identity and hot continuity files,
3. current user, channel, or scheduler input,
4. transcript history or a continuation note depending on runtime state.

The runtime receives the assembled envelope. It does not own LionClaw persona,
assistant home, policy, or continuity.

Prompt text can guide the agent, but it never grants permission. Capability
grants, runtime presets, network mode, mounted secrets, and channel delivery
remain kernel decisions.

## Program-Backed Runtime Flow

The ordinary program-backed runtime path is:

1. operator, channel, or scheduler submits a turn to LionClaw,
2. LionClaw opens or reuses the durable session,
3. LionClaw builds the prompt envelope,
4. LionClaw compiles the runtime execution plan,
5. LionClaw audits the allow/deny decision for that plan,
6. LionClaw launches the selected agent CLI inside the confined runtime,
7. the agent uses its own tool loop inside that boundary,
8. LionClaw streams answer/reasoning/status events back to the caller,
9. LionClaw persists the canonical answer, turn status, audit, and continuity
   side effects it owns.

Program-backed runtimes stream two message lanes:

- `answer`: canonical assistant reply text persisted into turn history
- `reasoning`: optional live thought/progress text that channels may render or ignore

Only `answer` is treated as the durable assistant reply.
For channel consumers, the kernel emits `turn_completed` after persisting the
final turn record. That event carries the canonical assistant text so UIs can
repair any lost or partial live projection before processing `done`.
Events that are not tied to a runtime turn may omit `session_id` and `turn_id`;
those are channel-scoped notices, not resumable turn state.

## Direct Runtime And Brokered Capabilities

Direct runtimes may return `RuntimeCapabilityRequest` items to the kernel. That
flow remains useful for tests, narrow workers, and future runtimes that do not
bring their own tool harness.

In that flow:

1. the runtime submits a typed capability request,
2. the kernel validates scope and policy,
3. the kernel executes through a broker only if allowed,
4. the kernel returns a typed result,
5. request and result are audited.

This is not the primary program-backed runtime path. LionClaw should not
rebuild a full tool broker merely to impersonate capabilities a selected
runtime already has. The stronger architecture is to run the real agent under
a strong outer contract.

## Runtime Execution Plan

The execution plan is the security contract for a runtime launch.

It includes:

- runtime id and kind
- project workspace root
- runtime-private state root
- drafts root
- execution preset
- workspace access mode
- network mode
- secret mount decision
- OCI image and backend
- timeout limits
- resource limits where configured
- allowed escape classes for runtime-specific host interactions
- compatibility key, including resolved OCI image identity

The everyday confined layout is mount-first:

- `/workspace`: the current project or task root
- `/runtime`: runtime-private writable state
- `/drafts`: runtime-private draft/output area
- `/lionclaw/skills/<alias>`: installed non-channel skill snapshot assets mounted read-only

The planner injects stable runtime-private environment defaults such as
`HOME=/runtime/home`, `LIONCLAW_DRAFTS_DIR=/drafts`, and
`LIONCLAW_SKILLS_DIR=/lionclaw/skills` when runtime-visible skills have mounted
assets, so engine-specific caches and config stay out of assistant continuity.

## Runtime State And Continuation

LionClaw and the runtime keep separate state.

LionClaw owns:

- canonical session transcript,
- per-turn status,
- audit trail,
- channel delivery history,
- scheduled job run records,
- assistant home continuity files,
- drafts promotion/discard.

The runtime owns:

- its private home,
- its native resumable session data,
- agent-specific caches,
- provider-specific config staged for the confined run.

Interactive program-backed turns still launch a fresh confined process for
each request, but the mounted `/runtime` state root is scoped to the LionClaw
session, project root, and execution security shape. That lets the harness
resume its own conversation state across turns without sharing private runtime
state across different projects, networks, or secret profiles.

LionClaw still keeps the canonical transcript itself. Fresh harness sessions
receive replayed transcript history in the prompt envelope. Resumed harness
sessions receive the new user input plus a continuation note instead of the
full prior transcript on every turn.

## Runtime Auth And Secrets

Runtime secrets live separately in
`~/.lionclaw/config/runtime-secrets.env`.

Presets either mount that whole file or mount no runtime secrets at all with
`mount-runtime-secrets = true|false`. When mounted, the Podman backend mounts
it read-only under `/run/secrets/` with a LionClaw-managed name beginning with
`lionclaw-runtime-secrets-`. LionClaw hardens the config directory to `0700`
and the runtime secret file to `0600` on Unix before loading it.

Host-only runtime auth comes from the host runtime itself. For Codex, the
operator signs in with `codex login`. Before a confined Codex turn, LionClaw
reads the host Codex auth store, refreshes host auth when needed, and stages
session-local copies of `auth.json` and `config.toml` under
`/runtime/home/.codex` before launch. The real host Codex home is never mounted
into the runtime container.

Codex runs inside LionClaw's outer Podman boundary with its official
external-sandbox mode enabled, then talks upstream directly as it normally
would.

Current runtime-visible secrets are explicit mounts. The longer-term hardening
direction is a tighter secret broker/proxy for credentials that should be used
without handing raw long-lived values to the runtime.

## CLI And Service Model

Operator-facing paths:

- `lionclaw onboard`
- `lionclaw runtime add ...`
- `lionclaw run [runtime]`
- `lionclaw run --continue-last-session [runtime]`
- `lionclaw run --timeout 4h [runtime]`
- `lionclaw skill add ...`
- `lionclaw channel add ...`
- `lionclaw channel attach ...`
- `lionclaw job add|ls|show|run|pause|resume|rm`
- `lionclaw service up --runtime [runtime]`
- `lionclaw service down`
- `lionclaw service status`
- `lionclaw service logs`

`lionclaw skill add` copies a skill into `~/.lionclaw/skills/<alias>`.
`lionclaw skill rm` deletes that installed alias from disk. `lionclaw channel
add --skill <alias>` makes that alias host-only; every other installed alias
is runtime-visible by default.

Background operation is explicit. If you want long-running channels,
auto-restart, or channel attach to start the daemon for you, LionClaw uses the
platform service manager for that job. The current managed-service
implementation uses systemd user services.

Direct `lionclaw run` reads the current installed skill and channel state each
time it launches a runtime. Managed daemons read that state at startup, so
skill or channel changes take effect after the daemon is restarted or
reconciled through `lionclaw service up` or `lionclaw channel attach`.

Raw HTTP is for workers, tests, and debugging. It is not the normal operator
experience.

Runtime turns use two kernel-enforced timers. The idle timeout detects a stalled
runtime with no output; the hard timeout is the absolute safety ceiling. Local
interactive runs default to 5 minutes idle and 2 hours hard. The `--timeout`
option sets the hard ceiling for `lionclaw run`; LionClaw derives the idle timer
from that value while keeping the kernel as the source of truth.

## Current Runtime Image

The shared OCI runtime image is defined at `containers/runtime/Containerfile`.
It currently installs Codex and OpenCode plus basic CLI dependencies such as
`bash`, `git`, `openssh-client`, and `ripgrep`.

Runtime compatibility includes the resolved local OCI image identity, so
rebuilding the same stable local tag still creates a new compatibility boundary
automatically.

## Implementation Checklist Anchor

- [x] Add `lionclaw` onboarding and declarative state reconciliation.
- [x] Move skill installs to canonical snapshot store under `~/.lionclaw/skills`.
- [x] Remove repo-path assumptions from worker resolution.
- [x] Add workspace identity bootstrap templates in `~/.lionclaw/workspaces/main/`.
- [x] Add runtime selection at invocation with default/global routing.
- [x] Add program-backed Codex runtime path.
- [x] Add program-backed OpenCode runtime path.
- [x] Add rootless Podman execution backend.
- [x] Add runtime-private `/workspace`, `/runtime`, and `/drafts` layout.
- [x] Add supervisor/service generation with restart policies.
- [x] Add pairing and channel health workflows.
- [x] Add marker-based skill injection cache as non-authoritative derived output.
