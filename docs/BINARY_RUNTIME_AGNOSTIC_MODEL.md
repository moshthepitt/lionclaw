# LionClaw Binary + Runtime-Agnostic Model (Pinned Decisions)

Date pinned: 2026-02-27

This document captures decisions we agreed to preserve while building LionClaw.

## Why this exists

LionClaw should behave like a real installed product.

Not a repo ritual. Not a source-checkout trick. Not a one-runtime wrapper pretending to be a system.

## Non-negotiable decisions

1. LionClaw is runtime-agnostic. `codex`, `claude-code`, `opencode`, and future runtimes are adapter choices, not product identity.
2. Everything beyond the small core is a skill. Channels are one important case, not the exception.
3. Runtime is selected at invocation (`lionclaw run` or `lionclaw service up`), not during `lionclaw channel add`.
4. Identity/persona is runtime-independent and comes from assistant-home workspace files plus small hot continuity files (`IDENTITY.md`, `SOUL.md`, `AGENTS.md`, `USER.md`, `MEMORY.md`, `continuity/ACTIVE.md`).
5. Anthropic `SKILL.md` is the skill instruction standard.
6. Security controls live in kernel policy, sandboxing, and audit, never in prompt-only logic.

## Installed layout (no repo required)

LionClaw uses `~/.lionclaw` as canonical state:

- `~/.lionclaw/db/lionclaw.db`
- `~/.lionclaw/config/lionclaw.toml` (or equivalent)
- `~/.lionclaw/workspaces/<agent-id>/` for identity/bootstrap docs
- `~/.lionclaw/skills/<skill-id>@<hash>/` installed immutable skill snapshots
- `~/.lionclaw/logs/`
- `~/.lionclaw/services/` generated service manifests (systemd/launchd/etc.)

No runtime flow should depend on repository-relative paths.

## Home model (pinned terms)

LionClaw uses the word "home" in three different layers. Keep them separate.

### 1. Instance home

`LIONCLAW_HOME` is the installation and state root for one LionClaw instance.

It owns:

- SQLite database
- operator config
- installed skill snapshots
- runtime cache artifacts
- logs
- generated service files
- machine-owned `config/home-id`

This is infrastructure state. It is not the assistant's personality, memory,
or delivery surface.

### 2. Assistant home workspace

`~/.lionclaw/workspaces/main/` is the default assistant home workspace.

This is distinct from the project/task root that LionClaw mounts into confined
runtimes at `/workspace`. By default, local `lionclaw run` uses the current
working directory as that project root.

This is the assistant's durable life context. It is where LionClaw keeps the
runtime-independent identity files that shape prompt assembly and future
assistant continuity:

- `IDENTITY.md`
- `SOUL.md`
- `AGENTS.md`
- `USER.md`

These files describe who LionClaw is, how it should behave, and who it serves.
They are product context, not capability grants.

External repos, project directories, or other local trees are not the same
thing as the assistant home workspace. They may be attached as explicit local
context for a session or skill, but they should not replace the assistant's
stable home context.

### 3. Home channel

LionClaw should eventually have one explicit default return path for proactive
assistant output: the home channel.

Conceptually this is a `{channel_id, peer_id}` pair used as the assistant's
default delivery destination for background work. It is a product-level anchor,
not a transport implementation detail.

Important: this is a pinned design direction, not a first-class user-facing
feature yet. Current delivery remains explicit per channel interaction or per
scheduled job configuration.

## Home model invariants

1. `LIONCLAW_HOME` remains machine-owned installation state.
2. `workspaces/main` is the default assistant home workspace unless explicitly
   overridden by future product features.
3. Assistant identity lives in workspace files, not in runtime-specific
   configuration.
4. `SOUL.md` shapes tone and stance, but never overrides kernel policy.
5. Channels remain external skills even when one of them becomes the configured
   home channel.
6. The assistant must have one clear default place to return results, but that
   default should be explicit rather than inferred from "last contact".

## Identity and prompt envelope

Per turn, LionClaw composes a runtime-agnostic prompt envelope from:

1. kernel safety/system sections,
2. assistant-home identity and hot continuity files (`IDENTITY.md`, `SOUL.md`, `AGENTS.md`, `USER.md`, `MEMORY.md`, `continuity/ACTIVE.md`),
3. selected skill context (`SKILL.md` source from installed snapshots),
4. current user/channel input.

Brokered filesystem actions may target a separate project/task root. Prompt identity and continuity do not follow that root.

Adapters receive the assembled envelope; they do not own persona.

## Skill injection model (inspired by skills-inject / vercel-labs ideas)

1. Source of truth is installed snapshots in `~/.lionclaw/skills/...`.
2. Injection output is derived and idempotent (marker-based replace blocks).
3. Injected files are cache artifacts under `~/.lionclaw/runtime/` (or similar), not authoritative state.
4. Injection can improve recall/ergonomics but cannot grant permissions or bypass policy.

For confined runtimes, LionClaw treats that runtime area as private scratch space rather than continuity:

- `/runtime` is the runtime-private writable root.
- `/drafts` is the runtime-private draft/output area.
- the planner points `HOME` and XDG state under `/runtime` so engine-specific caches and config stay out of assistant continuity.
- LionClaw scans `/drafts` on demand when the user lists outputs; keep/discard actions then move or delete validated files directly from that shared host/container directory.
- only the `answer` lane is canonical reply content; `reasoning` is an optional live stream for channels that choose to render it.

## Process and service model

Default background deployment:

1. one `lionclawd` process,
2. one worker process per active channel account (or strict trust boundary).

This may mean many processes (example: 13 channels plus the background service). That is acceptable for isolation.

From the operator side, LionClaw has one normal interactive path plus explicit service commands:

- `lionclaw run [runtime]` (interactive local use)
- `lionclaw run --continue-last-session [runtime]` (resume the latest local interactive session for the current project)
- `lionclaw apply` (reconcile desired state)
- `lionclaw service up` (start supervised stack + ensure auto-restart policy)
- `lionclaw service down`
- `lionclaw service status`
- `lionclaw service logs`

Background operation is explicit. If you want long-running channels and auto-restart, LionClaw uses the platform service manager for that job.

## CLI UX target

Normal user flow:

1. `lionclaw onboard`
2. `podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .`
3. `lionclaw runtime add codex --kind codex --bin codex --image lionclaw-runtime:v1`
4. `lionclaw run codex`

Runtime definitions, execution presets, and confinement settings live in
`~/.lionclaw/config/lionclaw.toml`, not in ad hoc shell configuration.
For now, runtime network policy is intentionally coarse:
`network-mode = "on"` or `network-mode = "none"`. `on` means the runtime gets a
private container network, not host networking.

Runtime secrets live separately in `~/.lionclaw/config/runtime-secrets.env`.
Presets either mount that whole file or mount no runtime secrets at all with
`mount-runtime-secrets = true|false`, and LionClaw mounts it read-only under
`/run/secrets/` with a LionClaw-managed name that starts with
`lionclaw-runtime-secrets-` inside the confined runtime. LionClaw hardens that
file to owner-only permissions on Unix before loading it.

Host-only runtime auth comes from the host Codex login itself. Today the
confined Codex path reads the host Codex auth store, normally
`~/.codex/auth.json`, preflights it before launch, starts a short-lived
private Podman pod with a tiny HAProxy sidecar, injects a runtime-specific
one-time placeholder token into the runtime container, and swaps that
placeholder for the discovered host bearer only inside the sidecar on
`POST /responses`. Codex talks to the sidecar over pod-local loopback, and
LionClaw routes that traffic to either `api.openai.com/v1` or
`chatgpt.com/backend-api/codex` depending on how the host Codex login is
authenticated. The raw host auth never enters the runtime container and the
runtime does not need host loopback reachability. Interactive local runs honor
`CODEX_HOME` when it is explicitly set in the current shell; background
services currently use the default host Codex home. If the host Codex login is
missing, the fix is `codex login`, not a separate LionClaw auth file. The
HAProxy sidecar runs from a LionClaw-managed version-pinned official image
reference, while the
runtime itself runs from the operator-configured runtime image. LionClaw
preflights the runtime image and auto-pulls the version-pinned sidecar image
reference when it is
missing. The
shared OCI runtime image definition lives in
`containers/runtime/Containerfile` and currently installs `codex` and
`opencode`. LionClaw runtime compatibility assumes configured runtime image
references are treated as immutable; rebuild under a new image tag when runtime
bits change.

Inside `lionclaw run`, recovery stays command-first:

- `/continue`
- `/retry`
- `/reset`
- `/exit`

The core keeps durable per-turn history, preserves partial assistant output across timeouts and restart interruption, and reopens the latest local session for the current project by most recent activity.
Interactive runtime continuity now has two layers:

- LionClaw keeps the durable session transcript, audit trail, and artifacts.
- The harness keeps resumable runtime-private state under a session-scoped
  `/runtime` mount that is partitioned by project root and security shape.

LionClaw still spawns a fresh confined process for each interactive turn, but
Codex/OpenCode resume their own prior session state from that mounted runtime
root. LionClaw only replays the full transcript into the prompt when the
harness session is fresh; resumed harness sessions receive the new user input
plus a continuation note instead of the entire prior transcript every turn.

Background channels remain an explicit service flow:

1. `lionclaw skill add <source>`
2. `lionclaw channel add telegram`
3. `lionclaw service up --runtime codex`
4. `lionclaw channel pairing list|approve|block`

Interactive channel skills stay explicit too:

1. `lionclaw skill add skills/channel-terminal --alias terminal`
2. `lionclaw channel add terminal --launch interactive`
3. `lionclaw channel attach terminal`

Interactive channels are foreground-only. They attach to the current TTY, restore the latest durable interactive session for that peer within the current project scope, resume any still-running answer stream from the last durable checkpoint, and are not managed by `lionclaw service up`.

No manual API choreography should be required for normal usage or operator flows.

## Implementation checklist anchor

- [x] Add `lionclaw` onboarding and declarative state reconciliation.
- [x] Move skill installs to canonical snapshot store under `~/.lionclaw/skills`.
- [x] Remove repo-path assumptions from worker resolution.
- [x] Add workspace identity bootstrap templates in `~/.lionclaw/workspaces/main/`.
- [x] Add runtime selection at invocation with default/global routing.
- [x] Add supervisor/service generation with restart policies.
- [x] Add `lionclaw` pairing and channel health workflows.
- [x] Add marker-based skill injection cache as non-authoritative derived output.
