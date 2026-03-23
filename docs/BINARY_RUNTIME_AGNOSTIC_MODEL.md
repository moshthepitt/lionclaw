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
4. Identity/persona is runtime-independent and comes from workspace files (`IDENTITY.md`, `SOUL.md`, `AGENTS.md`, `USER.md`).
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

## Identity and prompt envelope

Per turn, LionClaw composes a runtime-agnostic prompt envelope from:

1. kernel safety/system sections,
2. workspace identity files (`IDENTITY.md`, `SOUL.md`, `AGENTS.md`, `USER.md`),
3. selected skill context (`SKILL.md` source from installed snapshots),
4. current user/channel input.

Adapters receive the assembled envelope; they do not own persona.

## Skill injection model (inspired by skills-inject / vercel-labs ideas)

1. Source of truth is installed snapshots in `~/.lionclaw/skills/...`.
2. Injection output is derived and idempotent (marker-based replace blocks).
3. Injected files are cache artifacts under `~/.lionclaw/runtime/` (or similar), not authoritative state.
4. Injection can improve recall/ergonomics but cannot grant permissions or bypass policy.

## Process and service model

Default background deployment:

1. one `lionclawd` process,
2. one worker process per active channel account (or strict trust boundary).

This may mean many processes (example: 13 channels plus the background service). That is acceptable for isolation.

From the operator side, LionClaw has one normal interactive path plus explicit service commands:

- `lionclaw run [runtime]` (interactive local use)
- `lionclaw run --continue-last-session [runtime]` (resume the latest local interactive session)
- `lionclaw apply` (reconcile desired state)
- `lionclaw service up` (start supervised stack + ensure auto-restart policy)
- `lionclaw service down`
- `lionclaw service status`
- `lionclaw service logs`

Background operation is explicit. If you want long-running channels and auto-restart, LionClaw uses the platform service manager for that job.

## CLI UX target

Normal user flow:

1. `lionclaw onboard`
2. `lionclaw runtime add codex --kind codex --bin codex`
3. `lionclaw run codex`

Inside `lionclaw run`, recovery stays command-first:

- `/continue`
- `/retry`
- `/reset`
- `/exit`

The core keeps durable per-turn history, preserves partial assistant output across timeouts and restart interruption, and reopens the latest local session by most recent activity.

Background channels remain an explicit service flow:

1. `lionclaw skill add <source>`
2. `lionclaw channel add telegram`
3. `lionclaw service up --runtime codex`
4. `lionclaw channel pairing list|approve|block`

Interactive channel skills stay explicit too:

1. `lionclaw skill add skills/channel-terminal --alias terminal`
2. `lionclaw channel add terminal --launch interactive`
3. `lionclaw channel attach terminal`

Interactive channels are foreground-only. They attach to the current TTY, restore the latest durable interactive session for that peer, resume any still-running answer stream from the last durable checkpoint, and are not managed by `lionclaw service up`.

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
