# LionClaw Binary + Runtime-Agnostic Model (Pinned Decisions)

Date pinned: 2026-02-27

This document captures decisions we agreed to preserve while building LionClaw.

## Why this exists

LionClaw is a kernel/control plane that should work when installed as binaries on a machine that does not have this source repository.

## Non-negotiable decisions

1. LionClaw is runtime-agnostic. `codex`, `claude-code`, `opencode`, and future runtimes are adapter choices, not product identity.
2. Channels are skills. Transport workers (Telegram/Discord/etc.) live outside kernel Rust code.
3. Runtime is selected at invocation (`lionclaw run` or `lionclaw service up`), not during `lionclaw channel add`.
4. Identity/persona is kernel-owned and runtime-independent via workspace files (`IDENTITY.md`, `SOUL.md`, `AGENTS.md`, `USER.md`).
5. Anthropic `SKILL.md` stays unchanged as the skill instruction standard.
6. Security controls stay in kernel policy/sandbox/audit, never in prompt-only logic.

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

Per turn, kernel composes a runtime-agnostic prompt envelope from:

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

Default secure deployment:

1. one `lionclawd` kernel process,
2. one worker process per active channel account (or strict trust boundary).

This may mean many processes (example: 13 channels + kernel). That is acceptable for isolation.

Operationally, users get one interactive path plus explicit admin control through `lionclaw`:

- `lionclaw run [runtime]` (interactive local use)
- `lionclaw apply` (reconcile desired state)
- `lionclaw service up` (start supervised stack + ensure auto-restart policy)
- `lionclaw service down`
- `lionclaw service status`
- `lionclaw service logs`

Under the hood, LionClaw uses platform service managers (systemd --user / launchd / Windows equivalent) for restart and supervision.

## CLI UX target

Expected user flow:

1. `lionclaw onboard`
2. `lionclaw runtime add codex --kind codex --bin codex`
3. `lionclaw run codex`

Background/channel deployment remains explicit admin flow:

1. `lionclaw skill add <source>`
2. `lionclaw channel add telegram`
3. `lionclaw service up --runtime codex`
4. `lionclaw channel pairing list|approve|block`

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
