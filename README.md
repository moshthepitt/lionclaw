# LionClaw

LionClaw is a secure-first local agent kernel that lets you run your preferred CLI runtime (for example `codex` or `opencode`) behind one policy- and audit-enforced control plane.

## Why it exists

Most agent stacks are either feature-heavy or loosely enforced.
LionClaw takes the opposite path:

- Small, auditable kernel
- Default-deny capability checks
- Channels as installable skills (not hardcoded core features)
- Runtime-agnostic adapters so you can swap agent CLIs without changing channel workers

## What it does

LionClaw provides:

- Session and turn orchestration
- Skill install/enable state (`SKILL.md` compatible)
- Policy checks for privileged actions
- Append-only audit events
- Channel bridge APIs (`inbound` and `outbox` for external workers)
- Runtime adapter interface with current adapters: `mock`, `codex`, `opencode`

## Quick start

Prerequisites:

- Rust toolchain
- A runtime CLI installed (`codex` and/or `opencode`)
- `curl` and `jq` for API/script usage

Start kernel:

```bash
cargo run
```

Default endpoint: `http://127.0.0.1:3000`

Health check:

```bash
curl -sS http://127.0.0.1:3000/health | jq
```

## How to use

1. Install and enable a channel skill.
2. Bind that skill to a channel ID and runtime.
3. Run the channel worker (external process) for that skill.
4. Send messages through the channel; LionClaw routes turns to the configured runtime.

Fast path for Telegram-style channel skills:

```bash
./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --runtime-id codex
```

Start worker in the same command:

```bash
TELEGRAM_BOT_TOKEN=... ./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --runtime-id codex \
  --start-worker
```

## Runtime config

Codex adapter options:

- `LIONCLAW_CODEX_BIN` (default `codex`)
- `LIONCLAW_CODEX_MODEL` (optional)
- `LIONCLAW_CODEX_SANDBOX` (default `read-only`)

OpenCode adapter options:

- `LIONCLAW_OPENCODE_BIN` (default `opencode`)
- `LIONCLAW_OPENCODE_MODEL` (optional)
- `LIONCLAW_OPENCODE_AGENT` (optional)

General options:

- `LIONCLAW_DB_PATH` (default `./lionclaw.db`)
- `LIONCLAW_RUNTIME_TURN_TIMEOUT_MS` (default `120000`)

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [V0 Plan](docs/V0_PLAN.md)
- [Roadmap](docs/ROADMAP.md)
- [Scripts](scripts/README.md)
