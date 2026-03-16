# LionClaw

LionClaw is a secure-first local agent kernel with an operator CLI.

- `lionclaw` manages installed state, services, pairing, and channels.
- `lionclawd` runs the kernel daemon.
- Runtime adapters remain replaceable: current built-ins are `mock`, `codex`, and `opencode`.

## Why it exists

LionClaw keeps the core small and auditable while making channels installable skills instead of built-in transport code.

- Default-deny policy checks
- Kernel-owned audit trail
- Runtime-agnostic control plane
- Binary-installable state under `~/.lionclaw`

## What it does

LionClaw provides:

- SQLite-backed sessions, skills, policy, audit, and channel state
- Immutable skill snapshots under `~/.lionclaw/skills`
- Workspace identity files under `~/.lionclaw/workspaces/main`
- Prompt-envelope composition from identity files plus installed skill context
- Channel bridge APIs for external worker skills
- Linux `systemd --user` service generation through `lionclaw up`

## Quick start

Build both binaries:

```bash
cargo build --bins
```

Initialize local state:

```bash
./target/debug/lionclaw onboard
```

Register a Telegram skill and channel:

```bash
./target/debug/lionclaw skill add skills/channel-telegram --alias telegram
./target/debug/lionclaw channel add telegram
```

Apply desired state and start services with a runtime:

```bash
TELEGRAM_BOT_TOKEN=... ./target/debug/lionclaw up --runtime codex
```

Inspect the stack:

```bash
./target/debug/lionclaw status
./target/debug/lionclaw pairing list
```

## State layout

LionClaw defaults to `~/.lionclaw`:

- `db/lionclaw.db`
- `config/lionclaw.toml`
- `config/lionclaw.lock`
- `skills/<skill-id>@<hash>/`
- `workspaces/main/`
- `runtime/`
- `services/`

Override the root with `LIONCLAW_HOME`.

## Runtime config

- `LIONCLAW_DEFAULT_RUNTIME_ID`
- `LIONCLAW_CODEX_BIN`
- `LIONCLAW_CODEX_MODEL`
- `LIONCLAW_CODEX_SANDBOX`
- `LIONCLAW_OPENCODE_BIN`
- `LIONCLAW_OPENCODE_MODEL`
- `LIONCLAW_OPENCODE_AGENT`
- `LIONCLAW_RUNTIME_TURN_TIMEOUT_MS`

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Binary Model](docs/BINARY_RUNTIME_AGNOSTIC_MODEL.md)
- [Roadmap](docs/ROADMAP.md)
- [Scripts](scripts/README.md)
