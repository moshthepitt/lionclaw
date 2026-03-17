# LionClaw

LionClaw gives you one CLI to run local AI workspaces, skills, and channels:

- `lionclaw onboard`
- `lionclaw run [runtime]`

Use `lionclaw run` for normal interactive work. Use `lionclaw service ...` when you want background channels or automation.

## Why it exists

LionClaw keeps local AI setup understandable and controlled while making channels installable skills instead of built-in integrations.

- Default-deny capability policy
- Built-in audit trail
- Works with multiple runtimes through one CLI
- Binary-installable state under `~/.lionclaw`

## What it does

LionClaw provides:

- SQLite-backed sessions, skills, policy, audit, and channel state
- Immutable skill snapshots under `~/.lionclaw/skills`
- Workspace identity files under `~/.lionclaw/workspaces/main`
- Automatic runtime context from workspace identity files plus installed skill context
- A local interactive path via `lionclaw run`
- Background service management for channels via `lionclaw service ...`

## Quick start

Build both binaries:

```bash
cargo build --bins
```

Initialize local state:

```bash
./target/debug/lionclaw onboard
```

Configure a runtime profile:

```bash
./target/debug/lionclaw runtime add codex --kind codex --bin codex
```

Use LionClaw interactively:

```bash
./target/debug/lionclaw run codex
```

## Channels and background mode

For a local end-to-end channel test without Telegram, use the terminal channel skill:

```bash
./target/debug/lionclaw skill add skills/channel-terminal --alias terminal
./target/debug/lionclaw channel add terminal --launch interactive
./target/debug/lionclaw channel attach terminal --runtime codex
```

`channel attach` opens the interactive worker in your current TTY. If needed, it starts the background service for you, begins at the current stream head, and prints the pairing code and approval command on first contact.

To run multiple local terminal channels at once, register multiple interactive channels and attach each one in its own terminal:

```bash
./target/debug/lionclaw channel add terminal2 --skill terminal --launch interactive
./target/debug/lionclaw channel attach terminal2
```

Register a Telegram skill and channel:

```bash
./target/debug/lionclaw skill add skills/channel-telegram --alias telegram
./target/debug/lionclaw channel add telegram
```

Start the supervised stack for channels and automation:

```bash
TELEGRAM_BOT_TOKEN=... ./target/debug/lionclaw service up --runtime codex
```

Inspect or manage the background stack:

```bash
./target/debug/lionclaw service status
./target/debug/lionclaw channel pairing list
./target/debug/lionclaw service logs
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

Runtime profiles live in `~/.lionclaw/config/lionclaw.toml`.

These env vars are still supported for manual setup and migration:

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
