# LionClaw

LionClaw is a secure-first local agent kernel with one canonical user path:

- `lionclaw onboard`
- `lionclaw run [runtime]`

`lionclawd` still exists for supervised background operation, but normal interactive use goes through `lionclaw`, not raw HTTP or direct runtime CLI invocations.

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
- A kernel-backed local REPL via `lionclaw run`
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
./target/debug/lionclaw runtime add codex --kind codex --bin /abs/path/to/codex
```

Use LionClaw interactively:

```bash
./target/debug/lionclaw run codex
```

## Channels and background mode

For a local end-to-end channel test without Telegram, use the terminal channel skill:

```bash
./target/debug/lionclaw skill add skills/channel-terminal --alias terminal
./target/debug/lionclaw channel add terminal
./target/debug/lionclaw service up --runtime codex
./skills/channel-terminal/scripts/worker.sh
```

The terminal worker will print the pairing code and approval command on first contact. To run multiple local terminal channels at once, start separate worker processes with different `LIONCLAW_CHANNEL_ID` values.

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

Compatibility env vars still exist for low-level/manual use and temporary migration:

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
