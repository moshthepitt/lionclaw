# LionClaw

LionClaw is a local AI CLI for people who want power without handing the whole machine to a sprawling agent stack.

## Why it exists

Most agent tools make the wrong tradeoff.

They either grow into sprawling always-on systems you do not understand, or they stay thin and leave the model sitting too close to your machine.

LionClaw takes a harder line. Keep the trusted core small. Put policy, audit, state, and runtime control there. Push everything else outward.

That is the point of LionClaw:

- one CLI you actually use
- one small core you can reason about
- everything beyond that core installed as a skill
- background operation turned on only when you ask for it

Channels are skills. Future capabilities are skills. The core stays small on purpose.

## What it does

LionClaw provides:

- one local entrypoint for interactive AI work through `lionclaw run`
- immutable installed skills under `~/.lionclaw/skills`
- workspace identity files under `~/.lionclaw/workspaces/main`
- durable state for sessions, policy, audit, and channels
- multiple runtime adapters behind one CLI
- explicit service management for background channels and automation

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

When you want a real channel instead of the direct CLI path, install one as a skill.

For a local end-to-end test without Telegram, use the terminal channel skill:

```bash
./target/debug/lionclaw skill add skills/channel-terminal --alias terminal
./target/debug/lionclaw channel add terminal --launch interactive
./target/debug/lionclaw channel attach terminal --runtime codex
```

`channel attach` opens the worker in your current TTY. If needed, it starts LionClaw for you, begins at the current stream head, and prints the pairing code and approval command on first contact.

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

When you want channels or automation running in the background, use service mode:

```bash
TELEGRAM_BOT_TOKEN=... ./target/debug/lionclaw service up --runtime codex
```

Then inspect or manage it with:

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
