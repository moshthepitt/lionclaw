# LionClaw

LionClaw is a local AI CLI with one obvious daily path: `lionclaw run [runtime]`.

It keeps policy, audit, state, and runtime control in a small trusted core.
Channels, automation, and future capabilities stay outside that core as
installable skills.

## Why people use it

Local AI gets more useful as it gains memory, channels, automation, and runtime
control. Those capabilities should not all end up inside the part of the system
you have to trust.

LionClaw gives you:

- one command for everyday work
- durable local state for sessions, policy, audit, and channels
- multiple runtimes behind one CLI
- optional channels and background automation when you need them

That boundary is the point of LionClaw: keep the trusted core small, and make
everything beyond it a skill.

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

Continue the latest local session instead of starting fresh:

```bash
./target/debug/lionclaw run --continue-last-session codex
```

Inside the interactive REPL:

- `/continue` resumes from a partial timed-out, failed, cancelled, or interrupted reply
- `/retry` reruns the previous prompt
- `/reset` opens a fresh session
- `/exit` leaves the REPL

## Channels and background mode

When you want LionClaw available through a channel instead of the direct CLI
path, install a channel skill.

For a local channel in your current terminal, use the terminal channel skill:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That one command bootstraps a fresh test home on its own loopback bind, configures the terminal channel, and attaches it in your current TTY.

If you prefer the underlying manual steps, they are:

```bash
./target/debug/lionclaw skill add skills/channel-terminal --alias terminal
./target/debug/lionclaw channel add terminal --launch interactive
./target/debug/lionclaw channel attach terminal --runtime codex
```

`channel attach` opens the worker in your current TTY. If needed, it starts LionClaw for you, restores the latest interactive terminal session for that peer, resumes any still-running answer stream from the last durable checkpoint, and prints the pairing code and approval command on first contact. It only reuses a daemon when that daemon belongs to the same `LIONCLAW_HOME`.

To run multiple local terminal channels at once, register multiple interactive channels and attach each one in its own terminal:

```bash
./target/debug/lionclaw channel add terminal2 --skill terminal --launch interactive
./target/debug/lionclaw channel attach terminal2
```

If you want a Telegram bot as a channel, register the Telegram skill and
channel:

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

Use these env vars for manual setup and migration:

- `LIONCLAW_DEFAULT_RUNTIME_ID`
- `LIONCLAW_CODEX_BIN`
- `LIONCLAW_CODEX_MODEL`
- `LIONCLAW_CODEX_SANDBOX`
- `LIONCLAW_OPENCODE_BIN`
- `LIONCLAW_OPENCODE_MODEL`
- `LIONCLAW_OPENCODE_AGENT`
- `LIONCLAW_RUNTIME_TURN_IDLE_TIMEOUT_MS`
- `LIONCLAW_RUNTIME_TURN_HARD_TIMEOUT_MS`
- `LIONCLAW_RUNTIME_TURN_TIMEOUT_MS`

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Binary Model](docs/BINARY_RUNTIME_AGNOSTIC_MODEL.md)
- [Release Process](docs/RELEASE.md)
- [Roadmap](docs/ROADMAP.md)
- [Scripts](scripts/README.md)

## License

MIT. See [LICENSE](LICENSE).
