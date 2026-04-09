# LionClaw

Give yourself superpowers with a local AI orchestrator that can remember your
work, take action, and stay under your control.

LionClaw gives you a persistent assistant on your own machine while keeping
policy, audit, state, and runtime control in a small trusted core. When you
want the assistant to do more, you add installable skills instead of bloating
the core.

LionClaw currently supports Unix-like systems only. The trusted filesystem and
service assumptions in the current kernel target Linux/macOS-style Unix
environments; Windows support is out of scope for now.

## The Anatomy of a True Assistant

LionClaw is split into a rock-solid core and modular skills, giving you maximum
capability without compromising your local machine.

### Command & Control (The Core)

The core is the central engine of LionClaw. It stores durable sessions,
enforces policy, controls the runtime, and acts as the single local entry point
for execution. Because that orchestrator stays small and isolated, the rest of
the system can grow without turning the trusted boundary into a mess.

### Real-World Action (The Skills)

Skills are how your assistant gets its hands. Add the specific capabilities you
need when you need them: channels, automation, file access, background jobs,
and future integrations. The assistant becomes more useful without stuffing
that complexity into the core.

### Absolute Control (The Audit)

LionClaw records a durable audit trail for the actions that pass through the
core. You can inspect what the model executed, when it executed it, and why the
system allowed it. No hidden background magic, no invisible sidecar doing work
behind your back.

## Spin Up Your Orchestrator

LionClaw is built in Rust. Clone it, build it, and start your first persistent
session in under 60 seconds.

```bash
# 1. Build the core binaries
git clone https://github.com/moshthepitt/lionclaw.git
cd lionclaw
cargo build --release

# 2. Initialize your local environment
./target/release/lionclaw onboard

# 3. Attach a model and start executing
./target/release/lionclaw runtime add codex --kind codex --bin codex --image ghcr.io/lionclaw/codex-runtime:v1
./target/release/lionclaw run codex
```

Continue the latest local session instead of starting fresh:

```bash
./target/release/lionclaw run --continue-last-session codex
```

Inside the interactive REPL:

- `/continue` resumes from a partial timed-out, failed, cancelled, or interrupted reply
- `/retry` reruns the previous prompt
- `/reset` opens a fresh session
- `/exit` leaves the REPL

## One real recurring workflow

LionClaw can now run time-based jobs in fresh isolated sessions and optionally
deliver the final result back through a channel.

For a local terminal briefing loop:

```bash
./target/release/lionclaw skill add skills/channel-terminal --alias terminal
./target/release/lionclaw channel add terminal --launch interactive
./target/release/lionclaw channel attach terminal --runtime codex

./target/release/lionclaw job add daily-brief \
  --runtime codex \
  --schedule "every 1d" \
  --prompt "Inspect the current workspace and send me a short engineering brief with risks, drift, and next steps." \
  --deliver-channel terminal \
  --deliver-peer mosh
```

Inspect or control jobs with:

```bash
./target/release/lionclaw job ls
./target/release/lionclaw job show <job-id>
./target/release/lionclaw job runs <job-id>
./target/release/lionclaw job run <job-id>
./target/release/lionclaw job pause <job-id>
./target/release/lionclaw job resume <job-id>
./target/release/lionclaw job rm <job-id>
```

`job run` works even when a job is paused. Pausing stops automatic firing; it
does not block operator-triggered test runs.

When LionClaw is running in the background, `lionclawd` ticks the scheduler
every 30 seconds. Each scheduled run opens a fresh `scheduler` session, keeps
job-scoped policy separate from normal interactive turns, stores the full turn
history, runs one scheduled job at a time, and delivers only the final message
to the configured channel.

## Visible continuity

LionClaw keeps assistant continuity in the assistant home workspace instead of
in a hidden memory store. The hot prompt path loads `MEMORY.md` and
`continuity/ACTIVE.md`, while older context stays in daily notes, open-loop
files, artifacts, and a bounded transcript handoff summary. Continuity search
is indexed in `lionclaw.db`, but the Markdown files remain the source of truth.

Inspect and manage that continuity with:

```bash
./target/release/lionclaw continuity status
./target/release/lionclaw continuity search "release"
./target/release/lionclaw continuity get continuity/ACTIVE.md
./target/release/lionclaw continuity proposals ls
./target/release/lionclaw continuity proposals merge continuity/proposals/memory/<proposal>.md
./target/release/lionclaw continuity loops ls
./target/release/lionclaw continuity loops resolve continuity/open-loops/<loop>.md
```

## Channels and background mode

When you want LionClaw available somewhere other than the direct CLI path,
install a channel skill.

For a local channel in your current terminal, use the terminal channel skill:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That one command bootstraps a fresh test home on its own loopback bind, configures the terminal channel, and attaches it in your current TTY.

If you prefer the underlying manual steps, they are:

```bash
./target/release/lionclaw skill add skills/channel-terminal --alias terminal
./target/release/lionclaw channel add terminal --launch interactive
./target/release/lionclaw channel attach terminal --runtime codex
```

`channel attach` opens the worker in your current TTY. If needed, it starts LionClaw for you, restores the latest interactive terminal session for that peer, resumes any still-running answer stream from the last durable checkpoint, and prints the pairing code and approval command on first contact. It only reuses a daemon when that daemon belongs to the same `LIONCLAW_HOME`.

To run multiple local terminal channels at once, register multiple interactive channels and attach each one in its own terminal:

```bash
./target/release/lionclaw channel add terminal2 --skill terminal --launch interactive
./target/release/lionclaw channel attach terminal2
```

If you want Telegram as a channel, register the Telegram skill and channel:

```bash
./target/release/lionclaw skill add skills/channel-telegram --alias telegram
./target/release/lionclaw channel add telegram
```

When you want channels or automation running in the background, use service mode:

```bash
TELEGRAM_BOT_TOKEN=... ./target/release/lionclaw service up --runtime codex
```

Then inspect or manage it with:

```bash
./target/release/lionclaw service status
./target/release/lionclaw channel pairing list
./target/release/lionclaw service logs
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

Runtime profiles, execution presets, and confinement settings live in
`~/.lionclaw/config/lionclaw.toml`.

Runtime secrets for confined runtimes live separately in
`~/.lionclaw/config/runtime-secrets.env`. Presets either mount that whole file
or mount no runtime secrets at all with `mount-runtime-secrets = true|false`.
When mounted, Podman places it under `/run/secrets/` with a LionClaw-managed
name that starts with `lionclaw-runtime-secrets-`. Keep the source file
owner-only; LionClaw hardens it to `0600` on Unix before handing it to Podman.

`lionclaw runtime add` configures the runtime command that runs inside the
confinement image, plus the concrete host OCI engine path and image LionClaw
uses to launch it. Execution policy remains config-owned in LionClaw state, not
ambient shell state.

Daemon/service plumbing recognizes these env vars:

- `LIONCLAW_DEFAULT_RUNTIME_ID`
- `LIONCLAW_RUNTIME_TURN_IDLE_TIMEOUT_MS`
- `LIONCLAW_RUNTIME_TURN_HARD_TIMEOUT_MS`
- `LIONCLAW_RUNTIME_TURN_TIMEOUT_MS`

## Docs

Dive deeper:

- [Architecture](docs/ARCHITECTURE.md) - how the trusted core isolates state, policy, and runtime control
- [Binary Model](docs/BINARY_RUNTIME_AGNOSTIC_MODEL.md) - the product and runtime model behind `lionclaw run`
- [Continuity Model](docs/CONTINUITY_MODEL.md) - how LionClaw keeps visible assistant continuity without hidden memory state
- [Release Process](docs/RELEASE.md) - how releases are prepared and published
- [Roadmap](docs/ROADMAP.md) - what comes next, from channel skills to background automation
- [Scripts](scripts/README.md) - helper scripts for local setup and testing

## License

MIT. See [LICENSE](LICENSE).
