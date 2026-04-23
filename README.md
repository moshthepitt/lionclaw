# LionClaw

LionClaw is a personal AI assistant that runs locally on your machine.

It gives standard agent CLIs a durable home. You still use the real agent, but
LionClaw adds memory, scheduled jobs, local project context, explicit
credentials, and a controlled workspace around it.

The agent does the heavy lifting. LionClaw provides the security boundary,
session state, runtime launch path, and audit log for the decisions it owns.

Think of it as a new layer for AI: a local home for the interactive coding and
reasoning agents you already use. LionClaw gives them presence, continuity,
credentials, and long-running workflows while keeping them inside an
environment you define.

Note: LionClaw's direct `lionclaw run` path currently supports Unix-like
systems (Linux/macOS). Managed daemon paths, including `service up` and channel
auto-start, currently require Linux with systemd user services. Windows is out
of scope.

## Why LionClaw

An AI assistant that can read files, run commands, use your credentials, and
stay online is a powerful local operator. It is also a serious security
boundary problem.

LionClaw gives that operator a home you control: real agent runtimes,
persistent context, scheduled work, explicit credentials, and a narrow local
sandbox around every run.

Here is the bet LionClaw makes:

- Run the real agents. Supported runtimes use their native CLI paths, so
  Codex, OpenCode, and future runtimes keep their own capabilities and
  workflows.
- Keep the core small. The trusted Rust core focuses on policy, state,
  auditing, runtime launch, sessions, and jobs.
- Control the blast radius. A run sees only the workspace, network mode,
  runtime state, and secrets LionClaw explicitly grants.
- Keep context alive. Sessions, scheduled jobs, and channels survive beyond one
  terminal prompt.
- Extend without bloat. Channels and integrations live as skills outside the
  core, so LionClaw can grow without turning into a feature monolith.

### Bring Your Own Runtime

LionClaw currently supports Codex and OpenCode. The quick start uses Codex as
one concrete example because its host-auth path is the most exercised route
today. The model is runtime-agnostic: register a runtime, choose it for a run,
and LionClaw applies the same local contract around it.

### The Boundary Contract

LionClaw owns the boundary, not every thought inside it.

The selected agent can use its own tools inside the sandbox. LionClaw decides
what directories are mounted, what network exists, which credentials appear,
what session is active, and which boundary decisions are recorded.

## Quick Start

LionClaw is built in Rust. Clone it, build it, create the shared runtime image,
register one runtime, and start a confined session.

```bash
# 1. Build the core binaries
git clone https://github.com/moshthepitt/lionclaw.git
cd lionclaw
cargo build --release

# 2. Build the shared runtime image with a stable local tag
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .

# 3. Initialize your local environment
./target/release/lionclaw onboard

# 4. Example runtime: sign in to Codex once on the host
codex login

# 5. Register that runtime and start using it
./target/release/lionclaw runtime add codex --kind codex --bin codex --image lionclaw-runtime:v1
./target/release/lionclaw run codex
```

If you prefer OpenCode and have it configured, register it through the same
runtime mechanism:

```bash
./target/release/lionclaw runtime add opencode --kind opencode --bin opencode --image lionclaw-runtime:v1
./target/release/lionclaw run opencode
```

`lionclaw run ...` uses your current directory as the project root by default.
The confined runtime sees that project at `/workspace`. LionClaw keeps its own
state, continuity, runtime cache, services, and config under `LIONCLAW_HOME`.
Selected skill snapshots are mounted read-only under `/lionclaw/skills/<alias>`.

Runtime-specific provider settings stay with the runtime. For example, if a
Codex profile leaves `model` unset, LionClaw reuses the current host Codex
model for that launch instead of inventing a second LionClaw-only setting.

Continue the latest local session for the current project:

```bash
./target/release/lionclaw run --continue-last-session <runtime>
```

Long turns are bounded by two kernel-enforced timers: an idle timeout for stalled
runtimes and a hard safety limit. The local interactive default is 5 minutes idle
and 2 hours hard. For unusually long work, raise the hard limit:

```bash
./target/release/lionclaw run --timeout 4h <runtime>
```

Inside the interactive REPL:

- `/continue` resumes from a partial timed-out, failed, cancelled, or interrupted assistant answer
- `/retry` reruns the previous prompt
- `/reset` opens a fresh session
- `/exit` leaves the REPL

## How It Works

LionClaw has three layers.

### The Core

The Rust core is the trusted control plane. It stores durable sessions, manages
runtime profiles, compiles execution plans, launches confined runtimes,
records audit events, runs the scheduler, and exposes the channel bridge APIs.

The core should stay small enough to audit. It does not absorb every channel,
tool, provider, and workflow into one process.

### The Runtime

A runtime is the agent LionClaw runs. Today that means program-backed CLIs such
as Codex and OpenCode. Future runtimes can use the same launch, session,
policy, and channel contract.

Program-backed runtimes bring their own capabilities. LionClaw does not need
to impersonate those capabilities to be useful; it constrains the environment
in which they run.

### Skills And Channels

Skills are installable packages of instructions, channel workers, and
integration logic. Channels are skills because Telegram, terminal UI, Slack,
or any future transport should not become part of the trusted Rust core.

Skills can provide context to the selected runtime, run external workers, and
connect LionClaw to the outside world. They cannot grant permissions by
putting words in a prompt.

## One Real Recurring Workflow

LionClaw can run time-based jobs in fresh isolated sessions and optionally
deliver the final result back through a channel.

After registering a runtime, keep the terminal channel open in one shell and
create the recurring job from another shell. `channel attach` starts the
foreground worker and owns that terminal until you exit it.

Terminal A:

```bash
export LIONCLAW_RUNTIME=codex
export LIONCLAW_PEER="${USER:-local}"

./target/release/lionclaw skill add skills/channel-terminal --alias terminal
./target/release/lionclaw channel add terminal --launch interactive
./target/release/lionclaw channel attach terminal \
  --runtime "$LIONCLAW_RUNTIME" \
  --peer "$LIONCLAW_PEER"
```

On first contact, approve the peer with the command printed by the terminal
worker. Then, in Terminal B:

```bash
export LIONCLAW_RUNTIME=codex
export LIONCLAW_PEER="${USER:-local}"

./target/release/lionclaw job add daily-brief \
  --runtime "$LIONCLAW_RUNTIME" \
  --schedule "every 1d" \
  --prompt "Inspect the current workspace and send me a short engineering brief with risks, drift, and next steps." \
  --deliver-channel terminal \
  --deliver-peer "$LIONCLAW_PEER"
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

## Visible Continuity

LionClaw keeps assistant continuity in the assistant home workspace instead of
in a hidden memory store. The hot prompt path loads `MEMORY.md` and
`continuity/ACTIVE.md`, while older context stays in daily notes, open-loop
files, artifacts, and a bounded transcript handoff summary. Continuity search
is indexed in `lionclaw.db`, but the Markdown files remain the source of truth.

Inspect and manage continuity with:

```bash
./target/release/lionclaw continuity status
./target/release/lionclaw continuity search "release"
./target/release/lionclaw continuity get continuity/ACTIVE.md
./target/release/lionclaw continuity drafts ls --runtime <runtime>
./target/release/lionclaw continuity drafts promote report.md --runtime <runtime>
./target/release/lionclaw continuity drafts discard report.md --runtime <runtime>
./target/release/lionclaw continuity proposals ls
./target/release/lionclaw continuity proposals merge continuity/proposals/memory/<proposal>.md
./target/release/lionclaw continuity loops ls
./target/release/lionclaw continuity loops resolve continuity/open-loops/<loop>.md
```

LionClaw and the selected runtime keep separate continuity layers:

- LionClaw owns the durable session transcript, audit trail, assistant home,
  drafts, scheduled artifacts, and channel delivery history.
- The runtime owns its private resumable state under the confined `/runtime`
  mount, partitioned by session, project root, and execution security shape.

## Channels And Background Mode

When you want LionClaw somewhere other than the direct CLI path, install a
channel skill.

For a local channel in your current terminal on Linux with systemd user
services:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That command bootstraps a fresh test home on its own loopback bind, configures
the runtime and terminal channel, starts the managed daemon if needed, and
attaches it in your current TTY.

For an existing LionClaw home with a configured runtime, the manual channel
steps are:

```bash
./target/release/lionclaw skill add skills/channel-terminal --alias terminal
./target/release/lionclaw channel add terminal --launch interactive
./target/release/lionclaw channel attach terminal --runtime <runtime>
```

`channel attach` opens the worker in your current TTY. If needed, it starts
LionClaw for you through the managed daemon path, which currently uses systemd
user services. It restores the latest interactive terminal session for that
peer, resumes any still-running answer stream from the last durable checkpoint,
and prints the pairing code and approval command on first contact. It only
reuses a daemon when that daemon belongs to the same `LIONCLAW_HOME`, current
project, and compatible daemon config, including runtime, preset, and workspace
settings.

To run multiple local terminal channels at once, register multiple interactive
channels and attach each one in its own terminal:

```bash
./target/release/lionclaw channel add terminal2 --skill terminal --launch interactive
./target/release/lionclaw channel attach terminal2
```

For Telegram:

```bash
./target/release/lionclaw skill add skills/channel-telegram --alias telegram
./target/release/lionclaw channel add telegram
```

On Linux systems with systemd user services, run channels or automation in the
background with service mode:

```bash
TELEGRAM_BOT_TOKEN=... ./target/release/lionclaw service up --runtime <runtime>
```

Then inspect or manage it with:

```bash
./target/release/lionclaw service status
./target/release/lionclaw channel pairing list
./target/release/lionclaw service logs
```

## State Layout

LionClaw defaults to `~/.lionclaw`:

- `db/lionclaw.db`
- `config/lionclaw.toml`
- `config/lionclaw.lock`
- `config/runtime-secrets.env`
- `skills/<skill-id>@<hash>/`
- `workspaces/main/`
- `runtime/`
- `services/`

Override the root with `LIONCLAW_HOME`.

## Runtime And Security Model

Runtime profiles, execution presets, and confinement settings live in
`~/.lionclaw/config/lionclaw.toml`.

The everyday confined runtime layout is:

- `/workspace`: the project root, mounted read-only or read-write by preset
- `/runtime`: runtime-private writable state
- `/drafts`: runtime-private draft/output area
- `/lionclaw/skills/<alias>`: selected skill snapshot assets, mounted read-only

Current runtime network policy is intentionally coarse:
`network-mode = "on"` or `network-mode = "none"`. `on` maps to Podman's
private network namespace, not host networking. LionClaw does not expose a
fake allowlist mode before a real egress-control plane exists.

Runtime secrets for confined runtimes live separately in
`~/.lionclaw/config/runtime-secrets.env`. Presets either mount that whole file
or mount no runtime secrets at all with `mount-runtime-secrets = true|false`.
When mounted, Podman places it under `/run/secrets/` with a LionClaw-managed
name that starts with `lionclaw-runtime-secrets-`. LionClaw hardens the source
file to owner-only permissions on Unix before handing it to Podman.

Runtime auth stays runtime-specific. For host-auth runtimes, LionClaw reads
the validated host auth state, stages only the runtime-local files needed for
the confined launch, and avoids mounting the real host runtime home into the
container. The Codex path, for example, stages session-local copies of
`auth.json` and `config.toml` under `/runtime/home/.codex` and launches Codex
with its official external-sandbox mode inside LionClaw's outer Podman
boundary.

`lionclaw runtime add` configures the runtime command that runs inside the
runtime image, plus the concrete host `podman` executable and image LionClaw
uses to launch it. The shared runtime image definition lives at
`containers/runtime/Containerfile` and currently installs Codex and OpenCode.
Runtime compatibility keys include the resolved local OCI image identity, so
rebuilding the same stable local tag creates a new compatibility boundary
automatically.

## Docs

Dive deeper:

- [Architecture](docs/ARCHITECTURE.md) - how the trusted core, runtime boundary, channels, scheduler, and audit fit together
- [Runtime Model](docs/RUNTIME_MODEL.md) - the program-backed agent runtime model behind `lionclaw run`
- [Continuity Model](docs/CONTINUITY_MODEL.md) - how LionClaw keeps visible assistant continuity without hidden memory state
- [v0 Plan](docs/V0_PLAN.md) - current scope, constraints, and non-goals
- [Manual QA](docs/MANUAL_QA.md) - repeatable live smoke process for runtime, daemon, terminal, jobs, secrets, and project isolation
- [Release Process](docs/RELEASE.md) - how releases are prepared and published
- [Roadmap](docs/ROADMAP.md) - what comes next, from runtime boundary hardening to channels and skills
- [Scripts](scripts/README.md) - helper scripts for local setup and testing

## License

MIT. See [LICENSE](LICENSE).
