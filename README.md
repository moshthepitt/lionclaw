# LionClaw

LionClaw is a personal AI assistant that runs locally on your machine.

It gives standard agent CLIs a durable home. You still use the real agent, but
LionClaw adds memory, scheduled jobs, local project context, explicit
credentials, and a controlled work root around it.

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
- Control the blast radius. A run sees only the work root, network mode,
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

LionClaw is built in Rust. Clone it, build it, initialize the project, create
the shared runtime image, configure Codex on the default instance, and start a
confined session.

```bash
# 1. Build the core binaries
git clone https://github.com/moshthepitt/lionclaw.git
cd lionclaw
cargo build --release

# 2. Initialize this checkout as a LionClaw project
./target/release/lionclaw project init

# 3. Build the shared runtime image with a stable local tag
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .

# 4. Example runtime: sign in to Codex once on the host
codex login

# 5. Configure Codex on the selected instance and start using it
./target/release/lionclaw configure --runtime codex
./target/release/lionclaw status
./target/release/lionclaw run
```

OpenCode can still be registered through the lower-level runtime mechanism, but
it is not part of the configure happy path yet:

```bash
./target/release/lionclaw runtime add opencode --kind opencode --bin opencode --image lionclaw-runtime:v1
./target/release/lionclaw run opencode
```

`lionclaw project init` creates `.lionclaw/project.toml` and a default
`main` instance at `.lionclaw/instances/main`. A project is the local
management boundary. An instance home stores one LionClaw instance's config,
database, logs, installed skills, runtime cache, and assistant-home continuity.

Each project instance records a default work root. `project init` points
`main` at the project root, and `instance create <name>` does the same unless
you pass `--work-root PATH`. The confined runtime sees the selected instance's
work root at `/workspace`.

Targeting is explicit and deterministic:

```bash
./target/release/lionclaw run
./target/release/lionclaw --instance reviewer run
./target/release/lionclaw --project /path/to/project --instance reviewer run
```

Without `--home` or `--project`, LionClaw discovers only the current directory
and its immediate parent. It does not walk arbitrarily up the filesystem.
Use `--home PATH` only when you need to target one exact instance home and
bypass project discovery.

Installed non-channel skills are mounted read-only under `/lionclaw/skills/<alias>`
and projected into each runtime's native skill directory inside `/runtime/home`.
`lionclaw skill install` copies a skill into that canonical skills directory, and
`lionclaw skill rm` removes it physically. `lionclaw connect <channel>` binds
channel skills as host workers; every other installed alias is runtime-visible
by default.

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

`lionclaw run` picks up the current installed skills and channels every time
you start a new run. `lionclaw connect <channel>` is the normal path for
adding a channel: it discovers the channel skill, records the binding, handles
required channel env, and starts the worker in the selected instance.

## One Real Recurring Workflow

LionClaw can run time-based jobs in fresh isolated sessions and optionally
deliver the final result back through a channel.

After configuring a runtime, keep the terminal channel open in one shell and
create the recurring job from another shell. `connect terminal` starts the
foreground worker and owns that terminal until you exit it.

Terminal A:

```bash
export LIONCLAW_PEER="${USER:-local}"

./target/release/lionclaw connect terminal
```

On first contact, approve the peer with the command printed by the terminal
worker. Then, in Terminal B:

```bash
export LIONCLAW_RUNTIME=codex
export LIONCLAW_PEER="${USER:-local}"

./target/release/lionclaw job add daily-brief \
  --runtime "$LIONCLAW_RUNTIME" \
  --schedule "every 1d" \
  --prompt "Inspect the current work root and send me a short engineering brief with risks, drift, and next steps." \
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
  mount, partitioned by session, work root, and execution security shape.

## Channels And Background Mode

When you want LionClaw somewhere other than the direct CLI path, connect a
channel skill. The command targets the resolved project instance, so the same
shape works for the default instance, a named instance, or an exact home:

```bash
./target/release/lionclaw connect terminal
./target/release/lionclaw connect telegram --env-file ./telegram.env
./target/release/lionclaw --instance reviewer connect telegram --from-env TELEGRAM_BOT_TOKEN
```

Interactive channels run in the current terminal. Service channels use systemd
user services on Linux and store required channel env in the selected instance
home, not in accidental shell state. Missing env in a non-interactive connect
prints the exact variable names and scriptable repair commands.

Inspect or remove channels on the selected instance:

```bash
./target/release/lionclaw channel list
./target/release/lionclaw channel list --all
./target/release/lionclaw channel remove telegram
```

Pairing and logs remain explicit:

```bash
./target/release/lionclaw channel pairing list --channel-id telegram
./target/release/lionclaw service logs
```

## State Layout

Project-local LionClaw state lives under `.lionclaw/`:

- `project.toml`
- `instances/main/`
- `instances/<name>/`

Each instance home contains:

- `db/lionclaw.db`
- `config/lionclaw.toml`
- `config/instance.toml`
- `config/channels/<channel>.env`
- `config/runtime-secrets.env`
- `skills/<alias>/`
- `workspaces/main/`
- `runtime/`
- `services/`

Use `--home PATH` when you need to target one instance home directly and bypass
project discovery.

## Runtime And Security Model

Runtime profiles, execution presets, and confinement settings live in the
selected instance home's `config/lionclaw.toml`.

The everyday confined runtime layout is:

- `/workspace`: the selected work root, mounted read-only or read-write by preset
- `/runtime`: runtime-private writable state
- `/drafts`: runtime-private draft/output area
- `/lionclaw/skills/<alias>`: installed non-channel skill snapshot assets, mounted read-only

Current runtime network policy is intentionally coarse:
`network-mode = "on"` or `network-mode = "none"`. `on` maps to Podman's
private network namespace, not host networking. LionClaw does not expose a
fake allowlist mode before a real egress-control plane exists. On rootless
hosts this also depends on Podman being able to create that private network
namespace, commonly through a working `pasta` and `/dev/net/tun` path. LionClaw
preflights that requirement before interactive or managed-service startup when
the effective preset uses `network-mode = "on"`.

On Arch Linux, a common failure is `pasta failed ... /dev/net/tun: No such
device` when the `tun` kernel module is not loaded. Load it with:

```bash
sudo modprobe tun
```

Verify it is present:

```bash
ls /sys/module/tun
```

To keep it loaded across reboots:

```bash
printf 'tun\n' | sudo tee /etc/modules-load.d/tun.conf
```

Runtime secrets for confined runtimes live separately in the selected instance
home's `config/runtime-secrets.env`. Presets either mount that whole file or
mount no runtime secrets at all with `mount-runtime-secrets = true|false`.
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
