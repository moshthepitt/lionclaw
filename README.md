# LionClaw

LionClaw turns real agent CLIs into local assistants. It gives agents such as
Codex a project home, durable sessions, explicit local state, channels,
scheduled work, and a small trusted Rust core that owns the boundary around
each run.

The everyday path is command-first:

```bash
lionclaw project init
lionclaw configure --runtime codex
lionclaw run
```

Add channels and background operation when you need the assistant to stay
reachable beyond the current terminal:

```bash
lionclaw connect terminal
lionclaw connect telegram
lionclaw up
lionclaw status
lionclaw logs -f
lionclaw doctor
```

Use `./target/release/lionclaw` from a source checkout if the binary is not on
your `PATH`.

## Install From Source

```bash
git clone https://github.com/moshthepitt/lionclaw.git
cd lionclaw
cargo build --release
```

`lionclaw run` currently supports Unix-like systems. Managed background paths,
including `lionclaw up` and background channels, currently require Linux with a
systemd user manager.

## What LionClaw Owns

LionClaw owns the local boundary and the product entrypoint:

- project and instance selection
- runtime configuration and launch
- work-root, runtime-state, draft, skill, and secret mounts
- durable sessions and audit records
- channel bindings and background units
- scheduler-owned runs

The runtime remains the real agent harness. Codex still runs as Codex; LionClaw
controls where it runs, what local state it sees, which credentials are staged,
and which LionClaw-owned decisions are recorded.

## Projects And Instances

`lionclaw project init` creates `.lionclaw/project.toml` and a default `main`
instance at `.lionclaw/instances/main`. A project is the local management
boundary. An instance home stores one LionClaw instance's config, database,
logs, installed skills, runtime cache, and assistant-home continuity.

Each project instance records a default work root. `project init` points `main`
at the project root, and `instance create <name>` does the same unless you pass
`--work-root PATH`. The confined runtime sees the selected work root at
`/workspace`.

Targeting is explicit:

```bash
lionclaw run
lionclaw --instance reviewer run
lionclaw --project /path/to/project --instance reviewer run
```

Without `--home` or `--project`, LionClaw discovers only the current directory
and its immediate parent. Use `--home PATH` when you need to target one exact
instance home and bypass project discovery.

## Runtimes

The current happy path configures Codex:

```bash
lionclaw configure --runtime codex
lionclaw run
```

Runtime auth stays runtime-specific. For Codex, sign in with Codex on the host
first, then let LionClaw stage only the runtime-local auth files needed for the
confined launch.

Confined runtime layout:

- `/workspace`: selected work root
- `/runtime`: runtime-private writable state
- `/drafts`: runtime-private draft/output area
- `/lionclaw/skills/<alias>`: installed non-channel skill assets

Network policy is intentionally coarse today: `on` or `none`. `on` uses the
Podman network namespace, not host networking.

## Channels

Channels are skills that run outside the Rust core. They connect LionClaw to a
transport without baking Telegram, terminal UI, Slack, or future integrations
into the trusted kernel.

```bash
lionclaw connect terminal
lionclaw connect telegram --env-file ./telegram.env
lionclaw channel list
lionclaw channel remove telegram
```

Interactive channels run in the current terminal. Background channels are
managed through the platform backend and store required channel env in the
selected instance home, not in accidental shell state.

## Background Operation

Use the managed path when you want the selected instance to run background
workers and scheduled jobs:

```bash
lionclaw up
lionclaw status
lionclaw logs -f
lionclaw down
```

Project-wide forms are explicit:

```bash
lionclaw up --all
lionclaw down --all
lionclaw logs --all --tail 200
lionclaw doctor --all
```

## Jobs

Jobs run time-based prompts in fresh isolated sessions and can deliver the
final result through a configured channel.

```bash
lionclaw job add daily-brief \
  --runtime codex \
  --schedule "every 1d" \
  --prompt "Inspect the current work root and send a short engineering brief." \
  --deliver-channel terminal \
  --deliver-peer "$USER"

lionclaw job ls
lionclaw job show <job-id>
lionclaw job run <job-id>
lionclaw job pause <job-id>
lionclaw job resume <job-id>
lionclaw job rm <job-id>
```

`job run` works even when a job is paused. Pausing stops automatic firing; it
does not block operator-triggered test runs.

## Doctor

`lionclaw doctor` is read-only. It diagnoses target resolution, project state,
runtime config, channels, managed units, and configured bind drift without
allocating ports, starting units, stopping units, or changing files.

Findings render as stable runbook entries:

```text
[LC-D001] error: configured bind is occupied
target: instance main configured bind 127.0.0.1:8787
expected: no listener, or the owned managed daemon for this instance
observed: 127.0.0.1:8787 is used by a non-LionClaw process
inspect: ss -ltnp '( sport = :8787 )'
note: stop the process shown by inspect
repair: lionclaw up
```

Warnings alone exit 0, errors exit 1, and internal doctor failures exit 2.

## State Layout

Project-local state lives under `.lionclaw/`:

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
- `units/`

## Docs

- [Architecture](docs/ARCHITECTURE.md) - trusted core, runtime boundary, channels, scheduler, audit, and API contracts
- [Manual QA](docs/MANUAL_QA.md) - repeatable live acceptance checklist
- [Release Process](docs/RELEASE.md) - release preparation and publishing
- [Scripts](scripts/README.md) - developer and CI smoke helpers

## License

MIT. See [LICENSE](LICENSE).
