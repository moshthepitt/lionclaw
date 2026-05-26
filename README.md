# LionClaw

LionClaw runs real agent CLIs as project assistants.

Codex is still Codex. OpenCode is still OpenCode. LionClaw is the local control
plane around them: project identity, runtime configuration, durable sessions,
local state, channels, scheduled work, and an audit trail.

Use the agent harness you want. Keep the boundary around your project yours.

```bash
git clone https://github.com/moshthepitt/lionclaw.git
cd lionclaw
cargo build --release
export PATH="$PWD/target/release:$PATH"

cd /path/to/your/project
lionclaw doctor
```

`doctor` points at the next setup problem. Fix, rerun, repeat; when setup is
ready, run the command it prints next.

## Why LionClaw

AI is going to matter. Probably a lot.

And if it is going to matter, then we should not sleepwalk into a future where
a handful of large companies own the agents, the memory, the tools, the
workflows, the credentials, the logs, and eventually the shape of the work
itself. We have seen this movie before. It was called Web 2.0, and it ended
with everyone renting their own lives back from platforms.

LionClaw is a small refusal of that future.

The model may be commercial. That is fine. The agent harness may be Codex,
OpenCode, Claude Code, Gemini CLI, or something better that appears next month.
Also fine. But the boundary around the agent should belong to you: the project
it runs in, the state it sees, the credentials it receives, the channels that
can reach it, the jobs it runs, and the audit trail it leaves behind.

LionClaw keeps that boundary local, explicit, and swappable.

This matters because the future probably will not have one agent. It will have
many agent harnesses, each good at different things, each with its own strange
little strengths and weaknesses. LionClaw gives one project a stable assistant
home while those runtimes come and go.

It is also intentionally small. Not because small is cute, but because small is
auditable. Small can be understood. Small can be changed by one person with a
weekend, a grudge, and a good enough reason.

That is the bet: real agents, local boundaries, user-owned control.

## How LionClaw Works

A LionClaw project is a normal project with a `.lionclaw/` directory.

That directory is the local control plane. It holds the project identity,
instances, runtime profiles, sessions, audit records, installed skills, channel
bindings, jobs, and runtime-private state.

When you run:

```bash
lionclaw run
```

LionClaw opens the project operator console when attached to a terminal. The
console keeps the current context in the top ribbon and the everyday turn flow
in one Run surface: durable transcript, prompt input, live runtime status, and
recent file-change feedback. On wide terminals, project objects stay visible
beside Run and the right column keeps the inspector and changed-files pane
visible together. Runtime stream detail is shown inline for the active turn and
as inspectable activity instead of being mixed into the durable conversation.

Use the plain line-oriented path when scripting or when a terminal UI is not
wanted:

```bash
lionclaw run --plain
```

Non-terminal use automatically stays on the plain path.

The selected runtime still does the agent work. LionClaw owns the boundary
around it: the project it runs in, the state it sees, the mounts it receives,
the credentials that are staged, the durable session, and the record of what
happened.

That is the split: the runtime does the agent work. LionClaw decides where it
runs, what it can see, and what gets recorded.

## Set Up A Project

Run `doctor` from the project you want LionClaw to own:

```bash
cd /path/to/your/project
lionclaw doctor
```

Follow the repair commands until `doctor` reports no blocking setup issues.
Then run the configured runtime:

```bash
lionclaw run
```

In a terminal, `run` opens the operator console. Use `lionclaw run --plain`
for the line-oriented interactive path.

Inside the console, `Tab` and `Shift+Tab` move between Run and controls.
`Ctrl+O` focuses controls, `Left` and `Right` cycle project, runtime, boundary,
activity, audit, and files, `Ctrl+X` maximizes Run or the active control pane,
`Ctrl+P` opens commands and help, `Ctrl+C` interrupts an active turn, and
`Ctrl+D` exits when idle. `Enter` submits the prompt or activates a selected
project item when project controls are active. After a prompt is submitted,
Run remains the active scroll surface; typing printable text continues prompt
editing in that same surface.

For Codex, use a logged-in Codex CLI and run LionClaw where the `podman`
executable is available. If `run` reports a missing runtime image, build or
provide the image named in the error. The bundled image definition lives at
`containers/runtime/Containerfile`. That default image is intentionally
useful for everyday assistant work: it includes Codex, OpenCode, git, ripgrep,
curl, jq, Python, archive helpers, PDF text tools, and ffmpeg/ffprobe for
basic media inspection without bundling browsers, SDKs, OCR language packs, or
local model weights.

Container image recipes, including the Rust-enabled LionClaw development image,
are documented in `containers/README.md`.

`doctor` checks setup and prints the next run command when setup is no longer
blocked. `run` checks launch. Use `lionclaw --help` and subcommand `--help`
for current syntax.

Runtime auth stays runtime-specific. LionClaw stages only the runtime-local auth
files needed for the confined launch.

Confined runtime layout:

- `/workspace`: selected work root
- `/runtime`: runtime-private writable state
- `/drafts`: runtime-private draft/output area
- `/lionclaw/project/instances.json`: read-only project instance inventory,
  present for project-backed program runtimes
- `/lionclaw/skills/<alias>`: installed non-channel skill assets
- `/mnt/<target>`: optional operator-configured extra directories

Network policy is intentionally coarse today: `on` or `none`. `on` uses the
Podman network namespace, not host networking.

Extra runtime mounts are explicit runtime-profile state. Use a short target
token for the normal `/mnt/<token>` path, or an absolute container target when
needed:

```bash
lionclaw runtime mount add codex docs --source /absolute/docs
lionclaw runtime mount add codex /reference/docs --source /absolute/docs --read-write
lionclaw runtime mount list codex
lionclaw runtime mount remove codex docs
```

Mounts default to read-only. LionClaw rejects files, missing directories,
project/work-root metadata and instance-state paths, duplicate targets, and
reserved runtime paths such as `/workspace`, `/runtime`, `/drafts`,
`/attachments`, `/lionclaw`, `/run/secrets`, `/proc`, `/sys`, and `/dev`.
Podman bind-mount arguments must also be representable: commas are accepted
only when neither the source nor target path requires the `--mount` form by
containing `:`.

## Projects And Instances

`lionclaw project init` creates `.lionclaw/project.toml` and a default `main`
instance at `.lionclaw/instances/main`. A project is the local management
boundary. An instance home stores one LionClaw instance's config, database,
logs, installed skills, runtime cache, and assistant-home continuity.

Each project instance records a default work root. `project init` points `main`
at the project root, and `instance create <name>` does the same unless you pass
`--work-root PATH`. The confined runtime sees the selected work root at
`/workspace`.

Project-backed runtimes also receive `LIONCLAW_PROJECT_INSTANCE` and
`LIONCLAW_PROJECT_INSTANCES_FILE`. The file is a read-only generated projection
of configured project instance names, not a mount of raw `.lionclaw` metadata.
When the active preset allows program-backed `channel.send`, the same projection
can include restart-bound neighbor contact status; only configured neighbor
routes expose channel and provider refs.

Configure a preferred contact from the recipient instance:

```bash
lionclaw --instance reviewer connect ./channels/team-local --contact
lionclaw --instance reviewer channel add team-local --skill team-local --contact --conversation-ref member:reviewer
```

`connect --contact` can use a channel skill's `{instance}` contact template.
Pass `--conversation-ref` when you want to set the provider route explicitly.

Without `--home` or `--project`, LionClaw discovers only the current directory
and its immediate parent. Use `--home PATH` when you need to target one exact
instance home and bypass project discovery. Use `lionclaw instance --help` for
the current instance commands.

## Channels And Background Work

Channels are skills that run outside the Rust core. They connect LionClaw to a
transport without baking Telegram, Slack, or future integrations into the
trusted kernel.

Interactive channels run in the current terminal. Background channels are
managed through the platform backend and store required channel env in the
selected instance home, not in accidental shell state.

Use `lionclaw up` when you want LionClaw to stay reachable after the current
terminal is gone. Use command help for current channel and background syntax.

## Jobs

Jobs run time-based prompts in fresh isolated sessions and can deliver the
final result through a configured channel.

They are configured through `lionclaw job`. Use `lionclaw job --help` for the
current command surface.

## Doctor

`lionclaw doctor` is read-only. It diagnoses target resolution, project state,
runtime config, channels, managed units, and configured bind drift without
allocating ports, starting units, stopping units, or changing files.

If no project exists in the current directory or its immediate parent, `doctor`
treats the current directory as the diagnostic target, reports `no LionClaw
project found`, and tells you how to initialize it.

Findings render as stable runbook entries:

```text
[LC-D001] error: configured bind is occupied
target: instance main configured bind 127.0.0.1:8787
expected: no listener, or the owned managed daemon for this instance
observed: 127.0.0.1:8787 is used by a non-LionClaw process
inspect: ss -ltnp '( sport = :8787 )'
note: stop the process shown by inspect
```

Info and warnings are advisory and exit 0. Errors exit 1, and internal doctor
failures exit 2.

## Requirements

`lionclaw run` currently supports Unix-like systems. Managed background paths,
including `lionclaw up` and background channels, currently require Linux with a
systemd user manager.

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
