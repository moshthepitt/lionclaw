# Scripts

This directory contains the sharp tools.

## Why this exists
Most users should stay on the main `lionclaw` path: register a real agent
runtime, run it through LionClaw, and add channels or jobs through the CLI.

These scripts are here for the moments when you want direct control over install, bind, and worker startup. Manual setup. Debugging. Tight feedback loops.

Use them when you mean it.

## Available scripts
- `ci.sh`: runs the local CI gate, mirrors the GitHub Actions `ci` workflow, and is the preferred pre-push verification entrypoint.
- `lionclaw-project.sh`: incubates a project-local convenience flow that
  resolves a workspace, project-local LionClaw home, runtime image, runtime,
  terminal channel, and optional project skills while delegating durable state
  changes to the canonical `lionclaw` CLI.
- `bootstrap-terminal-test.sh`: bootstraps or refreshes a manual terminal-channel
  test home, gives a fresh home its own loopback bind, configures the runtime
  image and terminal channel, then attaches it in the current TTY.
- `install-channel-skill.sh`: wraps the canonical `lionclaw skill add`,
  `lionclaw channel add`, and `lionclaw apply` flow for a channel skill, then
  optionally starts the worker from the installed snapshot's `scripts/worker`.
- `attach-terminal-test.sh`: rebuilds LionClaw, stops managed services for a specific `LIONCLAW_HOME`, and attaches the interactive terminal channel in the current TTY.

## Usage
Run the same checks as GitHub CI:
```bash
./scripts/ci.sh
```

For channel-install scripts, prerequisites are:
- `lionclaw` on `PATH`, or `--lionclaw-bin /path/to/lionclaw`
- LionClaw running when `--start-worker` is used (default `http://127.0.0.1:8979`)
- `curl` when `--start-worker` is used
- A valid skill folder with `SKILL.md`

Basic install + bind:
```bash
./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram
```

Install + bind + start worker:
```bash
TELEGRAM_BOT_TOKEN=... ./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --start-worker
```

Optional low-level per-worker runtime override:
```bash
./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --skill-alias telegram \
  --runtime-id codex
```

Show options:
```bash
./scripts/install-channel-skill.sh --help
```

Project-local LionClaw helper:
```bash
./scripts/lionclaw-project.sh doctor
./scripts/lionclaw-project.sh configure
./scripts/lionclaw-project.sh run
```

By default this uses the current Git root as the project, reuses an existing
`<project>/lionclaw-home` when present, otherwise stores LionClaw state under
`<project>/.lionclaw/home`. If the project has a root `Containerfile`, or
exactly one `<project>/*/Containerfile`, the helper treats that directory as the
workspace, derives an image tag like `<workspace>-runtime:v1`, and installs
conventional project skills from `<project>/skills/*/SKILL.md`. Override those
defaults explicitly when needed:
```bash
LIONCLAW_RUNTIME_IMAGE=my-project-runtime:v1 \
LIONCLAW_RUNTIME_CONTAINERFILE=Containerfile \
LIONCLAW_PROJECT_SKILLS='pdf-to-markdown=skills/pdf-to-markdown' \
  ./scripts/lionclaw-project.sh configure
```

This helper is intentionally an incubation path. It should stay thin: project
path resolution, preflight, runtime image build, and orchestration only. Durable
configuration still flows through `lionclaw onboard`, `lionclaw runtime add`,
`lionclaw skill add`, `lionclaw channel add`, `lionclaw run`,
`lionclaw channel attach`, and `lionclaw service ...`.

Fresh terminal-channel test home in one command on Linux with systemd user
services:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That command uses `lionclaw onboard --bind auto` for a fresh home, so manual
test homes do not collide with another LionClaw daemon already using the
default bind. It configures the runtime with `lionclaw-runtime:v1` and builds
that shared local image first when it is missing. The attach step uses
LionClaw's managed daemon path, which currently needs systemd user services.

Override the runtime id, command, or channel:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e work codex terminal
```

Override the runtime image or kind with environment variables:
```bash
LIONCLAW_RUNTIME_IMAGE=lionclaw-runtime:v2 \
LIONCLAW_RUNTIME_KIND=opencode \
  ./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e opencode opencode terminal
```

Rebuild + restart + attach the terminal test channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

Override the runtime or channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e codex terminal
```
