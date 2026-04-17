# Scripts

This directory contains the sharp tools.

## Why this exists
Most users should stay on the main `lionclaw` path: register a real agent
runtime, run it through LionClaw, and add channels or jobs through the CLI.

These scripts are here for the moments when you want direct control over install, bind, and worker startup. Manual setup. Debugging. Tight feedback loops.

Use them when you mean it.

## Available scripts
- `ci.sh`: runs the local CI gate, mirrors the GitHub Actions `ci` workflow, and is the preferred pre-push verification entrypoint.
- `bootstrap-terminal-test.sh`: bootstraps or refreshes a manual terminal-channel test home, gives a fresh home its own loopback bind, configures the runtime and terminal channel, then attaches it in the current TTY.
- `install-channel-skill.sh`: installs a channel skill, enables it, binds it to a channel, and optionally starts the channel worker. It uses `scripts/worker` when present and otherwise uses `scripts/worker.sh`.
- `attach-terminal-test.sh`: rebuilds LionClaw, stops managed services for a specific `LIONCLAW_HOME`, and attaches the interactive terminal channel in the current TTY.

## Usage
Run the same checks as GitHub CI:
```bash
./scripts/ci.sh
```

For channel-install scripts, prerequisites are:
- LionClaw running (default `http://127.0.0.1:8979`)
- `curl` and `jq`
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
  --runtime-id codex
```

Show options:
```bash
./scripts/install-channel-skill.sh --help
```

Fresh terminal-channel test home in one command:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That command uses `lionclaw onboard --bind auto` for a fresh home, so manual test homes do not collide with another LionClaw daemon already using the default bind.

Override the runtime id, command, or channel:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e work codex terminal
```

Rebuild + restart + attach the terminal test channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

Override the runtime or channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e codex terminal
```
