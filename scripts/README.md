# Scripts

This directory contains operator helpers for LionClaw.

## Why this exists
LionClaw keeps channels as installable skills and the kernel API explicit. The raw flow (`install -> enable -> bind -> run worker`) is intentionally low-level for auditability; these scripts make that flow fast and repeatable for local setup.

## Available scripts
- `install-channel-skill.sh`: installs a channel skill, enables it, binds it to a channel, and optionally starts the channel worker.

## Usage
Prerequisites:
- LionClaw kernel running (default `http://127.0.0.1:3000`)
- `curl` and `jq`
- A valid skill folder with `SKILL.md`

Basic install + bind:
```bash
./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --runtime-id codex
```

Install + bind + start worker:
```bash
TELEGRAM_BOT_TOKEN=... ./scripts/install-channel-skill.sh \
  --channel-id telegram \
  --skill-source skills/channel-telegram \
  --runtime-id codex \
  --start-worker
```

Show options:
```bash
./scripts/install-channel-skill.sh --help
```
