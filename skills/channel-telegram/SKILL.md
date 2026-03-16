---
name: channel-telegram
description: Run and operate a Telegram channel worker for LionClaw using the kernel channel bridge APIs.
---

# Telegram Channel Worker

Use this skill when the user wants Telegram as a LionClaw channel.

This skill does not modify LionClaw kernel code. It runs an external worker that:

1. polls Telegram updates,
2. posts inbound text to `/v0/channels/inbound`,
3. pulls pending outbound replies from `/v0/channels/outbox/pull`,
4. sends replies to Telegram,
5. acknowledges delivery to `/v0/channels/outbox/ack`.

## Prerequisites

- `curl`
- `jq`
- Telegram bot token with DM access
- Running LionClaw daemon (default `http://127.0.0.1:8979`)

## Setup

1. Install + enable a LionClaw skill to bind to channel `telegram`.
2. Bind the channel to the enabled skill:

```json
{
  "channel_id": "telegram",
  "skill_id": "<enabled-skill-id>",
  "enabled": true,
  "config": {
    "runtime_id": "codex"
  }
}
```

3. Run the worker script:

```bash
TELEGRAM_BOT_TOKEN=... \
LIONCLAW_BASE_URL=http://127.0.0.1:8979 \
./skills/channel-telegram/scripts/worker.sh
```

## Notes

- Peer pairing remains kernel-enforced (`pending` -> `approved` via `/v0/channels/peers/approve`).
- `peer_id` is Telegram `chat.id` serialized as string.
- The worker stores Telegram offset in `.lionclaw-telegram-offset` by default.
