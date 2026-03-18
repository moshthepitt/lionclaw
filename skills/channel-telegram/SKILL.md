---
name: channel-telegram
description: Run and operate a Telegram channel worker for LionClaw using the kernel channel bridge APIs.
---

# Telegram Channel Worker

Use this skill when the user wants Telegram as a LionClaw channel.

What you run:

1. start LionClaw in service mode,
2. run the Telegram worker,
3. let it bridge Telegram traffic into LionClaw.

Under the hood, the worker:

1. polls Telegram updates,
2. posts inbound text to `/v0/channels/inbound` and treats the response as queued work, not completed work,
3. long-polls `/v0/channels/stream/pull`,
4. starts Telegram typing from kernel queue/runtime status events and coalesces `answer` deltas into provider-facing Telegram replies,
5. advances its consumer cursor through `/v0/channels/stream/ack`.

## Prerequisites

- `curl`
- `jq`
- Telegram bot token with DM access
- LionClaw running (default `http://127.0.0.1:8979`)

## Setup

1. Register the skill and channel:

```bash
lionclaw skill add skills/channel-telegram --alias telegram
lionclaw channel add telegram
```

2. Start LionClaw for background channels:

```bash
lionclaw service up --runtime codex
```

3. Run the worker script:

```bash
TELEGRAM_BOT_TOKEN=... \
LIONCLAW_BASE_URL=http://127.0.0.1:8979 \
./skills/channel-telegram/scripts/worker.sh
```

## Notes

- LionClaw enforces peer pairing (`pending` -> `approved` via `/v0/channels/peers/approve`).
- `peer_id` is Telegram `chat.id` serialized as string.
- The worker defaults `consumer_id` to `telegram:<channel_id>` and `start_mode=resume`, so undelivered stream events are replayed after worker restart.
- Telegram delivery is message-oriented by default: typing while a turn is active, final answer on `done`, no reasoning lane delivery.
- Runtime selection normally comes from `lionclaw service up --runtime ...`. `LIONCLAW_RUNTIME_ID` is an optional per-worker override for low-level testing.
- The worker stores Telegram offset in `.lionclaw-telegram-offset` by default.
