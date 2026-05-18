---
name: channel-telegram
description: Run and operate a Telegram channel worker for LionClaw using the kernel channel bridge APIs.
---

# Telegram Channel Worker

Use this skill when the user wants Telegram as a LionClaw channel.

What you run:

1. start LionClaw for background work,
2. run the Telegram worker,
3. let it bridge Telegram traffic into LionClaw.

Under the hood, the worker:

1. polls Telegram updates,
2. maps Telegram ids into stable refs such as `telegram:user:<id>`,
   `telegram:chat:<id>`, `telegram:topic:<id>`, and
   `telegram:message:<id>`,
3. posts normalized inbound facts to `/v0/channels/inbound` and treats the
   response as admission state, not completed work,
4. claims `lc_...` pairing tokens through the kernel pairing API without
   starting an agent turn for the claim message,
5. downloads and stages Telegram files only after the kernel returns
   `waiting_for_attachments`,
6. long-polls `/v0/channels/stream/pull` for progress events,
7. starts Telegram typing from kernel queue/runtime status events,
8. leases provider deliveries from `/v0/channels/outbox/pull`,
9. sends Telegram messages from outbox leases and reports provider outcomes to
   `/v0/channels/outbox/report`,
10. advances its progress cursor through `/v0/channels/stream/ack`.

## Prerequisites

- `python3`
- `uv`
- Telegram bot token with DM access
- LionClaw running (default `http://127.0.0.1:8979`)

## Setup

1. Register the skill and channel:

```bash
lionclaw skill add skills/channel-telegram --alias telegram
lionclaw channel add telegram --required-env TELEGRAM_BOT_TOKEN
```

2. Start LionClaw for background channels:

```bash
lionclaw up
```

3. Run the worker script:

```bash
TELEGRAM_BOT_TOKEN=... \
LIONCLAW_BASE_URL=http://127.0.0.1:8979 \
./skills/channel-telegram/scripts/worker
```

## Notes

- LionClaw enforces scoped pairing (`pending_approval` -> approved grant through `lionclaw channel pairing approve ...` or `/v0/channels/pairing/approve`).
- The worker accepts Telegram `message`, `edited_message`, `channel_post`, and
  `edited_channel_post` updates when they contain usable text, captions, or
  supported media.
- Telegram usernames and display names are metadata only. Authorization identity
  uses numeric Telegram ids encoded in stable refs.
- Private chats map to `dm`; group mentions and bot replies use Telegram
  entities and the bot's numeric id; forum topics map to scoped thread refs.
- Supported inbound media descriptors include photos, documents, audio, voice,
  video, stickers, video notes, and animations. Provider files are not
  downloaded for pending, blocked, ignored, or duplicate inbound.
- Pairing invite tokens can be claimed through Telegram with `/start lc_<token>`.
  DM links use `https://t.me/<bot_username>?start=lc_<token>`; group links use
  `https://t.me/<bot_username>?startgroup=lc_<token>` where Telegram exposes the
  payload to the bot.
- The worker defaults `consumer_id` to `telegram:<channel_id>` and `start_mode=resume`, so unacked progress events are replayed after worker restart.
- Telegram delivery is outbox-driven: typing comes from progress streams, final
  answers come from durable outbox leases, no reasoning lane delivery.
- Outbound text is normalized to Telegram-safe HTML with plain-text fallback,
  chunked to Telegram's message limits, and sent with link previews disabled so
  local workspace paths do not become broken Telegram links.
- Outbox attachments are sent as native Telegram media where possible
  (`sendPhoto`, `sendVideo`, `sendAudio`/`sendVoice`) and fall back to
  documents by MIME type. Short text on media deliveries is used as the first
  attachment caption.
- Topic `thread_ref` and `reply_to_ref` are converted back into Telegram
  delivery parameters, and each outbox lease is reported with its `attempt_id`.
- Runtime selection comes from the selected instance's default runtime; workers do not send `runtime_id` in inbound requests.
- The worker stores Telegram offset in `$LIONCLAW_HOME/runtime/channels/$LIONCLAW_CHANNEL_ID/telegram.offset` by default.
