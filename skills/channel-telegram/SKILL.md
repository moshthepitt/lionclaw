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
8. renders long-running turns as one provisional Telegram message and edits it
   on throttled progress state changes,
9. intercepts Telegram-local commands such as `/status`, `/stop`, `/retry`, and
   `/continue` without stealing runtime slash commands such as `/model`,
10. leases provider deliveries from `/v0/channels/outbox/pull`,
11. sends Telegram messages from outbox leases and reports provider outcomes to
   `/v0/channels/outbox/report`,
12. submits worker health checks through `/v0/channels/health/report`,
13. advances its progress cursor through `/v0/channels/stream/ack`.

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
- The worker defaults `consumer_id` to `telegram:<channel_id>` and
  `start_mode=resume`, so unacked progress events are replayed after worker
  restart. `LIONCLAW_STREAM_START_MODE` accepts `resume` or `tail`.
- Telegram delivery is outbox-driven: typing comes from progress streams, final
  answers come from durable outbox leases, no reasoning lane delivery.
- Telegram has its own visible command menu. `/help`, `/status`, `/new`,
  `/stop`, `/retry`, `/continue`, and `/settings` are channel-local aliases.
  `/retry`, `/continue`, and `/new` are translated to canonical `/lionclaw ...`
  controls before they enter the kernel. `/stop` uses the channel-safe active
  turn cancellation action with the expected turn id guard. `/model` and unknown
  slash commands pass through to the runtime after Telegram-only addressing
  syntax is removed. Bot commands explicitly targeted at a different Telegram
  bot are never captured as LionClaw-local controls, even inside an active forum
  topic. In groups, commands with a leading bot mention, for example
  `@lionclaw_bot /status`, are treated like first-column bot commands; runtime
  slash commands are forwarded without the leading mention or `@lionclaw_bot`
  command target.
- Fast turns only show typing. Long turns create one provisional message after a
  short threshold, edit it at a throttled cadence, and delete it when the durable
  outbox answer is ready. Cancelled and failed turns leave a terminal status.
  Transient edit failures are retried; permanent edit failures disable editing
  for that message and fall back to a normal Telegram message. Transient delete
  failures are persisted under the channel runtime directory and retried after
  worker restart so stale provisional messages are cleaned up.
- Outbound text is sent as plain text by default. Markdown hints render to
  Telegram-safe HTML only when the rendered chunks fit Telegram limits; otherwise
  delivery falls back to plain text. Link previews stay disabled so local
  workspace paths do not become broken Telegram links.
- Outbox attachments are sent as native Telegram media where possible
  (`sendPhoto`, `sendVideo`, `sendAudio`/`sendVoice`) and fall back to
  documents by MIME type. Short text on media deliveries is used as the first
  attachment caption when the rendered caption fits Telegram's limits.
- Topic `thread_ref` and `reply_to_ref` are converted back into Telegram
  delivery parameters, and each outbox lease is reported with its `attempt_id`.
- Runtime selection comes from the selected instance's default runtime; workers do not send `runtime_id` in inbound requests.
- The worker stores Telegram offset in `$LIONCLAW_HOME/runtime/channels/$LIONCLAW_CHANNEL_ID/telegram.offset` by default.
- The worker reports health every 60 seconds by default, configurable with
  `LIONCLAW_HEALTH_REPORT_INTERVAL_SECS`. Checks cover a fresh Telegram
  `getMe`, `getUpdates` polling failures or hangs, update lag, malformed
  provider updates quarantined by the current worker process, and delivery
  failures observed by the current worker process.

## Development Checks

Run these from the repository root when changing this skill:

```bash
uv run --project skills/channel-telegram black --check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram ruff check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram python -m unittest discover -s skills/channel-telegram/tests -q
```
