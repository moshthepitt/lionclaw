# Telegram Channel

Telegram-specific setup, behavior, and acceptance live in this skill directory.
The top-level architecture docs define only the provider-neutral channel
contract.

This README owns Telegram manual QA and diagnostic acceptance. Use `SKILL.md`
for worker setup, environment variables, polling/webhook modes, command
behavior, media handling, delivery rendering, and development checks.

## Manual QA

Telegram is credential-gated manual QA. Run it only when a real bot token and
chat flow are available; do not fake Telegram with local shims.

```bash
cd "$PROJ_A"
printf 'TELEGRAM_BOT_TOKEN=...\n' > telegram.env
"$LIONCLAW_BIN" connect telegram --env-file ./telegram.env
```

Expected when credentials are available:

- the default runtime image can run common assistant probes such as
  `codex --version`, `opencode --version`, `python3 --version`,
  `ffprobe -version`, `file --version`, `jq --version`, and `pdftotext -v`
- scoped grant approval is explicit
- the channel response comes from the configured runtime
- `lionclaw logs -f` can inspect the selected instance without raw HTTP
- `doctor` surfaces channel pairing/outbox state and worker health without
  contacting Telegram APIs
- the token is stored in selected-instance private channel env
- `doctor` does not print the token and shows the latest Telegram worker health
  report after the worker has submitted one
- `connect telegram` prints a one-use direct connection link when the worker
  reports the bot identity, so the first host can click instead of typing
  `/start`
- a DM pairing link shaped like `https://t.me/<bot_username>?start=lc_<token>`
  claims through the kernel and does not start an agent turn
- a group invite shaped like
  `https://t.me/<bot_username>?startgroup=lc_<token>` claims where Telegram
  exposes the payload to the bot
- in DM, `/settings` is account/channel focused, while `/connections` shows a
  `Connect group` button that creates a short-lived one-use `startgroup` link;
  the link can be opened by the host or shared with a trusted group admin
- in connected groups, chat-scoped menu commands are installed so `/ask message`
  strips the Telegram envelope and submits only `message` to the runtime; empty
  `/ask` opens a Telegram reply prompt
- in connected groups, only Telegram accounts that are also connected as
  approved direct hosts can run `/ask`, runtime slash commands, or LionClaw
  group controls
- unknown targeted Telegram groups receive a clean "not connected" setup hint
  without exposing `pc_...` approval codes, and no provider files are downloaded
  before approval
- Telegram delivery works through the configured runtime after scoped grant
  approval
- `/help`, `/status`, `/stop`, `/settings`, and `/connections` behave as
  documented: Telegram-local commands stay local, `/lionclaw reset` and
  `/lionclaw retry` enter LionClaw as canonical controls, and `/compact` reaches
  the runtime
- inline buttons for status and stop acknowledge clicks without leaking
  controls across users, chats, or forum topics
- a forum topic with a thread grant keeps replies in the same Telegram topic
- a conversation grant used inside a topic follows the channel scoped-grant
  behavior from Channels v2
- a photo, document, voice, or video attachment reaches the runtime under
  `/attachments/...`
- a rapid burst of short text messages from the same chat is received as one
  coherent turn when Telegram delivers the updates together
- a Telegram album is received as one turn with all album attachments, and a
  runtime-generated two-photo/two-video batch is returned as a native Telegram
  media group
- a Telegram location and venue reach the runtime as readable text with
  structured provider metadata
- unsupported Telegram content such as a contact or poll gets a clear local
  reply in DMs and addressed groups, and stays silent in unaddressed groups
- a runtime-generated image is returned to Telegram as a native media
  attachment, even when the runtime final text is empty
- Markdown in runtime answers renders as Telegram formatting, and local
  workspace links are shown as labels rather than broken Telegram links
- a long-running turn first shows typing, then one provisional message that is
  edited in place, then deletes that provisional message when the durable final
  answer arrives
- the original inbound Telegram message receives best-effort reactions for
  accepted, completed, stopped, or failed lifecycle states when reactions are
  available in that chat
- webhook mode rejects requests without
  `X-Telegram-Bot-Api-Secret-Token: <TELEGRAM_WEBHOOK_SECRET_TOKEN>` and accepts
  the same update when the configured secret is present
- a retryable Telegram delivery failure survives worker restart and is retried
  through the outbox lease/report flow

If credentials are not available, record this subphase as skipped with
`missing Telegram bot token`.

## Doctor Env Diagnostic

Only run this after configuring Telegram with required env:

```bash
cd "$PROJ_A"
mv .lionclaw/instances/main/config/channels/telegram.env \
  .lionclaw/instances/main/config/channels/telegram.env.qa-bak
"$LIONCLAW_BIN" doctor
mv .lionclaw/instances/main/config/channels/telegram.env.qa-bak \
  .lionclaw/instances/main/config/channels/telegram.env
```

Expected:

- `doctor` exits 1
- the finding names the missing Telegram env without printing secret values
- `inspect` is read-only and `repair` points at `lionclaw connect telegram`
