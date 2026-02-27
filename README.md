# LionClaw

LionClaw is a secure-first local agent kernel.

It is built around four constraints:

1. Anthropic skill format (`SKILL.md`) is used as-is.
2. Security boundaries are enforced by the kernel, not by prompt text.
3. Channels are skills (no default external channel in core).
4. Multiple agent runtimes are supported via adapters (`codex`, `claude-code`, `gemini-cli`, etc.).

## Status

`v0` is an implementation skeleton with:

- A runnable daemon (`lionclawd`) HTTP API
- SQLite-backed kernel services (sessions, skills, policy, audit)
- Runtime adapter contract + `mock`, `codex`, and `opencode` adapter implementations
- Kernel capability broker flow for runtime capability requests/results
- Channel bridge APIs for external skill workers (`inbound`, `outbox pull`, `outbox ack`)
- Planning and roadmap docs

See:

- `docs/V0_PLAN.md`
- `docs/ARCHITECTURE.md`
- `docs/ROADMAP.md`

## Run

```bash
cargo run
```

Server starts on `127.0.0.1:3000` by default.
SQLite database defaults to `./lionclaw.db` (override via `LIONCLAW_DB_PATH`).
Runtime turn timeout defaults to `120000` ms (override via `LIONCLAW_RUNTIME_TURN_TIMEOUT_MS`).

## Channel APIs

LionClaw exposes channel management APIs:

- `POST /v0/channels/bind`
- `GET /v0/channels/list`
- `GET /v0/channels/peers`
- `POST /v0/channels/peers/approve`
- `POST /v0/channels/peers/block`
- `POST /v0/channels/inbound`
- `POST /v0/channels/outbox/pull`
- `POST /v0/channels/outbox/ack`

## Runtime: Codex Adapter

LionClaw registers a `codex` runtime adapter at startup.

Secure defaults:

- `codex exec --json`
- `--sandbox read-only`
- `--ephemeral`
- `--skip-git-repo-check`

Optional overrides:

- `LIONCLAW_CODEX_BIN` (default: `codex`)
- `LIONCLAW_CODEX_MODEL` (unset by default)
- `LIONCLAW_CODEX_SANDBOX` (default: `read-only`)
- `LIONCLAW_CODEX_EPHEMERAL` (`true` by default)
- `LIONCLAW_CODEX_SKIP_GIT_REPO_CHECK` (`true` by default)

Use it by setting `runtime_id` on turn requests:

```json
{
  "session_id": "00000000-0000-0000-0000-000000000000",
  "user_text": "summarize the selected skills for this task",
  "runtime_id": "codex"
}
```

## Runtime: OpenCode Adapter

LionClaw also registers an `opencode` runtime adapter at startup.

Default invocation:

- `opencode run --format json "<prompt>"`

Optional overrides:

- `LIONCLAW_OPENCODE_BIN` (default: `opencode`)
- `LIONCLAW_OPENCODE_FORMAT` (default: `json`)
- `LIONCLAW_OPENCODE_MODEL` (unset by default)
- `LIONCLAW_OPENCODE_AGENT` (unset by default)
- `LIONCLAW_OPENCODE_XDG_DATA_HOME` (unset by default)
- `LIONCLAW_OPENCODE_CONTINUE_LAST_SESSION` (`false` by default)

Use it by setting `runtime_id: "opencode"` in turn requests.

## Channel Skills (External Workers)

LionClaw core does not include Telegram/Discord/WhatsApp transport code.
Each channel runs as an external skill worker that:

1. Receives platform updates.
2. Calls `POST /v0/channels/inbound` with normalized text payloads.
3. Polls `POST /v0/channels/outbox/pull` for pending replies.
4. Delivers replies to the platform.
5. Calls `POST /v0/channels/outbox/ack` with platform message IDs.

Bind a channel ID to an enabled skill:

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

Then run an external worker skill for `telegram`. A starter skill package is included at:

- `skills/channel-telegram`
