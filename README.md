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
- Channel-skill contract + local stub channel
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
