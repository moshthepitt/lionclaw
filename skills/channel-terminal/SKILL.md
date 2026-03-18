---
name: channel-terminal
description: Run and operate a local terminal/TUI channel for LionClaw using the kernel channel bridge APIs.
---

# Terminal Channel Worker

Use this skill when the user wants a real terminal/TUI channel instead of the direct CLI path.

What you run:

1. attach a terminal to LionClaw,
2. type messages as a local peer,
3. see answer, reasoning, status, and error lanes in separate regions.

Under the hood, the worker:

1. posts inbound text to `/v0/channels/inbound` and binds the returned `turn_id` when the message is queued,
2. long-polls `/v0/channels/stream/pull`,
3. renders typed outbound events in separate terminal regions,
4. advances its consumer cursor through `/v0/channels/stream/ack`.

## Prerequisites

- `python3`
- `uv`
- A configured LionClaw runtime if `channel attach` needs to start LionClaw for you

## Setup

1. Register the skill and channel:

```bash
lionclaw skill add skills/channel-terminal --alias terminal
lionclaw channel add terminal --launch interactive
```

2. Attach your terminal to the interactive channel:

```bash
lionclaw channel attach terminal
```

If LionClaw is not already running, `channel attach` will start it using the default runtime, or the runtime passed with `--runtime`.

For a fresh manual test home in one command:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

For repeated manual testing, you can use the repo helper:

```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

## Notes

- Pairing stays enforced by LionClaw. The TUI shows pending pairing state and the exact approval command to run through `lionclaw channel pairing approve ...`.
- The worker defaults to `channel_id=terminal` and `peer_id=$USER` (falling back to `$USERNAME` or `local-user`).
- `channel attach` uses an ephemeral consumer id and `start_mode=tail`, so each attach session starts from the current stream head instead of replaying old output from previous sessions.
- The TUI echoes your message immediately, shows a local pending state right away, and then follows live `queue.*` / `runtime.*` stream events for the active turn.
- Runtime selection normally comes from the running LionClaw service or `lionclaw channel attach <id> --runtime ...` when attach needs to start it.
- The legacy shell worker is kept only as a debug harness in `scripts/debug-worker.sh`.
