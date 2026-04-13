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

1. restores the latest interactive session snapshot for `(channel_id, peer_id)`,
2. posts inbound text to `/v0/channels/inbound` pinned to the chosen `session_id`,
3. long-polls `/v0/channels/stream/pull`,
4. renders typed outbound events in separate terminal regions,
5. starts recovery actions through `/v0/sessions/action`,
6. advances its consumer cursor through `/v0/channels/stream/ack`.

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

If LionClaw is not already running, `channel attach` will start it for you. Passing `--runtime <id>` pins that attached worker's turns to the requested runtime.

For a fresh manual test home in one command:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That helper gives a fresh test home its own loopback bind, so it does not inherit state from another LionClaw daemon already running elsewhere on your machine.

For repeated manual testing, you can use the repo helper:

```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

## Notes

- LionClaw enforces pairing. The TUI shows pending pairing state and the exact approval command to run through `lionclaw channel pairing approve ...`.
- The worker defaults to `channel_id=terminal` and `peer_id=$USER` (falling back to `$USERNAME` or `local-user`).
- `channel attach` restores the latest `history_policy=interactive` session for that peer, renders the last 12 durable turns into the transcript, and resumes a still-running answer stream from the last durable checkpoint when one exists.
- The TUI echoes your message immediately, shows a local pending state right away, and then follows live `queue.*` / `runtime.*` stream events for the active turn.
- Slash commands are built into the TUI:
  - `/continue`
  - `/retry`
  - `/reset`
  - `/quit`
  - `/exit`
- The Transcript pane is durable session history plus live answer deltas.
- The Thinking pane is live-only. It does not replay historical reasoning on attach.
- Runtime selection normally comes from the running LionClaw service, unless you pass `lionclaw channel attach <id> --runtime ...`, which pins that attached worker to a specific runtime.
- Attach only reuses a daemon when that daemon belongs to the same `LIONCLAW_HOME`, current project, and daemon-compatible runtime/preset config.
- A shell debug harness is available in `scripts/debug-worker.sh`.
