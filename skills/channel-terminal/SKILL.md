---
name: channel-terminal
description: Run and operate a local terminal/TUI channel for LionClaw using the kernel channel bridge APIs.
---

# Terminal Channel Worker

Use this skill when the user wants a proper terminal/TUI-backed local channel for LionClaw.

This skill stays outside LionClaw kernel code. It runs an external interactive worker that:

1. reads terminal input from a local peer,
2. posts inbound text to `/v0/channels/inbound`,
3. long-polls `/v0/channels/stream/pull`,
4. renders outbound `answer`, `reasoning`, `status`, and `error` events in separate terminal regions,
5. advances its consumer cursor through `/v0/channels/stream/ack`.

## Prerequisites

- `python3`
- `uv`
- Running LionClaw daemon (default `http://127.0.0.1:8979`)

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

If LionClaw is not already running, `channel attach` will ensure the daemon is started using the default runtime, or the runtime passed with `--runtime`.

## Notes

- Pairing remains kernel-enforced. The TUI shows pending pairing state and the exact approval command to run through `lionclaw channel pairing approve ...`.
- The worker defaults to `channel_id=terminal` and `peer_id=$USER` (falling back to `$USERNAME` or `local-user`).
- `channel attach` uses an ephemeral consumer id and `start_mode=tail`, so each attach session starts from the current stream head instead of replaying old output from previous sessions.
- Runtime selection normally comes from the daemon or `lionclaw channel attach <id> --runtime ...` when the daemon must be started.
- The legacy shell worker is kept only as a debug harness in `scripts/debug-worker.sh`.
