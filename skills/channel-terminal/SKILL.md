---
name: channel-terminal
description: Run and operate a local terminal channel worker for LionClaw using the kernel channel bridge APIs.
---

# Terminal Channel Worker

Use this skill when the user wants a local terminal-backed channel for LionClaw.

This skill does not modify LionClaw kernel code. It runs an external worker that:

1. reads terminal input from a local peer,
2. posts inbound text to `/v0/channels/inbound`,
3. long-polls `/v0/channels/stream/pull`,
4. renders outbound `answer`, `reasoning`, `status`, and `error` events to the terminal,
5. advances its consumer cursor through `/v0/channels/stream/ack`.

## Prerequisites

- `bash`
- `curl`
- `jq`
- Running LionClaw daemon (default `http://127.0.0.1:8979`)

## Setup

1. Register the skill and channel:

```bash
lionclaw skill add skills/channel-terminal --alias terminal
lionclaw channel add terminal
```

2. Start the LionClaw daemon or managed service stack:

```bash
lionclaw service up --runtime codex
```

3. Run the worker script:

```bash
LIONCLAW_BASE_URL=http://127.0.0.1:8979 \
./skills/channel-terminal/scripts/worker.sh
```

## Notes

- Pairing remains kernel-enforced. The worker prints pairing status and the approval command to run through `lionclaw channel pairing approve ...`.
- The worker defaults to `channel_id=terminal` and `peer_id=$USER` (falling back to `$USERNAME` or `local-user`).
- The worker defaults `consumer_id` to `terminal:<channel_id>:<peer_id>` and `start_mode=tail`, so a first connect starts from the current stream head instead of replaying old history.
- Runtime selection normally comes from the daemon or `lionclaw service up --runtime ...`. `LIONCLAW_RUNTIME_ID` is an optional per-worker override for low-level testing.
- Run one worker per channel/peer pair. To use multiple local terminal channels at once, run separate worker processes with different `LIONCLAW_CHANNEL_ID` values.
