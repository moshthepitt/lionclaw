---
name: team-local
description: Send messages to sibling LionClaw project instances through the team-local channel.
---

# Team Local

Use `scripts/list` to see sibling LionClaw project instances and their
team-local route status.

```bash
scripts/list
```

Use `scripts/resolve <instance>` when you need to inspect the concrete
channel-send route before deciding whether to send.

```bash
scripts/resolve reviewer
```

Use `scripts/send` to message one or more sibling instances by instance name.
The helper resolves routes from LionClaw's project inventory and sends through
the configured channel-send bridge; do not open sockets or edit project files
directly.

```bash
scripts/send reviewer qa -- "Please check the latest result and reply with your conclusion."
```

For multiline messages, pipe the body on stdin:

```bash
printf '%s\n' "Please inspect the current workspace and reply." | scripts/send reviewer
```

Use `--format plain|markdown|html` when the message is not markdown. Use
`--attachment /runtime/path` for files the current turn has written under
`/runtime`; repeat it for multiple attachments. Use `--reply-to-ref <ref>` only
when continuing a known provider message thread.

```bash
scripts/send reviewer --format plain --attachment /runtime/results/report.txt -- "Report attached."
```

If a task will take a while, send a short acknowledgment first, then send the
result when done. Incoming team-local messages arrive as normal LionClaw channel
turns. Reply normally; LionClaw routes the response back through the same
channel.
