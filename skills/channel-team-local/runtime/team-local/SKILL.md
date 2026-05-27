---
name: team-local
description: Send messages to sibling LionClaw project instances through the team-local channel.
---

# Team Local

Use `scripts/send` to message one or more sibling LionClaw project instances by
instance name. The helper resolves routes from LionClaw's project inventory and
sends through the configured channel-send bridge; do not open sockets or edit
project files directly.

```bash
scripts/send reviewer qa -- "Please check the latest result and reply with your conclusion."
```

For multiline messages, pipe the body on stdin:

```bash
printf '%s\n' "Please inspect the current workspace and reply." | scripts/send reviewer
```

If a task will take a while, send a short acknowledgment first, then send the
result when done. Incoming team-local messages arrive as normal LionClaw channel
turns. Reply normally; LionClaw routes the response back through the same
channel.
