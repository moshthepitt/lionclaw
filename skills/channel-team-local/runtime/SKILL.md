---
name: team-local
description: Send messages to sibling LionClaw project instances through the team-local channel.
---

# Team Local

Use `scripts/send` to message one or more sibling project instances.

```bash
scripts/send reviewer qa -- "Please check the latest result and reply with your conclusion."
```

For multiline messages, pipe the body on stdin:

```bash
printf '%s\n' "Please inspect the current workspace and reply." | scripts/send reviewer
```

Incoming team-local messages arrive as normal LionClaw channel turns. Reply normally; LionClaw routes the response back through the same channel.
