---
name: channel-team-local
description: First-party LionClaw channel for local project instance communication.
---

# Team Local Channel

This bundled channel lets LionClaw project instances deliver messages to sibling
instances on the same machine through each instance's own LionClaw daemon. The
same installed channel snapshot carries the worker and the runtime-facing
`runtime/team-local/` skill. The worker polls outbox frequently for low-latency
local delivery, but reports health at startup and then at most once per minute
while running.
