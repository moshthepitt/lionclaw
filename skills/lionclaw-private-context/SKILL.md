---
name: lionclaw-private-context
description: Host-side LionClaw private-context skill for deterministic projection, committed-turn recording, and explicit profile or memory management.
---

# LionClaw Private Context

Use this skill as LionClaw's default local private-context capability. It owns a
host-only SQLite store and exposes three deterministic surfaces:

- `scripts/projector`: pre-prompt private-context projection.
- `scripts/recorder`: post-turn recording from committed LionClaw transcript records.
- `scripts/context`: manual operator inspection, correction, deletion, and audit.

The runtime never receives direct access to the store. Projected context is
advisory prompt context only, and LionClaw core remains the final authority for
trust tier, prompt policy, budgets, rendering, and audit.

Do not infer profile or memory facts from arbitrary conversation. Store exact
committed turns for episodic recall, and create durable profile or memory records
only from manual operator commands or explicit user directive lines:

- `remember: <text>`
- `remember that <text>`
- `assistant style: <text>`
- `assistant workflow: <text>`
- `assistant default: <text>`
- `user preferences: <text>`
- `user standing requests: <text>`

The skill must stay deterministic: no model calls, no embeddings, no runtime DB
access, no LionClaw core DB reads, and no hidden transport or channel logic.
