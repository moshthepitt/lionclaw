---
name: lionclaw-private-context
description: Host-side private context projector for explicit LionClaw profiles and memory.
---

# LionClaw Private Context

This skill owns LionClaw's default local private-context store. It projects
operator-managed assistant profile, user profile, and retrieved memory records
through LionClaw's private-context projector boundary.

The runtime never receives direct access to the store. Projected context is
advisory prompt context only, and LionClaw core remains the final authority for
trust tier, prompt policy, budgets, rendering, and audit.
