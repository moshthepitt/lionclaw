# LionClaw Private Context

`lionclaw-private-context` is LionClaw's bundled first-party private-context
skill. It stores explicit local assistant profile, user profile, and memory
records in host-only state, then projects selected context through LionClaw's
existing private-context projector boundary.

Enable it by installing the skill and selecting it in operator config:

```toml
[private_context]
projector_skill = "lionclaw-private-context"
```

The skill-owned operator command is `scripts/context`. It expects
`LIONCLAW_SKILL_STATE_DIR` to point at the selected skill state directory.
