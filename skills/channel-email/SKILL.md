---
name: channel-email
description: Host-side Email work-inbox channel for LionClaw.
---

# Email Channel

This skill runs the host-side Email channel worker. It owns mailbox credentials,
IMAP/SMTP transport, held-mail state, and provider-specific email handling. The
runtime never receives mailbox credentials.

The channel admits only exact approved senders through LionClaw channel grants.
Unknown mail is held or suppressed by the worker and must not become runtime
work unless the local operator explicitly approves the sender or releases an
authenticated held item. Full message bodies are fetched only after approval
and are capped by `EMAIL_MAX_MESSAGE_BYTES` before any runtime work is queued.

This channel package also carries the runtime-facing `email` Agent Skill at
`runtime/email/`. LionClaw projects that facet into runtimes when this skill is
bound as the `email` channel, while keeping the host-side channel package and
mail transport credentials out of the runtime.

Detailed architecture, setup, QA, one-shot release behavior, and packaged asset
notes for this channel live in `README.md` in this skill directory.
