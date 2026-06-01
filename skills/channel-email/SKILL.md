---
name: channel-email
description: Host-side Email work-inbox channel for LionClaw.
---

# Email Channel

This skill runs the host-side Email channel worker. It owns mailbox auth,
IMAP/SMTP transport, held-mail state, and provider-specific email handling. The
runtime never receives mailbox credentials or OAuth tokens.

The channel admits only exact approved senders through LionClaw channel grants.
Unknown mail is held or suppressed by the worker and must not become runtime
work unless the local operator explicitly approves the sender or releases an
authenticated held item. Full message bodies are fetched only after approval
and are capped by `EMAIL_MAX_MESSAGE_BYTES` before any runtime work is queued.

Mailbox auth is either `EMAIL_AUTH_MODE=basic` with IMAP/SMTP passwords, or
`EMAIL_AUTH_MODE=xoauth2` with an absolute `EMAIL_XOAUTH2_TOKEN_CMD`; missing
`EMAIL_AUTH_MODE` defaults to basic for legacy configs. The normal OAuth2 setup
path is `lionclaw connect email <provider> ...`, which runs this channel's
setup helper from the installed channel snapshot. The channel binary also
exposes the lower-level `oauth2 setup` and `oauth2 token` commands; token
refresh runs host-side without a shell and prints only a short-lived access
token to stdout. Refresh-token state remains outside runtime projection.

This channel package also carries the runtime-facing `email` Agent Skill at
`runtime/email/`. LionClaw projects that facet into runtimes when this skill is
bound as the `email` channel, while keeping the host-side channel package and
mail transport credentials out of the runtime.

Detailed architecture, setup, QA, one-shot release behavior, and packaged asset
notes for this channel live in `README.md` in this skill directory.
