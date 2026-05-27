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
work unless the local operator explicitly approves or releases it.
