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
work unless the local operator explicitly approves or releases it. Full message
bodies are fetched only after approval and are capped by
`EMAIL_MAX_MESSAGE_BYTES` before any runtime work is queued.

This channel package also carries the runtime-facing `email` Agent Skill at
`runtime/email/`. LionClaw projects that facet into runtimes when this skill is
bound as the `email` channel, while keeping the host-side channel package and
mail transport credentials out of the runtime.

## Architecture

The worker owns IMAP, SMTP, mailbox credentials, local held-mail state, and
provider-specific email handling. It fetches only IMAP envelope/header facts,
`BODYSTRUCTURE`, and `RFC822.SIZE` before calling `/v0/channels/authorize`.
Full MIME bodies and attachments are fetched only after LionClaw grants the
sender/thread and only within the configured `EMAIL_MAX_MESSAGE_BYTES` cap.

Unknown senders are held with metadata only. Automated, bulk, list, bounce, and
no-reply mail is suppressed locally. Oversized mail is suppressed without
runtime work, and known provider size metadata is retained with held rows so
later one-shot releases still avoid downloading mail already known to exceed
the configured cap.

Admitted messages are posted through LionClaw's existing channel inbound and
attachment endpoints with first-class `thread_ref` and `reply_to_ref` fields.
Worker-local SQLite state is derived from the selected LionClaw home and mailbox
identity, so state placement follows the same instance boundary as the rest of
the channel and is not controlled by undeclared process environment.

Held-message UID references are valid only while the mailbox `UIDVALIDITY`
value is unchanged. If `UIDVALIDITY` changes, stale held entries are suppressed
instead of fetching or marking a different provider message.

## Runtime Facet

The runtime-visible `email` facet documents reply behavior and operator-driven
email administration. Permanent sender approval uses the existing channel grant
approval API for an exact `email:addr:<normalized-address>` sender ref.
One-shot held-message release uses an existing thread-scoped grant labeled
`email-release:<held-id>`; the worker recognizes that label on the authorization
response, admits the matching held item once, then revokes the grant through
`/v0/channels/grants/revoke`. Failed revocations are retried from worker-local
SQLite state.
