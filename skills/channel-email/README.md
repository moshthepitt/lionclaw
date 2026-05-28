# Email Channel

`channel-email` is LionClaw's bundled first-party work-inbox channel for a
dedicated mailbox. It owns mailbox credentials, IMAP/SMTP transport, held-mail
state, sender admission, threading, and delivery. Runtime agents never receive
mailbox credentials or raw mail transport access.

This README owns the email channel's architecture, setup, QA, and release
behavior. The runtime-facing email Agent Skill lives at `runtime/email/`.

## Architecture

The worker treats the mailbox as a strict direct-sender channel. Permanent
approval is an exact direct grant for a normalized sender ref such as:

```text
email:addr:alice@example.com
```

Email turns use DM-style admission plus `thread_actor` session binding. A direct
sender grant admits the sender, while LionClaw derives a narrower durable
session key for that sender and email thread. This keeps independent email
threads from sharing runtime history without requiring route grants for every
thread.

Before authorization, the worker fetches only bounded IMAP header facts,
`BODYSTRUCTURE`, and `RFC822.SIZE`. Full MIME bodies and attachments are fetched
only after LionClaw authorizes the sender, and only within
`EMAIL_MAX_MESSAGE_BYTES`.

Unknown senders are held with metadata only. Automated, bulk, list, bounce, and
no-reply mail is suppressed locally. Oversized mail is suppressed without
runtime work, and known provider size metadata is retained with held rows so a
later one-shot release still avoids downloading mail already known to exceed
the configured cap.

Processed IMAP candidates are marked seen in the dedicated mailbox after they
are held, suppressed, or admitted. This avoids reprocessing the same unread
window forever under fixed-size polling and keeps the channel's local held-mail
state as the operator-facing queue.

Admitted messages are posted through LionClaw's channel inbound and attachment
endpoints with first-class `thread_ref` and `reply_to_ref` fields. Worker-local
SQLite state is derived from the selected LionClaw home and mailbox identity, so
state placement follows the same instance boundary as the rest of the channel.

Held-message UID references are valid only while the mailbox `UIDVALIDITY`
value is unchanged. If `UIDVALIDITY` changes, stale held entries are suppressed
instead of fetching or marking a different provider message.

## One-Shot Release

One-shot held-message release uses a temporary direct sender grant labeled:

```text
email-release:<held-id>
```

Create that grant with the normal channel pairing operator surface:

```bash
"$LIONCLAW_BIN" channel pairing approve email --sender-ref email:addr:alice@example.com --label email-release:<held-id>
```

The worker admits only the held item whose id exactly matches that label. Other
mail from the same sender remains held while the release grant exists. Once the
matching held item is admitted or terminally suppressed, the worker revokes the
grant through LionClaw's grant-revoke API. Failed revocations are retried from
worker-local SQLite state.

Permanent sender approval uses the same command without the one-shot release
label:

```bash
"$LIONCLAW_BIN" channel pairing approve email --sender-ref email:addr:alice@example.com
```

## Setup

Run this only with a dedicated mailbox, not a personal inbox.

```bash
cd "$PROJ_A"
cat > email.env <<'EOF'
EMAIL_ADDRESS=assistant@example.com
EMAIL_IMAP_HOST=imap.example.com
EMAIL_IMAP_USERNAME=assistant@example.com
EMAIL_IMAP_PASSWORD=...
EMAIL_SMTP_HOST=smtp.example.com
EMAIL_SMTP_USERNAME=assistant@example.com
EMAIL_SMTP_PASSWORD=...
EMAIL_ADMIN_DIGEST_TO=operator@example.com
# Optional: defaults to 50 MiB.
EMAIL_MAX_MESSAGE_BYTES=52428800
EOF
"$LIONCLAW_BIN" connect email --env-file ./email.env
"$LIONCLAW_BIN" doctor
```

`connect email` installs the bundled channel snapshot, stores declared channel
env in the selected instance home, starts the background stack, and leaves
mailbox secrets out of runtime skill projection.

## Manual QA

Expected when credentials are available:

- `doctor` shows email worker health without printing mailbox secrets
- the channel-owned runtime skill facet is projected as the `email` skill for
  runtimes; no separate email companion skill install is needed
- an exact approved sender queues one channel turn with a structured email
  envelope, not raw MIME
- admitted email uses `thread_actor` session binding so separate threads from
  the same sender do not share runtime history
- an unknown non-automated sender is held and does not queue runtime work
- the held-mail digest includes held id, sender, subject, snippet, attachment
  count, sender/conversation/thread refs, and release guidance
- `channel pairing approve email --sender-ref ... --label email-release:<held-id>`
  releases only that held item once, leaves mismatched mail held, and is revoked
  after admission
- automated/list/bounce/no-reply mail is suppressed without auto-reply
- mail above `EMAIL_MAX_MESSAGE_BYTES` is suppressed without runtime work,
  including later one-shot release of held mail already known to exceed the cap
- attachments are staged only after admission
- repeated outbox delivery attempts do not send duplicate SMTP replies

## Packaged Assets

Packaged builds that include this channel must keep these assets together:

- `lionclaw-channel-email`
- `skills/channel-email/`
