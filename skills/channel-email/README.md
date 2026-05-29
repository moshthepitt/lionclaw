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
`BODYSTRUCTURE`, and `RFC822.SIZE`. It also requires a trusted provider
`Authentication-Results` header proving the `From` domain passed aligned sender
authentication before a permanent sender grant can admit mail. Full MIME bodies
and attachments are fetched only after LionClaw authorizes the sender and the
sender-auth check passes, and only within `EMAIL_MAX_MESSAGE_BYTES`.

Unknown senders are held with metadata only. Automated, bulk, list, bounce, and
no-reply mail is suppressed locally. Oversized mail is suppressed without
runtime work, and known provider size metadata is retained with held rows so a
later one-shot release still avoids downloading mail already known to exceed
the configured cap. Mail that lacks trusted sender authentication is held with
metadata only even when a permanent sender grant or one-shot release exists. A
release approves a specific held message from an authenticated sender; it does
not override failed provider sender authentication.

Held-mail operator digests are sent directly by the channel worker through the
same mailbox transport, not through LionClaw's user-facing outbox. Each delivery
attempt is recorded in worker-local SQLite and reported through normal channel
health so `doctor` can show digest delivery failures without contacting the
mailbox provider.

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

The worker admits only the held item whose id exactly matches that label and
still requires the message to satisfy the configured sender-authentication
policy. Other mail from the same sender remains held while the release grant
exists. Once the matching held item is admitted, remains held for failed sender
authentication, or is terminally suppressed, the worker consumes that exact
labeled grant through LionClaw's grant-consume API. Consumption removes the
temporary release grant without leaving a revoked sender scope; failed
consumptions are retried from worker-local SQLite state.

Permanent sender approval uses the same command without the one-shot release
label:

```bash
"$LIONCLAW_BIN" channel pairing approve email --sender-ref email:addr:alice@example.com
```

## Setup

Run this only with a dedicated mailbox, not a personal inbox.

### Basic Auth

```bash
cd "$PROJ_A"
cat > email.env <<'EOF'
EMAIL_ADDRESS=assistant@example.com
EMAIL_AUTH_RESULTS_HOST=mx.example.com
EMAIL_AUTH_MODE=basic
EMAIL_IMAP_HOST=imap.example.com
EMAIL_IMAP_USERNAME=assistant@example.com
EMAIL_IMAP_PASSWORD=...
EMAIL_SMTP_HOST=smtp.example.com
EMAIL_SMTP_USERNAME=assistant@example.com
EMAIL_SMTP_PASSWORD=...
EMAIL_SMTP_TLS=starttls
EMAIL_ADMIN_DIGEST_TO=operator@example.com
# Optional: defaults to 50 MiB.
EMAIL_MAX_MESSAGE_BYTES=52428800
EOF
"$LIONCLAW_BIN" connect email --env-file ./email.env
"$LIONCLAW_BIN" doctor
```

### OAuth2 / XOAUTH2

For providers that require OAuth, use the provider profile on `connect`.
LionClaw installs the bundled email channel snapshot, runs the channel's setup
helper from that snapshot, stores the generated channel env in the selected
instance home, and starts the background worker. The generated env points
`EMAIL_XOAUTH2_TOKEN_CMD` back to `lionclaw-channel-email oauth2 token`, so the
worker refreshes access tokens through the same packaged channel binary.
On a TTY, `"$LIONCLAW_BIN" connect email` starts the same setup helper as a
guided prompt.

Gmail:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" connect email gmail \
  --account assistant@gmail.com \
  --client-secret-json "$HOME/Downloads/client_secret_desktop.json" \
  --admin-to operator@example.com
"$LIONCLAW_BIN" doctor
```

The Google OAuth desktop client JSON is the credentials file downloaded from a
Google Cloud OAuth client whose application type is `Desktop app`. It contains
the OAuth `client_id` and client metadata for your own Google Cloud project; it
is not the mailbox password. Add the mailbox account as a test user if the
consent screen is still in testing. The Gmail preset uses `imap.gmail.com:993`,
`smtp.gmail.com:587`, `mx.google.com`, and the `https://mail.google.com/`
scope. Google device-code OAuth is not suitable for Gmail IMAP/SMTP because
Google's limited-input device flow does not include the Gmail mail scope. Use
the default loopback browser flow; on a remote shell, run with a fixed `--port`
and forward that port to `127.0.0.1`.
See Google's [Gmail IMAP/SMTP documentation][gmail-imap-smtp],
[XOAUTH2 mechanism documentation][gmail-xoauth2], and
[limited-input device flow documentation][google-device-flow].

Microsoft 365 and Outlook.com:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" connect email microsoft365 \
  --tenant common \
  --account assistant@example.com \
  --client-id 00000000-0000-0000-0000-000000000000 \
  --auth-results-host mx.example.com \
  --admin-to operator@example.com
"$LIONCLAW_BIN" doctor
```

Register the Microsoft app as a public/native client, add
`http://localhost/oauth2/callback` as a redirect URI, and grant delegated
permissions for IMAP and SMTP. Microsoft ignores the ephemeral port for
matching localhost redirect URIs. The Microsoft preset uses
`outlook.office365.com:993`, `smtp.office365.com:587`, and the documented
delegated scopes `offline_access`,
`https://outlook.office.com/IMAP.AccessAsUser.All`, and
`https://outlook.office.com/SMTP.Send`. Microsoft mailbox
`Authentication-Results` authserv-ids vary by tenant and routing path, so set
`--auth-results-host` from trusted provider headers for the dedicated mailbox.
See Microsoft's [IMAP/SMTP OAuth documentation][ms-imap-smtp-oauth] and
[redirect URI documentation][ms-redirect-uri].

Other OAuth2 IMAP/SMTP providers use the same connect form with explicit
provider facts:

```bash
"$LIONCLAW_BIN" connect email generic \
  --account assistant@example.com \
  --client-id client-id \
  --auth-url https://accounts.example.com/oauth2/authorize \
  --token-url https://accounts.example.com/oauth2/token \
  --scope imap.send \
  --scope smtp.send \
  --auth-results-host mx.example.com \
  --imap-host imap.example.com \
  --smtp-host smtp.example.com
```

To replace existing OAuth state for the same provider/account, pass `--force`
after the provider profile. For direct helper use outside `connect`, the same
implementation remains available:

```bash
EMAIL_CHANNEL_BIN="${EMAIL_CHANNEL_BIN:-$(command -v lionclaw-channel-email)}"
"$EMAIL_CHANNEL_BIN" oauth2 setup \
  --provider gmail \
  --account assistant@gmail.com \
  --client-secret-json "$HOME/Downloads/client_secret_desktop.json" \
  --env-file ./email.env
"$LIONCLAW_BIN" connect email --env-file ./email.env
```

The generated `EMAIL_XOAUTH2_TOKEN_CMD` starts with the absolute installed
channel helper path. The worker parses arguments, executes the helper without a
shell, closes stdin, drops stderr, reads one UTF-8 access token from stdout,
enforces a 10 second timeout and a 16 KiB stdout cap, and never logs the token.
OAuth refresh state is stored in a private local state file; LionClaw consumes
only the short-lived access token and never projects it to runtimes.

`EMAIL_AUTH_RESULTS_HOST` must match the mailbox provider's trusted
`Authentication-Results` authserv-id, such as `mx.google.com` for many Gmail
deliveries. This is safe only when that provider strips attacker-supplied
`Authentication-Results` headers for the same authserv-id or prepends its own
authoritative result before any untrusted copy. Do not point this setting at an
authserv-id that can be supplied by external senders.

`EMAIL_SMTP_TLS` is `starttls` by default for port 587 and `implicit` by
default for port 465. Use `insecure` only for loopback or private test relays;
production mailbox providers should use `starttls` or `implicit`.

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
- a forged or unauthenticated `From` stays held even if that sender has a
  permanent grant or exact one-shot release
- admitted email uses `thread_actor` session binding so separate threads from
  the same sender do not share runtime history
- an unknown non-automated sender is held and does not queue runtime work
- the held-mail digest includes held id, sender, subject, date, size,
  attachment count, sender-auth verdict, sender/conversation/thread/message
  refs, held reason, and hold-reason-specific approval, block, or release
  guidance without downloading provider body previews
- failed held-mail digest sends are recorded in worker-local audit state and
  appear in `doctor` through the `email.digest` worker health check
- `channel pairing approve email --sender-ref ... --label email-release:<held-id>`
  releases only that held item once, leaves mismatched mail held, and is
  consumed after a terminal local outcome
- automated/list/bounce/no-reply mail is suppressed without auto-reply
- mail above `EMAIL_MAX_MESSAGE_BYTES` is suppressed without runtime work,
  including later one-shot release of held mail already known to exceed the cap
- attachments are staged only after admission
- repeated outbox delivery attempts do not send duplicate SMTP replies
- public OAuth smoke starts with Microsoft OAuth + IMAP TLS + held unknown
  sender + approval + post-admission body fetch + SMTP reply; run Gmail OAuth
  as a separate smoke with loopback, SSH-forwarded loopback, or pre-provisioned
  helper state; keep generic IMAP/SMTP smoke coverage for non-OAuth providers

## Packaged Assets

Packaged builds that include this channel must keep these assets together:

- `lionclaw-channel-email`
- `skills/channel-email/`

[gmail-imap-smtp]: https://developers.google.com/gmail/imap/imap-smtp
[gmail-xoauth2]: https://developers.google.com/workspace/gmail/imap/xoauth2-protocol
[google-device-flow]: https://developers.google.com/identity/protocols/oauth2/limited-input-device
[ms-imap-smtp-oauth]: https://learn.microsoft.com/en-us/exchange/client-developer/legacy-protocols/how-to-authenticate-an-imap-pop-smtp-application-by-using-oauth
[ms-redirect-uri]: https://learn.microsoft.com/en-us/entra/identity-platform/reply-url
