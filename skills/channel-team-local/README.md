# Team Local Channel

`channel-team-local` is LionClaw's bundled first-party channel for local
project instance communication. It lets sibling instances on the same machine
deliver messages through each instance's own LionClaw daemon while still using
the normal channel APIs, grants, outbox, and audit trail.

This README owns the team-local channel's setup, architecture, QA, and packaged
asset notes.

## Architecture

The installed snapshot is self-contained. `scripts/worker` execs
`runtime/team-local/bin/lionclaw-channel-team-local` from the skill directory,
and install plumbing copies the compiled worker binary into that snapshot before
computing the installed skill hash.

The same binary backs the runtime-facing `runtime/team-local/scripts/list` and
`runtime/team-local/scripts/resolve` helpers for sender-side team discovery.
Runtime sends use the LionClaw-projected `channel_send` MCP tool with routes
from the projected inventory. Attachments are path-only; the kernel derives the
delivered filename and media type from the runtime file when it prepares the
channel-send attachment.

The worker is a separate Rust workspace crate named
`lionclaw-channel-team-local`; it does not depend on the `lionclaw` crate.

## Project Setup

Project setup installs and configures `team-local` for project instances by
default. It also ensures a `team-local` execution preset with the `channel-send`
escape class. That preset becomes the default only when the instance has no
default preset yet.

Each instance publishes its own contact route as:

```text
team-local:peer:<home-id>
```

Setup also approves existing sibling instances with ordinary direct channel
grants:

```text
channel_id = team-local
sender_ref = team-local:instance:<sibling-home-id>
routing_profile = direct
trust_tier = main
```

The worker delivers only through existing channel APIs. It pulls local outbox
deliveries, resolves the target route from project instance state, verifies the
target daemon is `lionclawd` with the expected home id and canonical home path,
preflights `/v0/channels/authorize`, then posts inbound events and attachments
to the target daemon. Attachment bytes move through the existing attachment
stage/finalize endpoints.

Team-local is address-only: explicit sends target instance names rather than
provider message threads. If a local outbox delivery contains a reply ref, the
worker reports a terminal `unsupported_reply_ref` failure and does not post it
to the target instance.

## Manual QA

Run Phase 1 of the repo-root `docs/MANUAL_QA.md` through `project init` and
`instance create reviewer`, then check the bundled channel artifacts for each
project instance:

```bash
test -x "$PROJ_A/.lionclaw/instances/main/skills/team-local/runtime/team-local/bin/lionclaw-channel-team-local"
test -x "$PROJ_A/.lionclaw/instances/main/skills/team-local/runtime/team-local/scripts/list"
test -x "$PROJ_A/.lionclaw/instances/main/skills/team-local/runtime/team-local/scripts/resolve"
test -x "$PROJ_A/.lionclaw/instances/reviewer/skills/team-local/runtime/team-local/bin/lionclaw-channel-team-local"
test -x "$PROJ_A/.lionclaw/instances/reviewer/skills/team-local/runtime/team-local/scripts/list"
test -x "$PROJ_A/.lionclaw/instances/reviewer/skills/team-local/runtime/team-local/scripts/resolve"
"$LIONCLAW_BIN" --instance main channel pairing list --channel-id team-local
"$LIONCLAW_BIN" --instance reviewer channel pairing list --channel-id team-local
```

Expected:

- `main` and `reviewer` have the bundled `team-local` channel installed with an
  embedded Rust worker and inventory helper binary
- runtime `list` and `resolve` helpers are present and executable
- direct sibling grants exist for local project-instance communication
- fresh setup creates a `team-local` preset with `channel-send`
- with that preset active and a configured neighbor contact, the runtime
  inventory projection uses schema version 2, leaves the selected instance
  entry identity-only, and exposes route fields only for neighbors whose
  channel is active in the sender

## Packaged Assets

Packaged builds that include this channel must keep these assets together:

- `lionclaw-channel-team-local`
- `skills/channel-team-local/`
