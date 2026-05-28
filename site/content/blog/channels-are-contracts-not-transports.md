+++
title = "Channels are contracts, not transports"
date = 2026-05-26
description = "Channels v2 makes provider integrations external skills while the LionClaw kernel owns admission, durable delivery, and audit."
draft = true
+++

The channel work after v0.6 tightened one of LionClaw's core boundaries:
channels are skills, not kernel transports.

That sounds like an implementation detail until you follow it through.
Telegram, email, Slack, Signal, Discord, a local team channel, and whatever
comes next should not each teach the trusted Rust core a new social model. The
core should receive normalized facts, make deterministic admission decisions,
record audit, and own the durable state that matters.

Provider workers should own provider mechanics.

Channels v2 is that split made explicit.

The Codex conversations that shaped this were useful because they caught the
temptation to solve Telegram, email, or future channels by making the Rust core
know too much. The rule that survived was blunt: channels must be skills. The
runtime should not be chosen when Telegram is installed. Pairing, admission,
outbox delivery, cancellation, and audit belong to the kernel contract; provider
behavior belongs at the edge.

## Normalized facts, scoped grants

The old channel shape was replaced with a provider-neutral inbound contract.
Workers report sender refs, conversation refs, thread refs, triggers, message
refs, bounded provider metadata, text, and attachment descriptors.

The kernel then admits the event through scoped grants:

- `direct`
- `conversation`
- `thread`
- `outbound`

A direct message approval does not imply group approval. A group approval does
not trust every sender in that group. A thread route is not silently collapsed
into a conversation route.

Pairing moved in the same direction. Pairing codes and invite tokens are stored
hashed. Claims validate expiry, scope, max claims, existing grants, and scoped
blocks before creating a grant. Audit records normalized identity and outcome
facts, not raw provider payloads.

This is the security model LionClaw needs: transport data comes in, kernel
policy decides what it means.

## Durable turns need durable delivery

Channel delivery is not the same thing as stream acknowledgement.

A worker reading a stream cursor proves only that the worker observed a turn. It
does not prove that Telegram, email, or another provider accepted the final
answer. So channel replies now go through a durable outbox.

The kernel stores provider-neutral outbox messages and delivery attempts.
Workers lease messages through the outbox API and report delivered, retryable
failed, or terminal failed outcomes. Stale reports are rejected. Backoff belongs
to the kernel. Provider receipts and errors are recorded with the attempt.

That gives LionClaw a delivery history it can inspect later. It also means jobs
and channel turns use the same durable delivery path instead of inventing
parallel success meanings.

## Attachments are staged, not trusted

Attachments also became a kernel-owned lifecycle.

Workers can describe inbound attachments, stage uploaded bytes, and finalize a
batch. The runtime receives a typed manifest and a read-only `/attachments`
projection. Rejected, missing, oversized, and unstaged attachments are recorded
in the manifest so a useful turn can still run when there is usable text or at
least one usable file.

The hardening is intentionally boring: digest-derived storage paths,
symlink-checked projections, size and count limits, cleanup of stale staging
state, and no raw provider file path becoming runtime authority.

## Health and cancellation became first-class

Channels now report normalized health checks through the kernel. `doctor` can
show missing reports, stale reports, pending pairings, failed outbox messages,
future timestamp anomalies, and provider-specific check statuses without
learning Telegram's API.

Cancellation is channel-neutral too. A channel session can request cancellation
of an active turn by session scope and expected turn id. The kernel owns the
terminal state transition, emits the final stream events, and audits the
outcome. Workers do not need to invent their own cancellation semantics.

## Telegram proved the split

Telegram is the loud proof point because it has messy real-world behavior:
DMs, groups, supergroups, topics, callbacks, media groups, command menus,
webhooks, polling, typing indicators, provider limits, and delivery errors.

The Telegram skill now maps those mechanics into the normalized channel model.
It uses stable Telegram refs for identity and routing, handles pairing claims
without trusting display names, routes groups and topics explicitly, stages
media through the attachment path, delivers final answers through the outbox,
and reports health without making Rust core speak Telegram.

The polished Telegram surface adds `/help`, `/status`, `/stop`, settings,
connections, group `/ask`, inline controls, progress messages, reaction
feedback, secret-token verification for webhooks, and actor-bound callbacks.
Those are provider UX details. They stay in the skill.

At the same time, the old bundled terminal channel was removed. Tests now use a
provider-independent loopback fixture that exercises the channel contracts
without pretending the terminal channel is product surface.

## Runtimes can send, but the kernel routes

The last piece is outbound runtime sending.

Program-backed runtimes can receive a turn-scoped `channel.send` socket only
when the selected execution preset grants that escape. The runtime provides
content, attachments, and an idempotency key. The kernel validates the active
session, turn, route, binding, attachment paths, content shape, and delivery
fingerprint before enqueueing a normal outbox message.

The bridge is transport-only. It does not make the runtime a channel authority.
It gives the agent a controlled way to send through the channel route that the
kernel has already approved.

That is the direction for channels in LionClaw: provider-rich at the edges,
small and deterministic at the core.

Build notes: this post is extracted from the v0.6 follow-up work in
[#72](https://github.com/moshthepitt/lionclaw/pull/72),
[#74](https://github.com/moshthepitt/lionclaw/pull/74),
[#75](https://github.com/moshthepitt/lionclaw/pull/75),
[#78](https://github.com/moshthepitt/lionclaw/pull/78),
[#80](https://github.com/moshthepitt/lionclaw/pull/80),
[#81](https://github.com/moshthepitt/lionclaw/pull/81),
[#84](https://github.com/moshthepitt/lionclaw/pull/84),
[#92](https://github.com/moshthepitt/lionclaw/pull/92),
[#94](https://github.com/moshthepitt/lionclaw/pull/94), and
[#95](https://github.com/moshthepitt/lionclaw/pull/95).
