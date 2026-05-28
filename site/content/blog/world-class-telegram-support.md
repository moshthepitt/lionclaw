+++
title = "World-class Telegram support is a trust-boundary feature"
date = 2026-05-28
description = "LionClaw's Telegram channel is not just a bot wrapper; it is a scoped, audited path into real project agents."
draft = true
+++

Telegram support looks like a channel feature. In LionClaw it is really a
trust-boundary feature.

The early Codex design sessions kept returning to the same correction:
Telegram must be a skill, not Rust core. The runtime should not matter when an
operator installs Telegram. The runtime is chosen later, when an admitted turn
actually runs. That distinction sounds small until the product has real DMs,
groups, topics, files, callbacks, progress updates, final delivery, and
cancellation. If those details leak into core, every provider becomes a new
security model.

LionClaw's Telegram work is the opposite. Telegram is rich at the edge, but the
kernel owns admission, session binding, attachment staging, outbox delivery,
health, cancellation, and audit.

## What world-class means here

World-class Telegram support is not just "the bot replies."

It means a Telegram user, group, topic, message, and file become stable
provider refs before they cross the boundary. It means a `/start` link or
group invite creates a scoped grant, not ambient trust. It means a group grant
does not imply every sender in the group is trusted, and a DM grant does not
quietly authorize a group. It means `/status`, `/stop`, `/settings`,
`/connections`, and group `/ask` run as channel controls without giving the
provider authority over runtime state.

It also means provider polish uses the same contracts as everything else:
polling or webhook intake, webhook secret-token verification, typing signals,
progress edits, final outbox deliveries, Telegram-safe HTML, native media
groups, topic replies, actor-bound callbacks, restart-tolerant retries, and
health checks that `doctor` can report without learning Telegram's API.

The attachment path matters most. Telegram can send photos, documents, audio,
voice, video, stickers, animations, albums, locations, and venues. LionClaw
does not hand raw provider paths to the runtime. The worker describes and
stages files only after the kernel asks for attachments. The runtime receives
the kernel-owned projection. Final answers and generated files go back out
through the durable outbox.

## Against the field

IronClaw has serious Telegram basics: bot setup, DM policies, allowlists,
pairing, group modes, polling, webhook secret support, and troubleshooting.
That is a credible channel. LionClaw's advantage is not pretending IronClaw
lacks Telegram. The advantage is the boundary shape: Telegram remains a skill,
while grants, normalized refs, staged attachments, durable delivery, and audit
are kernel contracts shared by every channel.

Hermes is broad and impressive on Telegram surface area. Its docs cover text,
voice, images, files, webhooks, proxies, home channels, cron topics, group
mention policy, private chat topics, and even local Bot API support for large
files. It also exposes the important operational trap: an agent can emit a
`MEDIA:/path` that the host gateway cannot read. LionClaw beats that class of
failure by refusing to make host-readable paths the media contract. Runtime
artifacts are staged through LionClaw-owned attachment and outbox paths.

NanoClaw has a good setup story and a broad channel philosophy. Telegram is an
installable adapter, pairing is operator-friendly, and its host/container split
is a useful reference point. LionClaw's edge is that Telegram does not become a
fork-installed special case. It plugs into the same channel contract as email,
team-local, and future providers, with the same health, grants, outbox, and
runtime boundary.

OpenClaw is the strongest comparison on feature breadth. Its Telegram docs
cover long polling, webhooks, group policy, topics, live stream previews,
formatting fallback, command menus, inline buttons, message actions, and a
large `openclaw message` surface. LionClaw does not beat that by checkbox
count. It beats it on product stance: a smaller trusted core, external channel
skills, real local runtime confinement, and provider-rich behavior that still
enters through deterministic admission and delivery contracts.

## The claim

Telegram is one of the highest-risk ways to reach an agent. It is public,
social, noisy, file-heavy, and easy to confuse with human intent.

So the standard is simple: if Telegram can reach a project agent, it should
enter as an authorized, scoped, replayable, inspectable event. If the agent
responds, that response should leave through a durable, auditable outbox. If a
human presses stop, the callback should be bound to the actor and active turn.
If a file moves, the kernel should own the handoff.

That is why this is world-class Telegram support for LionClaw. It is not
Telegram as a convenience wrapper. It is Telegram as a first-class channel skill
inside a small trusted boundary.

Build notes: this post is extracted from the v0.6 follow-up work in
[#72](https://github.com/moshthepitt/lionclaw/pull/72),
[#74](https://github.com/moshthepitt/lionclaw/pull/74),
[#75](https://github.com/moshthepitt/lionclaw/pull/75),
[#78](https://github.com/moshthepitt/lionclaw/pull/78),
[#80](https://github.com/moshthepitt/lionclaw/pull/80),
[#81](https://github.com/moshthepitt/lionclaw/pull/81),
[#92](https://github.com/moshthepitt/lionclaw/pull/92),
[#94](https://github.com/moshthepitt/lionclaw/pull/94), and
[#95](https://github.com/moshthepitt/lionclaw/pull/95).
