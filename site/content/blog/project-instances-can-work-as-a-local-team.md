+++
title = "Project instances can work as a local team"
date = 2026-05-28
description = "The latest post-v0.6 work gives sibling project instances a local channel path without turning LionClaw into a team platform."
draft = true
+++

LionClaw projects have instances. After v0.6, those instances became much more
useful.

An instance is not just a name in a config file. It has its own home, runtime
state, sessions, audit, installed skills, and default work root. That makes it
natural to run separate local assistants for separate roles: `main`,
`reviewer`, `qa`, `ops`, or whatever the project needs.

The missing piece was a local way for those instances to see and reach each
other without giving runtimes raw access to `.lionclaw/` metadata and without
building a separate team platform inside the core.

The recent team-local work fills that gap.

The Codex-assisted build process made the shape sharper than the first idea.
The goal was not "add an agent bus." It was a world-class local communication
path that did not create a shadow registry, did not invent a new routing DSL,
did not put transport logic in core, and did not require an operator to run a
long setup ceremony. The later review loop tightened the practical details:
verify daemon identity before delivery, publish health without flooding audit,
reject partial sends when a recipient cannot be resolved, and keep message
content on the existing `channel.send` contract.

## Runtimes get a projection, not the project internals

Project-backed program runtimes now receive a generated inventory projection:

```text
LIONCLAW_PROJECT_INSTANCE=<selected instance>
LIONCLAW_PROJECT_INSTANCES_FILE=/lionclaw/project/instances.json
```

That file tells the runtime which instance it is and which sibling instances
exist. It is read-only. It is generated. It is not a mount of raw project
metadata.

When contact-aware routing is configured, the projection can also include
sender-relative `channel_send` routes for neighbor instances. A runtime can see
only the configured routes that matter from its own point of view. If a
neighbor contact changes, the daemon fingerprint changes so stale project
daemons do not keep serving old routing state.

That is the right boundary: enough structure for useful local collaboration,
not enough structure to turn private LionClaw state into runtime authority.

## Session binding keeps contact from becoming authority

Channels can now request a durable session binding after admission.

That phrase is deliberately narrow. The binding selects where an authorized
turn lands: grant, actor, conversation, thread, conversation plus actor, or
thread plus actor. It does not grant route access. It does not grant actor
access. The normal channel gates still run first.

The kernel derives canonical session keys itself and rejects keys that do not
round-trip through its generator. Workers cannot smuggle raw key strings,
templates, or metadata-derived identity into the session table.

This matters for project teams because different contacts need different
durable session shapes. The sender may be the session. The conversation may be
the session. A thread may be the session. LionClaw can now express that without
letting the worker define identity.

## Direct grants can be approved without a ceremony

The direct-grant approval path is the small administrative piece that makes
known local contacts usable.

The kernel can approve inbound direct, conversation, and thread grants through
the existing grant store. It preserves blocked and revoked scopes, keeps exact
approvals idempotent, closes matching pending pairing requests, and records the
approval in audit.

This keeps approval in the channel model that already exists. There is no new
trust database and no hidden team registry.

## Team-local is a channel skill

The first concrete user of this work is `team-local`.

`team-local` is a bundled first-party channel, but it still follows the channel
shape. The worker uses existing channel APIs: outbox pull/report, daemon info,
authorization, inbound delivery, attachment staging, and health reporting.
Project setup installs it for project instances by default, so sibling
instances get local preferred contacts without extra operator choreography.

The identity is local and stable. Instances use project home IDs as local peer
identity, and direct grants are created between sibling homes. Before delivery,
the worker verifies daemon identity, expected home id, and canonical home path.
The recipient daemon still authorizes the sender.

The runtime-facing skill is intentionally simple:

```sh
/lionclaw/skills/team-local/scripts/list
/lionclaw/skills/team-local/scripts/resolve reviewer
/lionclaw/skills/team-local/scripts/send reviewer -- "Please review the patch."
```

The helper resolves recipients from the generated project inventory before it
opens the channel-send bridge. That means stale or misspelled recipient names
fail before partial sends begin.

The latest follow-up kept message content provider-neutral. A runtime can send
plain, markdown, or HTML text, include runtime-scoped attachments, and set a
reply reference. Core `channel.send` still owns the validation, idempotency,
authorization, attachment root checks, audit, and outbox enqueueing.

## Local teamwork without a platform

This is the important product shape:

- `main` can ask `reviewer` for a second pass.
- `reviewer` can send a result back through the same channel system.
- The route is local to the project.
- The kernel owns authorization and audit.
- The channel remains a skill.
- The runtime sees only a generated projection and a controlled send helper.

That is not a hosted team product. It is a local project gaining enough
structure for multiple assistants to cooperate inside an explicit boundary.

LionClaw does not need to own every private step inside an agent harness. It
does need to own the boundary where project assistants discover each other,
send work, and leave records. Team-local is the first practical version of that
boundary.

Build notes: this post is extracted from the v0.6 follow-up work in
[#101](https://github.com/moshthepitt/lionclaw/pull/101),
[#102](https://github.com/moshthepitt/lionclaw/pull/102),
[#104](https://github.com/moshthepitt/lionclaw/pull/104),
[#107](https://github.com/moshthepitt/lionclaw/pull/107),
[#108](https://github.com/moshthepitt/lionclaw/pull/108), and
[#109](https://github.com/moshthepitt/lionclaw/pull/109).
