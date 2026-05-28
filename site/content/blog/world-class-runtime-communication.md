+++
title = "World-class runtime communication is agent-to-agent without a platform"
date = 2026-05-28
description = "Team-local turns project instances into addressable local peers while keeping routing, delivery, and audit in the LionClaw kernel."
draft = true
+++

Agent-to-agent communication is easy to overbuild.

You can make a bus. You can make a new message DSL. You can make every runtime
parse special tags in its final answer. You can add a team platform beside the
local project. Each choice can work, but each also creates another authority
that has to be secured, explained, tested, and repaired.

The Codex-assisted team-local work kept rejecting that direction. The goal was
more specific: let local project instances talk to each other while preserving
LionClaw's existing boundary. No shadow registry. No core-owned provider
transport. No new DSL. No long setup ceremony. No partial sends when a
recipient name is wrong.

That is the right bar for runtime communication in LionClaw.

## The primitive

A LionClaw project can have instances: `main`, `reviewer`, `qa`, `ops`, or any
other local role. Each instance has its own home, sessions, audit, runtime
state, skills, and default work root.

Project-backed runtimes receive a generated read-only inventory:

```text
LIONCLAW_PROJECT_INSTANCE=<selected instance>
LIONCLAW_PROJECT_INSTANCES_FILE=/lionclaw/project/instances.json
```

That file is a projection, not raw `.lionclaw` metadata. When the execution
preset allows `channel.send`, the projection can also show sender-relative
routes to configured neighbors. Only configured routes expose channel and
conversation refs. Misconfigured or missing routes stay explicit without
becoming runtime authority.

The runtime-facing `team-local` skill stays small:

```sh
/lionclaw/skills/team-local/scripts/list
/lionclaw/skills/team-local/scripts/resolve reviewer
/lionclaw/skills/team-local/scripts/send reviewer qa -- "Please review this."
```

The helper resolves every recipient before opening the bridge. Duplicate
recipients are rejected. Empty recipients are rejected. If any recipient cannot
be resolved, nothing is sent. For successful sends, each recipient gets an
idempotent outbox path through the normal `channel.send` contract. Attachments
must live under `/runtime`. Message format and reply refs are provider-neutral.

## Where the boundary sits

The worker is a bundled first-party channel skill, with a Rust worker because
the job is local delivery, identity checking, filesystem discipline, and
outbox polling. It still is not core transport. It uses normal channel APIs.

Before delivery, the worker verifies the local daemon identity and canonical
home path. It verifies the target daemon identity and expected home. The target
daemon still authorizes the sender. Health is reported at a controlled cadence,
not on every idle polling tick. Delivery goes through the durable outbox, and
attachments are staged through the kernel-owned path.

The important thing is that a runtime never gets to say "I am main, send this
as reviewer." It gets a projection from its own point of view and a send helper
that opens a turn-scoped `channel.send` bridge only when the preset allows it.
The kernel validates the route, content, idempotency, active turn, attachment
paths, authorization, and audit record.

## Against the field

Hermes has useful delegation. A parent can spawn child agents with isolated
context and restricted tools. That is good for parallel subtasks, but the docs
also make the tradeoff clear: delegation is synchronous and not durable; if the
parent is interrupted, active children are cancelled and in-progress work is
discarded. LionClaw's team-local path is different. Peers are durable project
instances, and their messages are normal channel turns.

NanoClaw has one of the strongest competitor shapes for agent-to-agent work. It
keeps a per-session destination map, treats that map as both routing table and
container-visible ACL, and revalidates on host delivery. That is serious work.
Its current code also documents scar tissue: a test explicitly captures an
unbounded ping-pong gap in A2A routing. LionClaw's answer is to keep routing
through projected routes, all-recipient preflight, durable idempotent outbox
delivery, target authorization, and audit rather than relying on prompt-shaped
message markup as the transport contract.

OpenClaw is broad here too. It has multi-agent concepts, background subagents,
session tools, push completion, and channel delivery. It is a mature reference
for product breadth. The distinction is again the LionClaw stance: project
instance identity, local runtime confinement, and generated inventory are the
default boundary. Team-local is not a hosted workspace layer and not a general
gateway feature; it is a local project channel.

IronClaw has strong secure-runtime instincts and broad channel support. In the
inspected docs, though, I did not find the same project-instance peer primitive
as team-local. That leaves LionClaw with a sharper local-team story: real agent
harnesses behind separate local homes, addressable by project role, with the
kernel owning grants, delivery, and audit.

## What this unlocks

This makes a local project feel like a small team without becoming a team
platform.

`main` can ask `reviewer` for a second pass. `reviewer` can send findings back.
`qa` can ask for reproduction notes. A runtime can use Codex, OpenCode, or a
future real agent harness, because the harness is behind the boundary rather
than being the boundary.

That is the product idea behind world-class runtime communication in LionClaw:
agents can coordinate, but the coordination path is still local, explicit,
audited, and small enough to understand.

Build notes: this post is extracted from the v0.6 follow-up work in
[#101](https://github.com/moshthepitt/lionclaw/pull/101),
[#102](https://github.com/moshthepitt/lionclaw/pull/102),
[#104](https://github.com/moshthepitt/lionclaw/pull/104),
[#107](https://github.com/moshthepitt/lionclaw/pull/107),
[#108](https://github.com/moshthepitt/lionclaw/pull/108), and
[#109](https://github.com/moshthepitt/lionclaw/pull/109).
