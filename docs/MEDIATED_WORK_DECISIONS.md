# Mediated Work Decisions

Status: provisional product decisions, April 7, 2026.

This note records the current product direction for "real mediated work" so we
can return to it later and implement from a stable product stance instead of
re-litigating the core shape.

## Problem Statement

LionClaw is not itself the agent. It launches a real agent runtime such as
Codex or OpenCode. That means LionClaw should not try to duplicate the
runtime's reasoning, planning, editing strategy, or full tool system.

The narrower product question is:

- what must LionClaw own to keep the trusted core meaningful
- what should remain runtime-owned to avoid redundancy
- what boundaries are real and enforceable

## Product Decisions

### 1. LionClaw stays a kernel, not a second agent

LionClaw will own:

- session and context assembly
- runtime profile selection
- policy and audit
- assistant-owned continuity and artifacts
- final delivery to the user/channel
- explicit boundary escapes such as network, secrets, and scheduling

LionClaw will not own:

- task planning
- code reasoning
- editing strategy
- shell strategy
- subtask decomposition inside a single runtime

### 2. Runtime confinement is the primary execution boundary

If the runtime is a normal host process, app-layer mediation of ordinary file
reads and writes is mostly redundant and not meaningfully enforceable.

LionClaw should therefore treat the runtime profile as the primary boundary:

- trusted host runtime
- confined read-only runtime
- confined workspace read-write runtime
- confined workspace read-write runtime with delivery or network extensions

The kernel should choose that profile deterministically before the turn starts.
If LionClaw launches a trusted host runtime, that is a convenience mode, not a
containment claim. In that mode LionClaw should not pretend to mediate
ordinary file work more strongly than the host process boundary actually does.

### 3. Most ordinary in-workspace work should remain runtime-owned

Inside an allowed runtime boundary, the runtime should be free to:

- read the mounted workspace
- edit files in the mounted workspace
- use its own internal multi-step workflow
- use its own internal subagents if the runtime supports them

LionClaw should not re-implement a large per-file or per-command permission
system for that ordinary in-boundary work.

### 4. LionClaw should broker only real boundary escapes

The broker surface should stay small and meaningful.

The main brokered classes are:

- proactive or out-of-band delivery
- secret-backed service usage
- network egress outside the default profile
- scheduler and background execution control

These are the places where kernel mediation adds real value.

### 5. Normal reply is not the same thing as `channel.send`

Replying to the current conversation is ambient session behavior, not a special
capability grant.

The final answer to the current bound origin should normally be allowed and
delivered through LionClaw's existing session/channel path.

This does not mean channels move into the Rust core as built-in transports.
Channels remain external skills and workers. LionClaw owns the session-bound
reply/delivery contract, while channel skills continue to own transport-specific
I/O.

`channel.send` should mean proactive or extra delivery, for example:

- sending a follow-up after the main answer
- sending to a different target
- background or scheduled notifications
- attachment delivery

### 6. Artifacts and attachments are different concepts

Artifacts are durable assistant-owned outputs recorded in LionClaw state.
Examples:

- a generated report
- a daily brief
- a JSON result from a scheduled job

Attachments are delivery payloads for a channel transport.
Examples:

- sending a PDF to Telegram
- uploading a PNG to Slack
- attaching a CSV in email delivery

A file may become both:

- first an artifact
- later an attachment for a delivery action

But "artifact" is a memory/state concept, while "attachment" is a delivery
concept.

### 7. Outbox is optional and should stay narrow

Outbox is not the default path for ordinary replies.

Outbox only makes sense for:

- deferred delivery
- structured deliverables
- attachment staging
- background-job outputs awaiting delivery
- multiple candidate drafts

For direct interactive reply text, LionClaw probably does not need an outbox in
v1.

### 8. Secrets split into three classes

LionClaw should treat secrets as three different product problems.

Runtime auth secrets:

- credentials used to run the runtime itself
- not part of the mediated-work design

LionClaw-owned integration secrets:

- channel tokens
- webhook secrets
- scheduler or delivery integration secrets
- these should remain host-side and not be exposed to the runtime as plaintext

Runtime automation secrets:

- arbitrary external service credentials used directly by the runtime
- if allowed, these imply a higher-trust runtime profile
- LionClaw should not pretend the runtime cannot access them

LionClaw should prefer brokered secret-backed actions for product-owned
integrations, and only use runtime-injected secrets for explicitly higher-trust
automation modes.

### 9. LionClaw should not proxy every protocol

LionClaw should not try to proxy arbitrary SMTP, IMAP, FTP, or generic internet
automation just to keep the runtime from ever touching credentials.

That would bloat the core and duplicate the runtime.

Instead:

- broker product-owned integrations
- keep arbitrary automation in explicit higher-trust runtime profiles

### 10. Internal runtime orchestration stays runtime-owned

If Codex or OpenCode wants to launch internal subagents inside its own boundary,
LionClaw should generally let it.

LionClaw should care about:

- the outer runtime profile
- the resource budget
- the audit trail for the session/job

LionClaw should not try to micromanage internal agent decomposition.

Kernel-owned orchestration should be reserved for explicit outer workflows such
as:

- scheduled jobs
- separate sessions
- explicit cross-session fanout/fanin in future workflow features

### 11. Permissions should be coarse profiles, not a huge matrix

LionClaw should avoid a large per-tool or per-path permission surface.

The preferred control model is:

- coarse runtime profiles
- bound resources and delivery targets
- explicit brokered escape classes

This should feel more like execution policy than a giant tool-approval system.

## Working Model

LionClaw should decide mostly before the turn starts.

Session/job compilation should choose:

- runtime profile
- workspace mount
- assistant-home mount
- network mode
- delivery target
- whether outbox exists
- whether any brokered escape classes are enabled

During the turn, LionClaw should only adjudicate explicit escape requests.

## Why This Direction

This direction is based on three observations:

1. LionClaw's thesis is a small trusted core with extensibility through skills,
   not a full replacement agent.
2. Real users complain about approval fatigue, confusing sandbox states, and
   unclear workspace boundaries far more than they ask for larger permission
   matrices.
3. Without a real confinement boundary, app-layer mediation of ordinary file
   work is weak and redundant.

So the favored design is:

- a real runtime boundary
- a very small kernel-owned escape surface
- kernel-owned continuity, artifacts, audit, and delivery

## Deferred Questions

These are still open and should be decided when implementation starts:

- exact runtime profile set and naming
- whether v1 needs outbox at all
- whether attachment staging should be implemented before proactive delivery
- exact shape of brokered delivery requests
- how secret-backed service bindings should be configured and audited
- how much resource accounting LionClaw should do for runtime-owned internal
  subagents
