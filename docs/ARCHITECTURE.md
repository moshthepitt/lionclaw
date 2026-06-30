# LionClaw Architecture

LionClaw is a secure-first Claw: a small trusted kernel that turns real agent
CLIs into a persistent local assistant.

The agent does the reasoning and tool use. LionClaw gives it the local
contract around that work: sessions, channels, scheduled jobs, continuity,
runtime configuration, confinement, policy, and audit.

LionClaw currently targets Unix-like systems only. The direct `lionclaw run`
path is designed for Linux/macOS-style Unix environments. When attached to a
terminal, `run` opens the project operator console; `run --plain` and
non-terminal invocations use the line-oriented interactive path;
`run --runtime-tui` attaches the selected runtime's native terminal UI inside
the same LionClaw boundary. Managed background paths, including
`lionclaw up` and channel auto-start, currently use the systemd user manager;
launchd support is a future portability item.

## System Shape

```text
operator / channel / scheduler
        |
        v
  LionClaw kernel
        |
        | compiles runtime plan
        v
  confined agent runtime
  /workspace  /runtime  /drafts
```

The selected runtime may be a full agent harness such as Codex or OpenCode.
Those runtimes bring their own tool loops. LionClaw controls the outer
contract: where they run, what they can see, what network they get, what
secrets are mounted, which session invoked them, and which kernel decisions are
recorded.

## Trust Boundary

LionClaw splits responsibility into three classes.

### Kernel-Owned State

The Rust kernel owns:

- sessions and turn history
- runtime launch plans
- channel pairing, scoped grants, inbound queues, progress streams, durable outbox delivery
- scheduler definitions and run records
- policy grants and audit events
- assistant-home continuity files and derived search index
- the immutable applied runtime view loaded at startup from channel config and filesystem-installed skills

Kernel-owned mutations are policy checked where privileged and audited where
security or operator visibility matters.

### Runtime-Owned Work

Program-backed runtimes own their internal tool loops. The selected runtime can
use its native tools inside the boundary LionClaw gives it. LionClaw does not
mediate every private step inside those harnesses.

Instead, LionClaw constrains the runtime launch:

- selected work root mounted at `/workspace`
- session-scoped LionClaw runtime control state mounted at `/runtime`
- persistent runtime-native private home mounted at `/runtime/home`
- project-scoped draft/output area mounted at `/drafts`
- applied non-channel skills mounted read-only at `/lionclaw/skills/<alias>`
- network mode chosen by preset
- runtime secrets mounted only when preset allows it
- runtime auth staged into the runtime-private home
- timeout and cancellation enforced by the kernel
- OCI image identity included in compatibility decisions

### Skill-Owned Edges

Skills are installable packages of instructions, channel workers, and
integration logic. Channels are skills. Provider transports stay outside the
trusted Rust core and integrate through kernel APIs.

Skill text can influence prompt context. It cannot grant permissions.

## Kernel Modules

- `kernel.sessions`: session lifecycle, history policy, and aggregate turn metadata.
- `kernel.session_turns`: durable per-turn history, recovery state, and partial assistant output.
- `kernel.skills`: skill alias validation and installed-skill metadata helpers.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.jobs`: scheduled job definitions, run records, and SQLite persistence.
- `kernel.capability_broker`: explicit brokered capability execution for direct runtimes and narrow kernel surfaces.
- `lionclaw-runtime-api`: runtime adapter contract, registry, and shared runtime-facing event/program types.
- `kernel.runtime`: kernel-owned runtime registration, launch prerequisite checks, auth staging glue, and execution integration.
- `kernel.runtime.execution`: execution presets, plan compilation, OCI backend, and process execution.
- `kernel.prompt_context`: session-aware prompt context policy, section capping, and audit metadata for runtime-visible context.
- `kernel.private_context_projection`: classed private context projection boundary from skill-owned records into prompt-context policy.
- `kernel.scheduler`: due-job claiming, lease coordination, retry, and dispatch.
- `kernel.channel_state`: channel pairing requests, scoped grants, normalized inbound event admission, queued turns, progress stream state, and transcript history.
- `kernel.channel_outbox`: durable provider-neutral delivery leases, retry state, provider receipts, and scheduler delivery projections.
- `kernel.continuity`: assistant-home continuity files, `ACTIVE.md` projection, daily notes, artifacts, open loops, proposals, and retrieval helpers.
- `kernel.continuity_fs`: descriptor-rooted Unix filesystem helper for assistant-home continuity.
- `kernel.session_compactions`: persisted transcript compaction summaries and ranges.
- `kernel.audit`: append-only audit event log persisted in SQLite.

Shared crate primitives:

- `lionclaw-durable-fs`: atomic file publish/remove/rename for
  LionClaw-owned runtime, continuity, and operator-private state; successful
  file replacement syncs the file and containing directory.

## Runtime Adapter Contract

The shared Rust contract lives in `crates/lionclaw-runtime-api`. Concrete
conversation drivers live in crates by protocol, not by product: currently
`crates/lionclaw-runtime-acp`, `crates/lionclaw-runtime-codex`, and
`crates/lionclaw-runtime-mock`.

Runtime adapters implement:

- `info()`
- `session_start()`
- `turn()`
- `program_backed_turn()`
- `build_terminal_program()`
- `runtime_control()`
- `resolve_capability_requests()`
- `cancel()`
- `close()`

Adapters also declare a turn mode:

- `ProgramBacked`: LionClaw launches a real agent CLI inside the compiled
  execution plan. Codex and ACP profiles are the first supported examples.
- `Direct`: the runtime may return explicit `RuntimeCapabilityRequest` items
  for the kernel to broker.

The distinction is central. Program-backed runtimes are the everyday product
path. Direct runtimes and brokered capabilities are useful for tests, narrow
workers, and future runtimes that do not bring a full harness.

Adapters describe what they need and how to interpret runtime-owned state:
program invocations, optional runtime auth kind, native terminal programs,
saved runtime session state, and runtime controls. They do not receive the kernel
execution plan. The kernel gives program-backed adapters a constrained executor
plus observable runtime context, then still decides launch allowance, mounts,
secrets, auth materialization, audit, and persistence.

Side-effecting live protocol translation lives inside each protocol adapter.
Codex's app-server driver and the ACP driver own their protocol handshakes,
permission replies, runtime session continuity updates, and JSON-RPC loops.
Their shared output is a turn journal of `TurnEvent` records: one canonical
`RuntimeEvent` paired with an optional raw protocol payload retained only for
debugging. Kernel live routing projects the journal through `canonical_events`,
so raw payloads are never streamed to operators, replayed into prompts, or used
to build canonical assistant text.
The persisted journal is a kernel-owned debug/audit record of the driver
translation, readable through the scoped session-turn journal API. It is not the
production transcript reader for prompt history; canonical session turn state
remains the source for user-visible history and prompt reconstruction.

Runtime context may include host projections for runtime-visible paths. A
directory projection maps a runtime tree such as `/runtime` to the runtime state
root. An exact projection maps one runtime path, such as the channel-send Unix
socket, to one host path and intentionally blocks descendants. Projection fields
are private; constructors validate absolute runtime and host paths before the
kernel exposes them to adapters. Shared helpers in `lionclaw-runtime-api`
normalize relative runtime paths and reject parent traversal before adapters
turn runtime protocol fields into host paths.

Program-backed resume readiness is a typed adapter input, not a raw boolean.
The only positive `RuntimeSessionReady` value is derived from the hardened
LionClaw ready-marker check in `lionclaw-runtime-api`; adapters use it only to
gate loading runtime-private continuation state for kernel-managed turns.
Native terminal launches have no runtime-session readiness input.

## Program-Backed Runtime Flow

For `lionclaw run <runtime>`, channel turns, or scheduled jobs:

1. The caller submits a turn through the operator CLI, channel API, or scheduler.
2. The kernel opens or reuses a durable session.
3. The execution planner resolves the runtime profile, preset, work root,
   runtime state root, drafts root, network mode, secret mount decision, image,
   timeouts, and compatibility key.
4. The kernel audits `runtime.plan.allow` or `runtime.plan.deny`.
5. The kernel renders the prompt envelope through typed prompt context policy:
   session trust tier, history policy, runtime id, execution plan, and prompt
   target select ordered context items, byte caps, transcript tail, and current
   input.
6. The OCI backend launches the runtime in the confined layout.
7. The adapter maps the runtime protocol into a canonical turn journal. Codex
   uses its native `app-server` JSON-RPC protocol over stdio inside the
   confined process; OpenCode uses Agent Client Protocol over stdio.
8. The kernel persists the canonical journal, streams `canonical_events` to live
   consumers, checkpoints canonical answer text, records terminal turn status,
   audit, and any continuity changes it owns. Raw protocol payloads are stored
   only when explicit debug retention is enabled.

Program-backed runtimes stream two message lanes:

- `answer`: canonical assistant reply text persisted into turn history
- `reasoning`: optional live thought/progress text that channels may render or ignore

Only `answer` is treated as the durable assistant reply. Adapters may also emit
`message_boundary` for either lane when a runtime starts a new semantic message
item without sending text. The kernel uses `answer` boundaries when building
canonical assistant text so adjacent streamed items remain separate paragraphs;
UIs can use the same marker to render live transcript blocks without
runtime-specific parsing.
`message_boundary` is a stable stream event kind. Consumers that reconstruct
plain text from `message_delta` events should ignore it; consumers that render
conversation structure may use it as a paragraph/message-item break. Literal
`message_delta` text is otherwise preserved as emitted by the runtime adapter.
Runtime adapters can emit `file_change` events with structured
`file_change` payloads (`runtime`, optional `operation_id`, `status`, `paths`,
`total_count`) when the runtime reports edits. The text field remains a compact
display summary, but operator UIs should read the structured payload instead of
parsing status text. When present, `operation_id` identifies updates for the
same runtime edit operation so UIs can replace in-progress file-change rows
with their completed status instead of double-counting them.
Channel streams also emit a kernel-owned `turn_completed` event after the turn
record is finalized. Its `text` field is the canonical persisted assistant
reply and lets channel UIs reconcile live deltas against durable turn state
before the terminal `done` marker. Failed, timed-out, cancelled, and interrupted
turns publish typed status/error events followed by exactly one `done`.

Codex is launched through its app-server protocol with `externalSandbox`
permissions inside the outer Podman boundary. LionClaw does not use
`codex exec` as a fallback path. ACP runtimes such as OpenCode are configured
as profiles (`driver = "acp"`, command, args, model/mode, auth, confinement,
and optional `skill_projection`) served by `lionclaw-runtime-acp`; adding
another ACP harness is a profile change, not a new Rust crate. Protocol
request/notification assumptions are pinned by checked-in fixtures under the
protocol crate, including the target CLI version and immutable source commit;
update those fixtures with the driver when the target protocol contract
changes.
The runtime profile format is intentionally pre-v1 breaking: profiles use
`driver` and `command`; the removed `kind`/`executable` shape fails to load with
an explicit operator-facing error.

## Native Runtime TUI Flow

`lionclaw run --runtime-tui` is an explicit attached-runtime path for operators
who want the selected harness's own terminal UI. It is not the default
line-oriented turn path and it is not used by channels or scheduled jobs.

Flow:

1. The operator CLI resolves the normal LionClaw project, runtime, and durable
   interactive session.
2. The kernel materializes the confined runtime layout, runtime-visible skills,
   and a fresh attached-session context through the same typed prompt context
   policy into session-scoped runtime control state as both
   `AGENTS.generated.md` and the runtime-standard `AGENTS.md`.
3. The runtime adapter supplies a terminal program through
   `build_terminal_program()`.
4. The execution planner compiles the same mounted workspace, staged auth,
   network mode, and secret policy used by the selected runtime preset.
5. The OCI backend attaches the operator's terminal to the runtime process with
   a TTY.
6. On launch and exit, the kernel writes `runtime.tui.launch` and
   `runtime.tui.exit` audit events.

For Codex, the attached terminal program runs the real Codex CLI in
danger-full-access mode with approval disabled. That is intentional: LionClaw's
outer container, mounts, runtime state root, network preset, auth staging, and
audit trail are the active boundary. Codex also receives
`/runtime/AGENTS.generated.md` as its model instructions file, so LionClaw
policy-selected context is included without shadowing `/workspace/AGENTS.md`.
LionClaw also passes launch-time Codex config overrides for
`[projects."/workspace"] trust_level = "trusted"` and
`check_for_update_on_startup = false`, matching the result of approving Codex's
own workspace prompt inside the container while keeping runtime updates under
LionClaw's runtime image/update path. Those overrides do not rewrite
`/runtime/home/.codex/config.toml`, and the host Codex home is not mutated.

LionClaw does not scrape terminal output or import native TUI transcripts.
Attached mode is a bounded launcher: it never inserts canonical
`session_turns`, never updates prompt or session history, and never changes the
continuation, retry, or resumability state used by kernel-managed turns.
ACP drivers launch the harness's native interactive command from the profile
command without ACP protocol args, with the same confinement, staged auth,
skills, and non-privileged runtime layout as other runtime executions. Attached
launches do not consume LionClaw's program-backed runtime-session ready proof:
`RuntimeTerminalProgramInput` has no readiness field, and existing ready
markers remain intact for later kernel-managed turns. Program-backed Codex and
ACP continuity remain LionClaw-owned links in session-scoped runtime control
state, but native UI turns stay runtime-owned and cannot mutate the
runtime-native session that later driven turns resume. The attached native UI
itself is not a LionClaw turn, so LionClaw turn timeout overrides do not wrap
the runtime's own interactive session. Future terminal export APIs, if added,
are debug/display/audit surfaces only; canonical conversation state remains
owned by kernel-managed turns.
Each native TUI launch also holds a LionClaw-owned file lock in the session's
runtime state root, preventing separate operator processes from attaching two
native UIs to the same LionClaw session state at once.

Native TUI mode does not provide typed live answer/reasoning events to
channels. The normal operator console, `run --plain`, channel turns, and
scheduled jobs remain the paths that stream typed runtime events directly into
LionClaw while a turn is active. Runtime skill facets that depend on an active
LionClaw turn bridge, such as channel-bound `channel.send` facets, are not
projected into native TUI sessions, even when the selected execution preset
would enable those facets for normal kernel-managed turns.

Native TUI mode has no LionClaw command layer inside the attached runtime UI.
Once the TTY is attached, first-column commands belong to the selected
runtime's own interface; operators exit through the runtime's normal exit
gesture, such as Codex's Ctrl-D. This avoids terminal-editor proxying and keeps
runtime command semantics out of the kernel.
Terminal-generated interrupts and quits remain runtime-owned; LionClaw keeps
the parent process alive so it can audit native UI exit.

## Runtime Control Commands

The first column is command space on LionClaw-owned interactive surfaces.
`lionclaw run`, `run --plain`, and channel inbound routing reserve
`/lionclaw ...` for LionClaw-owned controls such as `/lionclaw retry`,
`/lionclaw reset`, and `/lionclaw exit`. Local-only controls such as
`/lionclaw exit` are acknowledged by channel routing but do not exit a channel
worker.

Other first-column slash commands are classified as runtime controls and are
persisted as `runtime_control` turns. The kernel records
`runtime.control.route`, `runtime.control.start`, `runtime.control.finish`, and
`runtime.control.outcome` audit events around those turns. Runtime adapters
decide whether a control is handled, unsupported, interactive-only, or failed.

This keeps native runtime commands such as `/compact` and `/rename` native to
the selected runtime on LionClaw-owned turn paths without teaching the kernel
runtime-specific command semantics. In native TUI mode those commands are
handled directly by the runtime UI. Leading-space slash input and path-like
slash input remain ordinary prompts.

## Direct Runtime And Brokered Capability Flow

Direct runtimes may submit `RuntimeCapabilityRequest` items. Kernel flow:

1. Validate the request scope against the selected turn, job, channel, or runtime context.
2. Evaluate policy for the requested capability.
3. Execute through the kernel broker only if allowed.
4. Return a typed `RuntimeCapabilityResult`.
5. Audit request and result.

This broker is not the normal filesystem and shell path for program-backed
runtimes. It is reserved for explicit LionClaw-owned actions, direct runtimes,
narrow non-runtime surfaces, and tests.

Brokered `channel.send` is route-bound to the current channel session. Runtime
payloads provide content only; the kernel derives channel, conversation, topic,
and reply routing from the approved session and active turn before enqueueing an
outbox delivery.

## Program-Backed `channel.send`

Program-backed runtimes use a turn-scoped MCP server for outbound channel
delivery. When the effective execution preset includes `channel-send`, the
kernel mounts a LionClaw-owned socket at `/runtime/lionclaw/channel-send.sock`,
writes a small stdio proxy under `/runtime`, and passes a `lionclaw` MCP server
spec to program-backed drivers that support MCP. The proxy is transport only:
it forwards JSON-RPC lines between stdio and the Unix socket. Tool listing,
`channel_send` calls, authorization, audit, and enqueueing all terminate in the
kernel broker outside the sandbox.

Without that escape class, the MCP server spec, proxy, and usable socket are
absent. The bridge is valid only while the runtime turn is active; turn
completion or timeout removes the socket and invalidates open connections.
Native runtime controls and native runtime TUI sessions do not receive this
bridge or MCP tool.

The host socket is created under the operator's short per-user runtime directory
rather than under the instance home, so long project paths do not exceed Unix
socket path limits. OCI launches that mount a Unix socket disable Podman's
SELinux process label for that turn; otherwise SELinux hosts can expose the
socket inode but deny `connect(2)`.

The socket is private transport for the MCP stdio proxy and accepts one MCP
JSON-RPC request per connection: write one newline-delimited JSON object, read
one newline-delimited JSON object, then close. The kernel supports `initialize`,
`tools/list`, and `tools/call channel_send`. `channel_send` arguments name a
configured channel route, provider-neutral content, and either an explicit
idempotency key or the JSON-RPC request id as a derived retry key.
Attachment content is not sent over the socket; the request names files under
`/runtime` control state or a generated-artifact directory declared by the
runtime adapter under the persistent native home. The persistent native home
itself is not an attachment root because it can hold runtime auth, config,
databases, caches, and history. The kernel reuses the existing runtime-artifact
copy and outbox attachment path. Attachment paths are interpreted relative to
the current runtime path projections; parent-directory and symlink escapes are
rejected. Attachment-only sends are valid; text-only sends must carry non-empty
text.

The bridge and MCP proxy are transport only. The kernel validates the current
session and turn from its own execution context, checks the active channel
binding, normalizes route fields, enforces `plain`/`markdown`/`html` format
hints, copies any attachments into LionClaw-owned outbox storage, and creates a
normal durable channel outbox delivery. Direct/runtime capability
`channel.send` remains policy-gated and derives its route from the active
channel session; MCP `channel_send` remains preset/bridge-gated and validates
the requested route against the turn's projected route inventory. Both paths
terminate in the kernel and use the same authorized enqueue helper instead of
maintaining separate outbox paths. Channel workers continue to lease and report
those deliveries through `/v0/channels/outbox/pull` and
`/v0/channels/outbox/report`.

Bridge setup, accept-loop, connection-task, and connection I/O failures are
audited under `runtime.channel_send.bridge_error`. Request denials, including
connection pressure over the bridge's concurrent connection cap, are audited as
`runtime.channel_send.denied`.

When project-instance runtime context is active, `channel.send` requests are
also checked against the sender-relative `channel_send` projection for that
selected instance. A project runtime can enqueue only routes that are present as
configured neighbor routes in its generated inventory.

Idempotency lives on the outbox row. Runtime channel sends use
`source_kind = "runtime_channel_send"`, a source id scoped to
`session_id`, `turn_id`, and the runtime idempotency key, plus a canonical
request fingerprint. Retrying the same key with the same payload returns the
same delivery id; reusing the key with a different payload returns a structured
conflict error.

## Execution Plan And Confined Layout

The everyday runtime layout is mount-first:

- `/workspace`: selected work root with preset-controlled read-only or read-write access
- `/runtime`: session-scoped LionClaw runtime control state
- `/runtime/home`: persistent runtime-native private home
- `/drafts`: project-scoped draft/output area
- `/lionclaw/project/instances.json`: generated read-only project instance
  inventory for project-backed program-backed runtime launches
- `/lionclaw/skills/<alias>`: installed non-channel skill assets mounted read-only
- `/attachments`: read-only channel attachment files for the current inbound
  event, present only after attachment finalization staged files for that turn
- `/mnt/<target>` or another explicit absolute target: operator-configured
  extra directory mounts stored on the selected runtime profile

For local `lionclaw run`, target resolution selects one project instance and
uses that instance's recorded work root. In project mode, the operator console
also renders the other configured project instances and can switch to another
already-configured instance when no turn is active. Switching reads that
instance's existing home, work root, runtime config, sessions, and audit scope;
it does not mutate project, instance, runtime, channel, skill, or default
configuration. The selected work root is mounted at `/workspace`. The instance
home remains LionClaw's state root and is not the project tree or work root.

Project-backed runtimes that launch confined programs get their selected
instance name through
`LIONCLAW_PROJECT_INSTANCE` and discover neighbors through
`LIONCLAW_PROJECT_INSTANCES_FILE=/lionclaw/project/instances.json`. The JSON
contract is intentionally small: `schema_version`, `default_instance`, and a
sorted `instances` array with `{ "name": ... }` entries. LionClaw generates this
from `.lionclaw/project.toml` and valid `.lionclaw/instances/<name>` homes, then
mounts only the generated projection read-only. The raw `.lionclaw` directory
remains masked from `/workspace` and blocked as a configured extra mount.
Already-running runtimes do not receive live updates when operators add or repair
instances; normal process or unit restart boundaries pick up the new inventory.

When the effective execution preset also includes `channel-send`, the same
projection path uses `schema_version = 2` and may include contact-aware
`channel_send` state for neighbor instances. The selected instance entry remains
identity-only (`{ "name": "main" }`) because the runtime already knows itself
through `LIONCLAW_PROJECT_INSTANCE`. Neighbor entries contain one of:
`unconfigured`, `configured`, `channel_missing`, or `misconfigured`. Only
`configured` exposes route fields:

```json
{
  "name": "reviewer",
  "channel_send": {
    "status": "configured",
    "channel_id": "example-channel",
    "conversation_ref": "provider:conversation:123",
    "thread_ref": null
  }
}
```

`channel_missing` means the neighbor has a preferred contact on a channel that
is not present in the selected sender's active applied channel bindings, so the
runtime is not handed an immediately failing route. `misconfigured` means the
neighbor contact config is invalid, unreadable, or ambiguous. The contact-aware
projection is captured when the kernel/runtime context is built; it is not a
fresh config scan on every runtime turn.

Preferred contacts are selected in the recipient instance's channel config. A
new contact marker clears any older preferred marker in the same instance:

```toml
[[channels]]
id = "example-channel"
skill = "example-skill"
launch_mode = "background"
worker = "scripts/worker"

[channels.contact]
conversation_ref = "provider:conversation:123"
# thread_ref is omitted when absent; TOML has no null value.
```

Channel metadata can provide a default contact template for `--contact`:

```toml
[contact]
conversation_ref_template = "provider:member:{instance}"
```

Only the `{instance}` variable is supported, and default templates must include
it. Operators may also pass an explicit `--conversation-ref` for static provider
routes; `--thread-ref` stays optional. `--contact` requires a project instance
target because direct homes do not have a stable project instance identity for
template rendering or neighbor projection.

## Channel Skill Runtime Projection

Channel-bound skill roots remain host-only by default. A channel skill can
publish a runtime-facing Agent Skill only by including a complete embedded skill
at `runtime/<alias>/SKILL.md`, where the embedded skill name matches `<alias>`.
Only that embedded skill root is mounted read-only under
`/lionclaw/skills/<alias>`; the channel package, worker script, metadata, and
other host-side assets are not projected into the runtime. Concrete worker
binaries, contact templates, routing schemes, and provider behavior belong in
the owning channel skill directory.

First-party channel snapshots can carry host-side worker assets and embedded
runtime-facing facets, but those details remain skill-owned. The kernel contract
is only that channel workers authenticate to LionClaw and use the channel
authorize, inbound, attachment, outbox, health, and grant approve/revoke/consume
APIs without importing the `lionclaw` crate.

Configured extra mounts are instance/runtime-profile scoped. Operators manage
them with `lionclaw runtime mount add|list|remove <runtime-id> ...`. The
positional mount value is target identity: a path-safe token resolves to
`/mnt/<token>`, while an absolute value is used as the exact container target.
Persisted state remains the execution planner's `MountSpec { source, target,
access }`; there is no separate mount name. Mount sources must be existing
canonical directories outside LionClaw project/work-root metadata and
instance-private state. Targets must be absolute, unique within the runtime
profile, and outside reserved runtime paths: `/workspace`, `/runtime`,
`/drafts`, `/attachments`, `/lionclaw`, `/run/secrets`, `/proc`, `/sys`, and
`/dev`. Configured mounts must also be representable as Podman bind-mount
arguments: when `:` in the source or target requires the `--mount` form, neither
path may contain `,`. The operator CLI, status/doctor checks, runtime launch
validation, and planner all validate the configured mounts so hand-edited config
cannot bypass planner safety checks.

The operator console treats the transcript as durable conversation: user prompts
and assistant answer deltas are rendered as message blocks in the main scroll
surface. Runtime status, reasoning, command, progress, and file-change events
are summarized as live activity for the active turn and exposed through control
panes instead of being appended as transcript lines.

The planner injects runtime-private environment defaults such as
`HOME=/runtime/home`, XDG config/cache/data/state roots under `/runtime/home`,
`LIONCLAW_RUNTIME_DIR=/runtime`, `LIONCLAW_DRAFTS_DIR=/drafts`, and
`LIONCLAW_SKILLS_DIR=/lionclaw/skills` when runtime-visible skills have
mounted assets. `/runtime` is session-scoped LionClaw control state.
`/runtime/home` is a separate persistent runtime-native home keyed by runtime,
work root, OCI compatibility identity, and execution security shape. That lets
Codex, OpenCode, and future runtimes keep native config, databases, histories,
and caches across LionClaw sessions without sharing them across different
projects or materially different secret/network/workspace/escape capability
shapes.

Interactive program-backed turns launch a fresh confined process for each
request. They receive the current LionClaw session's `/runtime` control state
and the matching persistent `/runtime/home` native home.

LionClaw keeps the canonical transcript itself. Fresh harness sessions get
replayed transcript history in the prompt envelope; resumed harness sessions
get a continuation note plus the new user input instead of the full prior
transcript on every turn.

LionClaw does not persist a separate draft registry. Draft listing scans the
shared drafts directory on demand, and explicit keep/discard actions move or
delete files from there.

## Network, Secrets, And Runtime Auth

Current runtime network policy is intentionally coarse:

- `network-mode = "on"`
- `network-mode = "none"`

`on` maps to the container engine's private network mode, not host networking.
LionClaw does not expose a fake allowlist mode before a real egress-control
plane exists. On rootless hosts, `on` also requires the container engine to be
able to stand up its private network namespace. LionClaw preflights that host
capability before interactive or managed-background startup.

Runtime secrets are loaded from the selected instance home's
`config/runtime-secrets.env`.
Presets either mount that whole file or mount no runtime secrets at all with
`mount-runtime-secrets = true|false`. The Podman backend mounts it read-only
under `/run/secrets/` with a LionClaw-managed name that starts with
`lionclaw-runtime-secrets-`. LionClaw hardens the config directory to `0700`
and the runtime secret file to `0600` on Unix before loading it.

Host-only runtime auth comes from the host runtime itself. Before a confined
Codex turn, LionClaw reads the host Codex auth store, normally
`~/.codex/auth.json`, refreshes that host auth when needed, then stages
persistent runtime-local `auth.json` under `/runtime/home/.codex` before
launch. LionClaw does not write or replace `/runtime/home/.codex/config.toml`;
required Codex launch constraints such as trusted `/workspace` and disabled
update checks are passed as CLI config overrides. Host Codex config, plugins,
apps, MCP servers, and paths are not imported into the confined runtime. The
real host Codex home is never mounted into the runtime container.

`lionclaw run` inherits an interactive shell's `CODEX_HOME` when set, and
`lionclaw up` persists that same override into the managed daemon environment
for background jobs and channels.

## API Contracts

Raw HTTP is for workers, tests, and debugging. Product-facing docs should lead
with the CLI.

### Session

- `POST /v0/sessions/open`
- `GET /v0/sessions/latest`
- `POST /v0/sessions/history`
- `POST /v0/sessions/turn/journal`
- `POST /v0/sessions/action`
- `POST /v0/sessions/turn`

### Channel

- `GET /v0/channels/list`
- `GET /v0/channels/pairing` (pairing requests and current grant state)
- `POST /v0/channels/pairing/invite`
- `POST /v0/channels/pairing/claim`
- `POST /v0/channels/pairing/approve`
- `POST /v0/channels/pairing/block`
- `POST /v0/channels/grants/approve`
- `POST /v0/channels/grants/revoke`
- `POST /v0/channels/grants/consume`
- `POST /v0/channels/authorize`
- `POST /v0/channels/inbound`
- `POST /v0/channels/attachments/stage` (multipart worker upload)
- `POST /v0/channels/attachments/finalize`
- `POST /v0/channels/stream/pull`
- `POST /v0/channels/stream/ack`
- `POST /v0/channels/outbox/pull`
- `POST /v0/channels/outbox/report`
- `POST /v0/channels/health/report`

### Job

- `POST /v0/jobs/create`
- `GET /v0/jobs/list`
- `POST /v0/jobs/get`
- `POST /v0/jobs/pause`
- `POST /v0/jobs/resume`
- `POST /v0/jobs/run`
- `POST /v0/jobs/remove`
- `POST /v0/jobs/runs`
- `POST /v0/jobs/tick`

### Continuity

- `GET /v0/continuity/status`
- `POST /v0/continuity/get`
- `POST /v0/continuity/search`
- `POST /v0/continuity/drafts/list`
- `POST /v0/continuity/drafts/promote`
- `POST /v0/continuity/drafts/discard`
- `GET /v0/continuity/proposals`
- `POST /v0/continuity/proposals/merge`
- `POST /v0/continuity/proposals/reject`
- `GET /v0/continuity/loops`
- `POST /v0/continuity/loops/resolve`

### Policy And Audit

- `POST /v0/policy/grant`
- `POST /v0/policy/revoke`
- `GET /v0/audit/query`

Prompt context selection emits `prompt.context.built` audit events. The payload
records session policy, runtime id, context mode, included/excluded/capped item
names, classes, sources, reasons, byte counts, and the prompt context policy
version. Policy version 2 removes the legacy direct private workspace prompt
file slots from selection and audit decisions. Policy version 3 renames the
projector audit field and source from memory projection to private context
projection. Policy version 4 makes private context projection class-scoped,
adding assistant profile, user profile, and memory classes with per-class
budget audit plus capped current-input metadata for projector requests. It
does not store prompt body content or content hashes. Current user input is
also budgeted; LionClaw rejects over-budget input instead of silently
truncating operator intent.

### Daemon Metadata

- `GET /health`
- `GET /v0/daemon/info`

`/health` is liveness only. `/v0/daemon/info` is the typed operator-facing
metadata endpoint used to classify a listener before reusing it.

## Channel-Skill Contract

External channel skills integrate over HTTP:

Channel routing keeps four concepts separate:

- channel instance/provider account: `channel_id`
- actor admission identity: normalized `sender_ref` plus direct grants and blocks
- route approval: conversation/thread grants over normalized route refs
- session binding: the deterministic session target for an already-admitted turn

Workers may request only a constrained `session_binding`: `grant` (default),
`actor`, `conversation`, `thread`, `conversation_actor`, or `thread_actor`. The
kernel validates the required normalized refs, confirms the requested binding
does not broaden the approved grant scope, then derives the session key itself.
Workers never supply raw session keys, templates, or metadata-derived session
identity.

Actor-qualified conversation/thread bindings may be covered by an approved
direct actor grant because they narrow that actor into a more specific history
key. Actorless conversation/thread bindings require an approved route grant and
cannot be opened from a direct actor grant alone.

`POST /v0/channels/authorize` returns the admission decision plus the derived
`session_key` when authorized. Authorized responses also include the matched
`grant_id`, `grant_routing_profile`, and optional `grant_label` so workers can
apply grant-scoped transport behavior without inferring trust state from local
configuration. Labels are operator-controlled metadata on the matched grant;
workers must treat them as exact data, not executable instructions.

1. `GET /v0/sessions/latest` restores the latest durable session snapshot for
   a deterministic `(channel_id, session_key)`.
2. `POST /v0/channels/inbound` submits normalized inbound facts. Approved
   grants queue a channel turn and receive an explicit outcome. Inbound v2
   carries attachment descriptors first; workers fetch binary files only after
   admission. Workers may include optional `session_binding`; omission preserves
   `grant` binding.
3. If the inbound outcome is `waiting_for_attachments`, the worker uploads each
   admitted file with `POST /v0/channels/attachments/stage`, then calls
   `POST /v0/channels/attachments/finalize`. Finalization rejects missing or
   unstaged descriptors and makes the turn claimable. At execution time, the
   kernel derives a runtime-only prompt manifest from the stored attachment
   rows. Descriptors rejected at admission by known size policy are recorded in
   the manifest immediately; if no stageable attachments remain, the turn queues
   without waiting for a worker finalize call.
4. `POST /v0/sessions/action` accepts tagged actions:
   `continue_last_partial`, `retry_last_turn`, `reset_session`, and
   `cancel_active_turn`. Channel cancellation is scoped by `session_id`,
   `channel_id`, and `session_key`, and may include `expected_turn_id` as a stale
   guard.
5. `POST /v0/channels/stream/pull` fetches typed progress events for a
   consumer cursor. Stream acknowledgment means the worker handled progress
   events; it does not imply provider message delivery.
6. `POST /v0/channels/outbox/pull` atomically leases due outbound deliveries.
   Deliveries carry `conversation_ref`, optional `thread_ref`, optional
   `reply_to_ref`, `content`, and a stable `delivery_id`. Each pull creates an
   `attempt_id`; workers perform one provider send attempt per lease. Pulls may
   optionally scope to a `conversation_ref` / `thread_ref` so per-peer workers
   do not lease another peer's delivery.
7. `POST /v0/channels/outbox/report` records provider outcomes. `delivered`
   stores provider receipt data, `retryable_failed` returns the delivery to
   kernel-owned exponential backoff, and `terminal_failed` closes it. Stale
   reports are recorded as rejected and never overwrite the current attempt.
8. `POST /v0/channels/health/report` appends a worker health sample for a
   configured channel. The worker sends `channel_id`, `reporter_id`, overall
   `status` (`ok`, `warning`, or `error`), `observed_at`, and provider-specific
   checks with stable `code`, `status`, `message`, and JSON `details`. Provider
   reachability checks stay in channel workers; the kernel validates and stores
   normalized samples, audits `channel.health.reported`, and never treats health
   reports as authority to start, stop, or repair workers. Reports with
   oversized identities or timestamps more than two minutes in the future are
   rejected; stored far-future reports are excluded from latest-health selection.
9. `POST /v0/channels/stream/ack` advances only the progress stream cursor.
10. Direct grant approval, pairing invite/claim, pairing approve/block, grant
   revoke, and grant consume endpoints manage channel trust. Direct approval
   creates a durable grant for already-known normalized channel refs without
   creating a pairing row, and closes exact matching pending operator approvals in the
   same transaction. Invite tokens are returned once, stored only as hashes, and
   claimed through worker-submitted provider facts.
   Blocking a sender scope also closes matching pending operator-approval
   pairing requests. Blocking a token invite by `pairing_id` marks that invite
   blocked without creating a sender grant. Blocks are enforced from the
   most-specific scope back to the direct sender.
   Grant consume is worker-facing cleanup for approved labeled grants that have
   already produced their intended terminal effect. It requires the exact
   `grant_id` and expected label, deletes only that approved grant, audits
   `channel.grant.consumed`, and does not leave a revoked scope. Operator
   denial remains `revoke` or `block`.

Attachment files are stored under LionClaw runtime state at
`runtime/channels/sha256-<channel-id-digest>/attachments/sha256-<event-id-digest>/sha256-<attachment-id-digest>/`.
The hash components are derived from the normalized IDs; raw provider IDs remain
in the database and runtime manifest. Staging uses a temp file and commits only
into kernel-derived path components without following symlinks. Admission and
staging enforce 10 attachments per event, 25 MiB per attachment, and 50 MiB per
event. Multipart staging accepts enough body to report typed policy rejections
for oversized uploads inside the event-size envelope; larger bodies are
transport rejected. Runtime turns see only staged files through a
manifest-derived, read-only `/attachments` projection mount. Projection copies
are removed after the runtime turn; maintenance removes stale projection
directories left behind by crashes. The runtime-only prompt manifest shape is:

```json
{
  "channel_attachments": [
    {
      "id": "att-1",
      "kind": "image",
      "filename": "image.png",
      "mime_type": "image/png",
      "size_bytes": 123,
      "sha256": "...",
      "path": "/attachments/att-1-4f4a9410ffcdf895/image.png",
      "caption": "optional caption"
    }
  ],
  "rejected_channel_attachments": [
    {
      "id": "att-2",
      "kind": "image",
      "reason_code": "not_staged"
    }
  ]
}
```

Queued channel turns emit machine-stable status/error codes through the same
stream contract. Kernel-generated lifecycle codes include:

- `queue.queued`
- `queue.started`
- `queue.completed`
- `queue.failed`
- `queue.timed_out`
- `queue.cancelled`
- `queue.interrupted`
- `runtime.started`
- `runtime.completed`
- `runtime.error`
- `runtime.timeout`
- `runtime.cancelled`
- `runtime.interrupted`

Stream events produced by actual runtime turns include `session_id` and
`turn_id`. The stream `peer_id` remains a provider-facing conversation hint for
typing/progress only; durable outbound routing uses the outbox fields
`conversation_ref`, `thread_ref`, and `reply_to_ref`. Internal session identity
is carried by `session_key` and the session row. With the default
`session_binding = grant`, channel session keys preserve the approved grant's
scope: direct sessions include the sender, conversation sessions include the
conversation and optional sender, and thread sessions include the conversation,
thread, and sender. Non-default bindings derive from normalized request refs
after authorization succeeds: actor, conversation, thread,
conversation+actor, or thread+actor.

Outbox `content` is provider-neutral JSON with `text`, a `format_hint`
(`plain`, `markdown`, or `html`; default `plain`), and an `attachments` array. Outbound
attachments are durable file descriptors copied into LionClaw-owned runtime
outbox storage before delivery is leased. Runtime adapters can emit typed
artifacts; the kernel converts those artifacts into outbox attachments without
requiring channel workers to know runtime-private storage layouts. Providers
choose native delivery methods from MIME type and report delivery outcomes
through the same lease/report flow as text messages.

Channel health reports are append-only rows in `channel_health_reports`.
Doctor reads the latest report per channel, along with kernel-visible pending
pairings and outbox status, to explain channel state without calling provider
APIs or mutating local state. Pending outbox health includes rows still marked
`leased` after their lease expiry, so a crashed worker cannot hide undelivered
messages until another worker pulls. Background channel reports older than ten
minutes are stale doctor warnings, and background channels with no reports warn.
Interactive channels can report opportunistically without missing-report or stale
warnings. Doctor also warns on impossible future report timestamps found in
stored state without treating those reports as current worker health.

## Session Continuity

`sessions.history_policy` controls how incomplete turns are reused in future
prompts:

- `interactive`: carry forward partial assistant output with an explicit marker
- `conservative`: carry forward only a structured failure note

`session_turns` is the durable source of truth for prompt history. It records:

- `kind = normal | retry | continue | runtime_control`
- `status = running | waiting_for_attachments | completed | failed | timed_out | cancelled | interrupted`
- `display_user_text`
- `prompt_user_text`
- `assistant_text`
- `error_code`
- `error_text`
- `runtime_id`

Runtime-visible prompt history is selected by `kernel.prompt_context`, not by a
global replay constant. The transcript tail is bounded by session trust tier
and history policy: Main interactive sessions get the broadest tail, Main
conservative sessions get less, and Untrusted sessions get smaller tails.
The current user input has a finite prompt-context budget and over-budget input
fails before runtime execution rather than being clipped.
When a program-backed runtime resumes an existing runtime conversation, the
primary prompt includes a runtime-session note and audits the transcript tail
as excluded; the separate fresh prompt is built and audited as its own context
surface.

Answer-lane text is checkpointed while a turn is still running so restart
reconciliation can preserve partial replies already emitted to the user.
Channel-backed running turns also persist the exact stream sequence through
which the durable assistant checkpoint is synchronized. Channels v2 stores
normalized provider facts, including text and attachment descriptors, in
`channel_inbound_events`, admits work through scoped grants, and derives
deterministic session keys from either the approved grant or the constrained
`session_binding`. Worker-supplied runtime selection is not part of the inbound
channel contract; the kernel resolves runtime execution from the
instance/default runtime configuration.
Session-key components escape `:` and `%` so provider refs such as
`provider:conversation:123` remain unambiguous.
Channel turn state is terminalized independently from the session turn state so
queue workers can distinguish `completed`, `failed`, `timed_out`, `cancelled`,
and `interrupted` without parsing runtime text. Cancelling a waiting or pending
channel turn finalizes it in the queue immediately. Cancelling a running channel
turn signals the in-memory runtime cancellation token, calls the adapter's
`cancel()` hook, persists `runtime.cancelled`, then advances the queue through
the same terminalization path.
Proactive pairing invites reuse `channel_pairing_requests` with
`claim_policy = token_claim`; raw invite tokens are never stored, claim counts
advance inside the same transaction that creates the scoped grant, and expired,
blocked, or over-claimed tokens cannot authorize a channel sender. Invite
creation may carry a channel-neutral operator actor; the kernel validates that
actor against an approved direct host grant on the same channel before minting
the token and records the actor in audit. Channel workers may publish a public
`pairing_url_template` in health-check details; operator commands can fill its
`{token}` placeholder with a one-use invite token without hard-coding provider
URL rules in the kernel or CLI. Conversation grants can be
conversation-wide (`sender_ref` absent) so a delegated group invite connects the
group rather than the admin who happened to claim the link. Non-direct channel
routes still require the sender to have an approved direct host grant before a
turn or local channel control is authorized; the route grant authorizes the
destination, and the direct grant authorizes the actor. Direct grant approval
uses the same `channel_grants` records for known scopes, audits
`channel.grant.approved` with no `pairing_id`, and does not bypass the direct
actor requirement for conversation or thread routes. `session_binding` is
applied only after those admission checks pass and cannot authorize an actor,
route, trigger, or attachment. Pairing claim audit stores normalized identity
and outcome facts only, never raw worker provider metadata.

Kernel bootstrap converts stale `running` session turns into durable
`interrupted` turns before they can be reused. Durable pending channel turns are
not interrupted on restart; bootstrap re-drives their channel workers so
accepted inbound work is recoverable after commit.
Attachment-waiting channel turns are not claimable. Bootstrap finalizes waiting
attachment batches older than one hour by rejecting unstaged descriptors with
`not_staged`, then queues the turn. Stale temp uploads older than one day are
removed, while committed staged blobs are retained.

## Assistant Continuity

Continuity lives under the assistant home workspace inside the selected
instance home at `workspaces/<daemon.workspace>/`.

The assistant home workspace contains:

- `MEMORY.md`
- `continuity/ACTIVE.md`
- `continuity/daily/...`
- `continuity/open-loops/...`
- `continuity/artifacts/...`
- `continuity/proposals/memory/...`

`MEMORY.md` remains a visible continuity record and search surface, but it is
not a magic runtime prompt input. LionClaw does not remember by stuffing files
into prompts. The kernel records what happened, lets controlled private context
projectors derive classed private context from exact kernel-selected source
refs, and admits those projected items only through audited prompt context
policy.

Private context projector selection is explicit operator config:

```toml
[private_context]
projector_skill = "lionclaw-private-context"
```

When unset, the kernel uses the noop private context projector. Installing a skill with
private context metadata does not auto-activate it. A selected skill declares the host
projector command in its `lionclaw.toml`:

```toml
version = 1

[private_context_projector]
command = "scripts/projector"
```

The selected skill alias is the projector id. Skill metadata does not duplicate
that identity. The command is a relative executable path under the selected
skill root. Absolute paths, parent traversal, missing files, non-executable
files, symlink files, and symlink parent components below the skill root are
rejected before the applied state is accepted.

A configured private context projector is a resident host process owned by the kernel
instance and the loaded applied state. It starts lazily on the first eligible
Main-session private context projection, runs with the skill root as its working
directory, and receives only the fixed kernel environment needed for v1:
`LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID` and `LIONCLAW_SKILL_STATE_DIR`, plus the small
ambient process-start allowlist `PATH`, `PATHEXT`, `SYSTEMROOT`, and
`SystemRoot` when present. The state directory is host-only selected-instance
state under `config/skill-state/<alias>` and is not mounted into the agent
runtime. On Unix, the projector starts in its own process group, and retirement
signals that group. The v1 lifecycle contract covers the projector and helper
subprocesses that remain in that inherited group; projector commands must not
daemonize, call `setsid` or `setpgid` to detach helpers, or leave unmanaged
background work behind.

The v1 protocol is JSONL. The kernel writes one
`PrivateContextProjectionRequest` JSON object per line to projector stdin and
reads one `PrivateContextProjection` JSON object per line from stdout. Each
request carries a fresh `request_id`, runtime/session policy, the prompt
`surface`, optional audit-safe `project_scope`, kernel-selected source refs,
optional capped `current_input` text when the prompt mode selects current user
input, and explicit `budgets` for `assistant_profile`, `user_profile`, and
`memory`. Each response must echo the same id and tag every returned item with a
class, text, and provenance. Items do not carry core-visible memory/profile
taxonomy beyond their class. There is no `protocol_version` field in v1. Unknown
response fields are ignored. Only one request is in flight at a time. The
request timeout is a kernel constant, not skill config.

Provenance may reference selected session turns, selected compaction summaries,
or skill-owned projector records. `ProjectorRecord` provenance must include the
configured projector id, a record id, and optionally a revision. Record ids and
revisions are audit handles only: 1..128 bytes, visible ASCII, no whitespace, no
control characters, and no `/` or `\` path separators. They must not contain
projected body text, and the kernel never opens the projector's storage.

Crash, EOF, malformed JSON, missing required fields, unknown enum values, wrong
request id, wrong projector id, and timeout omit all projected private context
safely. Class-level semantic failures omit only that class: known but
unrequested class, empty text, missing provenance, unsupported provenance, or
invalid `ProjectorRecord` handle. Projected output that exceeds a requested
class budget is deterministically capped or dropped within that class instead
of invalidating the whole response. Timeouts audit as `projector_timeout`; other
projector process failures audit as `projector_failed`. Fatal protocol failures
and timeouts retire the resident process before a later projection can start, so
a late response cannot satisfy a future request.

Projector output is projected context only. `PromptContextPolicy` remains the
final owner of trust-tier exclusion, byte caps, rendering, whole-section
omission on protocol-invalid output, and prompt-context audit metadata. Audit
records projector metadata, requested class budgets, source counts,
current-input byte counts, class status/reason, `byte_budget_capped`,
`item_count_capped`, and dropped item counts, never projected body text.
Projector stdout is the
protocol stream; stderr may be drained only as bounded operational diagnostics
and must not enter prompt text or prompt-context audit body fields. Final
Markdown wrapping is part of the prompt section cap; capped projected sections
keep only chunks that contribute visible projected text.

The same selected private-context skill may optionally declare a turn recorder:

```toml
[private_context_recorder]
command = "scripts/recorder"
```

Recorder command validation matches projector command validation: the command is
a relative executable path under the selected skill root, with absolute paths,
parent traversal, symlinks, non-executable files, and symlink parent components
rejected before applied state is accepted. If the selected private-context skill
does not declare `[private_context_recorder]`, the recorder hook is a no-op and
does not write audit events.

The recorder is a one-shot host process, not a resident protocol. After a session
turn has been durably finalized and `sessions.record_turn(...)` has committed,
the kernel may invoke the recorder for Main, Interactive sessions on the
`program_turn` and `channel_turn` surfaces. Failed,
timed-out, cancelled, interrupted, Untrusted, Conservative-history, and empty
transcript turns finalized through those surfaces do not send a recorder request;
when a recorder is configured, they produce only metadata skip audit. Bootstrap
restart reconciliation remains a separate kernel recovery path audited through
`session.turn.reconciled`, not `private_context.record`. Recorder failures never
fail the user turn.
Concurrent eligible turns are serialized per recorder state directory before
process spawn, including across independent kernel instances. The lock is held
outside `LIONCLAW_SKILL_STATE_DIR`, so ordinary recorder cleanup of its own
state directory cannot unlink the active lock.

The recorder runs with the skill root as working directory, stdin piped, stdout
and stderr piped, and the same small ambient process-start allowlist used for the
projector. The kernel sets `LIONCLAW_PRIVATE_CONTEXT_ID` to the selected skill
alias and `LIONCLAW_SKILL_STATE_DIR` to the host-only state directory under
`config/skill-state/<alias>`. On Unix, the recorder starts in its own process
group and timeout cleanup signals that inherited group; recorder commands must
not detach helper subprocesses from it.

The v1 recorder protocol is a single JSON object written to stdin followed by
EOF. The request carries `session_id`, `turn_id`, `sequence_no`, `runtime_id`,
`trust_tier`, `history_policy`, `surface`, optional audit-safe `project_scope`,
and capped transcript text. User text is capped at 16 KiB and assistant text at
32 KiB on UTF-8 character boundaries; each included role also reports included
and original byte counts. Whitespace-only roles are omitted. There is no
response body: stdout must be empty. Any stdout bytes are treated as
`invalid_output`. Non-zero exit status is `failed`; timeout is `timed_out`; spawn
or I/O failures are `failed`. The recorder timeout is two seconds. Stderr is
drained only to prevent pipe blocking and is never audited.

Recorder audit uses event type `private_context.record`. It records
`private_context_id`, session and turn ids, `sequence_no`, `runtime_id`,
`surface`, `project_scope`, `eligibility`, `status`, optional `reason`, optional
`exit_status`, transcript included/original byte counts, and elapsed time. It
never records user text, assistant text, stdout, stderr, or skill-owned record
contents.
`ACTIVE.md` is a kernel-generated hot projection from deterministic state and
existing continuity files; it can be selected under smaller Untrusted budgets.
Daily notes, artifacts, proposals, and open loops are visible Markdown records,
not hidden memory database rows.

Continuity search uses a derived SQLite FTS index in `lionclaw.db`; Markdown
files remain the canonical source of truth.

## Scheduler Model

The scheduler is kernel-owned, daemon-driven, and single-flight. A lease row
prevents duplicate or overlapping ticks.

Scheduled runs open fresh synthetic sessions:

- `channel_id = "scheduler"`
- `peer_id = "job:<job-id>"`
- `history_policy = conservative`

Scheduled jobs invoke the selected runtime with explicit job context and the
daemon's current runtime-visible skill set from applied filesystem state.
Optional delivery enqueues the final result in the channel outbox without
changing the latest interactive session for that conversation. Scheduler run
delivery status is `pending` after enqueue and becomes `delivered` or `failed`
only after the provider worker reports the outbox attempt outcome.

Paused jobs are skipped by normal scheduler ticks but can still be run
manually by the operator.

## Operator Launch Model

- Channel skills declare `lionclaw.toml` metadata: channel id, launch mode,
  worker entrypoint, required env names, optional env names that the operator
  may persist and pass through when present, and an optional setup hook. The v1
  metadata contract is small by design and does not claim permissions LionClaw
  does not enforce.
- Setup hooks are channel-owned commands declared under `[channel.setup]`.
  `lionclaw connect <channel> <profile> ...` and helper-style flags such as
  `lionclaw connect <channel> --provider gmail ...` pass setup arguments to
  that command after installing or refreshing a prior bundled channel snapshot,
  or selecting an installed external channel snapshot, and validate the
  generated env through the same declared-env contract as `--env-file`. Core
  does not contain provider-specific channel setup logic.
  Helpers launched by `connect` receive absolute LionClaw-managed setup env
  and state paths plus a small ambient allowlist for browser, proxy, locale,
  certificate, terminal, and temp-dir behavior; they do not inherit arbitrary
  shell secrets from the operator process, run with the installed channel skill
  directory as their working directory, and should not move generated
  credentials outside the managed paths. When setup runs, LionClaw backs up
  previous managed setup state, gives the helper a fresh managed state
  directory, and restores the previous state if setup or later channel startup
  fails. Before retaining generated or restored state, LionClaw revalidates the
  managed state directory as a private regular tree, rejects symlinks and
  special files, and hardens retained setup state permissions.
- `launch=background`: the channel worker is supervised through the platform
  backend. The current implementation uses systemd user units.
- `launch=interactive`: the channel worker is foreground-only and normally
  started by `lionclaw connect <channel>` in the current terminal. The low-level
  attach path remains available for debugging.
- Channel env is selected-instance state under `config/channels/` and may
  contain only names declared by the channel metadata. Generated unit env may
  reference that private file, but generated unit env is not the source of
  truth. Env updates merge by declared key; required values must stay
  non-empty, while an empty optional value clears the existing stored key so
  channel-owned setup helpers can remove obsolete optional values during
  reconfiguration.

Worker entrypoint resolution uses the metadata `worker` path and rejects
symlink escapes outside the skill directory.

`LIONCLAW_HOME` gets stable machine-owned `config/home-id` and managed-unit
identity state. Attach and background flows only reuse a daemon when
`/v0/daemon/info` reports the same home id, current project scope, and
daemon-compat fingerprint.
Managed systemd units are instance-scoped and carry `X-LionClaw-*` ownership
metadata so cleanup and stop operations only touch units owned by the selected
home.

## Security Posture In v0

1. Policy checks deny unless an explicit grant exists.
2. No default external channel is built into the core.
3. Runtime adapters registered by default: local `mock` only. `codex` and ACP
   profiles such as `opencode` are configured runtime profiles bound at startup.
4. Configured Codex/ACP profiles run through the shared execution planner
   and Podman backend, then map runtime output into kernel events.
5. Runtime idle timeout, hard timeout, and cancellation are kernel-enforced and
   audited. Local interactive runs default to a 30 minute idle timeout and a 2
   hour hard safety limit; daemon-backed work defaults to 30 minutes idle and 4
   hours hard unless env overrides are set.
6. Runtime execution policy supports per-turn working directory, idle timeout
   override, and constrained env passthrough. Configured kernel defaults are
   trusted directly; policy timeout bounds apply to explicit per-turn override
   requests. `LIONCLAW_*` runtime environment names are kernel-owned and are
   not accepted through env passthrough.
7. Ordinary confined runtime file work stays inside mounted work-root, runtime,
   and drafts paths.
8. Kernel brokers are reserved for explicit side effects and direct-runtime
   requests. Program-backed `channel.send` uses an explicit preset-gated
   runtime socket and enqueues provider-neutral outbox deliveries only.
   `net.egress`, `secret.request`, and `scheduler.run` remain broker-gated and
   denied until configured.
9. Audit covers API mutations, runtime plan allow/deny, runtime
   start/finish/error/timeout, channel lifecycle events, scheduler events, and
   brokered capability decisions.
10. Channel inbound is gated by scoped grants with duplicate event
    suppression and worker-controlled polling offsets.

## Planned Hardening

- Stronger egress policy and allowlist enforcement.
- Secret broker/proxy for credentials that should not be runtime-visible.
- Skill source pinning, provenance, signatures, and update review.
- Alternative confinement backends beyond the shipped OCI path.
- Wasmtime or equivalent sandbox path for untrusted helper tools, not as the
  primary program-backed runtime path.
- Runtime image provenance and security audit reporting.

## Adding A Runtime

Runtime integration is driver-per-protocol, not crate-per-product.

1. If the runtime speaks an existing conversation protocol, add a runtime
   profile only. For ACP harnesses such as OpenCode, configure
   `driver = "acp"`, `command`, `args`, model/mode, auth, confinement, and any
   native skill projection data. Do not add a Rust crate for another ACP
   product.
2. If the runtime brings a new conversation protocol, add one
   `crates/lionclaw-runtime-<protocol>` crate implementing the protocol driver
   behind the existing adapter and `RuntimeDriverProvider` boundary in
   `lionclaw-runtime-api`.
3. Keep protocol-specific auth staging, session ids, resume data, parsing, and
   raw protocol retention in that protocol crate. Core/kernel code consumes
   canonical `TurnEvent` journals and runtime profile data only.
4. Declare native skill support as profile data, for example
   `skill_projection = { kind = "native-dir", root = ".config/opencode/skills", format = "skill-md" }`.
   The kernel validates and materializes supported projection kinds but must
   not match on runtime product names.
5. Reuse workspace package versions and `workspace = true` lint settings for a
   new protocol crate. Only touch `kernel/runtime/builtins.rs` for intentional
   builtin test/kernel scaffolding.
6. Add protocol fixture tests proving canonical `RuntimeEvent` equivalence and
   at least one kernel-level integration case for live journal persistence and
   streaming behavior.
7. Update this architecture doc and `docs/MANUAL_QA.md` if the runtime or
   protocol introduces new auth, state, confinement, or operator workflow
   behavior.
