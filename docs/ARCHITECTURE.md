# LionClaw Architecture (v0)

LionClaw currently targets Unix-like systems only. The kernel's trusted
filesystem boundary and service assumptions are designed for Linux/macOS-style
Unix environments.

## Kernel Modules

- `kernel.sessions`: session lifecycle, history policy, and aggregate turn metadata.
- `kernel.session_turns`: durable per-turn history, recovery state, and partial assistant output.
- `kernel.skills`: installed skill registry and enable/disable state.
- `kernel.selector`: turn-time skill relevance selection.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.jobs`: scheduled job definitions, run records, and SQLite persistence.
- `kernel.capability_broker`: brokered capability execution (`fs`, `net`, `secret`, `channel.send`, `scheduler`).
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.scheduler`: due-job claiming, lease coordination, retry, and dispatch.
- `kernel.channel_state`: durable channel bindings, peer trust state, inbound logs, queued channel turns, outbound transcript history, and append-only channel stream delivery state.
- `kernel.continuity`: visible assistant-home continuity files, `ACTIVE.md` projection, daily notes, artifacts, open loops, memory proposals, and continuity retrieval helpers.
- `kernel.continuity_fs`: descriptor-rooted Unix filesystem helper for assistant-home continuity and hot workspace-file reads/writes.
- `kernel.audit`: append-only audit event log persisted in SQLite.
- `kernel.session_compactions`: persisted transcript compaction spans and summaries.

## API Contracts

### Session

- `POST /v0/sessions/open`
- `GET /v0/sessions/latest`
- `POST /v0/sessions/history`
- `POST /v0/sessions/action`
- `POST /v0/sessions/turn`

### Skill

- `POST /v0/skills/install`
- `GET /v0/skills/list`
- `POST /v0/skills/enable`
- `POST /v0/skills/disable`

### Channel

- `POST /v0/channels/bind`
- `GET /v0/channels/list`
- `GET /v0/channels/peers`
- `POST /v0/channels/peers/approve`
- `POST /v0/channels/peers/block`
- `POST /v0/channels/inbound`
- `POST /v0/channels/stream/pull`
- `POST /v0/channels/stream/ack`

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

### Policy

- `POST /v0/policy/grant`
- `POST /v0/policy/revoke`

### Audit

- `GET /v0/audit/query`

### Daemon Metadata

- `GET /health`
- `GET /v0/daemon/info`

`/health` is liveness only. `/v0/daemon/info` is the typed operator-facing metadata endpoint used to classify a listener before reusing it.

## Runtime Adapter Contract

- `info()`
- `session_start()`
- `turn()` emits typed runtime events into a kernel-owned sink and returns capability requests.
- `resolve_capability_requests()` emits typed follow-up events into the same sink.
- `cancel()`
- `close()`

## Capability Broker Contract

Runtime adapters submit `RuntimeCapabilityRequest` items. Kernel flow:

1. Validate requesting skill is selected for the turn.
2. Evaluate policy for the requested capability against the kernel-selected scope. Runtime-supplied scope strings may restate that scope, but they cannot widen it to another job, session, channel, or runtime scope.
3. If allowed, execute through kernel broker only.
4. Return `RuntimeCapabilityResult` to adapter.
5. Audit both request and result (`capability.request`, `capability.result`).

Runtime module layout:

- `kernel/runtime/mod.rs`: shared runtime contracts + registry.
- `kernel/runtime/builtins.rs`: mock runtime registration for tests and kernel fallback-free bootstrap.
- `kernel/runtime/execution/plan.rs`: typed execution presets, confinement config, and effective per-turn plans.
- `kernel/runtime/execution/planner.rs`: deterministic plan compilation from runtime config, preset config, and request overrides.
- `kernel/runtime/execution/backend.rs`: backend execution contract for confined runtime launch.
- `kernel/runtime/execution/oci.rs`: rootless Podman backend and typed command builder.
- `kernel/runtime/execution/process.rs`: shared process execution utility for adapters and backends.
- `kernel/runtime/adapters/mock.rs`: deterministic test adapter.
- `kernel/runtime/adapters/codex.rs`: production program-backed adapter for Codex protocol details.
- `kernel/runtime/adapters/opencode.rs`: production program-backed adapter for OpenCode protocol details.

Program-backed runtimes stream two message lanes:

- `answer`: canonical assistant reply text that is persisted into turn history.
- `reasoning`: optional live thought/progress text that channels may render or ignore.

LionClaw transports both lanes through the stream APIs, but only `answer` is treated as the durable assistant reply.
Configured OpenCode profiles are pinned to `--format json` so LionClaw always receives machine-readable events instead of a degraded plain-text stream.

The everyday confined runtime layout is mount-first:

- `/workspace`: project/workspace root with preset-controlled read-only or read-write access.
- `/runtime`: runtime-private writable state root.
- `/drafts`: runtime-private draft/output area.

For local `lionclaw run`, the project root defaults to the current working
directory and is mounted at `/workspace`. `LIONCLAW_HOME` remains LionClaw's
own state root and is not the everyday project tree.

The execution planner also injects stable runtime-private environment defaults such as `HOME=/runtime/home` and `LIONCLAW_DRAFTS_DIR=/drafts` so program-backed runtimes keep ephemeral state out of LionClaw continuity.
Interactive Codex/OpenCode turns still launch a fresh confined process for each
request, but the mounted `/runtime` state root is now scoped to the LionClaw
session, project root, and execution security shape. That lets the harness
resume its own conversation state across turns without sharing runtime-private
state across different projects or secret/network shapes.
LionClaw still keeps the canonical transcript itself. Fresh harness sessions get
replayed transcript history in the prompt envelope; resumed harness sessions get
a continuation note plus the new user input instead of the full prior transcript
on every turn.
LionClaw does not persist a separate draft registry. Draft listing scans that shared drafts directory on demand, and explicit keep/discard actions move or delete files directly from there.

Current runtime network policy is intentionally coarse: presets choose only
`network-mode = "on"` or `network-mode = "none"`. LionClaw does not expose a
fake allowlist mode before a real egress-control plane exists. `on` is mapped
explicitly to the container engine's private network mode rather than inherited
from engine defaults.

Runtime secrets are loaded from `~/.lionclaw/config/runtime-secrets.env`.
Presets either mount that whole file or mount no runtime secrets at all with
`mount-runtime-secrets = true|false`, and the Podman backend mounts it read-only
under Podman's default `/run/secrets/` directory with a LionClaw-managed name
that starts with `lionclaw-runtime-secrets-`.
LionClaw hardens the config directory to `0700` and the runtime secret file to
`0600` on Unix before loading it.

Host-only runtime auth is separate. Confined Codex turns read
`~/.lionclaw/config/runtime-auth.env` on the host, require
`OPENAI_API_KEY`, generate a per-turn local CA and TLS listener, write a
session-scoped `~/.codex/config.toml` under `/runtime/home`, and route Codex
through a short-lived HTTPS proxy at `https://host.containers.internal:<port>/v1`.
The container only sees a runtime-specific one-time placeholder bearer token
plus the generated CA certificate path via `CODEX_CA_CERTIFICATE`; the raw
OpenAI key stays on the host side of the proxy boundary. LionClaw runtime
compatibility assumes configured OCI image references are treated as immutable;
when runtime bits change, use a new image tag.

Channel bridge layout:

- `kernel/channel_state.rs`: durable channel bindings/peers/offsets/messages + stream event/cursor storage.
- `kernel/continuity.rs`: assistant-home continuity layout, proposals/open loops/artifacts, indexed search integration, and deterministic continuity projection helpers.
- `kernel/continuity_index.rs`: derived SQLite FTS index for assistant-home continuity files.
- `kernel/core.rs`: channel inbound processing, pairing/approval, continuity APIs, session snapshot lookup, and stream pull/ack APIs.
- `kernel/session_compactions.rs`: persisted structured transcript compaction summaries and ranges.
- `api/mod.rs`: HTTP routes for external channel skill workers.

Scheduler layout:

- `kernel/jobs.rs`: typed schedules (`once`, `interval`, `cron`), job/run persistence, and lease-backed due-claiming.
- `kernel/scheduler.rs`: single-flight scheduler engine, lease coordination, scheduled session execution, and final-result channel delivery.
- `kernel/core.rs`: thin job API/orchestration boundary that delegates scheduler execution.
- `daemon.rs`: background scheduler loop inside `lionclawd`.

Operator launch model:

- `launch_mode=service`: channel worker is supervised by `lionclaw service up` through the platform service manager.
- `launch_mode=interactive`: channel worker is foreground-only and started with `lionclaw channel attach <id>`.
- Worker entrypoint resolution requires `scripts/worker`.
- `LIONCLAW_HOME` gets a stable machine-owned `config/home-id`; attach and service flows only reuse a daemon when `/v0/daemon/info` reports the same `home_id`, current project scope, and daemon-compat fingerprint.

Adding a new adapter:

1. Add `kernel/runtime/adapters/<adapter>.rs` implementing `RuntimeAdapter`.
2. Export it from `kernel/runtime/adapters/mod.rs`.
3. Wire configured registration in `operator/runtime.rs`.
4. Only touch `kernel/runtime/builtins.rs` if the adapter is intentionally builtin test/kernel scaffolding.
5. Add unit tests in the adapter module + one kernel-level integration case.

## Channel-Skill Contract

External channel skills integrate over HTTP only:

1. `GET /v0/sessions/latest` to restore the latest repaired durable session snapshot for `(channel_id, peer_id)`.
2. `POST /v0/channels/inbound` to submit normalized inbound messages. Skills may pin normal inbound to a chosen session by sending `session_id`. For approved peers this queues a channel turn and returns an explicit outcome (`queued`, `duplicate`, `pairing_pending`, `peer_blocked`) plus `turn_id` when work was queued.
3. `POST /v0/sessions/action` to start `continue_last_partial`, `retry_last_turn`, or `reset_session` for a channel-backed session. `continue` and `retry` return immediately with a new `turn_id`; `reset` returns a fresh `session_id`.
4. `POST /v0/channels/stream/pull` to fetch typed outbound stream events for a consumer cursor. A fresh consumer may start from an exact sequence by sending `start_after_sequence`.
5. `POST /v0/channels/stream/ack` after a consumer has durably handled events through a sequence.
6. `GET /v0/channels/peers` + approve/block endpoints for pairing trust management.

Queued channel turns emit machine-stable status/error codes through the same stream contract. Kernel-generated lifecycle codes currently include:

- `queue.queued`
- `queue.started`
- `queue.completed`
- `queue.failed`
- `runtime.started`
- `runtime.completed`
- `runtime.error`
- `runtime.timeout`

## Session Continuity

- `sessions.history_policy` controls how incomplete turns are reused in future prompts:
  - `interactive`: carry forward partial assistant output with an explicit marker
  - `conservative`: carry forward only a structured failure note
- `session_turns` is the durable source of truth for prompt history. It records:
  - `kind = normal | retry | continue`
  - `status = running | completed | failed | timed_out | cancelled | interrupted`
  - `display_user_text`
  - `prompt_user_text`
  - `assistant_text`
  - `error_code`
  - `error_text`
  - `runtime_id`
- answer-lane text is checkpointed while a turn is still running so restart reconciliation can preserve partial replies already emitted to the user
- channel-backed running turns also persist `channel_turns.answer_checkpoint_sequence`, which is the exact stream sequence through which the durable assistant checkpoint is synchronized
- kernel bootstrap converts stale `running` session turns into durable `interrupted` turns before they can be reused
- `lionclaw run` opens `local-cli` sessions with `history_policy=interactive`.
- Recovery actions are kernel-owned:
  - `continue_last_partial`
  - `retry_last_turn`
  - `reset_session`
- The default history window is the last 12 durable turns.
- Prompt rendering prepends one bounded persisted structured transcript handoff summary before the recent raw turns.
- Channel-backed session mutation APIs (`sessions/open`, `sessions/action`, direct session turns) remain gated by channel peer approval in the kernel.

## Assistant Continuity

- Continuity lives under the assistant home workspace inside `LIONCLAW_HOME/workspaces/<daemon.workspace>/`.
- The assistant home workspace contains:
  - `MEMORY.md`
  - `continuity/ACTIVE.md`
  - `continuity/daily/...`
  - `continuity/open-loops/...`
  - `continuity/artifacts/...`
  - `continuity/proposals/memory/...`
- `MEMORY.md` is prompt-loaded but human-curated in v1.
- `continuity/ACTIVE.md` is kernel-generated from deterministic state and existing continuity files.
- Daily continuity notes are appended from deterministic kernel events such as:
  - pending pairing
  - scheduled job success/failure
  - failed turns
- Scheduler artifacts are recorded under `continuity/artifacts/...`.
- Memory proposals are written under `continuity/proposals/memory/...` and merged or rejected explicitly.
- Open loops are written under `continuity/open-loops/...` and resolved explicitly.
- Active proposal/open-loop files use deterministic title-keyed filenames of the form `"{slug}--{uuid-v5}.md"`.
- The managed active filename is the stable identity for proposal/open-loop cleanup; content edits are supported, but active-file renames are outside the managed continuity contract.
- Archives are historical records only; they do not suppress future active items with the same title.
- Continuity-adjacent API mutations treat SQLite state plus audit as the authoritative commit boundary.
- `continuity/ACTIVE.md` refresh is a derived post-commit projection; snapshot rebuilds are serialized in the kernel, and failures are audited as `continuity.refresh_failed` instead of turning committed mutations into outward API failures.
- The hot `ACTIVE.md` projection uses bounded, purpose-shaped queries for pending peers, attention jobs, and recent failed turns instead of broad list-and-filter scans.
- Continuity search uses a derived SQLite FTS index in `lionclaw.db`; the Markdown files remain the canonical source of truth.
- Read-only continuity enumeration and index-rebuild paths skip per-file `ENOENT` churn and continue from remaining canonical files, while still surfacing real boundary and permission failures.
- Transcript compaction summaries are stored in SQLite separately from file-backed continuity.
- Prompt history sees one bounded structured compaction handoff summary plus the recent raw tail.
- Active continuity state has two authorities only:
  - canonical active Markdown files under assistant home
  - each session's latest persisted compaction summary state
- Before a new compaction summary is persisted, the kernel flushes visible continuity artifacts:
  - memory proposals
  - open-loop updates
  - a daily note entry when new continuity was promoted
- Continuity reads and writes are rooted in the assistant home workspace through descriptor-based Unix filesystem operations rather than pathname preflight checks.
- When hidden semantic summarization is unavailable, deterministic kernel compaction still extracts:
  - current goal
  - constraints and preferences
  - durable memory proposals
  - open loops
  - recent files, decisions, and next steps
- Brokered filesystem access may target a different project/task root; continuity never follows that root.

## Scheduler Model

- Time-based only in v1: `once`, anchored `interval`, and cron-with-timezone.
- The scheduler is kernel-owned, daemon-driven, and single-flight by design. A single lease row prevents duplicate or overlapping ticks.
- Recurring jobs advance `next_run_at` before execution to avoid restart replay storms.
- Interrupted one-shot jobs remain claimable; interrupted recurring jobs resume from the next future slot.
- Every scheduled run opens a fresh synthetic session with:
  - `channel_id = "scheduler"`
  - `peer_id = "job:<job-id>"`
  - `history_policy = conservative`
- Scheduled jobs use explicit attached skill ids. They do not use turn-time auto-selection.
- Policy scope for scheduled work is `job:<job-id>`, separate from normal `session:<session-id>` checks.
- Optional delivery sends the final result through the existing channel stream/outbox path without changing the latest interactive session for that peer.
- Paused jobs are skipped by normal scheduler ticks but can still be run manually by the operator.

## Security Posture in v0

1. Default deny: policy checks deny unless grant exists.
2. No default external channel in core; all external transport is skill-worker code outside Rust kernel.
3. Runtime adapters registered by default: local `mock` only. `codex` and `opencode` are configured runtime profiles that bind program-backed adapters at startup.
4. Configured `codex` and `opencode` profiles run through the shared execution planner and Podman backend, then map runtime JSON output into kernel events.
5. Kernel-enforced runtime idle timeout + hard timeout + cancellation path (`runtime.turn.timeout` audit event with `timeout_kind=idle|hard`).
6. Runtime execution policy supports per-turn working directory, idle timeout override, and env passthrough constraints while the daemon keeps a separate hard timeout ceiling.
7. Ordinary confined runtime file work stays inside mounted workspace/runtime/drafts paths. Kernel brokers are reserved for explicit side effects:
   - the existing `fs.read` / `fs.write` broker remains available for narrow non-runtime surfaces and tests, not the everyday confined runtime path
   - `channel.send` records outbound transcript entries and appends typed stream events for external channel skills
   - `net.egress`, `secret.request`, `scheduler.run` are broker-gated and denied until configured
9. Auditing covers API mutations plus capability request/result decisions.
10. Channel inbound is gated by pairing approval (`pending` -> `approved`), with duplicate update suppression and worker-controlled polling offsets.

## Planned Hardening After v0

1. Wasmtime execution boundary.
2. Alternative confinement backends beyond the shipped OCI path.
3. Egress proxy with allowlist enforcement.
4. Secret broker issuing scoped, short-lived credentials for non-runtime-visible secrets.
5. Skill source pinning + signatures.
