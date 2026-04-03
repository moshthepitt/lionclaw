# LionClaw Architecture (v0)

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
- `kernel.continuity`: visible assistant-home continuity files, `ACTIVE.md` projection, daily notes, and artifact files.
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
- `kernel/runtime/builtins.rs`: built-in adapter IDs + registration.
- `kernel/runtime/adapters/mock.rs`: deterministic test adapter.
- `kernel/runtime/adapters/codex.rs`: production subprocess adapter.
- `kernel/runtime/adapters/opencode.rs`: production subprocess adapter.
- `kernel/runtime/adapters/subprocess.rs`: shared subprocess execution utility.

Channel bridge layout:

- `kernel/channel_state.rs`: durable channel bindings/peers/offsets/messages + stream event/cursor storage.
- `kernel/continuity.rs`: assistant-home continuity layout and deterministic continuity projection.
- `kernel/core.rs`: channel inbound processing, pairing/approval, session snapshot lookup, and stream pull/ack APIs.
- `kernel/session_compactions.rs`: persisted transcript compaction summaries and ranges.
- `api/mod.rs`: HTTP routes for external channel skill workers.

Scheduler layout:

- `kernel/jobs.rs`: typed schedules (`once`, `interval`, `cron`), job/run persistence, and lease-backed due-claiming.
- `kernel/scheduler.rs`: single-flight scheduler engine, lease coordination, scheduled session execution, and final-result channel delivery.
- `kernel/core.rs`: thin job API/orchestration boundary that delegates scheduler execution.
- `daemon.rs`: background scheduler loop inside `lionclawd`.

Operator launch model:

- `launch_mode=service`: channel worker is supervised by `lionclaw service up` through the platform service manager.
- `launch_mode=interactive`: channel worker is foreground-only and started with `lionclaw channel attach <id>`.
- Worker entrypoint resolution prefers `scripts/worker` and falls back to legacy `scripts/worker.sh`.
- `LIONCLAW_HOME` gets a stable machine-owned `config/home-id`; attach and service flows only reuse a daemon when `/v0/daemon/info` reports the same `home_id`.

Adding a new adapter:

1. Add `kernel/runtime/adapters/<adapter>.rs` implementing `RuntimeAdapter`.
2. Export it from `kernel/runtime/adapters/mod.rs`.
3. Register it in `kernel/runtime/builtins.rs`.
4. Add unit tests in the adapter module + one kernel-level integration case.

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
- Prompt rendering may prepend a bounded number of persisted transcript compaction summaries before the recent raw turns.
- Channel-backed session mutation APIs (`sessions/open`, `sessions/action`, direct session turns) remain gated by channel peer approval in the kernel.

## Assistant Continuity

- Continuity lives under the assistant home workspace inside `LIONCLAW_HOME/workspaces/<daemon.workspace>/`.
- The assistant home workspace contains:
  - `MEMORY.md`
  - `continuity/ACTIVE.md`
  - `continuity/daily/...`
  - `continuity/open-loops/...`
  - `continuity/artifacts/...`
- `MEMORY.md` is prompt-loaded but human-curated in v1.
- `continuity/ACTIVE.md` is kernel-generated from deterministic state and existing continuity files.
- Daily continuity notes are appended from deterministic kernel events such as:
  - pending pairing
  - scheduled job success/failure
  - failed turns
- Scheduler artifacts are recorded under `continuity/artifacts/...`.
- Transcript compaction summaries are stored in SQLite separately from file-backed continuity.
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
3. Runtime adapters registered by default: local `mock`, subprocess `codex`, and subprocess `opencode`.
4. `codex` adapter runs in secure defaults (`read-only` sandbox, `--ephemeral`) and kernel-owned capability broker routing.
5. `opencode` adapter runs in JSON event mode and maps runtime events into kernel events.
6. Kernel-enforced runtime idle timeout + hard timeout + cancellation path (`runtime.turn.timeout` audit event with `timeout_kind=idle|hard`).
7. Runtime execution policy supports per-turn working directory, idle timeout override, and env passthrough constraints while the daemon keeps a separate hard timeout ceiling.
8. Capability side effects route through kernel brokers only:
   - `fs.read` / `fs.write` use workspace-bounded filesystem broker.
   - `channel.send` records outbound transcript entries and appends typed stream events for external channel skills.
   - `net.egress`, `secret.request`, `scheduler.run` are broker-gated and denied until configured.
9. Auditing covers API mutations plus capability request/result decisions.
10. Channel inbound is gated by pairing approval (`pending` -> `approved`), with duplicate update suppression and worker-controlled polling offsets.

## Planned Hardening After v0

1. Wasmtime execution boundary.
2. Rootless container fallback for heavy tasks.
3. Egress proxy with allowlist enforcement.
4. Secret broker issuing scoped, short-lived credentials.
5. Skill source pinning + signatures.
