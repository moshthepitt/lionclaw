# LionClaw Architecture (v0)

## Kernel Modules

- `kernel.sessions`: session lifecycle and turn history metadata.
- `kernel.skills`: installed skill registry and enable/disable state.
- `kernel.selector`: turn-time skill relevance selection.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.capability_broker`: brokered capability execution (`fs`, `net`, `secret`, `channel.send`, `scheduler`).
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.channel_state`: durable channel bindings, peer trust state, inbound logs, outbound transcript history, and append-only channel stream delivery state.
- `kernel.audit`: append-only audit event log persisted in SQLite.

## API Contracts

### Session

- `POST /v0/sessions/open`
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

### Policy

- `POST /v0/policy/grant`
- `POST /v0/policy/revoke`

### Audit

- `GET /v0/audit/query`

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
2. Evaluate policy for requested capability and scope.
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
- `kernel/core.rs`: channel inbound processing, pairing/approval, and stream pull/ack APIs.
- `api/mod.rs`: HTTP routes for external channel skill workers.

Adding a new adapter:

1. Add `kernel/runtime/adapters/<adapter>.rs` implementing `RuntimeAdapter`.
2. Export it from `kernel/runtime/adapters/mod.rs`.
3. Register it in `kernel/runtime/builtins.rs`.
4. Add unit tests in the adapter module + one kernel-level integration case.

## Channel-Skill Contract

External channel skills integrate over HTTP only:

1. `POST /v0/channels/inbound` to submit normalized inbound messages.
2. `POST /v0/channels/stream/pull` to fetch typed outbound stream events for a consumer cursor.
3. `POST /v0/channels/stream/ack` after a consumer has durably handled events through a sequence.
4. `GET /v0/channels/peers` + approve/block endpoints for pairing trust management.

## Security Posture in v0

1. Default deny: policy checks deny unless grant exists.
2. No default external channel in core; all external transport is skill-worker code outside Rust kernel.
3. Runtime adapters registered by default: local `mock`, subprocess `codex`, and subprocess `opencode`.
4. `codex` adapter runs in secure defaults (`read-only` sandbox, `--ephemeral`) and kernel-owned capability broker routing.
5. `opencode` adapter runs in JSON event mode and maps runtime events into kernel events.
6. Kernel-enforced runtime turn timeout + cancellation path (`runtime.turn.timeout` audit event on timeout).
7. Runtime execution policy supports per-turn working directory, timeout, and env passthrough constraints.
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
