# LionClaw Architecture (v0)

## Kernel Modules

- `kernel.sessions`: session lifecycle and turn history metadata.
- `kernel.skills`: installed skill registry and enable/disable state.
- `kernel.selector`: turn-time skill relevance selection.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.capability_broker`: brokered capability execution (`fs`, `net`, `secret`, `channel.send`, `scheduler`).
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.channels`: channel-skill contract and registry.
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

### Policy

- `POST /v0/policy/grant`
- `POST /v0/policy/revoke`

### Audit

- `GET /v0/audit/query`

## Runtime Adapter Contract

- `info()`
- `session_start()`
- `turn()`
- `resolve_capability_requests()`
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

Adding a new adapter:

1. Add `kernel/runtime/adapters/<adapter>.rs` implementing `RuntimeAdapter`.
2. Export it from `kernel/runtime/adapters/mod.rs`.
3. Register it in `kernel/runtime/builtins.rs`.
4. Add unit tests in the adapter module + one kernel-level integration case.

## Channel-Skill Contract

- `id()`
- `init()`
- `health()`
- `send()`

## Security Posture in v0

1. Default deny: policy checks deny unless grant exists.
2. No default external channel in core.
3. Runtime adapters registered by default: local `mock`, subprocess `codex`, and subprocess `opencode`.
4. `codex` adapter runs in secure defaults (`read-only` sandbox, `--ephemeral`) and kernel-owned capability broker routing.
5. `opencode` adapter runs in JSON event mode and maps runtime events into kernel events.
6. Kernel-enforced runtime turn timeout + cancellation path (`runtime.turn.timeout` audit event on timeout).
7. Runtime execution policy supports per-turn working directory, timeout, and env passthrough constraints.
8. Capability side effects route through kernel brokers only:
   - `fs.read` / `fs.write` use workspace-bounded filesystem broker.
   - `channel.send` uses channel registry broker path.
   - `net.egress`, `secret.request`, `scheduler.run` are broker-gated and denied until configured.
9. Auditing covers API mutations plus capability request/result decisions.

## Planned Hardening After v0

1. Wasmtime execution boundary.
2. Rootless container fallback for heavy tasks.
3. Egress proxy with allowlist enforcement.
4. Secret broker issuing scoped, short-lived credentials.
5. Skill source pinning + signatures.
