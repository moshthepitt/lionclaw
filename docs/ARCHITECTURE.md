# LionClaw Architecture (v0)

## Kernel Modules

- `kernel.sessions`: session lifecycle and turn history metadata.
- `kernel.skills`: installed skill registry and enable/disable state.
- `kernel.selector`: turn-time skill relevance selection.
- `kernel.policy`: capability grant/revoke and allow checks.
- `kernel.runtime`: runtime adapter contract and registry.
- `kernel.channels`: channel-skill contract and registry.
- `kernel.audit`: append-only in-memory event log.

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
- `cancel()`
- `close()`

## Channel-Skill Contract

- `id()`
- `init()`
- `health()`
- `send()`

## Security Posture in v0

1. Default deny: policy checks deny unless grant exists.
2. No default external channel in core.
3. Local mock runtime only by default.
4. Auditing for all API-initiated mutations.

## Planned Hardening After v0

1. Wasmtime execution boundary.
2. Rootless container fallback for heavy tasks.
3. Egress proxy with allowlist enforcement.
4. Secret broker issuing scoped, short-lived credentials.
5. Skill source pinning + signatures.
