# LionClaw Roadmap (Execution Checklist)

This roadmap is the execution board to reach:

1. NanoClaw-level day-to-day usability, and
2. a stricter secure-first kernel model.

Status key:
- `[x]` done
- `[ ]` todo

## Pinned Direction (2026-02-27)

- [x] Record binary-install + runtime-agnostic target model in
      `docs/BINARY_RUNTIME_AGNOSTIC_MODEL.md`.
- [x] Keep runtime selection at invocation (`lionclaw run` / `lionclaw service up`), not channel install/bind.
- [ ] Keep channel integrations as external skill workers, never built-in channel transport.
- [ ] Move all installed-state assumptions to `~/.lionclaw` (no repo-path coupling).

## Non-Negotiable Invariants

- [ ] Anthropic `SKILL.md` format remains unchanged input.
- [ ] Security boundaries are enforced in kernel code, never by prompt text.
- [ ] Channels are skills; no default external channel is enabled in core.
- [ ] Default deny for privileged capabilities (`fs`, `exec`, `net`, `secret`, `channel.send`).
- [ ] Every privileged action is policy-checked and audit-logged.
- [ ] Secrets are brokered; raw long-lived host secrets are never mounted into runtime sandboxes.

## NanoClaw-Level Run Target (Definition)

To claim "NanoClaw-level runnable", LionClaw must support all items below:

- [ ] Local daemon can run continuously and recover after restart.
- [ ] At least one external messaging channel works end-to-end as an installable skill.
- [ ] Per-peer or per-group session isolation with durable turn history.
- [ ] Scheduled tasks/jobs that can execute skills and send responses.
- [ ] Runtime adapters for at least `codex`, `claude-code`, and `gemini-cli`.
- [ ] Skill install/list/enable/disable/update from local or git sources.
- [ ] Clear trust-tier routing (`main` vs `untrusted`) with different policy defaults.
- [ ] Security hardening baseline (sandboxing, egress controls, secret broker, audit query API).

## P0 - Kernel Foundation (Current State)

- [x] Daemon scaffold (`lionclawd`) and health endpoint.
- [x] Core API surface for sessions, skills, policy, and audit.
- [x] SQLite-backed services for sessions/skills/policy/audit.
- [x] Runtime adapter interface and mock runtime adapter.
- [x] Channel bridge persistence + APIs (`bind`, `inbound`, `stream pull/ack`, peer pairing).
- [x] Deterministic skill selector scaffold.
- [x] Basic audit logging for API mutations.

## P1 - Durable Kernel and Typed Policy (Next)

Goal: replace skeleton persistence and stringly policy with durable, typed controls.

- [x] Add SQLite persistence layer for sessions, turns, skills, policy grants, and audit events.
- [x] Add migrations and startup schema checks.
- [x] Replace in-memory stores with repository traits backed by SQLite.
- [x] Add typed capability enum and typed scope model (remove free-form strings where possible).
- [ ] Enforce policy checks for all privileged operations, not only `skill.use`.
- [ ] Add policy expiration cleanup and deterministic evaluation ordering.
- [ ] Add idempotent request handling for install/enable/disable/grant/revoke endpoints.

Exit criteria:
- [ ] Kernel survives restart with full state intact.
- [ ] Policy checks are centralized and covered by tests (deny-by-default proven).
- [ ] `cargo fmt -- --check`, `cargo check`, `cargo test` pass with new persistence backend.

## P2 - Runtime Adapters and Brokered Capability Loop

Goal: real runtime support with kernel-owned execution boundaries.

- [x] Implement subprocess adapter manager for long-lived runtime sessions.
- [x] Implement `codex` adapter.
- [ ] Implement `claude-code` adapter.
- [ ] Implement `gemini-cli` adapter.
- [x] Define runtime protocol for capability requests (`capability.request` / `capability.result`).
- [x] Route all side effects through kernel brokers (fs/net/secret/channel/send/scheduler).
- [ ] Add runtime health, timeout, cancellation, and backpressure controls.
- [x] Add per-runtime working directory and environment isolation policy.

Exit criteria:
- [ ] Same API flow works across all three adapters.
- [ ] No adapter performs privileged host actions without broker mediation.
- [ ] Adapter failures are observable in audit and surfaced via API errors.

## P3 - Skills Supply Chain (Secure Install/Update)

Goal: make skill install/update reproducible and auditable.

- [ ] Implement source parser support for local paths.
- [ ] Implement source parser support for generic git URLs.
- [ ] Implement source parser support for GitHub/GitLab shorthand and full URLs.
- [ ] Add canonical on-disk skill store for installed snapshots.
- [ ] Add path traversal and unsafe name protections for install paths.
- [ ] Compute deterministic skill folder hashes (path + content).
- [ ] Add project lockfile (`lionclaw.lock`) with deterministic ordering.
- [ ] Add install provenance metadata (source URL, ref, commit, hash, installed_at).
- [ ] Add update/diff flow before activation (show changed files and hash delta).
- [ ] Add signature verification hook (initially optional/warn-only, then enforceable policy).

Exit criteria:
- [ ] Reinstall from lockfile reproduces identical skill snapshots.
- [ ] Skill updates require explicit operator approval path.
- [ ] Audit includes skill provenance and pre/post hashes for every install/update.

## P4 - Skill Injection and Selection (Vercel/Inject-Inspired, Security-Preserving)

Goal: improve skill usage ergonomics without weakening trust boundaries.

- [ ] Add deterministic skill discovery in known roots (project + user scopes).
- [ ] Add skill metadata cache for selector performance.
- [ ] Add optional `INJECT.md` support as non-authoritative context summary.
- [ ] Implement marker-based injection blocks for generated instruction files.
- [ ] Make injection idempotent and replace-only between markers.
- [ ] Add priority ordering (`high`/`normal`/`low`) for injected summaries.
- [ ] Keep capability enforcement independent from injected content.
- [ ] Add config to disable injection globally or per runtime.

Exit criteria:
- [ ] Injection can improve relevance/recall but cannot grant permissions.
- [ ] Re-running injection produces no diff when inputs are unchanged.
- [ ] Selector decisions and policy checks remain deterministic and auditable.

## P5 - Channels-As-Skills and Scheduler

Goal: deliver practical assistant workflows while preserving core minimalism.

- [ ] Implement channel skill contract package format and lifecycle.
- [ ] Implement at least one production external channel skill (Telegram first).
- [ ] Add inbound message trust classification (`main`, `trusted`, `untrusted`).
- [ ] Add pairing/allowlist flow for unknown inbound peers.
- [ ] Add outbound policy checks (`channel.send`) with human approval hooks.
- [x] Add scheduler service for recurring jobs with per-job capability scope.
- [x] Add retries, dead-letter storage, and job audit trail.
- [x] Add CLI controls for channel and scheduler operations.

Exit criteria:
- [ ] End-to-end flow: inbound message -> selected skills -> runtime -> outbound reply.
- [ ] Untrusted channel traffic is isolated by stricter policy profile.
- [x] Scheduler jobs cannot escalate privileges beyond explicit grants.

## P6 - Sandbox, Secrets, and Egress Hardening

Goal: reach secure-first runtime boundary comparable to IronClaw principles.

- [ ] Add Wasmtime sandbox path for untrusted tool execution.
- [ ] Add rootless container fallback for heavy/legacy tasks.
- [ ] Add mandatory outbound proxy mode for runtime network calls.
- [ ] Add egress allowlist policy (host + scheme + optional path constraints).
- [ ] Add secret broker issuing scoped short-lived credentials.
- [ ] Add request/response leak detection for brokered secret flows.
- [ ] Add per-skill and per-session resource limits (cpu/mem/time/rate).
- [ ] Add security event telemetry into audit stream.

Exit criteria:
- [ ] Runtime cannot exfiltrate raw long-lived host secrets.
- [ ] Runtime network egress is blocked unless explicitly allowed by policy.
- [ ] Security controls are test-covered with deny-path integration tests.

## P7 - Release Hardening and Operational Readiness

Goal: ship a usable "LionClaw v1 (secure baseline)".

- [ ] Add install script and service unit templates (systemd/launchd).
- [ ] Add `local-safe-default` config profile.
- [ ] Add `remote-hardened` config profile.
- [ ] Add `dev-relaxed` config profile.
- [ ] Add security audit CLI command with actionable findings.
- [ ] Add backup/restore workflow for DB + lockfile + skill snapshots.
- [ ] Add upgrade/migration command with rollback plan.
- [ ] Add threat model doc and incident response playbook.
- [ ] Add benchmark suite for latency, throughput, and restart recovery.

Exit criteria:
- [ ] Fresh install to first working channel under 15 minutes on Linux/macOS.
- [ ] Security audit command reports green on default profile.
- [ ] Upgrade path preserves state and policy without manual surgery.

## What To Borrow From `vercel-labs/skills`

Adopt:

- [ ] Multi-source install parsing UX (local, git, GitHub/GitLab forms).
- [ ] Canonical skill store plus per-agent projection strategy.
- [ ] Deterministic project lockfile with sorted entries and minimal merge churn.
- [ ] Deterministic folder hashing for update detection.
- [ ] Safe name sanitization and path safety checks.

Do not adopt as-is:

- [ ] Any trust assumption that installed skills are inherently safe.
- [ ] Any path that lets skill text implicitly bypass kernel policy.
- [ ] Any update mechanism that auto-activates changed skills without explicit review.

## What To Borrow From `skills-inject`

Adopt:

- [ ] Marker-based idempotent injection (`start/end` block replacement).
- [ ] Priority-sorted injection summaries.
- [ ] Auto-detection of skill directories and target instruction files.

Constrain:

- [ ] Treat injected text as hinting context only, not authority.
- [ ] Disable injection for untrusted skill sources by default.
- [ ] Include source hash and skill id in injected section header for traceability.

## Tracking Cadence

- [ ] Weekly: milestone progress review and scope correction.
- [ ] Per behavior change: update tests + docs in same change.
- [ ] Per security-relevant change: add explicit security impact statement.
