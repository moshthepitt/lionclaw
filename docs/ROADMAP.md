# LionClaw Roadmap

This roadmap tracks the path from working local Claw to secure baseline.

LionClaw's product direction is now pinned:

> Claws turn agents into assistants. LionClaw turns real agent CLIs into a
> local assistant you can trust enough to leave running.

Status key:

- `[x]` done
- `[ ]` todo

## Pinned Direction

- [x] `lionclaw run [runtime]` is the canonical interactive path.
- [x] LionClaw runs real agent CLIs instead of replacing their tool loops.
- [x] Codex and OpenCode are program-backed runtimes.
- [x] Runtime selection happens at invocation or service startup.
- [x] Channels remain external skill workers.
- [x] Installed state lives under `~/.lionclaw`, not repository-relative paths.
- [x] The runtime boundary is mount-first: `/workspace`, `/runtime`, `/drafts`, and selected read-only skill assets.
- [x] LionClaw owns the boundary, session, channel, scheduler, continuity, and audit surfaces.

## Non-Negotiable Invariants

- [x] Security boundaries are enforced in kernel code, runtime confinement, and policy, never by prompt text.
- [x] Anthropic `SKILL.md` remains the skill instruction input format.
- [x] Channels are skills; no default external channel is enabled in core.
- [x] Runtime configuration lives in LionClaw config/state, not accidental shell state.
- [x] Prompt injection cannot grant capabilities.
- [x] Runtime launches go through an explicit execution plan.
- [ ] Every LionClaw-owned privileged action and every runtime boundary decision is audit-logged.
- [ ] Runtime-visible secrets are explicit, owner-only, and mounted only by preset.
- [ ] Credentials that should not be runtime-visible are served through a broker/proxy instead of raw long-lived mounts.
- [ ] Runtime egress can be denied by default and allowed only through explicit policy.

## P0 - Product Spine

Goal: make the product obvious and usable from one command path.

- [x] `lionclaw onboard`
- [x] `lionclaw runtime add ...`
- [x] `lionclaw run [runtime]`
- [x] `lionclaw run --continue-last-session [runtime]`
- [x] interactive `/continue`, `/retry`, `/reset`, `/exit`
- [x] current directory becomes the default project root
- [x] local session reuse is scoped by project and daemon compatibility
- [x] product docs lead with the Claw model and command path

Exit criteria:

- [x] A fresh user can build, onboard, register a supported runtime, and start a confined local run.
- [x] The README explains LionClaw as a personal local assistant using real agents.

## P1 - Durable Kernel

Goal: make LionClaw state survive restart and remain auditable.

- [x] SQLite persistence for sessions, turns, skills, policy, audit, jobs, and channels.
- [x] migrations and startup schema checks.
- [x] idempotent install/enable/disable/grant/revoke handling.
- [x] durable per-turn history with running/completed/failed/timed-out/cancelled/interrupted states.
- [x] partial answer checkpoints for restart recovery.
- [x] stale running turns reconciled on bootstrap.
- [x] typed daemon metadata endpoint for safe listener reuse.
- [ ] policy expiration cleanup and deterministic evaluation ordering.

Exit criteria:

- [x] Kernel survives restart with state intact.
- [x] Recovery actions are durable and command-first.
- [x] Deny-by-default policy behavior is covered by tests.

## P2 - Program-Backed Runtime Boundary

Goal: run real agents under a concrete local contract.

- [x] Runtime adapter interface.
- [x] Program-backed Codex adapter.
- [x] Program-backed OpenCode adapter.
- [x] Shared OCI runtime image.
- [x] Rootless Podman backend.
- [x] Execution planner for workspace, runtime, drafts, network, secrets, image, timeout, and compatibility decisions.
- [x] Runtime plan allow/deny audit events.
- [x] Runtime start/completion/error/timeout audit events.
- [x] Runtime idle timeout, hard timeout, and cancellation path.
- [x] Runtime-private `/runtime` state partitioned by session, project root, and security shape.
- [x] Canonical transcript remains in LionClaw while the harness may resume private state.
- [x] Codex host-auth preflight, refresh, and session-local staging.
- [x] OpenCode machine-readable event parsing.
- [ ] Runtime health/backpressure controls beyond timeout/cancel.
- [ ] Stronger runtime image provenance and compatibility reporting.
- [ ] Additional confinement backends beyond OCI where useful.

Exit criteria:

- [x] Codex can do real harness-native work inside the confined runtime.
- [x] OpenCode can stream answer/reasoning lanes through LionClaw.
- [x] No program-backed runtime receives host access outside its execution plan.
- [x] Adapter failures are surfaced through turn status, stream events, and audit.

## P3 - Visible Continuity

Goal: keep assistant continuity local, readable, and independent of any one runtime.

- [x] Assistant home workspace under `LIONCLAW_HOME/workspaces/main`.
- [x] bootstrapped identity files.
- [x] `MEMORY.md` loaded into the prompt.
- [x] kernel-generated `continuity/ACTIVE.md`.
- [x] deterministic daily notes for pairing, scheduled jobs, and failed turns.
- [x] scheduler artifacts under visible continuity files.
- [x] memory proposals and open loops as Markdown files.
- [x] SQLite FTS index derived from canonical Markdown files.
- [x] transcript compaction summaries stored separately from file-backed continuity.
- [x] prompt history uses bounded handoff summary plus recent raw turns.
- [ ] inferred open loops beyond deterministic/kernel-flushed paths.
- [ ] richer operator workflows for memory review.

Exit criteria:

- [x] Continuity remains readable and editable without special tooling.
- [x] Hidden runtime state does not become the canonical memory plane.

## P4 - Channels And Scheduler

Goal: turn the local agent into a practical assistant without growing the core into a transport monolith.

- [x] Channel bridge persistence and APIs.
- [x] Channel peer pairing and approval.
- [x] Interactive terminal channel skill.
- [x] Telegram channel skill path.
- [x] typed stream pull/ack contract.
- [x] answer and reasoning lanes.
- [x] scheduler service for `once`, anchored `interval`, and cron-with-timezone jobs.
- [x] job run records, retries, dead-letter storage, and audit trail.
- [x] final-result channel delivery for scheduled jobs.
- [x] CLI controls for channel and scheduler operations.
- [ ] inbound trust tiers (`main`, `trusted`, `untrusted`).
- [ ] outbound human approval hooks for sensitive channels.
- [ ] home channel product surface for proactive output.

Exit criteria:

- [x] A terminal channel can pair, approve, send, reattach, retry, and recover.
- [x] A scheduled job can run through a selected runtime and deliver the final answer through a channel.
- [ ] Untrusted channel traffic is isolated by stricter runtime/policy defaults.

## P5 - Runtime Portability

Goal: support more real agents without flattening them into one fake protocol.

- [x] runtime profiles configured in LionClaw state.
- [x] selected runtime at `run`, channel attach, service, and job boundaries.
- [x] Codex runtime.
- [x] OpenCode runtime.
- [ ] Claude Code runtime.
- [ ] Gemini CLI runtime.
- [ ] runtime capability matrix in docs.
- [ ] live manual QA path per supported program-backed runtime.

Exit criteria:

- [ ] Adding a runtime requires an adapter and QA notes, not a rewrite of channels, sessions, or scheduler.
- [ ] Runtime-specific auth/state behavior is documented before enabling production use.

## P6 - Skills Supply Chain

Goal: keep skills useful without making them implicitly trusted code.

- [x] local path skill install support.
- [x] canonical installed snapshot store under `~/.lionclaw/skills`.
- [x] path traversal and unsafe name protections.
- [x] deterministic skill folder hashes.
- [x] deterministic lockfile ordering.
- [x] installed non-channel skill snapshots mounted read-only into runtimes by alias.
- [x] marker-based injection cache as derived, non-authoritative output.
- [x] selector decisions and policy checks remain independent of injected text.
- [ ] generic git URL install support.
- [ ] GitHub/GitLab shorthand install support.
- [ ] install provenance metadata: source URL, ref, commit, hash, installed_at.
- [ ] update/diff flow before activation.
- [ ] optional signature verification hook, later enforceable by policy.
- [ ] priority-sorted injection summaries.
- [ ] config to disable injection globally or per runtime.

Exit criteria:

- [ ] Reinstall from lockfile reproduces identical snapshots.
- [ ] Skill updates require explicit operator approval.
- [ ] Audit includes skill provenance and hash changes.

## P7 - Security Hardening

Goal: reach a secure baseline for an assistant that can stay online.

- [x] rootless container execution backend.
- [x] coarse runtime network mode: `on` or `none`.
- [x] runtime secrets file hardened and mounted only by preset.
- [x] Codex auth staged without mounting real host Codex home.
- [x] runtime timeouts and cancellation.
- [ ] egress allowlist policy with host/scheme/path constraints.
- [ ] secret broker/proxy for non-runtime-visible credentials.
- [ ] request/response leak detection for brokered secret flows.
- [ ] per-runtime/per-job resource limits surfaced clearly in product config.
- [ ] security audit CLI with actionable findings.
- [ ] threat model doc and incident response playbook.
- [ ] runtime image provenance checks.
- [ ] Wasmtime or equivalent sandbox path for untrusted helper tools.

Exit criteria:

- [ ] Runtime network egress can be blocked unless explicitly allowed.
- [ ] Raw long-lived credentials that should stay host-only are not exposed to runtimes.
- [ ] Security controls have deny-path integration tests.

## P8 - Release And Operations

Goal: ship a usable LionClaw secure baseline.

- [ ] install script.
- [ ] service unit templates for systemd/launchd.
- [ ] `local-safe-default` config profile.
- [ ] `remote-hardened` config profile.
- [ ] `dev-relaxed` config profile.
- [ ] backup/restore workflow for DB, config, lockfile, and skill snapshots.
- [ ] upgrade/migration command with rollback plan.
- [ ] benchmark suite for latency, throughput, and restart recovery.

Exit criteria:

- [ ] Fresh install to first working local run under 15 minutes on Linux/macOS.
- [ ] Security audit command reports green on the default profile.
- [ ] Upgrade path preserves state and policy without manual surgery.

## Tracking Cadence

- [ ] Weekly: milestone progress review and scope correction.
- [ ] Per behavior change: update tests and docs in the same change.
- [ ] Per security-relevant change: include a security impact statement.
