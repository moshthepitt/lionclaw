# AGENTS.md

## Purpose
This repository builds LionClaw as a secure-first local Claw: a small trusted
kernel that runs real agent CLIs inside an explicit local boundary.
Contributors (human or AI) should follow the workflow and quality gates below.

## Source of Truth
- Project overview: `README.md`
- v0 scope and constraints: `docs/V0_PLAN.md`
- architecture and contracts: `docs/ARCHITECTURE.md`
- runtime model: `docs/RUNTIME_MODEL.md`
- milestone plan: `docs/ROADMAP.md`

## Working Rules
- Prefer TDD when practical: RED -> GREEN -> REFACTOR.
- Keep the core small, explicit, and auditable.
- Preserve deterministic behavior in kernel decisions and policy checks.
- Security boundaries are enforced by kernel code, not prompt instructions.
- Channels are skills; do not bake external channels into core defaults.
- Do not change API contracts silently; update docs in the same PR.
- Keep code clean, simple, modern, idiomatic, and DRY.
- If verification is blocked by sandbox DNS/connectivity issues, rerun the same command with approved escalation rather than adding non-standard local shims.

## Implementation Discipline
- Prefer durable, domain-oriented naming over temporary phase labels.
- Encode validation intent in types and request/response contracts.
- Keep module boundaries explicit (`sessions`, `skills`, `policy`, `runtime`, `channels`, `audit`).
- Default to deny for capability checks unless explicit grants exist.
- Treat policy and audit as first-class concerns for LionClaw-owned privileged
  actions and runtime boundary decisions.
- Build tests as composable scenarios with reusable setup helpers.
- When behavior changes, update tests and docs in the same change.

## Product Lessons
- LionClaw must have one obvious everyday command path.
- Prefer one canonical confinement and runtime-auth path over parallel ways to do the same job.
- `lionclaw run [runtime]` is the canonical interactive path.
- Raw HTTP is for workers, tests, and debugging only; never document it as normal usage.
- `systemd --user` is deployment/admin plumbing for background services and many channels, not the default local interactive path.
- Runtime configuration must live in LionClaw state/config, not rely on accidental shell PATH or session env.
- LionClaw owns the product entrypoint; runtimes are real agent harnesses behind it.
- LionClaw owns the boundary, not every private tool step inside a program-backed runtime.
- Channels remain external skills/workers; do not absorb transport logic into Rust core to paper over UX gaps.
- Product-facing docs must be command-first and readable without architecture context.
- Internal implementation names such as `lionclawd`, `kernel`, and raw HTTP APIs should appear in product docs only when operationally necessary.
- The README first screen must explain LionClaw in user terms and avoid “not X / not Y” framing.
- Product-facing docs should lead with the problem, the stance, and the command the reader runs.
- The README should preserve the core thesis: Claws turn agents into assistants; LionClaw runs real agents under a small trusted core and explicit local boundary.

## Required Verification Commands
Run from repository root before considering work complete:
- `cargo fmt -- --check`
- `cargo check`
- `cargo test`

## Required for Behavior-Changing PRs
- Failing test evidence first (or explicit rationale if adding net-new surface).
- Security impact statement (policy, secrets, egress, sandbox boundary).
- API/event contract impact statement.
- Docs updated for any contract or architectural change.
