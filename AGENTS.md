# AGENTS.md

## Purpose
This repository builds LionClaw as a secure-first local agent kernel.
Contributors (human or AI) should follow the workflow and quality gates below.

## Source of Truth
- Project overview: `README.md`
- v0 scope and constraints: `docs/V0_PLAN.md`
- architecture and contracts: `docs/ARCHITECTURE.md`
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
- Treat policy and audit as first-class concerns for every privileged action.
- Build tests as composable scenarios with reusable setup helpers.
- When behavior changes, update tests and docs in the same change.

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
