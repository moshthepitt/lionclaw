# LionClaw v0 Plan

## What

LionClaw v0 is a local daemon kernel that:

1. manages sessions,
2. loads and tracks Anthropic skills,
3. selects skills for each turn,
4. enforces capability policy,
5. routes turns to runtime adapters,
6. treats channels as installable skills,
7. records an audit trail.

## Why

1. Keep a small auditable core (NanoClaw-style minimalism).
2. Keep security invariants in kernel code (IronClaw-style secure-first posture).
3. Keep runtime optionality (`codex`, `claude-code`, `gemini-cli`) with one control plane.
4. Keep skill compatibility by preserving Anthropic skill format unchanged.

## How

1. Rust daemon with explicit service boundaries.
2. Anthropic `SKILL.md` parsing for identity/description metadata only.
3. Separate kernel policy store keyed by skill identity/hash.
4. Default-deny capability checks for all privileged actions.
5. Local-first operation (no default remote channel enabled).

## Guiding Principles

1. Small core, explicit boundaries.
2. Security is not a skill.
3. Channels are skills.
4. Anthropic skill format is immutable input.
5. Policy enforced at runtime boundary.
6. Secrets are brokered, never mounted into untrusted runtime contexts.
7. Egress is deny-by-default in final design (v0 includes policy scaffolding).
8. Every privileged action is auditable.
9. Versioned installs and permission diffs before activation.
10. Runtime adapters are replaceable, not special-cased.

## v0 Deliverables

1. Kernel API endpoints for session/skill/policy/audit basics.
2. SQLite-backed implementations of kernel services.
3. Runtime adapter interface + mock adapter.
4. Channel-skill interface + local channel stub.
5. Skill selection pipeline with deterministic, simple scoring.

## Non-Goals (v0)

1. Production sandboxing (WASM/container) implementation.
2. Production-grade secret broker.
3. Full SKILL.md semantic execution.
4. Remote registry and signature enforcement (planned in M3/M4).
