# LionClaw v0 Plan

LionClaw v0 proves the core product: a real agent CLI, running locally under a
controlled boundary, with durable sessions, visible continuity, channels,
scheduled jobs, and auditable kernel decisions.

## What v0 Is

LionClaw v0 is a secure-first local Claw.

It:

1. runs selected agent CLIs through `lionclaw run [runtime]`,
2. compiles and enforces runtime execution plans,
3. keeps durable LionClaw sessions and turn history,
4. projects assistant identity and continuity into the runtime while mounting installed non-channel skills for runtime use,
5. supports Codex and OpenCode as program-backed runtimes,
6. treats channels as external skill workers,
7. runs time-based jobs in isolated scheduler sessions,
8. records audit events for kernel-owned decisions and boundary crossings.

## What v0 Is Not

LionClaw v0 is not a new full assistant harness.

It does not:

1. reimplement Codex, OpenCode, Claude, or Gemini tool loops,
2. mediate every internal tool call made by a program-backed runtime,
3. pretend prompt instructions are a security boundary,
4. hide continuity in an opaque memory store,
5. make raw HTTP the normal operator experience,
6. bake Telegram, terminal UI, or any other external channel into the Rust core.

## Why

1. Claws are a useful new layer above AI agents: persistence, channels,
   scheduling, context, and long-running presence.
2. That layer is dangerous if it becomes a giant process with ambient access
   to the user's machine.
3. The safer route is to run the real agent directly inside an explicit local
   boundary.
4. LionClaw should be small enough to reason about, but capable enough to be
   useful every day.

## Core Principles

1. `lionclaw run [runtime]` is the canonical interactive path.
2. The selected runtime does the agent work.
3. LionClaw owns the boundary around that work.
4. Security controls live in kernel code, runtime confinement, and policy.
5. Prompt text can guide behavior; it cannot grant permission.
6. Channels are skills and workers, not core transports.
7. Runtime configuration lives in LionClaw state/config, not accidental shell
   PATH or session env.
8. LionClaw-owned privileged actions and runtime boundary decisions are
   auditable.
9. Harness-native actions are constrained by the runtime execution plan.
10. The trusted core should stay small, explicit, and boring.

## v0 Deliverables

1. Operator CLI for onboarding, runtime registration, local runs, service
   control, channels, jobs, and continuity.
2. SQLite-backed kernel services for sessions, turns, skills, policy, audit,
   channels, jobs, and continuity metadata.
3. Program-backed runtime path for Codex.
4. Program-backed runtime path for OpenCode.
5. Shared OCI runtime image and rootless Podman backend.
6. Runtime execution planner with workspace, runtime, drafts, network, secrets,
   timeout, image, and compatibility decisions.
7. Codex host-auth staging without mounting the real host Codex home into the
   runtime container.
8. Channel bridge API for external channel skills.
9. Terminal channel and Telegram channel skill paths.
10. Scheduler for time-based jobs with fresh synthetic sessions and optional
    channel delivery.
11. Visible assistant continuity under the assistant home workspace.
12. Manual QA pass covering real Codex, real Podman confinement, service reuse,
    terminal channel flow, scheduler flow, runtime secrets, and project
    isolation.

## Security Scope

Implemented v0 security model:

- Unix-only trusted filesystem assumptions.
- Runtime launches go through the shared execution planner.
- Confined runtimes see `/workspace`, `/runtime`, `/drafts`, and selected read-only skill assets.
- Runtime network mode is coarse: `on` or `none`.
- Runtime-visible secrets are mounted only when the selected preset allows it.
- The runtime secret file is hardened to owner-only permissions on Unix.
- Codex auth is staged into runtime-private state; the real host Codex home is
  not mounted into the container.
- Runtime idle timeout, hard timeout, and cancellation are kernel-enforced.
- Channel inbound is gated by pairing approval.
- Kernel-owned mutations and runtime boundary decisions are audited.

Deferred security hardening:

- precise egress allowlists,
- secret broker/proxy for non-runtime-visible credentials,
- signed skill supply chain,
- runtime image provenance checks,
- security audit CLI,
- alternative confinement backends,
- sandboxing for untrusted helper tools beyond the primary OCI runtime path.

## Runtime Scope

In v0, a runtime may be:

- **program-backed**, where LionClaw launches a real agent CLI inside the
  execution plan; or
- **direct**, where the adapter can return explicit kernel-brokered capability
  requests.

Codex and OpenCode are program-backed runtimes. The direct path remains useful
for tests and future narrow adapters, but it is not the main product path.

## Non-Goals

1. Production-grade secret broker for all credentials.
2. Full egress-control plane.
3. Remote skill registry and signature enforcement.
4. Multi-user hosted service.
5. Windows support.
6. Built-in external channels.
7. Replacing the selected agent's native capabilities with a LionClaw tool
   broker.
