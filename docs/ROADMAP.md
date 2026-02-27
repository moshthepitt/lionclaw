# LionClaw Roadmap

## M1 - Kernel Foundation

1. Daemon scaffold
2. Core APIs
3. Session/skill/policy/audit services
4. Mock runtime + local channel stub

## M2 - Runtime Layer

1. ACP-style runtime adapter execution manager
2. Adapters for `codex`, `claude-code`, `gemini-cli`
3. Session persistence and resume semantics
4. Runtime health and backpressure controls

## M3 - Skill Supply Chain and Security Controls

1. Skill source pinning and lockfile
2. Skill diffing on update
3. Signature verification support
4. Permission approval UX hooks

## M4 - Sandbox and Secret/Egress Hardening

1. Wasmtime sandbox for tool execution
2. Rootless container fallback
3. Mandatory outbound proxy with allowlist
4. Secret broker with scoped token injection
5. Leak detection and expanded audit telemetry
