# AGENTS.md

Contributor guide for any agent (human or AI) working in this repo. This file
is the **workflow and the guardrails** — not an explanation of how the engine
works. For that, read the source of truth below.

## Source of truth

*(These docs are being written; until one lands, the code is authoritative.)*

- Product overview and the everyday command path: `README.md`
- Architecture, the determinism wall, and the honesty moat: `docs/`
- Design decisions and their rationale: `docs/adr/`
- In-code: `crates/lionclaw/src/lib.rs` and `model/mod.rs` module docs.

## Guardrails you must not break

Two invariants *are* the product. Changing either needs a fault-injection test
that fails first, and a note in the PR. See `docs/` for what they mean and why.

- **The determinism wall** — `crates/lionclaw/src/model/` stays pure (only
  `std` / `serde` / `thiserror`; no I/O, clock, RNG, or async).
- **The honesty moat** — one mint site for an authoritative verdict, one path to
  a `Verified` finish, and no non-artifact role that can write or hold secrets.

## Working rules

- Clean, simple, DRY, idiomatic Rust. No jank, no half-built scaffolding, no
  tech debt. Pause on each change to find the true solution.
- **No backwards compatibility** (pre-launch): break old logs loudly rather than
  carry shims; bump `SCHEMA_VERSION` / `REDUCER_VERSION` when the log or fold
  changes, and keep `fold_litmus` / `resume` green at the new version.
- Every new guard gets a fault-injection test: break it → confirm the test
  *fails* → restore by editing it back (never `git checkout`).
- Prefer making illegal states unrepresentable over rejecting them by rule;
  exhaustive matches over `_ =>` wildcards.

## Verification (run from the repo root)

- `scripts/ci.sh` — fmt, check, clippy `-D warnings`, doc, `cargo test
  --workspace`, and the mission self-test.
- `cargo run -p lionclaw -- mission self-test` — drives the real stack
  (podman + an engine-run oracle), model-auth-free; skips cleanly without podman.

The agentic evals (`scripts/mission-eval.sh`) need a real runtime auth and are
not part of the gate; run them when changing planning or role prompts.

## For behavior-changing PRs

- A failing test first (or an explicit rationale for net-new surface).
- A note on determinism-wall / moat impact if either is touched.
- The `SCHEMA_VERSION` / `REDUCER_VERSION` bump and a passing `fold_litmus` /
  `resume` if the log or fold changed.
