# AGENTS.md

Contributor guide for any agent (human or AI) working in this repo. This file
is the **workflow and the guardrails** — not an explanation of how the engine
works. For that, read the source of truth below.

## Source of truth

*(The public README and architecture docs were intentionally removed on this
pre-launch branch. Until the replacement docs land, the code is authoritative.)*

- Product entrypoint and everyday command path: `crates/lionclaw/src/cli.rs`
- Architecture, the determinism wall, and the honesty moat:
  `crates/lionclaw/src/lib.rs`, `crates/lionclaw/src/model/mod.rs`,
  `crates/lionclaw/src/authority.rs`, and `crates/lionclaw/src/model/verdict.rs`
- Mission-type contract: `crates/lionclaw/src/mission_type/` and
  `mission-types/software-dev/`

## Guardrails you must not break

Two invariants *are* the product. Changing either needs a fault-injection test
that fails first, and a note in the PR. Use the source-of-truth files above for
what they mean and why.

- **The determinism wall** — `crates/lionclaw/src/model/` stays pure (only
  `std` / `serde` / `thiserror`; no I/O, clock, RNG, or async).
- **The honesty moat** — one mint site for an authoritative verdict, one path to
  a `Verified` finish, and no non-artifact role that can write or hold secrets.
  The terminal review (the engine-owned, contract-blind closing judge a mission
  type declares under `[terminal-review]`) sits deliberately *outside* the
  moat's mint: its verdict is advisory, gates closure only, and can never
  upgrade a finish. Its handoff is bound to a per-attempt random nonce so
  worker-planted code executed during the review cannot forge it (known
  residual: scraping the agent's runtime state under `/runtime` is not
  defended; the verdict stays advisory either way).

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
