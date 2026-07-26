# Scripts

Developer and CI helpers — not the everyday path. For real use, install the
mission types once and drive a mission:

```bash
lionclaw install && lionclaw doctor
lionclaw mission start --type software-dev --objective "…"
lionclaw mission advance   # drives until it parks, finishes, or awaits input
```

## Available scripts

- `ci.sh` — the local CI gate mirroring the GitHub Actions `ci` workflow
  (fmt, clippy, doc, Rust/Python tests, and the podman-gated mission
  self-test).
- `mission-eval.sh` — the agentic multi-run eval (needs podman + a codex auth).
- `mission-fixture.sh` — fixture helpers for the eval.
- `../benchmark/frontierswe/run-one.sh` — pinned one-task FrontierSWE bring-up
  through Harbor and the external LionClaw lead supervisor.

## Usage

Run the same checks as CI:

```bash
./scripts/ci.sh
```
