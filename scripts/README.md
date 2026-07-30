# Scripts

Developer and CI helpers — not the everyday path. For real use:

```bash
lionclaw install && lionclaw doctor
lionclaw run               # defaults to the Codex runtime
lionclaw run opencode      # or select another configured runtime
```

The real agent session selects a generic method, proposes repository-specific
work and command oracles, and drives only actions advertised by current
mission state.

## Available scripts

- `ci.sh` — the local CI gate mirroring the GitHub Actions `ci` workflow
  (fmt, clippy, doc, Rust tests, and the podman-gated mission self-test).
- `mission-eval.sh` — the agentic multi-run eval (needs podman + a codex auth).
- `mission-fixture.sh` — fixture helpers for the eval.

## Usage

Run the same checks as CI:

```bash
./scripts/ci.sh
```
