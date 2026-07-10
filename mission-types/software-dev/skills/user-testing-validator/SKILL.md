---
name: user-testing-validator
description: Real-surface validation coordinator for engineering validation assignments. Exercises assigned assertions through browser, API, CLI, background, generated-artifact, migration/data, public-library, or parity surfaces and returns per-target verdicts with fresh evidence.
---

# User Testing Validator

Use this skill when the validation assignment requires real user, caller, operator, or consumer-surface evidence for engineering assertions.

Worker-authored tests, source inspection, and worker screenshots are supporting context only. Fresh validator-collected evidence is the verdict source.

## Inputs

Read:

- Validation assignment, assigned target ids, requested surface/method, and any
  assignment-level setup or dependency notes.
- Assigned contract assertions, including compact fields/labels such as
  `Surface`, `Needs`, `Behavior`, `Evidence`, and optional `Fail`, `Oracle`, or
  `Scope`.
- `AGENTS.md`.
- Setup/oracle/credential/fixture/source-baseline paths and evidence requirements
  cited by the assignment or contracts.
- Latest worker report for each assigned target when present, plus prior
  validator reports when relevant. Treat reports as claims, not proof; do not
  let them anchor the verdict.

## Surface Selection

Choose the surface that matches the contract:

- Browser/UI: real navigation, interaction, visual/state checks, console errors, relevant network observations.
- HTTP API: real request/response traces, auth context, body/status/schema, persistence side effects.
- CLI/TUI: real commands or interactive steps, stdin/stdout/stderr, exit codes, TTY behavior when relevant.
- Background job: trigger, processing, logs, emitted events, retries, outputs, idempotency.
- Generated artifact/file output: run generator, inspect artifact, compare schema/golden/checksum, verify reproducibility.
- Migration/data: before/after state, existing-row compatibility, locks, idempotency, rollback constraints.
- Public library/API: import/call snippets, exported symbols, signatures/types, return/error behavior.
- Porting parity: source baseline, same inputs, differential command/API/module examples, accepted divergences.

Do not choose a lower-level shortcut merely because it is easier unless the contract explicitly makes that surface authoritative.

## Procedure

1. **Prepare setup**
   - Run required setup assigned by the assignment, contract, skill, or
     `AGENTS.md`.
   - Create disposable probes when useful to expose bugs: temporary tests,
     scripts, sample repos, fuzz cases, fixtures, data prefixes, or input
     corpora. Keep probes in temporary or evidence locations and do not mutate
     the candidate product or official oracles to change the verdict.
   - Respect off-limits surfaces from the user request, assignment, contract, or
     skill. Do not read hidden verifier internals, hidden tests, holdout labels,
     forbidden baseline paths, or forbidden oracle files while setting up or
     validating.
   - Parse each target's `Needs` before exercising the surface. Verify required
     prerequisite assertions, setup, fixtures, services, credentials, source
     baselines, accepted decisions, or oracles are present.
   - Use assigned accounts, namespaces, ports, temp dirs, data prefixes, and credentials.
   - If setup or a required `Needs` entry fails, attempt one non-disruptive
     recovery. If still blocked, fail affected targets with setup evidence.

2. **Partition lanes when useful**
   - Use `flow-validator` subagents for independent surface groups when resources can be isolated.
   - Give each lane target ids, contract paths or bodies, surface/method,
     relevant `Needs`, required `Evidence`, allowed resources, unique evidence
     subdirectory or artifact prefix, non-goals, and output schema.
   - Do not run parallel lanes against shared mutable resources without isolation.
   - Reject or fail lane results whose artifacts cannot be attributed to exact
     target ids. Assigned targets may not be reported as `skipped`; skipped,
     missing, or blocked assigned-target results map to `passed=false`.

3. **Exercise each assertion**
   - Perform the actor workflow from setup to expected result.
   - Capture evidence named by the target's `Evidence` field and validation
     assignment.
   - Compare observed behavior to `Behavior`, `Surface`, `Needs`, `Evidence`, and
     any optional `Fail`, `Oracle`, or `Scope` constraints.

4. **Save evidence**
   - Write screenshots, traces, logs, terminal captures, raw outputs, or other artifacts under `<evidence_dir>`.
   - Use descriptive filenames or subdirectories that include target id and lane id
     when applicable. Do not overwrite another target or lane's artifacts.

5. **Write regression ledgers for failures**
   - For each failed item, write `<regressions_dir>/<item_id>.md` with setup,
     unmet `Needs`, flow, expected, observed, missing or collected `Evidence`, and
     artifact paths.

6. **Synthesize verdicts**
   - `passed=true` only when required fresh evidence exists and observed behavior
     matches the contract and any assignment-added checks.
   - `passed=false` for behavior mismatch, blocked setup, missing oracle, missing evidence, unverifiable assertion, or wrong surface.
   - For subagent lanes, any `fail`, `blocked`, assigned-target `skipped`, missing
     assigned-target result, or missing required artifact maps to parent
     `passed=false` for that target.

## Minimum Evidence Floors

- Browser/UI: screenshots, direct interaction steps, console error check, relevant network observations.
- API: request/response trace with status and relevant body/headers; auth and persistence checks when applicable.
- CLI/TUI: command line or interaction steps, stdout/stderr, exit code, terminal snapshot when useful.
- Background job: trigger/setup, logs/events/output, state change, retry/failure visibility when applicable.
- Generated artifact: artifact path, generation command, diff/checksum/schema/golden check when useful.
- Migration/data: before/after state and compatibility/idempotency evidence.
- Public library/API: import/call snippet and observed public behavior.
- Porting parity: source baseline, differential/golden output, accepted-divergence evidence.

Missing required evidence means `passed=false`.

## Report

```markdown
## Setup
- <services, commands, accounts, namespaces, source baselines>

## Per-Target Verdicts
### <target-id>: passed | failed
- Surface: <browser/API/CLI/job/artifact/data/library/parity>
- Needs: <checked prerequisites, unmet dependencies, or None>
- Steps: <what was exercised>
- Evidence: <artifact paths and observations>
- Reason: <why contract passed or failed>

## Frictions And Blockers
- <deduped by root cause>

## Review Limitations
- <anything not tested and why>

## Guidance Suggestions
- <optional setup/skill/AGENTS.md suggestions for orchestrator>
```

Call `end_node` with one item per target, then exit immediately.
