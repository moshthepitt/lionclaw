---
name: scrutiny-validator
description: Adversarial scrutiny procedure for engineering validation assignments. Runs hard-gate commands, reviews the current implementation and evidence integrity against assigned contracts, can use feature-reviewer lanes, and returns per-target verdicts.
---

# Scrutiny Validator

Use this skill when the validation assignment asks for implementation scrutiny, hard-gate command review, evidence-integrity audit, or source review for engineering targets.

Scrutiny is not a substitute for real user/caller/operator surface validation. Use `user-testing-validator` or another surface-specific validator when the contract requires behavior through a real interface.

## Inputs

Read:

- Validation assignment, including exact target ids, assigned validation method,
  and any assignment-level setup or dependency notes.
- Assigned assertions. Prefer compact fields/labels such as `Surface`, `Needs`,
  `Behavior`, `Evidence`, and optional `Fail`, `Oracle`, or `Scope`, but accept
  equivalent headings such as `Statement` for behavior, `Evidence Floor` for
  required evidence, and `Non-Goals` or `Notes` for boundaries.
- `AGENTS.md`.
- Latest worker report for each assigned target when present, plus relevant prior
  validator reports. Treat reports as claims, not proof.
- Current product checkout files related to the targets. Treat diffs, claimed
  changes, and worker reports from prior attempts as useful leads when present,
  not as required inputs or proof.
- Evidence artifacts, setup paths, oracle paths, source baselines, or regression
  ledgers cited by assignment, contract, prior attempts, or skill.

## Procedure

1. **Establish scope**
   - Identify exact target ids and the current product behavior, files, and
     evidence surfaces being scrutinized.
   - Parse every assigned assertion into behavior, surface, required evidence,
     prerequisites, oracle, failure conditions, and scope boundaries. Accept
     equivalent headings such as `Statement`, `Evidence Floor`, `Non-Goals`, and
     `Notes` when they provide the same meaning.
   - If behavior, surface, or required evidence cannot be determined from the
     assertion and validation assignment, mark the target unverifiable.
   - Map each target's `Needs` and `Evidence` requirements before reviewing
     implementation or evidence surfaces. Do not assume prerequisites, setup,
     fixtures, services, source baselines, or accepted decisions that are not
     present.
   - Map each target to implementation files and evidence-producing files.

2. **Run hard-gate commands**
   - Run tests, lint, typecheck, build, generated-output checks, migration checks, or contract-required commands relevant to the target.
   - Capture command, exit code, focused output, and saved artifact path when the
     output is too large or important for the report alone.
   - Save durable scrutiny artifacts under `<evidence_dir>` when useful: command
     logs, focused outputs, generated-output diffs, fixture/golden review notes,
     or source-baseline comparison notes.
   - Do not hide exit codes with output truncation pipelines.

3. **Review implementation**
   - Confirm the current implementation satisfies the contract behavior, surface,
     required evidence, and any fail, oracle, scope, non-goal, or notes
     constraints, and accounts for every relevant prerequisite.
   - Check edge cases, error paths, data integrity, auth/authz, compatibility, migration safety, idempotency, and public API behavior when relevant.
   - Flag responsibility drift outside assignment or contract scope.

4. **Review evidence integrity**
   - Inspect changed or evidence-sensitive tests, fixtures, benchmark scripts,
     verifier code, golden files, generated expected outputs, source baselines,
     data files, and cited evidence artifacts when they are allowed and relevant.
   - Restrict that inspection to allowed paths. Do not read hidden verifier
     internals, hidden tests, holdout labels, forbidden baseline paths, or other
     off-limits surfaces; use allowed public artifacts and report unverifiable
     proof gaps instead.
   - Compare the available proof against each target's `Evidence` field. Missing
     required evidence owned by this validation task means `passed=false`.
   - If the contract requires real user/caller/operator surface evidence, do not
     pass it through scrutiny alone unless the validation assignment explicitly
     scopes this run as the scrutiny lane and names a sibling validator that owns
     the real-surface evidence.
   - Fail targets when proof was weakened, mocked away, or made benchmark/test-only.

5. **Use feature-reviewer lanes when useful**
   - For broad implementation areas or multiple feature areas, spawn
     `feature-reviewer` with one bounded review question, assigned target ids,
     contract paths or bodies, relevant current-checkout files or surfaces,
     relevant evidence artifacts or command output, claimed changes or prior
     reports as leads when useful, allowed commands/probes/evidence-write
     locations, off-limits surfaces, and expected output shape.
   - The subagent report is advisory. You own the verdict.

6. **Write regression ledger entries**
   - For each failed or unverifiable item, write
     `<regressions_dir>/<item_id>.md` with the failing command, review finding,
     observation, unmet `Needs`, missing `Evidence`, artifact paths, and
     reproduction details.

7. **Return verdicts**
   - One `items[]` entry per assigned target.
   - `passed=true` only when every check owned by this scrutiny assignment
     passes, relevant contract prerequisites are accounted for, scrutiny evidence
     artifacts are collected, and no integrity or responsibility-drift issue
     blocks the target.
   - If required real-surface evidence is outside this assignment's method, name
     the sibling validator that owns it. Do not claim scrutiny alone proves the
     full target.

## Failure Severity

Blocking findings include:

- Missing implementation for a target.
- Required command failure tied to the target.
- Unmet prerequisite from `Needs`.
- Missing required evidence owned by this validation task.
- Evidence-sensitive changes that invalidate proof.
- Security, data-integrity, migration, compatibility, or public API break.
- Required oracle/source baseline missing.
- Unverifiable assertion.

Prefer `passed=false` with concrete limitations over speculative passes.

## Report

```markdown
## Hard-Gate Commands
- `<command>` -> exit <code>. <focused output>

## Per-Target Verdicts
### <target-id>: passed | failed
- Needs: <checked prerequisites, unmet dependencies, or None>
- Evidence: <commands/source review/artifacts collected and any named sibling evidence owner>
- Review: <why contract is or is not satisfied>

## Evidence Integrity
- <changed proof surfaces, missing evidence, and credibility impact>

## Responsibility Drift
- <none or concrete drift>

## Review Limitations
- <unreviewed areas and why>

## Guidance Suggestions
- <optional skill/AGENTS.md/setup guidance for orchestrator>
```

Call `end_node` with one item per assigned target, then exit immediately.
