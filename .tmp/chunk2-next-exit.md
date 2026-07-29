# Chunk 2 Next Projection Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-2-clean`

Branch: `lionclaw2-simple-chunk-2-clean`

Accepted base: `8bdcaec73aca958bbc93ff69b1d2d9091161b7c8`

## RED and GREEN

The first architecture regression serialized a proposal checkpoint and failed
before the cutover:

```text
left: ["choices", "effects", "issues", "state"]
right: ["choices", "effects"]
```

After the cutover:

- every nonterminal state exposes at least one exact effect or choice;
- every nonterminal state exposes `Abort`;
- active effects expose their exact control choices;
- proposals, failures, messages, finish, and apply expose exact targets and
  parameters;
- green proof offers `Finish` without appending `MissionFinished`;
- stale proof removes `Finish` and returns the required proof effect;
- a queued continuation outranks finish, and a forged `MissionFinished` is
  inert;
- every event-log prefix refolds to the same `Next`.

## Result

- `next(&MissionState) -> Next` is the only workflow projection.
- `Next` contains typed `EffectIntent` and exact `Choice` values. The pure model
  retains the existing typed role and oracle dispatch payloads.
- The engine executes only returned effects. Every workflow-changing command
  validates its exact current choice before recording an event.
- `status --json`, `guide`, reports, inbox output, and human command guidance
  serialize or render the same `Next`.
- Nonterminal `MissionPhase`, `StepDecision`, `MissionDisposition`,
  `AttentionKind`, stored attention, allowed-action lists, per-conversation
  legal-action lists, finish-readiness exports, and hand-written CLI
  `next_actions` are deleted.
- Terminal state is the fact derived from `MissionFinished` or
  `MissionAborted`. Completed results expose one legal `Apply` choice until the
  exact `ResultApplied` fact is recorded.
- Finish authority is not duplicated in another helper: the fold admits
  `MissionFinished` only when the exact current `Next` contains the matching
  `Finish`.
- Role prompt reconstruction folds once at the durable request's recorded
  message boundary and validates the exact intent tuple.
- Role setup is split across two ordinary boxed async boundaries. Reverting
  that split reproduced a stack overflow in
  `production_retained_state_limit_preserves_turn_and_refuses_handoff`; the
  structural split passes the production regression without changing policy or
  resource authority.

## Versions and Contracts

- `SCHEMA_VERSION`: remains `33`; no event payload changed.
- `REDUCER_VERSION`: `66 -> 67`.
- Folded state replaces nonterminal `phase` and stored `open_attention` with an
  optional `terminal` fact and optional `applied_result`.
- CLI JSON removes `phase`, `disposition`, attention/issues, `next_actions`,
  and conversation `legal_actions`; it adds the exact `next` projection.
- The pre-launch reducer-version boundary replays old snapshots. No parallel
  compatibility workflow was added.

## Security Impact

- The determinism wall is unchanged: `lionclaw-model` gained no I/O, clock,
  randomness, async runtime, or model dependency.
- Workflow admission is stricter: commands and folded decisions require an
  exact current choice, including finish when other work is owed.
- Secrets, egress, confinement, capability ceilings, runtime authentication,
  and writable-role authority are unchanged.
- The role-runner async split changes future construction only; it grants no
  new authority and alters no runtime policy.

## Coverage Parity

No test file or prior test coverage was deleted. Existing scenario tests were
ported from phase, attention, and action-list assertions to exact `Next`
effects and choices. New regressions cover universal abort, explicit finish,
unadvertised transition rejection, queued-work finish rejection, prefix-stable
`Next`, and exact CLI serialization.

## Verification

Passed from the worktree root after the final code change:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
  - all runnable tests passed;
  - the two existing real-auth/network runtime continuity tests remained
    explicitly ignored.
- `bash ./scripts/ci.sh`
  - clippy passed with warnings denied;
  - rustdoc passed with warnings denied;
  - the full workspace test suite passed;
  - all eight Podman mission self-tests passed.
- `git diff HEAD --check`
- CodeIntel Rust inspection with
  `/home/mosh/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rust-analyzer`
  found the public `next` definition and its engine, fold, CLI, and test
  references with no backend warning.

## Review and Scope

The abandoned Chunk 2 worktree was used only as a mechanical, uncommitted
starting point. The implementation was reduced to the requested root design:
one pure workflow function, one exact command authorization surface, and no
benchmark or later-chunk behavior. No branch was pushed or merged.
