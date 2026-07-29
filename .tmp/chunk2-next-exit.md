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

The authority audits added three more RED regressions:

- a terminal mission still accepted plan, team, and mission-skill mutations;
- failed terminal conversation cleanup changed the filesystem but remained
  absent from `Next` and had no durable completion fact;
- a direct team revision could consume a pending joint proposal's team before
  approval, leaving the approved proposal permanently unpromotable.

After the cutover:

- every nonterminal state exposes at least one exact effect or choice;
- every nonterminal state exposes `Abort`;
- active effects expose their exact control choices;
- inflight requests expose exact neutral `ResolveEffect` intents, while fresh
  role and oracle dispatches execute only the request identities materialized
  from the current projection and later drivers recover them without replay;
- settled conversation scratch remains an exact `CleanupConversation` intent
  until its completion fact folds;
- plan proposals, team revisions, mission skills, and environment assignments
  are advertised only as legal administrative choices, and terminal missions
  reject them without appending;
- proposals, failures, messages, finish, and apply expose exact targets and
  parameters;
- green proof offers `Finish` without appending `MissionFinished`;
- stale proof removes `Finish` and returns the required proof effect;
- a queued continuation outranks finish, and a forged `MissionFinished` is
  inert;
- forged apply targets and manual controls absent from the exact current
  `Choice` set are inert during replay;
- every event-log prefix refolds to the same `Next`.

## Result

- `next(&MissionState) -> Next` is the only workflow projection.
- `Next` contains typed `EffectIntent` and exact `Choice` values. The pure model
  retains the existing typed role and oracle dispatch payloads.
- The driver has no private owned-effect set or second execute-versus-recover
  state machine. A fresh dispatch carries its exact materialized effect IDs
  through the same driver action. Any inflight request projects as neutral
  `ResolveEffect` in live views; a later driver encountering it recovers it
  without replay.
- Disposable conversation scratch is owned by the exact folded role attempt.
  `Next` projects cleanup before terminal apply or later work, and
  `ConversationResourcesCleaned` is the sole fact that retires the obligation.
- The engine executes only returned effects. Every workflow-changing command
  validates its exact current choice before recording an event.
- Administrative events are independently admitted by the fold only when the
  same exact choice is current, so raw or stale appended events are inert.
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
- One `Choice::authorizes_control` predicate is shared by command admission and
  replay admission for manual controls. Engine-owned deadline and automatic
  continuation facts retain their existing policy checks.
- Human guidance renders the exact mission, decision target, action, effect,
  recipient, mode, and required justification or feedback argument accepted by
  each CLI route.
- Role prompt reconstruction folds once at the durable request's recorded
  message boundary and validates the exact intent tuple.
- Role setup and the composed driver future use ordinary boxed async
  boundaries. Removing those boundaries reproduced a stack overflow in
  `production_retained_state_limit_preserves_turn_and_refuses_handoff`; the
  production regression passes on the default Tokio worker stack without
  changing policy or resource authority.

## Versions and Contracts

- `SCHEMA_VERSION`: `33 -> 34` for exact conversation cleanup completion facts.
- `REDUCER_VERSION`: `66 -> 69` across the original workflow replacement,
  administrative/cleanup authority hardening, and pending-proposal isolation.
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
  exact current choice, including administrative mutations and finish when
  other work is owed.
- Terminal cleanup is fail-closed and retry-visible. Removing scratch without
  recording its exact completion leaves the same cleanup intent projected;
  no role or oracle is rerun to repair it.
- Secrets, egress, confinement, capability ceilings, runtime authentication,
  and writable-role authority are unchanged.
- The async boxing changes future layout only; it grants no new authority and
  alters no runtime policy.

## Coverage Parity

No test file or prior test coverage was deleted. Existing scenario tests were
ported from phase, attention, and action-list assertions to exact `Next`
effects and choices. New regressions cover universal abort, explicit finish,
unadvertised transition rejection, queued-work finish rejection, prefix-stable
`Next`, exact CLI serialization, exact apply replay admission, terminal control
rejection, complete human decision commands, pending joint-proposal isolation,
and live-driver `ResolveEffect` projection.

Two adversarial QA rounds followed the initial implementation. The first found
and fixed exact apply admission, incomplete human choice rendering, terminal
administrative bypasses, and hidden cleanup/effect ownership. The second
rechecked replay, recovery, concurrency, terminal cleanup, JSON parity, command
completeness, and default-stack execution. It found and fixed the missing
reducer-side manual control guard, role-cleanup-before-policy-park ordering,
default-stack future growth, guidance ordering, and pure-model fixtures. It
also removed a duplicated task-acceptance predicate and a no-op oracle cleanup
call.

A follow-up adversarial rereview found and fixed the joint-proposal
administrative interleaving and the misleading recovery-only name for live
inflight work. The fix uses one shared pending-proposal predicate and one
neutral effect intent; it adds no phase, lease, owner set, or compatibility
state machine.

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
