# Chunk 3 Dynamic Oracles Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-3-dynamic-oracles`

Branch: `lionclaw2-simple-chunk-3-dynamic-oracles`

Accepted base: `9adf98fe090e646f8076c54d445a2eb807dd6576`

## RED and GREEN

The replacement began with focused RED coverage for the new mission-local
oracle contract:

- a joint proposal could not introduce an oracle and reference it from its
  proposed plan;
- oracle replacement had no plan-revision guard;
- oracle behavior and authority changes were not one proof identity;
- forged old-spec requests and outcomes were not rejected by one canonical
  identity;
- gap review still had its own acceptance, waiver, state, and workflow paths;
- static mission-type programs and Cargo-specific inputs still owned oracle
  execution.

Integration RED runs then found synthetic fixtures that bypassed the new
request and proposal contracts: deadline tests reused an oracle timeout above
their mission ceiling, old approval events omitted accepted runtime identities,
and resume tests forged request deadlines independently of the spec. Those
fixtures were ported to the production constructors and validation rules. No
production validation was relaxed.

After the replacement:

- one current `BTreeMap<OracleName, OracleSpec>` is folded into mission state;
- plan, team, and the prospective complete oracle map validate together;
- `None` leaves oracles unchanged and `Some(map)` is complete replacement;
- an oracle change requires a `PlanProposal`, so its existing `base_revision`
  rejects stale replacement without an `oracle_revision`;
- one decision event promotes plan, team, and oracles atomically or not at all;
- one canonical spec digest binds dispatch, effect ID, request, inflight state,
  outcome, authoritative receipt, freshness, retry identity, status, and report;
- structured argv executes directly through the existing confined OCI runner;
- shell syntax remains literal argument data;
- gap review is an ordinary taskless review-role attempt scheduled by `next`,
  with ordinary retry, repair, and revise recovery and no accept or waiver;
- every tested event prefix, snapshot, and cursor rebuild reproduces the same
  `next`.

## Result

`OracleSpec::Command` carries bounded structured argv, a clean
workspace-relative working directory, bounded environment, a timeout, grants,
and resources. Validation is pure and fail-closed against the immutable mission
authority and resource ceilings. Command oracles cannot receive secrets,
network, install, or write authority.

The runner resolves the current spec from folded mission state, recomputes its
digest, maps its working directory into the judged checkout, and compiles it
through the shared role authority boundary. The judged checkout remains
read-only. No shell wrapper, copied bundle executable, static oracle lookup, or
mission-type dispatch remains.

Static oracle discovery, named resource and device maps, bundled Cargo oracle
programs, the Cargo-home input, and Cargo-specific oracle environment setup are
deleted. Mission planning guidance now owns the complete dynamic oracle map.

Gap review retains its role, handoff, and durable `RoleAttemptReceipt`, but no
longer has a parallel workflow model. Blocking or failed review offers retry,
repair, or revise. Repair reopens the existing deliverable sink with the
review's bounded evidence.

## Versions and Contracts

- `SCHEMA_VERSION`: `34 -> 35` for dynamic oracle specs, spec-bound request and
  outcome events, and atomic proposal decision identities.
- `REDUCER_VERSION`: `69 -> 70` for the replacement fold and workflow rules.
- `MissionProposal` adds optional complete oracle replacement.
- `MissionState` owns the current oracle map and digest-scoped attempt counters.
- Oracle request, completion, verdict, and receipt shapes carry `spec_digest`.
- Approval records carry the exact proposed runtime identities needed for
  atomic team promotion.
- CLI JSON status and reports expose current specs and digests plus active,
  inflight, and authoritative digest identity.
- Static mission-type oracle declarations and paths are removed, with no
  compatibility adapter or second execution route.

## Security Impact

- The proof moat remains deny-by-default: judged source is read-only and command
  oracles receive no secrets, egress, install, or write authority.
- All grants and confinement resources validate against immutable mission
  ceilings before any event is appended.
- Working directories are syntactically clean, must exist as directories in the
  judged checkout, and are compiled through the shared judged-root moat.
- Environment names and values use the existing reserved-coordinate validation
  and explicit aggregate bounds.
- Timeouts are nonzero and bounded by the mission execution ceiling.
- The runner directly invokes the declared executable and argv. It never builds
  a shell command line.
- Spec or runtime, artifact, environment, or authority changes make retained
  proof stale. Forged old-spec events fold inert.
- `lionclaw-model` remains pure and gains no I/O, clock, randomness, async
  runtime, or model dependency.

## API and Event Contract Impact

This is an intentional event and JSON contract change, covered by the schema
and reducer version bumps. Dynamic oracles replace static mission-type oracle
configuration. Proposal approval is now one atomic event boundary. Oracle
receipts and operator views expose the canonical spec digest. Gap review uses
the existing role-attempt event and receipt contracts instead of special review
state or acceptance events.

## Coverage Parity

Existing scenario tests were ported rather than removed. New regression
coverage includes joint introduction, stale replacement, complete-map
semantics, digest coverage for argv/environment/cwd/grants/resources/runtime
and artifact identity, forged request and outcome rejection, digest-scoped
retry counters, invalid authority and environment rejection before append,
literal shell syntax, atomic proposal promotion, team-cutover freshness, gap
review recovery, and prefix/snapshot determinism.

The production self-test adds real confined Rust, Python, and JavaScript
commands, including nested working directories and a literal shell-syntax
argument. All execute through the OCI runner rather than a mock.

## Verification

Passed from the worktree root after the final code change:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
  - all runnable tests passed;
  - the two existing real-auth/network runtime continuity tests remained
    explicitly ignored.
- `bash ./scripts/ci.sh`
  - formatting, check, `clippy -D warnings`, and `rustdoc -D warnings` passed;
  - the complete workspace test suite passed;
  - all nine Podman mission self-tests passed, including the dynamic
    Rust/Python/JavaScript no-shell test.
- `bash -n scripts/mission-eval.sh`
- `git diff --check`
- static search found no retired static-oracle or special gap-review symbols.
- CodeIntel diagnostics used
  `/home/mosh/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rust-analyzer`
  across every changed Rust file. It reported only inactive-cfg hints and the
  known macro-input `None` false positive; Cargo and clippy reported no warning.

## Review and Scope

The implementation is based directly on accepted Chunk 2 head
`9adf98fe090e646f8076c54d445a2eb807dd6576`. It is a replacement: there is one
mission-local oracle model, one confined command compiler, one proposal
promotion boundary, and one ordinary role-proof path for gap review. No branch
was pushed or merged.
