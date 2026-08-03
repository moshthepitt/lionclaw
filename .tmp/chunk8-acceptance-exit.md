# Chunk 8 Honest End-to-End Acceptance Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-8-acceptance`

Branch: `lionclaw2-simple-chunk-8-acceptance`

Accepted signed base: `a909832e01734af9eeee40ed4a1d04720fd2983e`

Accepted product HEAD: `9d0e12c1a722249dc065f638987f91c9cc9644b2`

The branch was created directly from the exact `lionclaw2` head above. Neither the
planning-material worktree `.worktrees/lionclaw2` nor `main` was used as an
implementation base.

## Result

Chunk 8 acceptance passed. The matrix exercised real mission orchestration,
command proof, independent judgment, gap review, repair, crash/replay behavior,
destination-scoped authority, child outcomes, and authenticated native runtime
continuity. Product changes are limited to the acceptance harness, its
development image, and generic everyday OCI lifecycle fixes. Each execution has
a stable resource owner derived from its LionClaw runtime-state root and runtime
profile. Under the repository driver lock, relaunch removes only that exact
owner's stale main container, proxy container, internal network, and egress
network before creating replacements.

An external unmodified dependent-type-checker task was also exercised through
an evaluator bridge that invoked only `lionclaw run codex`. Its candidate source
and bridge remain acceptance artifacts, not LionClaw product code.

## Implementation

- The development image installs pinned `pytest 8.4.2` and `ruff 0.12.12` with
  `uv`, then verifies both binaries while building the image.
- Mission self-test now runs Rust, Python/pytest, Python/Ruff, and JavaScript
  command oracles as direct argv without a shell. Ruff uses `--no-cache` because
  proof workspaces are intentionally read-only.
- `scripts/mission-eval.sh` can select `core` or generic `methods`, install all
  bundled method types into an isolated LionClaw home, and run software-dev,
  optimization, research, review, and design missions through normal
  plan/approve/advance/finish flow.
- Generic fixtures require the claimed language/package-manager proof and check
  whether artifact-producing methods changed `HEAD` while read-only review did
  not.
- The optimization fixture makes non-reflexive equality behavior explicit and
  immutable. This prevented a superficially green but behavior-changing set
  optimization from being accepted.
- `LIONCLAW_EVAL_KEEP=1` preserves isolated mission roots for audit without
  touching the developer's real `~/.lionclaw` state.

## RED And Honest-Recovery Evidence

- Initial development image:
  - `python3 -m pytest --version` failed because pytest was absent.
  - `ruff --version` failed because Ruff was absent.
  - Evidence: `.tmp/chunk8-acceptance/logs/pytest-red.raw.log` and
    `.tmp/chunk8-acceptance/logs/ruff-red.raw.log`.
- First dynamic Ruff proof failed in the read-only prepared-input workspace
  because Ruff attempted to initialize `.ruff_cache`. The source fix is
  `ruff check --no-cache .`; the same full self-test then passed.
  - Evidence: `.tmp/chunk8-acceptance/logs/mission-self-test-pytest-ruff.raw.log`
    and `.tmp/chunk8-acceptance/logs/mission-self-test-final.raw.log`.
- First generic mission evaluator run exposed stale string-valued
  `task_assignments`; the current typed wire shape requires
  `{"type":"role","role_instance":...}`. The corrected evaluator then ran
  software-dev Python and JavaScript missions successfully.
  - Evidence: `.tmp/chunk8-acceptance/logs/software-dev-methods.raw.log` and
    `.tmp/chunk8-acceptance/logs/software-dev-methods-final.raw.log`.
- Preserved optimization mission `m4b4532803042` did not receive a pass claim.
  Gap review found that a set-based implementation collapsed repeated
  non-reflexive values such as the same NaN object. The evaluator fixture was
  strengthened before another mission was accepted.
  - Evidence: `.tmp/chunk8-acceptance/logs/optimization-failed-status.raw.json`
    and `.tmp/chunk8-acceptance/logs/optimization-after-repair-report.raw.json`.
- Hardened optimization mission `m3b67c4032419` first failed independent
  judgment for uncounted membership/equality predicates, then failed gap review
  for 25,000,000 hidden wrapper equality predicates. Both failures parked on
  explicit operator recovery choices; neither was auto-accepted or blindly
  retried. Two explicit `repair` decisions produced fresh work and proof.
  Final state: `attested`, current SHA
  `493168e650272dc2f920ffa084300209db9d62c6`, clean gap review with zero
  blocking/major/minor gaps.
  - Evidence: `.tmp/chunk8-acceptance/logs/optimization-hardened-failure-report.raw.json`,
    `.tmp/chunk8-acceptance/logs/optimization-post-repair-report.raw.json`,
    `.tmp/chunk8-acceptance/logs/optimization-second-repair-drive.raw.log`, and
    `.tmp/chunk8-acceptance/logs/optimization-final-report.raw.json`.
- The first official external dependent-type-checker result was honestly RED:
  the mission finished with independent proofs, but the unmodified evaluator
  accepted only 169/174 valid programs and rejected 81/81 invalid programs.
  Missing behaviors were higher-universe ordinary recursors, singleton
  large-elimination, scoped polymorphic inductive self-reference, and dependent
  let transparency. Evidence:
  `.tmp/chunk8-acceptance/official-evaluator/official-red-reward.json`.
- A first repair exposed a separate conventional-packaging defect: the committed
  candidate redirected Cargo output, so `cargo build --release` produced no
  binary at `type-checker/target/release/type-checker`. A verified packaging
  repair removed that product-level redirect before semantic repair continued.
- Three ordinary LionClaw repair missions used public specification-derived
  regression tests, nonzero command proofs, and fresh gap reviews. Final mission
  `m4786ae9e5883` finished `verified` at candidate
  `5439309f8b471350d5ab5a5fa4639eb41eb6ddf9` with a clean review.
- The final unmodified evaluator accepted 174/174 valid programs, rejected
  81/81 invalid programs, and assigned score `0.3503878189040147`. Evidence:
  `.tmp/chunk8-acceptance/official-evaluator/official-final-reward.json`.
- The evaluator also exposed global Podman-name collisions between isolated
  everyday runs. RED:
  `.tmp/chunk8-acceptance/logs/official-evaluator-everyday-network-red.raw.log`.
  The generic runtime-state/profile-derived resource owner fixed the collision;
  focused and full-suite GREEN evidence is in
  `official-evaluator-everyday-network-green.raw.log` and
  `official-evaluator-everyday-suite.raw.log`.
- Post-acceptance review found that deterministic everyday names could still
  collide after a hard process termination left same-owner OCI resources behind.
  The focused RED test observed no pre-launch cleanup request. The corrected
  path clears the exact resource set under the driver lock before execution;
  focused tests and a real Podman collision/remove/recreate probe passed.
  Evidence: `.tmp/chunk8-acceptance/logs/stale-oci-cleanup-red.raw.log`,
  `.tmp/chunk8-acceptance/logs/stale-oci-cleanup-green.raw.log`, and
  `.tmp/chunk8-acceptance/logs/stale-oci-real-surface.raw.log`.

## Acceptance Matrix

1. **Rust software change with Cargo proof**: passed.
   `LIONCLAW_EVAL_SCOPE=core LIONCLAW_EVAL_KEEP=1 bash ./scripts/mission-eval.sh 1`
   fixed the interval bug and reached `verified` with a changed head; separate
   plan/approve/verified flow also passed. Preserved root:
   `/tmp/tmp.QXN1SOdPc0`. Evidence:
   `.tmp/chunk8-acceptance/logs/rust-core-missions.raw.log`.
2. **Python change with pytest and Ruff proof**: passed. The generic
   software-dev Python mission reached `verified` with a changed head, and the
   full self-test passed its direct-argv `python3 -m pytest` plus
   `ruff check --no-cache` proof. Evidence:
   `.tmp/chunk8-acceptance/logs/generic-methods-preserved.raw.log` and
   `.tmp/chunk8-acceptance/logs/mission-self-test-final.raw.log`.
3. **JavaScript package-manager change**: passed. The generic software-dev
   JavaScript mission ran the package-manager test and reached `verified` with
   a changed head. Evidence:
   `.tmp/chunk8-acceptance/logs/generic-methods-preserved.raw.log`.
4. **Optimization with baseline, experiment, and metric proof**: passed after
   honest repair. Mission `m3b67c4032419` ended `attested`; command proof
   passed at the final SHA and independent gap review was fresh and clean.
   Evidence: `.tmp/chunk8-acceptance/logs/optimization-final-report.raw.json`.
5. **Research with judged evidence**: passed. Mission `m3927671a8e93` ended
   `attested`; source chronology and uncertainty were independently judged;
   head changed from `08208d6d...` to `b3e6714a...`. Evidence:
   `.tmp/chunk8-acceptance/logs/research-final-report.raw.json`.
6. **Review with no artifact write**: passed. Mission `mf61136ae41df` ended
   `attested`; base and current SHA are both
   `b8daa9502ae7408d28166a9af487f336de63ac27`. Evidence:
   `.tmp/chunk8-acceptance/logs/review-final-report.raw.json`.
7. **Design with judged deliverable**: passed. Mission `m80e035a94bf4` ended
   `attested`; independently judged `DESIGN.md` changed head from `c237a390...`
   to `1a4543a8...`. Evidence:
   `.tmp/chunk8-acceptance/logs/design-final-report.raw.json`.
8. **External oracle interrupted/resumed without duplicate execution**: passed.
   `cargo test -p lionclaw --test external_oracles -- --nocapture` ran ten
   non-OCI scenarios, including pending polling and both crash windows: 10
   passed, 0 failed, 1 ignored. Evidence:
   `.tmp/chunk8-acceptance/logs/external-oracles.raw.log`.
9. **Destination allow and deny**: passed.
   `command_oracles_may_request_only_destination_scoped_network` exercised
   allowed destination authority plus denied over-ceiling grants. Evidence:
   `.tmp/chunk8-acceptance/logs/destination-policy.raw.log`.
10. **Parent mission with successful and failed children**: passed.
    `cargo test -p lionclaw --test child_missions -- --nocapture`: 12 passed,
    including successful report/artifact handoff, failed-child retry, explicit
    failed-child acceptance fallback, crash reconnection, and parent stop.
    Evidence: `.tmp/chunk8-acceptance/logs/child-missions.raw.log`.
11. **Runtime process crash/restart/continue**: passed. The focused everyday
    restart test resumed native conversation state and released a settled
    mission. Authenticated Codex, OpenCode, and Hermes continuity proofs also
    reconstructed then resumed the same native state across exact effect
    cleanup. Evidence: `.tmp/chunk8-acceptance/logs/runtime-restart.raw.log` and
    the three `native-*-continuity.raw.log` plus receipt JSON files.
12. **Failed proof repaired**: passed in live optimization mission
    `m3b67c4032419`; two explicit repairs were needed before fresh proof and a
    clean gap review allowed finish.
13. **Failed proof replanned**: passed in
    `revising_one_of_multiple_proof_failures_replans_with_every_receipt`.
14. **Repeated deterministic failure does not blindly retry**: passed in
    `command_retry_is_reoffered_only_for_a_changed_outcome`; the live
    optimization failures also parked for operator-owned recovery.
15. **Finish requires fresh sufficient proof**: passed in
    `environment_digest_change_stales_authoritative_proof_and_reruns_oracle`.
    Evidence for items 13-15:
    `.tmp/chunk8-acceptance/logs/proof-recovery-invariants.raw.log`.
16. **Unmodified external task through vanilla LionClaw**: passed after honest
    repair. The external bridge supplied the task instruction unchanged to
    `lionclaw run codex`, observed normal mission state, and materialized only
    the verified deliverable. Initial official score was `0.0` at 169/174 valid
    accepts and 81/81 invalid rejects. Final official score was
    `0.3503878189040147` at 174/174 and 81/81, with geometric-mean throughput
    `0.3503878189040147` times the reference. The final candidate, public task
    metadata, bridge, mission report, and failure/final rewards are archived at
    `.tmp/chunk8-acceptance/official-evaluator/final-task-and-output.tar.gz`
    (SHA-256
    `cbbf622c9d00ef3a3bcee3b706535e237f68543b5cbc9c956a75c563d8d5104a`).
    Hidden verifier corpus, reference implementation, oracle solution, build
    outputs, and bytecode are intentionally excluded.

## Authenticated Native Runtime Continuity

All available credential-backed ignored tests were run explicitly against
`localhost/lionclaw-runtime:v1`, resolved to immutable image identity
`e1541b6609e7209de4087aac38b08151ad5feb9dd901cf464e088b41b32ba05d`.
Each receipt records `Reconstructed` then `Resumed`, retained native state,
removed credential projection, and cleaned effects:

- Codex: passed in 32.83 seconds. Preserved root:
  `/tmp/lionclaw-chunk8-final-codex-20260803-1228`. Receipt SHA-256:
  `8e6b5427989a2e8541ef50f2585585ab3656f45a2ea0ac13930166f63436f035`.
- OpenCode: passed in 40.88 seconds. Preserved root:
  `/tmp/lionclaw-chunk8-final-opencode-20260803-1228`. Receipt SHA-256:
  `a6ae343e61c1d64976ae23937ba10403cc75cd58c4107d3bd0fa1695b88358a1`.
- Hermes: passed in 41.26 seconds. Preserved root:
  `/tmp/lionclaw-chunk8-final-hermes-20260803-1228`. Receipt SHA-256:
  `01a5e208e3920ea58a3b01ab2687a829c633578414a8afbd4a7f23268669a8a8`.

## Required Gates

Passed on final product HEAD `9d0e12c1a722249dc065f638987f91c9cc9644b2`:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
  - 195 unit tests passed in the main library target;
  - every runnable workspace integration, crate, and doc test passed;
  - three credential-backed native-continuity tests were ignored by the default
    suite and were separately run successfully as recorded above.
- `bash ./scripts/ci.sh`
  - workspace check passed;
  - Clippy passed for all targets/features with warnings denied;
  - rustdoc passed with warnings denied;
  - every runnable workspace test passed;
  - all nine mandatory Podman mission self-tests passed;
  - the optional external-oracle OCI scenario was skipped because delegated
    CPU/memory cgroup controllers are unavailable.
- `git diff --check`
- `bash -n scripts/mission-eval.sh`
- Rust CodeIntel diagnostics were collected for all changed Rust files.
  `selftest.rs` and `everyday_run.rs` had zero diagnostics. `everyday.rs`
  reported five `non_snake_case` warnings on Rust enum-pattern tokens named
  `None`; Cargo check and Clippy accepted the source, so these are recorded
  analyzer false positives rather than suppressed in code.
- Development image rebuild from the changed Containerfile passed and produced
  `localhost/lionclaw-runtime-dev:v1`.

Final raw output is under `.tmp/chunk8-acceptance/logs/`, including
`final-cargo-fmt-check.raw.log`, `final-cargo-check.raw.log`,
`final-cargo-test.raw.log`, `final-scripts-ci.raw.log`,
`final-git-diff-check.raw.log`, `final-mission-eval-bash-n.raw.log`,
`codeintel-diagnostics.raw.json`, `codeintel-crash-cleanup.raw.json`, and the
three `stale-oci-*.raw.log` files.

## Ignored-Test Rationale

The normal workspace suite reports four ignored tests in total:

- Real Codex, OpenCode, and Hermes native continuity require explicit fresh
  evidence roots, real credentials, network, and the OCI image. Credentials
  were available, so all three were run explicitly and passed as recorded
  above; they were not left unverified.
- CI also considers
  `production_external_driver_uses_kernel_broker_without_container_credentials`;
  it requires delegated CPU and memory cgroup controllers. `scripts/ci.sh`
  detected both controllers unavailable and skipped this one scenario. The ten
  non-OCI external-oracle lifecycle, admission, identity, digest, and crash
  scenarios all passed. No test was skipped because a normal host directory
  was read-only.

## Product Coupling Scan

The case-insensitive final scan looked for `FrontierSWE`/`Frontier-SWE` and
`Harbor` across all tracked Rust product source, bundled mission methods and
prompts, containers, scripts, generated help source, and CI workflows:
`crates/*/src/**/*.rs`, `mission-types/**`, `containers/**`, `scripts/**`, and
`.github/workflows/**`. Result: zero matches. No exclusion was used. Evidence:
`.tmp/chunk8-acceptance/logs/product-coupling-scan-final.raw.log`.

The zero-coupling rule applies to product surfaces, not acceptance evidence.
Evidence deliberately retains the literal identifiers that were queried so the
required scan remains reproducible; those evidence-only mentions are not
product integration.

## Security Impact

- No policy, secret, egress, destination, sandbox, runtime-auth, or confinement
  authority is broadened.
- New Python tools exist only in the development image used for repository
  proof. The production runtime image is unchanged.
- Python, Ruff, and JavaScript proofs use direct structured argv; no shell
  interpretation was added.
- Mission eval uses a fresh temporary root, isolated `LIONCLAW_HOME`,
  network-off fixtures unless a mission explicitly grants otherwise, and
  normal kernel-owned runtime confinement.
- Preserved reports and continuity receipts contain digests and identities, not
  credential bytes. Credential projections were removed after each native
  continuity effect.
- Destination-scoped allow/deny, read-only workspaces, network-off prepared
  inputs, and external-oracle broker boundaries all passed their real checks.
- Crash recovery removes only names derived from the locked repository runtime
  root and authenticated runtime profile. It neither lists nor deletes unrelated
  operator resources.

## API, Event, Schema, And Reducer Impact

No public CLI, HTTP API, durable event payload, schema, fold, reducer, SQL shape,
or mission-type contract changed. The internal `AttachedRuntimeExecutor` seam
now requires a stale-resource cleanup operation before launch; all in-tree
implementations were migrated. Everyday OCI resource names remain internal
process-boundary identifiers derived from existing runtime state and profile
identity. Existing logs replay exactly as before; schema and reducer versions
are unchanged.

## Residual Risks

- The production external-driver OCI integration could not run on this host
  because delegated CPU/memory cgroup controllers are absent. This is a host
  prerequisite, not a credential or read-only-directory failure.
- Generic agentic missions are model-dependent. One optimization run exposed
  real semantic and metric gaps and required explicit repairs before passing;
  the retained failure reports are part of the acceptance evidence rather than
  hidden as flakiness.
- The external task is correct but slower than its reference implementation:
  geometric-mean throughput is `0.3503878189040147x`. Chunk 8 required honest,
  nonzero end-to-end evidence, not benchmark-specific optimization; no product
  or candidate special case was added to chase score.
- Preserved `/tmp` roots are local audit artifacts and may be removed only after
  their committed reports/receipts are no longer needed.

## Repository Integrity

Five signed product/evidence commits follow accepted base
`a909832e01734af9eeee40ed4a1d04720fd2983e` directly and linearly:

- `91978fd77af0ddc5d4718e4e8071a9296b6aff88` —
  `test(acceptance): harden mission matrix`
- `7c672f94781342cb8b1e965d02e3634805ec2a27` —
  `docs(acceptance): record Chunk 8 evidence`
- `68554233b3f4c7c87ca0e4127e5cac6a0206f7a4` —
  `fix(runtime): name everyday OCI resources`
- `a62fe51d9c753663fef6ea444d8aedce416905a0` —
  `docs(acceptance): finalize Chunk 8 evidence`
- `9d0e12c1a722249dc065f638987f91c9cc9644b2` —
  `fix(runtime): recover stale everyday resources`

All five have good signatures from
`Kelvin Jayanoris <kelvin@jayanoris.com>` using RSA key
`11001593BC0EB11379D7725896EDA40C1DFDD88A`. The subsequent evidence-only
correction is checked after creation because a commit cannot contain its own
hash.

No work was pushed, merged, rebased, submitted for review, ported to `main`, or
used to mutate `main`.
