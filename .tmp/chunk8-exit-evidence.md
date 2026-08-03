# Chunk 8 Honest End-to-End Acceptance Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-8-acceptance`

Branch: `lionclaw2-simple-chunk-8-acceptance`

Accepted signed base: `a909832e01734af9eeee40ed4a1d04720fd2983e`

Accepted implementation commit: `91978fd77af0ddc5d4718e4e8071a9296b6aff88`

The branch was created directly from the exact `lionclaw2` head above. Neither the
planning-material worktree `.worktrees/lionclaw2` nor `main` was used as an
implementation base.

## Result

Chunk 8 acceptance passed. The matrix exercised real mission orchestration,
command proof, independent judgment, gap review, repair, crash/replay behavior,
destination-scoped authority, child outcomes, and authenticated native runtime
continuity. Product changes are limited to the acceptance harness and its
development image.

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

Passed on implementation commit `91978fd77af0ddc5d4718e4e8071a9296b6aff88`:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
- `bash ./scripts/ci.sh`
  - workspace check passed;
  - Clippy passed for all targets/features with warnings denied;
  - rustdoc passed with warnings denied;
  - every runnable workspace test passed;
  - all nine named Podman mission self-tests passed.
- `git diff --check`
- `bash -n scripts/mission-eval.sh`
- Rust CodeIntel diagnostics for changed `selftest.rs`: zero diagnostics and
  zero warnings.
- Development image rebuild from the changed Containerfile: passed and tagged
  `localhost/lionclaw-runtime-dev:v1`.

Raw output is under `.tmp/chunk8-acceptance/logs/`, including
`cargo-fmt-check.raw.log`, `cargo-check.raw.log`, `cargo-test.raw.log`,
`scripts-ci.raw.log`, `codeintel-diagnostics.raw.json`, and
`dev-image-build.raw.log`.

## Ignored-Test Rationale

The normal workspace suite reports four source-level ignored tests:

- Real Codex, OpenCode, and Hermes native continuity require explicit fresh
  evidence roots, real credentials, network, and the OCI image. Credentials
  were available, so all three were run explicitly and passed as recorded
  above; they were not left unverified.
- `production_external_driver_uses_kernel_broker_without_container_credentials`
  requires delegated CPU and memory cgroup controllers. `scripts/ci.sh`
  detected both controllers unavailable and skipped this one scenario. The ten
  non-OCI external-oracle lifecycle, admission, identity, digest, and crash
  scenarios all passed. No test was skipped because a normal host directory
  was read-only.

## Product Coupling Scan

The case-insensitive scan covered `crates/*/src/**/*.rs` excluding only the
explicit diagnostic implementation `crates/lionclaw/src/selftest.rs`. It looked
for evaluator variables, fixture names, generic method assertion ids, and
benchmark/eval adapters. Result: zero matches. The self-test intentionally
embeds its real diagnostic fixtures; no engine, fold, model, runtime, policy,
or production dispatch branch knows the benchmark/evaluator cases. Evidence:
`.tmp/chunk8-acceptance/logs/product-coupling-scan.raw.log`.

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

## API, Event, Schema, And Reducer Impact

No public CLI/API, durable event payload, schema, fold, reducer, SQL shape, or
mission-type contract changed. The evaluator correction only emits the already
required tagged `TaskAssignment` JSON shape. Existing logs replay exactly as
before; schema and reducer versions are unchanged.

## Residual Risks

- The production external-driver OCI integration could not run on this host
  because delegated CPU/memory cgroup controllers are absent. This is a host
  prerequisite, not a credential or read-only-directory failure.
- Generic agentic missions are model-dependent. One optimization run exposed
  real semantic and metric gaps and required explicit repairs before passing;
  the retained failure reports are part of the acceptance evidence rather than
  hidden as flakiness.
- Preserved `/tmp` roots are local audit artifacts and may be removed only after
  their committed reports/receipts are no longer needed.

## Repository Integrity

Implementation commit `91978fd77af0ddc5d4718e4e8071a9296b6aff88` is a direct
child of accepted base `a909832e01734af9eeee40ed4a1d04720fd2983e` and has a
good signature from `Kelvin Jayanoris <kelvin@jayanoris.com>` using RSA key
`11001593BC0EB11379D7725896EDA40C1DFDD88A`.

No work was pushed, merged, rebased, submitted for review, ported to `main`, or
used to mutate `main`.
