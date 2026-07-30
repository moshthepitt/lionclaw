# Chunk 4 Generic Methods and Everyday Driver Exit Note

Worktree: `.worktrees/lionclaw2-simple-chunk-4-everyday-run`

Branch: `lionclaw2-simple-chunk-4-everyday-run`

Accepted base: `fbaa4d4634c19a20c5d2189f563797e5ed74a5a5`

Reviewed Chunk 4 head: `8dee5d06ab934fbe198826b4dc2ecefcc60e4179`

Final Chunk 4 implementation commits:

- `209e1c745728d8a3a3bed10d9c43cda575ec693d`
- `4533d8f01e8cf4ed52ee2c4dc8a05dad5e4fe3ab`

`git log --show-signature` reports a good signature from
`Kelvin Jayanoris <kelvin@jayanoris.com>` using key
`11001593BC0EB11379D7725896EDA40C1DFDD88A`.

## Review RED and Fixes

Focused regressions captured the five final review failures before their
implementation:

- a report task's accepted bytes and digest were absent from the judgment
  request, so an independently rediscovered fact could hide a garbage report;
- an unapplied terminal result remained the permanent everyday binding;
- the attached everyday runtime did not drive the complete projected bridge
  lifecycle;
- a task-bound report producer received the complete-planning prompt;
- plan validation rejected every two-dependency read-only synthesis task,
  without examining the actual artifact candidates.

The final implementation:

- adds `ReportEvidenceRef { task_id, effect_id, report_sha256 }`, derives the
  exact current report set in plan order, validates it at fold admission,
  reconstructs the accepted report bytes from the durable receipt/blob store,
  and presents each report to the judge as an explicitly untrusted deliverable;
- binds those report identities into proof freshness and identical-proof retry
  identity, so changing a report at the same artifact head invalidates the old
  judgment;
- routes assigned report tasks through the normal execution context while
  retaining planning context for taskless report/planning turns;
- accepts compatible report fan-in and rejects only distinct dependency commit
  candidates, before launching the read-only synthesizer;
- adds `lionclaw run [runtime] --new`, which releases only a terminal launcher
  binding, preserves the old mission and its legal Apply choice, and excludes
  all pre-existing terminal results from automatic reselection;
- adds an attached-runtime acceptance scenario that starts from `lionclaw run`,
  invokes the projected Node client through the authenticated Unix bridge,
  creates and proposes a mission, approves only an advertised choice, advances
  work, survives a simulated signal loss, resumes current event-log truth,
  finishes, reads the report, and applies the result.

The bad-report scenario proves the exact garbage report reaches the judge and
no Finish choice is advertised after rejection. The report synthesis scenarios
prove two read-only dependencies at one commit are legal and two distinct
artifact candidates fail with `task.divergent_read_only_lineage` before the
report runner is called.

Authenticated Hermes continuity testing then exposed one ACP protocol defect:
the first-class `session/set_mode` success response is legally empty, but the
client treated the pre-set `default` observation as post-set evidence and
rejected `dont_ask`. A focused RED regression reproduced the exact failure.
The ACP client now records a successful empty first-class setter as
`Acknowledged`, clears only the superseded pre-set observation before the RPC,
retains an intervening runtime notification as `Observed`, and continues to
reject explicit mismatches and post-configuration drift.

## Determinism and Honesty

`Next` remains the only workflow projection and exact advertised choices remain
the only decision authority. `--new` changes launcher selection only; it adds no
mission phase, event, choice, durable retry counter, or second state machine.
The old terminal mission and unapplied result remain durable and addressable by
mission id.

Judgment report identity is event-log data, not prompt-only provenance. The fold
re-derives current report references, forged request digests fold inert, prompt
reconstruction verifies the same references and bytes, and changed report
content stales prior judgment proof even when the artifact head is unchanged.
Report text remains untrusted and grants no authority.

The bridge lifecycle test retains production parsing, command allowlisting,
exact choice validation, store, fold, engine, workspace, and receipt behavior.
Only process/runtime transports are injected. Production continues to launch
the host executable behind the private authenticated socket.

## Versions and Contracts

- `SCHEMA_VERSION`: `35 -> 36`.
- `REDUCER_VERSION`: `71 -> 72`.
- `MissionEvent::RoleTurnRequested`, `RoleTurnProvenance`,
  `InflightEffect::RoleTurn`, `RoleDispatchIntent`, and `RoleTurnRequest` now
  carry the exact report evidence references.
- Task-bound `ProducesReport` requests now use `RolePromptTemplate::Execution`;
  taskless report/planning requests retain `RolePromptTemplate::Planning`.
- `lionclaw run` adds the public `--new` option.
- First-class ACP model/mode setters may produce the existing
  `RuntimeConfigurationConfirmation::Acknowledged` evidence when the successful
  protocol response carries no current-value observation. Echoed selections
  and runtime notifications remain `Observed`.
- No raw HTTP, new mission state, alternative decision projection, or
  compatibility alias was added.

The schema change is intentional pre-launch incompatibility. Old event-schema
logs fail closed. Fold/snapshot/resume coverage was rerun at reducer 72. During
the final adversarial review, one snapshot-forgery test was found to still
write reducer 71; it was corrected to use `REDUCER_VERSION`, restoring the
intended attack path.

The ACP confirmation correction uses the existing event shape and enum, so it
requires no additional schema or reducer bump.

## Security Impact

- Policy and confinement are unchanged in production. Report roles remain
  read-only, artifact capture remains absent, and divergent artifact lineage is
  rejected before runtime launch.
- Report payloads are bounded by the existing per-report blob limit and an
  aggregate judgment prompt limit. Digests are recomputed from durable payload
  references before use.
- No secrets, egress, install, device, or write grants were broadened.
- Runtime profile validation, auth preparation, confinement compilation,
  pinned image resolution, and native session persistence remain on the
  existing production paths.
- ACP first-class selection acknowledgements are accepted only after the
  requested id was uniquely advertised and the typed setter RPC succeeded.
  Explicit returned mismatches and later observed drift still fail closed.
- `--new` cannot release a live mission and cannot destroy, abort, apply, or
  otherwise mutate the old mission.
- The projected bridge retains token authentication, validated argv, no shell
  execution, repository pinning, one active request, and bounded cleanup.
  Its in-process backend is enabled only by explicit integration-test
  transports.

## Verification

Passed from the final worktree after the last source change:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
- `bash ./scripts/ci.sh`
  - workspace check passed;
  - Clippy passed for all targets/features with warnings denied;
  - rustdoc passed with warnings denied;
  - all runnable workspace tests passed;
  - all nine Podman mission self-tests passed.
- `git diff --check`

The final authenticated native-session continuity runs passed from clean,
signed source commit `4533d8f01e8cf4ed52ee2c4dc8a05dad5e4fe3ab`
against image identity
`9ed8b30de84295193f92baf66e88e6f28bd4f0f4aa77d1448e7f22220bb3a126`:

- Codex: `1 passed; 0 failed`, 26.17 seconds. Preserved root:
  `/tmp/lionclaw-chunk4-live-final-codex-20260730-204737`
- OpenCode: `1 passed; 0 failed`, 14.53 seconds. Preserved root:
  `/tmp/lionclaw-chunk4-live-final-opencode-20260730-204737`
- Hermes: `1 passed; 0 failed`, 24.12 seconds. Preserved root:
  `/tmp/lionclaw-chunk4-live-final-hermes-20260730-204737`

Receipt SHA-256 digests, in the same runtime order, are
`d92cf10b5b0892a82b0e742a36b1abe95786d4623b63f604aab724f95140e290`,
`8c8c1151b615880b895724fd6bfeaffeeddcc98410dee180c64ed76e925faa1c`,
and `a62b055bb415971f3eb0b30d43a4a489bdfc9e72b9890b3990de0a38ee249403`.

Each `lionclaw.runtime-continuity-proof.v2` receipt records:

- first observation `Reconstructed` and second observation `Resumed`;
- distinct first and second effect ids with exact effect cleanup;
- retained native home and runtime state;
- removal of the declared credential projection after cleanup;
- the same executing test-binary and proof-harness digests;
- a second-response digest equal to the hidden token digest.

The original failing Hermes root is preserved at
`/tmp/lionclaw-chunk4-live-hermes-20260730-203754`. It contains the pre-fix
typed `acp.runtime` failure and no passing receipt; it is retained as RED
evidence rather than represented as acceptance.

Earlier Chunk 4 acceptance also passed:

- generated root help, `lionclaw run --help`, and `lionclaw man`
- isolated clean install probe:
  `design optimization research review software-dev`, with no
  `metric-driven` alias
- tracked-product prohibited-integration grep: clean
- CodeIntel diagnostics across every changed Rust file; only inactive-cfg
  hints and the known macro-input `None` false positives were reported, while
  Cargo and Clippy reported no warning.

Focused final regressions passed:

- `cargo test -p lionclaw --test read_only_tasks`
- `cargo test -p lionclaw --test parallel_writers`
- `cargo test -p lionclaw --test everyday_run`
- `cargo test -p lionclaw --test store_hardening`
- `cargo test -p lionclaw --test fold_litmus`
- `cargo test -p lionclaw --test resume`
- `cargo test -p lionclaw-model --lib`

The authenticated real-Codex matrix passed with one fresh run per scenario:

- fix bug: `verified`, head changed;
- agent-authored planning, explicit approval, execution: `verified`, head
  changed;
- software-dev: `verified`, head changed;
- optimization: `attested`, head changed;
- research: `attested`, head changed;
- review: `attested`, head unchanged;
- design: `attested`, head changed.

The review method result directly exercised the changed task-bound report and
judgment prompts through real Codex while preserving the read-only product
head. The deterministic `lionclaw run` bridge test separately proves the whole
attached launcher, crash/recovery, report, and apply path.

## Residual Coverage

The three authenticated native-session continuity tests remain ignored by
default because they require explicit preserved roots, corresponding real auth,
network, and the OCI image. All three were explicitly run and passed as
recorded above. No authenticated continuity test remains blocked.

No work was pushed, merged, rebased, submitted for review, or ported to
`main`.
