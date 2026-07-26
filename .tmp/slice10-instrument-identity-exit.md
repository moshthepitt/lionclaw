# Slice 10 Instrument Identity Exit Note

Worktree: `.worktrees/lionclaw2-slice10-instrument-identity`

Base: `28d7612e9e1474be0c94433685d1a61b39ba4e73`

Branch: `lionclaw2-slice10-instrument-identity`

## Commits

- `6cf1e2e754618c058cbfa946972669fab765deb1`:
  `Bind judged receipts to role instrument identity`
  - Signature: GPG good signature from
    `Kelvin Jayanoris <kelvin@jayanoris.com>` using RSA key
    `11001593BC0EB11379D7725896EDA40C1DFDD88A`.
  - Changed: added role/runtime/skill instrument identity to judged role
    request provenance, folded it through receipts and gap-review acceptances,
    made role freshness compare one `RoleProofFreshness`, recorded runtime
    identities on team revisions, persisted skill digests in mission config,
    and added model-change plus selective SkillAdded regressions.
- Final bookkeeping commit:
  - Changed: this exit note only.
  - Signature: the commit object carrying this file is GPG-signed. Its own hash
    cannot be embedded in this file without rewriting history; verify with
    `git log --show-signature -1`.

## Schema and Reducer Versions

- `SCHEMA_VERSION`: `31 -> 32`.
  - Needed because the event/state wire shape changed:
    `MissionConfig` now carries the mission type skill digest catalog,
    `TeamConfigured` carries resolved per-role runtime/model/mode identity, and
    `RoleTurnRequested` carries the resolved judging role instrument identity.
- `REDUCER_VERSION`: `59 -> 60`.
  - Needed because fold freshness semantics changed for judged role receipts
    and gap-review acceptances: they now require matching deliverable head,
    environment digest, and role instrument identity.

## Instrument Scope

Included for judged role receipts:

- `judged_sha`: the deliverable head judged by the role receipt.
- `environment_digest`: the resolved immutable runtime image digest.
- `role_instance`: selects the role whose current instrument must match.
- `role_digest`: deterministic hash of role fields that affect judgment or
  runtime authority: output contract, instructions, role environment, grants,
  tmpfs resources, and deadline.
- `runtime`: the resolved runtime profile identity, including runtime name,
  requested model, and requested mode, recorded before dispatch on
  `TeamConfigured`.
- `skills`: sorted resolved skill name and digest set for the role turn,
  drawing from mission config and mission-local `SkillAdded` facts.

Excluded:

- Oracle-specific instrument identity fields. I verified the model contract
  around `MissionTypeRef`: the mission type is pinned by content digest and
  re-verified on engine open, and existing identity tests cover digest changes.
  Since oracle definitions are part of that pinned mission type, an
  `AuthoritativeVerdict` carrying `oracle`, `judged_sha`, and
  `environment_digest` is already instrument-bound for the oracle lane.
- Doctrine-floor version. I found no mid-mission doctrine-floor mutation vector;
  binary/schema/reducer wire breaks already prevent spanning this change.
- Prompt/template fields. They remain recorded on `RoleTurnRequested` and in
  effect provenance, but prompt/template is derived from role output and prompt
  hash at dispatch rather than a separate live mutation vector.
- Runtime configuration evidence and runtime usage. Those are observed after
  execution and cannot stale a proof because they are not bound at dispatch.
- Team assignment maps, judgment panels, plan guidance, and role purpose.
  Panel membership and plan revision are validated separately; these are not
  the judging instrument identity.

## Implementation Notes

- The single judged freshness path is `RoleProofFreshness::is_fresh_at`.
  `RoleTurnProvenance::is_fresh_at`, advisory receipt projection, and
  `ReviewAcceptance::is_fresh_at` delegate to that path.
- `AuthoritativeVerdict::is_fresh_at` intentionally remains SHA plus
  environment only. This avoids schema bloat in the oracle lane where the
  mission type digest already seals oracle definitions.
- `SkillAdded` updates the mission-local skill fact by name. Receipts for roles
  using that skill stale because their stored skill digest no longer matches;
  roles that do not use that skill stay fresh.
- Runtime/model changes are represented by a new `TeamConfigured` event with
  new resolved runtime identities. Receipts judged under the old team/runtime
  identity stale, and the engine refuses to mint new proof or finish if the
  live runtime profile no longer matches the recorded team identity.
- I used `Box<RoleInstrumentIdentity>` only inside the replay-time
  `InflightEffect::RoleTurn` enum to keep the enum size balanced under clippy.
  Public event and receipt types remain unboxed.

## Coverage Parity

Mandatory reconciliation command:

```text
git diff --name-status -M 28d7612e..HEAD -- '*/tests/' 'crates/*/src/**/*tests*.rs'
```

Full output:

```text
```

The exact required command produced no lines. To make coverage movement
auditable despite that narrow pathspec, I also ran this supplemental inventory:

```text
git diff --name-status -M 28d7612e..HEAD -- '*/tests/*' 'crates/*/src/**/*tests*.rs'
```

Supplemental output:

```text
M	crates/lionclaw-model/tests/team_cutover.rs
M	crates/lionclaw/tests/advisory_validator.rs
M	crates/lionclaw/tests/common/mod.rs
M	crates/lionclaw/tests/environment_assignment.rs
M	crates/lionclaw/tests/fold_litmus.rs
M	crates/lionclaw/tests/parallel_writers.rs
M	crates/lionclaw/tests/production_conversation_flow.rs
M	crates/lionclaw/tests/recovery.rs
M	crates/lionclaw/tests/reference_expansion.rs
M	crates/lionclaw/tests/resume.rs
M	crates/lionclaw/tests/skill_dispatch.rs
M	crates/lionclaw/tests/skill_prompts.rs
M	crates/lionclaw/tests/store_hardening.rs
M	crates/lionclaw/tests/terminal_review.rs
```

Deletion justifications: none. No test files were deleted.

## Verification

Passed from the repository root after the final code change:

- `cargo fmt -- --check`
- `cargo check --workspace`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `bash ./scripts/ci.sh`
  - Passed all eight Podman self-tests:
    `moat-refuses-over-privileged-judge`,
    `replanning-revises-atomically-and-strengthen-only`,
    `gap-review-gates-closure`,
    `writable-worker-writes-land-and-resume-no-dup`,
    `oracle-honesty-on-real-broken-code`,
    `confinement-read-only-workspace-erofs`,
    `runtime-native-skill-mount`,
    `prepared-input-feeds-network-off-oracle`.
- `git diff --check`

Docs note: this worktree has no top-level `README.md` or `docs/` directory.
The model/reducer comments were updated where this contract is represented.

Out-of-scope check: no files under `mission-types/`, `scripts/`, or
`containers/` changed.

## Open Status

No known functional gaps remain for this slice. No live acceptance mission was
run; the requested verification list for this prompt was the static Rust gates,
Podman CI script, diff check, and coverage-parity reconciliation above.
