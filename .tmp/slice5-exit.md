# Slice 5 exit

## Landed

- Team revisions now own role-instance identity, assignments, output semantics,
  runtime, timeout, environment, skills, and authority grants.
- Removed the Slice 5 sunset surface: task namespaces, planning output and
  inventory bridges, conversation recipients and IDs, role-run request
  identity, and the old conversation-derived workspace path.
- Task workspaces are task-owned; role runtime state remains role-owned.
- Joint plan/team promotion, exact judgment panels, taskless recovery,
  prepared role inputs, gap acknowledgment, and authoritative failure repair
  all use one team-owned path.
- The deleted Slice 4 proof corpus is restored against schema-24 team
  vocabulary. Ported production proofs also restored post-turn native-state
  enforcement, exact failure delivery markers, invalid-handoff rework, and
  active task-workspace observation where the tests proved the cutover wrong.
- One RoboRev pass (job 1420); all six findings were addressed.

Versions: `SCHEMA_VERSION = 24`, `REDUCER_VERSION = 49`.

Security impact: team role grants are validated against mission ceilings,
kernel-owned environment coordinates cannot be overridden, and prepared inputs
are mounted only when explicitly granted. No sender identity was introduced.

Contract impact: schema 24 deliberately replaces the unreleased pre-team event
and wire vocabulary without aliases or compatibility bridges. Reducer 49
preserves exact delivery and invalid-handoff evidence across atomic role
failures.

## Proof-corpus inventory

Cutover baseline `7ffd3f98` had deleted all 24 files below. Every file is
present again on the final tree; the 23 executable suites contain exactly 193
tests on both signed Slice 4 head `eabc0094` and this head. `common/mod.rs` is
shared test support. No test file remains deleted relative to `eabc0094`.

- `D -> restored` `advisory_validator.rs` (4)
- `D -> restored` `approval.rs` (2)
- `D -> restored` `common/mod.rs` (support)
- `D -> restored` `controlled_effects.rs` (14)
- `D -> restored` `conversation_resource_lifecycle.rs` (5)
- `D -> restored` `driver_recovery.rs` (17)
- `D -> restored` `eval_deterministic.rs` (2)
- `D -> restored` `fold_litmus.rs` (7)
- `D -> restored` `happy_path.rs` (10)
- `D -> restored` `identity.rs` (3)
- `D -> restored` `message_routing.rs` (1)
- `D -> restored` `mission_type_loading.rs` (42)
- `D -> restored` `phase0_liveness.rs` (2)
- `D -> restored` `planning.rs` (11)
- `D -> restored` `production_conversation_flow.rs` (9)
- `D -> restored` `queue_bounds.rs` (3)
- `D -> restored` `recovery.rs` (8)
- `D -> restored` `reference_expansion.rs` (6)
- `D -> restored` `replanning.rs` (9)
- `D -> restored` `resume.rs` (4)
- `D -> restored` `skill_dispatch.rs` (2)
- `D -> restored` `skill_prompts.rs` (7)
- `D -> restored` `store_hardening.rs` (6)
- `D -> restored` `terminal_review.rs` (19)

Suite dispositions: none died. `planning.rs` remains because planning,
ratification, and refinement machinery survives the sunset.

## Gates

All passed on the final proof-corpus tree:

- `cargo fmt -- --check`
- `cargo check`
- `cargo test`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `bash ./scripts/ci.sh`
- `git diff --check`

The CI script passed all eight real Podman self-tests: moat refusal, atomic
replanning, gap-review closure, writable resume, oracle honesty, read-only
confinement, runtime skill mounting, and prepared-input delivery.

## Acceptance

Fresh binary target: `/tmp/lionclaw-slice5-accept-target.8IKGkh`.
Mission type was verified from the repository path with digest
`abb8932f55fd1ec1841153918d667c3cb7fc5ffd34e4155608159a47ccddb172`.

Exactly one real mission ran: `m0fa582205c1c`. It produced isolated commit
`d817ba43468f1abbb7cb7193baaf1354aa2f25b3`, then parked. No retry, repair,
waiver, replanning generation, or replacement mission was attempted.

Final `mission status --json` projection:

```json
{
  "mission_id": "m0fa582205c1c",
  "phase": "attention_needed",
  "disposition": "parked",
  "finish": null,
  "revision": 1,
  "team_revision": 1,
  "current_sha": "d817ba43468f1abbb7cb7193baaf1354aa2f25b3",
  "contract": [{"id":"SUNSET-ABSENT","advisory":"failed","authoritative_pass":false}],
  "attention": [{"id":"oracle_verdict_failed:cargo-test","kind":"oracle_verdict_failed","assertion_ids":["SUNSET-ABSENT"],"oracle_exit_code":101,"oracle_exit_signal":null}],
  "gap_review": {"attempts":0,"role":"gap-reviewer","verdict":"skipped","acknowledged":false,"waived":false},
  "cleanup_failure": null,
  "driver_error": null
}
```

The reviewer found that the generated test used several incorrect historical
wire-key names and did not recurse into nested source modules. Independently,
the `cargo-test` oracle exited 101 on
`workspace::tests::commit_materialization_is_isolated_from_ambient_attributes`.

Exact event log:

```text
1 mission_created
2 team_configured
3 proposal_recorded
4 decision_recorded
5 team_configured
6 role_turn_requested
7 role_turn_completed
8 role_turn_requested
9 role_turn_completed
10 oracle_run_requested
11 oracle_run_completed
```
