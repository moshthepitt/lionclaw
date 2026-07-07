//! The drive-loop decision function, ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py` (`step`, `_step_mission`,
//! `_all_runnable_tasks`) onto the event-sourced core: `step()` is pure and
//! returns dispatch *intents*; the engine shell materializes them into
//! `…Requested` events (prompt assembly and blob writes are I/O and live in
//! the shell).
//!
//! Deliberate divergences from zenith, both consequences of enforcing the
//! isolation zenith disabled:
//! - **Writers serialize.** Each artifact-producing run gets its own
//!   worktree stacked on the previous artifact commit, so at most one work
//!   task dispatches at a time. Validators and oracles parallelize freely.
//! - **Auto-close.** There is no interactive orchestrator process to call
//!   `end_mission`; when nothing is runnable, inflight, or owed, the phase
//!   derivation closes the mission. Attention parks keep the human pauses.

use std::collections::BTreeMap;

use super::fold::oracle_obligation_outstanding;
use super::ids::{AssertionId, OracleName, RoleName, TaskId};
use super::plan::TaskKind;
use super::state::{InflightEffect, MissionPhase, MissionState, TaskStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    /// Nothing for the loop to start (waiting on inflight effects, or on a
    /// plan submission).
    Idle,
    /// Open attention — park at zero compute (durable interrupt).
    Park,
    /// Terminal phase; nothing will ever run again.
    Terminal,
    /// Dispatch one artifact-producing role run (writers serialize).
    DispatchRole(RoleDispatchIntent),
    /// Run engine oracles (parallelizable).
    RunOracles(Vec<OracleDispatchIntent>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDispatchIntent {
    pub task_id: TaskId,
    pub role: RoleName,
    pub attempt_no: u32,
    pub body: String,
    pub targets: Vec<AssertionId>,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleDispatchIntent {
    pub oracle: OracleName,
    pub assertion_ids: Vec<AssertionId>,
    pub judged_sha: String,
    pub attempt_no: u32,
}

pub fn step(state: &MissionState) -> StepDecision {
    match &state.phase {
        MissionPhase::Planning => StepDecision::Idle,
        MissionPhase::AttentionNeeded => StepDecision::Park,
        MissionPhase::Done { .. } | MissionPhase::Aborted { .. } => StepDecision::Terminal,
        MissionPhase::Running => step_running(state),
    }
}

fn step_running(state: &MissionState) -> StepDecision {
    // Effects already requested own the turn until their outcomes fold in.
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    let Some(plan) = &state.plan else {
        return StepDecision::Idle;
    };

    // A running task without an inflight effect can only mean an outcome is
    // about to be reconciled; never double-dispatch.
    let any_running = state
        .tasks
        .values()
        .any(|t| t.status == TaskStatus::Running);
    if any_running {
        return StepDecision::Idle;
    }

    // Runnable = pending, all deps cleared — in plan order (zenith
    // `_all_runnable_tasks`; list order is the topo tie-break). Work tasks
    // (writers) go first and serialize; a runnable validator (read-only
    // judge) dispatches once no work is runnable. Gates are never
    // "runnable" — their status is fold-derived (see `derive_gates`).
    let status_of = |id: &TaskId| state.tasks.get(id).map(|t| t.status);
    let runnable = |kind: TaskKind| {
        plan.tasks.iter().find(move |task| {
            task.kind == kind
                && status_of(&task.id) == Some(TaskStatus::Pending)
                && task
                    .depends_on
                    .iter()
                    .all(|dep| status_of(dep) == Some(TaskStatus::Cleared))
        })
    };
    if let Some(task) = runnable(TaskKind::Work).or_else(|| runnable(TaskKind::Validate)) {
        let attempt_no = state.tasks.get(&task.id).map_or(0, |t| t.attempts) + 1;
        return StepDecision::DispatchRole(RoleDispatchIntent {
            task_id: task.id.clone(),
            role: task
                .role
                .clone()
                .expect("plan validation guarantees work/validate tasks carry a role"),
            attempt_no,
            body: task.body.clone(),
            targets: task.targets.clone(),
            // Work stacks on the latest artifact; a validator judges it.
            base_sha: state.current_sha.clone(),
        });
    }

    // No work left to start: settle oracle obligations against the current
    // artifact commit, batched per oracle. Skip oracles that failed to run —
    // they park for a human (see `oracle_failures`) rather than loop.
    if oracle_obligation_outstanding(state) {
        let mut by_oracle: BTreeMap<OracleName, Vec<AssertionId>> = BTreeMap::new();
        for (id, assertion) in &state.contract {
            let Some(oracle) = &assertion.oracle else {
                continue;
            };
            if state.oracle_failures.contains_key(oracle) || state.waived_oracles.contains(oracle) {
                continue;
            }
            let fresh = assertion
                .last_authoritative
                .as_ref()
                .is_some_and(|v| v.judged_sha() == state.current_sha);
            if !fresh {
                by_oracle
                    .entry(oracle.clone())
                    .or_default()
                    .push(id.clone());
            }
        }
        let intents = by_oracle
            .into_iter()
            .map(|(oracle, assertion_ids)| {
                let attempt_no = state.oracle_attempts.get(&oracle).copied().unwrap_or(0) + 1;
                OracleDispatchIntent {
                    oracle,
                    assertion_ids,
                    judged_sha: state.current_sha.clone(),
                    attempt_no,
                }
            })
            .collect();
        return StepDecision::RunOracles(intents);
    }

    // Phase derivation would have closed the mission if nothing were owed;
    // reaching here means an outcome is still folding in.
    StepDecision::Idle
}

/// Reconcile targets on resume: inflight `…Requested` entries whose outcome
/// was never recorded (zenith `_reconcile_pending_attempts`). Pure selector;
/// the shell probes the world and appends real or synthesized outcomes.
pub fn unreconciled(state: &MissionState) -> Vec<(&String, &InflightEffect)> {
    state.inflight.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::event::{
        ArtifactOutcome, EventEnvelope, Handoff, MissionConfig, MissionEvent, PayloadRef,
        RunErrorKind, VersionStamps,
    };
    use crate::model::fold::fold;
    use crate::model::ids::MissionId;
    use crate::model::plan::{Assertion, PlanSubmission, Task};
    use crate::model::verdict::FinishClass;

    fn aid(raw: &str) -> AssertionId {
        AssertionId::new(raw).expect("valid assertion id")
    }

    fn tid(raw: &str) -> TaskId {
        TaskId::new(raw).expect("valid task id")
    }

    fn oname(raw: &str) -> OracleName {
        OracleName::new(raw).expect("valid oracle name")
    }

    fn rname(raw: &str) -> RoleName {
        RoleName::new(raw).expect("valid role name")
    }

    fn assertion(id: &str) -> Assertion {
        Assertion {
            id: aid(id),
            prose: format!("claim {id}"),
            oracle: None,
        }
    }

    fn assertion_with_oracle(id: &str, oracle: &str) -> Assertion {
        Assertion {
            id: aid(id),
            prose: format!("claim {id}"),
            oracle: Some(oname(oracle)),
        }
    }

    fn task(
        id: &str,
        kind: TaskKind,
        role: Option<&str>,
        body: &str,
        targets: &[&str],
        deps: &[&str],
    ) -> Task {
        Task {
            id: tid(id),
            kind,
            body: body.to_string(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: role.map(rname),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    fn work(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        task(
            id,
            TaskKind::Work,
            Some("implementer"),
            "produce it",
            targets,
            deps,
        )
    }

    fn validate(id: &str, targets: &[&str]) -> Task {
        task(
            id,
            TaskKind::Validate,
            Some("checker"),
            "check it",
            targets,
            &[],
        )
    }

    fn gate(id: &str) -> Task {
        task(id, TaskKind::Gate, None, "", &[], &[])
    }

    // --- event builders (defaults: recorded_at_ms 0, default stamps) ---

    fn created(base_sha: &str) -> MissionEvent {
        MissionEvent::MissionCreated {
            objective: "ship it".to_string(),
            plugin_name: "software-dev".to_string(),
            workspace_dir: "/workspace".to_string(),
            base_sha: base_sha.to_string(),
            config: MissionConfig {
                ratification_gate: false,
                ..Default::default()
            },
        }
    }

    fn plan(assertions: Vec<Assertion>, tasks: Vec<Task>) -> MissionEvent {
        MissionEvent::PlanSubmitted {
            plan: PlanSubmission { assertions, tasks },
            plan_hash: "deadbeef".to_string(),
        }
    }

    fn role_requested(task: &str, attempt_no: u32, key: &str) -> MissionEvent {
        MissionEvent::RoleRunRequested {
            task_id: tid(task),
            attempt_no,
            idempotency_key: key.to_string(),
            role: rname("implementer"),
            prompt: PayloadRef::inline("assembled prompt"),
            base_sha: "sha-0".to_string(),
        }
    }

    fn work_done(task: &str, key: &str, artifact: Option<(&str, &str)>) -> MissionEvent {
        MissionEvent::RoleRunCompleted {
            task_id: tid(task),
            attempt_no: 1,
            idempotency_key: key.to_string(),
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: artifact.map(|(base_sha, head_sha)| ArtifactOutcome {
                base_sha: base_sha.to_string(),
                head_sha: head_sha.to_string(),
            }),
        }
    }

    fn role_failed(task: &str, key: &str) -> MissionEvent {
        MissionEvent::RoleRunFailed {
            task_id: tid(task),
            attempt_no: 1,
            idempotency_key: key.to_string(),
            error_kind: RunErrorKind::Timeout,
            detail: "runner timed out".to_string(),
            synthesized: false,
        }
    }

    fn oracle_requested(
        ids: &[&str],
        oracle: &str,
        judged_sha: &str,
        attempt_no: u32,
        key: &str,
    ) -> MissionEvent {
        MissionEvent::OracleRunRequested {
            assertion_ids: ids.iter().map(|a| aid(a)).collect(),
            oracle: oname(oracle),
            judged_sha: judged_sha.to_string(),
            attempt_no,
            idempotency_key: key.to_string(),
        }
    }

    fn oracle_completed(
        ids: &[&str],
        oracle: &str,
        judged_sha: &str,
        attempt_no: u32,
        key: &str,
        exit_code: i32,
    ) -> MissionEvent {
        MissionEvent::OracleRunCompleted {
            assertion_ids: ids.iter().map(|a| aid(a)).collect(),
            oracle: oname(oracle),
            judged_sha: judged_sha.to_string(),
            attempt_no,
            idempotency_key: key.to_string(),
            exit_code,
            exit_signal: None,
            stdout: PayloadRef::inline("oracle stdout"),
            stderr: PayloadRef::inline(""),
            duration_ms: 0,
        }
    }

    /// Fold hand-built events with sequence numbers 1..=n so every state a
    /// test steps is one the real fold produced.
    fn fold_log(events: Vec<MissionEvent>) -> MissionState {
        fold(
            events
                .into_iter()
                .enumerate()
                .map(|(i, event)| EventEnvelope {
                    mission_id: MissionId::parse("mabc123abc123").expect("valid mission id"),
                    sequence_no: i as u64 + 1,
                    recorded_at_ms: 0,
                    stamps: VersionStamps::default(),
                    event,
                }),
        )
        .expect("log begins with MissionCreated")
    }

    fn dispatched(state: &MissionState) -> RoleDispatchIntent {
        match step(state) {
            StepDecision::DispatchRole(intent) => intent,
            other => panic!("expected DispatchRole, got {other:?}"),
        }
    }

    // --- phase precedence ---

    #[test]
    fn planning_phase_idles() {
        let state = fold_log(vec![created("sha-0")]);
        assert_eq!(state.phase, MissionPhase::Planning);
        assert_eq!(step(&state), StepDecision::Idle);
    }

    #[test]
    fn open_attention_parks() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("w1", &["A1"], &[]), work("w2", &[], &[])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            role_failed("w1", "k-w1-1"),
        ]);
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
        // w2 is runnable, but open attention parks the mission at zero compute.
        assert_eq!(step(&state), StepDecision::Park);
    }

    #[test]
    fn terminal_phases_never_run_again() {
        let aborted = fold_log(vec![
            created("sha-0"),
            MissionEvent::MissionAborted {
                reason: "operator stop".to_string(),
                actor: "human".to_string(),
            },
        ]);
        assert!(matches!(aborted.phase, MissionPhase::Aborted { .. }));
        assert_eq!(step(&aborted), StepDecision::Terminal);

        // All tasks cleared, nothing owed → the fold auto-closes; A1 has no
        // oracle binding and no advisory pass, so the finish is Unverified.
        let done = fold_log(vec![
            created("sha-0"),
            plan(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", None),
        ]);
        assert_eq!(
            done.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            }
        );
        assert_eq!(step(&done), StepDecision::Terminal);
    }

    // --- inflight and running guards ---

    #[test]
    fn inflight_role_run_owns_the_turn() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("w1", &[], &[]), work("w2", &[], &[])],
            ),
            role_requested("w1", 1, "k-w1-1"),
        ]);
        assert!(!state.inflight.is_empty());
        // w2 is runnable, but the requested effect owns the turn.
        assert_eq!(step(&state), StepDecision::Idle);
    }

    #[test]
    fn inflight_oracle_run_owns_the_turn() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion_with_oracle("A1", "tests")],
                vec![work("w1", &["A1"], &[])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
            oracle_requested(&["A1"], "tests", "sha-1", 1, "k-tests-1"),
        ]);
        // No task is Running here, so this isolates the inflight guard: the
        // obligation is still outstanding but must not be re-requested while
        // its oracle run is inflight.
        assert!(state
            .tasks
            .values()
            .all(|t| t.status != TaskStatus::Running));
        assert_eq!(step(&state), StepDecision::Idle);
    }

    #[test]
    fn running_task_without_inflight_never_double_dispatches() {
        let mut state = fold_log(vec![
            created("sha-0"),
            plan(vec![assertion("A1")], vec![work("w1", &[], &[])]),
            role_requested("w1", 1, "k-w1-1"),
        ]);
        // The reconcile window: the run's outcome exists in the world but has
        // not folded in yet. No Slice-1 fold transition leaves a task Running
        // with an empty inflight map, so drain the map by hand.
        state.inflight.clear();
        assert_eq!(state.tasks[&tid("w1")].status, TaskStatus::Running);
        assert_eq!(step(&state), StepDecision::Idle);
    }

    // --- runnable selection ---

    #[test]
    fn later_task_dispatches_when_earlier_is_blocked() {
        // "blocked" is declared first but gated on "opener": plan order is
        // only a tie-break, not a filter.
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("blocked", &[], &["opener"]), work("opener", &[], &[])],
            ),
        ]);
        assert_eq!(dispatched(&state).task_id, tid("opener"));
    }

    #[test]
    fn earliest_runnable_dispatches_and_writers_serialize() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("w1", &[], &[]), work("w2", &[], &[])],
            ),
        ]);
        // Two runnable writers → exactly one dispatch (DispatchRole carries a
        // single intent by construction), and it is the earlier in plan order.
        assert_eq!(dispatched(&state).task_id, tid("w1"));
    }

    #[test]
    fn work_is_preferred_over_a_runnable_validator() {
        // Both a work task and a validator are runnable; the writer goes
        // first (writers serialize), the validator waits its turn.
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![validate("v1", &["A1"]), work("w1", &["A1"], &[])],
            ),
        ]);
        assert_eq!(dispatched(&state).task_id, tid("w1"));
    }

    #[test]
    fn a_pending_gate_awaiting_its_validators_does_not_dispatch() {
        // The gate depends on a validator that hasn't run; the gate is not
        // "runnable" (gates never dispatch) and nothing else is ready, so
        // the loop idles waiting for the validator lane.
        let g = {
            let mut g = gate("g1");
            g.targets = vec![aid("A1")];
            g.depends_on = vec![tid("v1")];
            g
        };
        let state = fold_log(vec![
            created("sha-0"),
            plan(vec![assertion("A1")], vec![validate("v1", &["A1"]), g]),
            role_requested("v1", 1, "k-v1-1"),
        ]);
        // v1 is running (inflight), so the step idles rather than dispatching.
        assert_eq!(step(&state), StepDecision::Idle);
    }

    #[test]
    fn runnable_validator_dispatches_once_no_work_remains() {
        // A validator whose dependency has cleared dispatches (read-only
        // judge). Gates are never dispatched — their status is derived.
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("w1", &["A1"], &[]), validate("v1", &["A1"])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
        ]);
        assert_eq!(dispatched(&state).task_id, tid("v1"));
    }

    // --- dispatch intent fields ---

    #[test]
    fn dispatch_intent_carries_plan_fields_and_bases_on_current_head() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion("A1")],
                vec![work("w1", &[], &[]), work("w2", &["A1"], &["w1"])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
        ]);
        // w1's artifact moved the head; w2 stacks on it, not on the base.
        assert_eq!(state.current_sha, "sha-1");
        assert_eq!(
            step(&state),
            StepDecision::DispatchRole(RoleDispatchIntent {
                task_id: tid("w2"),
                role: rname("implementer"),
                attempt_no: 1,
                body: "produce it".to_string(),
                targets: vec![aid("A1")],
                base_sha: "sha-1".to_string(),
            })
        );
    }

    #[test]
    fn attempt_no_is_folded_attempts_plus_one() {
        let mut state = fold_log(vec![
            created("sha-0"),
            plan(vec![assertion("A1")], vec![work("w1", &[], &[])]),
            role_requested("w1", 1, "k-w1-1"),
        ]);
        assert_eq!(state.tasks[&tid("w1")].attempts, 1);
        // The failure→retry re-pend transition is Slice-4; simulate its
        // post-state so the numbering contract is pinned now.
        state.inflight.clear();
        state.tasks.get_mut(&tid("w1")).expect("w1 exists").status = TaskStatus::Pending;
        assert_eq!(dispatched(&state).attempt_no, 2);
    }

    // --- oracle scheduling ---

    #[test]
    fn oracle_runs_batch_per_oracle_at_current_head() {
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![
                    assertion_with_oracle("A1", "tests"),
                    assertion_with_oracle("A2", "build"),
                    assertion_with_oracle("A3", "tests"),
                ],
                vec![work("w1", &["A1", "A2", "A3"], &[])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
        ]);
        assert_eq!(
            step(&state),
            StepDecision::RunOracles(vec![
                OracleDispatchIntent {
                    oracle: oname("build"),
                    assertion_ids: vec![aid("A2")],
                    judged_sha: "sha-1".to_string(),
                    attempt_no: 1,
                },
                OracleDispatchIntent {
                    oracle: oname("tests"),
                    assertion_ids: vec![aid("A1"), aid("A3")],
                    judged_sha: "sha-1".to_string(),
                    attempt_no: 1,
                },
            ])
        );
    }

    #[test]
    fn stale_verdict_is_rejudged_at_new_head() {
        // The oracle passed at sha-1, then w2's artifact moved the head to
        // sha-2: the verdict is stale, so the obligation reopens and the next
        // run judges the new head with the next attempt number. (The fold
        // accepts any log order, so the oracle run is recorded before w2.)
        let state = fold_log(vec![
            created("sha-0"),
            plan(
                vec![assertion_with_oracle("A1", "tests")],
                vec![work("w1", &["A1"], &[]), work("w2", &[], &["w1"])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
            oracle_requested(&["A1"], "tests", "sha-1", 1, "k-tests-1"),
            oracle_completed(&["A1"], "tests", "sha-1", 1, "k-tests-1", 0),
            role_requested("w2", 1, "k-w2-1"),
            work_done("w2", "k-w2-1", Some(("sha-1", "sha-2"))),
        ]);
        assert_eq!(state.phase, MissionPhase::Running);
        assert_eq!(
            step(&state),
            StepDecision::RunOracles(vec![OracleDispatchIntent {
                oracle: oname("tests"),
                assertion_ids: vec![aid("A1")],
                judged_sha: "sha-2".to_string(),
                // oracle_attempts folded from the prior Requested, plus one.
                attempt_no: 2,
            }])
        );
    }

    #[test]
    fn fresh_verdict_settles_the_obligation() {
        // A fresh verdict at the current head — pass or fail — settles the
        // obligation: no re-request, the phase closes, and the step is
        // Terminal. Retry-after-fail is a human decision, not an engine loop.
        let cases = [(0, FinishClass::Verified), (1, FinishClass::Unverified)];
        for (exit_code, finish) in cases {
            let state = fold_log(vec![
                created("sha-0"),
                plan(
                    vec![assertion_with_oracle("A1", "tests")],
                    vec![work("w1", &["A1"], &[])],
                ),
                role_requested("w1", 1, "k-w1-1"),
                work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
                oracle_requested(&["A1"], "tests", "sha-1", 1, "k-tests-1"),
                oracle_completed(&["A1"], "tests", "sha-1", 1, "k-tests-1", exit_code),
            ]);
            assert_eq!(
                state.phase,
                MissionPhase::Done { finish },
                "exit {exit_code}"
            );
            assert_eq!(step(&state), StepDecision::Terminal, "exit {exit_code}");
        }
    }

    // --- reconcile selector ---

    #[test]
    fn unreconciled_lists_requests_without_outcomes() {
        let requested = vec![
            created("sha-0"),
            plan(vec![assertion("A1")], vec![work("w1", &[], &[])]),
            role_requested("w1", 1, "k-w1-1"),
        ];
        let state = fold_log(requested.clone());
        let pending = unreconciled(&state);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "k-w1-1");
        assert!(matches!(pending[0].1, InflightEffect::RoleRun { .. }));

        let mut reconciled = requested;
        reconciled.push(work_done("w1", "k-w1-1", None));
        assert!(unreconciled(&fold_log(reconciled)).is_empty());
    }
}
