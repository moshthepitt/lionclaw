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
//!   task dispatches at a time. Validators dispatch one at a time too (any
//!   role run does), but need no stacked worktree; only oracles batch and run
//!   in parallel.
//! - **Auto-close.** There is no interactive orchestrator process to call
//!   `end_mission`; when nothing is runnable, inflight, or owed, the phase
//!   derivation closes the mission. Attention parks keep the human pauses.

use std::collections::BTreeMap;

use super::fold::{oracle_obligation_outstanding, terminal_review_outstanding};
use super::ids::{AssertionId, OracleName, RoleName, TaskId};
use super::plan::TaskKind;
use super::state::{MissionPhase, MissionState, ReviewOutcome, TaskStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    /// Nothing for the loop to start (waiting on inflight effects, or on a
    /// plan proposal).
    Idle,
    /// Open attention — park at zero compute (durable interrupt).
    Park,
    /// Terminal phase; nothing will ever run again.
    Terminal,
    /// Dispatch one role run — a planning role, a writer, or a validator. All
    /// role runs serialize (at most one inflight); only writers also carry the
    /// stacked-worktree ordering constraint.
    DispatchRole(RoleDispatchIntent),
    /// Run engine oracles (parallelizable).
    RunOracles(Vec<OracleDispatchIntent>),
    /// Dispatch the closing terminal review: the config-declared
    /// `emits-gap-verdict` role, fresh-context and contract-blind (the intent
    /// carries no targets and no task body by construction).
    ReviewTerminal(TerminalReviewDispatchIntent),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalReviewDispatchIntent {
    pub role: RoleName,
    pub attempt_no: u32,
    /// The commit under review (== `current_sha` at dispatch); the role's
    /// checkout is pinned here and the verdict is stamped here.
    pub judged_sha: String,
}

pub fn step(state: &MissionState) -> StepDecision {
    match &state.phase {
        MissionPhase::Planning => step_planning(state),
        MissionPhase::AttentionNeeded => StepDecision::Park,
        MissionPhase::Done { .. } | MissionPhase::Aborted { .. } => StepDecision::Terminal,
        MissionPhase::Running => step_running(state),
    }
}

/// A node is runnable when it is Pending and every dependency has Cleared. The
/// pending/deps predicate shared by planning and execution scheduling.
fn is_runnable(
    id: &TaskId,
    deps: &[TaskId],
    status_of: &impl Fn(&TaskId) -> Option<TaskStatus>,
    retryable: &impl Fn(&TaskId) -> bool,
) -> bool {
    (status_of(id) == Some(TaskStatus::Pending) || retryable(id))
        && deps
            .iter()
            .all(|dep| status_of(dep) == Some(TaskStatus::Cleared))
}

/// The contract-free planning phase: dispatch the next runnable planning role.
/// Planning roles are read-only, so the workspace never moves — every node is
/// judged at `base_sha`. When nothing is runnable the mission idles (an empty
/// planning DAG ⇒ `AwaitingPlan`; a finished author parks on `PlanProposal`).
fn step_planning(state: &MissionState) -> StepDecision {
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    // A Running planning node means an outcome is folding in; never double-dispatch.
    if state
        .planning
        .tasks
        .values()
        .any(|t| t.status == TaskStatus::Running)
    {
        return StepDecision::Idle;
    }
    let status_of = |id: &TaskId| state.planning.tasks.get(id).map(|task| task.status);
    let retryable =
        |id: &TaskId| {
            state.planning.tasks.get(id).is_some_and(|task| {
                task.automatic_retry_remaining(state.config.recovery.max_attempts)
            })
        };
    let Some(task) = state
        .config
        .planning
        .tasks
        .iter()
        .find(|t| is_runnable(&t.id, &t.depends_on, &status_of, &retryable))
    else {
        return StepDecision::Idle;
    };
    let attempt_no = state.planning.tasks.get(&task.id).map_or(0, |t| t.attempts) + 1;
    StepDecision::DispatchRole(RoleDispatchIntent {
        task_id: task.id.clone(),
        role: task.role.clone(),
        attempt_no,
        body: task.body.clone(),
        // Planning has no contract; its roles are read-only at the base commit.
        targets: Vec::new(),
        base_sha: state.deliverable_head().to_string(),
    })
}

fn step_running(state: &MissionState) -> StepDecision {
    // Effects already requested own the turn until their outcomes fold in.
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    let Some(plan) = &state.plan else {
        return StepDecision::Idle;
    };

    // A running task without an inflight effect is an impossible folded state;
    // never double-dispatch if a future reducer encounters one.
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
    let status_of = |id: &TaskId| state.tasks.get(id).map(|task| task.status);
    let retryable = |id: &TaskId| {
        state
            .tasks
            .get(id)
            .is_some_and(|task| task.automatic_retry_remaining(state.config.recovery.max_attempts))
    };
    let runnable = |kind: TaskKind| {
        plan.tasks.iter().find(move |task| {
            task.kind == kind && is_runnable(&task.id, &task.depends_on, &status_of, &retryable)
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
            base_sha: state.deliverable_head().to_string(),
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
                .is_some_and(|v| v.is_fresh_at(state.deliverable_head()));
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
                    judged_sha: state.deliverable_head().to_string(),
                    attempt_no,
                }
            })
            .collect();
        return StepDecision::RunOracles(intents);
    }

    // Work settled and every oracle verdict fresh: a configured terminal
    // review without a fresh verdict at the current head dispatches the
    // closing reviewer. A parked failure never re-dispatches (attention parks
    // first; the guard mirrors the failed-oracle skip above).
    let review_retryable = matches!(
        &state.terminal_review.outcome,
        Some(ReviewOutcome::Failed { failure })
            if failure.automatically_retryable()
                && state.terminal_review.consecutive_failures
                    < state.config.recovery.max_attempts
    );
    if terminal_review_outstanding(state)
        && (!matches!(
            state.terminal_review.outcome,
            Some(ReviewOutcome::Failed { .. })
        ) || review_retryable)
    {
        let config = state
            .config
            .terminal_review
            .as_ref()
            .expect("terminal_review_outstanding implies the config is present");
        return StepDecision::ReviewTerminal(TerminalReviewDispatchIntent {
            role: config.role.clone(),
            attempt_no: state.terminal_review.attempts + 1,
            judged_sha: state.deliverable_head().to_string(),
        });
    }

    // Phase derivation would have closed the mission if nothing were owed;
    // reaching here means an outcome is still folding in.
    StepDecision::Idle
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::event::{
        ArtifactOutcome, EventEnvelope, Handoff, MissionConfig, MissionEvent, PayloadRef,
        RunErrorKind, VersionStamps,
    };
    use crate::model::fold::fold;
    use crate::model::ids::{EffectId, MissionId};
    use crate::model::plan::{Assertion, Plan, Task};
    use crate::model::state::RunFailure;
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
            mission_type: crate::model::MissionTypeRef {
                name: "software-dev".into(),
                digest: "d".into(),
            },
            runtime: "codex".into(),
            image_id: "img".into(),
            workspace_dir: "/workspace".to_string(),
            base_sha: base_sha.to_string(),
            config: MissionConfig {
                recovery: crate::model::RecoveryConfig { max_attempts: 1 },
                ..Default::default()
            },
        }
    }

    fn plan(assertions: Vec<Assertion>, tasks: Vec<Task>) -> MissionEvent {
        MissionEvent::PlanProposed {
            proposal: crate::model::PlanProposal {
                base_revision: 0,
                plan: Plan {
                    requirements: vec![],
                    assertions,
                    tasks,
                },
            },
            plan_hash: "deadbeef".to_string(),
        }
    }

    fn role_requested(task: &str, attempt_no: u32, key: &str) -> MissionEvent {
        MissionEvent::RoleRunRequested {
            task_id: tid(task),
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            role: rname("implementer"),
            runtime: "codex".to_string(),
            prompt: PayloadRef::inline("assembled prompt"),
            base_sha: "sha-0".to_string(),
            assignment_epoch: 1,
            recreate_workspace: true,
        }
    }

    fn work_done(task: &str, key: &str, artifact: Option<(&str, &str)>) -> MissionEvent {
        MissionEvent::RoleRunCompleted {
            task_id: tid(task),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: artifact.map(|(base_sha, head_sha)| ArtifactOutcome {
                base_sha: base_sha.to_string(),
                head_sha: head_sha.to_string(),
            }),
            final_response: PayloadRef::inline("done"),
        }
    }

    fn role_failed(task: &str, key: &str) -> MissionEvent {
        MissionEvent::RoleRunFailed {
            task_id: tid(task),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            failure: RunFailure {
                kind: RunErrorKind::Timeout,
                detail: "runner timed out".to_string(),
            },
            final_response: PayloadRef::inline(""),
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
            effect_id: EffectId::for_parts(&["test", key]),
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
            effect_id: EffectId::for_parts(&["test", key]),
            exit_code,
            exit_signal: None,
            stdout: PayloadRef::inline("oracle stdout"),
            stderr: PayloadRef::inline(""),
            prepared_inputs: Vec::new(),
            duration_ms: 0,
        }
    }

    /// Fold hand-built events with sequence numbers 1..=n so every state a
    /// test steps is one the real fold produced.
    fn fold_log(events: Vec<MissionEvent>) -> MissionState {
        let events = events.into_iter().flat_map(|event| {
            let approve = matches!(&event, MissionEvent::PlanProposed { .. }).then(|| {
                MissionEvent::DecisionRecorded {
                    attention_id: "plan_proposal:mission".into(),
                    action: crate::model::DecisionAction::Approve,
                    justification: "test fixture approves the plan".into(),
                }
            });
            std::iter::once(event).chain(approve)
        });
        fold(events.enumerate().map(|(i, event)| EventEnvelope {
            mission_id: MissionId::parse("mabc123abc123").expect("valid mission id"),
            sequence_no: i as u64 + 1,
            recorded_at_ms: 0,
            stamps: VersionStamps::default(),
            event,
        }))
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
        // No current fold transition leaves a task Running
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
        // Hand-apply the post-state of the failure→retry re-pend (a
        // RoleRunFailed + DecisionRecorded(Retry, node_failed:…) sequence,
        // covered in the fold tests) to pin the attempt-numbering contract here.
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
        // obligation. Pass closes; fail parks on the repair path.
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
            assert_eq!(crate::model::classify_finish(&state), finish);
            let expected = if exit_code == 0 {
                MissionPhase::Done { finish }
            } else {
                MissionPhase::AttentionNeeded
            };
            assert_eq!(state.phase, expected, "exit {exit_code}");
            assert_eq!(
                step(&state),
                if exit_code == 0 {
                    StepDecision::Terminal
                } else {
                    StepDecision::Park
                },
                "exit {exit_code}"
            );
        }
    }

    // --- terminal review: the closing dispatch ---

    fn created_with_review(base_sha: &str) -> MissionEvent {
        let mut event = created(base_sha);
        let MissionEvent::MissionCreated { config, .. } = &mut event else {
            unreachable!("created() builds MissionCreated");
        };
        config.terminal_review = Some(crate::model::event::TerminalReviewConfig {
            role: rname("gap-reviewer"),
        });
        event
    }

    fn review_requested(attempt_no: u32, key: &str, judged_sha: &str) -> MissionEvent {
        MissionEvent::TerminalReviewRequested {
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            role: rname("gap-reviewer"),
            runtime: "codex".to_string(),
            prompt: PayloadRef::inline("review prompt"),
            judged_sha: judged_sha.to_string(),
            nonce: "n0".to_string(),
        }
    }

    fn review_completed(
        key: &str,
        judged_sha: &str,
        passed: bool,
        blocking_gaps: usize,
    ) -> MissionEvent {
        use crate::model::event::{Gap, GapSeverity};
        MissionEvent::TerminalReviewCompleted {
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            judged_sha: judged_sha.to_string(),
            passed,
            gaps: (0..blocking_gaps)
                .map(|_| Gap {
                    id: None,
                    severity: GapSeverity::Blocking,
                    requirement: "r".into(),
                    expected: "e".into(),
                    observed: "o".into(),
                    evidence: "v".into(),
                })
                .collect(),
            report: PayloadRef::inline("map + observations"),
        }
    }

    /// Work committed to sha-1 and the oracle fresh-passing there: only the
    /// review obligation remains.
    fn review_brink() -> Vec<MissionEvent> {
        vec![
            created_with_review("sha-0"),
            plan(
                vec![assertion_with_oracle("A1", "tests")],
                vec![work("w1", &["A1"], &[])],
            ),
            role_requested("w1", 1, "k-w1-1"),
            work_done("w1", "k-w1-1", Some(("sha-0", "sha-1"))),
            oracle_requested(&["A1"], "tests", "sha-1", 1, "k-tests-1"),
            oracle_completed(&["A1"], "tests", "sha-1", 1, "k-tests-1", 0),
        ]
    }

    fn review_dispatched(state: &MissionState) -> TerminalReviewDispatchIntent {
        match step(state) {
            StepDecision::ReviewTerminal(intent) => intent,
            other => panic!("expected ReviewTerminal, got {other:?}"),
        }
    }

    #[test]
    fn reviewer_dispatches_only_after_oracles_settle_at_the_current_head() {
        // With the oracle still owed, the oracle batch wins the turn …
        let mut events = review_brink();
        events.truncate(4); // drop the oracle request/completion
        let owed = fold_log(events);
        assert!(matches!(step(&owed), StepDecision::RunOracles(_)));

        // … and only a settled, fresh-passing contract dispatches the
        // reviewer — contract-blind at the current head.
        let state = fold_log(review_brink());
        let intent = review_dispatched(&state);
        assert_eq!(intent.role, rname("gap-reviewer"));
        assert_eq!(intent.attempt_no, 1);
        assert_eq!(intent.judged_sha, "sha-1");
    }

    #[test]
    fn no_reviewer_dispatch_without_config() {
        let mut events = review_brink();
        events[0] = created("sha-0");
        let state = fold_log(events);
        assert_eq!(step(&state), StepDecision::Terminal);
    }

    #[test]
    fn inflight_terminal_review_owns_the_turn() {
        let mut events = review_brink();
        events.push(review_requested(1, "k-tr-1", "sha-1"));
        let state = fold_log(events);
        assert_eq!(step(&state), StepDecision::Idle);
    }

    #[test]
    fn stale_review_verdict_redispatches_after_head_move() {
        let mut events = review_brink();
        events.push(review_requested(1, "k-tr-1", "sha-1"));
        events.push(review_completed("k-tr-1", "sha-1", true, 0));
        // New work moves the head; the oracle re-judges; the clean sha-1
        // verdict is stale — a second review dispatches at the new head.
        events.push(work_done("w1", "k-w1-2", Some(("sha-1", "sha-2"))));
        events.push(oracle_completed(
            &["A1"],
            "tests",
            "sha-2",
            2,
            "k-tests-2",
            0,
        ));
        let state = fold_log(events);
        let intent = review_dispatched(&state);
        assert_eq!(intent.attempt_no, 2);
        assert_eq!(intent.judged_sha, "sha-2");
    }

    #[test]
    fn a_gap_park_steps_park_and_retry_redispatches_fresh() {
        use crate::model::event::DecisionAction;
        let mut events = review_brink();
        events.push(review_requested(1, "k-tr-1", "sha-1"));
        events.push(review_completed("k-tr-1", "sha-1", false, 1));
        let parked = fold_log(events.clone());
        assert_eq!(step(&parked), StepDecision::Park);

        events.push(MissionEvent::DecisionRecorded {
            attention_id: "terminal_review_gaps:mission".to_string(),
            action: DecisionAction::Retry,
            justification: "re-roll".to_string(),
        });
        let state = fold_log(events);
        // Attempts are preserved: the re-roll runs under attempt 2 (⇒ a
        // fresh effect ID) at the unchanged head.
        let intent = review_dispatched(&state);
        assert_eq!(intent.attempt_no, 2);
        assert_eq!(intent.judged_sha, "sha-1");
    }
}
