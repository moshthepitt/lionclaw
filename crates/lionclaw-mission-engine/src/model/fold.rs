//! The pure fold: `state = fold(log)`. No I/O, no clock, no RNG — enforced
//! by the model's dependency wall and the crate clippy config. Every
//! engine-deterministic transition happens here; the phase (including the
//! finish class) is re-derived after every event, never stored.
//!
//! Handoff application semantics ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py::_apply_handoff_collect`: a work
//! task that isn't `done` fails and raises attention; a validate task always
//! clears and folds its per-assertion verdicts in with sticky passes.

use super::event::{EventEnvelope, Handoff, MissionEvent};
use super::state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, TaskRuntimeState, TaskStatus,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};

/// Bump when fold semantics change; snapshots with a different version are
/// discarded and rebuilt from sequence zero.
pub const REDUCER_VERSION: u32 = 1;

/// Fold a mission's event stream. `None` until a `MissionCreated` arrives.
pub fn fold(events: impl IntoIterator<Item = EventEnvelope>) -> Option<MissionState> {
    let mut state: Option<MissionState> = None;
    for envelope in events {
        match state.as_mut() {
            None => state = bootstrap(&envelope),
            Some(state) => apply(state, &envelope),
        }
    }
    state
}

fn bootstrap(envelope: &EventEnvelope) -> Option<MissionState> {
    let MissionEvent::MissionCreated {
        objective,
        plugin_name,
        workspace_dir,
        base_sha,
        config,
    } = &envelope.event
    else {
        return None;
    };
    Some(MissionState {
        mission_id: envelope.mission_id.clone(),
        objective: objective.clone(),
        plugin_name: plugin_name.clone(),
        workspace_dir: workspace_dir.clone(),
        base_sha: base_sha.clone(),
        config: config.clone(),
        phase: MissionPhase::Planning,
        plan: None,
        contract: Default::default(),
        tasks: Default::default(),
        current_sha: base_sha.clone(),
        oracle_attempts: Default::default(),
        terminal_review_attempts: 0,
        terminal_review_done: None,
        inflight: Default::default(),
        open_attention: Default::default(),
        head: envelope.sequence_no,
    })
}

/// Apply one event. Total: impossible transitions are deterministic no-ops,
/// never panics — the log is the source of truth even when an older engine
/// wrote it.
pub fn apply(state: &mut MissionState, envelope: &EventEnvelope) {
    let seq = envelope.sequence_no;
    match &envelope.event {
        MissionEvent::MissionCreated { .. } => {}
        MissionEvent::PlanSubmitted { plan, .. } => {
            for assertion in &plan.assertions {
                state
                    .contract
                    .entry(assertion.id.clone())
                    .or_insert_with(|| AssertionState {
                        oracle: assertion.oracle.clone(),
                        advisory: AdvisoryStatus::Pending,
                        last_advisory: Default::default(),
                        last_authoritative: None,
                    });
            }
            for task in &plan.tasks {
                state
                    .tasks
                    .entry(task.id.clone())
                    .or_insert(TaskRuntimeState {
                        status: TaskStatus::Pending,
                        attempts: 0,
                    });
            }
            state.plan = Some(plan.clone());
        }
        MissionEvent::RoleRunRequested {
            task_id, attempt_no, ..
        } => {
            let task = state.tasks.entry(task_id.clone()).or_insert(TaskRuntimeState {
                status: TaskStatus::Pending,
                attempts: 0,
            });
            task.status = TaskStatus::Running;
            task.attempts = *attempt_no;
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::RoleRunCompleted {
            task_id,
            idempotency_key,
            handoff,
            artifact,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            if let Some(artifact) = artifact {
                state.current_sha = artifact.head_sha.clone();
            }
            apply_handoff(state, task_id, handoff, seq);
        }
        MissionEvent::RoleRunFailed {
            task_id,
            idempotency_key,
            error_kind,
            detail,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Failed;
            }
            push_attention(
                state,
                AttentionKind::NodeFailed,
                Some(task_id.clone()),
                format!("role run failed ({error_kind:?}): {detail}"),
                seq,
            );
        }
        MissionEvent::OracleRunRequested {
            oracle, attempt_no, ..
        } => {
            state.oracle_attempts.insert(oracle.clone(), *attempt_no);
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::OracleRunCompleted {
            assertion_ids,
            oracle,
            judged_sha,
            idempotency_key,
            exit_code,
            exit_signal,
            stdout,
            stderr,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            let verdict = AuthoritativeVerdict::from_oracle_outcome(
                oracle.clone(),
                judged_sha.clone(),
                *exit_code,
                *exit_signal,
                stdout.clone(),
                stderr.clone(),
                seq,
            );
            for assertion_id in assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    assertion.last_authoritative = Some(verdict.clone());
                }
            }
        }
        MissionEvent::OracleRunFailed {
            oracle,
            idempotency_key,
            detail,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            push_attention(
                state,
                AttentionKind::NodeFailed,
                None,
                format!("oracle '{oracle}' failed to run: {detail}"),
                seq,
            );
        }
        MissionEvent::TerminalReviewRequested { attempt_no, .. } => {
            state.terminal_review_attempts = *attempt_no;
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::TerminalReviewCompleted {
            idempotency_key,
            done,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            state.terminal_review_done = Some(*done);
        }
        MissionEvent::MissionAborted { reason, .. } => {
            state.phase = MissionPhase::Aborted {
                reason: reason.clone(),
            };
        }
    }
    state.head = seq;
    derive_phase(state);
}

fn track_inflight(state: &mut MissionState, event: &MissionEvent, seq: u64) {
    if let Some((key, effect)) = InflightEffect::from_request(event, seq) {
        state.inflight.insert(key, effect);
    }
}

fn apply_handoff(
    state: &mut MissionState,
    task_id: &super::ids::TaskId,
    handoff: &Handoff,
    seq: u64,
) {
    match handoff {
        Handoff::Work {
            done,
            report,
            request_attention,
        } => {
            let status = if *done {
                TaskStatus::Cleared
            } else {
                TaskStatus::Failed
            };
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = status;
            }
            if !*done {
                push_attention(
                    state,
                    AttentionKind::NodeFailed,
                    Some(task_id.clone()),
                    format!("work task reported not done: {}", summarize(report)),
                    seq,
                );
            } else if *request_attention {
                push_attention(
                    state,
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    summarize(report),
                    seq,
                );
            }
        }
        Handoff::Validate {
            items,
            request_attention,
            report,
            ..
        } => {
            // Validators always clear — they ran; their verdicts are data.
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Cleared;
            }
            for item in items {
                if let Some(assertion) = state.contract.get_mut(&item.item_id) {
                    assertion.last_advisory.insert(task_id.clone(), item.passed);
                    if item.passed {
                        assertion.advisory = AdvisoryStatus::Passed; // sticky
                    } else if assertion.advisory != AdvisoryStatus::Passed {
                        assertion.advisory = AdvisoryStatus::Failed;
                    }
                }
            }
            if *request_attention {
                push_attention(
                    state,
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    summarize(report),
                    seq,
                );
            }
        }
    }
}

fn summarize(report: &super::event::PayloadRef) -> String {
    match report {
        super::event::PayloadRef::Inline { text } => text.clone(),
        super::event::PayloadRef::Blob(blob) => format!("(report blob {})", blob.hex),
    }
}

fn push_attention(
    state: &mut MissionState,
    kind: AttentionKind,
    task_id: Option<super::ids::TaskId>,
    report: String,
    seq: u64,
) {
    let anchor = task_id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "engine".to_string());
    let id = format!("{kind:?}:{anchor}:{seq}").to_lowercase();
    state
        .open_attention
        .insert(id.clone(), AttentionItem { id, kind, task_id, report });
}

/// Re-derive the phase from scratch. Abort is the one sticky, event-anchored
/// exception (the fact isn't reconstructible from other fields).
fn derive_phase(state: &mut MissionState) {
    if matches!(state.phase, MissionPhase::Aborted { .. }) {
        return;
    }
    state.phase = if !state.open_attention.is_empty() {
        MissionPhase::AttentionNeeded
    } else if state.plan.is_none() {
        MissionPhase::Planning
    } else if tasks_active(state)
        || !state.inflight.is_empty()
        || oracle_obligation_outstanding(state)
    {
        MissionPhase::Running
    } else {
        MissionPhase::Done {
            finish: classify_finish(state),
        }
    };
}

fn tasks_active(state: &MissionState) -> bool {
    state
        .tasks
        .values()
        .any(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Running))
}

/// An oracle-bound assertion without a verdict at the current artifact commit
/// still owes the engine a run (a fresh *fail* settles the obligation — retry
/// is a human decision, not an engine loop).
pub(crate) fn oracle_obligation_outstanding(state: &MissionState) -> bool {
    state.contract.values().any(|assertion| {
        assertion.oracle.is_some()
            && assertion
                .last_authoritative
                .as_ref()
                .is_none_or(|v| v.judged_sha() != state.current_sha)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::event::{
        ArtifactOutcome, MissionConfig, PayloadRef, RunErrorKind, ValidationItem,
    };
    use super::super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
    use super::super::plan::{Assertion, PlanSubmission, Task, TaskKind};
    use super::super::verdict::FinishClass;
    use super::*;

    fn tid(raw: &str) -> TaskId {
        TaskId::new(raw).expect("task id")
    }

    fn aid(raw: &str) -> AssertionId {
        AssertionId::new(raw).expect("assertion id")
    }

    fn oracle(raw: &str) -> OracleName {
        OracleName::new(raw).expect("oracle name")
    }

    fn envelope(sequence_no: u64, event: MissionEvent) -> EventEnvelope {
        EventEnvelope {
            mission_id: MissionId::from_digest_prefix("abcdef0123456789"),
            sequence_no,
            recorded_at_ms: 0,
            stamps: Default::default(),
            event,
        }
    }

    /// Fold hand-built events with sequence numbers assigned by position.
    fn fold_log(events: Vec<MissionEvent>) -> Option<MissionState> {
        fold(
            events
                .into_iter()
                .enumerate()
                .map(|(i, event)| envelope(i as u64, event)),
        )
    }

    fn created() -> MissionEvent {
        MissionEvent::MissionCreated {
            objective: "objective".into(),
            plugin_name: "plugin".into(),
            workspace_dir: "/w".into(),
            base_sha: "base".into(),
            config: MissionConfig::default(),
        }
    }

    fn plan_submitted(assertions: Vec<Assertion>, tasks: Vec<Task>) -> MissionEvent {
        MissionEvent::PlanSubmitted {
            plan: PlanSubmission { assertions, tasks },
            plan_hash: "hash".into(),
        }
    }

    fn assertion(id: &str, oracle_name: Option<&str>) -> Assertion {
        Assertion {
            id: aid(id),
            prose: "claim".into(),
            oracle: oracle_name.map(oracle),
        }
    }

    fn work_task(id: &str) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Work,
            body: "do".into(),
            targets: vec![],
            role: Some(RoleName::new("implementer").expect("role name")),
            depends_on: vec![],
        }
    }

    fn validate_task(id: &str) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Validate,
            body: "check".into(),
            targets: vec![],
            role: Some(RoleName::new("reviewer").expect("role name")),
            depends_on: vec![],
        }
    }

    fn work_handoff(done: bool, request_attention: bool) -> Handoff {
        Handoff::Work {
            done,
            report: PayloadRef::inline("report"),
            request_attention,
        }
    }

    fn validate_handoff(items: &[(&str, bool)]) -> Handoff {
        Handoff::Validate {
            done: true,
            report: PayloadRef::inline("checked"),
            items: items
                .iter()
                .map(|(id, passed)| ValidationItem {
                    item_id: aid(id),
                    passed: *passed,
                })
                .collect(),
            passed: items.iter().all(|(_, passed)| *passed),
            request_attention: false,
        }
    }

    fn role_requested(task: &str, key: &str) -> MissionEvent {
        MissionEvent::RoleRunRequested {
            task_id: tid(task),
            attempt_no: 1,
            idempotency_key: key.into(),
            role: RoleName::new("implementer").expect("role name"),
            prompt: PayloadRef::inline("prompt"),
            base_sha: "base".into(),
        }
    }

    fn role_completed(
        task: &str,
        key: &str,
        handoff: Handoff,
        artifact: Option<ArtifactOutcome>,
    ) -> MissionEvent {
        MissionEvent::RoleRunCompleted {
            task_id: tid(task),
            attempt_no: 1,
            idempotency_key: key.into(),
            handoff,
            artifact,
        }
    }

    fn oracle_requested(assertion_id: &str, judged: &str, key: &str) -> MissionEvent {
        MissionEvent::OracleRunRequested {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            idempotency_key: key.into(),
        }
    }

    fn oracle_completed(
        assertion_id: &str,
        judged: &str,
        key: &str,
        exit_code: i32,
    ) -> MissionEvent {
        MissionEvent::OracleRunCompleted {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            idempotency_key: key.into(),
            exit_code,
            exit_signal: None,
            stdout: PayloadRef::inline("out"),
            stderr: PayloadRef::inline("err"),
            duration_ms: 5,
        }
    }

    #[test]
    fn fold_bootstraps_only_on_mission_created() {
        let state = fold_log(vec![created()]).expect("created bootstraps");
        assert_eq!(state.phase, MissionPhase::Planning);
        assert_eq!(state.base_sha, "base");
        assert_eq!(state.current_sha, "base");
        assert_eq!(state.head, 0);
        assert!(state.plan.is_none());
        assert!(state.tasks.is_empty() && state.contract.is_empty());

        let non_created = [
            plan_submitted(vec![], vec![]),
            role_completed("t1", "k1", work_handoff(true, false), None),
            MissionEvent::MissionAborted {
                reason: "stop".into(),
                actor: "human".into(),
            },
        ];
        for event in non_created {
            let name = event.event_type();
            assert!(
                fold_log(vec![event]).is_none(),
                "first event {name} must not bootstrap"
            );
        }
    }

    #[test]
    fn plan_submitted_initializes_contract_and_tasks() {
        let state = fold_log(vec![
            created(),
            plan_submitted(
                vec![
                    assertion("TESTS-PASS", Some("cargo-test")),
                    assertion("NO-ORACLE", None),
                ],
                vec![work_task("t1"), validate_task("v1")],
            ),
        ])
        .expect("state");
        assert!(state.plan.is_some());
        let bound = &state.contract[&aid("TESTS-PASS")];
        assert_eq!(bound.oracle, Some(oracle("cargo-test")));
        assert_eq!(bound.advisory, AdvisoryStatus::Pending);
        assert!(bound.last_advisory.is_empty());
        assert!(bound.last_authoritative.is_none());
        assert_eq!(state.contract[&aid("NO-ORACLE")].oracle, None);
        for id in ["t1", "v1"] {
            let task = &state.tasks[&tid(id)];
            assert_eq!(task.status, TaskStatus::Pending);
            assert_eq!(task.attempts, 0);
        }
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn work_handoff_status_and_attention() {
        struct Case {
            name: &'static str,
            done: bool,
            request_attention: bool,
            expect_status: TaskStatus,
            expect_kind: Option<AttentionKind>,
        }
        let cases = [
            Case {
                name: "done clears",
                done: true,
                request_attention: false,
                expect_status: TaskStatus::Cleared,
                expect_kind: None,
            },
            Case {
                name: "not done fails and raises node_failed",
                done: false,
                request_attention: false,
                expect_status: TaskStatus::Failed,
                expect_kind: Some(AttentionKind::NodeFailed),
            },
            Case {
                name: "done with request_attention parks as node_attention",
                done: true,
                request_attention: true,
                expect_status: TaskStatus::Cleared,
                expect_kind: Some(AttentionKind::NodeAttention),
            },
            Case {
                name: "not done wins over request_attention",
                done: false,
                request_attention: true,
                expect_status: TaskStatus::Failed,
                expect_kind: Some(AttentionKind::NodeFailed),
            },
        ];
        for case in cases {
            let state = fold_log(vec![
                created(),
                plan_submitted(vec![], vec![work_task("t1")]),
                role_completed(
                    "t1",
                    "k1",
                    work_handoff(case.done, case.request_attention),
                    None,
                ),
            ])
            .expect(case.name);
            assert_eq!(state.tasks[&tid("t1")].status, case.expect_status, "{}", case.name);
            match case.expect_kind {
                None => {
                    assert!(state.open_attention.is_empty(), "{}", case.name);
                    assert!(
                        matches!(state.phase, MissionPhase::Done { .. }),
                        "{}",
                        case.name
                    );
                }
                Some(kind) => {
                    assert_eq!(state.open_attention.len(), 1, "{}", case.name);
                    let item = state.open_attention.values().next().expect(case.name);
                    assert_eq!(item.kind, kind, "{}", case.name);
                    assert_eq!(item.task_id, Some(tid("t1")), "{}", case.name);
                    assert_eq!(state.phase, MissionPhase::AttentionNeeded, "{}", case.name);
                }
            }
        }
    }

    #[test]
    fn validate_handoff_always_clears_and_folds_sticky_advisory() {
        struct Case {
            name: &'static str,
            /// (validator task, verdict) applied in order.
            verdicts: &'static [(&'static str, bool)],
            expect_advisory: AdvisoryStatus,
            expect_last: &'static [(&'static str, bool)],
        }
        let cases = [
            Case {
                name: "pass then later fail stays passed, last_advisory records the false",
                verdicts: &[("v1", true), ("v2", false)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", true), ("v2", false)],
            },
            Case {
                name: "same validator flips pass to fail: sticky pass, latest verdict recorded",
                verdicts: &[("v1", true), ("v1", false)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", false)],
            },
            Case {
                name: "fail then pass lands passed",
                verdicts: &[("v1", false), ("v2", true)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", false), ("v2", true)],
            },
            Case {
                name: "single fail lands failed",
                verdicts: &[("v1", false)],
                expect_advisory: AdvisoryStatus::Failed,
                expect_last: &[("v1", false)],
            },
        ];
        for case in cases {
            let mut events = vec![
                created(),
                plan_submitted(
                    vec![assertion("A1", None)],
                    case.verdicts.iter().map(|(v, _)| validate_task(v)).collect(),
                ),
            ];
            for (i, (validator, passed)) in case.verdicts.iter().enumerate() {
                events.push(role_completed(
                    validator,
                    &format!("k{i}"),
                    validate_handoff(&[("A1", *passed)]),
                    None,
                ));
            }
            let state = fold_log(events).expect(case.name);
            let a = &state.contract[&aid("A1")];
            assert_eq!(a.advisory, case.expect_advisory, "{}", case.name);
            let expect_last: BTreeMap<TaskId, bool> = case
                .expect_last
                .iter()
                .map(|(v, passed)| (tid(v), *passed))
                .collect();
            assert_eq!(a.last_advisory, expect_last, "{}", case.name);
            // Validators always clear — even the ones that reported a fail.
            for (validator, _) in case.verdicts {
                assert_eq!(
                    state.tasks[&tid(validator)].status,
                    TaskStatus::Cleared,
                    "{}",
                    case.name
                );
            }
            assert!(state.open_attention.is_empty(), "{}", case.name);
        }
    }

    #[test]
    fn validator_reporting_not_done_still_clears() {
        // "Validators always clear" includes a validator that reports
        // done: false — it ran, its verdicts are data; only *work* tasks
        // fail on not-done. No attention is raised either.
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![assertion("A1", None)], vec![validate_task("v1")]),
            role_completed(
                "v1",
                "k1",
                Handoff::Validate {
                    done: false,
                    report: PayloadRef::inline("ran out of budget"),
                    items: vec![ValidationItem {
                        item_id: aid("A1"),
                        passed: false,
                    }],
                    passed: false,
                    request_attention: false,
                },
                None,
            ),
        ])
        .expect("state");
        assert_eq!(state.tasks[&tid("v1")].status, TaskStatus::Cleared);
        assert_eq!(state.contract[&aid("A1")].advisory, AdvisoryStatus::Failed);
        assert!(state.open_attention.is_empty());
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
    }

    #[test]
    fn role_run_failed_fails_task_and_raises_attention() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![], vec![work_task("t1")]),
            role_requested("t1", "k1"),
            MissionEvent::RoleRunFailed {
                task_id: tid("t1"),
                attempt_no: 1,
                idempotency_key: "k1".into(),
                error_kind: RunErrorKind::Timeout,
                detail: "took too long".into(),
                synthesized: false,
            },
        ])
        .expect("state");
        let task = &state.tasks[&tid("t1")];
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.attempts, 1);
        assert!(state.inflight.is_empty(), "outcome settles the request");
        let item = state.open_attention.values().next().expect("attention item");
        assert_eq!(item.kind, AttentionKind::NodeFailed);
        assert_eq!(item.task_id, Some(tid("t1")));
        assert!(item.report.contains("Timeout"), "{}", item.report);
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn artifact_outcome_moves_current_sha() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![], vec![work_task("t1")]),
            role_completed(
                "t1",
                "k1",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "head1".into(),
                }),
            ),
        ])
        .expect("state");
        assert_eq!(state.current_sha, "head1");
        assert_eq!(state.base_sha, "base", "base is immutable");
    }

    #[test]
    fn oracle_run_completed_mints_verdict_and_settles_inflight() {
        for (exit_code, expect_passed, expect_finish) in [
            (0, true, FinishClass::Verified),
            (1, false, FinishClass::Unverified),
            (101, false, FinishClass::Unverified),
        ] {
            let mut events = vec![
                created(),
                plan_submitted(
                    vec![assertion("TESTS-PASS", Some("cargo-test"))],
                    vec![work_task("t1")],
                ),
                role_completed("t1", "kr", work_handoff(true, false), None),
            ];
            // Task cleared, nothing inflight: the unsettled oracle
            // obligation alone keeps the mission running.
            let owed = fold_log(events.clone()).expect("owed state");
            assert!(owed.inflight.is_empty());
            assert!(!tasks_active(&owed));
            assert_eq!(owed.phase, MissionPhase::Running);

            events.push(oracle_requested("TESTS-PASS", "base", "ko"));
            let mid = fold_log(events.clone()).expect("mid state");
            assert!(mid.inflight.contains_key("ko"), "request is inflight");
            assert_eq!(mid.phase, MissionPhase::Running);

            events.push(oracle_completed("TESTS-PASS", "base", "ko", exit_code));
            let state = fold_log(events).expect("state");
            assert!(state.inflight.is_empty(), "exit {exit_code}");
            assert_eq!(state.oracle_attempts[&oracle("cargo-test")], 1);
            let verdict = state.contract[&aid("TESTS-PASS")]
                .last_authoritative
                .as_ref()
                .expect("verdict minted");
            assert_eq!(verdict.passed(), expect_passed, "exit {exit_code}");
            assert_eq!(verdict.exit_code(), exit_code);
            assert_eq!(verdict.judged_sha(), "base");
            // A fresh verdict — pass or fail — settles the obligation.
            assert_eq!(
                state.phase,
                MissionPhase::Done { finish: expect_finish },
                "exit {exit_code}"
            );
        }
    }

    #[test]
    fn oracle_run_failed_raises_attention_without_verdict() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![assertion("TESTS-PASS", Some("cargo-test"))], vec![]),
            oracle_requested("TESTS-PASS", "base", "ko"),
            MissionEvent::OracleRunFailed {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("cargo-test"),
                judged_sha: "base".into(),
                attempt_no: 1,
                idempotency_key: "ko".into(),
                detail: "spawn failed".into(),
                synthesized: false,
            },
        ])
        .expect("state");
        assert!(state.inflight.is_empty());
        assert!(state.contract[&aid("TESTS-PASS")].last_authoritative.is_none());
        let item = state.open_attention.values().next().expect("attention item");
        assert_eq!(item.kind, AttentionKind::NodeFailed);
        assert_eq!(item.task_id, None, "oracle failures anchor to the engine");
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn mission_aborted_is_sticky() {
        let state = fold_log(vec![
            created(),
            MissionEvent::MissionAborted {
                reason: "operator stop".into(),
                actor: "human".into(),
            },
            plan_submitted(vec![], vec![work_task("t1")]),
            role_completed("t1", "k1", work_handoff(true, false), None),
        ])
        .expect("state");
        assert_eq!(
            state.phase,
            MissionPhase::Aborted {
                reason: "operator stop".into()
            }
        );
        // Later facts still fold; only the phase is pinned.
        assert_eq!(state.tasks[&tid("t1")].status, TaskStatus::Cleared);
        assert_eq!(state.head, 3);
    }

    #[test]
    fn attention_ids_embed_seq_and_are_stable_across_refolds() {
        let events = vec![
            created(),
            plan_submitted(vec![], vec![work_task("t1")]),
            role_completed("t1", "k1", work_handoff(false, false), None), // seq 2
        ];
        let once = fold_log(events.clone()).expect("first fold");
        let twice = fold_log(events).expect("second fold");
        let keys: Vec<&str> = once.open_attention.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["nodefailed:t1:2"]);
        assert_eq!(
            keys,
            twice.open_attention.keys().map(String::as_str).collect::<Vec<_>>()
        );
        assert_eq!(once, twice, "the fold is deterministic");
    }

    #[test]
    fn unknown_ids_in_events_are_tolerated() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![assertion("KNOWN-1", None)], vec![]),
            // Task never declared by any plan.
            role_completed("ghost", "k1", work_handoff(true, false), None),
            // Validator verdict for an assertion the contract never heard of.
            role_completed("phantom", "k2", validate_handoff(&[("UNKNOWN-1", true)]), None),
            MissionEvent::RoleRunFailed {
                task_id: tid("specter"),
                attempt_no: 1,
                idempotency_key: "k3".into(),
                error_kind: RunErrorKind::Infra,
                detail: "gone".into(),
                synthesized: false,
            },
        ])
        .expect("state");
        // No panics and no phantom rows: outcomes only touch declared ids.
        assert!(state.tasks.is_empty());
        assert_eq!(state.contract.len(), 1);
        assert_eq!(state.contract[&aid("KNOWN-1")].advisory, AdvisoryStatus::Pending);
        // The failure still parks the mission even though the task is unknown.
        assert!(state.open_attention.keys().any(|k| k.contains("specter")));
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }
}
