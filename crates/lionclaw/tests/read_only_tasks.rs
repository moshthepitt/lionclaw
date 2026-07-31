//! Read-only task producers complete assigned work through report receipts
//! without receiving a writable checkout or minting a synthetic artifact.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{advisory_plan, approve_plan, BASE_SHA};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    Handoff, MissionProposal, OutputSemantics, PayloadRef, PlanProposal, RoleInstanceId, Task,
    TaskAttemptOutcome, TaskId, TaskStatus, ValidationItem,
};
use lionclaw::ports::RoleTurnOutcome;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[tokio::test]
async fn assigned_report_task_clears_without_write_or_artifact_authority() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mission_type = load_mission_type(
        &repo_root().join("mission-types/review"),
        &AuthorityCeiling::default(),
    )
    .expect("review mission type loads");
    let mut plan = advisory_plan();
    plan.tasks[0].body = "Review the target without changing it.".to_string();
    let mut team = mission_type.default_team.clone();
    team.revision = 1;
    team.task_assignments = BTreeMap::from([(
        plan.tasks[0].id.clone(),
        RoleInstanceId::new("investigator").unwrap().into(),
    )]);
    team.judgment_assignments = BTreeMap::from([(
        plan.assertions[0].id.clone(),
        vec![RoleInstanceId::new("reviewer").unwrap()],
    )]);
    let proposal = MissionProposal {
        team: Some(team),
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan,
        }),
        oracles: Some(BTreeMap::new()),
    };
    let saw_report_task = Arc::new(AtomicBool::new(false));
    let saw_bound_report = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&saw_report_task);
    let observed_bound_report = Arc::clone(&saw_bound_report);
    let runner = MockRoleRunner::new(Box::new(move |request| match request.role.output {
        OutputSemantics::ProducesReport => {
            assert!(request.task_id.is_some());
            assert!(!request.role.grants.writes);
            assert!(request.artifact_capture.is_none());
            assert!(request.prompt.contains("## Task"));
            assert!(!request
                .prompt
                .contains("Return a complete next team revision"));
            observed.store(true, Ordering::SeqCst);
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("review findings"),
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "review findings".to_string(),
            })
        }
        OutputSemantics::EmitsVerdict => {
            let digest = PayloadRef::inline("review findings")
                .content_sha256()
                .unwrap();
            assert!(request.prompt.contains("Untrusted report deliverables"));
            assert!(request.prompt.contains("review findings"));
            assert!(request.prompt.contains(&digest));
            observed_bound_report.store(true, Ordering::SeqCst);
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("independently checked"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "independently checked".to_string(),
            })
        }
        output => panic!("unexpected output contract {output:?}"),
    }));
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "Review the target", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal)
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    let view = harness.engine.advance(&mission_id).await.unwrap();
    let judged = harness.engine.advance(&mission_id).await.unwrap();
    let task = view.state.tasks.values().next().expect("review task");
    assert!(saw_report_task.load(Ordering::SeqCst));
    assert!(saw_bound_report.load(Ordering::SeqCst));
    assert_eq!(task.status, TaskStatus::Cleared);
    assert_eq!(task.candidate_sha.as_deref(), Some(BASE_SHA));
    assert!(matches!(
        task.last_outcome,
        Some(TaskAttemptOutcome::Accepted { .. })
    ));
    assert_eq!(view.state.deliverable_head(), BASE_SHA);
    assert!(judged.state.terminal.is_none());

    let events = harness.engine.store().load(&mission_id).await.unwrap();
    let request_index = events
        .iter()
        .position(|envelope| {
            matches!(
                &envelope.event,
                lionclaw::model::MissionEvent::RoleTurnRequested { report_refs, .. }
                    if !report_refs.is_empty()
            )
        })
        .unwrap();
    let mut forged_request = events[request_index].clone();
    let forged_effect = match &mut forged_request.event {
        lionclaw::model::MissionEvent::RoleTurnRequested {
            effect_id,
            report_refs,
            ..
        } => {
            report_refs[0].report_sha256 = "0".repeat(64);
            effect_id.clone()
        }
        _ => unreachable!(),
    };
    let mut forged_state = lionclaw::model::fold(events[..request_index].iter().cloned()).unwrap();
    let receipt_count = forged_state.role_attempt_receipts.len();
    lionclaw::model::apply(&mut forged_state, &forged_request);
    assert!(!forged_state.inflight.contains_key(&forged_effect));
    assert_eq!(forged_state.role_attempt_receipts.len(), receipt_count);

    let assertion_id = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    let mut changed = judged.state.clone();
    let report_ref = changed
        .judgment_report_refs(std::slice::from_ref(&assertion_id))
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let judgment_receipt = changed
        .role_attempt_receipts
        .values()
        .find(|receipt| receipt.effect_id != report_ref.effect_id)
        .unwrap()
        .clone();
    assert!(changed.role_attempt_is_fresh(&judgment_receipt));
    changed
        .role_attempt_receipts
        .get_mut(&report_ref.effect_id)
        .unwrap()
        .handoff = Some(Handoff::Work {
        done: true,
        report: PayloadRef::inline("changed report at the same artifact head"),
        request_attention: false,
    });
    assert!(!changed.role_attempt_is_fresh(&judgment_receipt));
}

#[tokio::test]
async fn bad_report_is_presented_to_the_judge_and_cannot_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mission_type = load_mission_type(
        &repo_root().join("mission-types/review"),
        &AuthorityCeiling::default(),
    )
    .expect("review mission type loads");
    let plan = advisory_plan();
    let mut team = mission_type.default_team.clone();
    team.revision = 1;
    team.task_assignments = BTreeMap::from([(
        plan.tasks[0].id.clone(),
        RoleInstanceId::new("investigator").unwrap().into(),
    )]);
    team.judgment_assignments = BTreeMap::from([(
        plan.assertions[0].id.clone(),
        vec![RoleInstanceId::new("reviewer").unwrap()],
    )]);
    let proposal = MissionProposal {
        team: Some(team),
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan,
        }),
        oracles: Some(BTreeMap::new()),
    };
    let judge_rejected_report = Arc::new(AtomicBool::new(false));
    let observed_rejection = Arc::clone(&judge_rejected_report);
    let runner = MockRoleRunner::new(Box::new(move |request| match request.role.output {
        OutputSemantics::ProducesReport => Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("garbage: no evidence for the assigned assertion"),
                request_attention: false,
            }),
            artifact: None,
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "garbage".to_string(),
        }),
        OutputSemantics::EmitsVerdict => {
            assert!(request
                .prompt
                .contains("garbage: no evidence for the assigned assertion"));
            observed_rejection.store(true, Ordering::SeqCst);
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("the submitted report is unsupported"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: false,
                        })
                        .collect(),
                    passed: false,
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "rejected".to_string(),
            })
        }
        output => panic!("unexpected output contract {output:?}"),
    }));
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "Review the target", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal)
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    harness.engine.advance(&mission_id).await.unwrap();
    let view = harness.engine.advance(&mission_id).await.unwrap();

    assert!(judge_rejected_report.load(Ordering::SeqCst));
    assert!(view.state.terminal.is_none());
    assert!(!view
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, lionclaw::model::Choice::Finish { .. })));
}

#[tokio::test]
async fn report_synthesis_accepts_multiple_compatible_read_only_dependencies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mission_type = load_mission_type(
        &repo_root().join("mission-types/review"),
        &AuthorityCeiling::default(),
    )
    .expect("review mission type loads");
    let mut plan = advisory_plan();
    let target = plan.assertions[0].id.clone();
    let left = TaskId::new("left-report").unwrap();
    let right = TaskId::new("right-report").unwrap();
    let synthesis = TaskId::new("synthesis").unwrap();
    plan.tasks = vec![
        Task {
            id: left.clone(),
            body: "Inspect the left concern.".to_string(),
            targets: Vec::new(),
            depends_on: Vec::new(),
        },
        Task {
            id: right.clone(),
            body: "Inspect the right concern.".to_string(),
            targets: Vec::new(),
            depends_on: Vec::new(),
        },
        Task {
            id: synthesis.clone(),
            body: "Synthesize both reports without changing the repository.".to_string(),
            targets: vec![target.clone()],
            depends_on: vec![left.clone(), right.clone()],
        },
    ];
    let reporter = mission_type
        .default_team
        .roles
        .get(&RoleInstanceId::new("investigator").unwrap())
        .unwrap()
        .clone();
    let mut team = mission_type.default_team.clone();
    team.revision = 1;
    let left_role = RoleInstanceId::new("left-reporter").unwrap();
    let right_role = RoleInstanceId::new("right-reporter").unwrap();
    let synthesis_role = RoleInstanceId::new("synthesizer").unwrap();
    for role_id in [&left_role, &right_role, &synthesis_role] {
        let mut role = reporter.clone();
        role.id = role_id.clone();
        team.roles.insert(role_id.clone(), role);
    }
    team.task_assignments = BTreeMap::from([
        (left.clone(), left_role.into()),
        (right.clone(), right_role.into()),
        (synthesis.clone(), synthesis_role.into()),
    ]);
    team.judgment_assignments =
        BTreeMap::from([(target, vec![RoleInstanceId::new("reviewer").unwrap()])]);
    let proposal = MissionProposal {
        team: Some(team),
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan,
        }),
        oracles: Some(BTreeMap::new()),
    };
    let synthesized = Arc::new(AtomicBool::new(false));
    let observed_synthesis = Arc::clone(&synthesized);
    let runner = MockRoleRunner::new(Box::new(move |request| match request.role.output {
        OutputSemantics::ProducesReport => {
            let task_id = request.task_id.as_ref().unwrap();
            if task_id == &synthesis {
                assert_eq!(request.dependency_refs.len(), 2);
                assert!(request
                    .dependency_refs
                    .iter()
                    .all(|reference| reference.sha == BASE_SHA));
                assert!(request.prompt.contains("report from left-report"));
                assert!(request.prompt.contains("report from right-report"));
                observed_synthesis.store(true, Ordering::SeqCst);
            }
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline(format!("report from {task_id}")),
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: format!("report from {task_id}"),
            })
        }
        OutputSemantics::EmitsVerdict => Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Validate {
                done: true,
                report: PayloadRef::inline("all reports assessed"),
                items: request
                    .assertion_ids
                    .iter()
                    .cloned()
                    .map(|item_id| ValidationItem {
                        item_id,
                        passed: true,
                    })
                    .collect(),
                passed: true,
                request_attention: false,
            }),
            artifact: None,
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "all reports assessed".to_string(),
        }),
        output => panic!("unexpected output contract {output:?}"),
    }));
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "Synthesize reports", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal)
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    for _ in 0..3 {
        harness.engine.advance(&mission_id).await.unwrap();
        if synthesized.load(Ordering::SeqCst) {
            break;
        }
    }

    assert!(synthesized.load(Ordering::SeqCst));
}
