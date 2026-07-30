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
    Handoff, MissionProposal, OutputSemantics, PayloadRef, PlanProposal, RoleInstanceId,
    TaskAttemptOutcome, TaskStatus, ValidationItem,
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
        RoleInstanceId::new("investigator").unwrap(),
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
    let observed = Arc::clone(&saw_report_task);
    let runner = MockRoleRunner::new(Box::new(move |request| match request.role.output {
        OutputSemantics::ProducesReport => {
            assert!(request.task_id.is_some());
            assert!(!request.role.grants.writes);
            assert!(request.artifact_capture.is_none());
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
        OutputSemantics::EmitsVerdict => Ok(RoleTurnOutcome {
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
    let task = view.state.tasks.values().next().expect("review task");
    assert!(saw_report_task.load(Ordering::SeqCst));
    assert_eq!(task.status, TaskStatus::Cleared);
    assert_eq!(task.candidate_sha.as_deref(), Some(BASE_SHA));
    assert!(matches!(
        task.last_outcome,
        Some(TaskAttemptOutcome::Accepted { .. })
    ));
    assert_eq!(view.state.deliverable_head(), BASE_SHA);
}
