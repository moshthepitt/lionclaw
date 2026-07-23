//! Slice 2: a read-only validator produces advisory verdicts alongside (or
//! instead of) an oracle. Advisory verdicts route and rank; they never mark
//! a mission verified — that requires authoritative oracle coverage.

mod common;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use common::{
    advisory_plan, approve_plan, initialize_repository, proposal, review_mission_type, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::model::{
    evaluate_gate, AdvisoryStatus, FinishClass, Handoff, MissionEvent, MissionPhase, MissionState,
    OutputSemantics, PayloadRef, Plan, RoleAttemptDisposition, RoleEffectSource, StopBar, Task,
    TaskId, TaskKind, ValidationItem,
};
use lionclaw::ports::{CapturedArtifact, RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

/// A role-aware mock: verdict roles return a ValidateHandoff, others a work
/// handoff that "commits" HEAD_SHA.
fn role_aware_runner(reviewer_passes: bool) -> MockRoleRunner {
    MockRoleRunner::new(Box::new(move |req: &RoleRunRequest| {
        let outcome = match req.role.output {
            OutputSemantics::EmitsVerdict => RoleRunOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("reviewed"),
                    items: vec![ValidationItem {
                        item_id: lionclaw::model::AssertionId::new("STYLE-OK").unwrap(),
                        passed: reviewer_passes,
                    }],
                    passed: reviewer_passes,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: String::new(),
            },
            OutputSemantics::EmitsGapVerdict => {
                lionclaw::testing::review_verdict(req, true, vec![])
            }
            OutputSemantics::ProducesArtifact => RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrote it"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    req.base_sha.clone(),
                    HEAD_SHA,
                )),
                runtime_configuration: Default::default(),
                final_response: String::new(),
            },
            output => panic!("unexpected output contract {output:?}"),
        };
        Ok(outcome)
    }))
}

fn gated_advisory_plan(validator: &str, gate: &str) -> Plan {
    let mut plan = advisory_plan();
    plan.tasks[1].id = TaskId::new(validator).unwrap();
    plan.tasks.push(Task {
        id: TaskId::new(gate).unwrap(),
        kind: TaskKind::Gate,
        body: String::new(),
        targets: vec![lionclaw::model::AssertionId::new("STYLE-OK").unwrap()],
        role: None,
        depends_on: vec![TaskId::new(validator).unwrap()],
    });
    plan
}

async fn drive(role_runner: MockRoleRunner) -> (MissionState, Vec<lionclaw::model::EventEnvelope>) {
    let dir = tempfile::tempdir().expect("tempdir");
    initialize_repository(dir.path());
    let store = MissionStore::open(dir.path()).await.expect("store");
    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| definition.stop = StopBar::Reviewed);
    let engine = Engine::new(
        store.clone(),
        mission_type,
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(role_runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "advisory-only mission",
            BASE_SHA,
        )
        .await
        .expect("create");
    engine
        .propose_plan(&mission_id, proposal(0, advisory_plan()))
        .await
        .expect("propose");
    approve_plan(&engine, &mission_id).await;
    engine.advance(&mission_id).await.expect("advance");
    let state = engine.load_state(&mission_id).await.expect("state");
    let events = store.load(&mission_id).await.expect("events");
    (state, events)
}

async fn run(reviewer_passes: bool) -> (MissionPhase, AdvisoryStatus) {
    let (state, _) = drive(role_aware_runner(reviewer_passes)).await;
    let advisory = state.advisory_status(&lionclaw::model::AssertionId::new("STYLE-OK").unwrap());
    (state.phase, advisory)
}

#[tokio::test]
async fn advisory_pass_is_internally_consistent_never_verified() {
    let (phase, advisory) = run(true).await;
    assert_eq!(advisory, AdvisoryStatus::Passed);
    // All-green advisory with no authoritative coverage: internally
    // consistent, NOT verified.
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::InternallyConsistent
        }
    );
}

#[tokio::test]
async fn advisory_fail_is_unverified() {
    let (phase, advisory) = run(false).await;
    assert_eq!(advisory, AdvisoryStatus::Failed);
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::Unverified
        }
    );
}

#[tokio::test]
async fn read_only_validator_artifacts_are_rejected_before_the_fold() {
    let runner = MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
        let (handoff, artifact) = match req.role.output {
            OutputSemantics::ProducesArtifact => (
                Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrote it"),
                    request_attention: false,
                },
                Some(CapturedArtifact::for_testing(
                    req.base_sha.clone(),
                    HEAD_SHA,
                )),
            ),
            OutputSemantics::EmitsVerdict => (
                Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("reviewed"),
                    items: vec![ValidationItem {
                        item_id: lionclaw::model::AssertionId::new("STYLE-OK").unwrap(),
                        passed: true,
                    }],
                    passed: true,
                    request_attention: false,
                },
                Some(CapturedArtifact::for_testing(
                    req.base_sha.clone(),
                    "forged-validator-head",
                )),
            ),
            output => panic!("unexpected output contract {output:?}"),
        };
        Ok(RoleRunOutcome {
            handoff: Some(handoff),
            artifact,
            runtime_configuration: Default::default(),
            final_response: String::new(),
        })
    }));

    let (state, events) = drive(runner).await;

    assert_eq!(state.current_sha, HEAD_SHA);
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert!(events.iter().any(|event| {
        matches!(
            &event.event,
            MissionEvent::RoleRunCompleted { outcome: Err(failure), .. }
                if failure.evidence().code.as_deref() == Some("workspace.capture_authority")
        )
    }));
}

#[tokio::test]
async fn replacement_validator_requires_new_receipt_and_retains_prior_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    initialize_repository(dir.path());
    let store = MissionStore::open(dir.path()).await.expect("store");
    let validator_attempt = Arc::new(AtomicUsize::new(0));
    let runner = MockRoleRunner::new(Box::new({
        let validator_attempt = validator_attempt.clone();
        move |req: &RoleRunRequest| {
            let outcome = match req.role.output {
                OutputSemantics::ProducesArtifact => RoleRunOutcome {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report: PayloadRef::inline("wrote it"),
                        request_attention: false,
                    }),
                    artifact: Some(CapturedArtifact::for_testing(
                        req.base_sha.clone(),
                        HEAD_SHA,
                    )),
                    runtime_configuration: Default::default(),
                    final_response: String::new(),
                },
                OutputSemantics::EmitsVerdict => {
                    let passed = validator_attempt.fetch_add(1, Ordering::SeqCst) == 0;
                    RoleRunOutcome {
                        handoff: Some(Handoff::Validate {
                            done: true,
                            report: PayloadRef::inline(if passed {
                                "original validator passed"
                            } else {
                                "replacement validator failed"
                            }),
                            items: vec![ValidationItem {
                                item_id: lionclaw::model::AssertionId::new("STYLE-OK").unwrap(),
                                passed,
                            }],
                            passed,
                            request_attention: false,
                        }),
                        artifact: None,
                        runtime_configuration: Default::default(),
                        final_response: String::new(),
                    }
                }
                output => panic!("unexpected output contract {output:?}"),
            };
            Ok(outcome)
        }
    }));
    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| definition.stop = StopBar::Reviewed);
    let engine = Engine::new(
        store.clone(),
        mission_type,
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "validator replacement",
            BASE_SHA,
        )
        .await
        .expect("create");
    let first_plan = gated_advisory_plan("review", "gate");
    engine
        .propose_plan(&mission_id, proposal(0, first_plan))
        .await
        .expect("propose original plan");
    approve_plan(&engine, &mission_id).await;
    engine
        .advance(&mission_id)
        .await
        .expect("run original plan");

    let original = engine
        .load_state(&mission_id)
        .await
        .expect("original state");
    let assertion_id = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    let original_validator = TaskId::new("review").unwrap();
    let original_effect =
        original.contract[&assertion_id].last_advisory[&original_validator].clone();
    let original_receipt = original.role_attempt_receipts[&original_effect].clone();
    assert!(matches!(
        original_receipt.disposition,
        RoleAttemptDisposition::Succeeded { .. }
    ));

    let replacement_plan = gated_advisory_plan("review-v2", "gate-v2");
    engine
        .propose_plan(&mission_id, proposal(1, replacement_plan))
        .await
        .expect("propose replacement plan");
    approve_plan(&engine, &mission_id).await;

    let pending = engine
        .load_state(&mission_id)
        .await
        .expect("replacement pending");
    assert_eq!(
        pending.role_attempt_receipts.get(&original_effect),
        Some(&original_receipt)
    );
    assert_eq!(
        pending.advisory_status(&assertion_id),
        AdvisoryStatus::Pending
    );
    assert!(matches!(
        evaluate_gate(
            &pending,
            pending.plan.as_ref().unwrap(),
            &TaskId::new("gate-v2").unwrap()
        ),
        lionclaw::model::GateResult::Blocked { .. }
    ));

    let json =
        lionclaw::evidence::role_attempt_receipt_json(store.blobs(), &pending, &original_receipt);
    assert_eq!(json["authority"], "current");
    assert_eq!(json["generation"], "superseded");
    assert_eq!(json["handoff"]["content"], "original validator passed");
    let human =
        lionclaw::evidence::render_role_attempt_receipt(store.blobs(), &pending, &original_receipt);
    assert!(human.contains("authority: current"));
    assert!(human.contains("generation: superseded"));
    assert!(human.contains("original validator passed"));

    engine
        .advance(&mission_id)
        .await
        .expect("run replacement validator");
    let settled = engine.load_state(&mission_id).await.expect("settled state");
    let replacement_validator = TaskId::new("review-v2").unwrap();
    let replacement_effect =
        settled.contract[&assertion_id].last_advisory[&replacement_validator].clone();
    assert_ne!(replacement_effect, original_effect);
    assert_eq!(
        settled.role_attempt_receipts.get(&original_effect),
        Some(&original_receipt)
    );
    let replacement_receipt = &settled.role_attempt_receipts[&replacement_effect];
    assert!(matches!(
        &replacement_receipt.source,
        RoleEffectSource::Task {
            plan_revision: 2,
            ..
        }
    ));
    assert_eq!(
        settled.advisory_status(&assertion_id),
        AdvisoryStatus::Failed
    );
    assert!(matches!(
        evaluate_gate(
            &settled,
            settled.plan.as_ref().unwrap(),
            &TaskId::new("gate-v2").unwrap()
        ),
        lionclaw::model::GateResult::Blocked { .. }
    ));
    assert_eq!(
        lionclaw::model::fold(store.load(&mission_id).await.unwrap()).unwrap(),
        settled
    );
}
