//! Gap review end-to-end: the engine-owned closing check. A clean
//! review closes with zero ceremony; blocking gaps park for a human;
//! remediation re-reviews at the new head; forged or failed handoffs park
//! rather than seal; pre-feature missions are untouched.

mod common;

use lionclaw_runtime_api::TypedFailure;

use std::sync::Mutex;

use common::{
    approve_plan, blocking_gap, fault_append_events, harness_with_type, proposal,
    proposal_with_team, review_mission_type, review_proposal, review_runner, simple_plan,
    test_mission_type, ParseTask, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::MissionView;
use lionclaw::model::{
    BlobRef, Choice, DecisionAction, EffectId, EnvironmentPreflight, FinishClass, Gap, GapSeverity,
    Handoff, MissionEvent, MissionProposal, MissionState, OutputSemantics, PayloadRef,
    RequirementId, RoleAttemptReceipt, RoleEffectSource, RolePromptTemplate, RoleResourceLifetime,
    SettledHandoff, Task, WorkspacePreparation,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::testing::{review_verdict, MockOracleRunner, MockRoleRunner};

const REVIEW_ROLE: &str = "gap-reviewer";
const REVIEW_FAILURE: &str = "proof_failed:review:gap-reviewer";

fn digest(ch: char) -> String {
    ch.to_string().repeat(64)
}

fn environment_event(image_digest: &str, team_revision: Option<u32>) -> MissionEvent {
    MissionEvent::EnvironmentAssigned {
        image_ref: image_digest.to_string(),
        image_id: image_digest.to_string(),
        preflight: EnvironmentPreflight {
            engine: "podman".to_string(),
            image_ref: image_digest.to_string(),
            image_id: image_digest.to_string(),
        },
        team_revision,
        reason: "switch test environment".to_string(),
    }
}

fn parked(view: &MissionView) -> Vec<String> {
    let decisions = common::decision_ids(&view.state);
    assert!(!decisions.is_empty(), "got {view:?}");
    decisions
}

fn review_failure_actions(view: &MissionView, decision_id: &str) -> Vec<DecisionAction> {
    view.next
        .choices
        .iter()
        .filter_map(|choice| match choice {
            Choice::Decide { id, action } if id == decision_id => Some(action.clone()),
            _ => None,
        })
        .collect()
}

fn assert_terminal(view: &MissionView) {
    assert!(view.state.is_terminal(), "got {view:?}");
}

fn gap_review_receipt(state: &MissionState) -> &RoleAttemptReceipt {
    let team = state.team.as_ref().expect("active team");
    let role_instance = team
        .gap_review_assignment
        .as_ref()
        .expect("assigned gap reviewer");
    state
        .role_attempt_receipts
        .values()
        .filter_map(|receipt| {
            let RoleEffectSource::Turn { request, .. } = &receipt.source;
            (request.role_instance == *role_instance
                && request.team_revision == team.revision
                && request.task_id.is_none()
                && request.assertion_ids.is_empty())
            .then_some((request.attempt_no, receipt))
        })
        .max_by_key(|(attempt_no, _)| *attempt_no)
        .map(|(_, receipt)| receipt)
        .expect("fold-authoritative gap-review receipt")
}

fn gap_review_failure(state: &MissionState) -> &TypedFailure {
    gap_review_receipt(state)
        .failure()
        .expect("failed gap-review receipt")
}

fn gap_review_verdict(state: &MissionState) -> (&RoleAttemptReceipt, &str, bool, &[Gap]) {
    let receipt = gap_review_receipt(state);
    let RoleEffectSource::Turn { request, .. } = &receipt.source;
    if request.role_instance.as_str() != REVIEW_ROLE || request.task_id.is_some() {
        panic!("gap-review outcome has non-review provenance")
    };
    let Some(SettledHandoff::Review { passed, gaps }) = receipt.settled_handoff() else {
        panic!("gap-review verdict has no settled review handoff")
    };
    (receipt, request.base_sha.as_str(), *passed, gaps)
}

fn work_outcome(request: &RoleTurnRequest, head_sha: &str) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("WORKER-REPORT-PROBE: everything is definitely finished"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(
            request.base_sha.clone(),
            head_sha,
        )),
        prepared_inputs: Vec::new(),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: String::new(),
    }
}

fn normal_outcome(request: &RoleTurnRequest, head_sha: &str) -> RoleTurnOutcome {
    if request.role.output == OutputSemantics::EmitsVerdict {
        RoleTurnOutcome {
            handoff: Some(Handoff::Validate {
                done: true,
                report: PayloadRef::inline("judged"),
                items: request
                    .assertion_ids
                    .iter()
                    .cloned()
                    .map(|item_id| lionclaw::model::ValidationItem {
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
            final_response: "judged".to_string(),
        }
    } else {
        work_outcome(request, head_sha)
    }
}

fn failing_review_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            Err(TypedFailure::permanent(
                "test.review_failed",
                "establish a retryable review obligation",
            ))
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }))
}

async fn reopen_failed_review(h: &common::TestHarness, mission_id: &lionclaw::model::MissionId) {
    let parked = h.engine.advance(mission_id).await.expect("initial advance");
    assert!(common::has_decision(
        &parked.state,
        "proof_failed:review:gap-reviewer"
    ));
    h.engine
        .decide(
            mission_id,
            "proof_failed:review:gap-reviewer",
            DecisionAction::Retry,
            "retry after the failed review",
        )
        .await
        .expect("retry review");
}

async fn started(
    dir: &tempfile::TempDir,
    runner: MockRoleRunner,
) -> (common::TestHarness, lionclaw::model::MissionId) {
    started_with_recovery(dir, runner, 3).await
}

async fn started_with_recovery(
    dir: &tempfile::TempDir,
    runner: MockRoleRunner,
    max_attempts: u32,
) -> (common::TestHarness, lionclaw::model::MissionId) {
    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.recovery.max_attempts = max_attempts;
    });
    let h = harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the failing test pass",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, review_proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    (h, mission_id)
}

async fn review_calls(
    h: &common::TestHarness,
    mission_id: &lionclaw::model::MissionId,
) -> Vec<(u32, String)> {
    let state = h.engine.load_state(mission_id).await.expect("state");
    let mut calls = state
        .role_attempt_receipts
        .values()
        .filter_map(|receipt| {
            let RoleEffectSource::Turn { request, .. } = &receipt.source;
            (request.role_instance.as_str() == REVIEW_ROLE && request.task_id.is_none())
                .then(|| (request.attempt_no, receipt.effect_id.to_string()))
        })
        .collect::<Vec<_>>();
    calls.sort_by_key(|(attempt_no, _)| *attempt_no);
    calls
}

fn orphaned_gap_review_request(
    mission_id: &lionclaw::model::MissionId,
    state: &MissionState,
    attempt_no: u32,
) -> (EffectId, lionclaw::store::NewEvent) {
    let role_instance = lionclaw::model::RoleInstanceId::new(REVIEW_ROLE).expect("review role");
    let team_revision = state.team.as_ref().expect("team").revision;
    let assignment_epoch = state.revision.max(1);
    let prompt_hash = PayloadRef::inline("crashed gap review")
        .content_sha256()
        .expect("inline prompt hash");
    let effect_id = EffectId::for_role_turn(
        mission_id,
        &role_instance,
        team_revision,
        None,
        attempt_no,
        assignment_epoch,
        &prompt_hash,
    );
    let event = lionclaw::store::NewEvent::new(MissionEvent::RoleTurnRequested {
        role_instance: role_instance.clone(),
        team_revision,
        task_id: None,
        assertion_ids: vec![],
        attempt_no,
        effect_id: effect_id.clone(),
        prompt_template: RolePromptTemplate::GapReview,
        prompt_hash,
        base_sha: state.current_sha.clone(),
        environment_digest: state.environment_digest().to_string(),
        instrument_identity: state
            .role_instrument_identity_for_revision(&role_instance, team_revision)
            .expect("role instrument identity"),
        dependency_refs: vec![],
        assignment_epoch,
        message_boundary: state.head,
        presented_messages: vec![],
        workspace_preparation: WorkspacePreparation::Preserve,
        requested_at_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    });
    (effect_id, event)
}

#[tokio::test]
async fn a_clean_review_closes_verified_with_no_park() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(true, vec![])])).await;

    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);
    assert_eq!(outcome.state.finish(), Some(FinishClass::Verified));
    // Exactly one reviewer run, judged at the final commit, recorded fresh.
    assert_eq!(review_calls(&h, &mission_id).await.len(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (receipt, judged_sha, passed, gaps) = gap_review_verdict(&state);
    assert_eq!(judged_sha, HEAD_SHA);
    assert_eq!(judged_sha, state.current_sha);
    assert!(passed);
    assert!(!gaps.iter().any(|gap| gap.severity == GapSeverity::Blocking));

    let RoleEffectSource::Turn { request, .. } = &receipt.source;
    assert!(request.is_fresh_at(&state));
    let mut changed_environment = state.clone();
    changed_environment.image_id = format!("sha256:{}", "b".repeat(64));
    assert!(!request.is_fresh_at(&changed_environment));
}

#[tokio::test]
async fn gap_review_uses_effect_owned_resources() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(|request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            assert_eq!(
                request.role.output.resource_lifetime(),
                RoleResourceLifetime::Effect
            );
            assert!(request.artifact_capture.is_none());
            Ok(review_verdict(request, true, vec![]))
        } else {
            assert_eq!(
                request.role.output.resource_lifetime(),
                if request.role.output == OutputSemantics::ProducesArtifact {
                    RoleResourceLifetime::Conversation
                } else {
                    RoleResourceLifetime::Effect
                }
            );
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);
}

#[tokio::test]
async fn gap_review_outcomes_bound_alternate_runner_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let oversized = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            let mut outcome = review_verdict(request, true, vec![]);
            outcome.runtime_configuration.applied_model = Some(oversized.clone());
            outcome.final_response = oversized.clone();
            Ok(outcome)
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    h.engine.advance(&mission_id).await.expect("advance");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let events = h.engine.store().load(&mission_id).await.expect("events");
    let success = events
        .iter()
        .find_map(|event| match &event.event {
            MissionEvent::RoleTurnCompleted {
                effect_id,
                outcome: Ok(success),
            } if state
                .role_attempt_receipts
                .get(effect_id)
                .is_some_and(|receipt| {
                    matches!(
                        &receipt.source,
                        RoleEffectSource::Turn { request, .. }
                            if request.role_instance.as_str() == REVIEW_ROLE
                    )
                }) =>
            {
                Some(success)
            }
            _ => None,
        })
        .expect("terminal review success");
    assert!(
        success
            .runtime_configuration
            .applied_model
            .as_ref()
            .expect("applied model")
            .len()
            <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT
    );
    assert!(
        h.engine
            .store()
            .blobs()
            .resolve(&success.final_response)
            .expect("final response")
            .len()
            <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT
    );
}

#[tokio::test]
async fn gap_review_uses_the_shared_role_output_boundary() {
    #[derive(Clone, Copy)]
    enum Fault {
        BlobReport,
        BlobReportWithInvalidGap,
        OversizedReport,
        OversizedGaps,
        Artifact,
    }

    for (fault, expected_code) in [
        (Fault::BlobReport, "handoff.payload_ref"),
        (Fault::BlobReportWithInvalidGap, "handoff.schema"),
        (Fault::OversizedReport, "handoff.report_too_large"),
        (Fault::OversizedGaps, "handoff.schema"),
        (Fault::Artifact, "workspace.capture_authority"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let runner = MockRoleRunner::new(Box::new(move |request| {
            if request.role.id.as_str() != REVIEW_ROLE {
                return Ok(normal_outcome(request, HEAD_SHA));
            }
            let mut outcome = review_verdict(request, true, vec![]);
            match fault {
                Fault::BlobReport => {
                    let Some(Handoff::Review { report, .. }) = &mut outcome.handoff else {
                        unreachable!("review_verdict returns a review handoff")
                    };
                    *report = PayloadRef::Blob(BlobRef {
                        algo: "sha256".into(),
                        hex: "0".repeat(64),
                        len: 1,
                    });
                }
                Fault::BlobReportWithInvalidGap => {
                    let Some(Handoff::Review { report, gaps, .. }) = &mut outcome.handoff else {
                        unreachable!("review_verdict returns a review handoff")
                    };
                    *report = PayloadRef::Blob(BlobRef {
                        algo: "sha256".into(),
                        hex: "0".repeat(64),
                        len: 1,
                    });
                    gaps.push(Gap {
                        id: Some("INVALID".into()),
                        severity: lionclaw::model::GapSeverity::Blocking,
                        requirement: String::new(),
                        expected: "expected".into(),
                        observed: "observed".into(),
                        evidence: "evidence".into(),
                    });
                }
                Fault::OversizedReport => {
                    let Some(Handoff::Review { report, .. }) = &mut outcome.handoff else {
                        unreachable!("review_verdict returns a review handoff")
                    };
                    *report =
                        PayloadRef::inline("x".repeat(lionclaw::model::MAX_ROLE_REPORT_BYTES + 1));
                }
                Fault::OversizedGaps => {
                    let Some(Handoff::Review { gaps, .. }) = &mut outcome.handoff else {
                        unreachable!("review_verdict returns a review handoff")
                    };
                    gaps.push(Gap {
                        id: Some("OVERSIZED".into()),
                        severity: lionclaw::model::GapSeverity::Blocking,
                        requirement: "requirement".into(),
                        expected: "expected".into(),
                        observed: "observed".into(),
                        evidence: "x".repeat(256 * 1024),
                    });
                }
                Fault::Artifact => {
                    outcome.artifact = Some(CapturedArtifact::for_testing(
                        request.base_sha.clone(),
                        HEAD_SHA,
                    ));
                }
            }
            Ok(outcome)
        }));
        let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

        let view = h.engine.advance(&mission_id).await.expect("advance");
        assert!(common::has_decision(
            &view.state,
            "proof_failed:review:gap-reviewer"
        ));
        let failure = gap_review_failure(&view.state);
        assert_eq!(failure.evidence().code.as_deref(), Some(expected_code));
    }
}

#[tokio::test]
async fn the_reviewer_prompt_is_fresh_context_and_contract_blind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let captured = std::sync::Arc::new(Mutex::new(None::<String>));
    let seen = captured.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            *seen.lock().expect("lock") = Some(request.prompt.clone());
            Ok(review_verdict(request, true, vec![]))
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let prompt = captured.lock().expect("lock").clone().expect("captured");
    // The objective and the domain role prose are in.
    assert!(prompt.contains("make the failing test pass"));
    assert!(prompt.contains("You are the gap reviewer of a finished mission"));
    // The plan, the contract, and the workers' words are structurally out.
    assert!(
        !prompt.contains("WORKER-REPORT-PROBE"),
        "worker report leaked"
    );
    assert!(!prompt.contains("cargo test exits 0"), "assertion leaked");
    assert!(
        !prompt.contains("Make the failing test pass."),
        "task body leaked"
    );
}

#[tokio::test]
async fn gap_review_receives_its_declared_skill_packages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let skill_root = dir.path().join("gap-check");
    std::fs::create_dir(&skill_root).expect("skill dir");
    std::fs::write(skill_root.join("SKILL.md"), "gap check").expect("skill file");

    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.skills.insert(
            "gap-check".to_string(),
            lionclaw::mission_type::SkillPackage {
                name: "gap-check".to_string(),
                root: skill_root.clone(),
                description: "gap check".to_string(),
                digest: "0".repeat(64),
            },
        );
        definition
            .default_team
            .roles
            .get_mut(&lionclaw::model::RoleInstanceId::new("gap-reviewer").unwrap())
            .unwrap()
            .skills
            .push("gap-check".to_string());
    });
    let proposed_team = mission_type.default_team.clone();

    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            assert_eq!(request.skills.len(), 1);
            assert_eq!(request.skills[0].name, "gap-check");
            assert_eq!(request.skills[0].root, skill_root);
            Ok(review_verdict(request, true, vec![]))
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let h = harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the failing test pass",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(
            &mission_id,
            proposal_with_team(0, simple_plan(), proposed_team),
        )
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(review_calls(&h, &mission_id).await.len(), 1);
}

#[tokio::test]
async fn blocking_gaps_offer_retry_repair_revise_but_never_accept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(false, vec![blocking_gap()])])).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention.len(), 1);
    let decision_id = REVIEW_FAILURE;
    assert_eq!(attention[0], decision_id);
    let actions = review_failure_actions(&outcome, decision_id);
    assert_eq!(
        actions,
        vec![
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Revise,
        ]
    );
    assert!(!actions.contains(&DecisionAction::Accept));
    assert_eq!(common::finish_choice(&outcome.state), None);
}

#[tokio::test]
async fn repeated_identical_blocking_review_suppresses_retry_and_rejects_forgery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(
        &dir,
        review_runner(vec![
            (false, vec![blocking_gap()]),
            (false, vec![blocking_gap()]),
        ]),
    )
    .await;

    let first = h.engine.advance(&mission_id).await.expect("first review");
    assert!(review_failure_actions(&first, REVIEW_FAILURE).contains(&DecisionAction::Retry));
    h.engine
        .decide(
            &mission_id,
            REVIEW_FAILURE,
            DecisionAction::Retry,
            "one retry for the blocking review",
        )
        .await
        .expect("first retry");

    let second = h.engine.advance(&mission_id).await.expect("second review");
    assert_eq!(
        review_failure_actions(&second, REVIEW_FAILURE),
        [DecisionAction::Repair, DecisionAction::Revise]
    );
    let head = second.state.head;
    assert_eq!(review_calls(&h, &mission_id).await.len(), 2);

    h.engine
        .decide(
            &mission_id,
            REVIEW_FAILURE,
            DecisionAction::Retry,
            "forged identical retry",
        )
        .await
        .expect_err("suppressed retry must not append");
    let unchanged = h.engine.load_state(&mission_id).await.expect("unchanged");
    assert_eq!(unchanged.head, head);
    assert_eq!(review_calls(&h, &mission_id).await.len(), 2);
}

#[tokio::test]
async fn changed_review_report_offers_a_fresh_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let reviews = Mutex::new(0usize);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() != REVIEW_ROLE {
            return Ok(normal_outcome(request, HEAD_SHA));
        }
        let mut seen = reviews.lock().expect("lock");
        *seen += 1;
        let report = format!("blocking review evidence {}", *seen);
        let mut outcome = review_verdict(request, false, vec![blocking_gap()]);
        let Some(Handoff::Review {
            report: handoff_report,
            ..
        }) = &mut outcome.handoff
        else {
            unreachable!("review_verdict returns review handoff")
        };
        *handoff_report = PayloadRef::inline(report.clone());
        outcome.final_response = report;
        Ok(outcome)
    }));
    let (h, mission_id) = started(&dir, runner).await;

    h.engine.advance(&mission_id).await.expect("first review");
    h.engine
        .decide(
            &mission_id,
            REVIEW_FAILURE,
            DecisionAction::Retry,
            "retry for new review evidence",
        )
        .await
        .expect("first retry");
    let second = h.engine.advance(&mission_id).await.expect("second review");

    assert!(review_failure_actions(&second, REVIEW_FAILURE).contains(&DecisionAction::Retry));
}

#[tokio::test]
async fn changed_review_artifact_offers_a_fresh_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let work_heads = Mutex::new(vec![HEAD_SHA.to_string(), "repaired-worker-commit".into()]);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            Ok(review_verdict(request, false, vec![blocking_gap()]))
        } else if request.role.output == OutputSemantics::EmitsVerdict {
            Ok(normal_outcome(request, HEAD_SHA))
        } else {
            let mut heads = work_heads.lock().expect("lock");
            let head = if heads.len() > 1 {
                heads.remove(0)
            } else {
                heads[0].clone()
            };
            Ok(work_outcome(request, &head))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let first = h.engine.advance(&mission_id).await.expect("first review");
    let first_head = first.state.current_sha;
    h.engine
        .decide(
            &mission_id,
            REVIEW_FAILURE,
            DecisionAction::Repair,
            "repair the blocking gap",
        )
        .await
        .expect("repair");
    let second = h.engine.advance(&mission_id).await.expect("second review");

    assert_ne!(second.state.current_sha, first_head);
    assert!(review_failure_actions(&second, REVIEW_FAILURE).contains(&DecisionAction::Retry));
}

#[tokio::test]
async fn changed_review_plan_offers_a_fresh_retry_at_the_same_artifact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(
        &dir,
        review_runner(vec![
            (false, vec![blocking_gap()]),
            (false, vec![blocking_gap()]),
        ]),
    )
    .await;

    let first = h.engine.advance(&mission_id).await.expect("first review");
    let first_head = first.state.current_sha;
    let mut next_plan = simple_plan();
    next_plan.requirements[0].prose = "green tests under the clarified contract".to_string();
    let mut next = review_proposal(1, next_plan);
    next.plan
        .as_mut()
        .expect("plan proposal")
        .requirement_changes = vec![RequirementId::new("GREEN-TESTS").unwrap()];
    h.engine
        .propose_plan(&mission_id, next)
        .await
        .expect("propose revision");
    approve_plan(&h.engine, &mission_id).await;
    let second = h.engine.advance(&mission_id).await.expect("second review");

    assert_eq!(second.state.current_sha, first_head);
    assert!(review_failure_actions(&second, REVIEW_FAILURE).contains(&DecisionAction::Retry));
}

#[tokio::test]
async fn changed_reviewer_identity_offers_a_fresh_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsGapVerdict {
            Ok(review_verdict(request, false, vec![blocking_gap()]))
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    h.engine.advance(&mission_id).await.expect("first review");

    let state = h.engine.load_state(&mission_id).await.expect("state");
    let plan = state.plan.as_ref().expect("plan");
    let mut next_team = common::team(2, Some(plan), true);
    let old_reviewer = lionclaw::model::RoleInstanceId::new(REVIEW_ROLE).unwrap();
    let next_reviewer = lionclaw::model::RoleInstanceId::new("gap-reviewer-v2").unwrap();
    let mut next_role = common::role("gap-reviewer-v2", OutputSemantics::EmitsGapVerdict);
    next_role.runtime = "opencode".to_string();
    next_team.roles.remove(&old_reviewer);
    next_team.roles.insert(next_reviewer.clone(), next_role);
    next_team.gap_review_assignment = Some(next_reviewer.clone());
    h.engine
        .propose_plan(
            &mission_id,
            MissionProposal {
                plan: None,
                team: Some(next_team),
                oracles: None,
            },
        )
        .await
        .expect("propose reviewer replacement");
    approve_plan(&h.engine, &mission_id).await;
    let second = h.engine.advance(&mission_id).await.expect("second review");
    let decision_id = format!("proof_failed:review:{next_reviewer}");

    assert!(review_failure_actions(&second, &decision_id).contains(&DecisionAction::Retry));
}

#[tokio::test]
async fn retried_blocking_review_reopens_after_environment_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let image_b = format!("sha256:{}", digest('b'));
    let (h, mission_id) = started(
        &dir,
        review_runner(vec![
            (false, vec![blocking_gap()]),
            (false, vec![blocking_gap()]),
        ]),
    )
    .await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0], "proof_failed:review:gap-reviewer");
    let image_a = outcome.state.environment_digest().to_string();
    let (first_receipt, _, _, _) = gap_review_verdict(&outcome.state);
    let RoleEffectSource::Turn { request, .. } = &first_receipt.source;
    assert_eq!(request.environment_digest, image_a);

    h.engine
        .decide(
            &mission_id,
            "proof_failed:review:gap-reviewer",
            DecisionAction::Retry,
            "retry under the next environment",
        )
        .await
        .expect("retry gaps");
    let retried = h.engine.load_state(&mission_id).await.expect("retried");

    fault_append_events(
        dir.path(),
        &mission_id,
        retried.head,
        &[lionclaw::store::NewEvent::new(environment_event(
            &image_b,
            retried.team.as_ref().map(|team| team.revision),
        ))],
        10,
    )
    .await;
    let stale = h.engine.load_state(&mission_id).await.expect("stale");
    assert_eq!(stale.environment_digest(), image_b);
    assert_eq!(common::finish_choice(&stale), None);

    let reopened = h
        .engine
        .advance(&mission_id)
        .await
        .expect("re-open gap review");
    let attention = parked(&reopened);
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0], REVIEW_FAILURE);
    assert!(review_failure_actions(&reopened, REVIEW_FAILURE).contains(&DecisionAction::Retry));
    assert_eq!(
        review_calls(&h, &mission_id).await.len(),
        2,
        "environment change must force a fresh gap review"
    );
    let (receipt, judged_sha, _, _) = gap_review_verdict(&reopened.state);
    assert_eq!(judged_sha, HEAD_SHA);
    let RoleEffectSource::Turn { request, .. } = &receipt.source;
    assert_eq!(request.environment_digest, image_b);
}

#[tokio::test]
async fn revising_terminal_gaps_carries_the_review_report_into_planning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(false, vec![blocking_gap()])])).await;

    h.engine.advance(&mission_id).await.expect("advance");
    h.engine
        .decide(
            &mission_id,
            "proof_failed:review:gap-reviewer",
            DecisionAction::Revise,
            "repair the observed behavior",
        )
        .await
        .expect("revise");

    let state = h.engine.load_state(&mission_id).await.expect("state");
    let lionclaw::model::PlanningRefinement::FailureEvidence(feedback) = state
        .planning_input
        .refinement
        .as_ref()
        .expect("planning refinement")
    else {
        panic!("terminal review revise must carry structured failure evidence");
    };
    assert_eq!(feedback.len(), 1);
    let feedback = &feedback[0];
    assert_eq!(feedback.justification, "repair the observed behavior");
    let lionclaw::model::DecisionEvidence::RoleAttempts { effect_ids } = &feedback.evidence else {
        panic!("terminal review feedback must retain exact role receipt identities");
    };
    let effect_id = effect_ids.first().expect("review receipt evidence");
    let details = state
        .role_attempt_receipts
        .get(effect_id)
        .expect("referenced gap-review receipt")
        .accepted_report()
        .expect("accepted gap-review report");
    assert_eq!(
        h.engine.store().blobs().resolve(details).unwrap(),
        "requirement map + observations"
    );
}

#[tokio::test]
async fn a_revision_resumes_work_and_re_reviews_at_the_new_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    // First verdict blocks; the re-review after remediation is clean. The
    // worker commits a NEW head on its second run so the verdict stales.
    let work_heads = Mutex::new(vec![HEAD_SHA.to_string(), "second-worker-commit".into()]);
    let reviews = Mutex::new(0usize);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            let mut seen = reviews.lock().expect("lock");
            *seen += 1;
            if *seen == 1 {
                Ok(review_verdict(request, false, vec![blocking_gap()]))
            } else {
                Ok(review_verdict(request, true, vec![]))
            }
        } else if request.role.output == OutputSemantics::EmitsVerdict {
            Ok(normal_outcome(request, HEAD_SHA))
        } else {
            let mut heads = work_heads.lock().expect("lock");
            let head = if heads.len() > 1 {
                heads.remove(0)
            } else {
                heads[0].clone()
            };
            Ok(work_outcome(request, &head))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(common::has_decision(
        &outcome.state,
        "proof_failed:review:gap-reviewer"
    ));
    let first_head = outcome.state.current_sha.clone();
    let first_review = gap_review_receipt(&outcome.state).effect_id.clone();

    // Remediation is a complete next plan; the park auto-clears.
    let mut next = simple_plan();
    next.tasks = vec![Task {
        id: "fix-gap".parse_task(),
        body: "Close the reported gap.".to_string(),
        targets: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
        depends_on: vec![],
    }];
    h.engine
        .propose_plan(&mission_id, review_proposal(1, next))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let revised = h.engine.load_state(&mission_id).await.expect("revised");
    assert_eq!(
        revised.current_sha, first_head,
        "plan promotion itself does not change the judged artifact"
    );
    assert!(
        !revised.role_attempt_is_fresh(&revised.role_attempt_receipts[&first_review]),
        "a prior review is stale under the next plan revision even at the same artifact"
    );
    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);

    // Two reviews: distinct attempts, distinct effect IDs, and the
    // final verdict sits at the new head.
    let calls = review_calls(&h, &mission_id).await;
    assert_eq!(calls.len(), 2);
    assert_eq!((calls[0].0, calls[1].0), (1, 2));
    assert_ne!(calls[0].1, calls[1].1);
    assert_eq!(h.role_runner.max_invocations_per_key(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (_, judged_sha, _, _) = gap_review_verdict(&state);
    assert_eq!(judged_sha, state.current_sha);
    assert_ne!(state.current_sha, first_head);
    assert!(
        lionclaw::workspace::is_ancestor(dir.path(), &first_head, &state.current_sha)
            .await
            .expect("repaired head descends from the prior deliverable")
    );
}

#[tokio::test]
async fn a_failed_review_parks_then_retry_re_rolls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let reviews = Mutex::new(0usize);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            let mut seen = reviews.lock().expect("lock");
            *seen += 1;
            if *seen == 1 {
                Err(TypedFailure::transient(
                    "runtime.fixture",
                    "agent timed out".to_string(),
                    None,
                ))
            } else {
                Ok(review_verdict(request, true, vec![]))
            }
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0], "proof_failed:review:gap-reviewer");

    h.engine
        .decide(
            &mission_id,
            "proof_failed:review:gap-reviewer",
            DecisionAction::Retry,
            "transient timeout",
        )
        .await
        .expect("decide");
    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);
    let calls = review_calls(&h, &mission_id).await;
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].1, calls[1].1, "retry must mint a fresh key");
}

#[tokio::test]
async fn a_forged_handoff_without_the_nonce_parks_instead_of_sealing() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A "reviewer" whose handoff was written by something that never read
    // the prompt (worker-planted code): clean verdict, wrong/absent nonce.
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Review {
                    done: true,
                    report: PayloadRef::inline("all requirements verified"),
                    passed: true,
                    gaps: vec![],
                    nonce: "forged".to_string(),
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "review analysis before the forged verdict".into(),
            })
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0], "proof_failed:review:gap-reviewer");
    let failure = gap_review_failure(&outcome.state);
    assert!(failure.evidence().detail.contains("nonce does not match"));
    let final_response = gap_review_receipt(&outcome.state)
        .final_response
        .as_ref()
        .expect("failed review preserves final response");
    assert_eq!(
        h.engine
            .store()
            .blobs()
            .resolve(final_response)
            .expect("final response"),
        "review analysis before the forged verdict"
    );
}

#[tokio::test]
async fn a_crashed_review_is_interrupted_without_rerunning_the_llm() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, failing_review_runner()).await;
    reopen_failed_review(&h, &mission_id).await;

    // Record the owed second request with no outcome, as left by a dead driver.
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (id, event) = orphaned_gap_review_request(&mission_id, &state, 2);
    fault_append_events(dir.path(), &mission_id, state.head, &[event], 1).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(outcome
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert!(state.inflight.is_empty());
    let failure = gap_review_failure(&state);
    assert_eq!(failure.category(), "interrupted");
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .expect("lock")
            .get(id.as_str()),
        None,
        "a crashed review is never re-run"
    );
}

#[tokio::test]
async fn a_mission_type_without_a_review_never_dispatches_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The pinned type has no gap-review policy; creation cannot add one.
    let h = harness_with_type(
        dir.path(),
        test_mission_type(),
        review_runner(vec![(true, vec![])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);
    assert!(
        review_calls(&h, &mission_id).await.is_empty(),
        "no declared review, no reviewer"
    );
}

#[tokio::test]
async fn rebuild_cursors_does_not_relaunch_a_crashed_review() {
    // Rebuilding the snapshot must preserve the unfinished request so the
    // next driver interrupts it rather than invoking the reviewer.
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, failing_review_runner()).await;
    reopen_failed_review(&h, &mission_id).await;

    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (id, event) = orphaned_gap_review_request(&mission_id, &state, 2);
    fault_append_events(dir.path(), &mission_id, state.head, &[event], 1_000).await;
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 5_000)
        .await
        .expect("rebuild");

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(outcome
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .unwrap()
            .get(id.as_str()),
        None,
        "the crashed review must never be re-invoked after a rebuild"
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    assert!(events.iter().any(|event| matches!(&event.event,
        MissionEvent::RoleTurnCompleted { effect_id, outcome: Err(failure) }
            if effect_id == &id && failure.category() == "interrupted")));
}

#[tokio::test]
async fn a_attested_bar_mission_type_without_a_review_is_refused_at_creation() {
    // Directly constructed mission types obey the same creation invariant as
    // loaded bundles; there is no caller-owned config that can weaken it.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.stop = lionclaw::model::StopBar::Attested;
        definition.requires_gap_review = true;
        definition.default_team.gap_review_assignment = None;
    });
    let h = harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![(true, vec![])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let err = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect_err("a attested-bar mission without a review must be refused");
    assert!(err.to_string().contains("gap-review"), "got {err}");
}

#[tokio::test]
async fn repairing_a_failed_review_reopens_the_sink_with_receipt_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let work_heads = Mutex::new(vec![HEAD_SHA.to_string(), "second-worker-commit".into()]);
    let reviews = Mutex::new(0usize);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            let mut seen = reviews.lock().expect("lock");
            *seen += 1;
            if *seen == 1 {
                Err(TypedFailure::transient(
                    "runtime.fixture",
                    "agent timed out".to_string(),
                    None,
                ))
            } else {
                Ok(review_verdict(request, true, vec![]))
            }
        } else if request.role.output == OutputSemantics::EmitsVerdict {
            Ok(normal_outcome(request, HEAD_SHA))
        } else {
            let mut heads = work_heads.lock().expect("lock");
            let head = if heads.len() > 1 {
                heads.remove(0)
            } else {
                heads[0].clone()
            };
            Ok(work_outcome(request, &head))
        }
    }));
    let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

    let initial = h.engine.advance(&mission_id).await.expect("advance");
    let initial_head = initial.state.current_sha.clone();
    let review_effect = gap_review_receipt(&initial.state).effect_id.clone();
    h.engine
        .decide(
            &mission_id,
            "proof_failed:review:gap-reviewer",
            DecisionAction::Repair,
            "repair using the failed review evidence",
        )
        .await
        .expect("repair");

    let repaired = h.engine.load_state(&mission_id).await.expect("repaired");
    let sink = &repaired.tasks[&"fix".parse_task()];
    assert_eq!(sink.status, lionclaw::model::TaskStatus::Pending);
    assert_eq!(
        sink.pending_base_sha.as_deref(),
        Some(initial_head.as_str())
    );
    assert!(sink.feedback.iter().any(|feedback| {
        feedback.justification == "repair using the failed review evidence"
            && feedback.evidence.role_attempts().contains(&review_effect)
    }));
    assert!(repaired.role_attempt_receipts.contains_key(&review_effect));

    let outcome = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_terminal(&outcome);
    assert_eq!(
        review_calls(&h, &mission_id).await.len(),
        2,
        "repair must re-review the repaired deliverable"
    );
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (_, judged_sha, _, _) = gap_review_verdict(&state);
    assert_eq!(judged_sha, state.current_sha);
    assert_ne!(state.current_sha, initial_head);
    assert!(
        lionclaw::workspace::is_ancestor(dir.path(), &initial_head, &state.current_sha)
            .await
            .expect("repair descends from the reviewed deliverable")
    );
}

#[tokio::test]
async fn a_mission_type_naming_an_unknown_or_non_verdict_reviewer_is_refused_at_creation() {
    for (role, expected) in [
        ("ghost", "does not name a role instance"),
        ("implementer", "must emit gap verdicts"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mission_type = review_mission_type();
        mission_type.edit_for_testing(|definition| {
            definition.default_team.gap_review_assignment =
                Some(lionclaw::model::RoleInstanceId::new(role).expect("role name"));
        });
        let h = harness_with_type(
            dir.path(),
            mission_type,
            review_runner(vec![(true, vec![])]),
            MockOracleRunner::exiting(0),
        )
        .await;
        let err = h
            .engine
            .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
            .await
            .expect_err("an unresolvable reviewer must be refused");
        assert!(err.to_string().contains(expected), "{role}: got {err}");
    }
}

#[tokio::test]
async fn a_done_false_review_handoff_parks_as_incomplete_not_as_a_verdict() {
    // Regression (QA round 3): done=false means "the review itself did not
    // complete" — an ordinary proof failure, never a sealed verdict and
    // never a gaps park.
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Review {
                    done: false,
                    report: PayloadRef::inline("ran out of context"),
                    passed: false,
                    gaps: vec![],
                    nonce: lionclaw::prompt::handoff_nonce(&request.prompt)
                        .expect("gap-review prompt has a nonce")
                        .to_string(),
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: String::new(),
            })
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0], "proof_failed:review:gap-reviewer");
    let failure = gap_review_failure(&outcome.state);
    assert!(failure.evidence().detail.contains("did not complete"));
}

#[tokio::test]
async fn an_ordinary_validator_handoff_cannot_seal_the_gap_review() {
    // The production parser binds this role to the dedicated review schema.
    // Alternate runners use the same closed handoff boundary.
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.id.as_str() == REVIEW_ROLE {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("looks clean"),
                    items: vec![],
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: String::new(),
            })
        } else {
            Ok(normal_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0], "proof_failed:review:gap-reviewer");
    let failure = gap_review_failure(&outcome.state);
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
}
