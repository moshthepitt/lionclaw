//! Terminal review end-to-end: the engine-owned closing check. A clean
//! review closes with zero ceremony; blocking gaps park for a human;
//! remediation re-reviews at the new head; forged or failed handoffs park
//! rather than seal; pre-feature missions are untouched.

mod common;

use lionclaw_runtime_api::TypedFailure;

use std::sync::Mutex;

use common::{
    approve_plan, blocking_gap, fault_append_events, harness_with_type, proposal,
    review_mission_type, review_runner, simple_plan, test_mission_type, ParseTask, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::engine::{MissionDisposition, MissionView, TERMINAL_REVIEW_TASK_TAG as REVIEW_TAG};
use lionclaw::model::{
    BlobRef, DecisionAction, FinishClass, Gap, GapSeverity, Handoff, MissionEvent, MissionPhase,
    MissionState, PayloadRef, ReviewAcceptanceKind, RoleAttemptReceipt, RoleEffectSource,
    RoleResourceLifetime, SettledHandoff, Task, TaskKind,
};
use lionclaw::ports::{CapturedArtifact, RoleRunOutcome, RoleRunRequest};
use lionclaw::testing::{review_verdict, MockOracleRunner, MockRoleRunner};

fn parked(view: &MissionView) -> Vec<&lionclaw::model::AttentionItem> {
    assert_eq!(view.disposition, MissionDisposition::Parked, "got {view:?}");
    view.state.open_attention.values().collect()
}

fn assert_terminal(view: &MissionView) {
    assert_eq!(
        view.disposition,
        MissionDisposition::Terminal,
        "got {view:?}"
    );
}

fn terminal_review_receipt(state: &MissionState) -> &RoleAttemptReceipt {
    state
        .terminal_review_receipt()
        .expect("fold-authoritative terminal-review receipt")
}

fn terminal_review_failure(state: &MissionState) -> &TypedFailure {
    terminal_review_receipt(state)
        .failure()
        .expect("failed terminal-review receipt")
}

fn terminal_review_verdict(state: &MissionState) -> (&RoleAttemptReceipt, &str, bool, &[Gap]) {
    let receipt = terminal_review_receipt(state);
    let RoleEffectSource::TerminalReview { judged_sha, .. } = &receipt.source else {
        panic!("terminal-review outcome has task provenance")
    };
    let Some(SettledHandoff::Review { passed, gaps }) = receipt.settled_handoff() else {
        panic!("terminal-review verdict has no settled review handoff")
    };
    (receipt, judged_sha, *passed, gaps)
}

fn work_outcome(request: &RoleRunRequest, head_sha: &str) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("WORKER-REPORT-PROBE: everything is definitely finished"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(
            request.base_sha.clone(),
            head_sha,
        )),
        runtime_configuration: Default::default(),
        final_response: String::new(),
    }
}

fn failing_review_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|request| {
        if request.task_id.as_str() == REVIEW_TAG {
            Err(TypedFailure::permanent(
                "test.review_failed",
                "establish a retryable review obligation",
            ))
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }))
}

async fn reopen_failed_review(h: &common::TestHarness, mission_id: &lionclaw::model::MissionId) {
    let parked = h.engine.advance(mission_id).await.expect("initial advance");
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    h.engine
        .decide(
            mission_id,
            "terminal_review_failed:mission",
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
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    (h, mission_id)
}

fn review_calls(h: &common::TestHarness) -> Vec<(u32, String)> {
    h.role_runner
        .calls
        .lock()
        .expect("lock")
        .iter()
        .filter(|(task, ..)| task.as_str() == REVIEW_TAG)
        .map(|(_, attempt, key)| (*attempt, key.clone()))
        .collect()
}

#[tokio::test]
async fn a_clean_review_closes_verified_with_no_park() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(true, vec![])])).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_terminal(&outcome);
    assert!(matches!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    ));
    // Exactly one reviewer run, judged at the final commit, recorded fresh.
    assert_eq!(review_calls(&h).len(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (_, judged_sha, passed, gaps) = terminal_review_verdict(&state);
    assert_eq!(judged_sha, HEAD_SHA);
    assert_eq!(judged_sha, state.current_sha);
    assert!(passed);
    assert!(!gaps.iter().any(|gap| gap.severity == GapSeverity::Blocking));
}

#[tokio::test]
async fn terminal_review_uses_effect_owned_resources() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(|request| {
        if request.task_id.as_str() == REVIEW_TAG {
            assert_eq!(
                request.role.output.resource_lifetime(),
                RoleResourceLifetime::Effect
            );
            assert!(request.artifact_capture.is_none());
            Ok(review_verdict(request, true, vec![]))
        } else {
            assert_eq!(
                request.role.output.resource_lifetime(),
                RoleResourceLifetime::Conversation
            );
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_terminal(&outcome);
}

#[tokio::test]
async fn terminal_review_outcomes_bound_alternate_runner_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let oversized = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            let mut outcome = review_verdict(request, true, vec![]);
            outcome.runtime_configuration.applied_model = Some(oversized.clone());
            outcome.final_response = oversized.clone();
            Ok(outcome)
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    h.engine.advance(&mission_id).await.expect("advance");
    let events = h.engine.store().load(&mission_id).await.expect("events");
    let success = events
        .iter()
        .find_map(|event| match &event.event {
            MissionEvent::TerminalReviewCompleted {
                outcome: Ok(success),
                ..
            } => Some(success),
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
async fn terminal_review_uses_the_shared_role_output_boundary() {
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
            if request.task_id.as_str() != REVIEW_TAG {
                return Ok(work_outcome(request, HEAD_SHA));
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
        assert_eq!(view.disposition, MissionDisposition::Parked);
        let failure = terminal_review_failure(&view.state);
        assert_eq!(failure.evidence().code.as_deref(), Some(expected_code));
    }
}

#[tokio::test]
async fn the_reviewer_prompt_is_fresh_context_and_contract_blind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let captured = std::sync::Arc::new(Mutex::new(None::<String>));
    let seen = captured.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            *seen.lock().expect("lock") = Some(request.prompt.clone());
            Ok(review_verdict(request, true, vec![]))
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let prompt = captured.lock().expect("lock").clone().expect("captured");
    // The objective and the domain role prose are in.
    assert!(prompt.contains("make the failing test pass"));
    assert!(prompt.contains("Hunt product gaps against the objective."));
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
async fn terminal_review_receives_its_declared_skill_packages() {
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
            },
        );
        definition
            .roles
            .get_mut(&lionclaw::model::RoleName::new("gap-reviewer").unwrap())
            .unwrap()
            .skills
            .push("gap-check".to_string());
    });

    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            assert_eq!(request.skills.len(), 1);
            assert_eq!(request.skills[0].name, "gap-check");
            assert_eq!(request.skills[0].root, skill_root);
            Ok(review_verdict(request, true, vec![]))
        } else {
            Ok(work_outcome(request, HEAD_SHA))
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
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(review_calls(&h).len(), 1);
}

#[tokio::test]
async fn blocking_gaps_park_then_accept_closes_with_acknowledged_gaps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(false, vec![blocking_gap()])])).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "terminal_review_gaps:mission");

    h.engine
        .decide(
            &mission_id,
            "terminal_review_gaps:mission",
            DecisionAction::Accept,
            "gap is acceptable for this release",
        )
        .await
        .expect("decide");
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert_terminal(&outcome);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let accepted = state.terminal_review.accepted.expect("acceptance recorded");
    assert_eq!(accepted.kind, ReviewAcceptanceKind::AcknowledgedGaps);
    assert_eq!(accepted.judged_sha, HEAD_SHA);
    // The receipt preserves the exact reason without claiming a caller actor.
    assert_eq!(accepted.justification, "gap is acceptable for this release");
}

#[tokio::test]
async fn revising_terminal_gaps_carries_the_review_report_into_planning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(false, vec![blocking_gap()])])).await;

    h.engine.advance(&mission_id).await.expect("advance");
    h.engine
        .decide(
            &mission_id,
            "terminal_review_gaps:mission",
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
    assert_eq!(feedback.justification, "repair the observed behavior");
    let lionclaw::model::DecisionEvidence::RoleAttempts { effect_ids } = &feedback.evidence else {
        panic!("terminal review feedback must retain exact role receipt identities");
    };
    let effect_id = effect_ids.first().expect("review receipt evidence");
    let details = state
        .role_attempt_receipts
        .get(effect_id)
        .expect("referenced terminal-review receipt")
        .accepted_report()
        .expect("accepted terminal-review report");
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
        if request.task_id.as_str() == REVIEW_TAG {
            let mut seen = reviews.lock().expect("lock");
            *seen += 1;
            if *seen == 1 {
                Ok(review_verdict(request, false, vec![blocking_gap()]))
            } else {
                Ok(review_verdict(request, true, vec![]))
            }
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
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    let first_head = outcome.state.current_sha.clone();

    // Remediation is a complete next plan; the park auto-clears.
    let mut next = simple_plan();
    next.tasks = vec![Task {
        id: "fix-gap".parse_task(),
        kind: TaskKind::Work,
        body: "Close the reported gap.".to_string(),
        targets: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
        role: Some(lionclaw::model::RoleName::new("implementer").expect("role")),
        depends_on: vec![],
    }];
    h.engine
        .propose_plan(&mission_id, proposal(1, next))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert_terminal(&outcome);

    // Two reviews: distinct attempts, distinct effect IDs, and the
    // final verdict sits at the new head.
    let calls = review_calls(&h);
    assert_eq!(calls.len(), 2);
    assert_eq!((calls[0].0, calls[1].0), (1, 2));
    assert_ne!(calls[0].1, calls[1].1);
    assert_eq!(h.role_runner.max_invocations_per_key(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (_, judged_sha, _, _) = terminal_review_verdict(&state);
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
        if request.task_id.as_str() == REVIEW_TAG {
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
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0].id, "terminal_review_failed:mission");

    h.engine
        .decide(
            &mission_id,
            "terminal_review_failed:mission",
            DecisionAction::Retry,
            "transient timeout",
        )
        .await
        .expect("decide");
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert_terminal(&outcome);
    let calls = review_calls(&h);
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].1, calls[1].1, "retry must mint a fresh key");
}

#[tokio::test]
async fn a_forged_handoff_without_the_nonce_parks_instead_of_sealing() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A "reviewer" whose handoff was written by something that never read
    // the prompt (worker-planted code): clean verdict, wrong/absent nonce.
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Review {
                    done: true,
                    report: PayloadRef::inline("all requirements verified"),
                    passed: true,
                    gaps: vec![],
                    nonce: "forged".to_string(),
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "review analysis before the forged verdict".into(),
            })
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started_with_recovery(&dir, runner, 1).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0].id, "terminal_review_failed:mission");
    assert!(attention[0].report.contains("nonce mismatch"));
    let failure = terminal_review_failure(&outcome.state);
    assert_eq!(
        failure.evidence().final_response,
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
    let judged_sha = state.current_sha.clone();
    let id = lionclaw::model::EffectId::for_terminal_review_request(&mission_id, &judged_sha, 2);
    let event = lionclaw::store::NewEvent::new(MissionEvent::TerminalReviewRequested {
        attempt_no: 2,
        effect_id: id.clone(),
        role: lionclaw::model::RoleName::new("gap-reviewer").expect("role"),
        runtime: "codex".to_string(),
        prompt: PayloadRef::inline("prompt"),
        judged_sha,
        nonce: "n0".to_string(),
        requested_at_ms: 0,
        not_before_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    });
    fault_append_events(dir.path(), &mission_id, state.head, &[event], 1).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert!(state.inflight.is_empty());
    let failure = terminal_review_failure(&state);
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
    // The pinned type has no terminal-review policy; creation cannot add one.
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
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_terminal(&outcome);
    assert!(
        review_calls(&h).is_empty(),
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
    let judged_sha = state.current_sha.clone();
    let id = lionclaw::model::EffectId::for_terminal_review_request(&mission_id, &judged_sha, 2);
    let event = lionclaw::store::NewEvent::new(MissionEvent::TerminalReviewRequested {
        attempt_no: 2,
        effect_id: id.clone(),
        role: lionclaw::model::RoleName::new("gap-reviewer").expect("role"),
        runtime: "codex".to_string(),
        prompt: PayloadRef::inline("p"),
        judged_sha,
        nonce: "n0".to_string(),
        requested_at_ms: 0,
        not_before_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    });
    fault_append_events(dir.path(), &mission_id, state.head, &[event], 1_000).await;
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 5_000)
        .await
        .expect("rebuild");

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
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
        MissionEvent::TerminalReviewCompleted { effect_id, outcome: Err(failure), .. }
            if effect_id == &id && failure.category() == "interrupted")));
}

#[tokio::test]
async fn a_reviewed_bar_mission_type_without_a_review_is_refused_at_creation() {
    // Directly constructed mission types obey the same creation invariant as
    // loaded bundles; there is no caller-owned config that can weaken it.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = review_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.stop = lionclaw::model::StopBar::Reviewed;
        definition.terminal_review = None;
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
        .expect_err("a reviewed-bar mission without a review must be refused");
    assert!(err.to_string().contains("terminal review"), "got {err}");
}

#[tokio::test]
async fn a_stale_waiver_reopens_the_review_after_new_work() {
    // Regression (QA round 1): a waiver is granted at a head, never
    // inherited. The live sequence: park on a review failure, propose
    // remediation in WHILE parked, waive the failure — the revised work then
    // moves the head, the waiver goes stale, and the review re-dispatches at
    // the new head instead of the mission closing reviewless.
    let dir = tempfile::tempdir().expect("tempdir");
    let work_heads = Mutex::new(vec![HEAD_SHA.to_string(), "second-worker-commit".into()]);
    let reviews = Mutex::new(0usize);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
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

    // Park on the review failure; propose follow-up work while parked.
    let initial = h.engine.advance(&mission_id).await.expect("advance");
    let waived_head = initial.state.current_sha.clone();
    let mut next = simple_plan();
    next.tasks = vec![Task {
        id: "more".parse_task(),
        kind: TaskKind::Work,
        body: "Follow-up work proposed while parked.".to_string(),
        targets: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
        role: Some(lionclaw::model::RoleName::new("implementer").expect("role")),
        depends_on: vec![],
    }];
    h.engine
        .propose_plan(&mission_id, proposal(1, next))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    // Waive the failure at the CURRENT head; the pending work resumes.
    h.engine
        .decide(
            &mission_id,
            "terminal_review_failed:mission",
            DecisionAction::Accept,
            "reviewer infra is down today",
        )
        .await
        .expect("waive");

    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert_terminal(&outcome);
    // The revised work moved the head, staling the waiver: a second review
    // ran at the new head and its verdict is on record.
    assert_eq!(review_calls(&h).len(), 2, "the stale waiver must re-review");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let (_, judged_sha, _, _) = terminal_review_verdict(&state);
    assert_eq!(judged_sha, state.current_sha);
    assert_ne!(state.current_sha, waived_head);
    assert!(
        lionclaw::workspace::is_ancestor(dir.path(), &waived_head, &state.current_sha)
            .await
            .expect("post-waiver work descends from the waived deliverable")
    );
}

#[tokio::test]
async fn a_mission_type_naming_an_unknown_or_non_verdict_reviewer_is_refused_at_creation() {
    for (role, expected) in [
        ("ghost", "is not provided"),
        ("implementer", "must be emits-gap-verdict"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mission_type = review_mission_type();
        mission_type.edit_for_testing(|definition| {
            definition.terminal_review = Some(lionclaw::model::TerminalReviewConfig {
                role: lionclaw::model::RoleName::new(role).expect("role name"),
            });
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
    // complete" — an infra park to retry/waive, never a sealed verdict and
    // never a gaps park.
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Review {
                    done: false,
                    report: PayloadRef::inline("ran out of context"),
                    passed: false,
                    gaps: vec![],
                    nonce: lionclaw::prompt::handoff_nonce(&request.prompt)
                        .expect("terminal-review prompt has a nonce")
                        .to_string(),
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: String::new(),
            })
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0].id, "terminal_review_failed:mission");
    assert!(attention[0].report.contains("did not complete"));
}

#[tokio::test]
async fn an_ordinary_validator_handoff_cannot_seal_the_terminal_review() {
    // The production parser binds this role to the dedicated review schema.
    // Alternate runners use the same closed handoff boundary.
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == REVIEW_TAG {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("looks clean"),
                    items: vec![],
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: String::new(),
            })
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let attention = parked(&outcome);
    assert_eq!(attention[0].id, "terminal_review_failed:mission");
    let failure = terminal_review_failure(&outcome.state);
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
}
