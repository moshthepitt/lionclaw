//! Terminal review end-to-end: the engine-owned closing check. A clean
//! review closes with zero ceremony; blocking gaps park for a human;
//! remediation re-reviews at the new head; forged or failed handoffs park
//! rather than seal; pre-feature missions are untouched.

mod common;

use std::sync::Mutex;

use common::{
    blocking_gap, default_config, harness_with_type, review_config, review_mission_type,
    review_runner, simple_plan, ParseTask, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::AdvanceOutcome;
use lionclaw::model::{
    ArtifactOutcome, DecisionAction, FinishClass, Handoff, MissionEvent, MissionPhase, PayloadRef,
    ReviewAcceptance, ReviewOutcome, Task, TaskKind,
};
use lionclaw::ports::{RoleRunFailure, RoleRunOutcome, RoleRunRequest};
use lionclaw::testing::{review_verdict, MockOracleRunner, MockRoleRunner};

const REVIEW_TAG: &str = "terminal-review";

fn work_outcome(request: &RoleRunRequest, head_sha: &str) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Handoff::Work {
            done: true,
            report: PayloadRef::inline("WORKER-REPORT-PROBE: everything is definitely finished"),
            request_attention: false,
        },
        artifact: Some(ArtifactOutcome {
            base_sha: request.base_sha.clone(),
            head_sha: head_sha.to_string(),
        }),
        model_id: None,
    }
}

async fn started(
    dir: &tempfile::TempDir,
    runner: MockRoleRunner,
) -> (common::TestHarness, lionclaw::model::MissionId) {
    let h = harness_with_type(
        dir.path(),
        review_mission_type(),
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
            review_config(),
        )
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
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
    assert!(
        matches!(
            &outcome,
            AdvanceOutcome::Terminal {
                phase: MissionPhase::Done {
                    finish: FinishClass::Verified
                }
            }
        ),
        "got {outcome:?}"
    );
    // Exactly one reviewer run, judged at the final commit, recorded fresh.
    assert_eq!(review_calls(&h).len(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let Some(ReviewOutcome::Verdict(v)) = &state.terminal_review.outcome else {
        panic!("verdict recorded");
    };
    assert_eq!(v.judged_sha, HEAD_SHA);
    assert!(v.is_fresh_at(&state.current_sha));
    assert!(!v.blocking());
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
async fn blocking_gaps_park_then_continue_closes_with_acknowledged_gaps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(false, vec![blocking_gap()])])).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = outcome else {
        panic!("expected a park, got {outcome:?}");
    };
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "terminal_review_gaps:mission");

    h.engine
        .decide(
            &mission_id,
            "terminal_review_gaps:mission",
            DecisionAction::Continue,
            "gap is acceptable for this release",
            "test",
        )
        .await
        .expect("decide");
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert!(matches!(outcome, AdvanceOutcome::Terminal { .. }));
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(
        state.terminal_review.accepted,
        Some(ReviewAcceptance::AcknowledgedGaps {
            judged_sha: HEAD_SHA.to_string()
        })
    );
}

#[tokio::test]
async fn an_amendment_resumes_work_and_re_reviews_at_the_new_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    // First verdict blocks; the re-review after remediation is clean. The
    // worker commits a NEW head on its second run so the verdict stales.
    let work_heads = Mutex::new(vec![HEAD_SHA.to_string(), format!("{:040}", 3)]);
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
    assert!(matches!(outcome, AdvanceOutcome::Parked { .. }));

    // Remediation is an amendment; the park auto-clears (no second decision).
    h.engine
        .amend_plan(
            &mission_id,
            lionclaw::model::AmendmentOps {
                add: vec![Task {
                    id: "fix-gap".parse_task(),
                    kind: TaskKind::Work,
                    body: "Close the reported gap.".to_string(),
                    targets: vec![],
                    role: Some(lionclaw::model::RoleName::new("implementer").expect("role")),
                    depends_on: vec![],
                }],
                ..Default::default()
            },
            "test",
            "close the review gap",
            1,
        )
        .await
        .expect("amend");
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert!(
        matches!(outcome, AdvanceOutcome::Terminal { .. }),
        "got {outcome:?}"
    );

    // Two reviews: distinct attempts, distinct idempotency keys, and the
    // final verdict sits at the new head.
    let calls = review_calls(&h);
    assert_eq!(calls.len(), 2);
    assert_eq!((calls[0].0, calls[1].0), (1, 2));
    assert_ne!(calls[0].1, calls[1].1);
    assert_eq!(h.role_runner.max_invocations_per_key(), 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let Some(ReviewOutcome::Verdict(v)) = &state.terminal_review.outcome else {
        panic!("verdict recorded");
    };
    assert_eq!(v.judged_sha, format!("{:040}", 3));
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
                Err(RoleRunFailure {
                    kind: lionclaw::model::RunErrorKind::Timeout,
                    detail: "agent timed out".to_string(),
                })
            } else {
                Ok(review_verdict(request, true, vec![]))
            }
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = outcome else {
        panic!("expected a park, got {outcome:?}");
    };
    assert_eq!(attention[0].id, "terminal_review_failed:mission");

    h.engine
        .decide(
            &mission_id,
            "terminal_review_failed:mission",
            DecisionAction::Retry,
            "transient timeout",
            "test",
        )
        .await
        .expect("decide");
    let outcome = h.engine.advance(&mission_id).await.expect("re-advance");
    assert!(matches!(outcome, AdvanceOutcome::Terminal { .. }));
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
                handoff: Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("all requirements verified"),
                    items: vec![],
                    passed: true,
                    request_attention: false,
                    gaps: vec![],
                    nonce: Some("forged".to_string()),
                },
                artifact: None,
                model_id: None,
            })
        } else {
            Ok(work_outcome(request, HEAD_SHA))
        }
    }));
    let (h, mission_id) = started(&dir, runner).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = outcome else {
        panic!("a forged handoff must never seal the mission, got {outcome:?}");
    };
    assert_eq!(attention[0].id, "terminal_review_failed:mission");
    assert!(attention[0].report.contains("nonce mismatch"));
}

#[tokio::test]
async fn a_crashed_review_synthesizes_failure_without_rerunning_the_llm() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = started(&dir, review_runner(vec![(true, vec![])])).await;

    // Drive to the brink by hand: record the review request, lease it to a
    // "dead" worker, then resume (mirrors the role-run crash discipline).
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let event = lionclaw::store::NewEvent::new(MissionEvent::TerminalReviewRequested {
        attempt_no: 1,
        idempotency_key: "crashed-review".to_string(),
        role: lionclaw::model::RoleName::new("gap-reviewer").expect("role"),
        prompt: PayloadRef::inline("prompt"),
        judged_sha: BASE_SHA.to_string(),
        nonce: "n0".to_string(),
    });
    h.engine
        .store()
        .append(&mission_id, state.head, &[event], 1)
        .await
        .expect("append request");
    let leases = h
        .engine
        .store()
        .pull_due(&mission_id, "dead-worker", 1, 60_000, 2)
        .await
        .expect("lease");
    assert_eq!(leases.len(), 1);

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(
        matches!(outcome, AdvanceOutcome::Parked { .. }),
        "got {outcome:?}"
    );
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert!(state.inflight.is_empty());
    let Some(ReviewOutcome::Failed { detail }) = &state.terminal_review.outcome else {
        panic!("synthesized failure recorded");
    };
    assert!(detail.contains("unknowable"));
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .expect("lock")
            .get("crashed-review"),
        None,
        "a crashed review is never re-run"
    );
}

#[tokio::test]
async fn a_mission_without_the_config_never_dispatches_a_review() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The mission type provides the reviewer role, but this mission was
    // created without the config (a pre-feature log's shape).
    let h = harness_with_type(
        dir.path(),
        review_mission_type(),
        review_runner(vec![(true, vec![])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(matches!(outcome, AdvanceOutcome::Terminal { .. }));
    assert!(review_calls(&h).is_empty(), "no config, no reviewer");
}
