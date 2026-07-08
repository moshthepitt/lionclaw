//! Mid-mission re-planning: an amendment adds / supersedes / cancels tasks and
//! *strengthens* the contract, atomically, without a new path to `Verified`.
//! The load-bearing test is `replan_cannot_launder_a_regression`: re-planning
//! verified work is allowed (no sealing), and honesty holds because the oracle
//! re-judges the real tree at the final head.

mod common;

use common::{advisory_plan, default_config, harness, simple_plan, ParseTask, BASE_SHA, HEAD_SHA};
use lionclaw_mission_engine::engine::{AdvanceOutcome, AmendError};
use lionclaw_mission_engine::model::{
    AmendmentError, AmendmentOps, ArtifactOutcome, Assertion, AssertionId, FinishClass, Handoff,
    MissionConfig, MissionPhase, OracleBinding, OracleName, PayloadRef, PlanSubmission, RoleName,
    RunErrorKind, Supersession, Task, TaskKind, TaskStatus,
};
use lionclaw_mission_engine::ports::{RoleRunFailure, RoleRunOutcome};
use lionclaw_mission_engine::store::NewEvent;
use lionclaw_mission_engine::testing::{MockOracleRunner, MockRoleRunner};

const HEAD2_SHA: &str = "0000000000000000000000000000000000000003";

// --- builders ---------------------------------------------------------------

fn work_task(id: &str, target: &str, deps: &[&str]) -> Task {
    Task {
        id: id.parse_task(),
        kind: TaskKind::Work,
        body: format!("do {id}"),
        targets: vec![AssertionId::new(target).unwrap()],
        role: Some(RoleName::new("implementer").unwrap()),
        depends_on: deps.iter().map(|d| d.parse_task()).collect(),
    }
}

fn oracle_assertion(id: &str) -> Assertion {
    Assertion {
        id: AssertionId::new(id).unwrap(),
        prose: format!("{id} holds"),
        oracle: Some(OracleName::new("cargo-test").unwrap()),
    }
}

fn work_done(head: &str, request_attention: bool) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Handoff::Work {
            done: true,
            report: PayloadRef::inline("did it"),
            request_attention,
        },
        artifact: Some(ArtifactOutcome {
            base_sha: BASE_SHA.to_string(),
            head_sha: head.to_string(),
        }),
        model_id: None,
    }
}

// --- patch: an amendment can add a runnable node ----------------------------

#[tokio::test]
async fn patch_adds_a_runnable_node_that_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Amend before advancing: add a second oracle-bound assertion + its coverer.
    let ops = AmendmentOps {
        add: vec![work_task("feat", "FEATURE-OK", &[])],
        add_assertion: vec![oracle_assertion("FEATURE-OK")],
        ..Default::default()
    };
    h.engine
        .amend_plan(&m, ops, "orchestrator", "add a feature", 1)
        .await
        .unwrap();

    let done = h.engine.advance(&m).await.unwrap();
    assert!(
        matches!(
            done,
            AdvanceOutcome::Terminal {
                phase: MissionPhase::Done {
                    finish: FinishClass::Verified
                }
            }
        ),
        "got {done:?}"
    );
    let ran: Vec<String> = h
        .role_runner
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|(t, _, _)| t.to_string())
        .collect();
    assert!(
        ran.iter().any(|t| t == "fix") && ran.iter().any(|t| t == "feat"),
        "both nodes dispatched: {ran:?}"
    );
    let state = h.engine.load_state(&m).await.unwrap();
    assert_eq!(state.revision, 2);
}

// --- supersede-to-fix: replace a failed node --------------------------------

#[tokio::test]
async fn supersede_replaces_a_failed_node_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    // "fix" fails; its replacement "fix2" succeeds.
    let runner = MockRoleRunner::new(Box::new(|req| {
        if req.task_id.as_str() == "fix" {
            Err(RoleRunFailure {
                kind: RunErrorKind::TurnFailed,
                detail: "boom".into(),
            })
        } else {
            Ok(work_done(HEAD_SHA, false))
        }
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // "fix" fails → the mission parks on the failed node (amendable).
    let parked = h.engine.advance(&m).await.unwrap();
    assert!(
        matches!(parked, AdvanceOutcome::Parked { .. }),
        "got {parked:?}"
    );

    // Supersede the failed node with a fix; the oracle then runs → Verified.
    let ops = AmendmentOps {
        add: vec![work_task("fix2", "TESTS-PASS", &[])],
        supersede: vec![Supersession {
            old: "fix".parse_task(),
            new: "fix2".parse_task(),
        }],
        ..Default::default()
    };
    h.engine
        .amend_plan(&m, ops, "orchestrator", "try again", 1)
        .await
        .unwrap();

    let done = h.engine.advance(&m).await.unwrap();
    assert!(
        matches!(
            done,
            AdvanceOutcome::Terminal {
                phase: MissionPhase::Done {
                    finish: FinishClass::Verified
                }
            }
        ),
        "got {done:?}"
    );
    // Audit: the old node is a Superseded tombstone, gone from the live plan,
    // still present in the raw log.
    let state = h.engine.load_state(&m).await.unwrap();
    assert_eq!(
        state.tasks[&"fix".parse_task()].status,
        TaskStatus::Superseded
    );
    let plan = state.plan.unwrap();
    assert!(
        !plan.tasks.iter().any(|t| t.id == "fix".parse_task()),
        "fix removed from live plan"
    );
    let log = h.engine.store().load(&m).await.unwrap();
    assert!(
        log.iter().any(|e| e.event.event_type() == "plan_amended"),
        "the amendment survives in the raw event log"
    );
}

// --- THE honesty test: re-planning cannot make the red go away --------------

/// Re-plan a red (failed) node with a replacement, then let the mission finish.
/// The grade is decided by the ORACLE re-running on the real re-planned tree —
/// never by the act of re-planning. `fix_passes_oracle` selects whether the
/// replacement's resulting tree actually passes the oracle. Returns the finish
/// grade and whether the oracle judged the re-planned head.
async fn replan_red_node(fix_passes_oracle: bool) -> (FinishClass, bool) {
    let dir = tempfile::tempdir().unwrap();
    // "fix" fails (parks the mission); "fix2" completes as WORK at a new head —
    // but whether the tree actually passes is the oracle's call.
    let runner = MockRoleRunner::new(Box::new(|req| {
        if req.task_id.as_str() == "fix" {
            Err(RoleRunFailure {
                kind: RunErrorKind::TurnFailed,
                detail: "boom".into(),
            })
        } else {
            Ok(work_done(HEAD2_SHA, false))
        }
    }));
    let oracle = MockOracleRunner::new(Box::new(move |req| {
        // The re-planned tree passes iff the fix genuinely works.
        let exit = if req.judged_sha == HEAD2_SHA && !fix_passes_oracle {
            1
        } else {
            0
        };
        Ok(lionclaw_mission_engine::ports::OracleOutcome {
            exit_code: exit,
            exit_signal: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            duration_ms: 1,
        })
    }));
    let h = harness(dir.path(), runner, oracle).await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // The node fails → the mission parks (amendable, still Unverified).
    let parked = h.engine.advance(&m).await.unwrap();
    assert!(
        matches!(parked, AdvanceOutcome::Parked { .. }),
        "got {parked:?}"
    );

    // Re-plan the red node (no sealing forbids touching a non-verified node).
    let ops = AmendmentOps {
        add: vec![work_task("fix2", "TESTS-PASS", &[])],
        supersede: vec![Supersession {
            old: "fix".parse_task(),
            new: "fix2".parse_task(),
        }],
        ..Default::default()
    };
    h.engine
        .amend_plan(&m, ops, "orchestrator", "try a different approach", 1)
        .await
        .unwrap();

    // The oracle judges the real re-planned tree; the grade follows IT.
    let done = h.engine.advance(&m).await.unwrap();
    let re_judged = h
        .oracle_runner
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(_, sha)| sha == HEAD2_SHA);
    match done {
        AdvanceOutcome::Terminal {
            phase: MissionPhase::Done { finish },
        } => (finish, re_judged),
        other => panic!("expected Done, got {other:?}"),
    }
}

#[tokio::test]
async fn replan_cannot_launder_a_non_fix() {
    // A re-plan whose tree still fails the oracle stays Unverified — re-planning
    // alone can never make the red go away; only a real oracle pass can.
    let (finish, re_judged) = replan_red_node(false).await;
    assert_eq!(finish, FinishClass::Unverified);
    assert!(re_judged, "the oracle judged the re-planned tree");
}

#[tokio::test]
async fn replan_with_a_real_fix_verifies() {
    // A re-plan whose tree genuinely passes the oracle reaches Verified.
    let (finish, re_judged) = replan_red_node(true).await;
    assert_eq!(finish, FinishClass::Verified);
    assert!(re_judged, "the oracle judged the re-planned tree");
}

// --- strengthen-only --------------------------------------------------------

#[tokio::test]
async fn bind_oracle_replacing_a_bound_oracle_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // TESTS-PASS is already bound to cargo-test; binding a *different* oracle
    // would unbind it → refused (contract is strengthen-only).
    let ops = AmendmentOps {
        bind_oracle: vec![OracleBinding {
            assertion: AssertionId::new("TESTS-PASS").unwrap(),
            oracle: OracleName::new("cargo-clippy").unwrap(),
        }],
        ..Default::default()
    };
    let err = h.engine.amend_plan(&m, ops, "o", "", 1).await.unwrap_err();
    assert!(
        matches!(
            err,
            AmendError::Rejected(AmendmentError::OracleUnbound { .. })
        ),
        "got {err:?}"
    );
    // Nothing changed.
    assert_eq!(h.engine.load_state(&m).await.unwrap().revision, 1);
}

#[tokio::test]
async fn bind_oracle_on_unbound_assertion_adds_an_authoritative_obligation() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    // A plan whose sole assertion has NO oracle (advisory-only) + its coverer.
    let plan = PlanSubmission {
        assertions: vec![Assertion {
            id: AssertionId::new("FEATURE-OK").unwrap(),
            prose: "the feature works".to_string(),
            oracle: None,
        }],
        tasks: vec![work_task("build", "FEATURE-OK", &[])],
    };
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, plan).await.unwrap();

    // Bind an oracle to the unbound assertion (a strengthening) before work runs.
    let ops = AmendmentOps {
        bind_oracle: vec![OracleBinding {
            assertion: AssertionId::new("FEATURE-OK").unwrap(),
            oracle: OracleName::new("cargo-test").unwrap(),
        }],
        ..Default::default()
    };
    h.engine
        .amend_plan(&m, ops, "o", "enforce it", 1)
        .await
        .unwrap();

    // The new obligation is now authoritative: the oracle runs and the mission
    // reaches Verified (it could not before — an oracle-less assertion caps at
    // InternallyConsistent).
    let done = h.engine.advance(&m).await.unwrap();
    assert!(
        matches!(
            done,
            AdvanceOutcome::Terminal {
                phase: MissionPhase::Done {
                    finish: FinishClass::Verified
                }
            }
        ),
        "got {done:?}"
    );
    assert!(
        h.oracle_runner
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(o, _)| o == "cargo-test"),
        "the bound oracle ran"
    );
}

// --- atomicity --------------------------------------------------------------

#[tokio::test]
async fn cancel_sole_coverer_without_replacement_is_refused_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Cancelling the only coverer of TESTS-PASS orphans it → the resulting plan
    // is invalid → refused, plan unchanged.
    let ops = AmendmentOps {
        cancel: vec!["fix".parse_task()],
        ..Default::default()
    };
    let err = h.engine.amend_plan(&m, ops, "o", "", 1).await.unwrap_err();
    assert!(
        matches!(err, AmendError::Rejected(AmendmentError::Invalid(_))),
        "got {err:?}"
    );
    let state = h.engine.load_state(&m).await.unwrap();
    assert_eq!(state.revision, 1);
    assert_eq!(state.tasks[&"fix".parse_task()].status, TaskStatus::Pending);
}

// --- stale revision ---------------------------------------------------------

#[tokio::test]
async fn amendment_targeting_a_stale_revision_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    let ops = AmendmentOps {
        add: vec![work_task("x", "TESTS-PASS", &[])],
        ..Default::default()
    };
    let err = h.engine.amend_plan(&m, ops, "o", "", 99).await.unwrap_err();
    assert!(
        matches!(
            err,
            AmendError::StaleRevision {
                targeted: 99,
                current: 1
            }
        ),
        "got {err:?}"
    );
}

// --- quiesce ----------------------------------------------------------------

#[tokio::test]
async fn amendment_while_an_effect_is_in_flight_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Inject a role-run request with a long, still-live lease (a concurrent
    // driver is running it) so the mission is not quiescent.
    let state = h.engine.load_state(&m).await.unwrap();
    let ev = NewEvent::new(
        lionclaw_mission_engine::model::MissionEvent::RoleRunRequested {
            task_id: "fix".parse_task(),
            attempt_no: 1,
            idempotency_key: "live-key".to_string(),
            role: RoleName::new("implementer").unwrap(),
            prompt: PayloadRef::inline("p"),
            base_sha: BASE_SHA.to_string(),
        },
    );
    h.engine
        .store()
        .append(&m, state.head, &[ev], 1_000)
        .await
        .unwrap();
    h.engine
        .store()
        .pull_due(&m, "other-worker", 1, 3_600_000, 1_000)
        .await
        .unwrap();

    let ops = AmendmentOps {
        add: vec![work_task("x", "TESTS-PASS", &[])],
        ..Default::default()
    };
    let err = h.engine.amend_plan(&m, ops, "o", "", 1).await.unwrap_err();
    assert!(matches!(err, AmendError::MissionBusy), "got {err:?}");
}

// --- id reuse ---------------------------------------------------------------

#[tokio::test]
async fn readding_a_retired_task_id_is_refused() {
    // A retired task keeps a Superseded tombstone in state.tasks; re-adding its
    // id would birth the new task tombstoned (never runnable), so it is refused.
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Retire "fix" (supersede it with "fix2").
    h.engine
        .amend_plan(
            &m,
            AmendmentOps {
                add: vec![work_task("fix2", "TESTS-PASS", &[])],
                supersede: vec![Supersession {
                    old: "fix".parse_task(),
                    new: "fix2".parse_task(),
                }],
                ..Default::default()
            },
            "o",
            "",
            1,
        )
        .await
        .unwrap();

    // Re-adding a task with the retired id "fix" (over a new assertion) is refused.
    let err = h
        .engine
        .amend_plan(
            &m,
            AmendmentOps {
                add: vec![work_task("fix", "FEATURE-OK", &[])],
                add_assertion: vec![oracle_assertion("FEATURE-OK")],
                ..Default::default()
            },
            "o",
            "",
            2,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            AmendError::Rejected(AmendmentError::TaskIdReused { .. })
        ),
        "got {err:?}"
    );
    // The refused amendment changed nothing.
    assert_eq!(h.engine.load_state(&m).await.unwrap().revision, 2);
}

// --- a rejected amendment has no side effects -------------------------------

#[tokio::test]
async fn a_rejected_amendment_appends_nothing() {
    // The phase/revision guards run BEFORE the reconcile/quiesce loop, so a
    // rejected amendment never appends a synthesized reconcile event — even
    // when an expired-lease effect is in flight.
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Inject a role-run request with an already-EXPIRED lease (reconcile would
    // synthesize a failure for it if it ran).
    let state = h.engine.load_state(&m).await.unwrap();
    let ev = NewEvent::new(
        lionclaw_mission_engine::model::MissionEvent::RoleRunRequested {
            task_id: "fix".parse_task(),
            attempt_no: 1,
            idempotency_key: "expired-key".to_string(),
            role: RoleName::new("implementer").unwrap(),
            prompt: PayloadRef::inline("p"),
            base_sha: BASE_SHA.to_string(),
        },
    );
    h.engine
        .store()
        .append(&m, state.head, &[ev], 1_000)
        .await
        .unwrap();
    h.engine
        .store()
        .pull_due(&m, "dead", 1, 1, 1_000)
        .await
        .unwrap(); // 1ms lease → expired

    let log_before = h.engine.store().load(&m).await.unwrap().len();
    // A stale-revision amendment is rejected up front — no reconcile append.
    let err = h
        .engine
        .amend_plan(
            &m,
            AmendmentOps {
                add: vec![work_task("x", "TESTS-PASS", &[])],
                ..Default::default()
            },
            "o",
            "",
            99,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, AmendError::StaleRevision { .. }),
        "got {err:?}"
    );
    let log_after = h.engine.store().load(&m).await.unwrap().len();
    assert_eq!(
        log_before, log_after,
        "a rejected amendment appends nothing"
    );
}

// --- immateriality ----------------------------------------------------------

#[tokio::test]
async fn an_immaterial_amendment_is_refused() {
    // An empty amendment — or one that only re-binds an already-bound oracle —
    // changes nothing, so it must not bump the revision or re-open ratification.
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Empty ops.
    let err = h
        .engine
        .amend_plan(&m, AmendmentOps::default(), "o", "", 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, AmendError::Rejected(AmendmentError::Immaterial)),
        "empty: got {err:?}"
    );
    // A no-op bind (TESTS-PASS is already bound to cargo-test).
    let err = h
        .engine
        .amend_plan(
            &m,
            AmendmentOps {
                bind_oracle: vec![OracleBinding {
                    assertion: AssertionId::new("TESTS-PASS").unwrap(),
                    oracle: OracleName::new("cargo-test").unwrap(),
                }],
                ..Default::default()
            },
            "o",
            "",
            1,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, AmendError::Rejected(AmendmentError::Immaterial)),
        "idempotent bind: got {err:?}"
    );
    // Nothing changed.
    assert_eq!(h.engine.load_state(&m).await.unwrap().revision, 1);
}

// --- ratification -----------------------------------------------------------

#[tokio::test]
async fn material_amendment_reopens_ratification_when_the_gate_is_on() {
    use lionclaw_mission_engine::model::DecisionAction;
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    // Gate ON.
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            MissionConfig::default(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();

    // Ratify revision 1.
    h.engine.advance(&m).await.unwrap(); // parks on ratify
    h.engine
        .decide(&m, "ratify:mission", DecisionAction::Ratify, "ok", "human")
        .await
        .unwrap();

    // A material amendment bumps the revision and clears ratification.
    let ops = AmendmentOps {
        add: vec![work_task("feat", "FEATURE-OK", &[])],
        add_assertion: vec![oracle_assertion("FEATURE-OK")],
        ..Default::default()
    };
    h.engine
        .amend_plan(&m, ops, "o", "more scope", 1)
        .await
        .unwrap();
    let state = h.engine.load_state(&m).await.unwrap();
    assert_eq!(state.revision, 2);
    assert!(!state.ratified, "revision 2 is unratified");

    // The mission re-parks on Ratify rather than running the new work.
    let parked = h.engine.advance(&m).await.unwrap();
    let AdvanceOutcome::Parked { attention } = parked else {
        panic!("got {parked:?}")
    };
    assert!(
        attention.iter().any(|a| a.id == "ratify:mission"),
        "re-parked on ratify: {attention:?}"
    );
}

#[tokio::test]
async fn amendment_flows_without_ratification_when_the_gate_is_off() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, advisory_plan()).await.unwrap();
    let ops = AmendmentOps {
        add: vec![work_task("feat", "FEATURE-OK", &[])],
        add_assertion: vec![oracle_assertion("FEATURE-OK")],
        ..Default::default()
    };
    // No ratification required; the amendment simply applies.
    h.engine.amend_plan(&m, ops, "o", "", 1).await.unwrap();
    let state = h.engine.load_state(&m).await.unwrap();
    assert_eq!(state.revision, 2);
    assert!(!state.ratified);
}

// --- resume determinism -----------------------------------------------------

#[tokio::test]
async fn amended_mission_snapshot_fold_equals_full_refold() {
    use lionclaw_mission_engine::model::{fold, REDUCER_VERSION};
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let m = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    h.engine.submit_plan(&m, simple_plan()).await.unwrap();
    let ops = AmendmentOps {
        add: vec![work_task("feat", "FEATURE-OK", &[])],
        add_assertion: vec![oracle_assertion("FEATURE-OK")],
        ..Default::default()
    };
    h.engine.amend_plan(&m, ops, "o", "", 1).await.unwrap();
    h.engine.advance(&m).await.unwrap();

    // The bumped REDUCER_VERSION snapshot must cover the whole log and match a
    // fresh full refold — the amendment folds deterministically and resumes.
    let events = h.engine.store().load(&m).await.unwrap();
    let head = events.last().unwrap().sequence_no;
    let (upto, reducer) = h
        .engine
        .store()
        .snapshot_meta(&m)
        .await
        .unwrap()
        .expect("snapshot written");
    assert_eq!(upto, head);
    assert_eq!(reducer, REDUCER_VERSION);
    let via_snapshot = h
        .engine
        .store()
        .load_state_snapshotted(&m)
        .await
        .unwrap()
        .unwrap();
    let via_full = fold(events).unwrap();
    assert_eq!(via_snapshot, via_full);
}
