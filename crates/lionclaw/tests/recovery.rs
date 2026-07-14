mod common;

use std::sync::{Arc, Mutex};

use common::{approve_plan, default_config, harness, proposal, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    ArtifactOutcome, DecisionAction, Handoff, MissionPhase, PayloadRef, RunErrorKind,
};
use lionclaw::ports::{OracleOutcome, RoleRunFailure, RoleRunOutcome};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn completed_work(base_sha: &str) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Handoff::Work {
            done: true,
            report: PayloadRef::inline("completed"),
            request_attention: false,
        },
        artifact: Some(ArtifactOutcome {
            base_sha: base_sha.to_string(),
            head_sha: HEAD_SHA.to_string(),
        }),
        model_id: None,
    }
}

#[tokio::test]
async fn invalid_handoff_is_reworked_automatically_with_exact_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let calls = Arc::new(Mutex::new(0_u32));
    let seen = calls.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        captured.lock().unwrap().push(request.prompt.clone());
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Ok(RoleRunOutcome {
                handoff: Handoff::Work {
                    done: false,
                    report: PayloadRef::inline("unfinished"),
                    request_attention: false,
                },
                artifact: None,
                model_id: None,
            });
        }
        Ok(completed_work(&request.base_sha))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission("/repo", "recover output", BASE_SHA, default_config())
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let outcome = h.engine.advance(&id).await.unwrap();
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    let prompts = prompts.lock().unwrap();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[1].contains("Required rework"));
    assert!(prompts[1].contains("role reported done=false"));
    let calls = h.role_runner.calls.lock().unwrap();
    assert_eq!(calls.iter().map(|call| call.1).collect::<Vec<_>>(), [1, 2]);
    assert_ne!(calls[0].2, calls[1].2);
}

#[tokio::test]
async fn transient_runtime_failure_retries_but_launch_failure_parks_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = Arc::new(Mutex::new(0_u32));
    let seen = attempts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Err(RoleRunFailure {
                kind: RunErrorKind::Timeout,
                detail: "provider temporarily unavailable".into(),
            });
        }
        assert!(request.prompt.contains("provider temporarily unavailable"));
        Ok(completed_work(&request.base_sha))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission("/repo", "retry transient", BASE_SHA, default_config())
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    assert_eq!(
        h.engine.advance(&id).await.unwrap().disposition,
        MissionDisposition::Terminal
    );
    assert_eq!(*attempts.lock().unwrap(), 2);

    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|_| {
        Err(RoleRunFailure {
            kind: RunErrorKind::Launch,
            detail: "runtime profile is invalid".into(),
        })
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission("/repo", "fail launch", BASE_SHA, default_config())
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    let view = h.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Parked);
    let attention: Vec<_> = view.state.open_attention.values().collect();
    assert_eq!(attention[0].id, "node_failed:fix");
    assert_eq!(h.role_runner.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oracle_repair_reopens_the_owner_with_both_evidence_streams() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let worker = MockRoleRunner::new(Box::new(move |request| {
        captured.lock().unwrap().push(request.prompt.clone());
        Ok(completed_work(&request.base_sha))
    }));
    let oracle_runs = Mutex::new(0_u32);
    let oracle = MockOracleRunner::new(Box::new(move |_| {
        let mut count = oracle_runs.lock().unwrap();
        *count += 1;
        Ok(if *count == 1 {
            OracleOutcome {
                exit_code: 1,
                exit_signal: None,
                stdout: b"test output".to_vec(),
                stderr: b"compiler diagnostic".to_vec(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }
        } else {
            OracleOutcome {
                exit_code: 0,
                exit_signal: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }
        })
    }));
    let h = harness(dir.path(), worker, oracle).await;
    let id = h
        .engine
        .create_mission("/repo", "repair failure", BASE_SHA, default_config())
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    assert_eq!(
        h.engine.advance(&id).await.unwrap().disposition,
        MissionDisposition::Parked
    );
    h.engine
        .decide(
            &id,
            "oracle_verdict_failed:cargo-test",
            DecisionAction::Repair,
            "fix the compiler error",
        )
        .await
        .unwrap();
    let view = h.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Terminal);
    assert!(matches!(view.state.phase, MissionPhase::Done { .. }));
    let prompts = prompts.lock().unwrap();
    assert_eq!(prompts.len(), 2);
    for expected in [
        "fix the compiler error",
        "stdout:\ntest output",
        "stderr:\ncompiler diagnostic",
    ] {
        assert!(prompts[1].contains(expected), "missing {expected:?}");
    }
}
