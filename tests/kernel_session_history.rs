use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use lionclaw::{
    contracts::{
        SessionActionKind, SessionHistoryPolicy, SessionHistoryRequest, SessionLatestQuery,
        SessionOpenRequest, SessionTurnKind, SessionTurnRequest, SessionTurnStatus, TrustTier,
    },
    home::RUNTIME_SESSION_READY_MARKER,
    kernel::{
        runtime::{
            ConfinementConfig, OciConfinementConfig, RuntimeAdapter, RuntimeAdapterInfo,
            RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender, RuntimeExecutionProfile,
            RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnResult,
        },
        Kernel, KernelError, KernelOptions,
    },
};
use sqlx::{Row, SqlitePool};
use tempfile::TempDir;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::test]
async fn interactive_history_carries_partial_reply_forward() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(200),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "partial",
            Arc::new(PartialTimeoutAdapter {
                partial_text: "partial answer".to_string(),
                sleep_for: Duration::from_millis(120),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: recorded_prompts.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "interactive-peer",
        SessionHistoryPolicy::Interactive,
    )
    .await;

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "tell me something long".to_string(),
            runtime_id: Some("partial".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should time out");
    assert!(matches!(err, KernelError::RuntimeTimeout(_)));

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(12),
        })
        .await
        .expect("history");
    assert_eq!(history.turns.len(), 1);
    assert_eq!(history.turns[0].status, SessionTurnStatus::TimedOut);
    assert_eq!(history.turns[0].assistant_text, "partial answer");

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "still going?".to_string(),
            runtime_id: Some("capture".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("follow-up turn");
    assert_eq!(response.assistant_text, "captured");

    let prompt = recorded_prompts.lock().expect("prompt lock")[0].clone();
    assert!(prompt.contains("tell me something long"));
    assert!(prompt.contains("[Partial assistant reply; previous turn timed out before completion]"));
    assert!(prompt.contains("partial answer"));
    assert!(prompt.contains("still going?"));
}

#[tokio::test]
async fn conservative_history_uses_failure_note_without_partial_text() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(200),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "partial",
            Arc::new(PartialTimeoutAdapter {
                partial_text: "private partial".to_string(),
                sleep_for: Duration::from_millis(120),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: recorded_prompts.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "conservative-peer",
        SessionHistoryPolicy::Conservative,
    )
    .await;

    let _ = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "hidden partial".to_string(),
            runtime_id: Some("partial".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should time out");

    let _ = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "next input".to_string(),
            runtime_id: Some("capture".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("follow-up turn");

    let prompt = recorded_prompts.lock().expect("prompt lock")[0].clone();
    assert!(prompt.contains("timed out before completion"));
    assert!(!prompt.contains("private partial"));
}

#[tokio::test]
async fn continue_and_retry_actions_create_durable_turns() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(200),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "partial",
            Arc::new(PartialTimeoutAdapter {
                partial_text: "partial reply".to_string(),
                sleep_for: Duration::from_millis(120),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: recorded_prompts.clone(),
                reply: "continued".to_string(),
            }),
        )
        .await;

    let session = open_session(&kernel, "action-peer", SessionHistoryPolicy::Interactive).await;

    let _ = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "original prompt".to_string(),
            runtime_id: Some("partial".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should time out");

    let continue_turn = kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::ContinueLastPartial,
            Some("capture".to_string()),
        )
        .await
        .expect("continue action");
    assert_eq!(continue_turn.assistant_text, "continued");

    let retry_turn = kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::RetryLastTurn,
            Some("capture".to_string()),
        )
        .await
        .expect("retry action");
    assert_eq!(retry_turn.assistant_text, "continued");

    let prompts = recorded_prompts.lock().expect("prompt lock").clone();
    assert!(
        prompts[0].contains(
            "Continue your previous assistant reply from where it stopped. Do not restart from the beginning unless necessary."
        ),
        "continue prompt should use the synthesized continuation instruction"
    );
    assert!(
        prompts[1].contains("original prompt"),
        "retry prompt should reuse the original prompt text"
    );

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(12),
        })
        .await
        .expect("history");
    assert_eq!(history.turns.len(), 3);
    assert_eq!(history.turns[1].kind, SessionTurnKind::Continue);
    assert_eq!(history.turns[1].display_user_text, "/continue");
    assert_eq!(history.turns[2].kind, SessionTurnKind::Retry);
    assert_eq!(history.turns[2].display_user_text, "/retry");
}

#[tokio::test]
async fn continue_requires_interactive_history_policy() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(200),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "partial",
            Arc::new(PartialTimeoutAdapter {
                partial_text: "hidden partial".to_string(),
                sleep_for: Duration::from_millis(120),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "conservative-action",
        SessionHistoryPolicy::Conservative,
    )
    .await;

    let _ = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "original prompt".to_string(),
            runtime_id: Some("partial".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should time out");

    let err = kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::ContinueLastPartial,
            Some("capture".to_string()),
        )
        .await
        .expect_err("continue should be rejected for conservative sessions");
    assert!(
        matches!(err, KernelError::BadRequest(message) if message.contains("interactive session history"))
    );
}

#[tokio::test]
async fn retry_uses_repaired_latest_turn_instead_of_zero_information_tail() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: recorded_prompts.clone(),
                reply: "retried".to_string(),
            }),
        )
        .await;

    let session = open_session(&kernel, "repair-action", SessionHistoryPolicy::Interactive).await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 1,
            status: SessionTurnStatus::Completed,
            prompt_user_text: "real prompt",
            assistant_text: "real answer",
            error_code: None,
            error_text: None,
        },
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 2,
            status: SessionTurnStatus::Failed,
            prompt_user_text: "",
            assistant_text: "",
            error_code: None,
            error_text: None,
        },
    )
    .await;

    let response = kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::RetryLastTurn,
            Some("capture".to_string()),
        )
        .await
        .expect("retry action");
    assert_eq!(response.assistant_text, "retried");

    let prompt = recorded_prompts.lock().expect("prompt lock")[0].clone();
    assert!(prompt.contains("real prompt"));
    assert!(!prompt.contains("## Prior Turn 2"));
}

#[tokio::test]
async fn kernel_restart_interrupts_running_turn_and_preserves_partial_output() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    kernel
        .register_runtime_adapter(
            "blocking",
            Arc::new(BlockingAnswerAdapter {
                answer: "checkpointed partial".to_string(),
                sleep_for: Duration::from_secs(30),
            }),
        )
        .await;

    let session = open_session(&kernel, "restart-peer", SessionHistoryPolicy::Interactive).await;
    let session_id = session.session_id;
    let turn_kernel = kernel.clone();
    let turn_task = tokio::spawn(async move {
        turn_kernel
            .turn_session(SessionTurnRequest {
                session_id,
                user_text: "long running".to_string(),
                runtime_id: Some("blocking".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
    });

    wait_for_turn_checkpoint(&env.db_path(), session_id, "checkpointed partial").await;
    turn_task.abort();
    let _ = turn_task.await;
    drop(kernel);

    let rebooted = Kernel::new(&env.db_path()).await.expect("rebooted kernel");
    let history = rebooted
        .session_history(SessionHistoryRequest {
            session_id,
            limit: Some(12),
        })
        .await
        .expect("history after restart");

    assert_eq!(history.turns.len(), 1);
    let turn = &history.turns[0];
    assert_eq!(turn.status, SessionTurnStatus::Interrupted);
    assert_eq!(turn.assistant_text, "checkpointed partial");
    assert_eq!(turn.error_code.as_deref(), Some("runtime.interrupted"));
    assert_eq!(
        turn.error_text.as_deref(),
        Some("turn interrupted by kernel restart")
    );
    assert!(turn.finished_at.is_some());

    let pool = connect_pool(&env.db_path()).await;
    let row = sqlx::query("SELECT turn_count FROM sessions WHERE session_id = ?1")
        .bind(session_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("fetch session turn count");
    let turn_count: i64 = row.get("turn_count");
    assert_eq!(turn_count, 1);

    rebooted
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                reply: "continued after restart".to_string(),
            }),
        )
        .await;
    let continued = rebooted
        .run_session_action(
            session_id,
            SessionActionKind::ContinueLastPartial,
            Some("capture".to_string()),
        )
        .await
        .expect("continue after restart");
    assert_eq!(continued.assistant_text, "continued after restart");
}

#[tokio::test]
async fn interactive_history_carries_interrupted_partial_reply_forward() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: recorded_prompts.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "interrupted-peer",
        SessionHistoryPolicy::Interactive,
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 1,
            status: SessionTurnStatus::Interrupted,
            prompt_user_text: "original prompt",
            assistant_text: "partial after crash",
            error_code: Some("runtime.interrupted"),
            error_text: Some("turn interrupted by kernel restart"),
        },
    )
    .await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "continue from that".to_string(),
            runtime_id: Some("capture".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("follow-up turn");
    assert_eq!(response.assistant_text, "captured");

    let prompt = recorded_prompts.lock().expect("prompt lock")[0].clone();
    assert!(prompt.contains("## Prior Turn 1"));
    assert!(prompt.contains("original prompt"));
    assert!(prompt
        .contains("[Partial assistant reply; previous turn was interrupted before completion]"));
    assert!(prompt.contains("partial after crash"));
}

#[tokio::test]
async fn session_history_overfetches_usable_turns_past_zero_information_rows() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let session = open_session(&kernel, "repair-peer", SessionHistoryPolicy::Interactive).await;

    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 1,
            status: SessionTurnStatus::Completed,
            prompt_user_text: "first",
            assistant_text: "alpha",
            error_code: None,
            error_text: None,
        },
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 2,
            status: SessionTurnStatus::Failed,
            prompt_user_text: "",
            assistant_text: "",
            error_code: None,
            error_text: None,
        },
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 3,
            status: SessionTurnStatus::Completed,
            prompt_user_text: "third",
            assistant_text: "gamma",
            error_code: None,
            error_text: None,
        },
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 4,
            status: SessionTurnStatus::Cancelled,
            prompt_user_text: "",
            assistant_text: "",
            error_code: None,
            error_text: None,
        },
    )
    .await;
    insert_session_turn(
        &env.db_path(),
        SessionTurnSeed {
            session_id: session.session_id,
            sequence_no: 5,
            status: SessionTurnStatus::Completed,
            prompt_user_text: "fifth",
            assistant_text: "omega",
            error_code: None,
            error_text: None,
        },
    )
    .await;

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(2),
        })
        .await
        .expect("history");

    assert_eq!(history.turns.len(), 2);
    assert_eq!(history.turns[0].display_user_text, "third");
    assert_eq!(history.turns[1].display_user_text, "fifth");
}

#[tokio::test]
async fn latest_session_snapshot_prefers_reset_session_without_turns() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(&kernel, "reset-peer", SessionHistoryPolicy::Interactive).await;
    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "normal run".to_string(),
            runtime_id: Some("capture".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("seed completed turn");

    let reset = kernel
        .session_action(lionclaw::contracts::SessionActionRequest {
            session_id: session.session_id,
            action: SessionActionKind::ResetSession,
        })
        .await
        .expect("reset session");

    let snapshot = kernel
        .latest_session_snapshot(SessionLatestQuery {
            channel_id: "local-cli".to_string(),
            peer_id: "reset-peer".to_string(),
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("latest session snapshot");

    assert_eq!(
        snapshot.session.as_ref().map(|session| session.session_id),
        Some(reset.session_id)
    );
    assert!(snapshot.turns.is_empty());
    assert!(snapshot.resume_after_sequence.is_none());
}

#[tokio::test]
async fn session_scoped_runtime_state_resumes_without_replaying_full_history() {
    let env = TestEnv::new();
    let workspace_root = env.path().join("workspace");
    let project_root = env.path().join("project");
    let runtime_root = env.path().join("runtime");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(workspace_root),
            project_workspace_root: Some(project_root),
            runtime_root: Some(runtime_root),
            workspace_name: Some("main".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    let recorded_roots = Arc::new(Mutex::new(Vec::new()));
    let resumed_flags = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "resumable",
            Arc::new(ResumableCaptureAdapter {
                prompts: recorded_prompts.clone(),
                runtime_roots: recorded_roots.clone(),
                resumed_flags: resumed_flags.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(&kernel, "resume-peer", SessionHistoryPolicy::Interactive).await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "first request".to_string(),
            runtime_id: Some("resumable".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("first turn");

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "second request".to_string(),
            runtime_id: Some("resumable".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("second turn");

    let prompts = recorded_prompts.lock().expect("prompt lock").clone();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[0].contains("first request"));
    assert!(prompts[1].contains("second request"));
    assert!(
        !prompts[1].contains("User: first request"),
        "resumed harness sessions should not replay the full prior transcript"
    );
    assert!(
        prompts[1]
            .contains("Continue the existing runtime conversation for this LionClaw session."),
        "resumed harness sessions should get the runtime-session continuation note"
    );

    let roots = recorded_roots.lock().expect("roots lock").clone();
    assert_eq!(roots.len(), 2);
    assert_eq!(
        roots[0], roots[1],
        "same LionClaw session should reuse runtime state root"
    );
    let resumed_flags = resumed_flags.lock().expect("resumed flags lock").clone();
    assert_eq!(resumed_flags, vec![false, true]);
    assert!(
        roots[0].join(RUNTIME_SESSION_READY_MARKER).is_file(),
        "successful turns should mark the runtime state root as resumable"
    );
}

#[tokio::test]
async fn partial_failure_does_not_mark_runtime_session_resumable_for_continue() {
    let env = TestEnv::new();
    let workspace_root = env.path().join("workspace");
    let project_root = env.path().join("project");
    let runtime_root = env.path().join("runtime");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(200),
            runtime_turn_hard_timeout: Duration::from_millis(400),
            workspace_root: Some(workspace_root),
            project_workspace_root: Some(project_root),
            runtime_root: Some(runtime_root),
            workspace_name: Some("main".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let resumed_flags = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "flaky",
            Arc::new(ResumableFlakyAdapter {
                resumed_flags: resumed_flags.clone(),
                turn_counter: AtomicUsize::new(0),
            }),
        )
        .await;

    let session = open_session(&kernel, "flaky-peer", SessionHistoryPolicy::Interactive).await;

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "start a long answer".to_string(),
            runtime_id: Some("flaky".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("first turn should fail after partial output");
    assert!(matches!(err, KernelError::Runtime(_)));

    let response = kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::ContinueLastPartial,
            Some("flaky".to_string()),
        )
        .await
        .expect("continue should still succeed from canonical history");
    assert_eq!(response.assistant_text, "continued");

    let resumed_flags = resumed_flags.lock().expect("resumed flags lock").clone();
    assert_eq!(resumed_flags, vec![false, false]);
}

#[tokio::test]
async fn failed_turn_clears_runtime_session_resumability_for_next_turn() {
    let env = TestEnv::new();
    let workspace_root = env.path().join("workspace");
    let project_root = env.path().join("project");
    let runtime_root = env.path().join("runtime");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(workspace_root),
            project_workspace_root: Some(project_root),
            runtime_root: Some(runtime_root),
            workspace_name: Some("main".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_roots = Arc::new(Mutex::new(Vec::new()));
    let resumed_flags = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "recovering",
            Arc::new(ResumeAfterFailureAdapter {
                runtime_roots: recorded_roots.clone(),
                resumed_flags: resumed_flags.clone(),
                turn_counter: AtomicUsize::new(0),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "recovering-peer",
        SessionHistoryPolicy::Interactive,
    )
    .await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "first request".to_string(),
            runtime_id: Some("recovering".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("first turn");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "second request".to_string(),
            runtime_id: Some("recovering".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("second turn should fail");
    assert!(matches!(err, KernelError::Runtime(_)));

    let roots = recorded_roots.lock().expect("roots lock").clone();
    assert_eq!(roots.len(), 2);
    assert!(
        !roots[1].join(RUNTIME_SESSION_READY_MARKER).is_file(),
        "failed turns should clear resumability before the next follow-up"
    );

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "third request".to_string(),
            runtime_id: Some("recovering".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("third turn");

    let resumed_flags = resumed_flags.lock().expect("resumed flags lock").clone();
    assert_eq!(resumed_flags, vec![false, true, false]);
}

#[tokio::test]
async fn retry_restarts_harness_session_from_canonical_history() {
    let env = TestEnv::new();
    let workspace_root = env.path().join("workspace");
    let project_root = env.path().join("project");
    let runtime_root = env.path().join("runtime");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(workspace_root),
            project_workspace_root: Some(project_root),
            runtime_root: Some(runtime_root),
            workspace_name: Some("main".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    let recorded_roots = Arc::new(Mutex::new(Vec::new()));
    let resumed_flags = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "resumable",
            Arc::new(ResumableCaptureAdapter {
                prompts: recorded_prompts.clone(),
                runtime_roots: recorded_roots.clone(),
                resumed_flags: resumed_flags.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(&kernel, "retry-peer", SessionHistoryPolicy::Interactive).await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "first request".to_string(),
            runtime_id: Some("resumable".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("first turn");

    kernel
        .run_session_action(
            session.session_id,
            SessionActionKind::RetryLastTurn,
            Some("resumable".to_string()),
        )
        .await
        .expect("retry turn");

    let prompts = recorded_prompts.lock().expect("prompt lock").clone();
    assert_eq!(prompts.len(), 2);
    assert!(
        !prompts[1].contains("## Runtime Session"),
        "retry should force a fresh harness session rather than sending a continuation note"
    );

    let roots = recorded_roots.lock().expect("roots lock").clone();
    assert_eq!(roots.len(), 2);
    assert_eq!(
        roots[0], roots[1],
        "retry should reuse the same session-scoped root path"
    );
    let resumed_flags = resumed_flags.lock().expect("resumed flags lock").clone();
    assert_eq!(resumed_flags, vec![false, false]);
}

#[tokio::test]
async fn runtime_profile_change_invalidates_resumable_state() {
    let env = TestEnv::new();
    let workspace_root = env.path().join("workspace");
    let project_root = env.path().join("project");
    let runtime_root = env.path().join("runtime");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::create_dir_all(&project_root).expect("create project root");

    let recorded_prompts = Arc::new(Mutex::new(Vec::new()));
    let recorded_roots = Arc::new(Mutex::new(Vec::new()));
    let resumed_flags = Arc::new(Mutex::new(Vec::new()));

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(workspace_root.clone()),
            project_workspace_root: Some(project_root.clone()),
            runtime_root: Some(runtime_root.clone()),
            workspace_name: Some("main".to_string()),
            runtime_execution_profiles: [(
                "resumable".to_string(),
                test_runtime_profile(&env, "runtime-v1"),
            )]
            .into_iter()
            .collect(),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "resumable",
            Arc::new(ResumableCaptureAdapter {
                prompts: recorded_prompts.clone(),
                runtime_roots: recorded_roots.clone(),
                resumed_flags: resumed_flags.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_session(
        &kernel,
        "runtime-profile-peer",
        SessionHistoryPolicy::Interactive,
    )
    .await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "first request".to_string(),
            runtime_id: Some("resumable".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("first turn");

    drop(kernel);

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(workspace_root),
            project_workspace_root: Some(project_root),
            runtime_root: Some(runtime_root),
            workspace_name: Some("main".to_string()),
            runtime_execution_profiles: [(
                "resumable".to_string(),
                test_runtime_profile(&env, "runtime-v2"),
            )]
            .into_iter()
            .collect(),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel reinit");

    kernel
        .register_runtime_adapter(
            "resumable",
            Arc::new(ResumableCaptureAdapter {
                prompts: recorded_prompts.clone(),
                runtime_roots: recorded_roots.clone(),
                resumed_flags: resumed_flags.clone(),
                reply: "captured".to_string(),
            }),
        )
        .await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "second request".to_string(),
            runtime_id: Some("resumable".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("second turn");

    let roots = recorded_roots.lock().expect("roots lock").clone();
    assert_eq!(roots.len(), 2);
    assert_ne!(roots[0], roots[1]);

    let resumed_flags = resumed_flags.lock().expect("resumed flags lock").clone();
    assert_eq!(resumed_flags, vec![false, false]);
}

async fn open_session(
    kernel: &Kernel,
    peer_id: &str,
    history_policy: SessionHistoryPolicy,
) -> lionclaw::contracts::SessionOpenResponse {
    kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(history_policy),
        })
        .await
        .expect("open session")
}

struct PartialTimeoutAdapter {
    partial_text: String,
    sleep_for: Duration,
}

struct CapturePromptAdapter {
    prompts: Arc<Mutex<Vec<String>>>,
    reply: String,
}

struct ResumableCaptureAdapter {
    prompts: Arc<Mutex<Vec<String>>>,
    runtime_roots: Arc<Mutex<Vec<PathBuf>>>,
    resumed_flags: Arc<Mutex<Vec<bool>>>,
    reply: String,
}

struct ResumableFlakyAdapter {
    resumed_flags: Arc<Mutex<Vec<bool>>>,
    turn_counter: AtomicUsize,
}

struct ResumeAfterFailureAdapter {
    runtime_roots: Arc<Mutex<Vec<PathBuf>>>,
    resumed_flags: Arc<Mutex<Vec<bool>>>,
    turn_counter: AtomicUsize,
}

struct BlockingAnswerAdapter {
    answer: String,
    sleep_for: Duration,
}

#[async_trait]
impl RuntimeAdapter for PartialTimeoutAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "partial".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("partial-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: self.partial_text.clone(),
        });
        tokio::time::sleep(self.sleep_for).await;
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for CapturePromptAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "capture".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("capture-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.prompts.lock().expect("prompt lock").push(input.prompt);
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: self.reply.clone(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for ResumableCaptureAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "resumable".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_state_root = input
            .runtime_state_root
            .clone()
            .expect("runtime state root should be provided");
        let resumes_existing_session = runtime_state_root
            .join(RUNTIME_SESSION_READY_MARKER)
            .is_file();
        self.runtime_roots
            .lock()
            .expect("roots lock")
            .push(runtime_state_root);
        self.resumed_flags
            .lock()
            .expect("resumed flags lock")
            .push(resumes_existing_session);

        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("resumable-{}", Uuid::new_v4()),
            resumes_existing_session,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.prompts.lock().expect("prompt lock").push(input.prompt);
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: self.reply.clone(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for ResumableFlakyAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "flaky".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_state_root = input
            .runtime_state_root
            .clone()
            .expect("runtime state root should be provided");
        let resumes_existing_session = runtime_state_root
            .join(RUNTIME_SESSION_READY_MARKER)
            .is_file();
        self.resumed_flags
            .lock()
            .expect("resumed flags lock")
            .push(resumes_existing_session);

        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("flaky-{}", Uuid::new_v4()),
            resumes_existing_session,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let turn_index = self.turn_counter.fetch_add(1, Ordering::SeqCst);
        if turn_index == 0 {
            let _ = events.send(RuntimeEvent::MessageDelta {
                lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
                text: "partial".to_string(),
            });
            let _ = events.send(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: "failed after partial".to_string(),
            });
            anyhow::bail!("failed after partial");
        }

        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: "continued".to_string(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for ResumeAfterFailureAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "recovering".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_state_root = input
            .runtime_state_root
            .clone()
            .expect("runtime state root should be provided");
        let resumes_existing_session = runtime_state_root
            .join(RUNTIME_SESSION_READY_MARKER)
            .is_file();
        self.runtime_roots
            .lock()
            .expect("roots lock")
            .push(runtime_state_root);
        self.resumed_flags
            .lock()
            .expect("resumed flags lock")
            .push(resumes_existing_session);

        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("recovering-{}", Uuid::new_v4()),
            resumes_existing_session,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        match self.turn_counter.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let _ = events.send(RuntimeEvent::MessageDelta {
                    lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
                    text: "first success".to_string(),
                });
                let _ = events.send(RuntimeEvent::Done);
                Ok(RuntimeTurnResult::default())
            }
            1 => {
                let _ = events.send(RuntimeEvent::MessageDelta {
                    lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
                    text: "partial failure".to_string(),
                });
                let _ = events.send(RuntimeEvent::Error {
                    code: Some("runtime.error".to_string()),
                    text: "failed after success".to_string(),
                });
                anyhow::bail!("failed after success");
            }
            _ => {
                let _ = events.send(RuntimeEvent::MessageDelta {
                    lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
                    text: "fresh recovery".to_string(),
                });
                let _ = events.send(RuntimeEvent::Done);
                Ok(RuntimeTurnResult::default())
            }
        }
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for BlockingAnswerAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "blocking".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("blocking-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: self.answer.clone(),
        });
        tokio::time::sleep(self.sleep_for).await;
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

struct TestEnv {
    temp_dir: TempDir,
}

fn test_runtime_profile(env: &TestEnv, compatibility_key: &str) -> RuntimeExecutionProfile {
    RuntimeExecutionProfile::new(
        ConfinementConfig::Oci(OciConfinementConfig {
            engine: env.fake_podman_path().display().to_string(),
            image: Some("ghcr.io/lionclaw/test-resumable-runtime:latest".to_string()),
            ..OciConfinementConfig::default()
        }),
        compatibility_key.to_string(),
        None,
        None,
    )
}

impl TestEnv {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create temp dir"),
        }
    }

    fn path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    fn db_path(&self) -> PathBuf {
        self.path().join("lionclaw.db")
    }

    fn fake_podman_path(&self) -> PathBuf {
        let path = self.path().join("podman");
        if !path.exists() {
            std::fs::write(
                &path,
                "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
            )
            .expect("write fake podman");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake podman");
        }
        path
    }
}

async fn connect_pool(db_path: &std::path::Path) -> SqlitePool {
    let db_url = format!("sqlite://{}", db_path.display());
    SqlitePool::connect(&db_url).await.expect("connect db")
}

struct SessionTurnSeed<'a> {
    session_id: Uuid,
    sequence_no: i64,
    status: SessionTurnStatus,
    prompt_user_text: &'a str,
    assistant_text: &'a str,
    error_code: Option<&'a str>,
    error_text: Option<&'a str>,
}

async fn insert_session_turn(db_path: &std::path::Path, seed: SessionTurnSeed<'_>) {
    let pool = connect_pool(db_path).await;
    sqlx::query(
        "INSERT INTO session_turns \
         (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms) \
         VALUES (?1, ?2, ?3, 'normal', ?4, ?5, ?6, ?7, ?8, ?9, 'mock', 1, CASE WHEN ?4 = 'running' THEN NULL ELSE 2 END)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(seed.session_id.to_string())
    .bind(seed.sequence_no)
    .bind(seed.status.as_str())
    .bind(seed.prompt_user_text)
    .bind(seed.prompt_user_text)
    .bind(seed.assistant_text)
    .bind(seed.error_code)
    .bind(seed.error_text)
    .execute(&pool)
    .await
    .expect("insert session turn");
}

async fn wait_for_turn_checkpoint(db_path: &std::path::Path, session_id: Uuid, expected: &str) {
    let pool = connect_pool(db_path).await;
    for _ in 0..40 {
        let row = sqlx::query(
            "SELECT assistant_text \
             FROM session_turns \
             WHERE session_id = ?1 \
             ORDER BY sequence_no DESC \
             LIMIT 1",
        )
        .bind(session_id.to_string())
        .fetch_optional(&pool)
        .await
        .expect("query assistant text");
        if row
            .as_ref()
            .map(|value| value.get::<String, _>("assistant_text"))
            .is_some_and(|text| text == expected)
        {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("assistant checkpoint never reached expected value");
}
