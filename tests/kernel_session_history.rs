use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use lionclaw::{
    contracts::{
        SessionActionKind, SessionHistoryPolicy, SessionHistoryRequest, SessionOpenRequest,
        SessionTurnKind, SessionTurnRequest, SessionTurnStatus, TrustTier,
    },
    kernel::{
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
            RuntimeEventSender, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
            RuntimeTurnResult,
        },
        Kernel, KernelError, KernelOptions,
    },
};
use tempfile::TempDir;
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

struct TestEnv {
    temp_dir: TempDir,
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
}
