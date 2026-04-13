use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use lionclaw::{
    contracts::{SessionOpenRequest, SessionTurnRequest, TrustTier},
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
async fn runtime_timeout_triggers_cancel_close_and_audit() {
    let sandbox = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &sandbox.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(200),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let cancel_calls = Arc::new(AtomicUsize::new(0));
    let close_calls = Arc::new(AtomicUsize::new(0));
    kernel
        .register_runtime_adapter(
            "slow",
            Arc::new(SlowRuntimeAdapter {
                cancel_calls: cancel_calls.clone(),
                close_calls: close_calls.clone(),
                sleep_for: Duration::from_millis(250),
            }),
        )
        .await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "timeout-peer".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "trigger timeout".to_string(),
            runtime_id: Some("slow".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should timeout");

    match err {
        KernelError::RuntimeTimeout(message) => {
            assert!(
                message.contains("idle timed out"),
                "timeout error should include timeout reason"
            );
        }
        other => panic!("unexpected error variant: {}", other),
    }

    assert_eq!(
        cancel_calls.load(Ordering::SeqCst),
        1,
        "cancel should be called once on timeout"
    );
    assert_eq!(
        close_calls.load(Ordering::SeqCst),
        1,
        "close should be called once even when turn times out"
    );

    let timeout_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.turn.timeout".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query timeout audit");

    assert_eq!(timeout_events.events.len(), 1, "one timeout event expected");
    assert_eq!(
        timeout_events.events[0].details["runtime_id"].as_str(),
        Some("slow"),
        "timeout audit should include runtime id"
    );
    assert_eq!(
        timeout_events.events[0].details["timeout_kind"].as_str(),
        Some("idle"),
        "timeout audit should distinguish idle timeout"
    );
}

#[tokio::test]
async fn runtime_activity_resets_idle_timeout() {
    let sandbox = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &sandbox.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(50),
            runtime_turn_hard_timeout: Duration::from_millis(400),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "chatty",
            Arc::new(ChattyRuntimeAdapter {
                idle_gap: Duration::from_millis(20),
                event_count: 5,
            }),
        )
        .await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "chatty-peer".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "stay alive while events keep flowing".to_string(),
            runtime_id: Some("chatty".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should stay alive");

    assert_eq!(
        turn.assistant_text, "done",
        "chatty runtime should complete"
    );
}

struct SlowRuntimeAdapter {
    cancel_calls: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
    sleep_for: Duration,
}

struct ChattyRuntimeAdapter {
    idle_gap: Duration,
    event_count: usize,
}

#[async_trait]
impl RuntimeAdapter for SlowRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "slow".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("slow-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        tokio::time::sleep(self.sleep_for).await;
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
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
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl RuntimeAdapter for ChattyRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "chatty".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("chatty-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        for _ in 0..self.event_count {
            let _ = events.send(RuntimeEvent::Status {
                code: None,
                text: "still working".to_string(),
            });
            tokio::time::sleep(self.idle_gap).await;
        }
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: lionclaw::kernel::runtime::RuntimeMessageLane::Answer,
            text: "done".to_string(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
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

    fn db_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("lionclaw.db")
    }
}
