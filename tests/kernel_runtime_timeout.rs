use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use lionclaw::{
    contracts::{SessionOpenRequest, SessionTurnRequest, SessionTurnStatus, TrustTier},
    kernel::{
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeControlExecution,
            RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender, RuntimeSessionHandle,
            RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnResult,
        },
        Kernel, KernelOptions,
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

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "trigger timeout".to_string(),
            runtime_id: Some("slow".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should resolve as timed out");
    assert_eq!(turn.status, SessionTurnStatus::TimedOut);
    assert_eq!(turn.error_code.as_deref(), Some("runtime.timeout"));
    assert!(
        turn.error_text
            .as_deref()
            .is_some_and(|message| message.contains("idle timed out")),
        "timeout response should include timeout reason"
    );

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
async fn runtime_timeout_handles_closed_event_stream_without_output() {
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
            "closed-events",
            Arc::new(ClosedEventStreamRuntimeAdapter {
                id: "closed-events",
                cancel_calls: cancel_calls.clone(),
                close_calls: close_calls.clone(),
                sleep_for: Duration::from_millis(250),
            }),
        )
        .await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "closed-event-stream-peer".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "close event stream before output".to_string(),
            runtime_id: Some("closed-events".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should resolve as timed out after the runtime closes its event stream");

    assert_eq!(turn.status, SessionTurnStatus::TimedOut);
    assert_eq!(turn.error_code.as_deref(), Some("runtime.timeout"));
    assert!(
        turn.error_text
            .as_deref()
            .is_some_and(|message| message.contains("idle timed out")),
        "closed event stream should still resolve through the normal timeout path"
    );
    assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn runtime_control_timeout_handles_closed_event_stream_without_output() {
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
            "closed-control-events",
            Arc::new(ClosedEventStreamRuntimeAdapter {
                id: "closed-control-events",
                cancel_calls: cancel_calls.clone(),
                close_calls: close_calls.clone(),
                sleep_for: Duration::from_millis(250),
            }),
        )
        .await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "closed-control-event-stream-peer".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "/closed-events".to_string(),
            runtime_id: Some("closed-control-events".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("control should resolve as timed out after the runtime closes its event stream");

    assert_eq!(turn.status, SessionTurnStatus::TimedOut);
    assert_eq!(turn.error_code.as_deref(), Some("runtime.timeout"));
    assert!(
        turn.error_text
            .as_deref()
            .is_some_and(|message| message.contains("idle timed out")),
        "closed control event stream should still resolve through the normal timeout path"
    );
    assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(close_calls.load(Ordering::SeqCst), 1);

    let timeout_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.control.timeout".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query timeout audit");

    assert_eq!(timeout_events.events.len(), 1, "one timeout event expected");
    assert_eq!(
        timeout_events.events[0].details["runtime_id"].as_str(),
        Some("closed-control-events"),
        "control timeout audit should include runtime id"
    );
    assert_eq!(
        timeout_events.events[0].details["timeout_kind"].as_str(),
        Some("idle"),
        "control timeout audit should distinguish idle timeout"
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

#[tokio::test]
async fn runtime_hard_timeout_reports_safety_limit_and_audit_kind() {
    let sandbox = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &sandbox.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(100),
            runtime_turn_hard_timeout: Duration::from_millis(250),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "chatty",
            Arc::new(ChattyRuntimeAdapter {
                idle_gap: Duration::from_millis(10),
                event_count: 100,
            }),
        )
        .await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "hard-timeout-peer".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "keep producing events until the hard ceiling".to_string(),
            runtime_id: Some("chatty".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should resolve as hard timed out");
    assert_eq!(turn.status, SessionTurnStatus::TimedOut);
    assert_eq!(turn.error_code.as_deref(), Some("runtime.timeout"));
    assert!(
        turn.error_text
            .as_deref()
            .is_some_and(|message| message.contains("safety limit")),
        "hard timeout should be explained as a safety limit"
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
        Some("chatty"),
        "timeout audit should include runtime id"
    );
    assert_eq!(
        timeout_events.events[0].details["timeout_kind"].as_str(),
        Some("hard"),
        "timeout audit should distinguish hard timeout"
    );
    assert_eq!(
        timeout_events.events[0].details["timeout_ms"].as_u64(),
        Some(250),
        "timeout audit should include the hard timeout budget"
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

struct ClosedEventStreamRuntimeAdapter {
    id: &'static str,
    cancel_calls: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
    sleep_for: Duration,
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
impl RuntimeAdapter for ClosedEventStreamRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: self.id.to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("{}-{}", self.id, Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        drop(events);
        tokio::time::sleep(self.sleep_for).await;
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
    }

    async fn runtime_control(
        &self,
        _execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        drop(events);
        tokio::time::sleep(self.sleep_for).await;
        Ok(RuntimeControlOutcome::Handled {
            message: "control completed after closing events".to_string(),
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
