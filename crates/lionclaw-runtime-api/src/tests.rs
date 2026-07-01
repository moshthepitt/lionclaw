use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{
    canonical_events, clear_state_value, execute_program_backed_turn, load_ready_state_value,
    safe_relative_path, ExecutionOutput, NetworkMode, RawTurnPayload, RuntimeAdapter,
    RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender,
    RuntimeExecutionContext, RuntimeMessageLane, RuntimeNativeHomeArtifactDir,
    RuntimePathProjection, RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec,
    RuntimeProgramTurnExecution, RuntimeRegistry, RuntimeSessionHandle, RuntimeSessionReady,
    RuntimeSessionStartInput, RuntimeTerminalConfig, RuntimeTurnInput, RuntimeTurnJournalSender,
    RuntimeTurnMode, TurnEvent, RUNTIME_SESSION_READY_MARKER,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};

#[test]
fn canonical_events_drops_retained_raw_payloads() {
    let journal = vec![
        TurnEvent::with_raw(
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "hi".to_string(),
            },
            RawTurnPayload {
                driver: "test-driver".to_string(),
                payload: "{\"method\":\"message/delta\"}".to_string(),
            },
        ),
        TurnEvent::canonical(RuntimeEvent::Done),
    ];

    let events: Vec<RuntimeEvent> = canonical_events(&journal).cloned().collect();

    assert_eq!(
        events,
        vec![
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "hi".to_string(),
            },
            RuntimeEvent::Done,
        ],
    );
}

#[test]
fn runtime_terminal_config_rejects_removed_resume_args() {
    let err = serde_json::from_value::<RuntimeTerminalConfig>(serde_json::json!({
        "resume-args": ["--session", "{session_id}"]
    }))
    .expect_err("removed terminal resume args should not be ignored");

    assert!(
        err.to_string().contains("unknown field"),
        "unexpected error: {err}"
    );
}

#[derive(Clone)]
struct StubAttempt {
    stdout_lines: Vec<String>,
    output: ExecutionOutput,
}

struct TestProgramAdapter {
    retry_used: std::sync::atomic::AtomicBool,
}

impl Default for TestProgramAdapter {
    fn default() -> Self {
        Self {
            retry_used: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl RuntimeAdapter for TestProgramAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "test-program".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: "test-program-session".to_string(),
            resumes_existing_session: false,
        })
    }

    fn build_turn_program(
        &self,
        input: &RuntimeTurnInput,
        context: &RuntimeExecutionContext,
    ) -> Result<RuntimeProgramSpec> {
        if context.network_mode != NetworkMode::On {
            return Err(anyhow!("unexpected runtime execution context"));
        }
        Ok(RuntimeProgramSpec {
            executable: "agent".to_string(),
            args: vec!["run".to_string()],
            environment: Vec::new(),
            stdin: input.prompt.clone(),
            auth: None,
        })
    }

    fn parse_program_output_line(&self, line: &str) -> Vec<RuntimeEvent> {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("answer:") {
            vec![RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: rest.to_string(),
            }]
        } else if let Some(rest) = line.strip_prefix("error:") {
            vec![RuntimeEvent::Error {
                code: Some("test.error".to_string()),
                text: rest.to_string(),
            }]
        } else if line == "done" {
            vec![RuntimeEvent::Done]
        } else {
            Vec::new()
        }
    }

    fn prepare_program_retry_after_failure(
        &self,
        _input: &RuntimeTurnInput,
        _output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
        _journal: &RuntimeTurnJournalSender,
    ) -> Result<bool> {
        Ok(!self
            .retry_used
            .swap(true, std::sync::atomic::Ordering::SeqCst))
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _events: RuntimeEventSender,
    ) -> Result<()> {
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

struct StubExecutor {
    attempts: Arc<Mutex<VecDeque<StubAttempt>>>,
}

impl StubExecutor {
    fn new(attempts: Vec<StubAttempt>) -> Self {
        Self {
            attempts: Arc::new(Mutex::new(VecDeque::from(attempts))),
        }
    }
}

#[async_trait]
impl RuntimeProgramExecutor for StubExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        stdout: mpsc::UnboundedSender<String>,
    ) -> Result<ExecutionOutput> {
        let attempt = self
            .attempts
            .lock()
            .map_err(|_| anyhow!("attempt lock poisoned"))?
            .pop_front()
            .ok_or_else(|| anyhow!("no attempt configured"))?;
        for line in attempt.stdout_lines {
            stdout.send(line).map_err(|_| anyhow!("stdout closed"))?;
        }
        Ok(attempt.output)
    }

    async fn execute_captured(&mut self, _program: RuntimeProgramSpec) -> Result<ExecutionOutput> {
        Err(anyhow!("captured execution is not used by this test"))
    }

    async fn spawn(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> Result<Box<dyn RuntimeProgramSession>> {
        Err(anyhow!("interactive execution is not used by this test"))
    }
}

fn success_output() -> ExecutionOutput {
    ExecutionOutput {
        exit_code: Some(0),
        ..ExecutionOutput::default()
    }
}

fn failed_output() -> ExecutionOutput {
    ExecutionOutput {
        exit_code: Some(1),
        stderr: b"failed".to_vec(),
        ..ExecutionOutput::default()
    }
}

fn turn_input(fresh_prompt: Option<String>) -> RuntimeTurnInput {
    RuntimeTurnInput {
        runtime_session_id: "session".to_string(),
        prompt: "hello".to_string(),
        fresh_prompt,
        runtime_skill_ids: Vec::new(),
    }
}

fn execution_context() -> RuntimeExecutionContext {
    RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: None,
        runtime_path_projections: Vec::new(),
        mcp_servers: Vec::new(),
    }
}

#[tokio::test]
async fn runtime_registry_lists_ids_deterministically() {
    let registry = RuntimeRegistry::new();
    registry
        .register("z-runtime", Arc::new(TestProgramAdapter::default()))
        .await;
    registry
        .register("a-runtime", Arc::new(TestProgramAdapter::default()))
        .await;

    assert_eq!(
        registry.list().await,
        vec!["a-runtime".to_string(), "z-runtime".to_string()]
    );
}

#[test]
fn runtime_execution_context_resolves_longest_runtime_path_projection() {
    let context = RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: Some(PathBuf::from("/host/runtime-root")),
        runtime_path_projections: vec![
            RuntimePathProjection::directory("/runtime", "/host/runtime-root")
                .expect("valid runtime projection"),
            RuntimePathProjection::exact(
                "/runtime/lionclaw/channel-send.sock",
                "/tmp/lionclaw/cs.sock",
            )
            .expect("valid runtime projection"),
        ],
        mcp_servers: Vec::new(),
    };

    assert_eq!(
        context.host_path_for_runtime_path("/runtime/artifacts/sketch.txt"),
        Some(PathBuf::from("/host/runtime-root/artifacts/sketch.txt"))
    );
    assert_eq!(
        context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock"),
        Some(PathBuf::from("/tmp/lionclaw/cs.sock"))
    );
    assert_eq!(
        context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock/file"),
        None
    );
    assert_eq!(context.host_path_for_runtime_path("/runtime2/file"), None);
    assert_eq!(
        context.host_path_for_runtime_path("/runtime/./artifacts/sketch.txt"),
        Some(PathBuf::from("/host/runtime-root/artifacts/sketch.txt"))
    );
    assert_eq!(
        context.host_path_for_runtime_path("/runtime/../outside"),
        None
    );
    assert_eq!(
        context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock/../outside"),
        None
    );
}

#[test]
fn runtime_execution_context_keeps_equal_depth_blocks_order_independent() {
    let context = RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: None,
        runtime_path_projections: vec![
            RuntimePathProjection::exact("/runtime/channel.sock", "/tmp/channel.sock")
                .expect("valid runtime projection"),
            RuntimePathProjection::directory("/runtime/channel.sock", "/tmp/channel-tree")
                .expect("valid runtime projection"),
        ],
        mcp_servers: Vec::new(),
    };

    assert_eq!(
        context.host_path_for_runtime_path("/runtime/channel.sock/file"),
        None
    );
}

#[test]
fn runtime_session_ready_gates_ready_state_loading() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path();
    std::fs::write(runtime_state_root.join("session-id"), "session_saved\n")
        .expect("write state file");

    assert_eq!(
        load_ready_state_value(
            runtime_state_root,
            "session-id",
            "test session",
            RuntimeSessionReady::not_ready()
        )
        .expect("load state"),
        None
    );

    let missing_marker_ready = RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
        .expect("missing marker is not an error");
    assert!(!missing_marker_ready.is_ready());
    assert_eq!(
        load_ready_state_value(
            runtime_state_root,
            "session-id",
            "test session",
            missing_marker_ready
        )
        .expect("load state"),
        None
    );

    std::fs::write(
        runtime_state_root.join(RUNTIME_SESSION_READY_MARKER),
        "ready\n",
    )
    .expect("write ready marker");
    let runtime_session_ready = RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
        .expect("ready marker should load");
    assert!(runtime_session_ready.is_ready());
    assert_eq!(
        load_ready_state_value(
            runtime_state_root,
            "session-id",
            "test session",
            runtime_session_ready
        )
        .expect("load state"),
        Some("session_saved".to_string())
    );
}

#[test]
fn clear_state_value_removes_regular_state_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path();
    let state_file = runtime_state_root.join("session-id");
    std::fs::write(&state_file, "session_saved\n").expect("write state file");

    clear_state_value(runtime_state_root, "session-id", "test session").expect("clear state");

    assert!(
        !state_file.exists(),
        "state file should be removed after clear"
    );
    clear_state_value(runtime_state_root, "session-id", "test session")
        .expect("clearing a missing state file is idempotent");
}

#[cfg(unix)]
#[test]
fn clear_state_value_rejects_symlinked_state_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path();
    let outside = temp_dir.path().join("outside");
    std::fs::write(&outside, "do not remove\n").expect("write outside file");
    std::os::unix::fs::symlink(&outside, runtime_state_root.join("session-id"))
        .expect("create state symlink");

    let err = clear_state_value(runtime_state_root, "session-id", "test session")
        .expect_err("state symlink should be rejected");

    assert!(err.to_string().contains("cannot be a symlink"));
    assert!(outside.exists(), "clear must not follow state symlinks");
}

#[test]
fn runtime_path_projection_rejects_unvalidated_paths() {
    let relative_runtime_path = RuntimePathProjection::directory("runtime", "/host/runtime-root")
        .expect_err("relative runtime path should be rejected");
    assert!(
        relative_runtime_path
            .to_string()
            .contains("must be absolute"),
        "unexpected error: {relative_runtime_path}"
    );

    let parent_runtime_path =
        RuntimePathProjection::directory("/runtime/../outside", "/host/runtime-root")
            .expect_err("parent runtime path should be rejected");
    assert!(
        parent_runtime_path
            .to_string()
            .contains("must be normalized"),
        "unexpected error: {parent_runtime_path}"
    );

    let relative_host_path = RuntimePathProjection::exact("/runtime/channel.sock", "channel.sock")
        .expect_err("relative host path should be rejected");
    assert!(
        relative_host_path.to_string().contains("host path"),
        "unexpected error: {relative_host_path}"
    );
}

#[test]
fn safe_relative_path_normalizes_without_parent_traversal() {
    assert_eq!(
        safe_relative_path("artifacts/./sketch.txt"),
        Some(PathBuf::from("artifacts/sketch.txt"))
    );
    assert_eq!(safe_relative_path("."), Some(PathBuf::new()));
    assert_eq!(safe_relative_path("../outside"), None);
    assert_eq!(safe_relative_path("/absolute"), None);
}

#[test]
fn native_home_artifact_dir_accepts_only_non_empty_relative_paths() {
    let dir = RuntimeNativeHomeArtifactDir::new("artifacts/./images")
        .expect("relative artifact dir should be accepted");
    assert_eq!(dir.relative_path(), PathBuf::from("artifacts/images"));

    assert!(RuntimeNativeHomeArtifactDir::new(".").is_err());
    assert!(RuntimeNativeHomeArtifactDir::new("../outside").is_err());
    assert!(RuntimeNativeHomeArtifactDir::new("/runtime/home/images").is_err());
}

#[tokio::test]
async fn program_backed_turn_streams_output_and_finishes() {
    let adapter = TestProgramAdapter::default();
    let executor = StubExecutor::new(vec![StubAttempt {
        stdout_lines: vec!["answer:hello".to_string()],
        output: success_output(),
    }]);
    let (events, mut event_rx) = mpsc::unbounded_channel();

    let result = execute_program_backed_turn(
        &adapter,
        RuntimeProgramTurnExecution {
            input: turn_input(None),
            context: execution_context(),
            executor: Box::new(executor),
        },
        events,
    )
    .await
    .expect("turn should succeed");

    assert!(result.capability_requests.is_empty());
    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "hello"
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent {
            event: RuntimeEvent::Done,
            raw: None
        })
    ));
}

#[tokio::test]
async fn program_backed_turn_retries_with_fresh_prompt_before_emitting_error() {
    let adapter = TestProgramAdapter::default();
    let executor = StubExecutor::new(vec![
        StubAttempt {
            stdout_lines: vec!["error:stale".to_string()],
            output: failed_output(),
        },
        StubAttempt {
            stdout_lines: vec!["answer:fresh".to_string()],
            output: success_output(),
        },
    ]);
    let (events, mut event_rx) = mpsc::unbounded_channel();

    execute_program_backed_turn(
        &adapter,
        RuntimeProgramTurnExecution {
            input: turn_input(Some("fresh prompt".to_string())),
            context: execution_context(),
            executor: Box::new(executor),
        },
        events,
    )
    .await
    .expect("turn should retry and succeed");

    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "fresh"
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent {
            event: RuntimeEvent::Done,
            raw: None
        })
    ));
    assert!(
        event_rx.try_recv().is_err(),
        "stale error should stay buffered"
    );
}

#[tokio::test]
async fn program_backed_turn_surfaces_failure_after_retry() {
    let adapter = TestProgramAdapter::default();
    let executor = StubExecutor::new(vec![
        StubAttempt {
            stdout_lines: vec!["error:first".to_string()],
            output: failed_output(),
        },
        StubAttempt {
            stdout_lines: vec!["error:second".to_string()],
            output: failed_output(),
        },
    ]);
    let (events, mut event_rx) = mpsc::unbounded_channel();

    let err = execute_program_backed_turn(
        &adapter,
        RuntimeProgramTurnExecution {
            input: turn_input(Some("fresh prompt".to_string())),
            context: execution_context(),
            executor: Box::new(executor),
        },
        events,
    )
    .await
    .expect_err("turn should fail");

    assert!(err.to_string().contains("second"));
    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent { event: RuntimeEvent::Error { text, .. }, raw: None }) if text == "second"
    ));
    assert!(
        event_rx.try_recv().is_err(),
        "stale retry error should stay buffered"
    );
}

#[tokio::test]
async fn program_backed_turn_does_not_deadlock_when_process_finishes_before_stdout() {
    let adapter = TestProgramAdapter::default();
    let executor = StubExecutor::new(vec![StubAttempt {
        stdout_lines: Vec::new(),
        output: success_output(),
    }]);
    let (events, _event_rx) = mpsc::unbounded_channel();

    timeout(
        Duration::from_millis(100),
        execute_program_backed_turn(
            &adapter,
            RuntimeProgramTurnExecution {
                input: turn_input(None),
                context: execution_context(),
                executor: Box::new(executor),
            },
            events,
        ),
    )
    .await
    .expect("turn should not hang")
    .expect("turn should succeed");
}

#[tokio::test]
async fn program_backed_turn_observes_slow_stdout_before_completion() {
    struct SlowExecutor;

    #[async_trait]
    impl RuntimeProgramExecutor for SlowExecutor {
        async fn execute_streaming(
            &mut self,
            _program: RuntimeProgramSpec,
            stdout: mpsc::UnboundedSender<String>,
        ) -> Result<ExecutionOutput> {
            sleep(Duration::from_millis(20)).await;
            stdout
                .send("answer:slow".to_string())
                .map_err(|_| anyhow!("stdout closed"))?;
            Ok(success_output())
        }

        async fn execute_captured(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<ExecutionOutput> {
            Err(anyhow!("captured execution is not used by this test"))
        }

        async fn spawn(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<Box<dyn RuntimeProgramSession>> {
            Err(anyhow!("interactive execution is not used by this test"))
        }
    }

    let adapter = TestProgramAdapter::default();
    let executor = SlowExecutor;
    let (events, mut event_rx) = mpsc::unbounded_channel();

    execute_program_backed_turn(
        &adapter,
        RuntimeProgramTurnExecution {
            input: turn_input(None),
            context: execution_context(),
            executor: Box::new(executor),
        },
        events,
    )
    .await
    .expect("turn should succeed");

    assert!(matches!(
        event_rx.recv().await,
        Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "slow"
    ));
}
