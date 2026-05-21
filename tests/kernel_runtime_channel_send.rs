mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use common::{write_skill_source, TestHome};
use lionclaw::{
    contracts::{
        ChannelOutboxPullRequest, SessionOpenRequest, SessionTurnRequest, SessionTurnStatus,
        TrustTier,
    },
    kernel::{
        runtime::{
            EffectiveExecutionPlan, EscapeClass, ExecutionPreset, NetworkMode, RuntimeAdapter,
            RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender,
            RuntimeProgramTurnExecution, RuntimeSessionHandle, RuntimeSessionStartInput,
            RuntimeTurnMode, RuntimeTurnResult, WorkspaceAccess,
        },
        Kernel, KernelOptions,
    },
    operator::config::ChannelLaunchMode,
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::sleep,
};
use uuid::Uuid;

const CHANNEL_SEND_SOCKET_ENV: &str = "LIONCLAW_CHANNEL_SEND_SOCKET";

type RecordedEnvironments = Arc<Mutex<Vec<Vec<(String, String)>>>>;

#[derive(Clone, Copy)]
enum ProbeFileSetup {
    None,
    Attachment,
    EscapeSymlink,
}

#[tokio::test]
async fn program_backed_runtime_without_channel_send_escape_gets_no_socket_env() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-no-escape").await;
    let kernel = kernel_with_channel_send_preset(&env, false).await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "probe-runtime",
            Arc::new(ChannelSendProbeRuntime::record_environment(
                observed.clone(),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-no-escape").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "probe channel send env".to_string(),
            runtime_id: Some("probe-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let environments = observed.lock().expect("observed env lock");
    assert_eq!(environments.len(), 1);
    assert!(
        env_value(&environments[0], CHANNEL_SEND_SOCKET_ENV).is_none(),
        "runtime must not receive a channel.send bridge without the escape class"
    );
}

#[tokio::test]
async fn program_backed_runtime_with_channel_send_escape_enqueues_outbox_delivery() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-happy").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    let socket_paths = Arc::new(Mutex::new(Vec::new()));
    let request = json!({
        "idempotency_key": "send-design-sketch",
        "channel_id": "local-cli",
        "conversation_ref": "member:reviewer",
        "thread_ref": "design-thread",
        "reply_to_ref": "source-message",
        "content": {
            "text": "See attached sketch.",
            "format_hint": "markdown",
            "attachments": [{
                "path": "/runtime/artifacts/sketch.txt",
                "filename": "sketch.txt",
                "mime_type": "text/plain"
            }]
        }
    });
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::send_requests(
                vec![request],
                responses.clone(),
                socket_paths.clone(),
                ProbeFileSetup::Attachment,
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-happy").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send channel message".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["ok"].as_bool(), Some(true));
    let delivery_id = responses[0]["delivery_id"]
        .as_str()
        .expect("delivery id in response");

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "test-worker".to_string(),
            conversation_ref: Some("member:reviewer".to_string()),
            thread_ref: Some("design-thread".to_string()),
            limit: Some(10),
            lease_ms: None,
        })
        .await
        .expect("pull outbox");
    assert_eq!(outbox.deliveries.len(), 1);
    let delivery = &outbox.deliveries[0];
    assert_eq!(delivery.delivery_id.to_string(), delivery_id);
    assert_eq!(delivery.conversation_ref, "member:reviewer");
    assert_eq!(delivery.thread_ref.as_deref(), Some("design-thread"));
    assert_eq!(delivery.reply_to_ref.as_deref(), Some("source-message"));
    assert_eq!(delivery.content.text, "See attached sketch.");
    assert_eq!(delivery.content.format_hint, "markdown");
    assert_eq!(delivery.content.attachments.len(), 1);
    assert_eq!(
        delivery.content.attachments[0].filename.as_deref(),
        Some("sketch.txt")
    );
    assert_eq!(
        delivery.content.attachments[0].mime_type.as_deref(),
        Some("text/plain")
    );

    let sockets = socket_paths.lock().expect("socket paths lock");
    let socket = &sockets[0];
    assert!(
        !socket.exists(),
        "channel.send socket should be removed after turn completion"
    );
}

#[tokio::test]
async fn channel_send_bridge_is_idempotent_for_same_payload_and_conflicts_on_change() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-idempotent").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    let base_request = json!({
        "idempotency_key": "retryable-call",
        "channel_id": "local-cli",
        "conversation_ref": "member:reviewer",
        "content": {
            "text": "same payload",
            "format_hint": "plain"
        }
    });
    let changed_request = json!({
        "idempotency_key": "retryable-call",
        "channel_id": "local-cli",
        "conversation_ref": "member:reviewer",
        "content": {
            "text": "different payload",
            "format_hint": "plain"
        }
    });
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::send_requests(
                vec![base_request.clone(), base_request, changed_request],
                responses.clone(),
                Arc::new(Mutex::new(Vec::new())),
                ProbeFileSetup::None,
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-idempotent").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send idempotent messages".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["ok"].as_bool(), Some(true));
    assert_eq!(responses[1]["ok"].as_bool(), Some(true));
    assert_eq!(responses[0]["delivery_id"], responses[1]["delivery_id"]);
    assert_eq!(responses[2]["ok"].as_bool(), Some(false));
    assert_eq!(responses[2]["error"]["code"].as_str(), Some("conflict"));

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "test-worker".to_string(),
            conversation_ref: Some("member:reviewer".to_string()),
            thread_ref: None,
            limit: Some(10),
            lease_ms: None,
        })
        .await
        .expect("pull outbox");
    assert_eq!(
        outbox.deliveries.len(),
        1,
        "idempotent retry must not enqueue a duplicate delivery"
    );
}

#[tokio::test]
async fn channel_send_bridge_returns_structured_validation_errors() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-validation").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::send_requests(
                vec![
                    json!({
                        "idempotency_key": "bad-format",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "rtf"
                        }
                    }),
                    json!({
                        "idempotency_key": "empty-content",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "   ",
                            "format_hint": "plain"
                        }
                    }),
                    json!({
                        "idempotency_key": "unknown-channel",
                        "channel_id": "missing",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain"
                        }
                    }),
                    json!({
                        "idempotency_key": "attachment-escape",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain",
                            "attachments": [{
                                "path": "/runtime/../other-session/secret.txt"
                            }]
                        }
                    }),
                    json!({
                        "idempotency_key": "attachment-missing-path",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain",
                            "attachments": [{}]
                        }
                    }),
                    json!({
                        "idempotency_key": "attachment-symlink-escape",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain",
                            "attachments": [{
                                "path": "/runtime/escape-link/secret.txt"
                            }]
                        }
                    }),
                ],
                responses.clone(),
                Arc::new(Mutex::new(Vec::new())),
                ProbeFileSetup::EscapeSymlink,
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-validation").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send invalid messages".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    assert_eq!(responses.len(), 6);
    assert_eq!(responses[0]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[0]["error"]["code"].as_str(),
        Some("invalid_format")
    );
    assert_eq!(responses[1]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[1]["error"]["code"].as_str(),
        Some("empty_content")
    );
    assert_eq!(responses[2]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[2]["error"]["code"].as_str(),
        Some("unknown_channel")
    );
    assert_eq!(responses[3]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[3]["error"]["code"].as_str(),
        Some("invalid_attachment")
    );
    assert_eq!(responses[4]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[4]["error"]["code"].as_str(),
        Some("invalid_attachment")
    );
    assert_eq!(responses[5]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[5]["error"]["code"].as_str(),
        Some("invalid_attachment")
    );
    assert!(responses[4]["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("attachment path is required")));

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "test-worker".to_string(),
            conversation_ref: Some("member:reviewer".to_string()),
            thread_ref: None,
            limit: Some(10),
            lease_ms: None,
        })
        .await
        .expect("pull outbox");
    assert!(
        outbox.deliveries.is_empty(),
        "invalid channel.send requests must not enqueue deliveries"
    );
}

#[tokio::test]
async fn channel_send_bridge_reports_missing_required_fields_as_validation_errors() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-missing-fields").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::send_requests(
                vec![
                    json!({
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain"
                        }
                    }),
                    json!({
                        "idempotency_key": "missing-channel",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain"
                        }
                    }),
                    json!({
                        "idempotency_key": "missing-conversation",
                        "channel_id": "local-cli",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain"
                        }
                    }),
                    json!({
                        "idempotency_key": "missing-content",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer"
                    }),
                ],
                responses.clone(),
                Arc::new(Mutex::new(Vec::new())),
                ProbeFileSetup::None,
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-missing-fields").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send incomplete messages".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    assert_eq!(responses.len(), 4);
    for response in &responses {
        assert_eq!(response["ok"].as_bool(), Some(false));
        assert_eq!(response["error"]["code"].as_str(), Some("invalid_request"));
    }
    assert!(responses[0]["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("idempotency_key is required")));
    assert!(responses[1]["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("channel_id is required")));
    assert!(responses[2]["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("conversation_ref is required")));
    assert!(responses[3]["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("content is required")));
}

#[tokio::test]
async fn channel_send_bridge_socket_is_removed_after_timeout() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-timeout").await;
    let mut options = channel_send_kernel_options(&env, true);
    options.runtime_turn_idle_timeout = Duration::from_millis(50);
    options.runtime_turn_hard_timeout = Duration::from_millis(200);
    let kernel = env.kernel_with_options(options).await;
    let socket_paths = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "sleep-runtime",
            Arc::new(ChannelSendProbeRuntime::sleep_after_start(
                socket_paths.clone(),
                Duration::from_millis(250),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-timeout").await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "timeout with bridge".to_string(),
            runtime_id: Some("sleep-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("timeout should return a terminal turn response");

    assert_eq!(response.status, SessionTurnStatus::TimedOut);
    let sockets = socket_paths.lock().expect("socket paths lock");
    let socket = &sockets[0];
    assert!(
        !socket.exists(),
        "channel.send socket should be removed after timeout"
    );
}

#[tokio::test]
async fn channel_send_bridge_drops_open_connections_after_turn_completion() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-stale-connection").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let held_stream = Arc::new(Mutex::new(None));
    kernel
        .register_runtime_adapter(
            "stale-connection-runtime",
            Arc::new(ChannelSendProbeRuntime::hold_open_connection(
                held_stream.clone(),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-stale-connection").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "open stale channel send connection".to_string(),
            runtime_id: Some("stale-connection-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let mut stream = held_stream
        .lock()
        .expect("held stream lock")
        .take()
        .expect("runtime should hold an open socket connection");
    let request = json!({
        "idempotency_key": "after-turn-complete",
        "channel_id": "local-cli",
        "conversation_ref": "member:reviewer",
        "content": {
            "text": "this must not enqueue",
            "format_hint": "plain"
        }
    });
    let mut line = serde_json::to_vec(&request).expect("serialize request");
    line.push(b'\n');
    drop(stream.write_all(&line).await);
    drop(stream.shutdown().await);

    let mut response = String::new();
    let mut reader = BufReader::new(stream);
    if let Ok(Ok(_)) =
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response)).await
    {
        if !response.trim().is_empty() {
            let response: Value = serde_json::from_str(response.trim()).expect("decode response");
            assert_eq!(response["ok"].as_bool(), Some(false));
            assert_eq!(response["error"]["code"].as_str(), Some("bridge_closed"));
        }
    }
    sleep(Duration::from_millis(50)).await;

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "test-worker".to_string(),
            conversation_ref: Some("member:reviewer".to_string()),
            thread_ref: None,
            limit: Some(10),
            lease_ms: None,
        })
        .await
        .expect("pull outbox");
    assert!(
        outbox.deliveries.is_empty(),
        "channel.send must not enqueue after the turn-scoped bridge is dropped"
    );
}

#[derive(Clone)]
enum RuntimeAction {
    RecordEnvironment {
        observed: RecordedEnvironments,
    },
    SendRequests {
        requests: Vec<Value>,
        responses: Arc<Mutex<Vec<Value>>>,
        socket_paths: Arc<Mutex<Vec<PathBuf>>>,
        file_setup: ProbeFileSetup,
    },
    Sleep {
        socket_paths: Arc<Mutex<Vec<PathBuf>>>,
        duration: Duration,
    },
    HoldOpenConnection {
        held_stream: Arc<Mutex<Option<UnixStream>>>,
    },
}

struct ChannelSendProbeRuntime {
    action: RuntimeAction,
}

impl ChannelSendProbeRuntime {
    fn record_environment(observed: RecordedEnvironments) -> Self {
        Self {
            action: RuntimeAction::RecordEnvironment { observed },
        }
    }

    fn send_requests(
        requests: Vec<Value>,
        responses: Arc<Mutex<Vec<Value>>>,
        socket_paths: Arc<Mutex<Vec<PathBuf>>>,
        file_setup: ProbeFileSetup,
    ) -> Self {
        Self {
            action: RuntimeAction::SendRequests {
                requests,
                responses,
                socket_paths,
                file_setup,
            },
        }
    }

    fn sleep_after_start(socket_paths: Arc<Mutex<Vec<PathBuf>>>, duration: Duration) -> Self {
        Self {
            action: RuntimeAction::Sleep {
                socket_paths,
                duration,
            },
        }
    }

    fn hold_open_connection(held_stream: Arc<Mutex<Option<UnixStream>>>) -> Self {
        Self {
            action: RuntimeAction::HoldOpenConnection { held_stream },
        }
    }
}

#[async_trait]
impl RuntimeAdapter for ChannelSendProbeRuntime {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "channel-send-probe".to_string(),
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
            runtime_session_id: format!("channel-send-probe-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        run_probe_action(&self.action, &execution.plan).await?;
        drop(events.send(RuntimeEvent::Done));
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        _events: RuntimeEventSender,
    ) -> Result<()> {
        if results.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("probe runtime does not resolve capabilities"))
        }
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

async fn run_probe_action(action: &RuntimeAction, plan: &EffectiveExecutionPlan) -> Result<()> {
    match action {
        RuntimeAction::RecordEnvironment { observed } => {
            observed
                .lock()
                .expect("observed env lock")
                .push(plan.environment.clone());
            Ok(())
        }
        RuntimeAction::SendRequests {
            requests,
            responses,
            socket_paths,
            file_setup,
        } => {
            let runtime_root = runtime_mount_source(plan)?;
            let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
                .context("channel send socket env missing")?;
            let host_socket = host_path_for_runtime_path(plan, &socket)?;
            socket_paths
                .lock()
                .expect("socket paths lock")
                .push(host_socket);
            prepare_probe_files(&runtime_root, *file_setup).await?;
            for request in requests {
                let response = send_channel_send_request(plan, request.clone()).await?;
                responses.lock().expect("responses lock").push(response);
            }
            Ok(())
        }
        RuntimeAction::Sleep {
            socket_paths,
            duration,
        } => {
            let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
                .context("channel send socket env missing")?;
            let host_socket = host_path_for_runtime_path(plan, &socket)?;
            if !host_socket.exists() {
                return Err(anyhow!(
                    "channel send socket '{}' was not created",
                    host_socket.display()
                ));
            }
            socket_paths
                .lock()
                .expect("socket paths lock")
                .push(host_socket);
            sleep(*duration).await;
            Ok(())
        }
        RuntimeAction::HoldOpenConnection { held_stream } => {
            let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
                .context("channel send socket env missing")?;
            let host_socket = host_path_for_runtime_path(plan, &socket)?;
            let stream = UnixStream::connect(&host_socket)
                .await
                .with_context(|| format!("connect {}", host_socket.display()))?;
            *held_stream.lock().expect("held stream lock") = Some(stream);
            Ok(())
        }
    }
}

async fn prepare_probe_files(runtime_root: &Path, setup: ProbeFileSetup) -> Result<()> {
    match setup {
        ProbeFileSetup::None => Ok(()),
        ProbeFileSetup::Attachment => {
            let artifact = runtime_root.join("artifacts/sketch.txt");
            let artifact_parent = artifact.parent().context("artifact parent missing")?;
            tokio::fs::create_dir_all(artifact_parent)
                .await
                .context("create artifact parent")?;
            tokio::fs::write(&artifact, b"sketch bytes")
                .await
                .context("write artifact")?;
            Ok(())
        }
        ProbeFileSetup::EscapeSymlink => {
            let outside = runtime_root
                .parent()
                .context("runtime root parent missing")?
                .join("channel-send-symlink-escape");
            tokio::fs::create_dir_all(&outside)
                .await
                .context("create symlink escape target")?;
            tokio::fs::write(outside.join("secret.txt"), b"secret bytes")
                .await
                .context("write symlink escape target")?;
            std::os::unix::fs::symlink(&outside, runtime_root.join("escape-link"))
                .context("create symlink escape")?;
            Ok(())
        }
    }
}

async fn send_channel_send_request(plan: &EffectiveExecutionPlan, request: Value) -> Result<Value> {
    let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
        .context("channel send socket env missing")?;
    let host_socket = host_path_for_runtime_path(plan, &socket)?;
    let mut stream = UnixStream::connect(&host_socket)
        .await
        .with_context(|| format!("connect {}", host_socket.display()))?;
    let mut line = serde_json::to_vec(&request).expect("serialize request");
    line.push(b'\n');
    stream.write_all(&line).await.expect("write request");
    stream.shutdown().await.expect("shutdown request writer");

    let mut response = String::new();
    let mut reader = BufReader::new(stream);
    reader
        .read_line(&mut response)
        .await
        .expect("read response");
    serde_json::from_str(response.trim()).context("decode response")
}

fn env_value(environment: &[(String, String)], key: &str) -> Option<String> {
    environment
        .iter()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, value)| value.clone())
}

fn runtime_mount_source(plan: &EffectiveExecutionPlan) -> Result<PathBuf> {
    plan.mounts
        .iter()
        .find(|mount| mount.target == "/runtime")
        .map(|mount| mount.source.clone())
        .context("runtime mount missing")
}

fn host_path_for_runtime_path(
    plan: &EffectiveExecutionPlan,
    runtime_path: &str,
) -> Result<PathBuf> {
    let mount = plan
        .mounts
        .iter()
        .filter(|mount| {
            runtime_path == mount.target || runtime_path.starts_with(&format!("{}/", mount.target))
        })
        .max_by_key(|mount| mount.target.len())
        .context("no mount for runtime path")?;
    if runtime_path == mount.target {
        return Ok(mount.source.clone());
    }
    let relative = Path::new(runtime_path)
        .strip_prefix(&mount.target)
        .context("strip mount target")?;
    Ok(mount.source.join(relative))
}

async fn kernel_with_channel_send_preset(env: &TestHome, enabled: bool) -> Kernel {
    env.kernel_with_options(channel_send_kernel_options(env, enabled))
        .await
}

fn channel_send_kernel_options(env: &TestHome, enabled: bool) -> KernelOptions {
    let mut escape_classes = BTreeSet::new();
    if enabled {
        escape_classes.insert(EscapeClass::ChannelSend);
    }
    KernelOptions {
        default_preset_name: Some("test-preset".to_string()),
        execution_presets: BTreeMap::from([(
            "test-preset".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                mount_runtime_secrets: false,
                escape_classes,
            },
        )]),
        runtime_root: Some(env.home().runtime_dir()),
        workspace_name: Some("main".to_string()),
        ..KernelOptions::default()
    }
}

async fn install_and_bind_channel(env: &TestHome, channel_id: &str, skill_name: &str) {
    let skill_source = write_skill_source(
        env.temp_dir(),
        skill_name,
        &format!("{skill_name} for runtime channel send tests"),
        true,
    );
    env.install_skill(skill_name, &skill_source).await;
    env.add_channel(channel_id, skill_name, ChannelLaunchMode::Background)
        .await;
}

async fn open_test_session(kernel: &Kernel, peer_id: &str) -> Uuid {
    kernel
        .open_session(SessionOpenRequest {
            channel_id: "api".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session")
        .session_id
}
