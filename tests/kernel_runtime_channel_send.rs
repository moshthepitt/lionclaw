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
            RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeControlExecution,
            RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender, RuntimeProgramTurnExecution,
            RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
            RuntimeTurnResult, WorkspaceAccess,
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
const TEST_CHANNEL_SEND_CONNECTION_LIMIT: usize = 16;

type RecordedEnvironments = Arc<Mutex<Vec<Vec<(String, String)>>>>;

#[derive(Clone, Copy)]
enum ProbeFileSetup {
    None,
    Attachment,
    InvalidAttachments,
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
async fn direct_runtime_with_channel_send_escape_does_not_start_bridge() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-direct").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "direct-runtime",
            Arc::new(DirectSocketProbeRuntime {
                socket_dir: env.home().runtime_dir().join("sockets"),
                observed: observed.clone(),
            }),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-direct").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "direct runtime should use brokered capabilities".to_string(),
            runtime_id: Some("direct-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let observed = observed.lock().expect("observed socket lock").clone();
    assert_eq!(
        observed,
        vec![false],
        "direct runtimes must not start the program-backed channel.send bridge"
    );
}

#[tokio::test]
async fn runtime_control_with_channel_send_escape_gets_no_socket_env() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-control").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::record_environment(
                observed.clone(),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-control").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "/probe-control".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("runtime control should complete");

    let environments = observed.lock().expect("observed env lock");
    assert_eq!(environments.len(), 1);
    assert!(
        env_value(&environments[0], CHANNEL_SEND_SOCKET_ENV).is_none(),
        "runtime controls must not receive the program-backed channel.send bridge"
    );
}

#[tokio::test]
async fn program_backed_runtime_with_channel_send_escape_enqueues_outbox_delivery() {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-happy").await;
    let socket_dir = env.home().runtime_dir().join("sockets");
    tokio::fs::create_dir_all(&socket_dir)
        .await
        .expect("create loose socket dir");
    #[cfg(unix)]
    tokio::fs::set_permissions(&socket_dir, std::fs::Permissions::from_mode(0o755))
        .await
        .expect("loosen socket dir permissions");
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

    let socket = {
        let sockets = socket_paths.lock().expect("socket paths lock");
        sockets[0].clone()
    };
    assert!(
        !socket.exists(),
        "channel.send socket should be removed after turn completion"
    );
    #[cfg(unix)]
    {
        let mode = tokio::fs::metadata(&socket_dir)
            .await
            .expect("socket dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "channel.send socket directory is private");
    }
}

#[tokio::test]
async fn channel_send_bridge_allows_attachment_only_delivery() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-attachment-only").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    let request = json!({
        "idempotency_key": "send-attachment-only",
        "channel_id": "local-cli",
        "conversation_ref": "member:reviewer",
        "content": {
            "text": "   ",
            "format_hint": "plain",
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
                Arc::new(Mutex::new(Vec::new())),
                ProbeFileSetup::Attachment,
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-attachment-only").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send attachment-only channel message".to_string(),
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
    assert_eq!(outbox.deliveries.len(), 1);
    assert_eq!(outbox.deliveries[0].content.attachments.len(), 1);
    assert_eq!(
        outbox.deliveries[0].content.attachments[0]
            .filename
            .as_deref(),
        Some("sketch.txt")
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
                    json!({
                        "idempotency_key": "attachment-directory",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain",
                            "attachments": [{
                                "path": "/runtime/artifacts"
                            }]
                        }
                    }),
                    json!({
                        "idempotency_key": "attachment-final-symlink",
                        "channel_id": "local-cli",
                        "conversation_ref": "member:reviewer",
                        "content": {
                            "text": "hello",
                            "format_hint": "plain",
                            "attachments": [{
                                "path": "/runtime/artifacts/link.txt"
                            }]
                        }
                    }),
                ],
                responses.clone(),
                Arc::new(Mutex::new(Vec::new())),
                ProbeFileSetup::InvalidAttachments,
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
    let expected_codes = [
        "invalid_format",
        "empty_content",
        "unknown_channel",
        "invalid_attachment",
        "invalid_attachment",
        "invalid_attachment",
        "invalid_attachment",
        "invalid_attachment",
    ];
    assert_eq!(responses.len(), expected_codes.len());
    for (response, expected_code) in responses.iter().zip(expected_codes) {
        assert_eq!(response["ok"].as_bool(), Some(false));
        assert_eq!(response["error"]["code"].as_str(), Some(expected_code));
    }
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

    let denied = kernel
        .query_audit(
            None,
            Some("runtime.channel_send.denied".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query denied audit events");
    let invalid_request_denials = denied
        .events
        .iter()
        .filter(|event| event.details["reason"].as_str() == Some("invalid_request"))
        .count();
    assert_eq!(invalid_request_denials, 4);
    assert!(denied.events.iter().any(|event| {
        event.details["channel_id"].as_str() == Some("")
            && event.details["conversation_ref"].as_str() == Some("member:reviewer")
    }));
    assert!(denied.events.iter().any(|event| {
        event.details["channel_id"].as_str() == Some("local-cli")
            && event.details["conversation_ref"].as_str() == Some("")
    }));
}

#[tokio::test]
async fn channel_send_bridge_audits_setup_failures() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-setup-failure").await;
    tokio::fs::create_dir_all(env.home().runtime_dir())
        .await
        .expect("create runtime root");
    tokio::fs::write(env.home().runtime_dir().join("sockets"), b"not a directory")
        .await
        .expect("block socket directory");
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::record_environment(Arc::new(
                Mutex::new(Vec::new()),
            ))),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-setup-failure").await;

    let result = kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "fail channel send setup".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await;

    assert!(
        result.is_err(),
        "setup should fail before runtime execution"
    );
    let audit = kernel
        .query_audit(
            Some(session),
            Some("runtime.channel_send.bridge_error".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query bridge error audit events");
    assert!(
        audit.events.iter().any(|event| {
            event.details["stage"].as_str() == Some("host_socket_dir")
                && event.details["runtime_id"].as_str() == Some("channel-send-runtime")
                && event.details["turn_id"].as_str().is_some()
                && event.details["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("not a regular directory"))
        }),
        "setup failures must be audited under runtime.channel_send.*"
    );
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

#[tokio::test]
async fn channel_send_bridge_rejects_excess_connections() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-connection-limit").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "connection-limit-runtime",
            Arc::new(ChannelSendProbeRuntime::open_many_connections(
                responses.clone(),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-connection-limit").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "open too many channel send connections".to_string(),
            runtime_id: Some("connection-limit-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["ok"].as_bool(), Some(false));
    assert_eq!(
        responses[0]["error"]["code"].as_str(),
        Some("connection_limit")
    );

    let denied = kernel
        .query_audit(
            Some(session),
            Some("runtime.channel_send.denied".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query denied audit events");
    assert!(denied.events.iter().any(|event| {
        event.details["reason"].as_str() == Some("connection_limit")
            && event.details["runtime_id"].as_str() == Some("connection-limit-runtime")
    }));
}

#[tokio::test]
async fn channel_send_bridge_audits_wire_protocol_denials() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "runtime-channel-send-wire-errors").await;
    let kernel = kernel_with_channel_send_preset(&env, true).await;
    let responses = Arc::new(Mutex::new(Vec::new()));
    let mut oversized = vec![b'x'; 64 * 1024 + 1];
    oversized.push(b'\n');
    kernel
        .register_runtime_adapter(
            "channel-send-runtime",
            Arc::new(ChannelSendProbeRuntime::send_raw_requests(
                vec![
                    b"{not-json}\n".to_vec(),
                    b"{\"unterminated\":true".to_vec(),
                    oversized,
                ],
                responses.clone(),
            )),
        )
        .await;
    let session = open_test_session(&kernel, "runtime-channel-send-wire-errors").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session,
            user_text: "send malformed channel messages".to_string(),
            runtime_id: Some("channel-send-runtime".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete");

    let responses = responses.lock().expect("responses lock").clone();
    let expected_codes = ["invalid_json", "invalid_request", "request_too_large"];
    assert_eq!(responses.len(), expected_codes.len());
    for (response, expected_code) in responses.iter().zip(expected_codes) {
        assert_eq!(response["ok"].as_bool(), Some(false));
        assert_eq!(response["error"]["code"].as_str(), Some(expected_code));
    }

    let denied = kernel
        .query_audit(
            None,
            Some("runtime.channel_send.denied".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query denied audit events");
    for expected_code in expected_codes {
        assert!(
            denied.events.iter().any(|event| {
                event.details["reason"].as_str() == Some(expected_code)
                    && event.details["channel_id"].as_str() == Some("")
                    && event.details["conversation_ref"].as_str() == Some("")
            }),
            "missing denied audit event for {expected_code}"
        );
    }
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
    SendRawRequests {
        requests: Vec<Vec<u8>>,
        responses: Arc<Mutex<Vec<Value>>>,
    },
    Sleep {
        socket_paths: Arc<Mutex<Vec<PathBuf>>>,
        duration: Duration,
    },
    HoldOpenConnection {
        held_stream: Arc<Mutex<Option<UnixStream>>>,
    },
    OpenManyConnections {
        responses: Arc<Mutex<Vec<Value>>>,
    },
}

struct ChannelSendProbeRuntime {
    action: RuntimeAction,
}

struct DirectSocketProbeRuntime {
    socket_dir: PathBuf,
    observed: Arc<Mutex<Vec<bool>>>,
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

    fn send_raw_requests(requests: Vec<Vec<u8>>, responses: Arc<Mutex<Vec<Value>>>) -> Self {
        Self {
            action: RuntimeAction::SendRawRequests {
                requests,
                responses,
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

    fn open_many_connections(responses: Arc<Mutex<Vec<Value>>>) -> Self {
        Self {
            action: RuntimeAction::OpenManyConnections { responses },
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

    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        _events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        run_probe_action(&self.action, &execution.plan).await?;
        Ok(RuntimeControlOutcome::Handled {
            message: "control handled".to_string(),
        })
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

#[async_trait]
impl RuntimeAdapter for DirectSocketProbeRuntime {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "direct-socket-probe".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("direct-socket-probe-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.observed
            .lock()
            .expect("observed socket lock")
            .push(socket_dir_has_channel_send_socket(&self.socket_dir));
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
            Err(anyhow!("direct socket probe does not resolve capabilities"))
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
        RuntimeAction::SendRawRequests {
            requests,
            responses,
        } => {
            for request in requests {
                let response = send_raw_channel_send_request(plan, request).await?;
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
        RuntimeAction::OpenManyConnections { responses } => {
            let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
                .context("channel send socket env missing")?;
            let host_socket = host_path_for_runtime_path(plan, &socket)?;
            let mut held_streams = Vec::new();
            for _ in 0..TEST_CHANNEL_SEND_CONNECTION_LIMIT {
                held_streams.push(
                    UnixStream::connect(&host_socket)
                        .await
                        .with_context(|| format!("connect {}", host_socket.display()))?,
                );
                sleep(Duration::from_millis(10)).await;
            }
            sleep(Duration::from_millis(50)).await;

            let stream = UnixStream::connect(&host_socket)
                .await
                .with_context(|| format!("connect {}", host_socket.display()))?;
            let mut response = String::new();
            let mut reader = BufReader::new(stream);
            tokio::time::timeout(Duration::from_millis(250), reader.read_line(&mut response))
                .await
                .context("timed out waiting for connection limit response")?
                .context("read connection limit response")?;
            responses
                .lock()
                .expect("responses lock")
                .push(serde_json::from_str(response.trim()).context("decode response")?);
            drop(held_streams);
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
        ProbeFileSetup::InvalidAttachments => {
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

            let artifacts = runtime_root.join("artifacts");
            tokio::fs::create_dir_all(&artifacts)
                .await
                .context("create artifacts dir")?;
            let regular_file = artifacts.join("regular.txt");
            tokio::fs::write(&regular_file, b"regular bytes")
                .await
                .context("write regular artifact")?;
            std::os::unix::fs::symlink(&regular_file, artifacts.join("link.txt"))
                .context("create final symlink artifact")?;
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

async fn send_raw_channel_send_request(
    plan: &EffectiveExecutionPlan,
    request: &[u8],
) -> Result<Value> {
    let socket = env_value(&plan.environment, CHANNEL_SEND_SOCKET_ENV)
        .context("channel send socket env missing")?;
    let host_socket = host_path_for_runtime_path(plan, &socket)?;
    let mut stream = UnixStream::connect(&host_socket)
        .await
        .with_context(|| format!("connect {}", host_socket.display()))?;
    stream.write_all(request).await.expect("write request");
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

fn socket_dir_has_channel_send_socket(socket_dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(socket_dir) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("channel-send-") && name.ends_with(".sock"))
    })
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
