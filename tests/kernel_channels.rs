mod common;

use async_trait::async_trait;
use common::{write_skill_source, TestHome};
use lionclaw::{
    contracts::{
        ChannelInboundOutcome, ChannelInboundRequest, ChannelPeerApproveRequest,
        ChannelPeerBlockRequest, ChannelStreamAckRequest, ChannelStreamEventView,
        ChannelStreamPullRequest, ChannelStreamStartMode, SessionActionKind, SessionActionRequest,
        SessionHistoryPolicy, SessionHistoryRequest, SessionLatestQuery, SessionOpenRequest,
        SessionTurnKind, SessionTurnRequest, SessionTurnStatus, StreamEventKindDto, StreamLaneDto,
        TrustTier,
    },
    kernel::{
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
            RuntimeEventSender, RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput,
            RuntimeTurnInput, RuntimeTurnResult,
        },
        InboundChannelText, Kernel, KernelError, KernelOptions,
    },
    operator::{config::ChannelLaunchMode, reconcile::add_channel},
};
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn add_channel_requires_installed_alias() {
    let env = TestHome::new().await;

    let err = add_channel(
        env.home(),
        "local-cli".to_string(),
        "missing-skill".to_string(),
        ChannelLaunchMode::Service,
        Vec::new(),
    )
    .await
    .expect_err("missing alias should fail");
    assert!(err
        .to_string()
        .contains("installed skill alias 'missing-skill' not found"));
}

#[tokio::test]
async fn add_channel_rejects_invalid_alias() {
    let env = TestHome::new().await;

    let err = add_channel(
        env.home(),
        "local-cli".to_string(),
        "../not-valid".to_string(),
        ChannelLaunchMode::Service,
        Vec::new(),
    )
    .await
    .expect_err("invalid alias should fail");
    assert!(err.to_string().contains("skill alias"));
}

#[tokio::test]
async fn list_channels_returns_config_derived_binding_fields() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(
        env.temp_dir(),
        "interactive-skill",
        "interactive skill for channel tests",
        true,
    );
    env.install_skill("interactive-skill", &skill_source).await;
    add_channel(
        env.home(),
        "terminal".to_string(),
        "interactive-skill".to_string(),
        ChannelLaunchMode::Interactive,
        Vec::new(),
    )
    .await
    .expect("add interactive channel");

    let kernel = env.kernel().await;
    let bindings = kernel
        .list_channels()
        .await
        .expect("list channels")
        .bindings;

    assert_eq!(bindings.len(), 1);
    let binding = &bindings[0];
    assert_eq!(binding.channel_id, "terminal");
    assert_eq!(binding.skill_alias, "interactive-skill");
    assert_eq!(binding.launch_mode, "interactive");
}

#[tokio::test]
async fn channel_peer_must_be_approved_before_inbound_turn_executes() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "channel-inbound-skill").await;
    let kernel = env.kernel().await;

    let pending = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-local".to_string(),
            text: "hello inbound-skill".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(1001),
            external_message_id: Some("msg-1001".to_string()),
        })
        .await
        .expect("pending inbound handled");
    assert_eq!(pending.outcome, ChannelInboundOutcome::PairingPending);
    assert!(pending.turn_id.is_none());

    let peers = kernel
        .list_channel_peers(Some("local-cli".to_string()))
        .await
        .expect("list peers");
    let pairing_code = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == "peer-local")
        .and_then(|peer| peer.pairing_code.clone())
        .expect("pending peer pairing code");
    kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-local".to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect("approve peer");

    let queued = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-local".to_string(),
            text: "please run inbound-skill now".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(1002),
            external_message_id: Some("msg-1002".to_string()),
        })
        .await
        .expect("approved inbound turn should succeed");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");
    assert!(queued.session_id.is_some());

    let duplicate = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-local".to_string(),
            text: "please run inbound-skill now".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(1002),
            external_message_id: Some("msg-1002".to_string()),
        })
        .await
        .expect("duplicate update should be ignored");
    assert_eq!(duplicate.outcome, ChannelInboundOutcome::Duplicate);

    let stream = wait_for_stream_events(&kernel, "local-cli", "local-cli-test", |events| {
        let codes = events
            .iter()
            .filter_map(|event| event.code.as_deref())
            .collect::<Vec<_>>();
        codes.contains(&"queue.queued")
            && codes.contains(&"queue.started")
            && codes.contains(&"queue.completed")
    })
    .await;
    let codes = stream
        .events
        .iter()
        .filter_map(|event| event.code.as_deref())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"queue.queued"));
    assert!(codes.contains(&"queue.started"));
    assert!(codes.contains(&"queue.completed"));
    let (completed_position, _) =
        assert_turn_completed_before_done(&stream.events, queued_turn_id, "queued channel turn");
    assert!(stream.events[completed_position]
        .text
        .as_deref()
        .is_some_and(|text| text.contains("[mock]")));
}

#[tokio::test]
async fn channel_stream_pull_and_ack_round_trip() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "telegram", "channel-outbox-skill").await;
    let kernel = env.kernel().await;

    let inbound = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "telegram".to_string(),
            peer_id: "peer-tele".to_string(),
            text: "hello kernel".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(4001),
            external_message_id: Some("update-4001".to_string()),
        })
        .await
        .expect("process inbound");
    assert_eq!(inbound.outcome, ChannelInboundOutcome::PairingPending);
    assert!(inbound.turn_id.is_none());

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(10),
            wait_ms: Some(0),
        })
        .await
        .expect("pull stream");
    assert!(!stream.events.is_empty());
    let last_sequence = stream.events.last().expect("last event").sequence;
    assert!(stream.events.iter().any(|event| {
        event.kind == StreamEventKindDto::MessageDelta
            && event.lane == Some(StreamLaneDto::Answer)
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("Pairing required"))
    }));

    let ack = kernel
        .ack_channel_stream(ChannelStreamAckRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            through_sequence: last_sequence,
        })
        .await
        .expect("ack stream");
    assert!(ack.acknowledged);

    let empty = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(10),
            wait_ms: Some(0),
        })
        .await
        .expect("pull acked stream");
    assert!(empty.events.is_empty());
}

#[tokio::test]
async fn channel_stream_tail_starts_from_current_head() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "channel-tail-skill").await;
    let kernel = env.kernel().await;

    let pending = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "terminal".to_string(),
            peer_id: "peer-tail".to_string(),
            text: "hello before tail connect".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(5001),
            external_message_id: Some("tail-5001".to_string()),
        })
        .await
        .expect("process inbound");
    assert_eq!(pending.outcome, ChannelInboundOutcome::PairingPending);

    let initial = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Tail),
            start_after_sequence: None,
            limit: Some(10),
            wait_ms: Some(0),
        })
        .await
        .expect("tail pull");
    assert!(initial.events.is_empty());
}

#[tokio::test]
async fn channel_stream_rejects_tail_with_explicit_start_sequence() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "tail-sequence-skill").await;
    let kernel = env.kernel().await;

    let err = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Tail),
            start_after_sequence: Some(12),
            limit: Some(10),
            wait_ms: Some(0),
        })
        .await
        .expect_err("tail and start_after_sequence should be rejected");

    assert!(
        matches!(err, KernelError::BadRequest(message) if message.contains("start_after_sequence"))
    );
}

#[tokio::test]
async fn channel_stream_long_poll_wakes_for_new_events() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "telegram", "channel-wait-skill").await;
    let kernel = env.kernel().await;

    let delayed_inbound = async {
        sleep(Duration::from_millis(50)).await;
        let pending = kernel
            .process_inbound_channel_text(InboundChannelText {
                channel_id: "telegram".to_string(),
                peer_id: "peer-wait".to_string(),
                text: "hello after long poll".to_string(),
                session_id: None,
                runtime_id: Some("mock".to_string()),
                update_id: Some(6001),
                external_message_id: Some("wait-6001".to_string()),
            })
            .await
            .expect("delayed inbound");
        assert_eq!(pending.outcome, ChannelInboundOutcome::PairingPending);
    };

    let (stream, _) = tokio::join!(
        kernel.pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(10),
            wait_ms: Some(1_000),
        }),
        delayed_inbound
    );
    let stream = stream.expect("long-poll stream");

    assert!(stream.events.iter().any(|event| {
        event.kind == StreamEventKindDto::MessageDelta
            && event.lane == Some(StreamLaneDto::Answer)
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("Pairing required"))
    }));
}

#[tokio::test]
async fn channel_backed_session_open_requires_approved_peer() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "open-skill").await;
    let kernel = env.kernel().await;

    let unapproved_err = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-open".to_string(),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("unapproved peer should be rejected");
    assert!(matches!(
        unapproved_err,
        KernelError::BadRequest(message) if message.contains("not approved")
    ));

    create_pending_peer(&kernel, "terminal", "peer-open", "mock", 7001, "open-7001").await;
    let pending_err = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-open".to_string(),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("pending peer should be rejected");
    assert!(matches!(
        pending_err,
        KernelError::BadRequest(message) if message.contains("pending approval")
    ));

    approve_peer(&kernel, "terminal", "peer-open").await;
    let opened = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-open".to_string(),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("approved peer session open");
    assert_eq!(opened.trust_tier.as_str(), TrustTier::Main.as_str());
}

#[tokio::test]
async fn direct_channel_session_turn_streams_turn_completed_then_done() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "direct-turn-skill").await;
    let kernel = env.kernel().await;
    create_pending_peer(
        &kernel,
        "terminal",
        "peer-direct",
        "mock",
        7051,
        "direct-7051",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-direct").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-direct".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open direct channel session");

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "direct channel turn".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("direct channel turn");
    assert!(response.assistant_text.starts_with("[mock] "));
    assert!(response.assistant_text.contains("direct channel turn"));

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-consumer".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull direct turn stream");
    assert_turn_completed_before_done(&stream.events, response.turn_id, "direct channel turn");
}

#[tokio::test]
async fn latest_session_snapshot_is_project_scoped() {
    let env = TestHome::new().await;
    let project_a = env.temp_dir().join("project-a");
    let project_b = env.temp_dir().join("project-b");
    std::fs::create_dir_all(&project_a).expect("project a");
    std::fs::create_dir_all(&project_b).expect("project b");

    let kernel_a = env
        .kernel_with_options(KernelOptions {
            project_workspace_root: Some(project_a),
            ..KernelOptions::default()
        })
        .await;
    let session_a = kernel_a
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open project-a session");

    let kernel_b = env
        .kernel_with_options(KernelOptions {
            project_workspace_root: Some(project_b),
            ..KernelOptions::default()
        })
        .await;

    let snapshot = kernel_b
        .latest_session_snapshot(SessionLatestQuery {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("latest snapshot");
    assert!(snapshot.session.is_none());

    let session_b = kernel_b
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open project-b session");
    assert_ne!(session_a.session_id, session_b.session_id);
}

#[tokio::test]
async fn latest_session_snapshot_uses_stream_head_before_first_answer_checkpoint() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "resume-head-skill").await;
    let kernel = env.kernel().await;
    kernel
        .register_runtime_adapter(
            "slow-answer",
            std::sync::Arc::new(SlowAnswerAdapter {
                answer: "later".to_string(),
                delay: Duration::from_millis(400),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-resume-head",
        "slow-answer",
        7251,
        "resume-head-7251",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-resume-head").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-resume-head".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-resume-head".to_string(),
            text: "resume before answer".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7252),
            external_message_id: Some("resume-head-7252".to_string()),
            runtime_id: Some("slow-answer".to_string()),
        })
        .await
        .expect("queue running turn");

    let resume_after_sequence =
        wait_for_running_snapshot_without_answer(&kernel, "terminal", "peer-resume-head").await;

    let resumed = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-resume-head".to_string(),
            start_mode: None,
            start_after_sequence: Some(resume_after_sequence),
            limit: Some(20),
            wait_ms: Some(1_000),
        })
        .await
        .expect("resume stream");
    let answer_text = resumed
        .events
        .iter()
        .filter(|event| {
            event.kind == StreamEventKindDto::MessageDelta
                && event.lane == Some(StreamLaneDto::Answer)
        })
        .filter_map(|event| event.text.as_deref())
        .collect::<String>();
    assert_eq!(answer_text, "later");
}

#[tokio::test]
async fn channel_session_actions_return_immediately_and_respect_peer_blocking() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "action-skill").await;
    let kernel = env.kernel().await;
    kernel
        .register_runtime_adapter(
            "slow-answer",
            std::sync::Arc::new(SlowAnswerAdapter {
                answer: "completed".to_string(),
                delay: Duration::from_millis(500),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-action",
        "slow-answer",
        7301,
        "action-7301",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-action").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-action".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive action session");

    let first = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "seed action".to_string(),
            runtime_id: Some("slow-answer".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("seed turn");
    assert_eq!(first.assistant_text, "completed");

    let started_at = Instant::now();
    let retry = kernel
        .session_action(SessionActionRequest {
            session_id: session.session_id,
            action: SessionActionKind::RetryLastTurn,
        })
        .await
        .expect("retry session action");
    assert!(started_at.elapsed() < Duration::from_millis(250));
    assert_eq!(retry.session_id, session.session_id);
    assert!(retry.turn_id.is_some());

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.assistant_text == "completed"
        },
        "completed retry turn",
    )
    .await;

    kernel
        .block_channel_peer(ChannelPeerBlockRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-action".to_string(),
        })
        .await
        .expect("block peer");

    let err = kernel
        .session_action(SessionActionRequest {
            session_id: session.session_id,
            action: SessionActionKind::RetryLastTurn,
        })
        .await
        .expect_err("blocked peer should reject action");
    assert!(matches!(err, KernelError::Conflict(message) if message.contains("blocked")));
}

#[tokio::test]
async fn channel_runtime_error_event_persists_failed_turn_and_supports_continue() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "failure-skill").await;
    let kernel = env.kernel().await;
    kernel
        .register_runtime_adapter(
            "partial-failure",
            std::sync::Arc::new(PartialFailureAdapter {
                partial: "partial before fail".to_string(),
                message: "adapter failed after partial output".to_string(),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-failure",
        "partial-failure",
        7301,
        "failure-7301",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-failure").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-failure".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-failure".to_string(),
            text: "fail-case".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7302),
            external_message_id: Some("failure-7302".to_string()),
            runtime_id: Some("partial-failure".to_string()),
        })
        .await
        .expect("queue failing turn");
    assert_eq!(queued.session_id, Some(session.session_id));

    wait_for_audit_event_count(&kernel, "channel.turn.failed", 1).await;
    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(12),
        })
        .await
        .expect("session history");
    let latest = history.turns.last().expect("failed turn");
    assert_eq!(latest.status, SessionTurnStatus::Failed);
    assert_eq!(latest.assistant_text, "partial before fail");
    assert_eq!(latest.error_code.as_deref(), Some("runtime.error"));
    assert_eq!(
        latest.error_text.as_deref(),
        Some("adapter failed after partial output")
    );

    let continued = kernel
        .session_action(SessionActionRequest {
            session_id: session.session_id,
            action: SessionActionKind::ContinueLastPartial,
        })
        .await
        .expect("continue partial");
    assert_eq!(continued.session_id, session.session_id);
    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.kind == SessionTurnKind::Continue
                && turn.status == SessionTurnStatus::Failed
                && turn.assistant_text == "partial before fail"
        },
        "continued failed turn",
    )
    .await;
}

async fn wait_for_audit_event_count(
    kernel: &Kernel,
    kind: &str,
    expected_minimum: usize,
) -> lionclaw::contracts::AuditQueryResponse {
    for _ in 0..40 {
        let response = kernel
            .query_audit(None, Some(kind.to_string()), None, Some(50))
            .await
            .expect("query audit");
        if response.events.len() >= expected_minimum {
            return response;
        }
        sleep(Duration::from_millis(25)).await;
    }

    kernel
        .query_audit(None, Some(kind.to_string()), None, Some(50))
        .await
        .expect("query audit")
}

async fn wait_for_stream_events<F>(
    kernel: &Kernel,
    channel_id: &str,
    consumer_id: &str,
    predicate: F,
) -> lionclaw::contracts::ChannelStreamPullResponse
where
    F: Fn(&[ChannelStreamEventView]) -> bool,
{
    for _ in 0..40 {
        let response = kernel
            .pull_channel_stream(ChannelStreamPullRequest {
                channel_id: channel_id.to_string(),
                consumer_id: consumer_id.to_string(),
                start_mode: Some(ChannelStreamStartMode::Resume),
                start_after_sequence: None,
                limit: Some(50),
                wait_ms: Some(0),
            })
            .await
            .expect("pull channel stream");
        if predicate(&response.events) {
            return response;
        }
        sleep(Duration::from_millis(25)).await;
    }

    kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: channel_id.to_string(),
            consumer_id: consumer_id.to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull channel stream")
}

async fn install_and_bind_channel(env: &TestHome, channel_id: &str, skill_name: &str) {
    let skill_source = write_skill_source(
        env.temp_dir(),
        skill_name,
        &format!("{skill_name} for channel tests"),
        true,
    );
    env.install_skill(skill_name, &skill_source).await;
    env.add_channel(channel_id, skill_name, ChannelLaunchMode::Service)
        .await;
}

fn assert_turn_completed_before_done(
    events: &[ChannelStreamEventView],
    turn_id: uuid::Uuid,
    context: &str,
) -> (usize, usize) {
    let completed_position = events
        .iter()
        .position(|event| {
            event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::TurnCompleted
        })
        .unwrap_or_else(|| panic!("{context} should publish turn_completed"));
    let done_position = events
        .iter()
        .position(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done)
        .unwrap_or_else(|| panic!("{context} should publish done"));
    let done_count = events
        .iter()
        .filter(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done)
        .count();
    assert_eq!(done_count, 1, "{context} should publish exactly one done");
    assert!(
        completed_position < done_position,
        "{context} should publish turn_completed before done"
    );
    (completed_position, done_position)
}

fn assert_error_before_done(
    events: &[ChannelStreamEventView],
    turn_id: uuid::Uuid,
    context: &str,
) -> (usize, usize) {
    let error_position = events
        .iter()
        .position(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Error)
        .unwrap_or_else(|| panic!("{context} should publish error"));
    let done_position = events
        .iter()
        .position(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done)
        .unwrap_or_else(|| panic!("{context} should publish done"));
    let done_count = events
        .iter()
        .filter(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done)
        .count();
    assert_eq!(done_count, 1, "{context} should publish exactly one done");
    assert!(
        error_position < done_position,
        "{context} should publish error before done"
    );
    (error_position, done_position)
}

async fn create_pending_peer(
    kernel: &Kernel,
    channel_id: &str,
    peer_id: &str,
    runtime_id: &str,
    update_id: i64,
    external_message_id: &str,
) {
    let response = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: channel_id.to_string(),
            peer_id: peer_id.to_string(),
            text: "seed pairing".to_string(),
            session_id: None,
            runtime_id: Some(runtime_id.to_string()),
            update_id: Some(update_id),
            external_message_id: Some(external_message_id.to_string()),
        })
        .await
        .expect("create pending peer");
    assert_eq!(response.outcome, ChannelInboundOutcome::PairingPending);
}

async fn approve_peer(kernel: &Kernel, channel_id: &str, peer_id: &str) {
    let peers = kernel
        .list_channel_peers(Some(channel_id.to_string()))
        .await
        .expect("list peers");
    let pairing_code = peers
        .peers
        .iter()
        .find(|value| value.peer_id == peer_id)
        .and_then(|peer| peer.pairing_code.clone())
        .expect("pairing code");
    kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id: channel_id.to_string(),
            peer_id: peer_id.to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect("approve peer");
}

async fn wait_for_latest_turn<F>(kernel: &Kernel, session_id: uuid::Uuid, predicate: F, label: &str)
where
    F: Fn(&lionclaw::contracts::SessionTurnView) -> bool,
{
    for _ in 0..60 {
        let history = kernel
            .session_history(SessionHistoryRequest {
                session_id,
                limit: Some(12),
            })
            .await
            .expect("session history");
        if history.turns.last().is_some_and(&predicate) {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {label}");
}

async fn wait_for_running_snapshot_without_answer(
    kernel: &Kernel,
    channel_id: &str,
    peer_id: &str,
) -> i64 {
    for _ in 0..60 {
        let snapshot = kernel
            .latest_session_snapshot(SessionLatestQuery {
                channel_id: channel_id.to_string(),
                peer_id: peer_id.to_string(),
                history_policy: Some(SessionHistoryPolicy::Interactive),
            })
            .await
            .expect("latest session snapshot");
        let no_answer_yet = snapshot.turns.last().is_some_and(|turn| {
            turn.status == SessionTurnStatus::Running && turn.assistant_text.is_empty()
        });
        if no_answer_yet {
            if let Some(sequence) = snapshot.resume_after_sequence {
                return sequence;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for running snapshot without assistant checkpoint");
}

struct PartialFailureAdapter {
    partial: String,
    message: String,
}

#[async_trait]
impl RuntimeAdapter for PartialFailureAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "partial-failure".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("partial-failure:{}", input.session_id),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: "starting a partial reply before failure".to_string(),
            })
            .expect("send reasoning");
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: self.partial.clone(),
            })
            .expect("send partial answer");
        event_tx
            .send(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: self.message.clone(),
            })
            .expect("send runtime error");
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _event_tx: RuntimeEventSender,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

struct FailingSessionStartAdapter {
    message: String,
}

#[async_trait]
impl RuntimeAdapter for FailingSessionStartAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "failing-start".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Err(anyhow::anyhow!(self.message.clone()))
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        _event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        unreachable!("session_start fails before turn execution")
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _event_tx: RuntimeEventSender,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

struct ErrorThenCloseFailAdapter {
    error_code: String,
    error_text: String,
    close_text: String,
}

#[async_trait]
impl RuntimeAdapter for ErrorThenCloseFailAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "error-close-fail".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("error-close-fail:{}", input.session_id),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        event_tx
            .send(RuntimeEvent::Error {
                code: Some(self.error_code.clone()),
                text: self.error_text.clone(),
            })
            .expect("send runtime error");
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _event_tx: RuntimeEventSender,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<(), anyhow::Error> {
        Err(anyhow::anyhow!(self.close_text.clone()))
    }
}

struct SlowAnswerAdapter {
    answer: String,
    delay: Duration,
}

#[async_trait]
impl RuntimeAdapter for SlowAnswerAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "slow-answer".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("slow-answer:{}", input.session_id),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        sleep(self.delay).await;
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: self.answer.clone(),
            })
            .expect("send answer");
        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _event_tx: RuntimeEventSender,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn channel_session_start_failure_streams_error_before_done() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "start-failure-skill").await;
    let kernel = env.kernel().await;
    kernel
        .register_runtime_adapter(
            "failing-start",
            std::sync::Arc::new(FailingSessionStartAdapter {
                message: "runtime failed before turn".to_string(),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-start-failure",
        "failing-start",
        7351,
        "start-failure-7351",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-start-failure").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-start-failure".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "fail before stream".to_string(),
            runtime_id: Some("failing-start".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("session_start failure should fail turn");
    assert!(
        matches!(err, KernelError::Runtime(message) if message.contains("runtime failed before turn"))
    );

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(12),
        })
        .await
        .expect("session history");
    let failed_turn = history.turns.last().expect("failed turn");
    assert_eq!(failed_turn.status, SessionTurnStatus::Failed);
    assert_eq!(failed_turn.error_code.as_deref(), Some("runtime.error"));
    assert_eq!(
        failed_turn.error_text.as_deref(),
        Some("runtime failed before turn")
    );

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-start-failure".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull failure stream");
    let (error_position, _) =
        assert_error_before_done(&stream.events, failed_turn.turn_id, "session_start failure");
    let error_event = &stream.events[error_position];
    assert_eq!(error_event.code.as_deref(), Some("runtime.error"));
    assert_eq!(
        error_event.text.as_deref(),
        Some("runtime failed before turn")
    );
}

#[tokio::test]
async fn channel_close_failure_does_not_override_streamed_runtime_error() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "close-failure-skill").await;
    let kernel = env.kernel().await;
    kernel
        .register_runtime_adapter(
            "error-close-fail",
            std::sync::Arc::new(ErrorThenCloseFailAdapter {
                error_code: "runtime.error".to_string(),
                error_text: "runtime emitted error".to_string(),
                close_text: "runtime close failed".to_string(),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-close-failure",
        "error-close-fail",
        7401,
        "close-failure-7401",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-close-failure").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-close-failure".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "close failure".to_string(),
            runtime_id: Some("error-close-fail".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("close failure should fail turn");
    assert!(
        matches!(err, KernelError::Runtime(message) if message.contains("runtime emitted error"))
    );

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(12),
        })
        .await
        .expect("session history");
    let failed_turn = history.turns.last().expect("failed turn");
    assert_eq!(failed_turn.status, SessionTurnStatus::Failed);
    assert_eq!(failed_turn.error_code.as_deref(), Some("runtime.error"));
    assert_eq!(
        failed_turn.error_text.as_deref(),
        Some("runtime emitted error")
    );

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-close-failure".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull close failure stream");
    let error_count = stream
        .events
        .iter()
        .filter(|event| {
            event.turn_id == Some(failed_turn.turn_id) && event.kind == StreamEventKindDto::Error
        })
        .count();
    assert_eq!(
        error_count, 1,
        "stream should contain exactly one canonical error"
    );
    let (error_position, _) =
        assert_error_before_done(&stream.events, failed_turn.turn_id, "close failure");
    let error_event = &stream.events[error_position];
    assert_eq!(error_event.code.as_deref(), Some("runtime.error"));
    assert_eq!(error_event.text.as_deref(), Some("runtime emitted error"));
}
