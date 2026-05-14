mod common;

use async_trait::async_trait;
use common::{write_skill_source, TestHome};
use lionclaw::{
    contracts::{
        ChannelAttachmentDescriptor, ChannelInboundOutcome, ChannelInboundRequest,
        ChannelPairingApproveRequest, ChannelPeerBlockRequest, ChannelStreamAckRequest,
        ChannelStreamEventView, ChannelStreamPullRequest, ChannelStreamStartMode, ChannelTrigger,
        SessionActionKind, SessionActionRequest, SessionHistoryPolicy, SessionHistoryRequest,
        SessionLatestQuery, SessionOpenRequest, SessionTurnKind, SessionTurnRequest,
        SessionTurnStatus, StreamEventKindDto, StreamLaneDto, TrustTier,
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
use sqlx::Row;
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn add_channel_requires_installed_alias() {
    let env = TestHome::new().await;

    let err = add_channel(
        env.home(),
        "local-cli".to_string(),
        "missing-skill".to_string(),
        ChannelLaunchMode::Background,
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
        ChannelLaunchMode::Background,
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
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);
    assert!(pending.turn_id.is_none());

    let pairing_code = pending.pairing_code.expect("pending pairing code");
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "local-cli".to_string(),
            pairing_id: None,
            pairing_code: Some(pairing_code),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve pairing");

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
            && events.iter().any(|event| {
                event.turn_id == Some(queued_turn_id) && event.kind == StreamEventKindDto::Done
            })
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
    assert_eq!(inbound.outcome, ChannelInboundOutcome::PendingApproval);
    assert!(inbound.turn_id.is_none());
    approve_peer(&kernel, "telegram", "peer-tele").await;

    let queued = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "telegram".to_string(),
            peer_id: "peer-tele".to_string(),
            text: "hello approved kernel".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(4002),
            external_message_id: Some("update-4002".to_string()),
        })
        .await
        .expect("process approved inbound");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);

    let stream = wait_for_stream_events(&kernel, "telegram", "telegram-worker", |events| {
        events
            .iter()
            .filter_map(|event| event.code.as_deref())
            .any(|code| code == "queue.completed")
    })
    .await;
    assert!(!stream.events.is_empty());
    let last_sequence = stream.events.last().expect("last event").sequence;
    assert!(stream
        .events
        .iter()
        .any(|event| event.peer_id == "peer-tele"));

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
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);

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
        assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);
        approve_peer(&kernel, "telegram", "peer-wait").await;
        let queued = kernel
            .process_inbound_channel_text(InboundChannelText {
                channel_id: "telegram".to_string(),
                peer_id: "peer-wait".to_string(),
                text: "hello after approval".to_string(),
                session_id: None,
                runtime_id: Some("mock".to_string()),
                update_id: Some(6002),
                external_message_id: Some("wait-6002".to_string()),
            })
            .await
            .expect("delayed approved inbound");
        assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
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

    assert!(stream
        .events
        .iter()
        .filter_map(|event| event.code.as_deref())
        .any(|code| code == "queue.queued"));
}

#[tokio::test]
async fn channel_backed_session_open_requires_approved_peer() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "open-skill").await;
    let kernel = env.kernel().await;

    let unapproved_err = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: direct_session_key("terminal", "peer-open"),
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
            peer_id: direct_session_key("terminal", "peer-open"),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("pending peer should be rejected");
    assert!(matches!(
        pending_err,
        KernelError::BadRequest(message) if message.contains("not approved")
    ));

    approve_peer(&kernel, "terminal", "peer-open").await;
    let opened = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: direct_session_key("terminal", "peer-open"),
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
            peer_id: direct_session_key("terminal", "peer-direct"),
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
async fn channels_v2_pending_pairing_hashes_code_and_rejects_runtime_id() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "v2-pairing-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let rejected = serde_json::from_value::<ChannelInboundRequest>(serde_json::json!({
        "channel_id": "terminal",
        "event_id": "event-runtime",
        "sender_ref": "alice",
        "conversation_ref": "alice",
        "text": "hello",
        "attachments": [],
        "trigger": "dm",
        "provider_metadata": {},
        "runtime_id": "mock"
    }));
    assert!(
        rejected.is_err(),
        "worker-supplied runtime_id must be rejected"
    );

    let pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "event-1",
            "alice",
            "alice",
            None,
            "hello",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("pending v2 inbound");
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);
    let pairing_id = pending.pairing_id.expect("pairing id");
    let pairing_code = pending.pairing_code.expect("one-time pairing code");
    assert!(pairing_code.starts_with("pc_"));
    assert_eq!(pairing_code.len(), 23);

    let pairings = kernel
        .list_channel_pairings(Some("terminal".to_string()), None)
        .await
        .expect("list pairings");
    assert_eq!(pairings.pairings.len(), 1);
    let serialized = serde_json::to_value(&pairings.pairings[0]).expect("serialize pairing");
    assert!(serialized.get("pairing_code").is_none());

    let pool = connect_test_pool(&env.home().db_path()).await;
    let code_row =
        sqlx::query("SELECT code_hash FROM channel_pairing_requests WHERE pairing_id = ?1")
            .bind(pairing_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("query code hash");
    let code_hash: String = code_row.get("code_hash");
    assert_ne!(code_hash, pairing_code);
    assert!(!code_hash.contains(&pairing_code));

    let duplicate = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "event-1",
            "alice",
            "alice",
            None,
            "hello again",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("duplicate v2 inbound");
    assert_eq!(duplicate.outcome, ChannelInboundOutcome::Duplicate);
    assert_eq!(duplicate.reason_code.as_deref(), Some("duplicate_event"));

    let invalid_approve = kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "terminal".to_string(),
            pairing_id: Some(pairing_id),
            pairing_code: Some(pairing_code.clone()),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect_err("approve must identify pairing by id or code, not both");
    assert!(
        matches!(invalid_approve, KernelError::BadRequest(message) if message.contains("exactly one"))
    );

    let grant = kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "terminal".to_string(),
            pairing_id: None,
            pairing_code: Some(pairing_code),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: Some("alice".to_string()),
        })
        .await
        .expect("approve by raw code")
        .grant;
    assert_eq!(grant.routing_profile.as_str(), "direct");
    assert_eq!(grant.sender_ref.as_deref(), Some("alice"));

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "event-2",
            "alice",
            "alice",
            None,
            "run this",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("approved v2 inbound");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(
        queued.session_key.as_deref(),
        Some(direct_session_key("terminal", "alice").as_str())
    );
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    let channel_message_columns = sqlx::query("PRAGMA table_info(channel_messages)")
        .fetch_all(&pool)
        .await
        .expect("query channel message columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(
        !channel_message_columns.iter().any(|name| name == "content"),
        "channel message records must not store message content"
    );
    let inbound_event_columns = sqlx::query("PRAGMA table_info(channel_inbound_events)")
        .fetch_all(&pool)
        .await
        .expect("query inbound event columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(
        !inbound_event_columns.iter().any(|name| name == "text"),
        "normalized inbound facts must not duplicate message text"
    );
    let session_turn_row = sqlx::query(
        "SELECT display_user_text, prompt_user_text FROM session_turns WHERE turn_id = ?1",
    )
    .bind(queued_turn_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query persisted session turn");
    assert_eq!(
        session_turn_row.get::<String, _>("display_user_text"),
        "run this"
    );
    assert_eq!(
        session_turn_row.get::<String, _>("prompt_user_text"),
        "run this"
    );

    let accepted = wait_for_audit_event_count(&kernel, "channel.inbound.accepted", 1).await;
    assert!(accepted.events.iter().any(|event| {
        event.details["reason_code"].as_str() == Some("accepted")
            && event.details["event_id"].as_str() == Some("event-2")
    }));
}

#[tokio::test]
async fn channels_v2_scoped_grants_triggers_and_attachment_wait_state() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "slack", "v2-routing-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let pending_conversation = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "conversation-pending",
            "alice",
            "room-1",
            None,
            "mention",
            ChannelTrigger::Mention,
        ))
        .await
        .expect("pending conversation");
    assert_eq!(
        pending_conversation.outcome,
        ChannelInboundOutcome::PendingApproval
    );
    let conversation_pairing_id = pending_conversation.pairing_id.expect("pairing id");

    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "slack".to_string(),
            pairing_id: Some(conversation_pairing_id),
            pairing_code: None,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: Some("alice in room".to_string()),
        })
        .await
        .expect("approve conversation by id");

    let queued_topic = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "conversation-topic",
            "alice",
            "room-1",
            Some("topic-a"),
            "mention in topic",
            ChannelTrigger::Mention,
        ))
        .await
        .expect("conversation grant handles topic mention");
    assert_eq!(queued_topic.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(
        queued_topic.session_key.as_deref(),
        Some("channel:slack:conversation:room-1:sender:alice")
    );

    let ignored_plain = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "conversation-plain",
            "alice",
            "room-1",
            None,
            "plain message",
            ChannelTrigger::None,
        ))
        .await
        .expect("plain conversation message");
    assert_eq!(ignored_plain.outcome, ChannelInboundOutcome::TriggerIgnored);
    assert_eq!(
        ignored_plain.reason_code.as_deref(),
        Some("trigger_insufficient")
    );

    let pending_thread = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "thread-pending",
            "bob",
            "room-1",
            Some("topic-b"),
            "thread continuation",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("pending thread");
    let thread_pairing_id = pending_thread.pairing_id.expect("thread pairing id");
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "slack".to_string(),
            pairing_id: Some(thread_pairing_id),
            pairing_code: None,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve thread");

    let queued_thread = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "thread-queued",
            "bob",
            "room-1",
            Some("topic-b"),
            "thread again",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("thread continuation");
    assert_eq!(queued_thread.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(
        queued_thread.session_key.as_deref(),
        Some("channel:slack:thread:room-1:topic-b")
    );

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![attachment_descriptor("att-1")],
            ..v2_text_request(
                "slack",
                "thread-attachment",
                "bob",
                "room-1",
                Some("topic-b"),
                "see image",
                ChannelTrigger::ThreadContinuation,
            )
        })
        .await
        .expect("attachment inbound");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let duplicate_attachment = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![
                attachment_descriptor("att-duplicate"),
                attachment_descriptor(" att-duplicate "),
            ],
            ..v2_text_request(
                "slack",
                "thread-duplicate-attachment",
                "bob",
                "room-1",
                Some("topic-b"),
                "ambiguous image",
                ChannelTrigger::ThreadContinuation,
            )
        })
        .await
        .expect_err("duplicate attachment ids must be rejected");
    assert!(
        matches!(duplicate_attachment, KernelError::BadRequest(message) if message.contains("duplicate attachment_id"))
    );

    let waiting_turn_id = waiting.turn_id.expect("waiting turn id");
    sleep(Duration::from_millis(100)).await;

    let pool = connect_test_pool(&env.home().db_path()).await;
    let status_row = sqlx::query("SELECT status FROM channel_turns WHERE turn_id = ?1")
        .bind(waiting_turn_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("query waiting turn");
    let status: String = status_row.get("status");
    assert_eq!(status, "waiting_for_attachments");
    let transcript_row = sqlx::query(
        "SELECT status, display_user_text, prompt_user_text FROM session_turns WHERE turn_id = ?1",
    )
    .bind(waiting_turn_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query waiting transcript turn");
    assert_eq!(
        transcript_row.get::<String, _>("status"),
        "waiting_for_attachments"
    );
    assert_eq!(
        transcript_row.get::<String, _>("display_user_text"),
        "see image"
    );
    assert_eq!(
        transcript_row.get::<String, _>("prompt_user_text"),
        "see image"
    );

    let colon_pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "colon-thread-pending",
            "telegram:user:456",
            "telegram:chat:-123",
            Some("telegram:topic:77"),
            "thread with provider-shaped refs",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("pending colon thread");
    let colon_pairing_id = colon_pending.pairing_id.expect("colon pairing id");
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "slack".to_string(),
            pairing_id: Some(colon_pairing_id),
            pairing_code: None,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve colon thread");
    let colon_queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "colon-thread-queued",
            "telegram:user:456",
            "telegram:chat:-123",
            Some("telegram:topic:77"),
            "thread again with provider-shaped refs",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("queue colon thread");
    assert_eq!(colon_queued.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(
        colon_queued.session_key.as_deref(),
        Some("channel:slack:thread:telegram%3Achat%3A-123:telegram%3Atopic%3A77")
    );
    let colon_turn_id = colon_queued.turn_id.expect("colon turn id");
    let colon_stream = wait_for_stream_events(&kernel, "slack", "colon-worker", |events| {
        events.iter().any(|event| {
            event.turn_id == Some(colon_turn_id) && event.code.as_deref() == Some("queue.completed")
        })
    })
    .await;
    assert!(colon_stream.events.iter().any(|event| {
        event.turn_id == Some(colon_turn_id) && event.peer_id == "telegram:chat:-123"
    }));

    let colon_session_id = colon_queued.session_id.expect("colon session id");
    let action = kernel
        .turn_session(SessionTurnRequest {
            session_id: colon_session_id,
            user_text: "follow up on escaped thread session".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("mutate escaped thread session");
    assert!(action
        .assistant_text
        .contains("follow up on escaped thread session"));
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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("slow-answer".to_string()),
            ..KernelOptions::default()
        })
        .await;
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
    let _session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: direct_session_key("terminal", "peer-resume-head"),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            event_id: "resume-head-7252".to_string(),
            sender_ref: "peer-resume-head".to_string(),
            conversation_ref: "peer-resume-head".to_string(),
            thread_ref: None,
            message_ref: Some("resume-head-7252".to_string()),
            text: Some("resume before answer".to_string()),
            attachments: Vec::new(),
            reply_to_ref: None,
            trigger: ChannelTrigger::Dm,
            received_at: None,
            provider_metadata: serde_json::json!({"update_id": 7252}),
        })
        .await
        .expect("queue running turn");

    let resume_after_sequence = wait_for_running_snapshot_without_answer(
        &kernel,
        "terminal",
        &direct_session_key("terminal", "peer-resume-head"),
    )
    .await;

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
            peer_id: direct_session_key("terminal", "peer-action"),
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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("partial-failure".to_string()),
            ..KernelOptions::default()
        })
        .await;
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
            peer_id: direct_session_key("terminal", "peer-failure"),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            event_id: "failure-7302".to_string(),
            sender_ref: "peer-failure".to_string(),
            conversation_ref: "peer-failure".to_string(),
            thread_ref: None,
            message_ref: Some("failure-7302".to_string()),
            text: Some("fail-case".to_string()),
            attachments: Vec::new(),
            reply_to_ref: None,
            trigger: ChannelTrigger::Dm,
            received_at: None,
            provider_metadata: serde_json::json!({"update_id": 7302}),
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

#[tokio::test]
async fn channel_inbound_first_column_slash_input_uses_runtime_control_route() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "runtime-control-skill").await;
    let kernel = env.kernel().await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-runtime-control",
        "mock",
        7451,
        "runtime-control-7451",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-runtime-control").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-runtime-control".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive runtime-control session");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-runtime-control".to_string(),
            text: "/handled now".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7452),
            external_message_id: Some("runtime-control-7452".to_string()),
            runtime_id: Some("mock".to_string()),
        })
        .await
        .expect("queue runtime control");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.turn_id == queued_turn_id
                && turn.kind == SessionTurnKind::RuntimeControl
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/handled now"
                && turn.prompt_user_text.is_empty()
                && turn.assistant_text == "mock runtime handled control"
        },
        "completed channel runtime-control turn",
    )
    .await;

    let stream =
        wait_for_stream_events(&kernel, "terminal", "terminal-runtime-control", |events| {
            let codes = events
                .iter()
                .filter_map(|event| event.code.as_deref())
                .collect::<Vec<_>>();
            codes.contains(&"queue.completed")
                && events.iter().any(|event| {
                    event.turn_id == Some(queued_turn_id)
                        && event.kind == StreamEventKindDto::TurnCompleted
                })
        })
        .await;
    assert_turn_completed_before_done(&stream.events, queued_turn_id, "channel runtime control");

    let audit = wait_for_audit_event_count(&kernel, "runtime.control.route", 1).await;
    let queued_turn_id_text = queued_turn_id.to_string();
    assert!(audit.events.iter().any(|event| {
        event.details["turn_id"].as_str() == Some(queued_turn_id_text.as_str())
            && event.details["origin"].as_str() == Some("channel_inbound")
            && event.details["command_name"].as_str() == Some("handled")
    }));
}

#[tokio::test]
async fn channel_inbound_lionclaw_retry_uses_lionclaw_action_route() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "lionclaw-action-skill").await;
    let kernel = env.kernel().await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-lionclaw-action",
        "mock",
        7501,
        "lionclaw-action-7501",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-lionclaw-action").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-lionclaw-action".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive action session");

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "seed retry source".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("seed retry source");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-lionclaw-action".to_string(),
            text: "/lionclaw retry".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7502),
            external_message_id: Some("lionclaw-action-7502".to_string()),
            runtime_id: Some("mock".to_string()),
        })
        .await
        .expect("queue LionClaw retry");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.turn_id == queued_turn_id
                && turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/lionclaw retry"
                && turn.prompt_user_text == "seed retry source"
                && turn.assistant_text.contains("seed retry source")
        },
        "completed channel LionClaw retry",
    )
    .await;

    let stream =
        wait_for_stream_events(&kernel, "terminal", "terminal-lionclaw-action", |events| {
            events.iter().any(|event| {
                event.turn_id == Some(queued_turn_id)
                    && event.kind == StreamEventKindDto::TurnCompleted
            })
        })
        .await;
    assert_turn_completed_before_done(&stream.events, queued_turn_id, "channel LionClaw retry");

    let audit = wait_for_audit_event_count(&kernel, "channel.lionclaw_control", 1).await;
    let queued_turn_id_text = queued_turn_id.to_string();
    assert!(audit.events.iter().any(|event| {
        event.details["turn_id"].as_str() == Some(queued_turn_id_text.as_str())
            && event.details["command_name"].as_str() == Some("retry")
    }));
}

#[tokio::test]
async fn channel_inbound_bare_retry_stays_runtime_owned() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "bare-runtime-skill").await;
    let kernel = env.kernel().await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-bare-runtime",
        "mock",
        7551,
        "bare-runtime-7551",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-bare-runtime").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-bare-runtime".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive runtime-owned session");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-bare-runtime".to_string(),
            text: "/retry".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7552),
            external_message_id: Some("bare-runtime-7552".to_string()),
            runtime_id: Some("mock".to_string()),
        })
        .await
        .expect("queue bare runtime control");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.turn_id == queued_turn_id
                && turn.kind == SessionTurnKind::RuntimeControl
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/retry"
                && turn.prompt_user_text.is_empty()
                && turn.assistant_text == "mock runtime does not support '/retry'"
        },
        "completed bare runtime-owned slash command",
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
    env.add_channel(channel_id, skill_name, ChannelLaunchMode::Background)
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

fn direct_session_key(channel_id: &str, peer_id: &str) -> String {
    format!("channel:{channel_id}:direct:{}", session_key_part(peer_id))
}

fn session_key_part(value: &str) -> String {
    value.replace('%', "%25").replace(':', "%3A")
}

fn v2_text_request(
    channel_id: &str,
    event_id: &str,
    sender_ref: &str,
    conversation_ref: &str,
    thread_ref: Option<&str>,
    text: &str,
    trigger: ChannelTrigger,
) -> ChannelInboundRequest {
    ChannelInboundRequest {
        channel_id: channel_id.to_string(),
        event_id: event_id.to_string(),
        sender_ref: sender_ref.to_string(),
        conversation_ref: conversation_ref.to_string(),
        thread_ref: thread_ref.map(str::to_string),
        message_ref: Some(event_id.to_string()),
        text: Some(text.to_string()),
        attachments: Vec::new(),
        reply_to_ref: None,
        trigger,
        received_at: None,
        provider_metadata: serde_json::json!({}),
    }
}

fn attachment_descriptor(attachment_id: &str) -> ChannelAttachmentDescriptor {
    ChannelAttachmentDescriptor {
        attachment_id: attachment_id.to_string(),
        kind: "image".to_string(),
        mime_type: Some("image/png".to_string()),
        filename: Some("image.png".to_string()),
        size_bytes: Some(12),
        provider_file_ref: "provider-file-1".to_string(),
        caption: None,
    }
}

async fn connect_test_pool(db_path: &std::path::Path) -> sqlx::SqlitePool {
    let db_url = format!("sqlite://{}", db_path.display());
    sqlx::SqlitePool::connect(&db_url)
        .await
        .expect("connect test db")
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
    assert_eq!(response.outcome, ChannelInboundOutcome::PendingApproval);
}

async fn approve_peer(kernel: &Kernel, channel_id: &str, peer_id: &str) {
    let pairings = kernel
        .list_channel_pairings(Some(channel_id.to_string()), None)
        .await
        .expect("list pairings");
    let pairing_id = pairings
        .pairings
        .iter()
        .find(|value| value.sender_ref.as_deref() == Some(peer_id))
        .map(|pairing| pairing.pairing_id)
        .expect("pairing id");
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: channel_id.to_string(),
            pairing_id: Some(pairing_id),
            pairing_code: None,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve pairing");
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
            peer_id: direct_session_key("terminal", "peer-start-failure"),
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
            peer_id: direct_session_key("terminal", "peer-close-failure"),
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
