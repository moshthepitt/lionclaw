mod common;

use async_trait::async_trait;
use common::{write_skill_source, TestHome};
use lionclaw::{
    api::build_router,
    contracts::{
        ChannelAttachmentDescriptor, ChannelAttachmentFinalizeOutcome,
        ChannelAttachmentFinalizeRequest, ChannelAttachmentMissingReport,
        ChannelAttachmentStageResponse, ChannelAttachmentStatus, ChannelGrantView,
        ChannelInboundOutcome, ChannelInboundRequest, ChannelOutboxDeliveryStatusDto,
        ChannelOutboxPullRequest, ChannelOutboxReportOutcomeDto, ChannelOutboxReportRequest,
        ChannelPairingApproveRequest, ChannelPairingBlockRequest, ChannelPairingBlockResponse,
        ChannelPairingClaimOutcome, ChannelPairingClaimRequest, ChannelPairingInviteRequest,
        ChannelPairingStatus, ChannelRoutingProfile, ChannelStreamAckRequest,
        ChannelStreamEventView, ChannelStreamPullRequest, ChannelStreamStartMode, ChannelTrigger,
        DaemonInfoResponse, SessionActionKind, SessionActionRequest, SessionHistoryPolicy,
        SessionHistoryRequest, SessionLatestQuery, SessionOpenRequest, SessionTurnKind,
        SessionTurnRequest, SessionTurnStatus, StreamEventKindDto, StreamLaneDto, TrustTier,
    },
    kernel::{
        channel_attachments::{MAX_CHANNEL_ATTACHMENT_BYTES, MAX_CHANNEL_EVENT_ATTACHMENT_BYTES},
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
            RuntimeEventSender, RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput,
            RuntimeTurnInput, RuntimeTurnResult,
        },
        ChannelAttachmentStageContent, ChannelAttachmentStageInput, Kernel, KernelError,
        KernelOptions,
    },
    operator::{config::ChannelLaunchMode, reconcile::add_channel},
};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::time::{sleep, Duration, Instant};

fn expect_blocked_grant(blocked: ChannelPairingBlockResponse) -> ChannelGrantView {
    let grant = blocked.grant.expect("block response should include grant");
    assert_eq!(grant.status, "blocked");
    grant
}

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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "local-cli",
            "msg-1001",
            "peer-local",
            "peer-local",
            None,
            "hello inbound-skill",
            ChannelTrigger::Dm,
        ))
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
        .ingest_channel_inbound(v2_text_request(
            "local-cli",
            "msg-1002",
            "peer-local",
            "peer-local",
            None,
            "please run inbound-skill now",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("approved inbound turn should succeed");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");
    assert!(queued.session_id.is_some());

    let duplicate = kernel
        .ingest_channel_inbound(v2_text_request(
            "local-cli",
            "msg-1002",
            "peer-local",
            "peer-local",
            None,
            "please run inbound-skill now",
            ChannelTrigger::Dm,
        ))
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

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "channel-worker-test".to_string(),
            limit: Some(10),
            lease_ms: Some(120_000),
        })
        .await
        .expect("pull channel outbox");
    assert_eq!(outbox.deliveries.len(), 1);
    assert_eq!(outbox.deliveries[0].conversation_ref, "peer-local");
    assert_eq!(
        outbox.deliveries[0].reply_to_ref.as_deref(),
        Some("msg-1002")
    );
    assert!(outbox.deliveries[0].content.text.contains("[mock]"));
    let report = kernel
        .report_channel_outbox(ChannelOutboxReportRequest {
            delivery_id: outbox.deliveries[0].delivery_id,
            attempt_id: outbox.deliveries[0].attempt_id,
            channel_id: "local-cli".to_string(),
            worker_id: "channel-worker-test".to_string(),
            outcome: ChannelOutboxReportOutcomeDto::Delivered,
            provider_receipt: Some(serde_json::json!({"message_ref": "provider-1"})),
            error_code: None,
            error_text: None,
        })
        .await
        .expect("report channel outbox delivered");
    assert!(report.accepted);
    assert_eq!(report.status, ChannelOutboxDeliveryStatusDto::Delivered);
}

#[tokio::test]
async fn channel_outbox_rejects_stale_reports_and_keeps_retry_backoff_in_kernel() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "local-cli", "channel-outbox-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "local-cli",
            "msg-outbox-stale-run",
            "peer-outbox",
            "peer-outbox",
            None,
            "hello outbox",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("pending inbound handled");
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "local-cli".to_string(),
            pairing_id: None,
            pairing_code: pending.pairing_code,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve pairing");
    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "local-cli",
            "msg-outbox-stale",
            "peer-outbox",
            "peer-outbox",
            None,
            "hello outbox",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("approved inbound handled");
    let queued_turn_id = queued.turn_id.expect("queued turn id");
    wait_for_stream_events(&kernel, "local-cli", "outbox-stale-stream", |events| {
        events.iter().any(|event| {
            event.turn_id == Some(queued_turn_id) && event.kind == StreamEventKindDto::Done
        })
    })
    .await;

    let first_pull = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "worker-1".to_string(),
            limit: Some(1),
            lease_ms: Some(1),
        })
        .await
        .expect("pull first outbox lease");
    assert_eq!(first_pull.deliveries.len(), 1);
    sleep(Duration::from_millis(5)).await;
    let second_pull = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "worker-2".to_string(),
            limit: Some(1),
            lease_ms: Some(120_000),
        })
        .await
        .expect("re-lease expired outbox delivery");
    assert_eq!(second_pull.deliveries.len(), 1);
    assert_eq!(
        second_pull.deliveries[0].delivery_id,
        first_pull.deliveries[0].delivery_id
    );
    assert_eq!(second_pull.deliveries[0].attempt_count, 2);

    let stale_report = kernel
        .report_channel_outbox(ChannelOutboxReportRequest {
            delivery_id: first_pull.deliveries[0].delivery_id,
            attempt_id: first_pull.deliveries[0].attempt_id,
            channel_id: "local-cli".to_string(),
            worker_id: "worker-1".to_string(),
            outcome: ChannelOutboxReportOutcomeDto::Delivered,
            provider_receipt: Some(serde_json::json!({"message_ref": "late"})),
            error_code: None,
            error_text: None,
        })
        .await
        .expect("report stale delivery");
    assert!(!stale_report.accepted);
    assert_eq!(stale_report.status, ChannelOutboxDeliveryStatusDto::Leased);

    let retryable_report = kernel
        .report_channel_outbox(ChannelOutboxReportRequest {
            delivery_id: second_pull.deliveries[0].delivery_id,
            attempt_id: second_pull.deliveries[0].attempt_id,
            channel_id: "local-cli".to_string(),
            worker_id: "worker-2".to_string(),
            outcome: ChannelOutboxReportOutcomeDto::RetryableFailed,
            provider_receipt: None,
            error_code: Some("provider.rate_limited".to_string()),
            error_text: Some("try later".to_string()),
        })
        .await
        .expect("report retryable delivery failure");
    assert!(retryable_report.accepted);
    assert_eq!(
        retryable_report.status,
        ChannelOutboxDeliveryStatusDto::Pending
    );
    assert!(retryable_report.next_attempt_at.is_some());
    let immediate_pull = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "local-cli".to_string(),
            worker_id: "worker-2".to_string(),
            limit: Some(1),
            lease_ms: Some(120_000),
        })
        .await
        .expect("pull during backoff");
    assert!(immediate_pull.deliveries.is_empty());
}

#[tokio::test]
async fn channel_stream_pull_and_ack_round_trip() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "telegram", "channel-outbox-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let inbound = kernel
        .ingest_channel_inbound(v2_text_request(
            "telegram",
            "update-4001",
            "peer-tele",
            "peer-tele",
            None,
            "hello kernel",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("process inbound");
    assert_eq!(inbound.outcome, ChannelInboundOutcome::PendingApproval);
    assert!(inbound.turn_id.is_none());
    approve_pairing(&kernel, "telegram", "peer-tele").await;

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "telegram",
            "update-4002",
            "peer-tele",
            "peer-tele",
            None,
            "hello approved kernel",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("process approved inbound");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    let stream = wait_for_stream_events(&kernel, "telegram", "telegram-worker", |events| {
        events.iter().any(|event| {
            event.turn_id == Some(queued_turn_id)
                && event.code.as_deref() == Some("queue.completed")
        }) && events.iter().any(|event| {
            event.turn_id == Some(queued_turn_id) && event.kind == StreamEventKindDto::Done
        })
    })
    .await;
    assert!(!stream.events.is_empty());
    assert_turn_completed_before_done(&stream.events, queued_turn_id, "acked channel turn");
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
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "tail-5001",
            "peer-tail",
            "peer-tail",
            None,
            "hello before tail connect",
            ChannelTrigger::Dm,
        ))
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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let delayed_inbound = async {
        sleep(Duration::from_millis(50)).await;
        let pending = kernel
            .ingest_channel_inbound(v2_text_request(
                "telegram",
                "wait-6001",
                "peer-wait",
                "peer-wait",
                None,
                "hello after long poll",
                ChannelTrigger::Dm,
            ))
            .await
            .expect("delayed inbound");
        assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);
        approve_pairing(&kernel, "telegram", "peer-wait").await;
        let queued = kernel
            .ingest_channel_inbound(v2_text_request(
                "telegram",
                "wait-6002",
                "peer-wait",
                "peer-wait",
                None,
                "hello after approval",
                ChannelTrigger::Dm,
            ))
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
        .expect_err("unapproved grant should be rejected");
    assert!(matches!(
        unapproved_err,
        KernelError::BadRequest(message) if message.contains("not approved")
    ));

    create_pending_pairing(&kernel, "terminal", "peer-open", "open-7001").await;
    let pending_err = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: direct_session_key("terminal", "peer-open"),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("pending pairing should be rejected");
    assert!(matches!(
        pending_err,
        KernelError::BadRequest(message) if message.contains("not approved")
    ));

    approve_pairing(&kernel, "terminal", "peer-open").await;
    let opened = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: direct_session_key("terminal", "peer-open"),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("approved grant session open");
    assert_eq!(opened.trust_tier.as_str(), TrustTier::Main.as_str());
}

#[tokio::test]
async fn direct_channel_session_turn_streams_turn_completed_then_done() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "direct-turn-skill").await;
    let kernel = env.kernel().await;
    create_pending_pairing(&kernel, "terminal", "peer-direct", "direct-7051").await;
    approve_pairing(&kernel, "terminal", "peer-direct").await;
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
    assert!(pairings.grants.is_empty());
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

    let access_state = kernel
        .list_channel_pairings(Some("terminal".to_string()), None)
        .await
        .expect("list channel access state");
    let listed_grant = access_state
        .grants
        .iter()
        .find(|value| value.sender_ref.as_deref() == Some("alice"))
        .expect("approved grant listed");
    assert_eq!(listed_grant.status, "approved");
    assert_eq!(listed_grant.trust_tier.as_str(), "main");

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

    let legacy_message_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'channel_messages'",
    )
    .fetch_one(&pool)
    .await
    .expect("query legacy channel message table count");
    assert_eq!(
        legacy_message_tables, 0,
        "legacy channel_messages table must not remain in the final schema"
    );
    let outbox_columns = sqlx::query("PRAGMA table_info(channel_outbox_messages)")
        .fetch_all(&pool)
        .await
        .expect("query channel outbox columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(
        outbox_columns.iter().any(|name| name == "content_json"),
        "durable channel outbound content must live in the outbox"
    );
    let inbound_event_columns = sqlx::query("PRAGMA table_info(channel_inbound_events)")
        .fetch_all(&pool)
        .await
        .expect("query inbound event columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(
        inbound_event_columns.iter().any(|name| name == "text"),
        "normalized inbound facts must preserve the v2 text contract"
    );
    let inbound_text: String =
        sqlx::query_scalar("SELECT text FROM channel_inbound_events WHERE event_id = ?1")
            .bind("event-2")
            .fetch_one(&pool)
            .await
            .expect("query persisted inbound text");
    assert_eq!(inbound_text, "run this");
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
async fn channels_v2_block_by_scope_closes_matching_pending_pairing() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "slack", "v2-scope-block-skill").await;
    let kernel = env.kernel().await;

    let pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "scope-block-pending",
            "mallory",
            "room-1",
            Some("topic-a"),
            "please approve this thread",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("create pending thread pairing");
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);
    let pairing_id = pending.pairing_id.expect("pairing id");
    let pairing_code = pending.pairing_code.expect("pairing code");

    expect_blocked_grant(
        kernel
            .block_channel_pairing(ChannelPairingBlockRequest {
                channel_id: "slack".to_string(),
                pairing_id: None,
                sender_ref: Some("mallory".to_string()),
                conversation_ref: Some("room-1".to_string()),
                thread_ref: Some("topic-a".to_string()),
                reason: Some("operator_blocked".to_string()),
            })
            .await
            .expect("block pending scope"),
    );

    let access_state = kernel
        .list_channel_pairings(Some("slack".to_string()), None)
        .await
        .expect("list channel access state");
    let listed_pairing = access_state
        .pairings
        .iter()
        .find(|pairing| pairing.pairing_id == pairing_id)
        .expect("blocked pairing listed");
    assert_eq!(listed_pairing.status, ChannelPairingStatus::Blocked);
    assert!(access_state.grants.iter().any(|grant| {
        grant.status == "blocked"
            && grant.sender_ref.as_deref() == Some("mallory")
            && grant.conversation_ref.as_deref() == Some("room-1")
            && grant.thread_ref.as_deref() == Some("topic-a")
    }));

    let pending_state = kernel
        .list_channel_pairings(
            Some("slack".to_string()),
            Some(ChannelPairingStatus::Pending),
        )
        .await
        .expect("list pending pairings");
    assert!(
        pending_state.pairings.is_empty(),
        "blocked scope must not leave a stale pending approval"
    );

    let approve_by_id = kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "slack".to_string(),
            pairing_id: Some(pairing_id),
            pairing_code: None,
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect_err("blocked pairing id must not be approvable");
    assert!(matches!(approve_by_id, KernelError::Conflict(message) if message.contains("blocked")));

    let approve_by_code = kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "slack".to_string(),
            pairing_id: None,
            pairing_code: Some(pairing_code),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect_err("blocked pairing code must not be approvable");
    assert!(
        matches!(approve_by_code, KernelError::Conflict(message) if message.contains("blocked"))
    );

    let inbound = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "scope-block-after",
            "mallory",
            "room-1",
            Some("topic-a"),
            "still blocked",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("blocked inbound handled");
    assert_eq!(inbound.outcome, ChannelInboundOutcome::Blocked);
    assert_eq!(inbound.reason_code.as_deref(), Some("blocked_grant"));
}

#[tokio::test]
async fn channels_v2_direct_block_denies_scoped_session_access() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "slack", "v2-direct-block-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    let thread_pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "direct-block-thread-pending",
            "alice",
            "room-1",
            Some("topic-a"),
            "approve this thread",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("create thread pairing");
    approve_pairing_id(
        &kernel,
        "slack",
        thread_pending.pairing_id.expect("thread pairing id"),
    )
    .await;

    let thread = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "direct-block-thread-queued",
            "alice",
            "room-1",
            Some("topic-a"),
            "run thread turn",
            ChannelTrigger::ThreadContinuation,
        ))
        .await
        .expect("queue thread turn");
    assert_eq!(thread.outcome, ChannelInboundOutcome::Queued);

    let conversation_pending = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "direct-block-conversation-pending",
            "alice",
            "room-1",
            None,
            "approve this room",
            ChannelTrigger::Mention,
        ))
        .await
        .expect("create conversation pairing");
    approve_pairing_id(
        &kernel,
        "slack",
        conversation_pending
            .pairing_id
            .expect("conversation pairing id"),
    )
    .await;

    let conversation = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "direct-block-conversation-queued",
            "alice",
            "room-1",
            None,
            "run conversation turn",
            ChannelTrigger::Mention,
        ))
        .await
        .expect("queue conversation turn");
    assert_eq!(conversation.outcome, ChannelInboundOutcome::Queued);

    kernel
        .block_channel_pairing(ChannelPairingBlockRequest {
            channel_id: "slack".to_string(),
            pairing_id: None,
            sender_ref: Some("alice".to_string()),
            conversation_ref: None,
            thread_ref: None,
            reason: Some("operator_blocked".to_string()),
        })
        .await
        .expect("block direct sender");

    let conversation_open = kernel
        .open_session(SessionOpenRequest {
            channel_id: "slack".to_string(),
            peer_id: conversation_session_key("slack", "room-1", "alice"),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("direct block must deny conversation session open");
    assert!(matches!(
        conversation_open,
        KernelError::Conflict(message) if message.contains("blocked")
    ));

    let thread_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: thread.session_id.expect("thread session id"),
            user_text: "blocked thread follow up".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("direct block must deny thread session mutation");
    assert!(matches!(
        thread_turn,
        KernelError::Conflict(message) if message.contains("blocked")
    ));
}

#[tokio::test]
async fn channel_pairing_invite_returns_raw_token_once_and_direct_claim_creates_grant() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "invite-direct-skill").await;
    let kernel = env.kernel().await;

    let invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "terminal".to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: Some("alice".to_string()),
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create direct invite");
    assert!(invite.token.starts_with("lc_"));
    assert_eq!(invite.max_claims, 1);

    let pool = connect_test_pool(&env.home().db_path()).await;
    let row = sqlx::query(
        "SELECT code_hash, claim_policy, status, label, max_claims, claim_count \
         FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query invite pairing");
    let code_hash: String = row.get("code_hash");
    assert_ne!(code_hash, invite.token);
    assert!(!code_hash.contains(&invite.token));
    assert_eq!(row.get::<String, _>("claim_policy"), "token_claim");
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert_eq!(row.get::<String, _>("label"), "alice");
    assert_eq!(row.get::<i64, _>("max_claims"), 1);
    assert_eq!(row.get::<i64, _>("claim_count"), 0);

    let pairings = kernel
        .list_channel_pairings(Some("terminal".to_string()), None)
        .await
        .expect("list invite pairing");
    let serialized = serde_json::to_value(&pairings.pairings[0]).expect("serialize pairing");
    assert!(serialized.get("token").is_none());
    assert!(serialized.get("code_hash").is_none());

    let claimed = kernel
        .claim_channel_pairing(claim_request(
            "terminal",
            &invite.token,
            "sender-alice",
            "dm-alice",
            None,
        ))
        .await
        .expect("claim direct invite");
    assert_eq!(claimed.outcome, ChannelPairingClaimOutcome::Approved);
    let grant_id = claimed.grant_id.expect("direct claim grant id");

    let access = kernel
        .list_channel_pairings(Some("terminal".to_string()), None)
        .await
        .expect("list grants");
    let grant = access
        .grants
        .iter()
        .find(|grant| grant.grant_id == grant_id)
        .expect("claimed grant");
    assert_eq!(grant.routing_profile, ChannelRoutingProfile::Direct);
    assert_eq!(grant.sender_ref.as_deref(), Some("sender-alice"));
    assert!(grant.conversation_ref.is_none());
    assert_eq!(grant.label.as_deref(), Some("alice"));

    let claimed_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query claimed invite");
    assert_eq!(claimed_row.get::<String, _>("status"), "approved");
    assert_eq!(claimed_row.get::<i64, _>("claim_count"), 1);

    let reused = kernel
        .claim_channel_pairing(claim_request(
            "terminal",
            &invite.token,
            "sender-bob",
            "dm-bob",
            None,
        ))
        .await
        .expect("reuse one-claim invite");
    assert_eq!(reused.outcome, ChannelPairingClaimOutcome::AlreadyClaimed);
    assert!(reused.grant_id.is_none());

    let outbound = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "terminal".to_string(),
            requested_profile: ChannelRoutingProfile::Outbound,
            label: None,
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect_err("outbound invite should be rejected");
    assert!(matches!(
        outbound,
        KernelError::BadRequest(message) if message.contains("outbound grants are not user invites")
    ));

    let created = wait_for_audit_event_count(&kernel, "channel.pairing.invite_created", 1).await;
    assert!(created.events.iter().any(|event| {
        event.details["pairing_id"].as_str() == Some(&invite.pairing_id.to_string())
            && event.details["requested_profile"].as_str() == Some("direct")
    }));
    wait_for_audit_event_count(&kernel, "channel.pairing.claim_approved", 1).await;
    wait_for_audit_event_count(&kernel, "channel.pairing.claim_denied", 1).await;
}

#[tokio::test]
async fn channel_pairing_invite_block_by_pairing_id_revokes_pending_token() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "invite-block-skill").await;
    let kernel = env.kernel().await;

    let invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "terminal".to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: Some("exposed invite".to_string()),
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create invite to block");

    let blocked = kernel
        .block_channel_pairing(ChannelPairingBlockRequest {
            channel_id: "terminal".to_string(),
            pairing_id: Some(invite.pairing_id),
            sender_ref: None,
            conversation_ref: None,
            thread_ref: None,
            reason: Some("token_exposed".to_string()),
        })
        .await
        .expect("block pending token invite");
    assert!(blocked.grant.is_none());
    assert_eq!(blocked.blocked_pairing_ids, vec![invite.pairing_id]);

    let pool = connect_test_pool(&env.home().db_path()).await;
    let blocked_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query blocked invite");
    assert_eq!(blocked_row.get::<String, _>("status"), "blocked");
    assert_eq!(blocked_row.get::<i64, _>("claim_count"), 0);

    let claim = kernel
        .claim_channel_pairing(claim_request(
            "terminal",
            &invite.token,
            "sender-alice",
            "dm-alice",
            None,
        ))
        .await
        .expect("claim blocked invite");
    assert_eq!(claim.outcome, ChannelPairingClaimOutcome::InvalidToken);
    assert_eq!(claim.reason_code.as_deref(), Some("invalid_token"));
    assert!(claim.grant_id.is_none());

    let access = kernel
        .list_channel_pairings(Some("terminal".to_string()), None)
        .await
        .expect("list blocked invite");
    let listed = access
        .pairings
        .iter()
        .find(|pairing| pairing.pairing_id == invite.pairing_id)
        .expect("blocked invite listed");
    assert_eq!(listed.status, ChannelPairingStatus::Blocked);
    assert!(access.grants.is_empty());

    let blocked_event = wait_for_audit_event_count(&kernel, "channel.pairing.blocked", 1).await;
    assert!(blocked_event.events.iter().any(|event| {
        event.details["pairing_id"].as_str() == Some(&invite.pairing_id.to_string())
            && event.details["reason_code"].as_str() == Some("token_exposed")
            && event.details["grant_id"].is_null()
    }));
}

#[tokio::test]
async fn channel_pairing_claim_audit_excludes_provider_metadata_and_raw_tokens() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "invite-audit-skill").await;
    let kernel = env.kernel().await;

    let invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "terminal".to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: None,
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create direct invite");
    let raw_token = invite.token.clone();
    let approved = kernel
        .claim_channel_pairing(ChannelPairingClaimRequest {
            channel_id: "terminal".to_string(),
            token: raw_token.clone(),
            sender_ref: "sender-alice".to_string(),
            conversation_ref: "dm-alice".to_string(),
            thread_ref: None,
            provider_metadata: serde_json::json!({
                "message_text": format!("/start {raw_token}"),
                "nested": { "payload": raw_token.clone() },
            }),
        })
        .await
        .expect("claim invite with token-bearing metadata");
    assert_eq!(approved.outcome, ChannelPairingClaimOutcome::Approved);
    let approved_audit =
        wait_for_audit_event_count(&kernel, "channel.pairing.claim_approved", 1).await;
    assert_audit_details_exclude_raw_tokens(&approved_audit.events[0].details, &[&invite.token]);

    let invalid_token = "lc_invalid-denied-token";
    let denied = kernel
        .claim_channel_pairing(ChannelPairingClaimRequest {
            channel_id: "terminal".to_string(),
            token: invalid_token.to_string(),
            sender_ref: "sender-bob".to_string(),
            conversation_ref: "dm-bob".to_string(),
            thread_ref: None,
            provider_metadata: serde_json::json!({
                "message_text": format!("LC-ACCEPT {invalid_token}"),
                "original_invite": raw_token.clone(),
            }),
        })
        .await
        .expect("deny invalid claim with token-bearing metadata");
    assert_eq!(denied.outcome, ChannelPairingClaimOutcome::InvalidToken);
    let denied_audit = wait_for_audit_event_count(&kernel, "channel.pairing.claim_denied", 1).await;
    assert_audit_details_exclude_raw_tokens(
        &denied_audit.events[0].details,
        &[&raw_token, invalid_token],
    );
}

#[tokio::test]
async fn channel_pairing_invite_rejects_expiry_overflow() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "invite-expiry-skill").await;
    let kernel = env.kernel().await;

    let err = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "terminal".to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: None,
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: Some(i64::MAX as u64),
            max_claims: None,
        })
        .await
        .expect_err("overflowing invite expiry should be rejected");

    assert!(matches!(
        err,
        KernelError::BadRequest(message) if message.contains("expires_in_ms is too large")
    ));
}

#[tokio::test]
async fn channel_pairing_conversation_invite_scopes_each_claimed_sender() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "slack", "invite-conversation-skill").await;
    let kernel = env.kernel().await;

    let invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "slack".to_string(),
            requested_profile: ChannelRoutingProfile::Conversation,
            label: Some("room invite".to_string()),
            conversation_ref: Some("room-1".to_string()),
            thread_ref: None,
            expires_in_ms: None,
            max_claims: Some(2),
        })
        .await
        .expect("create conversation invite");

    let mismatch = kernel
        .claim_channel_pairing(claim_request(
            "slack",
            &invite.token,
            "alice",
            "room-2",
            None,
        ))
        .await
        .expect("claim wrong conversation");
    assert_eq!(mismatch.outcome, ChannelPairingClaimOutcome::ScopeMismatch);
    assert_eq!(
        mismatch.reason_code.as_deref(),
        Some("conversation_ref_mismatch")
    );

    let pool = connect_test_pool(&env.home().db_path()).await;
    let alice = kernel
        .claim_channel_pairing(claim_request(
            "slack",
            &invite.token,
            "alice",
            "room-1",
            None,
        ))
        .await
        .expect("claim conversation invite for alice");
    assert_eq!(alice.outcome, ChannelPairingClaimOutcome::Approved);

    let replayed_alice = kernel
        .claim_channel_pairing(claim_request(
            "slack",
            &invite.token,
            "alice",
            "room-1",
            None,
        ))
        .await
        .expect("replay alice conversation invite");
    assert_eq!(
        replayed_alice.outcome,
        ChannelPairingClaimOutcome::AlreadyClaimed
    );
    assert_eq!(replayed_alice.grant_id, alice.grant_id);
    let one_claim_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query one claimed invite");
    assert_eq!(one_claim_row.get::<String, _>("status"), "pending");
    assert_eq!(one_claim_row.get::<i64, _>("claim_count"), 1);

    let bob = kernel
        .claim_channel_pairing(claim_request("slack", &invite.token, "bob", "room-1", None))
        .await
        .expect("claim conversation invite for bob");
    assert_eq!(bob.outcome, ChannelPairingClaimOutcome::Approved);

    let claimed_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query multi-claim invite");
    assert_eq!(claimed_row.get::<String, _>("status"), "approved");
    assert_eq!(claimed_row.get::<i64, _>("claim_count"), 2);

    let access = kernel
        .list_channel_pairings(Some("slack".to_string()), None)
        .await
        .expect("list conversation grants");
    for sender in ["alice", "bob"] {
        assert!(access.grants.iter().any(|grant| {
            grant.routing_profile == ChannelRoutingProfile::Conversation
                && grant.sender_ref.as_deref() == Some(sender)
                && grant.conversation_ref.as_deref() == Some("room-1")
                && grant.thread_ref.is_none()
        }));
    }

    let over_claimed = kernel
        .claim_channel_pairing(claim_request(
            "slack",
            &invite.token,
            "carol",
            "room-1",
            None,
        ))
        .await
        .expect("over-claim conversation invite");
    assert_eq!(
        over_claimed.outcome,
        ChannelPairingClaimOutcome::AlreadyClaimed
    );

    let blocked_invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "slack".to_string(),
            requested_profile: ChannelRoutingProfile::Conversation,
            label: None,
            conversation_ref: Some("room-blocked".to_string()),
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create blocked conversation invite");
    let blocked_grant = expect_blocked_grant(
        kernel
            .block_channel_pairing(ChannelPairingBlockRequest {
                channel_id: "slack".to_string(),
                pairing_id: None,
                sender_ref: Some("mallory".to_string()),
                conversation_ref: None,
                thread_ref: None,
                reason: Some("qa_block".to_string()),
            })
            .await
            .expect("block sender scope"),
    );
    let blocked = kernel
        .claim_channel_pairing(claim_request(
            "slack",
            &blocked_invite.token,
            "mallory",
            "room-blocked",
            None,
        ))
        .await
        .expect("claim conversation invite with blocked sender");
    assert_eq!(blocked.outcome, ChannelPairingClaimOutcome::ScopeMismatch);
    assert_eq!(blocked.reason_code.as_deref(), Some("scope_blocked"));
    assert_eq!(blocked.grant_id, Some(blocked_grant.grant_id));
    let blocked_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(blocked_invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query blocked invite");
    assert_eq!(blocked_row.get::<String, _>("status"), "pending");
    assert_eq!(blocked_row.get::<i64, _>("claim_count"), 0);
}

#[tokio::test]
async fn channel_pairing_thread_expired_and_invalid_claims_are_denied() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "telegram", "invite-thread-skill").await;
    let kernel = env.kernel().await;

    let thread_invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "telegram".to_string(),
            requested_profile: ChannelRoutingProfile::Thread,
            label: None,
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create thread invite");

    let missing_thread = kernel
        .claim_channel_pairing(claim_request(
            "telegram",
            &thread_invite.token,
            "telegram:user:1",
            "telegram:chat:-1",
            None,
        ))
        .await
        .expect("claim thread invite without thread_ref");
    assert_eq!(
        missing_thread.outcome,
        ChannelPairingClaimOutcome::ScopeMismatch
    );
    assert_eq!(
        missing_thread.reason_code.as_deref(),
        Some("thread_ref_required")
    );

    let claimed_thread = kernel
        .claim_channel_pairing(claim_request(
            "telegram",
            &thread_invite.token,
            "telegram:user:1",
            "telegram:chat:-1",
            Some("telegram:topic:42"),
        ))
        .await
        .expect("claim thread invite");
    assert_eq!(claimed_thread.outcome, ChannelPairingClaimOutcome::Approved);
    let thread_grant_id = claimed_thread.grant_id.expect("thread claim grant id");
    let access = kernel
        .list_channel_pairings(Some("telegram".to_string()), None)
        .await
        .expect("list thread grants");
    let thread_grant = access
        .grants
        .iter()
        .find(|grant| grant.grant_id == thread_grant_id)
        .expect("claimed thread grant");
    assert_eq!(thread_grant.routing_profile, ChannelRoutingProfile::Thread);
    assert_eq!(thread_grant.sender_ref.as_deref(), Some("telegram:user:1"));
    assert_eq!(
        thread_grant.conversation_ref.as_deref(),
        Some("telegram:chat:-1")
    );
    assert_eq!(
        thread_grant.thread_ref.as_deref(),
        Some("telegram:topic:42")
    );

    let blocked_thread_invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "telegram".to_string(),
            requested_profile: ChannelRoutingProfile::Thread,
            label: None,
            conversation_ref: Some("telegram:chat:-blocked".to_string()),
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create blocked thread invite");
    let blocked_thread_grant = expect_blocked_grant(
        kernel
            .block_channel_pairing(ChannelPairingBlockRequest {
                channel_id: "telegram".to_string(),
                pairing_id: None,
                sender_ref: Some("telegram:user:blocked".to_string()),
                conversation_ref: Some("telegram:chat:-blocked".to_string()),
                thread_ref: None,
                reason: Some("qa_block".to_string()),
            })
            .await
            .expect("block thread conversation scope"),
    );
    let blocked_thread = kernel
        .claim_channel_pairing(claim_request(
            "telegram",
            &blocked_thread_invite.token,
            "telegram:user:blocked",
            "telegram:chat:-blocked",
            Some("telegram:topic:blocked"),
        ))
        .await
        .expect("claim thread invite in blocked conversation");
    assert_eq!(
        blocked_thread.outcome,
        ChannelPairingClaimOutcome::ScopeMismatch
    );
    assert_eq!(blocked_thread.reason_code.as_deref(), Some("scope_blocked"));
    assert_eq!(blocked_thread.grant_id, Some(blocked_thread_grant.grant_id));
    let pool = connect_test_pool(&env.home().db_path()).await;
    let blocked_thread_row = sqlx::query(
        "SELECT status, claim_count FROM channel_pairing_requests WHERE pairing_id = ?1",
    )
    .bind(blocked_thread_invite.pairing_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("query blocked thread invite");
    assert_eq!(blocked_thread_row.get::<String, _>("status"), "pending");
    assert_eq!(blocked_thread_row.get::<i64, _>("claim_count"), 0);

    let expired_invite = kernel
        .invite_channel_pairing(ChannelPairingInviteRequest {
            channel_id: "telegram".to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: None,
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: None,
            max_claims: None,
        })
        .await
        .expect("create expirable invite");
    sqlx::query("UPDATE channel_pairing_requests SET expires_at_ms = 1 WHERE pairing_id = ?1")
        .bind(expired_invite.pairing_id.to_string())
        .execute(&pool)
        .await
        .expect("expire invite");

    let expired = kernel
        .claim_channel_pairing(claim_request(
            "telegram",
            &expired_invite.token,
            "telegram:user:2",
            "telegram:user:2",
            None,
        ))
        .await
        .expect("claim expired invite");
    assert_eq!(expired.outcome, ChannelPairingClaimOutcome::Expired);
    assert!(expired.grant_id.is_none());

    let invalid = kernel
        .claim_channel_pairing(claim_request(
            "telegram",
            "lc_not-a-real-token",
            "telegram:user:3",
            "telegram:user:3",
            None,
        ))
        .await
        .expect("claim invalid invite");
    assert_eq!(invalid.outcome, ChannelPairingClaimOutcome::InvalidToken);
    assert_eq!(invalid.reason_code.as_deref(), Some("invalid_token"));
    assert!(invalid.grant_id.is_none());

    wait_for_audit_event_count(&kernel, "channel.pairing.claim_denied", 3).await;
}

#[tokio::test]
async fn channels_v2_admission_rolls_back_dedupe_when_turn_cannot_be_queued() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "v2-atomic-skill").await;
    let kernel_without_runtime = env.kernel().await;

    let pending = kernel_without_runtime
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "atomic-pending",
            "alice",
            "alice",
            None,
            "hello",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("create pending pairing");
    let pairing_code = pending.pairing_code.expect("pairing code");
    kernel_without_runtime
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id: "terminal".to_string(),
            pairing_id: None,
            pairing_code: Some(pairing_code),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: None,
        })
        .await
        .expect("approve pairing");

    let err = kernel_without_runtime
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "atomic-event",
            "alice",
            "alice",
            None,
            "run after runtime exists",
            ChannelTrigger::Dm,
        ))
        .await
        .expect_err("missing default runtime should reject approved inbound");
    assert!(
        matches!(err, KernelError::BadRequest(message) if message.contains("runtime_id is required"))
    );

    let pool = connect_test_pool(&env.home().db_path()).await;
    let persisted_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM channel_inbound_events WHERE channel_id = 'terminal' AND event_id = 'atomic-event'",
    )
    .fetch_one(&pool)
    .await
    .expect("query atomic event count");
    assert_eq!(persisted_event_count, 0);

    let kernel_with_runtime = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;
    let queued = kernel_with_runtime
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "atomic-event",
            "alice",
            "alice",
            None,
            "run after runtime exists",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("retry should queue after runtime is configured");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
}

#[tokio::test]
async fn channels_v2_migration_uses_legacy_message_id_for_event_identity() {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(":memory:")
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect in-memory migration db");

    sqlx::raw_sql(
        r#"
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY NOT NULL,
            channel_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            project_scope TEXT NOT NULL,
            trust_tier TEXT NOT NULL CHECK (trust_tier IN ('main', 'untrusted')),
            history_policy TEXT NOT NULL CHECK (history_policy IN ('interactive', 'conservative')),
            created_at_ms INTEGER NOT NULL,
            last_turn_at_ms INTEGER,
            last_activity_at_ms INTEGER,
            turn_count INTEGER NOT NULL
        );

        CREATE TABLE channel_peers (
            channel_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'blocked')),
            trust_tier TEXT NOT NULL CHECK (trust_tier IN ('main', 'untrusted')),
            pairing_code TEXT NOT NULL,
            first_seen_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            PRIMARY KEY (channel_id, peer_id)
        );

        CREATE TABLE channel_messages (
            message_id TEXT PRIMARY KEY NOT NULL,
            channel_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
            external_message_id TEXT,
            update_id INTEGER,
            content TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL
        );

        CREATE TABLE session_turns (
            turn_id TEXT PRIMARY KEY NOT NULL,
            session_id TEXT NOT NULL,
            sequence_no INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('normal', 'retry', 'continue', 'runtime_control')),
            status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'timed_out', 'cancelled', 'interrupted')),
            display_user_text TEXT NOT NULL,
            prompt_user_text TEXT NOT NULL,
            assistant_text TEXT NOT NULL,
            error_code TEXT,
            error_text TEXT,
            runtime_id TEXT NOT NULL,
            started_at_ms INTEGER NOT NULL,
            finished_at_ms INTEGER,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            UNIQUE (session_id, sequence_no)
        );

        CREATE TABLE channel_turns (
            turn_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            inbound_message_id TEXT NOT NULL UNIQUE,
            runtime_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
            last_error TEXT,
            queued_at_ms INTEGER NOT NULL,
            started_at_ms INTEGER,
            finished_at_ms INTEGER,
            answer_checkpoint_sequence INTEGER,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            FOREIGN KEY (inbound_message_id) REFERENCES channel_messages(message_id) ON DELETE CASCADE
        );

        INSERT INTO sessions
            (session_id, channel_id, peer_id, project_scope, trust_tier, history_policy, created_at_ms, last_turn_at_ms, last_activity_at_ms, turn_count)
        VALUES
            ('session-1', 'terminal', 'alice', 'project-test', 'main', 'interactive', 1000, 1002, 1002, 2);

        INSERT INTO channel_peers
            (channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms)
        VALUES
            ('terminal', 'alice', 'approved', 'main', 'raw-code', 1000, 1002);

        INSERT INTO channel_messages
            (message_id, channel_id, peer_id, direction, external_message_id, update_id, content, created_at_ms)
        VALUES
            ('message-a', 'terminal', 'alice', 'inbound', 'duplicate-provider-id', NULL, 'first prompt', 1001),
            ('message-b', 'terminal', 'alice', 'inbound', 'duplicate-provider-id', NULL, 'second prompt', 1002);

        INSERT INTO channel_turns
            (turn_id, channel_id, peer_id, session_id, inbound_message_id, runtime_id, status, last_error, queued_at_ms, started_at_ms, finished_at_ms, answer_checkpoint_sequence)
        VALUES
            ('turn-a', 'terminal', 'alice', 'session-1', 'message-a', 'mock', 'completed', NULL, 1001, 1001, 1001, NULL),
            ('turn-b', 'terminal', 'alice', 'session-1', 'message-b', 'mock', 'completed', NULL, 1002, 1002, 1002, NULL);
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed legacy v1 channel schema");

    sqlx::raw_sql(include_str!(
        "../migrations/202605140002_channels_v2_core.sql"
    ))
    .execute(&pool)
    .await
    .expect("run channels v2 migration");

    let event_ids = sqlx::query_scalar::<_, String>(
        "SELECT event_id FROM channel_inbound_events ORDER BY event_id",
    )
    .fetch_all(&pool)
    .await
    .expect("query migrated inbound events");
    assert_eq!(
        event_ids,
        vec![
            "v1-message:message-a".to_string(),
            "v1-message:message-b".to_string(),
        ]
    );

    let turn_event_ids = sqlx::query_scalar::<_, String>(
        "SELECT inbound_event_id FROM channel_turns ORDER BY turn_id",
    )
    .fetch_all(&pool)
    .await
    .expect("query migrated channel turns");
    assert_eq!(turn_event_ids, event_ids);

    sqlx::raw_sql(include_str!(
        "../migrations/202605140005_channel_outbox.sql"
    ))
    .execute(&pool)
    .await
    .expect("run channel outbox migration");
    let legacy_message_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'channel_messages'",
    )
    .fetch_one(&pool)
    .await
    .expect("query legacy channel message table count");
    assert_eq!(legacy_message_tables, 0);
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

    let ignored_unpaired_plain = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "unpaired-plain",
            "charlie",
            "room-1",
            None,
            "ordinary message",
            ChannelTrigger::None,
        ))
        .await
        .expect("plain unpaired message");
    assert_eq!(
        ignored_unpaired_plain.outcome,
        ChannelInboundOutcome::TriggerIgnored
    );
    assert_eq!(
        ignored_unpaired_plain.reason_code.as_deref(),
        Some("trigger_insufficient")
    );
    assert!(
        ignored_unpaired_plain.pairing_id.is_none(),
        "untargeted inbound must not create a pairing"
    );
    assert!(
        ignored_unpaired_plain.pairing_code.is_none(),
        "untargeted inbound must not mint a pairing code"
    );

    let unpaired_access_state = kernel
        .list_channel_pairings(Some("slack".to_string()), None)
        .await
        .expect("list access state after untargeted inbound");
    assert!(
        unpaired_access_state
            .pairings
            .iter()
            .all(|pairing| pairing.sender_ref.as_deref() != Some("charlie")),
        "untargeted inbound must not surface as an approval request"
    );

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

    expect_blocked_grant(
        kernel
            .block_channel_pairing(ChannelPairingBlockRequest {
                channel_id: "slack".to_string(),
                pairing_id: None,
                sender_ref: Some("mallory".to_string()),
                conversation_ref: None,
                thread_ref: None,
                reason: Some("test_block".to_string()),
            })
            .await
            .expect("block sender"),
    );

    let access_state = kernel
        .list_channel_pairings(Some("slack".to_string()), None)
        .await
        .expect("list slack channel access state");
    assert!(access_state.grants.iter().any(|grant| {
        grant.sender_ref.as_deref() == Some("mallory") && grant.status == "blocked"
    }));

    let blocked = kernel
        .ingest_channel_inbound(v2_text_request(
            "slack",
            "blocked-sender",
            "mallory",
            "room-1",
            None,
            "untrusted plain message",
            ChannelTrigger::None,
        ))
        .await
        .expect("blocked sender inbound");
    assert_eq!(blocked.outcome, ChannelInboundOutcome::Blocked);
    assert_eq!(blocked.reason_code.as_deref(), Some("blocked_grant"));
    assert!(
        blocked.turn_id.is_none(),
        "blocked channel inbound must not create a turn"
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
    let bob_thread_session_key = thread_session_key("slack", "room-1", "topic-b", "bob");
    assert_eq!(
        queued_thread.session_key.as_deref(),
        Some(bob_thread_session_key.as_str())
    );
    let queued_thread_turn_id = queued_thread.turn_id.expect("thread turn id");
    let queued_thread_session_id = queued_thread.session_id.expect("thread session id");
    let thread_stream = wait_for_stream_events(&kernel, "slack", "thread-worker", |events| {
        stream_has_completed_and_done(events, queued_thread_turn_id)
    })
    .await;
    assert_turn_completed_before_done(&thread_stream.events, queued_thread_turn_id, "thread turn");

    kernel
        .block_channel_pairing(ChannelPairingBlockRequest {
            channel_id: "slack".to_string(),
            pairing_id: None,
            sender_ref: Some("mallory".to_string()),
            conversation_ref: Some("room-1".to_string()),
            thread_ref: Some("topic-b".to_string()),
            reason: Some("test_thread_block".to_string()),
        })
        .await
        .expect("block other thread sender");

    let bob_thread_action = kernel
        .turn_session(SessionTurnRequest {
            session_id: queued_thread_session_id,
            user_text: "authorized bob thread follow up".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("other sender block must not block bob's thread session");
    assert!(bob_thread_action
        .assistant_text
        .contains("authorized bob thread follow up"));

    let blocked_thread_open = kernel
        .open_session(SessionOpenRequest {
            channel_id: "slack".to_string(),
            peer_id: thread_session_key("slack", "room-1", "topic-b", "mallory"),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("blocked thread sender should not open a session");
    assert!(matches!(
        blocked_thread_open,
        KernelError::Conflict(message) if message.contains("blocked")
    ));

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            text: None,
            attachments: vec![captioned_attachment_descriptor("att-1", "see image")],
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
    let attachments_json: String = sqlx::query_scalar(
        "SELECT attachments_json FROM channel_inbound_events WHERE event_id = ?1",
    )
    .bind("thread-attachment")
    .fetch_one(&pool)
    .await
    .expect("query stored attachment descriptors");
    let stored_attachments: Vec<ChannelAttachmentDescriptor> =
        serde_json::from_str(&attachments_json).expect("decode stored attachment descriptors");
    assert_eq!(stored_attachments.len(), 1);
    assert_eq!(stored_attachments[0].caption.as_deref(), Some("see image"));

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
    let colon_thread_session_key = thread_session_key(
        "slack",
        "telegram:chat:-123",
        "telegram:topic:77",
        "telegram:user:456",
    );
    assert_eq!(
        colon_queued.session_key.as_deref(),
        Some(colon_thread_session_key.as_str())
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
async fn channel_attachment_stage_finalize_queues_manifest_and_runtime_mount() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-skill").await;
    let release_runtime = std::sync::Arc::new(tokio::sync::Notify::new());
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("paused-echo".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;
    kernel
        .register_runtime_adapter(
            "paused-echo",
            std::sync::Arc::new(PausedEchoAdapter {
                release: release_runtime.clone(),
            }),
        )
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-attach", "attach-1001").await;
    approve_pairing(&kernel, "terminal", "peer-attach").await;

    let staged_content = b"hello staged attachment".to_vec();
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![
                ChannelAttachmentDescriptor {
                    attachment_id: "att-doc".to_string(),
                    kind: "document".to_string(),
                    mime_type: Some("text/plain".to_string()),
                    filename: Some("../report.txt".to_string()),
                    size_bytes: Some(staged_content.len() as i64),
                    provider_file_ref: "provider-doc".to_string(),
                    caption: Some("doc caption".to_string()),
                },
                ChannelAttachmentDescriptor {
                    attachment_id: "att-missing".to_string(),
                    kind: "image".to_string(),
                    mime_type: Some("image/png".to_string()),
                    filename: Some("missing.png".to_string()),
                    size_bytes: None,
                    provider_file_ref: "provider-missing".to_string(),
                    caption: None,
                },
            ],
            ..v2_text_request(
                "terminal",
                "attach-1002",
                "peer-attach",
                "peer-attach",
                None,
                "please inspect attached files",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let orphan_path = env
        .home()
        .runtime_dir()
        .join("channels")
        .join(test_storage_component("terminal"))
        .join("attachments")
        .join(test_storage_component("attach-1002"))
        .join(test_storage_component("att-doc"))
        .join("report.txt");
    std::fs::create_dir_all(orphan_path.parent().expect("orphan parent"))
        .expect("create orphan attachment dir");
    std::fs::write(&orphan_path, b"orphaned pre-commit upload").expect("write orphaned upload");
    let upload_event_dir = orphan_path
        .parent()
        .expect("attachment dir")
        .parent()
        .expect("event dir")
        .to_path_buf();

    let staged = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "attach-1002".to_string(),
            attachment_id: "att-doc".to_string(),
            kind: "document".to_string(),
            filename: Some("../report.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: Some("doc caption".to_string()),
            content: ChannelAttachmentStageContent::Bytes(staged_content.clone()),
        })
        .await
        .expect("stage attachment");
    assert_eq!(staged.status, ChannelAttachmentStatus::Staged);
    assert_eq!(staged.size_bytes, staged_content.len() as i64);
    let expected_runtime_path = format!(
        "/attachments/{}/report.txt",
        test_runtime_attachment_component("att-doc")
    );
    assert_eq!(
        staged.runtime_path.as_deref(),
        Some(expected_runtime_path.as_str())
    );
    std::fs::write(
        upload_event_dir.join("unmanifested.txt"),
        b"must not be mounted",
    )
    .expect("write unmanifested attachment-side file");

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "attach-1002".to_string(),
            worker_id: "attachment-worker-test".to_string(),
            missing: vec![ChannelAttachmentMissingReport {
                attachment_id: "att-missing".to_string(),
                reason_code: "provider_missing".to_string(),
                reason_text: None,
            }],
        })
        .await
        .expect("finalize attachments");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);
    assert_eq!(finalized.turn_id, waiting.turn_id);
    let audit = wait_for_audit_event_count(&kernel, "channel.attachments.finalized", 1).await;
    let audit_event = audit
        .events
        .iter()
        .find(|event| event.details["event_id"].as_str() == Some("attach-1002"))
        .expect("attachment finalized audit event");
    assert_eq!(audit_event.details["staged_count"].as_i64(), Some(1));
    assert_eq!(audit_event.details["rejected_count"].as_i64(), Some(1));

    let session_id = waiting.session_id.expect("waiting session id");
    let mount_source = wait_for_attachment_mount_source(&kernel, session_id).await;
    assert_ne!(
        std::fs::canonicalize(&mount_source).expect("canonical projection"),
        std::fs::canonicalize(&upload_event_dir).expect("canonical upload event dir")
    );
    assert!(!mount_source.join("unmanifested.txt").exists());
    assert!(mount_source
        .join(test_runtime_attachment_component("att-doc"))
        .join("report.txt")
        .exists());
    release_runtime.notify_one();

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| turn.status == SessionTurnStatus::Completed,
        "attachment turn completion",
    )
    .await;
    wait_for_path_removed(&mount_source, "runtime projection cleanup").await;

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id,
            limit: Some(12),
        })
        .await
        .expect("session history");
    let turn = history.turns.last().expect("attachment turn");
    assert_eq!(turn.display_user_text, "please inspect attached files");
    assert!(!turn
        .display_user_text
        .contains("lionclaw_channel_attachment_manifest"));
    assert_eq!(turn.prompt_user_text, "please inspect attached files");
    assert!(turn.assistant_text.contains("channel_attachments"));
    assert!(turn
        .assistant_text
        .contains("lionclaw_channel_attachment_manifest"));
    assert!(turn.assistant_text.contains(&expected_runtime_path));
    assert!(turn.assistant_text.contains("provider_missing"));

    let pool = connect_test_pool(&env.home().db_path()).await;
    let row = sqlx::query(
        "SELECT status, filename, storage_path FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'attach-1002' AND attachment_id = 'att-doc'",
    )
    .fetch_one(&pool)
    .await
    .expect("query staged attachment");
    assert_eq!(row.get::<String, _>("status"), "staged");
    assert_eq!(row.get::<String, _>("filename"), "report.txt");
    let storage_path: String = row.get("storage_path");
    assert!(storage_path.starts_with(env.home().runtime_dir().to_str().expect("utf8 runtime dir")));
    assert!(!storage_path.contains(".."));
    assert_eq!(
        std::fs::read(&storage_path).expect("read staged file"),
        staged_content
    );
}

#[tokio::test]
async fn channel_attachment_waiting_turn_blocks_later_session_turns() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-order-skill").await;
    let prompts = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("recording-echo".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;
    kernel
        .register_runtime_adapter(
            "recording-echo",
            std::sync::Arc::new(RecordingEchoAdapter {
                prompts: prompts.clone(),
            }),
        )
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-order", "order-pairing").await;
    approve_pairing(&kernel, "terminal", "peer-order").await;

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![attachment_descriptor("order-att")],
            ..v2_text_request(
                "terminal",
                "order-waiting",
                "peer-order",
                "peer-order",
                None,
                "inspect the attachment first",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let later = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "order-later",
            "peer-order",
            "peer-order",
            None,
            "plain follow up after attachment",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("later turn queues");
    assert_eq!(later.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(later.session_id, waiting.session_id);

    let pool = connect_test_pool(&env.home().db_path()).await;
    let later_turn_id = later.turn_id.expect("later turn id");
    assert_channel_turn_status_stays(
        &pool,
        later_turn_id,
        "pending",
        Duration::from_millis(250),
        "later turn must stay queued behind waiting attachments",
    )
    .await;
    assert!(
        prompts.lock().await.is_empty(),
        "runtime must not start a later turn while an earlier turn waits for attachments"
    );

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "order-waiting".to_string(),
            worker_id: "attachment-worker-test".to_string(),
            missing: vec![ChannelAttachmentMissingReport {
                attachment_id: "order-att".to_string(),
                reason_code: "provider_missing".to_string(),
                reason_text: None,
            }],
        })
        .await
        .expect("finalize waiting attachment turn");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);

    let waiting_turn_id = waiting.turn_id.expect("waiting turn id");
    wait_for_joined_turn_statuses(&pool, waiting_turn_id, "completed", "completed").await;
    wait_for_joined_turn_statuses(&pool, later_turn_id, "completed", "completed").await;

    let recorded = prompts.lock().await.clone();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[0].contains("inspect the attachment first"));
    assert!(recorded[0].contains("lionclaw_channel_attachment_manifest"));
    assert!(recorded[1].contains("plain follow up after attachment"));
    assert!(!recorded[1].contains("lionclaw_channel_attachment_manifest"));
}

#[tokio::test]
async fn channel_lionclaw_retry_preserves_attachment_manifest_and_mount() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-retry-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-attach-retry",
        "attach-retry-1001",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-attach-retry").await;

    let staged_content = b"retry attachment content".to_vec();
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![ChannelAttachmentDescriptor {
                attachment_id: "att-retry".to_string(),
                kind: "document".to_string(),
                mime_type: Some("text/plain".to_string()),
                filename: Some("retry.txt".to_string()),
                size_bytes: Some(staged_content.len() as i64),
                provider_file_ref: "provider-retry".to_string(),
                caption: Some("retry caption".to_string()),
            }],
            ..v2_text_request(
                "terminal",
                "attach-retry-1002",
                "peer-attach-retry",
                "peer-attach-retry",
                None,
                "inspect retry attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let staged = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "attach-retry-1002".to_string(),
            attachment_id: "att-retry".to_string(),
            kind: "document".to_string(),
            filename: Some("retry.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: Some("retry caption".to_string()),
            content: ChannelAttachmentStageContent::Bytes(staged_content),
        })
        .await
        .expect("stage retry attachment");
    assert_eq!(staged.status, ChannelAttachmentStatus::Staged);
    let expected_runtime_path = format!(
        "/attachments/{}/retry.txt",
        test_runtime_attachment_component("att-retry")
    );

    kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "attach-retry-1002".to_string(),
            worker_id: "attachment-retry-worker-test".to_string(),
            missing: Vec::new(),
        })
        .await
        .expect("finalize retry source attachments");

    let session_id = waiting.session_id.expect("waiting session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect retry attachment"
                && turn.assistant_text.contains(&expected_runtime_path)
        },
        "attachment source turn completion",
    )
    .await;

    let retry = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "attach-retry-1003",
            "peer-attach-retry",
            "peer-attach-retry",
            None,
            "/lionclaw retry",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue attachment retry command");
    assert_eq!(retry.outcome, ChannelInboundOutcome::Queued);
    let retry_turn_id = retry.turn_id.expect("retry turn id");

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.turn_id == retry_turn_id
                && turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/lionclaw retry"
                && turn.prompt_user_text == "inspect retry attachment"
                && turn
                    .assistant_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains(&expected_runtime_path)
        },
        "attachment retry completion",
    )
    .await;

    let retry_again = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "attach-retry-1004",
            "peer-attach-retry",
            "peer-attach-retry",
            None,
            "/lionclaw retry",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue second attachment retry command");
    assert_eq!(retry_again.outcome, ChannelInboundOutcome::Queued);
    let retry_again_turn_id = retry_again.turn_id.expect("second retry turn id");

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.turn_id == retry_again_turn_id
                && turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/lionclaw retry"
                && turn.prompt_user_text == "inspect retry attachment"
                && turn
                    .assistant_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains(&expected_runtime_path)
        },
        "second attachment retry completion",
    )
    .await;

    let plain_same_prompt = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "attach-retry-1005",
            "peer-attach-retry",
            "peer-attach-retry",
            None,
            "inspect retry attachment",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue same prompt without attachments");
    assert_eq!(plain_same_prompt.outcome, ChannelInboundOutcome::Queued);
    let plain_same_prompt_turn_id = plain_same_prompt
        .turn_id
        .expect("plain same prompt turn id");

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.turn_id == plain_same_prompt_turn_id
                && turn.kind == SessionTurnKind::Normal
                && turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect retry attachment"
        },
        "same prompt without attachments completion",
    )
    .await;

    let plain_retry = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "attach-retry-1006",
            "peer-attach-retry",
            "peer-attach-retry",
            None,
            "/lionclaw retry",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue same prompt retry without attachments");
    assert_eq!(plain_retry.outcome, ChannelInboundOutcome::Queued);
    let plain_retry_turn_id = plain_retry.turn_id.expect("plain retry turn id");

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.turn_id == plain_retry_turn_id
                && turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect retry attachment"
        },
        "same prompt retry without attachments completion",
    )
    .await;

    let plain_retry_again = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "attach-retry-1007",
            "peer-attach-retry",
            "peer-attach-retry",
            None,
            "/lionclaw retry",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue second same prompt retry without attachments");
    assert_eq!(plain_retry_again.outcome, ChannelInboundOutcome::Queued);
    let plain_retry_again_turn_id = plain_retry_again
        .turn_id
        .expect("second plain retry turn id");

    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.turn_id == plain_retry_again_turn_id
                && turn.kind == SessionTurnKind::Retry
                && turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect retry attachment"
        },
        "second same prompt retry without attachments completion",
    )
    .await;

    let attachment_mounts = attachment_mount_sources(&kernel, session_id).await;
    assert_eq!(
        attachment_mounts.len(),
        3,
        "only the attachment-backed source turn and retries should mount staged attachments"
    );
    for source in attachment_mounts {
        wait_for_path_removed(&source, "attachment retry projection cleanup").await;
    }
}

#[tokio::test]
async fn channel_attachment_only_turn_runs_from_runtime_manifest() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-only-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-attach-only", "attach-only-1001").await;
    approve_pairing(&kernel, "terminal", "peer-attach-only").await;

    let staged_content = b"attachment-only content".to_vec();
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            text: None,
            attachments: vec![ChannelAttachmentDescriptor {
                attachment_id: "att-only".to_string(),
                kind: "document".to_string(),
                mime_type: Some("text/plain".to_string()),
                filename: Some("only.txt".to_string()),
                size_bytes: Some(staged_content.len() as i64),
                provider_file_ref: "provider-only".to_string(),
                caption: None,
            }],
            ..v2_text_request(
                "terminal",
                "attach-only-1002",
                "peer-attach-only",
                "peer-attach-only",
                None,
                "ignored fallback text",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment-only turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let staged = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "attach-only-1002".to_string(),
            attachment_id: "att-only".to_string(),
            kind: "document".to_string(),
            filename: Some("only.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(staged_content),
        })
        .await
        .expect("stage attachment-only file");
    assert_eq!(staged.status, ChannelAttachmentStatus::Staged);
    let expected_runtime_path = format!(
        "/attachments/{}/only.txt",
        test_runtime_attachment_component("att-only")
    );
    assert_eq!(
        staged.runtime_path.as_deref(),
        Some(expected_runtime_path.as_str())
    );

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "attach-only-1002".to_string(),
            worker_id: "attachment-only-worker-test".to_string(),
            missing: Vec::new(),
        })
        .await
        .expect("finalize attachment-only turn");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);

    let session_id = waiting.session_id.expect("waiting session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.display_user_text.is_empty()
                && turn.prompt_user_text.is_empty()
                && turn.assistant_text.contains("channel_attachments")
                && turn.assistant_text.contains(&expected_runtime_path)
        },
        "attachment-only runtime manifest",
    )
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn channel_attachment_projection_rejects_symlinked_parent() {
    use std::os::unix::fs::symlink;

    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-symlink-projection-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-symlink", "symlink-1001").await;
    approve_pairing(&kernel, "terminal", "peer-symlink").await;
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![ChannelAttachmentDescriptor {
                attachment_id: "att-doc".to_string(),
                kind: "document".to_string(),
                mime_type: Some("text/plain".to_string()),
                filename: Some("report.txt".to_string()),
                size_bytes: Some(11),
                provider_file_ref: "provider-doc".to_string(),
                caption: None,
            }],
            ..v2_text_request(
                "terminal",
                "symlink-1002",
                "peer-symlink",
                "peer-symlink",
                None,
                "inspect symlink attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "symlink-1002".to_string(),
            attachment_id: "att-doc".to_string(),
            kind: "document".to_string(),
            filename: Some("report.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"hello world".to_vec()),
        })
        .await
        .expect("stage attachment");

    let outside = env.temp_dir().join("outside-projections");
    std::fs::create_dir_all(&outside).expect("create outside projection target");
    let projection_parent = env
        .home()
        .runtime_dir()
        .join("channels")
        .join(test_storage_component("terminal"))
        .join("attachment-projections");
    symlink(&outside, &projection_parent).expect("symlink projection parent");

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "symlink-1002".to_string(),
            worker_id: "attachment-worker-symlink-test".to_string(),
            missing: Vec::new(),
        })
        .await
        .expect("finalize attachments");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);

    let session_id = waiting.session_id.expect("waiting session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Failed
                && turn
                    .error_text
                    .as_deref()
                    .is_some_and(|text| text.contains("attachment storage path"))
        },
        "symlinked projection parent failure",
    )
    .await;

    assert!(
        std::fs::read_dir(&outside)
            .expect("read outside projection target")
            .next()
            .is_none(),
        "projection creation must not follow symlinked parents"
    );
}

#[tokio::test]
async fn channel_attachment_temp_cleanup_ignores_staged_blobs() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-temp-cleanup-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-temp", "temp-1001").await;
    approve_pairing(&kernel, "terminal", "peer-temp").await;
    let content = b"committed staged blob".to_vec();
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![ChannelAttachmentDescriptor {
                attachment_id: "att-temp".to_string(),
                kind: "document".to_string(),
                mime_type: Some("text/plain".to_string()),
                filename: Some("report.txt".to_string()),
                size_bytes: Some(content.len() as i64),
                provider_file_ref: "provider-temp".to_string(),
                caption: None,
            }],
            ..v2_text_request(
                "terminal",
                "temp-1002",
                "peer-temp",
                "peer-temp",
                None,
                "temp cleanup attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "temp-1002".to_string(),
            attachment_id: "att-temp".to_string(),
            kind: "document".to_string(),
            filename: Some("report.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(content.clone()),
        })
        .await
        .expect("stage attachment");

    let pool = connect_test_pool(&env.home().db_path()).await;
    let storage_path: String = sqlx::query_scalar(
        "SELECT storage_path FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'temp-1002' AND attachment_id = 'att-temp'",
    )
    .fetch_one(&pool)
    .await
    .expect("query staged storage path");
    let staged_path = std::path::PathBuf::from(storage_path);
    assert_eq!(
        std::fs::read(&staged_path).expect("read staged attachment before cleanup"),
        content
    );

    let temp_path = staged_path
        .parent()
        .expect("attachment dir")
        .join(".upload-old.tmp");
    std::fs::write(&temp_path, b"stale partial upload").expect("write stale temp upload");
    std::fs::File::open(&temp_path)
        .expect("open stale temp upload")
        .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
        .expect("age stale temp upload");

    let _restarted = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    assert!(!temp_path.exists(), "stale temp upload should be removed");
    assert_eq!(
        std::fs::read(&staged_path).expect("read staged attachment after cleanup"),
        content
    );
}

#[tokio::test]
async fn attachment_maintenance_removes_stale_runtime_projections() {
    let env = TestHome::new().await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;
    let projection_dir = env
        .home()
        .runtime_dir()
        .join("channels")
        .join(test_storage_component("terminal"))
        .join("attachment-projections")
        .join(test_storage_component("projection-cleanup-1001"))
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&projection_dir).expect("create stale projection");
    std::fs::write(projection_dir.join("projected.txt"), b"projected copy")
        .expect("write projected copy");
    std::fs::File::open(&projection_dir)
        .expect("open stale projection dir")
        .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
        .expect("age stale projection dir");

    kernel
        .reconcile_stale_channel_attachments()
        .await
        .expect("run attachment maintenance");

    assert!(
        !projection_dir.exists(),
        "stale runtime projection should be removed"
    );
}

#[tokio::test]
async fn channel_attachment_storage_identity_does_not_collide_after_sanitizing() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-collision-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-collision", "collision-1001").await;
    approve_pairing(&kernel, "terminal", "peer-collision").await;
    let first_id = "a/b";
    let second_id = "a_2fb";
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![
                ChannelAttachmentDescriptor {
                    attachment_id: first_id.to_string(),
                    kind: "document".to_string(),
                    filename: Some("same.txt".to_string()),
                    size_bytes: Some(5),
                    provider_file_ref: "provider-first".to_string(),
                    mime_type: Some("text/plain".to_string()),
                    caption: None,
                },
                ChannelAttachmentDescriptor {
                    attachment_id: second_id.to_string(),
                    kind: "document".to_string(),
                    filename: Some("same.txt".to_string()),
                    size_bytes: Some(6),
                    provider_file_ref: "provider-second".to_string(),
                    mime_type: Some("text/plain".to_string()),
                    caption: None,
                },
            ],
            ..v2_text_request(
                "terminal",
                "collision-1002",
                "peer-collision",
                "peer-collision",
                None,
                "compare both attachments",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let first_runtime_path = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "collision-1002".to_string(),
            attachment_id: first_id.to_string(),
            kind: "document".to_string(),
            filename: Some("same.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"first".to_vec()),
        })
        .await
        .expect("stage first attachment")
        .runtime_path
        .expect("first runtime path");
    let second_runtime_path = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "collision-1002".to_string(),
            attachment_id: second_id.to_string(),
            kind: "document".to_string(),
            filename: Some("same.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"second".to_vec()),
        })
        .await
        .expect("stage second attachment")
        .runtime_path
        .expect("second runtime path");
    assert_ne!(first_runtime_path, second_runtime_path);

    let pool = connect_test_pool(&env.home().db_path()).await;
    let rows = sqlx::query(
        "SELECT attachment_id, storage_path FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'collision-1002' \
         ORDER BY attachment_id",
    )
    .fetch_all(&pool)
    .await
    .expect("query collision attachment paths");
    let first_path = rows
        .iter()
        .find(|row| row.get::<String, _>("attachment_id") == first_id)
        .map(|row| row.get::<String, _>("storage_path"))
        .expect("first storage path");
    let second_path = rows
        .iter()
        .find(|row| row.get::<String, _>("attachment_id") == second_id)
        .map(|row| row.get::<String, _>("storage_path"))
        .expect("second storage path");
    assert_ne!(first_path, second_path);
    assert_eq!(
        std::fs::read(first_path).expect("read first staged file"),
        b"first"
    );
    assert_eq!(
        std::fs::read(second_path).expect("read second staged file"),
        b"second"
    );
}

#[tokio::test]
async fn channel_attachment_stage_requires_waiting_declared_attachment() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-precondition-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    let pending = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![attachment_descriptor("pending-attachment")],
            ..v2_text_request(
                "terminal",
                "precondition-pending",
                "peer-unapproved",
                "peer-unapproved",
                None,
                "unapproved attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("unapproved attachment event is pending");
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);

    let err = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "precondition-pending".to_string(),
            attachment_id: "pending-attachment".to_string(),
            kind: "image".to_string(),
            filename: Some("image.png".to_string()),
            mime_type: Some("image/png".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"hello".to_vec()),
        })
        .await
        .expect_err("pending approval event cannot stage");
    assert!(matches!(err, KernelError::Conflict(message) if message.contains("not waiting")));

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-precondition",
        "precondition-1001",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-precondition").await;
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![attachment_descriptor("declared-attachment")],
            ..v2_text_request(
                "terminal",
                "precondition-waiting",
                "peer-precondition",
                "peer-precondition",
                None,
                "waiting attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("queue waiting attachment turn");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let err = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "precondition-waiting".to_string(),
            attachment_id: "not-declared".to_string(),
            kind: "image".to_string(),
            filename: Some("image.png".to_string()),
            mime_type: Some("image/png".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"hello".to_vec()),
        })
        .await
        .expect_err("undeclared attachment cannot stage");
    assert!(matches!(err, KernelError::BadRequest(message) if message.contains("not declared")));

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "precondition-1002",
            "peer-precondition",
            "peer-precondition",
            None,
            "no attachments here",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue regular turn");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);

    let err = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "precondition-1002".to_string(),
            attachment_id: "not-declared".to_string(),
            kind: "image".to_string(),
            filename: Some("image.png".to_string()),
            mime_type: Some("image/png".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::Bytes(b"hello".to_vec()),
        })
        .await
        .expect_err("non-waiting event cannot stage");
    assert!(matches!(err, KernelError::Conflict(message) if message.contains("not waiting")));
}

#[tokio::test]
async fn channel_attachment_descriptor_size_policy_records_rejections() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-size-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-size", "size-1001").await;
    approve_pairing(&kernel, "terminal", "peer-size").await;

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![ChannelAttachmentDescriptor {
                size_bytes: Some((MAX_CHANNEL_ATTACHMENT_BYTES + 1) as i64),
                ..attachment_descriptor("too-large")
            }],
            ..v2_text_request(
                "terminal",
                "size-1002",
                "peer-size",
                "peer-size",
                None,
                "oversized attachment",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("oversized descriptor with text should queue with rejection manifest");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let session_id = queued.session_id.expect("queued session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "oversized attachment"
                && !turn
                    .prompt_user_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains("too-large")
                && turn.assistant_text.contains("attachment_too_large")
        },
        "oversized descriptor rejection manifest",
    )
    .await;

    let pool = connect_test_pool(&env.home().db_path()).await;
    let (status, reason_code): (String, String) = sqlx::query_as(
        "SELECT status, rejection_code FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'size-1002' AND attachment_id = 'too-large'",
    )
    .fetch_one(&pool)
    .await
    .expect("query oversized descriptor rejection");
    assert_eq!(status, "rejected");
    assert_eq!(reason_code, "attachment_too_large");

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            attachments: vec![
                ChannelAttachmentDescriptor {
                    attachment_id: "large-a".to_string(),
                    size_bytes: Some(MAX_CHANNEL_ATTACHMENT_BYTES as i64),
                    ..attachment_descriptor("large-a")
                },
                ChannelAttachmentDescriptor {
                    attachment_id: "large-b".to_string(),
                    size_bytes: Some(MAX_CHANNEL_ATTACHMENT_BYTES as i64),
                    ..attachment_descriptor("large-b")
                },
                ChannelAttachmentDescriptor {
                    attachment_id: "large-c".to_string(),
                    size_bytes: Some(
                        (MAX_CHANNEL_EVENT_ATTACHMENT_BYTES - (MAX_CHANNEL_ATTACHMENT_BYTES * 2)
                            + 1) as i64,
                    ),
                    ..attachment_descriptor("large-c")
                },
            ],
            ..v2_text_request(
                "terminal",
                "size-1003",
                "peer-size",
                "peer-size",
                None,
                "oversized event",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("event-total overflow should leave stageable descriptors waiting");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );
    let (status, reason_code): (String, String) = sqlx::query_as(
        "SELECT status, rejection_code FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'size-1003' AND attachment_id = 'large-c'",
    )
    .fetch_one(&pool)
    .await
    .expect("query event-total descriptor rejection");
    assert_eq!(status, "rejected");
    assert_eq!(reason_code, "event_attachments_too_large");
}

#[tokio::test]
async fn channel_attachment_stage_policy_rejection_is_manifested() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-stage-policy-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-policy", "policy-1001").await;
    approve_pairing(&kernel, "terminal", "peer-policy").await;

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            text: Some("inspect upload".to_string()),
            attachments: vec![ChannelAttachmentDescriptor {
                size_bytes: Some((MAX_CHANNEL_ATTACHMENT_BYTES - 1) as i64),
                ..attachment_descriptor("too-large-upload")
            }],
            ..v2_text_request(
                "terminal",
                "policy-1002",
                "peer-policy",
                "peer-policy",
                None,
                "inspect upload",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let rejected = kernel
        .stage_channel_attachment(ChannelAttachmentStageInput {
            channel_id: "terminal".to_string(),
            event_id: "policy-1002".to_string(),
            attachment_id: "too-large-upload".to_string(),
            kind: "image".to_string(),
            filename: Some("image.png".to_string()),
            mime_type: Some("image/png".to_string()),
            caption: None,
            content: ChannelAttachmentStageContent::RejectedByPolicy {
                reason_code: "attachment_too_large".to_string(),
                size_bytes: (MAX_CHANNEL_ATTACHMENT_BYTES + 1) as i64,
                sha256: "0".repeat(64),
            },
        })
        .await
        .expect("oversized staged upload is recorded as rejected");
    assert_eq!(rejected.status, ChannelAttachmentStatus::Rejected);
    assert_eq!(
        rejected.reason_code.as_deref(),
        Some("attachment_too_large")
    );
    assert_eq!(rejected.runtime_path, None);

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "policy-1002".to_string(),
            worker_id: "attachment-worker-policy-test".to_string(),
            missing: vec![ChannelAttachmentMissingReport {
                attachment_id: "too-large-upload".to_string(),
                reason_code: "provider_missing".to_string(),
                reason_text: None,
            }],
        })
        .await
        .expect("finalize rejected attachment");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);

    let session_id = waiting.session_id.expect("waiting session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect upload"
                && !turn
                    .prompt_user_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains("attachment_too_large")
        },
        "policy rejected attachment manifest",
    )
    .await;

    let pool = connect_test_pool(&env.home().db_path()).await;
    let reason_code: String = sqlx::query_scalar(
        "SELECT rejection_code FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'policy-1002' AND attachment_id = 'too-large-upload'",
    )
    .fetch_one(&pool)
    .await
    .expect("query policy rejection code");
    assert_eq!(reason_code, "attachment_too_large");
}

#[tokio::test]
async fn channel_attachment_stage_http_oversized_upload_records_policy_rejection() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "attachment-stage-http-policy-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-http-policy", "http-policy-1001").await;
    approve_pairing(&kernel, "terminal", "peer-http-policy").await;

    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            text: Some("inspect oversized upload".to_string()),
            attachments: vec![ChannelAttachmentDescriptor {
                size_bytes: Some((MAX_CHANNEL_ATTACHMENT_BYTES - 1) as i64),
                ..attachment_descriptor("too-large-http-upload")
            }],
            ..v2_text_request(
                "terminal",
                "http-policy-1002",
                "peer-http-policy",
                "peer-http-policy",
                None,
                "inspect oversized upload",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("attachment turn waits");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api");
    let bind_addr = listener.local_addr().expect("test api addr").to_string();
    let app = build_router(
        std::sync::Arc::new(kernel.clone()),
        test_daemon_info(&env, bind_addr.clone()),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test api");
    });

    let oversized_size = MAX_CHANNEL_ATTACHMENT_BYTES + 1024 * 1024 + 1;
    let file_content = vec![b'x'; oversized_size];
    let mut hasher = Sha256::new();
    hasher.update(&file_content);
    let expected_sha = hex::encode(hasher.finalize());
    let boundary = "lionclaw-http-attachment-policy-test";
    let body = multipart_stage_body(
        boundary,
        &[
            ("channel_id", "terminal"),
            ("event_id", "http-policy-1002"),
            ("attachment_id", "too-large-http-upload"),
            ("kind", "image"),
            ("filename", "image.png"),
            ("mime_type", "image/png"),
        ],
        "image.png",
        "image/png",
        &file_content,
    );

    let response = reqwest::Client::new()
        .post(format!("http://{bind_addr}/v0/channels/attachments/stage"))
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .expect("stage oversized upload over http");
    let status = response.status();
    let text = response.text().await.expect("read stage response");
    assert!(status.is_success(), "{status}: {text}");
    let rejected: ChannelAttachmentStageResponse =
        serde_json::from_str(&text).expect("decode stage response");
    assert_eq!(rejected.status, ChannelAttachmentStatus::Rejected);
    assert_eq!(rejected.size_bytes, oversized_size as i64);
    assert_eq!(rejected.sha256, expected_sha);
    assert_eq!(
        rejected.reason_code.as_deref(),
        Some("attachment_too_large")
    );
    assert_eq!(rejected.runtime_path, None);

    let finalized = kernel
        .finalize_channel_attachments(ChannelAttachmentFinalizeRequest {
            channel_id: "terminal".to_string(),
            event_id: "http-policy-1002".to_string(),
            worker_id: "attachment-worker-http-policy-test".to_string(),
            missing: Vec::new(),
        })
        .await
        .expect("finalize http policy rejected attachment");
    assert_eq!(finalized.outcome, ChannelAttachmentFinalizeOutcome::Queued);

    let session_id = waiting.session_id.expect("waiting session id");
    wait_for_latest_turn(
        &kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "inspect oversized upload"
                && !turn
                    .prompt_user_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains("attachment_too_large")
                && !turn.assistant_text.contains("not_staged")
        },
        "http policy rejected attachment manifest",
    )
    .await;

    let pool = connect_test_pool(&env.home().db_path()).await;
    let reason_code: String = sqlx::query_scalar(
        "SELECT rejection_code FROM channel_attachments \
         WHERE channel_id = 'terminal' AND event_id = 'http-policy-1002' AND attachment_id = 'too-large-http-upload'",
    )
    .fetch_one(&pool)
    .await
    .expect("query http policy rejection code");
    assert_eq!(reason_code, "attachment_too_large");
    server.abort();
}

#[tokio::test]
async fn bootstrap_finalizes_stale_channel_attachment_batches() {
    let env = TestHome::new().await;
    let (_kernel, session_id, pool) = create_aged_waiting_attachment_batch(
        &env,
        "attachment-stale-bootstrap-skill",
        "peer-stale-bootstrap",
        "stale-bootstrap-1001",
        "stale-bootstrap-1002",
    )
    .await;

    let restarted = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;
    assert_stale_attachment_batch_finalized(
        &restarted,
        &pool,
        session_id,
        "stale-bootstrap-1002",
        "bootstrap stale attachment turn completion",
    )
    .await;
}

#[tokio::test]
async fn attachment_maintenance_finalizes_stale_batches_without_restart() {
    let env = TestHome::new().await;
    let (kernel, session_id, pool) = create_aged_waiting_attachment_batch(
        &env,
        "attachment-stale-maintenance-skill",
        "peer-stale-maintenance",
        "stale-maintenance-1001",
        "stale-maintenance-1002",
    )
    .await;

    kernel
        .reconcile_stale_channel_attachments()
        .await
        .expect("run attachment maintenance");
    assert_stale_attachment_batch_finalized(
        &kernel,
        &pool,
        session_id,
        "stale-maintenance-1002",
        "live stale attachment turn completion",
    )
    .await;
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

    create_pending_pairing(&kernel, "terminal", "peer-resume-head", "resume-head-7251").await;
    approve_pairing(&kernel, "terminal", "peer-resume-head").await;
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

    let answer_text = wait_for_answer_delta_after_sequence(
        &kernel,
        "terminal",
        "terminal-resume-head",
        resume_after_sequence,
    )
    .await;
    assert_eq!(answer_text, "later");
}

#[tokio::test]
async fn bootstrap_recovers_durable_pending_channel_turns() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "recover-pending-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", "peer-recover", "recover-pairing").await;
    approve_pairing(&kernel, "terminal", "peer-recover").await;
    let session_key = direct_session_key("terminal", "peer-recover");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key.clone(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Conservative),
        })
        .await
        .expect("open channel session");

    let turn_id = uuid::Uuid::new_v4();
    let inbound_event_id = "recover-pending-event";
    let now_ms = chrono::Utc::now().timestamp_millis();
    let pool = connect_test_pool(&env.home().db_path()).await;
    sqlx::query(
        "INSERT INTO channel_inbound_events \
         (event_id, channel_id, sender_ref, conversation_ref, thread_ref, message_ref, text, trigger, attachments_json, reply_to_ref, provider_metadata_json, received_at_ms, created_at_ms) \
         VALUES (?1, 'terminal', 'peer-recover', 'peer-recover', NULL, ?1, 'recover after restart', 'dm', '[]', NULL, '{}', ?2, ?2)",
    )
    .bind(inbound_event_id)
    .bind(now_ms)
    .execute(&pool)
    .await
    .expect("seed inbound event");
    sqlx::query(
        "INSERT INTO session_turns \
         (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms) \
         VALUES (?1, ?2, 1, 'normal', 'running', 'recover after restart', 'recover after restart', '', NULL, NULL, 'mock', ?3, NULL)",
    )
    .bind(turn_id.to_string())
    .bind(session.session_id.to_string())
    .bind(now_ms)
    .execute(&pool)
    .await
    .expect("seed running session turn");
    sqlx::query(
        "INSERT INTO channel_turns \
         (turn_id, channel_id, session_key, session_id, inbound_event_id, runtime_id, status, last_error, answer_checkpoint_sequence, queued_at_ms, started_at_ms, finished_at_ms) \
         VALUES (?1, 'terminal', ?2, ?3, ?4, 'mock', 'pending', NULL, NULL, ?5, NULL, NULL)",
    )
    .bind(turn_id.to_string())
    .bind(&session_key)
    .bind(session.session_id.to_string())
    .bind(inbound_event_id)
    .bind(now_ms)
    .execute(&pool)
    .await
    .expect("seed pending channel turn");

    let restarted = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;
    wait_for_latest_turn(
        &restarted,
        session.session_id,
        |turn| turn.turn_id == turn_id && turn.status == SessionTurnStatus::Completed,
        "bootstrap recovered pending channel turn",
    )
    .await;

    let (session_status, channel_status) =
        wait_for_joined_turn_statuses(&pool, turn_id, "completed", "completed").await;
    assert_eq!(session_status, "completed");
    assert_eq!(channel_status, "completed");
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

    create_pending_pairing(&kernel, "terminal", "peer-action", "action-7301").await;
    approve_pairing(&kernel, "terminal", "peer-action").await;
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
        .block_channel_pairing(ChannelPairingBlockRequest {
            channel_id: "terminal".to_string(),
            pairing_id: None,
            sender_ref: Some("peer-action".to_string()),
            conversation_ref: None,
            thread_ref: None,
            reason: Some("test_block".to_string()),
        })
        .await
        .expect("block direct grant");

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

    create_pending_pairing(&kernel, "terminal", "peer-failure", "failure-7301").await;
    approve_pairing(&kernel, "terminal", "peer-failure").await;
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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-runtime-control",
        "runtime-control-7451",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-runtime-control").await;
    let session_key = direct_session_key("terminal", "peer-runtime-control");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key,
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive runtime-control session");

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "runtime-control-7452",
            "peer-runtime-control",
            "peer-runtime-control",
            None,
            "/handled now",
            ChannelTrigger::Dm,
        ))
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
                && stream_has_completed_and_done(events, queued_turn_id)
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
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-lionclaw-action",
        "lionclaw-action-7501",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-lionclaw-action").await;
    let session_key = direct_session_key("terminal", "peer-lionclaw-action");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key,
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
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "lionclaw-action-7502",
            "peer-lionclaw-action",
            "peer-lionclaw-action",
            None,
            "/lionclaw retry",
            ChannelTrigger::Dm,
        ))
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
            stream_has_completed_and_done(events, queued_turn_id)
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
async fn channel_inbound_lionclaw_reset_completes_queued_turn() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "lionclaw-reset-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-lionclaw-reset",
        "lionclaw-reset-7521",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-lionclaw-reset").await;
    let session_key = direct_session_key("terminal", "peer-lionclaw-reset");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key,
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive reset session");

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "lionclaw-reset-7522",
            "peer-lionclaw-reset",
            "peer-lionclaw-reset",
            None,
            "/lionclaw reset",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue LionClaw reset");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.turn_id == queued_turn_id
                && turn.status == SessionTurnStatus::Completed
                && turn.display_user_text == "/lionclaw reset"
                && turn.prompt_user_text.is_empty()
                && turn.assistant_text.starts_with("opened a fresh session: ")
        },
        "completed channel LionClaw reset",
    )
    .await;

    let stream = wait_for_stream_events(&kernel, "terminal", "terminal-lionclaw-reset", |events| {
        stream_has_completed_and_done(events, queued_turn_id)
    })
    .await;
    assert_turn_completed_before_done(&stream.events, queued_turn_id, "channel LionClaw reset");
}

#[tokio::test]
async fn channel_inbound_unknown_lionclaw_command_does_not_become_runtime_prompt() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "lionclaw-unknown-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-lionclaw-unknown",
        "lionclaw-unknown-7531",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-lionclaw-unknown").await;
    let session_key = direct_session_key("terminal", "peer-lionclaw-unknown");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key,
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive unknown-command session");

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "lionclaw-unknown-7532",
            "peer-lionclaw-unknown",
            "peer-lionclaw-unknown",
            None,
            "/lionclaw frobnicate",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("queue unknown LionClaw command");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let queued_turn_id = queued.turn_id.expect("queued turn id");

    wait_for_latest_turn(
        &kernel,
        session.session_id,
        |turn| {
            turn.turn_id == queued_turn_id
                && turn.status == SessionTurnStatus::Failed
                && turn.display_user_text == "/lionclaw frobnicate"
                && turn.prompt_user_text.is_empty()
                && turn.error_text.as_deref()
                    == Some("bad request: unknown LionClaw command: frobnicate")
        },
        "failed unknown channel LionClaw command",
    )
    .await;
}

#[tokio::test]
async fn channel_inbound_bare_retry_stays_runtime_owned() {
    let env = TestHome::new().await;
    install_and_bind_channel(&env, "terminal", "bare-runtime-skill").await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-bare-runtime",
        "bare-runtime-7551",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-bare-runtime").await;
    let session_key = direct_session_key("terminal", "peer-bare-runtime");
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: session_key,
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive runtime-owned session");

    let queued = kernel
        .ingest_channel_inbound(v2_text_request(
            "terminal",
            "bare-runtime-7552",
            "peer-bare-runtime",
            "peer-bare-runtime",
            None,
            "/retry",
            ChannelTrigger::Dm,
        ))
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

async fn wait_for_answer_delta_after_sequence(
    kernel: &Kernel,
    channel_id: &str,
    consumer_id: &str,
    sequence: i64,
) -> String {
    for _ in 0..60 {
        let response = kernel
            .pull_channel_stream(ChannelStreamPullRequest {
                channel_id: channel_id.to_string(),
                consumer_id: consumer_id.to_string(),
                start_mode: None,
                start_after_sequence: Some(sequence),
                limit: Some(20),
                wait_ms: Some(250),
            })
            .await
            .expect("resume stream");
        let answer_text = response
            .events
            .iter()
            .filter(|event| {
                event.kind == StreamEventKindDto::MessageDelta
                    && event.lane == Some(StreamLaneDto::Answer)
            })
            .filter_map(|event| event.text.as_deref())
            .collect::<String>();
        if !answer_text.is_empty() {
            return answer_text;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for resumed answer delta");
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

fn test_daemon_info(env: &TestHome, bind_addr: String) -> DaemonInfoResponse {
    DaemonInfoResponse {
        daemon: "lionclawd".to_string(),
        status: "ok".to_string(),
        home_id: "test-home".to_string(),
        home_root: env.home().root().to_string_lossy().into_owned(),
        bind_addr,
        project_scope: "test-project".to_string(),
        daemon_fingerprint: "test-fingerprint".to_string(),
    }
}

fn multipart_stage_body(
    boundary: &str,
    fields: &[(&str, &str)],
    file_name: &str,
    mime_type: &str,
    file_content: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {mime_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(file_content);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
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

fn stream_has_completed_and_done(events: &[ChannelStreamEventView], turn_id: uuid::Uuid) -> bool {
    events.iter().any(|event| {
        event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::TurnCompleted
    }) && events
        .iter()
        .any(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done)
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

fn conversation_session_key(channel_id: &str, conversation_ref: &str, sender_ref: &str) -> String {
    format!(
        "channel:{channel_id}:conversation:{}:sender:{}",
        session_key_part(conversation_ref),
        session_key_part(sender_ref)
    )
}

fn thread_session_key(
    channel_id: &str,
    conversation_ref: &str,
    thread_ref: &str,
    sender_ref: &str,
) -> String {
    format!(
        "channel:{channel_id}:thread:{}:{}:sender:{}",
        session_key_part(conversation_ref),
        session_key_part(thread_ref),
        session_key_part(sender_ref)
    )
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

fn claim_request(
    channel_id: &str,
    token: &str,
    sender_ref: &str,
    conversation_ref: &str,
    thread_ref: Option<&str>,
) -> ChannelPairingClaimRequest {
    ChannelPairingClaimRequest {
        channel_id: channel_id.to_string(),
        token: token.to_string(),
        sender_ref: sender_ref.to_string(),
        conversation_ref: conversation_ref.to_string(),
        thread_ref: thread_ref.map(str::to_string),
        provider_metadata: serde_json::json!({}),
    }
}

fn assert_audit_details_exclude_raw_tokens(details: &serde_json::Value, raw_tokens: &[&str]) {
    assert!(
        details.get("provider_metadata").is_none(),
        "pairing claim audit must not persist worker provider metadata"
    );
    let details_raw = serde_json::to_string(details).expect("serialize audit details");
    for raw_token in raw_tokens {
        assert!(
            !details_raw.contains(raw_token),
            "pairing claim audit must not persist raw token {raw_token}"
        );
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

fn captioned_attachment_descriptor(
    attachment_id: &str,
    caption: &str,
) -> ChannelAttachmentDescriptor {
    ChannelAttachmentDescriptor {
        caption: Some(caption.to_string()),
        ..attachment_descriptor(attachment_id)
    }
}

fn test_storage_component(raw: &str) -> String {
    format!("sha256-{}", test_sha256_hex(raw.trim().as_bytes()))
}

fn test_runtime_attachment_component(raw: &str) -> String {
    let digest = test_sha256_hex(raw.trim().as_bytes());
    format!(
        "{}-{}",
        test_safe_path_label(raw, "attachment", 80),
        &digest[..16]
    )
}

fn test_safe_path_label(raw: &str, fallback: &str, max_len: usize) -> String {
    let raw = raw.trim();
    let mut out = String::with_capacity(raw.len().max(1));
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-') {
            out.push(byte as char);
        } else {
            out.push('_');
            out.push(char::from_digit((byte >> 4).into(), 16).expect("hex high nibble"));
            out.push(char::from_digit((byte & 0x0f).into(), 16).expect("hex low nibble"));
        }
    }
    if out.is_empty() {
        out.push_str(fallback);
    }
    if out == "." || out == ".." {
        out.insert(0, '_');
    }
    if out.len() > max_len {
        out.truncate(max_len);
    }
    out
}

fn test_sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

async fn connect_test_pool(db_path: &std::path::Path) -> sqlx::SqlitePool {
    let db_url = format!("sqlite://{}", db_path.display());
    sqlx::SqlitePool::connect(&db_url)
        .await
        .expect("connect test db")
}

async fn create_aged_waiting_attachment_batch(
    env: &TestHome,
    skill_name: &str,
    peer_id: &str,
    pairing_event_id: &str,
    inbound_event_id: &str,
) -> (Kernel, uuid::Uuid, sqlx::SqlitePool) {
    install_and_bind_channel(env, "terminal", skill_name).await;
    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;

    create_pending_pairing(&kernel, "terminal", peer_id, pairing_event_id).await;
    approve_pairing(&kernel, "terminal", peer_id).await;
    let waiting = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            text: None,
            attachments: vec![captioned_attachment_descriptor("stale-att", "stale image")],
            ..v2_text_request(
                "terminal",
                inbound_event_id,
                peer_id,
                peer_id,
                None,
                "stale image",
                ChannelTrigger::Dm,
            )
        })
        .await
        .expect("create waiting attachment turn");
    assert_eq!(
        waiting.outcome,
        ChannelInboundOutcome::WaitingForAttachments
    );

    let pool = connect_test_pool(&env.home().db_path()).await;
    sqlx::query(
        "UPDATE channel_attachment_batches \
         SET created_at_ms = 1, updated_at_ms = 1 \
         WHERE channel_id = ?1 AND event_id = ?2",
    )
    .bind("terminal")
    .bind(inbound_event_id)
    .execute(&pool)
    .await
    .expect("age attachment batch");

    let session_id = waiting.session_id.expect("waiting session id");
    (kernel, session_id, pool)
}

async fn assert_stale_attachment_batch_finalized(
    kernel: &Kernel,
    pool: &sqlx::SqlitePool,
    session_id: uuid::Uuid,
    event_id: &str,
    label: &str,
) {
    wait_for_latest_turn(
        kernel,
        session_id,
        |turn| {
            turn.status == SessionTurnStatus::Completed
                && turn.prompt_user_text == "stale image"
                && !turn
                    .prompt_user_text
                    .contains("lionclaw_channel_attachment_manifest")
                && turn.assistant_text.contains("not_staged")
        },
        label,
    )
    .await;

    let status: String = sqlx::query_scalar(
        "SELECT status FROM channel_attachment_batches \
         WHERE channel_id = ?1 AND event_id = ?2",
    )
    .bind("terminal")
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("query finalized batch");
    assert_eq!(status, "finalized");
}

async fn create_pending_pairing(kernel: &Kernel, channel_id: &str, peer_id: &str, event_id: &str) {
    let response = kernel
        .ingest_channel_inbound(v2_text_request(
            channel_id,
            event_id,
            peer_id,
            peer_id,
            None,
            "seed pairing",
            ChannelTrigger::Dm,
        ))
        .await
        .expect("create pending pairing");
    assert_eq!(response.outcome, ChannelInboundOutcome::PendingApproval);
}

async fn approve_pairing(kernel: &Kernel, channel_id: &str, peer_id: &str) {
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

async fn approve_pairing_id(kernel: &Kernel, channel_id: &str, pairing_id: uuid::Uuid) {
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
        .expect("approve pairing by id");
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

async fn wait_for_path_removed(path: &std::path::Path, label: &str) {
    for _ in 0..60 {
        if !path.exists() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {label}");
}

async fn wait_for_attachment_mount_source(
    kernel: &Kernel,
    session_id: uuid::Uuid,
) -> std::path::PathBuf {
    for _ in 0..60 {
        let plan_events = kernel
            .query_audit(
                Some(session_id),
                Some("runtime.plan.allow".to_string()),
                None,
                Some(10),
            )
            .await
            .expect("query runtime plan audit");
        if let Some(source) = plan_events
            .events
            .iter()
            .filter_map(|event| event.details["mounts"].as_array())
            .flatten()
            .find(|mount| {
                mount["target"].as_str() == Some("/attachments")
                    && mount["access"].as_str() == Some("read-only")
            })
            .and_then(|mount| mount["source"].as_str())
        {
            return std::path::PathBuf::from(source);
        }
        sleep(Duration::from_millis(25)).await;
    }

    panic!("timed out waiting for runtime attachment mount source");
}

async fn attachment_mount_sources(
    kernel: &Kernel,
    session_id: uuid::Uuid,
) -> Vec<std::path::PathBuf> {
    let plan_events = kernel
        .query_audit(
            Some(session_id),
            Some("runtime.plan.allow".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query runtime plan audit");

    plan_events
        .events
        .iter()
        .filter_map(|event| event.details["mounts"].as_array())
        .flatten()
        .filter(|mount| {
            mount["target"].as_str() == Some("/attachments")
                && mount["access"].as_str() == Some("read-only")
        })
        .filter_map(|mount| mount["source"].as_str())
        .map(std::path::PathBuf::from)
        .collect()
}

async fn wait_for_joined_turn_statuses(
    pool: &sqlx::SqlitePool,
    turn_id: uuid::Uuid,
    expected_session_status: &str,
    expected_channel_status: &str,
) -> (String, String) {
    for _ in 0..40 {
        let statuses = query_joined_turn_statuses(pool, turn_id).await;
        if statuses.0 == expected_session_status && statuses.1 == expected_channel_status {
            return statuses;
        }
        sleep(Duration::from_millis(25)).await;
    }

    query_joined_turn_statuses(pool, turn_id).await
}

async fn assert_channel_turn_status_stays(
    pool: &sqlx::SqlitePool,
    turn_id: uuid::Uuid,
    expected_status: &str,
    duration: Duration,
    label: &str,
) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let status: String =
            sqlx::query_scalar("SELECT status FROM channel_turns WHERE turn_id = ?1")
                .bind(turn_id.to_string())
                .fetch_one(pool)
                .await
                .expect("query channel turn status");
        assert_eq!(status, expected_status, "{label}");
        sleep(Duration::from_millis(25)).await;
    }
}

async fn query_joined_turn_statuses(
    pool: &sqlx::SqlitePool,
    turn_id: uuid::Uuid,
) -> (String, String) {
    sqlx::query_as(
        "SELECT session_turns.status, channel_turns.status \
         FROM session_turns \
         JOIN channel_turns ON channel_turns.turn_id = session_turns.turn_id \
         WHERE session_turns.turn_id = ?1",
    )
    .bind(turn_id.to_string())
    .fetch_one(pool)
    .await
    .expect("query joined turn statuses")
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

struct RecordingEchoAdapter {
    prompts: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeAdapter for RecordingEchoAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "recording-echo".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("recording-echo:{}", input.session_id),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        let answer = {
            let mut prompts = self.prompts.lock().await;
            prompts.push(input.prompt);
            format!("recorded answer {}", prompts.len())
        };
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: answer,
            })
            .expect("send echoed prompt");
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

struct PausedEchoAdapter {
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait]
impl RuntimeAdapter for PausedEchoAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "paused-echo".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("paused-echo:{}", input.session_id),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        event_tx: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult, anyhow::Error> {
        self.release.notified().await;
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: input.prompt,
            })
            .expect("send echoed prompt");
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

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-start-failure",
        "start-failure-7351",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-start-failure").await;
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

    create_pending_pairing(
        &kernel,
        "terminal",
        "peer-close-failure",
        "close-failure-7401",
    )
    .await;
    approve_pairing(&kernel, "terminal", "peer-close-failure").await;
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
