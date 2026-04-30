use std::path::PathBuf;

use async_trait::async_trait;
use lionclaw::{
    contracts::{
        ChannelBindRequest, ChannelInboundOutcome, ChannelInboundRequest,
        ChannelPeerApproveRequest, ChannelPeerBlockRequest, ChannelStreamAckRequest,
        ChannelStreamEventView, ChannelStreamPullRequest, ChannelStreamStartMode,
        SessionActionKind, SessionActionRequest, SessionHistoryPolicy, SessionHistoryRequest,
        SessionLatestQuery, SessionOpenRequest, SessionTurnRequest, SessionTurnStatus,
        SkillInstallRequest, StreamEventKindDto, StreamLaneDto, TrustTier,
    },
    kernel::{
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
            RuntimeEventSender, RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput,
            RuntimeTurnInput, RuntimeTurnResult,
        },
        InboundChannelText, Kernel, KernelError, KernelOptions,
    },
};
use tempfile::TempDir;
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn channel_bind_requires_installed_alias() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-skill".to_string(),
            alias: "channel-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: channel-skill
description: channel skill
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");
    kernel
        .remove_skill("channel-skill")
        .await
        .expect("remove skill alias");

    let err = kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "local-cli".to_string(),
            skill_alias: "channel-skill".to_string(),
            enabled: Some(true),
            config: None,
        })
        .await
        .expect_err("bind should fail for missing alias");

    assert!(
        matches!(err, KernelError::NotFound(message) if message.contains("skill not found")),
        "missing skill alias should be rejected for channel binding"
    );
}

#[tokio::test]
async fn channel_peer_must_be_approved_before_inbound_turn_executes() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let _session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "seed".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("seed session");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-inbound-skill".to_string(),
            alias: "channel-inbound-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-inbound-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: inbound-skill
description: inbound skill for channel flow
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "local-cli".to_string(),
            skill_alias: skill.alias.clone(),
            enabled: Some(true),
            config: Some(serde_json::json!({"runtime_id": "mock"})),
        })
        .await
        .expect("bind channel");

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
    let pending = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == "peer-local")
        .expect("pending peer should exist");
    assert_eq!(pending.status, "pending");
    let pairing_code = pending
        .pairing_code
        .clone()
        .expect("pending peer should expose pairing code");

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
    let queued_turn_id = queued
        .turn_id
        .expect("queued inbound response should include turn id");
    assert!(queued.session_id.is_some());

    // Duplicate update id should be ignored by dedupe index.
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

    let turn_events = wait_for_audit_event_count(&kernel, "channel.turn.completed", 1).await;
    assert_eq!(
        turn_events.events.len(),
        1,
        "only one completed channel turn should be recorded"
    );

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "local-cli".to_string(),
            consumer_id: "local-cli-test".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull queued stream");
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
    assert!(
        stream.events[completed_position]
            .text
            .as_deref()
            .is_some_and(|text| text.contains("[mock]")),
        "queued channel turn should publish a canonical completed answer snapshot"
    );

    let queued_events = kernel
        .query_audit(
            None,
            Some("channel.outbound.queued".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query queued outbound events");
    let recorded_events = kernel
        .query_audit(
            None,
            Some("channel.outbound.recorded".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query recorded outbound events");
    assert!(
        !queued_events.events.is_empty() && !recorded_events.events.is_empty(),
        "pairing prompt and assistant response should both produce outbound audit records"
    );
}

#[tokio::test]
async fn channel_stream_pull_and_ack_round_trip() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-outbox-skill".to_string(),
            alias: "channel-outbox-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-outbox-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: outbox-skill
description: channel outbox skill
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "telegram".to_string(),
            skill_alias: skill.alias,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind channel");

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
    let pairing_event = stream
        .events
        .iter()
        .find(|event| {
            event.kind == StreamEventKindDto::MessageDelta
                && event.lane == Some(StreamLaneDto::Answer)
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("Pairing required"))
        })
        .expect("pairing prompt should stream as answer delta");
    assert_eq!(
        pairing_event.session_id, None,
        "channel-scoped pairing prompts must not advertise a fake session"
    );
    assert_eq!(
        pairing_event.turn_id, None,
        "channel-scoped pairing prompts must not advertise a fake turn"
    );
    let through_sequence = stream
        .events
        .last()
        .expect("stream events should exist")
        .sequence;

    let ack = kernel
        .ack_channel_stream(ChannelStreamAckRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            through_sequence,
        })
        .await
        .expect("ack stream");
    assert!(ack.acknowledged, "first ack should succeed");

    let stream_after_ack = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "telegram".to_string(),
            consumer_id: "telegram-worker".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(10),
            wait_ms: Some(0),
        })
        .await
        .expect("pull stream after ack");
    assert!(
        stream_after_ack.events.is_empty(),
        "acked events must not appear in resumed channel stream"
    );
}

#[tokio::test]
async fn channel_stream_tail_starts_from_current_head() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-tail-skill".to_string(),
            alias: "channel-tail-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-tail-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: tail-skill
description: channel tail skill
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "terminal".to_string(),
            skill_alias: skill.alias,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind channel");

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
    assert!(
        initial.events.is_empty(),
        "tail mode should not replay stream history on first connect"
    );
}

#[tokio::test]
async fn channel_stream_rejects_tail_with_explicit_start_sequence() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "tail-sequence-skill", "mock").await;

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
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-wait-skill".to_string(),
            alias: "channel-wait-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-wait-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: wait-skill
description: channel wait skill
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "telegram".to_string(),
            skill_alias: skill.alias,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind channel");

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

    assert!(
        stream.events.iter().any(|event| {
            event.kind == StreamEventKindDto::MessageDelta
                && event.lane == Some(StreamLaneDto::Answer)
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("Pairing required"))
        }),
        "long-poll should return newly appended stream events"
    );
}

#[tokio::test]
async fn channel_backed_session_open_requires_approved_peer() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "open-skill", "mock").await;

    let unapproved_err = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-open".to_string(),
            trust_tier: TrustTier::Untrusted,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect_err("unapproved peer should be rejected");
    assert!(
        matches!(unapproved_err, KernelError::BadRequest(message) if message.contains("not approved"))
    );

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
    assert!(
        matches!(pending_err, KernelError::BadRequest(message) if message.contains("pending approval"))
    );

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
    assert_eq!(opened.history_policy, SessionHistoryPolicy::Interactive);
}

#[tokio::test]
async fn direct_channel_session_turn_streams_turn_completed_then_done() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "direct-turn-skill", "mock").await;
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
        .expect("run direct channel turn");

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-direct-turn".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(50),
            wait_ms: Some(0),
        })
        .await
        .expect("pull direct channel stream");
    assert_turn_completed_before_done(&stream.events, response.turn_id, "direct channel turn");
}

#[tokio::test]
async fn channel_inbound_uses_pinned_session_when_provided() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "pin-skill", "mock").await;
    create_pending_peer(&kernel, "terminal", "peer-pin", "mock", 7101, "pin-7101").await;
    approve_peer(&kernel, "terminal", "peer-pin").await;

    let session_a = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-pin".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open session a");
    let session_b = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-pin".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open session b");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-pin".to_string(),
            text: "stay on session a".to_string(),
            session_id: Some(session_a.session_id),
            update_id: Some(7102),
            external_message_id: Some("pin-7102".to_string()),
            runtime_id: Some("mock".to_string()),
        })
        .await
        .expect("pinned inbound");
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    assert_eq!(queued.session_id, Some(session_a.session_id));

    wait_for_audit_event_count(&kernel, "channel.turn.completed", 1).await;
    let history_a = kernel
        .session_history(SessionHistoryRequest {
            session_id: session_a.session_id,
            limit: Some(12),
        })
        .await
        .expect("history a");
    assert_eq!(history_a.turns.len(), 1);
    assert_eq!(history_a.turns[0].display_user_text, "stay on session a");

    let history_b = kernel
        .session_history(SessionHistoryRequest {
            session_id: session_b.session_id,
            limit: Some(12),
        })
        .await
        .expect("history b");
    assert!(history_b.turns.is_empty());

    let local_session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-pin".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open mismatched local session");
    let mismatched = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-pin".to_string(),
            text: "wrong session".to_string(),
            session_id: Some(local_session.session_id),
            update_id: Some(7103),
            external_message_id: Some("pin-7103".to_string()),
            runtime_id: Some("mock".to_string()),
        })
        .await
        .expect_err("mismatched session id should fail");
    assert!(
        matches!(mismatched, KernelError::BadRequest(message) if message.contains("does not belong"))
    );
}

#[tokio::test]
async fn latest_session_snapshot_returns_precise_resume_sequence() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "resume-skill", "two-chunk").await;
    kernel
        .register_runtime_adapter(
            "two-chunk",
            std::sync::Arc::new(TwoChunkAnswerAdapter {
                first_chunk: "first".to_string(),
                second_chunk: " second".to_string(),
                delay: Duration::from_millis(250),
            }),
        )
        .await;

    create_pending_peer(
        &kernel,
        "terminal",
        "peer-resume",
        "two-chunk",
        7201,
        "resume-7201",
    )
    .await;
    approve_peer(&kernel, "terminal", "peer-resume").await;
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-resume".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive channel session");

    let queued = kernel
        .ingest_channel_inbound(ChannelInboundRequest {
            channel_id: "terminal".to_string(),
            peer_id: "peer-resume".to_string(),
            text: "resume this".to_string(),
            session_id: Some(session.session_id),
            update_id: Some(7202),
            external_message_id: Some("resume-7202".to_string()),
            runtime_id: Some("two-chunk".to_string()),
        })
        .await
        .expect("queue running turn");
    assert_eq!(queued.session_id, Some(session.session_id));

    wait_for_assistant_text(&kernel, session.session_id, "first").await;
    let snapshot = wait_for_latest_snapshot_resume(&kernel, "terminal", "peer-resume").await;
    assert_eq!(
        snapshot.session.as_ref().map(|session| session.session_id),
        Some(session.session_id)
    );
    let resume_after_sequence = snapshot
        .resume_after_sequence
        .expect("running turn should expose resume cursor");

    let resumed = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "terminal-resume".to_string(),
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
    assert_eq!(answer_text, " second");
}

#[tokio::test]
async fn latest_session_snapshot_is_project_scoped() {
    let env = TestEnv::new();
    let project_a = env.temp_dir.path().join("project-a");
    let project_b = env.temp_dir.path().join("project-b");
    std::fs::create_dir_all(&project_a).expect("project a");
    std::fs::create_dir_all(&project_b).expect("project b");

    let kernel_a = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            project_workspace_root: Some(project_a),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel a");
    let session_a = kernel_a
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open project-a session");

    let kernel_b = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            project_workspace_root: Some(project_b),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel b");

    let snapshot = kernel_b
        .latest_session_snapshot(SessionLatestQuery {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("latest snapshot");
    assert!(
        snapshot.session.is_none(),
        "project-b kernel should not see project-a sessions"
    );

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
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "resume-head-skill", "slow-answer").await;
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

    let resume_after_sequence = wait_for_running_snapshot_without_answer(
        &kernel,
        "terminal",
        "peer-resume-head",
        session.session_id,
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
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "action-skill", "slow-answer").await;
    kernel
        .register_runtime_adapter(
            "slow-answer",
            std::sync::Arc::new(SlowAnswerAdapter {
                answer: "completed".to_string(),
                delay: Duration::from_millis(250),
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
    assert!(started_at.elapsed() < Duration::from_millis(150));
    assert_eq!(retry.session_id, session.session_id);
    assert!(retry.turn_id.is_some());

    wait_for_session_turn_count(&kernel, session.session_id, 2).await;
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

struct TestEnv {
    temp_dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create temp dir"),
        }
    }

    fn db_path(&self) -> PathBuf {
        self.temp_dir.path().join("lionclaw.db")
    }
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

async fn install_and_bind_channel(
    kernel: &Kernel,
    channel_id: &str,
    skill_name: &str,
    runtime_id: &str,
) {
    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: format!("local/{skill_name}"),
            alias: skill_name.to_string(),
            reference: Some("main".to_string()),
            hash: Some(format!("{skill_name}-hash")),
            skill_md: Some(format!(
                "---\nname: {skill_name}\ndescription: {skill_name} for channel tests\n---"
            )),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: channel_id.to_string(),
            skill_alias: skill.alias.clone(),
            enabled: Some(true),
            config: Some(serde_json::json!({"runtime_id": runtime_id})),
        })
        .await
        .expect("bind channel");
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
    assert!(
        completed_position < done_position,
        "{context} should publish turn_completed before done"
    );
    (completed_position, done_position)
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
    let peer = peers
        .peers
        .iter()
        .find(|value| value.peer_id == peer_id)
        .expect("peer should exist");
    let pairing_code = peer
        .pairing_code
        .clone()
        .expect("pending peer should expose pairing code");
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

async fn wait_for_assistant_text(kernel: &Kernel, session_id: uuid::Uuid, expected: &str) {
    for _ in 0..60 {
        let history = kernel
            .session_history(SessionHistoryRequest {
                session_id,
                limit: Some(12),
            })
            .await
            .expect("session history");
        if history
            .turns
            .last()
            .is_some_and(|turn| turn.assistant_text == expected)
        {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for assistant text '{expected}'");
}

async fn wait_for_latest_snapshot_resume(
    kernel: &Kernel,
    channel_id: &str,
    peer_id: &str,
) -> lionclaw::contracts::SessionLatestResponse {
    for _ in 0..60 {
        let snapshot = kernel
            .latest_session_snapshot(SessionLatestQuery {
                channel_id: channel_id.to_string(),
                peer_id: peer_id.to_string(),
                history_policy: Some(SessionHistoryPolicy::Interactive),
            })
            .await
            .expect("latest session snapshot");
        if snapshot.resume_after_sequence.is_some() {
            return snapshot;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for latest session resume sequence");
}

async fn wait_for_running_snapshot_without_answer(
    kernel: &Kernel,
    channel_id: &str,
    peer_id: &str,
    session_id: uuid::Uuid,
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
        let history = kernel
            .session_history(SessionHistoryRequest {
                session_id,
                limit: Some(12),
            })
            .await
            .expect("session history");
        let no_answer_yet = history.turns.last().is_some_and(|turn| {
            turn.status == lionclaw::contracts::SessionTurnStatus::Running
                && turn.assistant_text.is_empty()
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

async fn wait_for_session_turn_count(kernel: &Kernel, session_id: uuid::Uuid, expected: usize) {
    for _ in 0..60 {
        let history = kernel
            .session_history(SessionHistoryRequest {
                session_id,
                limit: Some(12),
            })
            .await
            .expect("session history");
        if history.turns.len() >= expected {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for session turn count {expected}");
}

struct TwoChunkAnswerAdapter {
    first_chunk: String,
    second_chunk: String,
    delay: Duration,
}

#[async_trait]
impl RuntimeAdapter for TwoChunkAnswerAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "two-chunk".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle, anyhow::Error> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("two-chunk:{}", input.session_id),
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
                lane: RuntimeMessageLane::Answer,
                text: self.first_chunk.clone(),
            })
            .expect("send first chunk");
        sleep(self.delay).await;
        event_tx
            .send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: self.second_chunk.clone(),
            })
            .expect("send second chunk");
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

#[tokio::test]
async fn channel_runtime_error_event_persists_failed_turn_and_supports_continue() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    install_and_bind_channel(&kernel, "terminal", "failure-skill", "partial-failure").await;
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
    let latest_turn = history.turns.last().expect("latest turn");
    assert_eq!(latest_turn.status, SessionTurnStatus::Failed);
    assert_eq!(latest_turn.assistant_text, "partial before fail");
    assert_eq!(latest_turn.error_code.as_deref(), Some("runtime.error"));
    assert_eq!(
        latest_turn.error_text.as_deref(),
        Some("adapter failed after partial output")
    );

    let continued = kernel
        .session_action(SessionActionRequest {
            session_id: session.session_id,
            action: SessionActionKind::ContinueLastPartial,
        })
        .await
        .expect("continue should be allowed after partial failure");
    assert_eq!(continued.session_id, session.session_id);
    assert!(continued.turn_id.is_some());
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
