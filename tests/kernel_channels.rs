use std::path::PathBuf;

use lionclaw::{
    contracts::{
        ChannelBindRequest, ChannelOutboxAckRequest, ChannelOutboxPullRequest,
        ChannelPeerApproveRequest, PolicyGrantRequest, SessionOpenRequest, SkillInstallRequest,
        TrustTier,
    },
    kernel::{Kernel, KernelError},
};
use tempfile::TempDir;

#[tokio::test]
async fn channel_bind_requires_enabled_skill() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: channel-skill
description: channel skill
---"#
                    .to_string(),
            ),
        })
        .await
        .expect("install skill");

    let err = kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "local-cli".to_string(),
            skill_id: skill.skill_id,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect_err("bind should fail for disabled skill");

    assert!(
        matches!(err, KernelError::BadRequest(message) if message.contains("enabled skill")),
        "disabled skill should be rejected for channel binding"
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
        })
        .await
        .expect("seed session");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-inbound-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-inbound-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: inbound-skill
description: inbound skill for channel flow
---"#
                    .to_string(),
            ),
        })
        .await
        .expect("install skill");

    kernel
        .enable_skill(skill.skill_id.clone())
        .await
        .expect("enable skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "local-cli".to_string(),
            skill_id: skill.skill_id.clone(),
            enabled: Some(true),
            config: Some(serde_json::json!({"runtime_id": "mock"})),
        })
        .await
        .expect("bind channel");

    kernel
        .process_inbound_channel_text(
            "local-cli",
            "peer-local",
            "hello inbound-skill",
            Some("mock".to_string()),
            Some(1001),
            Some("msg-1001".to_string()),
        )
        .await
        .expect("pending inbound handled");

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

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill.skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant skill use");

    kernel
        .process_inbound_channel_text(
            "local-cli",
            "peer-local",
            "please run inbound-skill now",
            Some("mock".to_string()),
            Some(1002),
            Some("msg-1002".to_string()),
        )
        .await
        .expect("approved inbound turn should succeed");

    // Duplicate update id should be ignored by dedupe index.
    kernel
        .process_inbound_channel_text(
            "local-cli",
            "peer-local",
            "please run inbound-skill now",
            Some("mock".to_string()),
            Some(1002),
            Some("msg-1002".to_string()),
        )
        .await
        .expect("duplicate update should be ignored");

    let turn_events = kernel
        .query_audit(
            None,
            Some("channel.turn.succeeded".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query turn events");
    assert_eq!(
        turn_events.events.len(),
        1,
        "only one succeeded channel turn should be recorded"
    );

    let outbound_events = kernel
        .query_audit(
            None,
            Some("channel.outbound.queued".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query outbound events");
    assert!(
        outbound_events.events.len() >= 2,
        "pairing prompt and assistant response should both send outbound messages"
    );
}

#[tokio::test]
async fn channel_outbox_pull_and_ack_round_trip() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/channel-outbox-skill".to_string(),
            reference: Some("main".to_string()),
            hash: Some("channel-outbox-skill-hash".to_string()),
            skill_md: Some(
                r#"---
name: outbox-skill
description: channel outbox skill
---"#
                    .to_string(),
            ),
        })
        .await
        .expect("install skill");

    kernel
        .enable_skill(skill.skill_id.clone())
        .await
        .expect("enable skill");

    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "telegram".to_string(),
            skill_id: skill.skill_id,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind channel");

    let accepted = kernel
        .process_inbound_channel_text(
            "telegram",
            "peer-tele",
            "hello kernel",
            Some("mock".to_string()),
            Some(4001),
            Some("update-4001".to_string()),
        )
        .await
        .expect("process inbound");
    assert!(accepted, "new inbound message should be accepted");

    let outbox = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "telegram".to_string(),
            limit: Some(10),
        })
        .await
        .expect("pull outbox");
    assert_eq!(outbox.messages.len(), 1, "pairing prompt should be queued");

    let ack = kernel
        .ack_channel_outbox(ChannelOutboxAckRequest {
            message_id: outbox.messages[0].message_id,
            external_message_id: "telegram-msg-1".to_string(),
        })
        .await
        .expect("ack outbound");
    assert!(ack.acknowledged, "first ack should succeed");

    let outbox_after_ack = kernel
        .pull_channel_outbox(ChannelOutboxPullRequest {
            channel_id: "telegram".to_string(),
            limit: Some(10),
        })
        .await
        .expect("pull outbox after ack");
    assert!(
        outbox_after_ack.messages.is_empty(),
        "acked message must not appear in pending outbox"
    );
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
