use std::path::PathBuf;

use lionclaw::{
    contracts::{
        PolicyGrantRequest, SessionOpenRequest, SessionTurnRequest, SkillInstallRequest,
        StreamEventKindDto, TrustTier,
    },
    kernel::Kernel,
};
use tempfile::TempDir;

#[tokio::test]
async fn runtime_capability_requests_are_kernel_gated() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-cap-protocol".to_string(),
            trust_tier: TrustTier::Main,
        })
        .await
        .expect("open session");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: "local/capability-protocol".to_string(),
            reference: Some("main".to_string()),
            hash: Some("capability-protocol-hash".to_string()),
            skill_md: Some(
                r#"---
name: capability-protocol
description: Handles capability-gated runtime operations
---"#
                    .to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .enable_skill(skill.skill_id.clone())
        .await
        .expect("enable skill");

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill.skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant skill.use");

    let denied_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "use capability-protocol [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn before fs.read grant");

    assert!(
        denied_turn.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:denied"))
        }),
        "capability request should be denied before capability grant"
    );

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill.skill_id.clone(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant fs.read");

    let granted_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "use capability-protocol [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after fs.read grant");

    assert!(
        granted_turn.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:granted"))
        }),
        "capability request should be granted after explicit fs.read policy grant"
    );

    let capability_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("capability.request".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query capability audit");

    assert!(
        capability_events.events.len() >= 2,
        "capability events should be audited for each request"
    );

    let latest = &capability_events.events[0];
    let previous = &capability_events.events[1];

    let latest_allowed = latest
        .details
        .get("allowed")
        .and_then(|value| value.as_bool())
        .expect("latest allowed bool");
    let previous_allowed = previous
        .details
        .get("allowed")
        .and_then(|value| value.as_bool())
        .expect("previous allowed bool");

    assert!(latest_allowed, "latest request should be allowed");
    assert!(!previous_allowed, "previous request should be denied");
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
