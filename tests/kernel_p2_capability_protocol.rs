mod common;

use lionclaw::contracts::{
    PolicyGrantRequest, SessionOpenRequest, SessionTurnRequest, StreamEventKindDto, TrustTier,
};

use common::{write_skill_source, TestHome};

#[tokio::test]
async fn runtime_capability_requests_are_kernel_gated() {
    let env = TestHome::new().await;
    std::fs::write(
        env.home().workspace_dir("main").join("README.md"),
        "capability protocol",
    )
    .expect("seed workspace read target");
    let skill_source = write_skill_source(
        env.temp_dir(),
        "capability-protocol",
        "Handles capability-gated runtime operations",
        false,
    );
    env.install_skill("capability-protocol", &skill_source)
        .await;
    let kernel = env.kernel().await;

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "peer-cap-protocol".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");
    let skill = kernel
        .list_skills()
        .await
        .expect("list skills")
        .skills
        .into_iter()
        .find(|skill| skill.alias == "capability-protocol")
        .expect("installed skill");

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

    assert!(denied_turn.stream_events.iter().any(|event| {
        event.kind == StreamEventKindDto::Status
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("capability:req-1:denied"))
    }));

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

    assert!(granted_turn.stream_events.iter().any(|event| {
        event.kind == StreamEventKindDto::Status
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("capability:req-1:granted"))
    }));

    let capability_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("capability.request".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query capability audit");

    assert!(capability_events.events.len() >= 2);
    let latest = &capability_events.events[0];
    let previous = &capability_events.events[1];
    assert_eq!(
        latest
            .details
            .get("allowed")
            .and_then(|value| value.as_bool()),
        Some(true)
    );
    assert_eq!(
        previous
            .details
            .get("allowed")
            .and_then(|value| value.as_bool()),
        Some(false)
    );
}
