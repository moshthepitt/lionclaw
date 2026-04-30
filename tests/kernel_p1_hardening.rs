mod common;

use lionclaw::{
    applied::AppliedState,
    contracts::{PolicyGrantRequest, SessionOpenRequest, SessionTurnRequest, TrustTier},
    operator::reconcile::add_skill,
};
use tokio::time::{sleep, Duration};

use common::{write_skill_source, TestHome};

#[tokio::test]
async fn restart_persists_session_skill_policy_and_audit() {
    let env = TestHome::new().await;
    std::fs::write(
        env.home().workspace_dir("main").join("README.md"),
        "restart hardening workspace file",
    )
    .expect("write workspace readme");
    let skill_source = write_skill_source(
        env.temp_dir(),
        "restart-skill",
        "Handles restart durability requests",
        false,
    );
    env.install_skill("restart-skill", &skill_source).await;

    let (session_id, skill_id, grant_id) = {
        let kernel = env.kernel().await;
        let opened = open_main_session(&kernel, "peer-restart").await;
        let skill_id = env.installed_skill_id("restart-skill").await;

        let grant = kernel
            .grant_policy(PolicyGrantRequest {
                skill_alias: "restart-skill".to_string(),
                capability: "fs.read".to_string(),
                scope: "*".to_string(),
                ttl_seconds: None,
            })
            .await
            .expect("grant policy");

        let turn = kernel
            .turn_session(SessionTurnRequest {
                session_id: opened.session_id,
                user_text: "please use restart skill for this task [cap:fs.read]".to_string(),
                runtime_id: Some("mock".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("turn should succeed");
        assert!(turn.runtime_skills.contains(&skill_id));
        assert!(turn.stream_events.iter().any(|event| {
            event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("capability:req-1:granted"))
        }));

        let audit = kernel
            .query_audit(Some(opened.session_id), None, None, Some(50))
            .await
            .expect("query audit");
        assert!(audit
            .events
            .iter()
            .any(|event| event.event_type == "session.open"));
        assert!(audit
            .events
            .iter()
            .any(|event| event.event_type == "session.turn"));

        (opened.session_id, skill_id, grant.grant_id)
    };

    let kernel = env.kernel().await;
    let persisted_skill = env.installed_skill("restart-skill").await;
    assert_eq!(persisted_skill.skill_id, skill_id);
    assert_eq!(persisted_skill.alias, "restart-skill");

    let turn_after_restart = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "restart skill should still be available [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after restart");
    assert!(turn_after_restart.runtime_skills.contains(&skill_id));
    assert!(turn_after_restart.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:granted"))
    }));

    let revoked = kernel
        .revoke_policy(grant_id)
        .await
        .expect("revoke persisted grant");
    assert!(revoked.revoked);
}

#[tokio::test]
async fn expiring_policy_grant_is_enforced() {
    let env = TestHome::new().await;
    std::fs::write(
        env.home().workspace_dir("main").join("README.md"),
        "ttl hardening workspace file",
    )
    .expect("write workspace readme");
    let skill_source = write_skill_source(
        env.temp_dir(),
        "ttl-skill",
        "Handles expiring policy windows",
        false,
    );
    env.install_skill("ttl-skill", &skill_source).await;
    let kernel = env.kernel().await;

    let opened = open_main_session(&kernel, "peer-ttl").await;
    let skill_id = env.installed_skill_id("ttl-skill").await;

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_alias: "ttl-skill".to_string(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: Some(1),
        })
        .await
        .expect("grant ttl policy");

    let allowed_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "ttl skill now [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn during ttl");
    assert!(allowed_turn.runtime_skills.contains(&skill_id));
    assert!(allowed_turn.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:granted"))
    }));

    sleep(Duration::from_millis(1500)).await;

    let denied_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "ttl skill now [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after ttl");
    assert!(denied_turn.runtime_skills.contains(&skill_id));
    assert!(denied_turn.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:denied"))
    }));
}

#[tokio::test]
async fn audit_query_respects_filters_limit_and_order() {
    let env = TestHome::new().await;
    let kernel = env.kernel().await;

    let first_session = open_main_session(&kernel, "peer-audit-a").await;
    let second_session = open_main_session(&kernel, "peer-audit-b").await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: first_session.session_id,
            user_text: "first event".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn one");

    let first_turn = kernel
        .query_audit(
            Some(first_session.session_id),
            Some("session.turn".to_string()),
            None,
            Some(1),
        )
        .await
        .expect("query first turn");
    let cutoff = first_turn
        .events
        .first()
        .expect("expected first turn event")
        .timestamp
        + chrono::Duration::milliseconds(1);

    sleep(Duration::from_millis(5)).await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: first_session.session_id,
            user_text: "second event".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn two");

    sleep(Duration::from_millis(5)).await;

    kernel
        .turn_session(SessionTurnRequest {
            session_id: second_session.session_id,
            user_text: "third event".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn three");

    let first_only = kernel
        .query_audit(
            Some(first_session.session_id),
            Some("session.turn".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query first session turns");
    assert_eq!(first_only.events.len(), 2);

    let after_cutoff = kernel
        .query_audit(
            Some(first_session.session_id),
            Some("session.turn".to_string()),
            Some(cutoff),
            Some(10),
        )
        .await
        .expect("query cutoff turns");
    assert_eq!(after_cutoff.events.len(), 1);

    let limited = kernel
        .query_audit(None, Some("session.turn".to_string()), None, Some(2))
        .await
        .expect("query limited turns");
    assert_eq!(limited.events.len(), 2);
    assert!(limited.events[0].timestamp >= limited.events[1].timestamp);
}

#[tokio::test]
async fn skill_add_is_idempotent_and_policy_revoke_is_safe_to_repeat() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "repeat-skill", "repeat", false);
    env.install_skill("repeat-skill", &skill_source).await;
    env.install_skill("repeat-skill", &skill_source).await;
    let kernel = env.kernel().await;

    let skills = env.installed_skills().await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].alias, "repeat-skill");

    let grant = kernel
        .grant_policy(PolicyGrantRequest {
            skill_alias: "repeat-skill".to_string(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant policy");

    assert!(
        kernel
            .revoke_policy(grant.grant_id)
            .await
            .expect("revoke grant")
            .revoked
    );
    assert!(
        !kernel
            .revoke_policy(grant.grant_id)
            .await
            .expect("repeat revoke")
            .revoked
    );
}

#[tokio::test]
async fn skill_add_replaces_alias_revision() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "active-skill", "first revision", false);
    env.install_skill("active-alias", &skill_source).await;
    let first_skill_id = env.installed_skill_id("active-alias").await;

    std::fs::write(
        skill_source.join("SKILL.md"),
        "---\nname: active-skill\ndescription: second revision\n---\n",
    )
    .expect("rewrite skill");
    env.install_skill("active-alias", &skill_source).await;

    let updated = env.installed_skill("active-alias").await;
    assert_ne!(updated.skill_id, first_skill_id);
    assert_eq!(updated.description, "second revision");
}

#[tokio::test]
async fn skill_rm_hides_alias_until_reinstall() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "remove-skill", "remove", false);
    env.install_skill("active-alias", &skill_source).await;
    assert_eq!(
        env.installed_skill("active-alias").await.alias,
        "active-alias"
    );

    assert!(env.remove_skill("active-alias").await);
    assert!(lionclaw::applied::AppliedState::load(env.home())
        .await
        .expect("load applied state")
        .skill_by_alias("active-alias")
        .is_none());

    env.install_skill("active-alias", &skill_source).await;
    assert_eq!(
        env.installed_skill("active-alias").await.alias,
        "active-alias"
    );
}

#[tokio::test]
async fn skill_add_rejects_invalid_alias() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "bad-alias", "invalid", false);

    let err = add_skill(
        env.home(),
        "../bad".to_string(),
        skill_source.display().to_string(),
        "local".to_string(),
    )
    .await
    .expect_err("invalid alias should fail");
    assert!(err.to_string().contains("skill alias"));
}

#[tokio::test]
async fn duplicate_skill_ids_are_rejected_when_loading_applied_state() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "same-skill", "same", false);
    env.install_skill("alpha", &skill_source).await;
    env.install_skill("beta", &skill_source).await;

    let err = AppliedState::load(env.home())
        .await
        .expect_err("duplicate skill ids should fail");
    let message = err.to_string();
    assert!(message.contains("collides with another installed skill"));
}

async fn open_main_session(
    kernel: &lionclaw::kernel::Kernel,
    peer_id: &str,
) -> lionclaw::contracts::SessionOpenResponse {
    kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session")
}
