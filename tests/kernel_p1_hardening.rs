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
        assert!(turn.runtime_skill_ids.contains(&skill_id));
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
    assert!(turn_after_restart.runtime_skill_ids.contains(&skill_id));
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
async fn running_kernel_keeps_skill_and_policy_until_restart() {
    let env = TestHome::new().await;
    std::fs::write(
        env.home().workspace_dir("main").join("README.md"),
        "running kernel snapshot workspace file",
    )
    .expect("write workspace readme");
    let skill_source =
        write_skill_source(env.temp_dir(), "snapshot-skill", "first revision", false);
    env.install_skill("snapshot-skill", &skill_source).await;

    let kernel = env.kernel().await;
    let opened = open_main_session(&kernel, "peer-running-snapshot").await;
    let first_skill_id = env.installed_skill_id("snapshot-skill").await;
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_alias: "snapshot-skill".to_string(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant policy");

    assert!(env.remove_skill("snapshot-skill").await);

    let restarted_kernel = env.kernel().await;

    let turn_before_restart = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "existing kernel should keep loaded skill [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn before restart");
    assert!(turn_before_restart
        .runtime_skill_ids
        .contains(&first_skill_id));
    assert!(turn_before_restart.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:granted"))
    }));

    let turn_after_restart = restarted_kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "restarted kernel should drop removed skill [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after restart");
    assert!(!turn_after_restart
        .runtime_skill_ids
        .contains(&first_skill_id));
    assert!(turn_after_restart.stream_events.iter().all(|event| {
        !event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:granted"))
    }));
}

#[tokio::test]
async fn running_kernel_keeps_loaded_revision_until_restart() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(
        env.temp_dir(),
        "revisioned-running-skill",
        "first revision",
        false,
    );
    env.install_skill("active-alias", &skill_source).await;

    let kernel = env.kernel().await;
    let first_skill_id = env.installed_skill_id("active-alias").await;

    std::fs::write(
        skill_source.join("SKILL.md"),
        "---\nname: revisioned-running-skill\ndescription: second revision\n---\n",
    )
    .expect("rewrite skill");
    env.install_skill("active-alias", &skill_source).await;
    let second_skill_id = env.installed_skill_id("active-alias").await;
    assert_ne!(first_skill_id, second_skill_id);

    let opened_before_restart = open_main_session(&kernel, "peer-running-revision").await;
    let turn_before_restart = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened_before_restart.session_id,
            user_text: "running kernel should keep first revision".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn before restart");
    assert!(turn_before_restart
        .runtime_skill_ids
        .contains(&first_skill_id));
    assert!(!turn_before_restart
        .runtime_skill_ids
        .contains(&second_skill_id));

    let restarted_kernel = env.kernel().await;
    let opened_after_restart =
        open_main_session(&restarted_kernel, "peer-running-revision-new").await;
    let turn_after_restart = restarted_kernel
        .turn_session(SessionTurnRequest {
            session_id: opened_after_restart.session_id,
            user_text: "restarted kernel should use second revision".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after restart");
    assert!(!turn_after_restart
        .runtime_skill_ids
        .contains(&first_skill_id));
    assert!(turn_after_restart
        .runtime_skill_ids
        .contains(&second_skill_id));
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
    assert!(allowed_turn.runtime_skill_ids.contains(&skill_id));
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
    assert!(denied_turn.runtime_skill_ids.contains(&skill_id));
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
async fn skill_rm_clears_policy_grants_for_reinstall() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "remove-grant", "remove", false);
    env.install_skill("active-alias", &skill_source).await;

    let kernel = env.kernel().await;
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_alias: "active-alias".to_string(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant policy");

    assert!(env.remove_skill("active-alias").await);
    env.install_skill("active-alias", &skill_source).await;

    let kernel = env.kernel().await;
    let opened = open_main_session(&kernel, "peer-remove-grant").await;
    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "removed skills should not keep old grants [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after reinstall");

    assert!(turn.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:denied"))
    }));
}

#[tokio::test]
async fn skill_alias_replacement_clears_old_policy_grants() {
    let env = TestHome::new().await;
    let skill_source =
        write_skill_source(env.temp_dir(), "revisioned-skill", "first revision", false);
    env.install_skill("active-alias", &skill_source).await;

    let kernel = env.kernel().await;
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_alias: "active-alias".to_string(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant policy");

    std::fs::write(
        skill_source.join("SKILL.md"),
        "---\nname: revisioned-skill\ndescription: second revision\n---\n",
    )
    .expect("rewrite skill to second revision");
    env.install_skill("active-alias", &skill_source).await;

    std::fs::write(
        skill_source.join("SKILL.md"),
        "---\nname: revisioned-skill\ndescription: first revision\n---\n",
    )
    .expect("rewrite skill back to first revision");
    env.install_skill("active-alias", &skill_source).await;

    let kernel = env.kernel().await;
    let opened = open_main_session(&kernel, "peer-replace-grant").await;
    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "replaced aliases should not resurrect old grants [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after alias replacement");

    assert!(turn.stream_events.iter().any(|event| {
        event
            .text
            .as_deref()
            .is_some_and(|text| text.contains("capability:req-1:denied"))
    }));
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
async fn identical_skill_content_can_back_multiple_aliases() {
    let env = TestHome::new().await;
    let skill_source = write_skill_source(env.temp_dir(), "same-skill", "same", false);
    env.install_skill("alpha", &skill_source).await;
    env.install_skill("beta", &skill_source).await;

    let applied = AppliedState::load(env.home())
        .await
        .expect("load applied state");
    let alpha = applied.skill_by_alias("alpha").expect("alpha skill");
    let beta = applied.skill_by_alias("beta").expect("beta skill");

    assert_ne!(alpha.skill_id, beta.skill_id);
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
