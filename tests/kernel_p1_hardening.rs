use std::path::PathBuf;

use lionclaw::{
    contracts::{
        PolicyGrantRequest, SessionOpenRequest, SessionTurnRequest, SkillInstallRequest, TrustTier,
    },
    kernel::{Kernel, KernelError},
};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn restart_persists_session_skill_policy_and_audit() {
    let sandbox = temp_env();
    let db_path = sandbox.db_path();

    let (session_id, skill_id, grant_id) = {
        let kernel = Kernel::new(&db_path).await.expect("kernel init");

        let opened = open_main_session(&kernel, "peer-restart").await;
        let installed = install_skill(
            &kernel,
            "local/restart-skill",
            r#"---
name: restart-skill
description: Handles restart durability requests
---"#,
        )
        .await;

        kernel
            .enable_skill(installed.skill_id.clone())
            .await
            .expect("enable skill");

        let grant = kernel
            .grant_policy(PolicyGrantRequest {
                skill_id: installed.skill_id.clone(),
                capability: "skill.use".to_string(),
                scope: "*".to_string(),
                ttl_seconds: None,
            })
            .await
            .expect("grant policy");

        let turn = kernel
            .turn_session(SessionTurnRequest {
                session_id: opened.session_id,
                user_text: "please use restart skill for this task".to_string(),
                runtime_id: Some("mock".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("turn should succeed");
        assert!(
            turn.selected_skills.contains(&installed.skill_id),
            "policy-gated skill should be selected before restart"
        );

        let audit = kernel
            .query_audit(Some(opened.session_id), None, None, Some(50))
            .await
            .expect("query audit");
        assert!(
            audit
                .events
                .iter()
                .any(|event| event.event_type == "session.open"),
            "session.open should be persisted"
        );
        assert!(
            audit
                .events
                .iter()
                .any(|event| event.event_type == "session.turn"),
            "session.turn should be persisted"
        );

        (opened.session_id, installed.skill_id, grant.grant_id)
    };

    let kernel = Kernel::new(&db_path).await.expect("kernel restart init");

    let listed = kernel.list_skills().await.expect("list skills");
    let persisted_skill = listed
        .skills
        .iter()
        .find(|skill| skill.skill_id == skill_id)
        .expect("installed skill must persist");
    assert!(persisted_skill.enabled, "enabled state should persist");

    let turn_after_restart = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "restart skill should still be available".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after restart");
    assert!(
        turn_after_restart.selected_skills.contains(&skill_id),
        "session + policy should persist across restart"
    );

    let revoked = kernel
        .revoke_policy(grant_id)
        .await
        .expect("revoke persisted grant");
    assert!(
        revoked.revoked,
        "persisted grant should be revocable after restart"
    );
}

#[tokio::test]
async fn expiring_policy_grant_is_enforced() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");

    let opened = open_main_session(&kernel, "peer-ttl").await;
    let installed = install_skill(
        &kernel,
        "local/ttl-skill",
        r#"---
name: ttl-skill
description: Handles expiring policy windows
---"#,
    )
    .await;

    kernel
        .enable_skill(installed.skill_id.clone())
        .await
        .expect("enable skill");

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: installed.skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: Some(1),
        })
        .await
        .expect("grant ttl policy");

    let allowed_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "ttl skill now".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn during ttl");
    assert!(
        allowed_turn.selected_skills.contains(&installed.skill_id),
        "skill should be usable before ttl expiry"
    );

    sleep(Duration::from_millis(1500)).await;

    let denied_turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: opened.session_id,
            user_text: "ttl skill now".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn after ttl");
    assert!(
        denied_turn.selected_skills.is_empty(),
        "skill should be denied after ttl expiry"
    );
}

#[tokio::test]
async fn audit_query_respects_filters_limit_and_order() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");

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

    let recent_turns = kernel
        .query_audit(
            None,
            Some("session.turn".to_string()),
            Some(cutoff),
            Some(10),
        )
        .await
        .expect("query recent turns");
    assert_eq!(
        recent_turns.events.len(),
        2,
        "since filter should exclude first turn event"
    );
    assert!(
        recent_turns.events[0].timestamp >= recent_turns.events[1].timestamp,
        "events should be returned in descending timestamp order"
    );

    let limited = kernel
        .query_audit(
            Some(first_session.session_id),
            Some("session.turn".to_string()),
            None,
            Some(1),
        )
        .await
        .expect("query limited turns");
    assert_eq!(limited.events.len(), 1, "limit must cap returned rows");
    assert_eq!(
        limited.events[0].session_id,
        Some(first_session.session_id),
        "session filter should only include requested session"
    );
}

#[tokio::test]
async fn install_is_idempotent_and_revoke_is_safe_to_repeat() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");

    let install_a = install_skill(
        &kernel,
        "local/idempotent",
        r#"---
name: idempotent-skill
description: Handles idempotent operations
---"#,
    )
    .await;

    let install_b = install_skill(
        &kernel,
        "local/idempotent",
        r#"---
name: idempotent-skill
description: Handles idempotent operations
---"#,
    )
    .await;

    assert_eq!(
        install_a.skill_id, install_b.skill_id,
        "same provenance should map to same installed skill"
    );

    let listed = kernel.list_skills().await.expect("list skills");
    assert_eq!(
        listed.skills.len(),
        1,
        "idempotent install must avoid duplicates"
    );

    kernel
        .enable_skill(install_a.skill_id.clone())
        .await
        .expect("enable skill");

    let grant = kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: install_a.skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant policy");

    let first_revoke = kernel
        .revoke_policy(grant.grant_id)
        .await
        .expect("first revoke");
    assert!(first_revoke.revoked, "first revoke should remove grant");

    let second_revoke = kernel
        .revoke_policy(grant.grant_id)
        .await
        .expect("second revoke");
    assert!(
        !second_revoke.revoked,
        "second revoke should be safe and report no-op"
    );
}

#[tokio::test]
async fn enable_rejects_reusing_alias_for_different_skill() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");

    let first = kernel
        .install_skill(SkillInstallRequest {
            source: "local/first-skill".to_string(),
            alias: "shared-alias".to_string(),
            reference: Some("main".to_string()),
            hash: Some("first-hash".to_string()),
            skill_md: Some("---\nname: first-skill\ndescription: first skill\n---\n".to_string()),
            snapshot_path: None,
        })
        .await
        .expect("install first skill");
    kernel
        .enable_skill(first.skill_id)
        .await
        .expect("enable first skill");

    let second = kernel
        .install_skill(SkillInstallRequest {
            source: "local/second-skill".to_string(),
            alias: "shared-alias".to_string(),
            reference: Some("main".to_string()),
            hash: Some("second-hash".to_string()),
            skill_md: Some("---\nname: second-skill\ndescription: second skill\n---\n".to_string()),
            snapshot_path: None,
        })
        .await
        .expect("install second disabled skill revision");

    let err = kernel
        .enable_skill(second.skill_id)
        .await
        .expect_err("same alias must not identify two enabled skills");

    match err {
        KernelError::Conflict(message) => assert!(
            message.contains("skill alias 'shared-alias' is already enabled"),
            "unexpected conflict: {message}"
        ),
        other => panic!("expected alias conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn install_rejects_invalid_alias_as_bad_request() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");

    let err = kernel
        .install_skill(SkillInstallRequest {
            source: "local/invalid-alias".to_string(),
            alias: "not path safe".to_string(),
            reference: Some("main".to_string()),
            hash: Some("invalid-alias-hash".to_string()),
            skill_md: Some(
                "---\nname: invalid-alias\ndescription: invalid alias\n---\n".to_string(),
            ),
            snapshot_path: None,
        })
        .await
        .expect_err("invalid alias should be a caller error");

    match err {
        KernelError::BadRequest(message) => assert!(
            message.contains("may only contain ASCII"),
            "unexpected bad request: {message}"
        ),
        other => panic!("expected bad request, got {other:?}"),
    }
}

#[tokio::test]
async fn install_reuses_existing_skill_for_same_content_identity() {
    let sandbox = temp_env();
    let kernel = Kernel::new(&sandbox.db_path()).await.expect("kernel init");
    let skill_md = "---\nname: duplicate-content\ndescription: same content\n---\n";

    let first = kernel
        .install_skill(SkillInstallRequest {
            source: "local/duplicate-one".to_string(),
            alias: "duplicate-one".to_string(),
            reference: Some("main".to_string()),
            hash: Some("same-content-hash".to_string()),
            skill_md: Some(skill_md.to_string()),
            snapshot_path: None,
        })
        .await
        .expect("install first source");
    let second = kernel
        .install_skill(SkillInstallRequest {
            source: "local/duplicate-two".to_string(),
            alias: "duplicate-two".to_string(),
            reference: Some("main".to_string()),
            hash: Some("same-content-hash".to_string()),
            skill_md: Some(skill_md.to_string()),
            snapshot_path: None,
        })
        .await
        .expect("install same content from second source");

    assert_eq!(second.skill_id, first.skill_id);
    assert_eq!(second.alias, "duplicate-two");

    let listed = kernel.list_skills().await.expect("list skills");
    assert_eq!(listed.skills.len(), 1);
    assert_eq!(listed.skills[0].skill_id, first.skill_id);
    assert_eq!(listed.skills[0].alias, "duplicate-two");
}

struct TestEnv {
    temp_dir: TempDir,
}

impl TestEnv {
    fn db_path(&self) -> PathBuf {
        self.temp_dir.path().join("lionclaw.db")
    }
}

fn temp_env() -> TestEnv {
    TestEnv {
        temp_dir: tempfile::tempdir().expect("create temp dir"),
    }
}

async fn open_main_session(
    kernel: &Kernel,
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

async fn install_skill(
    kernel: &Kernel,
    source: &str,
    skill_md: &str,
) -> lionclaw::contracts::SkillInstallResponse {
    kernel
        .install_skill(SkillInstallRequest {
            source: source.to_string(),
            alias: source
                .split('/')
                .next_back()
                .unwrap_or("test-skill")
                .to_string(),
            reference: Some("main".to_string()),
            hash: Some("fixed-hash".to_string()),
            skill_md: Some(skill_md.to_string()),
            snapshot_path: None,
        })
        .await
        .expect("install skill")
}
