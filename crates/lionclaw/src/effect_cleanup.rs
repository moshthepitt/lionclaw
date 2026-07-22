use async_trait::async_trait;

use crate::model::EffectResource;
use crate::ports::{EffectCleaner, EffectCleanupFailure, EffectCleanupRequest};
use crate::workspace;

pub struct LocalEffectCleaner {
    oci_engine: String,
}

impl LocalEffectCleaner {
    pub fn new(oci_engine: String) -> Self {
        Self { oci_engine }
    }
}

#[async_trait]
impl EffectCleaner for LocalEffectCleaner {
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let resource_name = request.effect_id.resource_name();
        let effect_dir =
            crate::resources::MissionDirs::new(&request.state_dir, &request.mission_id)
                .effect(&request.effect_id);
        let mut failures = Vec::new();

        if let Err(error) =
            lionclaw_confinement::remove_oci_container(&self.oci_engine, &resource_name).await
        {
            failures.push((EffectResource::Container, error.to_string()));
        }
        if let Err(error) =
            lionclaw_confinement::remove_oci_secret(&self.oci_engine, &resource_name).await
        {
            failures.push((EffectResource::RuntimeSecret, error.to_string()));
        }
        if let Err(error) = effect_dir.remove().await {
            failures.push((EffectResource::EffectDirectory, error.to_string()));
        }
        if request.discard_artifact {
            if let Err(error) = workspace::discard_worker_result(
                &request.workspace_dir,
                request.mission_id.as_str(),
                &request.effect_id,
            )
            .await
            {
                failures.push((EffectResource::WriterRef, error.to_string()));
            }
        }

        let Some((resource, detail)) = failures.first().cloned() else {
            return Ok(());
        };
        let detail = if failures.len() == 1 {
            detail
        } else {
            failures
                .into_iter()
                .map(|(resource, detail)| format!("{resource:?}: {detail}"))
                .collect::<Vec<_>>()
                .join("; ")
        };
        Err(EffectCleanupFailure { resource, detail })
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use super::*;
    use crate::model::{EffectId, MissionId};

    async fn git(repo: &Path, args: &[&str]) -> std::process::Output {
        tokio::process::Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cleanup_is_exact_idempotent_and_preserves_published_state() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        assert!(git(&repo, &["init", "-q"]).await.status.success());
        for (key, value) in [
            ("user.name", "test"),
            ("user.email", "test@local"),
            ("commit.gpgsign", "false"),
        ] {
            assert!(git(&repo, &["config", key, value]).await.status.success());
        }
        std::fs::write(repo.join("file"), "content\n").unwrap();
        assert!(git(&repo, &["add", "file"]).await.status.success());
        assert!(git(&repo, &["commit", "-q", "-m", "base"])
            .await
            .status
            .success());
        let head = String::from_utf8(git(&repo, &["rev-parse", "HEAD"]).await.stdout)
            .unwrap()
            .trim()
            .to_string();

        let mission_id = MissionId::parse("mabc123def456").unwrap();
        let effect_id = EffectId::for_parts(&["cleanup", "test"]);
        let effect_ref = format!("refs/mission/{mission_id}/{effect_id}");
        let published_ref = format!("refs/mission/{mission_id}/published");
        for reference in [&effect_ref, &published_ref] {
            assert!(git(&repo, &["update-ref", reference, &head])
                .await
                .status
                .success());
        }

        let state_dir = repo.join(".lionclaw");
        let mission_dirs = crate::resources::MissionDirs::new(&state_dir, &mission_id);
        let effect_dir = mission_dirs.effect(&effect_id).role().root().to_path_buf();
        std::fs::create_dir_all(&effect_dir).unwrap();
        let conversation_id = crate::model::ConversationId::for_role_instance(
            &mission_id,
            crate::model::TaskNamespace::Execution,
            &crate::model::TaskId::new("task-1").unwrap(),
            &crate::model::RoleName::new("worker").unwrap(),
            1,
        );
        let conversation = mission_dirs.conversation(&conversation_id);
        std::fs::create_dir_all(conversation.work()).unwrap();
        std::fs::write(conversation.work().join("uncommitted"), "keep").unwrap();
        let cache = state_dir.join("inputs/sha256/published/sentinel");
        std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
        std::fs::write(&cache, "keep").unwrap();

        let engine = dir.path().join("fake-oci");
        std::fs::write(
            &engine,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"${0%/*}/oci.log\"\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&engine).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&engine, permissions).unwrap();
        let cleaner = LocalEffectCleaner::new(engine.to_string_lossy().into_owned());
        let request = EffectCleanupRequest {
            mission_id: mission_id.clone(),
            effect_id: effect_id.clone(),
            workspace_dir: repo.clone(),
            state_dir: state_dir.clone(),
            discard_artifact: true,
        };

        cleaner.cleanup(request.clone()).await.unwrap();
        cleaner.cleanup(request).await.unwrap();
        assert!(!effect_dir.exists());
        assert_eq!(
            std::fs::read_to_string(conversation.work().join("uncommitted")).unwrap(),
            "keep"
        );
        assert!(cache.is_file());
        assert!(
            !git(&repo, &["show-ref", "--verify", "--quiet", &effect_ref])
                .await
                .status
                .success()
        );
        assert!(
            git(&repo, &["show-ref", "--verify", "--quiet", &published_ref])
                .await
                .status
                .success()
        );

        assert!(git(&repo, &["update-ref", &effect_ref, &head])
            .await
            .status
            .success());
        std::fs::create_dir_all(&effect_dir).unwrap();
        cleaner
            .cleanup(EffectCleanupRequest {
                mission_id,
                effect_id: effect_id.clone(),
                workspace_dir: repo.clone(),
                state_dir,
                discard_artifact: false,
            })
            .await
            .unwrap();
        assert!(
            git(&repo, &["show-ref", "--verify", "--quiet", &effect_ref])
                .await
                .status
                .success()
        );

        let log = std::fs::read_to_string(dir.path().join("oci.log")).unwrap();
        let name = effect_id.resource_name();
        assert_eq!(
            log.lines().collect::<Vec<_>>(),
            vec![
                format!("rm --force --ignore {name}"),
                format!("secret rm --ignore {name}"),
                format!("rm --force --ignore {name}"),
                format!("secret rm --ignore {name}"),
                format!("rm --force --ignore {name}"),
                format!("secret rm --ignore {name}"),
            ]
        );
    }

    #[tokio::test]
    async fn cleanup_removes_all_effect_owned_role_state_without_a_conversation() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&state_dir).unwrap();
        assert!(git(&repo, &["init", "-q"]).await.status.success());

        let mission_id = MissionId::parse("mabc123def456").unwrap();
        let effect_id = EffectId::for_parts(&["terminal-review", "cleanup"]);
        let mission_dirs = crate::resources::MissionDirs::new(&state_dir, &mission_id);
        let role_effect = mission_dirs.effect(&effect_id).role();
        role_effect.prepare().unwrap();
        role_effect.role_state().prepare().unwrap();
        std::fs::create_dir_all(role_effect.role_state().work()).unwrap();
        std::fs::write(
            role_effect.role_state().work().join("checkout"),
            "transient",
        )
        .unwrap();
        std::fs::write(
            role_effect.role_state().scratch().join("scratch"),
            "transient",
        )
        .unwrap();
        std::fs::write(
            role_effect.role_state().runtime().join("session"),
            "transient",
        )
        .unwrap();
        assert!(!mission_dirs.root().join("conversations").exists());

        let engine = dir.path().join("fake-oci");
        std::fs::write(&engine, "#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = std::fs::metadata(&engine).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&engine, permissions).unwrap();
        let cleaner = LocalEffectCleaner::new(engine.to_string_lossy().into_owned());
        cleaner
            .cleanup(EffectCleanupRequest {
                mission_id,
                effect_id,
                workspace_dir: repo,
                state_dir,
                discard_artifact: false,
            })
            .await
            .unwrap();

        assert!(!role_effect.root().exists());
        assert!(!mission_dirs.root().join("conversations").exists());
    }
}
