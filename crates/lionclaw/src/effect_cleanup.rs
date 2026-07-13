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
        let attempt_dir = request
            .state_dir
            .join("missions")
            .join(request.mission_id.as_str())
            .join("attempts")
            .join(request.effect_id.as_str());
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
        if let Err(error) = workspace::remove_dir(&attempt_dir).await {
            failures.push((EffectResource::AttemptDirectory, error.to_string()));
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
        let attempt_dir = state_dir
            .join("missions")
            .join(mission_id.as_str())
            .join("attempts")
            .join(effect_id.as_str());
        std::fs::create_dir_all(&attempt_dir).unwrap();
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
        assert!(!attempt_dir.exists());
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
        std::fs::create_dir_all(&attempt_dir).unwrap();
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
}
