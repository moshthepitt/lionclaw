use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const DEFAULT_WORKSPACE: &str = "main";
pub const RUNTIME_PROJECTS_DIR: &str = "projects";
pub const RUNTIME_SESSIONS_DIR: &str = "sessions";
pub const RUNTIME_DRAFTS_DIR: &str = "drafts";
pub const RUNTIME_SESSION_READY_MARKER: &str = ".lionclaw-runtime-session";

#[derive(Debug, Clone)]
pub struct LionClawHome {
    root: PathBuf,
}

impl LionClawHome {
    pub fn from_env() -> Self {
        let root = std::env::var("LIONCLAW_HOME")
            .map(PathBuf::from)
            .ok()
            .filter(|value| !value.as_os_str().is_empty())
            .or_else(default_home_root)
            .unwrap_or_else(|| PathBuf::from(".lionclaw"));

        Self { root }
    }

    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn db_dir(&self) -> PathBuf {
        self.root.join("db")
    }

    pub fn db_path(&self) -> PathBuf {
        self.db_dir().join("lionclaw.db")
    }

    pub fn config_dir(&self) -> PathBuf {
        self.root.join("config")
    }

    pub fn config_path(&self) -> PathBuf {
        self.config_dir().join("lionclaw.toml")
    }

    pub fn runtime_secrets_env_path(&self) -> PathBuf {
        self.config_dir().join("runtime-secrets.env")
    }

    pub fn home_id_path(&self) -> PathBuf {
        self.config_dir().join("home-id")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.config_dir().join("lionclaw.lock")
    }

    pub fn skills_dir(&self) -> PathBuf {
        self.root.join("skills")
    }

    pub fn runtime_dir(&self) -> PathBuf {
        self.root.join("runtime")
    }

    pub fn runtime_channel_dir(&self, channel_id: &str) -> PathBuf {
        self.runtime_dir().join("channels").join(channel_id)
    }

    pub fn runtime_project_dir(
        &self,
        runtime_id: &str,
        workspace: &str,
        project_root: &Path,
    ) -> PathBuf {
        runtime_project_dir_from_parts(
            &self.runtime_dir(),
            runtime_id,
            workspace,
            Some(project_root),
        )
    }

    pub fn runtime_project_generated_agents_path(
        &self,
        runtime_id: &str,
        workspace: &str,
        project_root: &Path,
    ) -> PathBuf {
        runtime_project_generated_agents_path_from_parts(
            &self.runtime_dir(),
            runtime_id,
            workspace,
            Some(project_root),
        )
    }

    pub fn runtime_project_drafts_dir(
        &self,
        runtime_id: &str,
        workspace: &str,
        project_root: &Path,
    ) -> PathBuf {
        runtime_project_drafts_dir_from_parts(
            &self.runtime_dir(),
            runtime_id,
            workspace,
            Some(project_root),
        )
    }

    pub fn runtime_session_state_dir(
        &self,
        runtime_id: &str,
        workspace: &str,
        project_root: &Path,
        session_id: Uuid,
        compatibility_key: &str,
        shape_key: &str,
    ) -> PathBuf {
        runtime_session_state_dir_from_parts(
            &self.runtime_dir(),
            runtime_id,
            workspace,
            Some(project_root),
            session_id,
            compatibility_key,
            shape_key,
        )
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn services_dir(&self) -> PathBuf {
        self.root.join("services")
    }

    pub fn services_env_dir(&self) -> PathBuf {
        self.services_dir().join("env")
    }

    pub fn services_systemd_dir(&self) -> PathBuf {
        self.services_dir().join("systemd")
    }

    pub fn workspace_dir(&self, workspace: &str) -> PathBuf {
        self.root.join("workspaces").join(workspace)
    }

    pub async fn ensure_base_dirs(&self) -> Result<()> {
        for path in [
            self.root(),
            self.db_dir(),
            self.config_dir(),
            self.skills_dir(),
            self.runtime_dir(),
            self.logs_dir(),
            self.services_dir(),
            self.services_env_dir(),
            self.services_systemd_dir(),
            self.workspace_dir(DEFAULT_WORKSPACE),
        ] {
            tokio::fs::create_dir_all(&path)
                .await
                .with_context(|| format!("failed to create {}", path.display()))?;
        }
        #[cfg(unix)]
        {
            use std::{fs::Permissions, os::unix::fs::PermissionsExt};

            tokio::fs::set_permissions(self.config_dir(), Permissions::from_mode(0o700))
                .await
                .with_context(|| format!("failed to chmod {}", self.config_dir().display()))?;
        }

        self.ensure_home_id().await?;
        Ok(())
    }

    pub async fn read_home_id(&self) -> Result<Option<String>> {
        let path = self.home_id_path();
        if !tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?
        {
            return Ok(None);
        }

        let value = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(Some(value.trim().to_string()).filter(|value| !value.is_empty()))
    }

    pub async fn ensure_home_id(&self) -> Result<String> {
        if let Some(home_id) = self.read_home_id().await? {
            return Ok(home_id);
        }

        let home_id = Uuid::new_v4().to_string();
        let path = self.home_id_path();
        tokio::fs::write(&path, format!("{home_id}\n"))
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(home_id)
    }
}

pub fn runtime_project_partition_key(project_root: Option<&Path>) -> String {
    let digest_source = project_root
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "global".to_string());
    hashed_partition_key("project", digest_source.as_bytes())
}

pub fn runtime_profile_partition_key(source: &[u8]) -> String {
    hashed_partition_key("runtime", source)
}

pub fn runtime_project_dir_from_parts(
    runtime_root: &Path,
    runtime_id: &str,
    workspace: &str,
    project_root: Option<&Path>,
) -> PathBuf {
    runtime_root
        .join(runtime_id)
        .join(workspace)
        .join(RUNTIME_PROJECTS_DIR)
        .join(runtime_project_partition_key(project_root))
}

pub fn runtime_project_generated_agents_path_from_parts(
    runtime_root: &Path,
    runtime_id: &str,
    workspace: &str,
    project_root: Option<&Path>,
) -> PathBuf {
    runtime_project_dir_from_parts(runtime_root, runtime_id, workspace, project_root)
        .join("AGENTS.generated.md")
}

pub fn runtime_project_drafts_dir_from_parts(
    runtime_root: &Path,
    runtime_id: &str,
    workspace: &str,
    project_root: Option<&Path>,
) -> PathBuf {
    runtime_project_dir_from_parts(runtime_root, runtime_id, workspace, project_root)
        .join(RUNTIME_DRAFTS_DIR)
}

pub fn runtime_session_state_dir_from_parts(
    runtime_root: &Path,
    runtime_id: &str,
    workspace: &str,
    project_root: Option<&Path>,
    session_id: Uuid,
    compatibility_key: &str,
    shape_key: &str,
) -> PathBuf {
    runtime_project_dir_from_parts(runtime_root, runtime_id, workspace, project_root)
        .join(RUNTIME_SESSIONS_DIR)
        .join(session_id.to_string())
        .join(compatibility_key)
        .join(shape_key)
}

fn hashed_partition_key(prefix: &str, source: &[u8]) -> String {
    let digest = Sha256::digest(source);
    format!("{prefix}-{}", &hex::encode(digest)[..12])
}

fn default_home_root() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".lionclaw"))
}

#[cfg(test)]
mod tests {
    use super::{
        runtime_profile_partition_key, runtime_project_partition_key, LionClawHome,
        DEFAULT_WORKSPACE, RUNTIME_DRAFTS_DIR, RUNTIME_PROJECTS_DIR, RUNTIME_SESSIONS_DIR,
    };
    use uuid::Uuid;

    #[test]
    fn derives_canonical_paths() {
        let home = LionClawHome::new("/tmp/lionclaw-home".into());
        let project_root = std::path::Path::new("/tmp/project");
        let project_key = runtime_project_partition_key(Some(project_root));
        let compatibility_key = runtime_profile_partition_key(b"codex-v1");
        let session_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid");

        assert_eq!(
            home.db_path(),
            std::path::PathBuf::from("/tmp/lionclaw-home/db/lionclaw.db")
        );
        assert_eq!(
            home.config_path(),
            std::path::PathBuf::from("/tmp/lionclaw-home/config/lionclaw.toml")
        );
        assert_eq!(
            home.runtime_secrets_env_path(),
            std::path::PathBuf::from("/tmp/lionclaw-home/config/runtime-secrets.env")
        );
        assert_eq!(
            home.home_id_path(),
            std::path::PathBuf::from("/tmp/lionclaw-home/config/home-id")
        );
        assert_eq!(
            home.lock_path(),
            std::path::PathBuf::from("/tmp/lionclaw-home/config/lionclaw.lock")
        );
        assert_eq!(
            home.workspace_dir(DEFAULT_WORKSPACE),
            std::path::PathBuf::from("/tmp/lionclaw-home/workspaces/main")
        );
        assert_eq!(
            home.runtime_channel_dir("telegram"),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime/channels/telegram")
        );
        assert_eq!(
            home.runtime_project_dir("codex", DEFAULT_WORKSPACE, project_root),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime")
                .join("codex")
                .join("main")
                .join(RUNTIME_PROJECTS_DIR)
                .join(&project_key)
        );
        assert_eq!(
            home.runtime_project_drafts_dir("codex", DEFAULT_WORKSPACE, project_root),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime")
                .join("codex")
                .join("main")
                .join(RUNTIME_PROJECTS_DIR)
                .join(&project_key)
                .join(RUNTIME_DRAFTS_DIR)
        );
        assert_eq!(
            home.runtime_session_state_dir(
                "codex",
                DEFAULT_WORKSPACE,
                project_root,
                session_id,
                &compatibility_key,
                "workspace-read-write__network-on__secrets-off"
            ),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime")
                .join("codex")
                .join("main")
                .join(RUNTIME_PROJECTS_DIR)
                .join(project_key)
                .join(RUNTIME_SESSIONS_DIR)
                .join(session_id.to_string())
                .join(compatibility_key)
                .join("workspace-read-write__network-on__secrets-off")
        );
    }

    #[tokio::test]
    async fn ensure_home_id_creates_and_preserves_identity() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        home.ensure_base_dirs().await.expect("base dirs");
        let first = home.ensure_home_id().await.expect("home id");
        let second = home.ensure_home_id().await.expect("home id");

        assert_eq!(first, second);
        assert_eq!(
            home.read_home_id().await.expect("read home id"),
            Some(first)
        );
    }
}
