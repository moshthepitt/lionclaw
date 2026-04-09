use std::path::PathBuf;

use anyhow::{Context, Result};
use uuid::Uuid;

pub const DEFAULT_WORKSPACE: &str = "main";

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

    pub fn runtime_workspace_dir(&self, runtime_id: &str, workspace: &str) -> PathBuf {
        self.runtime_dir().join(runtime_id).join(workspace)
    }

    pub fn runtime_workspace_home_dir(&self, runtime_id: &str, workspace: &str) -> PathBuf {
        self.runtime_workspace_dir(runtime_id, workspace)
            .join("home")
    }

    pub fn runtime_workspace_drafts_dir(&self, runtime_id: &str, workspace: &str) -> PathBuf {
        self.runtime_workspace_dir(runtime_id, workspace)
            .join("drafts")
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

fn default_home_root() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".lionclaw"))
}

#[cfg(test)]
mod tests {
    use super::{LionClawHome, DEFAULT_WORKSPACE};

    #[test]
    fn derives_canonical_paths() {
        let home = LionClawHome::new("/tmp/lionclaw-home".into());

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
            home.runtime_workspace_home_dir("codex", DEFAULT_WORKSPACE),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime/codex/main/home")
        );
        assert_eq!(
            home.runtime_workspace_drafts_dir("codex", DEFAULT_WORKSPACE),
            std::path::PathBuf::from("/tmp/lionclaw-home/runtime/codex/main/drafts")
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
