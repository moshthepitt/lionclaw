use std::path::PathBuf;

use anyhow::{Context, Result};

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

        Ok(())
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
    }
}
