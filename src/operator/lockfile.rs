use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::LionClawHome;
use crate::operator::config::ChannelLaunchMode;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorLockfile {
    #[serde(default)]
    pub skills: Vec<LockedSkill>,
    #[serde(default)]
    pub channels: Vec<LockedChannel>,
}

impl OperatorLockfile {
    pub async fn load(home: &LionClawHome) -> Result<Self> {
        let path = home.lock_path();
        if !tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?
        {
            return Ok(Self::default());
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut lockfile: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        lockfile.normalize();
        Ok(lockfile)
    }

    pub async fn save(&self, home: &LionClawHome) -> Result<()> {
        let path = home.lock_path();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut normalized = self.clone();
        normalized.normalize();
        let content = toml::to_string_pretty(&normalized).context("failed to encode lockfile")?;
        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn find_skill(&self, alias: &str) -> Option<&LockedSkill> {
        self.skills.iter().find(|skill| skill.alias == alias)
    }

    pub fn find_channel(&self, id: &str) -> Option<&LockedChannel> {
        self.channels.iter().find(|channel| channel.id == id)
    }

    pub fn normalize(&mut self) {
        self.skills.sort_by_key(|skill| skill.alias.clone());
        self.channels.sort_by_key(|channel| channel.id.clone());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockedSkill {
    pub alias: String,
    pub source: String,
    pub reference: String,
    pub skill_id: String,
    pub hash: String,
    pub snapshot_dir: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockedChannel {
    pub id: String,
    pub skill: String,
    pub skill_id: String,
    pub enabled: bool,
    #[serde(default)]
    pub launch_mode: ChannelLaunchMode,
}
