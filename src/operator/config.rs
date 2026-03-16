use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::{LionClawHome, DEFAULT_WORKSPACE};
use crate::kernel::skills::sanitize_skill_name;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorConfig {
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub skills: Vec<ManagedSkillConfig>,
    #[serde(default)]
    pub channels: Vec<ManagedChannelConfig>,
}

impl OperatorConfig {
    pub async fn load(home: &LionClawHome) -> Result<Self> {
        let path = home.config_path();
        if !tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?
        {
            return Ok(Self::default());
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        config.normalize();
        Ok(config)
    }

    pub async fn save(&self, home: &LionClawHome) -> Result<()> {
        let path = home.config_path();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut normalized = self.clone();
        normalized.normalize();
        let content =
            toml::to_string_pretty(&normalized).context("failed to encode operator config")?;
        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn upsert_skill(&mut self, skill: ManagedSkillConfig) {
        self.skills.retain(|existing| existing.alias != skill.alias);
        self.skills.push(skill);
        self.normalize();
    }

    pub fn remove_skill(&mut self, alias: &str) -> bool {
        let before = self.skills.len();
        self.skills.retain(|skill| skill.alias != alias);
        self.normalize();
        before != self.skills.len()
    }

    pub fn upsert_channel(&mut self, channel: ManagedChannelConfig) {
        self.channels.retain(|existing| existing.id != channel.id);
        self.channels.push(channel);
        self.normalize();
    }

    pub fn remove_channel(&mut self, id: &str) -> bool {
        let before = self.channels.len();
        self.channels.retain(|channel| channel.id != id);
        self.normalize();
        before != self.channels.len()
    }

    pub fn workspace_root(&self, home: &LionClawHome) -> PathBuf {
        home.workspace_dir(&self.daemon.workspace)
    }

    fn normalize(&mut self) {
        self.skills
            .sort_by(|left, right| left.alias.cmp(&right.alias));
        self.channels.sort_by(|left, right| left.id.cmp(&right.id));
        for channel in &mut self.channels {
            channel.required_env.sort();
            channel.required_env.dedup();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_workspace")]
    pub workspace: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            workspace: default_workspace(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedSkillConfig {
    pub alias: String,
    pub source: String,
    #[serde(default = "default_reference")]
    pub reference: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedChannelConfig {
    pub id: String,
    pub skill: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub required_env: Vec<String>,
}

pub fn default_bind() -> String {
    "127.0.0.1:3000".to_string()
}

pub fn default_workspace() -> String {
    DEFAULT_WORKSPACE.to_string()
}

fn default_reference() -> String {
    "local".to_string()
}

fn default_enabled() -> bool {
    true
}

pub fn derive_skill_alias(source: &str) -> String {
    let raw = source
        .strip_prefix("local:")
        .unwrap_or(source)
        .trim_end_matches('/')
        .split('/')
        .next_back()
        .unwrap_or("skill");

    let alias = sanitize_skill_name(raw)
        .trim_start_matches("channel-")
        .to_string();
    if alias.is_empty() {
        "skill".to_string()
    } else {
        alias
    }
}

pub fn normalize_local_source(source: &str) -> Result<String> {
    let raw = source.strip_prefix("local:").unwrap_or(source);
    let absolute = std::fs::canonicalize(Path::new(raw))
        .with_context(|| format!("failed to resolve source '{}'", source))?;
    Ok(format!("local:{}", absolute.display()))
}

#[cfg(test)]
mod tests {
    use super::{derive_skill_alias, normalize_local_source, OperatorConfig};

    #[test]
    fn derives_channel_alias_from_source_path() {
        assert_eq!(derive_skill_alias("skills/channel-telegram"), "telegram");
        assert_eq!(
            derive_skill_alias("local:/tmp/custom-skill"),
            "custom-skill"
        );
    }

    #[tokio::test]
    async fn missing_config_loads_default() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = OperatorConfig::load(&home).await.expect("load");
        assert!(config.skills.is_empty());
        assert!(config.channels.is_empty());
    }

    #[test]
    fn normalizes_local_source_uri() {
        let absolute = normalize_local_source(".").expect("normalize");
        assert!(absolute.starts_with("local:/"));
    }
}
