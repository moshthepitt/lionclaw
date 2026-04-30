use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    home::LionClawHome,
    kernel::{
        db::ms_to_datetime,
        skills::{derive_skill_id, parse_skill_frontmatter, validate_skill_alias},
    },
    operator::{
        config::{ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        snapshot::{hash_directory, SKILL_INSTALL_METADATA_FILE},
    },
};

#[derive(Debug, Clone, Default)]
pub struct AppliedState {
    skills: Vec<AppliedSkill>,
    channels: Vec<AppliedChannel>,
    skills_by_id: BTreeMap<String, AppliedSkill>,
    skills_by_alias: BTreeMap<String, AppliedSkill>,
    channels_by_id: BTreeMap<String, AppliedChannel>,
}

impl AppliedState {
    pub async fn load(home: &LionClawHome) -> Result<Self> {
        let config = OperatorConfig::load(home).await?;
        Self::from_home(home, &config)
    }

    pub fn from_home(home: &LionClawHome, config: &OperatorConfig) -> Result<Self> {
        let skills_root = canonical_skills_root(home)?;
        let mut skills = Vec::new();
        let mut channels = Vec::with_capacity(config.channels.len());
        let mut skill_aliases = BTreeSet::new();
        let mut skill_ids = BTreeSet::new();
        let mut channel_ids = BTreeSet::new();
        let mut entries = fs::read_dir(&skills_root)
            .with_context(|| format!("failed to read directory {}", skills_root.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("failed to iterate directory {}", skills_root.display()))?;
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let alias = entry
                .file_name()
                .to_str()
                .ok_or_else(|| {
                    anyhow!(
                        "invalid installed skill name under {}",
                        skills_root.display()
                    )
                })?
                .to_string();
            if alias.starts_with('.') {
                continue;
            }
            validate_skill_alias(&alias)?;
            if !skill_aliases.insert(alias.clone()) {
                return Err(anyhow!(
                    "installed skill alias '{alias}' appears more than once"
                ));
            }

            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to stat {}", entry.path().display()))?;
            if metadata.file_type().is_symlink() {
                return Err(anyhow!(
                    "installed skill '{}' must not be a symlink",
                    entry.path().display()
                ));
            }
            if !metadata.is_dir() {
                return Err(anyhow!(
                    "installed skill '{}' is not a directory",
                    entry.path().display()
                ));
            }
            let skill = AppliedSkill::from_installed(&skills_root, entry.path())?;
            if !skill_ids.insert(skill.skill_id.clone()) {
                return Err(anyhow!(
                    "installed skill alias '{}' collides with another installed skill on skill id '{}'",
                    skill.alias,
                    skill.skill_id
                ));
            }
            skills.push(skill);
        }

        for channel in &config.channels {
            if !channel_ids.insert(channel.id.clone()) {
                return Err(anyhow!(
                    "operator config contains duplicate channel id '{}'",
                    channel.id
                ));
            }
            validate_skill_alias(&channel.skill)?;
            if !skill_aliases.contains(channel.skill.as_str()) {
                return Err(anyhow!(
                    "configured channel '{}' references missing installed skill alias '{}'",
                    channel.id,
                    channel.skill
                ));
            }
            channels.push(AppliedChannel::from_config(channel));
        }

        Ok(Self::from_parts(skills, channels))
    }

    pub fn skills(&self) -> &[AppliedSkill] {
        &self.skills
    }

    pub fn channels(&self) -> &[AppliedChannel] {
        &self.channels
    }

    pub fn skill_by_id(&self, skill_id: &str) -> Option<&AppliedSkill> {
        self.skills_by_id.get(skill_id)
    }

    pub fn skill_by_alias(&self, alias: &str) -> Option<&AppliedSkill> {
        self.skills_by_alias.get(alias)
    }

    pub fn channel(&self, channel_id: &str) -> Option<&AppliedChannel> {
        self.channels_by_id.get(channel_id)
    }

    pub fn runtime_visible_skills(&self) -> Vec<AppliedSkill> {
        let host_only_aliases = self
            .channels
            .iter()
            .map(|channel| channel.skill_alias.as_str())
            .collect::<BTreeSet<_>>();
        self.skills
            .iter()
            .filter(|skill| !host_only_aliases.contains(skill.alias.as_str()))
            .cloned()
            .collect()
    }

    pub fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"lionclaw-applied-state-v1\0");

        for skill in &self.skills {
            hasher.update(b"skill\0");
            hasher.update(skill.alias.as_bytes());
            hasher.update(b"\0");
            hasher.update(skill.skill_id.as_bytes());
            hasher.update(b"\0");
            hasher.update(skill.hash.as_bytes());
            hasher.update(b"\0");
        }

        for channel in &self.channels {
            hasher.update(b"channel\0");
            hasher.update(channel.id.as_bytes());
            hasher.update(b"\0");
            hasher.update(channel.skill_alias.as_bytes());
            hasher.update(b"\0");
            hasher.update(channel.launch_mode.as_str().as_bytes());
            hasher.update(b"\0");
        }

        hex::encode(hasher.finalize())
    }

    fn from_parts(skills: Vec<AppliedSkill>, channels: Vec<AppliedChannel>) -> Self {
        let skills_by_id = skills
            .iter()
            .cloned()
            .map(|skill| (skill.skill_id.clone(), skill))
            .collect::<BTreeMap<_, _>>();
        let skills_by_alias = skills
            .iter()
            .cloned()
            .map(|skill| (skill.alias.clone(), skill))
            .collect::<BTreeMap<_, _>>();
        let channels_by_id = channels
            .iter()
            .cloned()
            .map(|channel| (channel.id.clone(), channel))
            .collect::<BTreeMap<_, _>>();

        Self {
            skills,
            channels,
            skills_by_id,
            skills_by_alias,
            channels_by_id,
        }
    }
}

pub fn compute_daemon_fingerprint(
    runtime_config_fingerprint: &str,
    applied_state: &AppliedState,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"lionclaw-daemon-fingerprint-v1\0");
    hasher.update(runtime_config_fingerprint.as_bytes());
    hasher.update(b"\0");
    hasher.update(applied_state.fingerprint().as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Default, Deserialize)]
struct InstalledSkillMetadata {
    #[serde(default)]
    source: String,
    #[serde(default)]
    reference: String,
    #[serde(default)]
    installed_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AppliedSkill {
    pub skill_id: String,
    pub alias: String,
    pub name: String,
    pub description: String,
    pub source: String,
    pub reference: Option<String>,
    pub hash: String,
    pub snapshot_path: PathBuf,
    pub installed_at: DateTime<Utc>,
}

impl AppliedSkill {
    fn from_installed(skills_root: &Path, snapshot_path: PathBuf) -> Result<Self> {
        let alias = snapshot_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("invalid installed skill path '{}'", snapshot_path.display()))?
            .to_string();
        validate_skill_alias(&alias)?;

        let snapshot_path = fs::canonicalize(&snapshot_path)
            .with_context(|| format!("failed to resolve {}", snapshot_path.display()))?;
        if snapshot_path.parent() != Some(skills_root) {
            return Err(anyhow!(
                "installed skill '{}' must stay directly under '{}'",
                snapshot_path.display(),
                skills_root.display()
            ));
        }

        let skill_md_path = snapshot_path.join("SKILL.md");
        let skill_md_metadata = fs::symlink_metadata(&skill_md_path)
            .with_context(|| format!("failed to stat {}", skill_md_path.display()))?;
        if skill_md_metadata.file_type().is_symlink() || !skill_md_metadata.is_file() {
            return Err(anyhow!(
                "installed skill '{}' must contain a regular SKILL.md",
                snapshot_path.display()
            ));
        }

        let skill_md = fs::read_to_string(&skill_md_path)
            .with_context(|| format!("failed to read {}", skill_md_path.display()))?;
        let (name, description) = parse_skill_frontmatter(&skill_md);
        let hash = hash_directory(&snapshot_path)?;
        let metadata = read_installed_skill_metadata(&snapshot_path)?;
        let source = metadata
            .as_ref()
            .map(|value| value.source.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| snapshot_path.display().to_string());
        let reference = metadata
            .as_ref()
            .map(|value| value.reference.trim().to_string())
            .filter(|value| !value.is_empty());
        let installed_at = metadata
            .as_ref()
            .and_then(|value| value.installed_at_ms.and_then(ms_to_datetime))
            .or_else(|| {
                fs::metadata(&snapshot_path)
                    .ok()
                    .and_then(|value| value.modified().ok())
                    .map(DateTime::<Utc>::from)
            })
            .unwrap_or_else(Utc::now);

        Ok(Self {
            skill_id: derive_skill_id(&name, &hash),
            alias,
            name,
            description,
            source,
            reference,
            hash,
            snapshot_path,
            installed_at,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppliedChannel {
    pub id: String,
    pub skill_alias: String,
    pub launch_mode: ChannelLaunchMode,
    pub updated_at: DateTime<Utc>,
}

impl AppliedChannel {
    fn from_config(config: &ManagedChannelConfig) -> Self {
        Self {
            id: config.id.clone(),
            skill_alias: config.skill.clone(),
            launch_mode: config.launch_mode,
            updated_at: Utc::now(),
        }
    }
}

pub fn canonical_skills_root(home: &LionClawHome) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(home.skills_dir())
        .with_context(|| format!("failed to stat {}", home.skills_dir().display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "skills root '{}' must not be a symlink",
            home.skills_dir().display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "skills root '{}' is not a directory",
            home.skills_dir().display()
        ));
    }

    fs::canonicalize(home.skills_dir())
        .with_context(|| format!("failed to resolve {}", home.skills_dir().display()))
}

fn read_installed_skill_metadata(snapshot_root: &Path) -> Result<Option<InstalledSkillMetadata>> {
    let metadata_path = snapshot_root.join(SKILL_INSTALL_METADATA_FILE);
    let metadata = match fs::symlink_metadata(&metadata_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", metadata_path.display()));
        }
    };

    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "installed skill metadata '{}' must not be a symlink",
            metadata_path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(anyhow!(
            "installed skill metadata '{}' is not a regular file",
            metadata_path.display()
        ));
    }

    let content = fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let metadata = toml::from_str(&content)
        .with_context(|| format!("failed to parse {}", metadata_path.display()))?;
    Ok(Some(metadata))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::AppliedState;
    use crate::{home::LionClawHome, operator::reconcile::onboard};

    #[tokio::test]
    async fn load_ignores_hidden_staging_directories() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let visible = home.skills_dir().join("visible");
        fs::create_dir_all(&visible).expect("visible dir");
        fs::write(
            visible.join("SKILL.md"),
            "---\nname: visible\ndescription: visible\n---\n",
        )
        .expect("visible skill");

        let hidden = home.skills_dir().join(".visible.tmp-123");
        fs::create_dir_all(&hidden).expect("hidden dir");
        fs::write(
            hidden.join("SKILL.md"),
            "---\nname: hidden\ndescription: hidden\n---\n",
        )
        .expect("hidden skill");

        let applied = AppliedState::load(&home).await.expect("load state");
        let aliases = applied
            .skills()
            .iter()
            .map(|skill| skill.alias.as_str())
            .collect::<Vec<_>>();

        assert_eq!(aliases, vec!["visible"]);
    }
}
