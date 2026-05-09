use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{home::LionClawHome, operator::snapshot::SKILL_INSTALL_METADATA_FILE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledSkill {
    pub alias: String,
    pub source: Option<String>,
    pub reference: Option<String>,
    pub status: SkillStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillStatus {
    Ready,
    Invalid(String),
}

impl SkillStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ready => "ready",
            Self::Invalid(reason) => reason,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SkillInstallMetadata {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    reference: Option<String>,
}

pub async fn list_installed_skills(home: &LionClawHome) -> Result<Vec<InstalledSkill>> {
    let skills_root = home.skills_dir();
    let root_metadata = match tokio::fs::symlink_metadata(&skills_root).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect {}", skills_root.display()));
        }
    };
    if root_metadata.file_type().is_symlink() {
        return Ok(vec![InstalledSkill {
            alias: skills_root
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("skills")
                .to_string(),
            source: None,
            reference: None,
            status: SkillStatus::Invalid(format!("{} is a symlink", skills_root.display())),
        }]);
    }
    if !root_metadata.is_dir() {
        return Ok(vec![InstalledSkill {
            alias: skills_root
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("skills")
                .to_string(),
            source: None,
            reference: None,
            status: SkillStatus::Invalid(format!("{} is not a directory", skills_root.display())),
        }]);
    }

    let mut entries = tokio::fs::read_dir(&skills_root)
        .await
        .with_context(|| format!("failed to read {}", skills_root.display()))?;
    let mut skills = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed to iterate {}", skills_root.display()))?
    {
        let Some(alias) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if alias.starts_with('.') {
            continue;
        }
        let path = entry.path();
        skills.push(read_installed_skill(alias, &path).await);
    }
    skills.sort_by(|left, right| left.alias.cmp(&right.alias));
    Ok(skills)
}

async fn read_installed_skill(alias: String, path: &Path) -> InstalledSkill {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) => {
            return InstalledSkill {
                alias,
                source: None,
                reference: None,
                status: SkillStatus::Invalid(format!("cannot inspect skill: {err}")),
            };
        }
    };
    if metadata.file_type().is_symlink() {
        return InstalledSkill {
            alias,
            source: None,
            reference: None,
            status: SkillStatus::Invalid("skill directory is a symlink".to_string()),
        };
    }
    if !metadata.is_dir() {
        return InstalledSkill {
            alias,
            source: None,
            reference: None,
            status: SkillStatus::Invalid("skill path is not a directory".to_string()),
        };
    }

    let metadata_path = path.join(SKILL_INSTALL_METADATA_FILE);
    let content = match tokio::fs::read_to_string(&metadata_path).await {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return InstalledSkill {
                alias,
                source: None,
                reference: None,
                status: SkillStatus::Invalid("install metadata is missing".to_string()),
            };
        }
        Err(err) => {
            return InstalledSkill {
                alias,
                source: None,
                reference: None,
                status: SkillStatus::Invalid(format!("install metadata cannot be read: {err}")),
            };
        }
    };
    match toml::from_str::<SkillInstallMetadata>(&content) {
        Ok(metadata) => InstalledSkill {
            alias,
            source: metadata.source.filter(|value| !value.trim().is_empty()),
            reference: metadata.reference.filter(|value| !value.trim().is_empty()),
            status: SkillStatus::Ready,
        },
        Err(err) => InstalledSkill {
            alias,
            source: None,
            reference: None,
            status: SkillStatus::Invalid(format!("install metadata cannot be parsed: {err}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_installed_skills_reports_metadata_and_invalid_entries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let terminal = home.skills_dir().join("terminal");
        let broken = home.skills_dir().join("broken");
        tokio::fs::create_dir_all(&terminal)
            .await
            .expect("terminal dir");
        tokio::fs::create_dir_all(&broken)
            .await
            .expect("broken dir");
        tokio::fs::write(
            terminal.join(SKILL_INSTALL_METADATA_FILE),
            "source = \"local:/skills/terminal\"\nreference = \"local\"\n",
        )
        .await
        .expect("metadata");

        let skills = list_installed_skills(&home).await.expect("skills");

        assert_eq!(skills.len(), 2);
        assert_eq!(skills[0].alias, "broken");
        assert!(matches!(skills[0].status, SkillStatus::Invalid(_)));
        assert_eq!(skills[1].alias, "terminal");
        assert_eq!(skills[1].source.as_deref(), Some("local:/skills/terminal"));
        assert_eq!(skills[1].reference.as_deref(), Some("local"));
        assert_eq!(skills[1].status, SkillStatus::Ready);
    }
}
