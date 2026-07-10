use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const MISSION_LOCK_FILE: &str = "mission.lock.toml";

pub(crate) fn is_path_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestFile {
    #[serde(rename = "mission-type")]
    pub mission_type: ManifestMissionType,
    #[serde(default)]
    pub planning: crate::model::PlanningDag,
    /// The optional engine-owned closing review. Required for the reviewed
    /// stop bar and resolved against the loaded role inventory.
    #[serde(default, rename = "terminal-review")]
    pub terminal_review: Option<ManifestTerminalReview>,
    #[serde(default)]
    pub skills: BTreeMap<String, ManifestSkill>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestTerminalReview {
    pub role: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestMissionType {
    pub name: String,
    pub stop: String,
    pub image: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestSkill {
    pub source: ManifestSkillSource,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ManifestSkillSource {
    Path(PathSkillSource),
    Git(GitSkillSource),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PathSkillSource {
    pub path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GitSkillSource {
    pub git: String,
    pub rev: String,
    #[serde(default)]
    pub subdir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct MissionLockFile {
    pub version: u32,
    pub skills: BTreeMap<String, LockedSkill>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct LockedSkill {
    pub path: PathBuf,
    pub source: LockedSkillSource,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(crate) enum LockedSkillSource {
    Path {
        path: PathBuf,
    },
    Git {
        git: String,
        rev: String,
        #[serde(default)]
        subdir: PathBuf,
    },
}
