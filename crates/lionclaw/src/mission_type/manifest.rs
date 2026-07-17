use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{RoleName, TaskId};

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
    pub planning: ManifestPlanningDag,
    #[serde(default)]
    pub recovery: crate::model::RecoveryConfig,
    #[serde(default)]
    pub execution: crate::model::ExecutionPolicy,
    /// The optional engine-owned closing review. Required for the reviewed
    /// stop bar and resolved against the loaded role inventory.
    #[serde(default, rename = "terminal-review")]
    pub terminal_review: Option<ManifestTerminalReview>,
    #[serde(default)]
    pub inputs: Vec<ManifestInput>,
}

/// Source manifest shape. Role output is intentionally absent here: the role
/// file is the mission author's single source of truth. The loader resolves
/// and persists it into the model's `PlanningTask` contract.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestPlanningDag {
    #[serde(default)]
    pub tasks: Vec<ManifestPlanningTask>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestPlanningTask {
    pub id: TaskId,
    pub role: RoleName,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct ManifestInput {
    pub name: String,
    pub network: bool,
    pub key_files: Vec<PathBuf>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct MissionLockFile {
    pub version: u32,
    #[serde(default)]
    pub skills: BTreeMap<String, LockedSkill>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct LockedSkill {
    pub digest: String,
    pub source: LockedSkillSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
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
