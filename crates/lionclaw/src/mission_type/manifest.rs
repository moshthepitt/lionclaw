use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{AuthorityCeilings, ConfinementResources, RoleInstanceId};

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
    pub team: ManifestTeam,
    #[serde(default)]
    pub ceilings: AuthorityCeilings,
    #[serde(default, rename = "resource-ceilings")]
    pub resource_ceilings: ConfinementResources,
    #[serde(default, rename = "oracle-resources")]
    pub oracle_resources: BTreeMap<String, ConfinementResources>,
    #[serde(default, rename = "oracle-devices")]
    pub oracle_devices: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub recovery: crate::model::RecoveryConfig,
    #[serde(default)]
    pub execution: crate::model::ExecutionPolicy,
    #[serde(default)]
    pub inputs: Vec<ManifestInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct ManifestTeam {
    pub planning_assignment: RoleInstanceId,
    #[serde(default)]
    pub gap_review_assignment: Option<RoleInstanceId>,
    #[serde(default)]
    pub requires_gap_review: bool,
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
pub(crate) struct ManifestMissionType {
    pub name: String,
    pub stop: String,
    pub image: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
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
