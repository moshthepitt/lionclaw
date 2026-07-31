//! Mission-owned team snapshots and authority requests.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ids::lowercase_hex, AssertionId, InputName, NetworkGrant, OutputSemantics, RoleInstanceId,
    TaskId,
};
use crate::prelude::*;

pub const MAX_GUIDANCE_BYTES: usize = 64 * 1024;
pub const MAX_TMPFS_RESOURCE_OVERRIDES: usize = 16;
pub const KERNEL_ENVIRONMENT_KEYS: &[&str] = &[
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "TMPDIR",
    "GIT_OPTIONAL_LOCKS",
    "LIONCLAW_WORKSPACE_DIR",
    "LIONCLAW_OUTPUT",
    "MISSION_EFFECT",
];

pub fn validate_environment_entry(name: &str, value: &str) -> Result<(), String> {
    let mut characters = name.chars();
    let valid_name = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if !valid_name {
        return Err(format!("key '{name}' is invalid"));
    }
    if KERNEL_ENVIRONMENT_KEYS.contains(&name) {
        return Err(format!("key '{name}' is owned by the LionClaw kernel"));
    }
    if value.contains('\0') {
        return Err(format!("value for '{name}' contains NUL"));
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityGrants {
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub network: NetworkGrant,
    #[serde(default)]
    pub install: bool,
    #[serde(default)]
    pub writes: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub devices: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub inputs: BTreeSet<InputName>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCeilings {
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub network: NetworkGrant,
    #[serde(default)]
    pub install: bool,
    #[serde(default)]
    pub writes: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub devices: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub inputs: BTreeSet<InputName>,
}

impl AuthorityGrants {
    pub fn within(&self, ceilings: &AuthorityCeilings) -> bool {
        (!self.secrets || ceilings.secrets)
            && self.network.within(&ceilings.network)
            && (!self.install || ceilings.install)
            && (!self.writes || ceilings.writes)
            && self.devices.is_subset(&ceilings.devices)
            && self.inputs.is_subset(&ceilings.inputs)
    }
}

impl AuthorityCeilings {
    /// Whether every child ceiling is no broader than this parent ceiling.
    pub fn contains(&self, child: &Self) -> bool {
        (!child.secrets || self.secrets)
            && child.network.within(&self.network)
            && (!child.install || self.install)
            && (!child.writes || self.writes)
            && child.devices.is_subset(&self.devices)
            && child.inputs.is_subset(&self.inputs)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ConfinementResources {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tmpfs: Vec<String>,
}

impl ConfinementResources {
    pub const fn is_empty(&self) -> bool {
        self.tmpfs.is_empty()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.tmpfs.len() > MAX_TMPFS_RESOURCE_OVERRIDES {
            return Err(format!(
                "declares {} tmpfs resources; limit is {MAX_TMPFS_RESOURCE_OVERRIDES}",
                self.tmpfs.len()
            ));
        }
        let mut targets = BTreeSet::new();
        for entry in &self.tmpfs {
            let parsed = ConfinementTmpfsResource::parse(entry)?;
            if !targets.insert(parsed.target().to_string()) {
                return Err(format!(
                    "declares tmpfs target '{}' more than once",
                    parsed.target()
                ));
            }
        }
        Ok(())
    }

    pub fn within(&self, ceilings: &Self) -> Result<(), String> {
        self.validate()?;
        ceilings.validate()?;
        let ceiling_by_target = ceilings
            .tmpfs
            .iter()
            .map(|entry| {
                ConfinementTmpfsResource::parse(entry).map(|parsed| (parsed.target.clone(), parsed))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for entry in &self.tmpfs {
            let parsed = ConfinementTmpfsResource::parse(entry)?;
            let Some(ceiling) = ceiling_by_target.get(&parsed.target) else {
                return Err(format!(
                    "tmpfs target '{}' has no mission resource ceiling",
                    parsed.target
                ));
            };
            if parsed.size_bytes > ceiling.size_bytes {
                return Err(format!(
                    "tmpfs target '{}' requests {}, above ceiling {}",
                    parsed.target, parsed.size_text, ceiling.size_text
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinementTmpfsResource {
    target: String,
    size_bytes: u64,
    size_text: String,
}

impl ConfinementTmpfsResource {
    pub fn parse(entry: &str) -> Result<Self, String> {
        if entry.contains('\0') {
            return Err("tmpfs resource contains NUL".to_string());
        }
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            return Err("tmpfs resource is required".to_string());
        }
        let (raw_target, raw_options) = trimmed
            .split_once(':')
            .ok_or_else(|| format!("tmpfs resource '{trimmed}' must declare size=<bytes>"))?;
        let target = normalize_resource_target(raw_target)?;
        let mut writable = false;
        let mut size = None;
        for raw_option in raw_options.split(',') {
            let option = raw_option.trim();
            if option.is_empty() {
                return Err(format!(
                    "tmpfs resource '{target}' declares an empty option"
                ));
            }
            if option == "rw" {
                if writable {
                    return Err(format!(
                        "tmpfs resource '{target}' declares rw more than once"
                    ));
                }
                writable = true;
                continue;
            }
            if let Some(value) = option.strip_prefix("size=") {
                if size.is_some() {
                    return Err(format!(
                        "tmpfs resource '{target}' declares size more than once"
                    ));
                }
                let value = value.trim();
                let bytes = parse_confinement_size_bytes(value).map_err(|detail| {
                    format!("tmpfs resource '{target}' size is invalid: {detail}")
                })?;
                size = Some((bytes, value.to_string()));
                continue;
            }
            return Err(format!(
                "tmpfs resource '{target}' option '{option}' is not allowed; declare only rw and size=<bytes>"
            ));
        }
        if !writable {
            return Err(format!("tmpfs resource '{target}' must declare rw"));
        }
        let (size_bytes, size_text) =
            size.ok_or_else(|| format!("tmpfs resource '{target}' must declare size=<bytes>"))?;
        Ok(Self {
            target,
            size_bytes,
            size_text,
        })
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn size_text(&self) -> &str {
        &self.size_text
    }

    pub fn runtime_argument(&self) -> String {
        format!("{}:rw,size={}", self.target, self.size_text)
    }
}

fn normalize_resource_target(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("tmpfs target is required".to_string());
    }
    if !trimmed.starts_with('/') {
        return Err(format!("tmpfs target '{trimmed}' must be absolute"));
    }
    let parts = trimmed
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return Err("tmpfs target must not be '/'".to_string());
    }
    if parts.iter().any(|part| *part == "." || *part == "..") {
        return Err(format!(
            "tmpfs target '{trimmed}' must not contain '.' or '..' components"
        ));
    }
    Ok(format!("/{}", parts.join("/")))
}

pub fn parse_confinement_size_bytes(raw: &str) -> Result<u64, String> {
    if raw.is_empty() {
        return Err("size is required".to_string());
    }
    let split_at = raw
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(raw.len());
    let (digits, suffix) = raw.split_at(split_at);
    if digits.is_empty() {
        return Err(format!("'{raw}' has no numeric prefix"));
    }
    let value = digits
        .parse::<u64>()
        .map_err(|_| format!("'{raw}' is not a valid integer size"))?;
    if value == 0 {
        return Err("size must be greater than zero".to_string());
    }
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "ki" | "kib" => 1024,
        "m" | "mb" | "mi" | "mib" => 1024 * 1024,
        "g" | "gb" | "gi" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "ti" | "tib" => 1024_u64.pow(4),
        _ => return Err(format!("unsupported size suffix '{suffix}'")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("'{raw}' overflows bytes"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionGuidance {
    pub text: String,
    pub digest: String,
}

impl MissionGuidance {
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        let digest = lowercase_hex(&Sha256::digest(text.as_bytes()));
        Self { text, digest }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.text.len() > MAX_GUIDANCE_BYTES {
            return Err(format!(
                "guidance is {} bytes; limit is {MAX_GUIDANCE_BYTES}",
                self.text.len()
            ));
        }
        let expected = lowercase_hex(&Sha256::digest(self.text.as_bytes()));
        if self.digest != expected {
            return Err("guidance digest does not match its text".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleInstance {
    pub id: RoleInstanceId,
    pub purpose: String,
    pub output: OutputSemantics,
    pub runtime: String,
    pub instructions: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub grants: AuthorityGrants,
    #[serde(default, skip_serializing_if = "ConfinementResources::is_empty")]
    pub resources: ConfinementResources,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_secs: Option<u64>,
}

/// The one closed execution assignment for a generic plan task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskAssignment {
    Role {
        role_instance: RoleInstanceId,
    },
    ChildMission {
        mission: Box<super::ChildMissionAssignment>,
    },
}

impl TaskAssignment {
    pub const fn role_instance(&self) -> Option<&RoleInstanceId> {
        match self {
            Self::Role { role_instance } => Some(role_instance),
            Self::ChildMission { .. } => None,
        }
    }

    pub const fn child_mission(&self) -> Option<&super::ChildMissionAssignment> {
        match self {
            Self::ChildMission { mission } => Some(mission),
            Self::Role { .. } => None,
        }
    }
}

impl From<RoleInstanceId> for TaskAssignment {
    fn from(role_instance: RoleInstanceId) -> Self {
        Self::Role { role_instance }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamRevision {
    pub revision: u32,
    pub roles: BTreeMap<RoleInstanceId, RoleInstance>,
    pub planning_assignment: RoleInstanceId,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub task_assignments: BTreeMap<TaskId, TaskAssignment>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub judgment_assignments: BTreeMap<AssertionId, Vec<RoleInstanceId>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_review_assignment: Option<RoleInstanceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidance: Option<MissionGuidance>,
}

impl TeamRevision {
    pub fn role(&self, id: &RoleInstanceId) -> Option<&RoleInstance> {
        self.roles.get(id)
    }

    pub fn task_role(&self, task_id: &TaskId) -> Option<&RoleInstance> {
        self.task_assignments
            .get(task_id)?
            .role_instance()
            .and_then(|role| self.role(role))
    }

    pub fn task_output(&self, task_id: &TaskId) -> Option<OutputSemantics> {
        match self.task_assignments.get(task_id)? {
            TaskAssignment::Role { role_instance } => {
                self.role(role_instance).map(|role| role.output)
            }
            TaskAssignment::ChildMission { mission } => Some(mission.output),
        }
    }

    pub fn validate_shape(&self) -> Result<(), String> {
        if self.roles.is_empty() {
            return Err("team has no role instances".to_string());
        }
        for (id, role) in &self.roles {
            if id != &role.id {
                return Err(format!(
                    "role map key '{id}' does not match role instance '{}'",
                    role.id
                ));
            }
            if role.purpose.trim().is_empty() {
                return Err(format!("role instance '{id}' has no purpose"));
            }
            if role.instructions.trim().is_empty() {
                return Err(format!("role instance '{id}' has no instructions"));
            }
            if role.runtime.trim().is_empty() {
                return Err(format!("role instance '{id}' has no runtime"));
            }
            if role.deadline_secs == Some(0) {
                return Err(format!("role instance '{id}' deadline must be positive"));
            }
            for (name, value) in &role.environment {
                validate_environment_entry(name, value)
                    .map_err(|detail| format!("role instance '{id}' environment {detail}"))?;
            }
            role.resources
                .validate()
                .map_err(|detail| format!("role instance '{id}' resources {detail}"))?;
            let mut skills = BTreeSet::new();
            for skill in &role.skills {
                if !skills.insert(skill) {
                    return Err(format!("role instance '{id}' repeats skill '{skill}'"));
                }
            }
        }
        let Some(planner) = self.roles.get(&self.planning_assignment) else {
            return Err(format!(
                "planning assignment '{}' does not name a role instance",
                self.planning_assignment
            ));
        };
        if planner.output != OutputSemantics::ProposesPlan {
            return Err(format!(
                "planning assignment '{}' must propose plans",
                self.planning_assignment
            ));
        }
        if let Some(gap_id) = &self.gap_review_assignment {
            let Some(gap) = self.roles.get(gap_id) else {
                return Err(format!(
                    "gap review assignment '{gap_id}' does not name a role instance"
                ));
            };
            if gap.output != OutputSemantics::EmitsGapVerdict {
                return Err(format!(
                    "gap review assignment '{gap_id}' must emit gap verdicts"
                ));
            }
        }
        if let Some(guidance) = &self.guidance {
            guidance.validate()?;
        }
        Ok(())
    }
}
