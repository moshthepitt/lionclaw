//! Mission-owned team snapshots and authority requests.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{ids::lowercase_hex, AssertionId, InputName, OutputSemantics, RoleInstanceId, TaskId};
use crate::prelude::*;

pub const MAX_GUIDANCE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityGrants {
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub network: bool,
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
    pub network: bool,
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
            && (!self.network || ceilings.network)
            && (!self.install || ceilings.install)
            && (!self.writes || ceilings.writes)
            && self.devices.is_subset(&ceilings.devices)
            && self.inputs.is_subset(&ceilings.inputs)
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamRevision {
    pub revision: u32,
    pub roles: BTreeMap<RoleInstanceId, RoleInstance>,
    pub planning_assignment: RoleInstanceId,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub task_assignments: BTreeMap<TaskId, RoleInstanceId>,
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
