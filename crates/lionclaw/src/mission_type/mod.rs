//! Mission types are data: a directory of prose the engine loads and validates
//! fail-closed. The engine imports no domain — adding one is authoring a
//! directory, no Rust.
//!
//! ```text
//! <domain>/
//! ├─ mission.toml            # identity + the honesty bar (stop)
//! ├─ playbook.md             # required mission-specific method
//! ├─ roles/<name>.md         # frontmatter (output, network, secrets, runtime) + prompt
//! ├─ skills/<name>/SKILL.md   # optional role skills and their resources
//! ├─ inputs/<name>            # optional prepared-input program
//! └─ oracles/<name>          # executable; exit 0 = pass
//! ```

mod bounded_tree;
mod bundled;
mod digest;
mod frontmatter;
mod home;
mod install;
mod loader;
mod locator;
mod manifest;
mod prepared_input;
mod skill_install;
mod skills;

pub use bundled::BundledMissionTypes;
pub(crate) use digest::ContentDigest;
pub use home::Home;
pub use install::{install_mission_type, materialize_mission_type, InstallOutcome};
pub(crate) use loader::load_materialized_mission_type;
pub use loader::{load_mission_type, MissionTypeError};
pub use locator::MissionTypeLocator;
pub use skill_install::{add_mission_skill, add_skill, remove_skill, SkillChange, SkillSource};
pub(crate) use skills::load_skill_package;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::model::{
    validate_environment_entry, AuthorityCeilings, ConfinementResources, InputName, OracleName,
    StopBar, TeamRevision,
};

/// Aggregate program and declared-key content admitted to one prepared-input
/// cache identity.
pub(crate) const MAX_PREPARED_INPUT_CONTENT_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

pub(crate) fn has_shebang(path: &Path) -> bool {
    use std::io::Read;
    let mut bytes = [0_u8; 2];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut bytes))
        .is_ok_and(|_| &bytes == b"#!")
}

/// One resolved Agent Skills package in the loaded mission-type closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPackage {
    pub name: String,
    pub root: PathBuf,
    /// The validated, whitespace-trimmed `description` from `SKILL.md`
    /// frontmatter — carried into the role prompt's assigned-skill section.
    pub description: String,
}

/// One mission-declared input producer. The program receives the judged tree
/// read-only at `/workspace` and writes its complete output to `/output`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedInput {
    pub name: InputName,
    pub program: PathBuf,
    pub network: bool,
    pub key_files: Vec<PathBuf>,
    pub environment: BTreeMap<String, String>,
}

/// Untrusted mission-type data. Production callers obtain a sealed
/// [`MissionType`] from [`load_mission_type`]; keeping the definition separate
/// makes a stale or caller-selected content pin unrepresentable.
#[derive(Debug, Clone)]
pub struct MissionTypeDefinition {
    pub name: String,
    pub stop: StopBar,
    /// The confinement image every role and oracle runs in (from `mission.toml`).
    pub image: String,
    /// Domain-owned environment shared by roles and oracles. Kernel-owned
    /// execution coordinates cannot be overridden here.
    pub environment: BTreeMap<String, String>,
    pub default_team: TeamRevision,
    pub ceilings: AuthorityCeilings,
    pub resource_ceilings: ConfinementResources,
    pub requires_gap_review: bool,
    /// Mission-level role recovery budget.
    pub recovery: crate::model::RecoveryConfig,
    pub execution: crate::model::ExecutionPolicy,
    pub playbook: Option<String>,
    pub skills: BTreeMap<String, SkillPackage>,
    pub inputs: BTreeMap<InputName, PreparedInput>,
    pub oracles: BTreeMap<OracleName, PathBuf>,
    pub oracle_resources: BTreeMap<OracleName, ConfinementResources>,
    pub oracle_devices: BTreeMap<OracleName, std::collections::BTreeSet<String>>,
}

/// One validated mission-type closure sealed to its content identity.
#[derive(Debug, Clone)]
pub struct MissionType {
    definition: MissionTypeDefinition,
    digest: String,
    source_owner: Option<std::sync::Arc<tempfile::TempDir>>,
}

impl std::ops::Deref for MissionType {
    type Target = MissionTypeDefinition;

    fn deref(&self) -> &Self::Target {
        &self.definition
    }
}

impl MissionType {
    pub(crate) fn from_loaded(definition: MissionTypeDefinition, digest: String) -> Self {
        Self {
            definition,
            digest,
            source_owner: None,
        }
    }

    pub(crate) fn with_source_owner(mut self, owner: std::sync::Arc<tempfile::TempDir>) -> Self {
        self.source_owner = Some(owner);
        self
    }

    /// Content digest over the complete loaded bundle, recorded at mission
    /// creation and verified on every engine open.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn for_testing(definition: MissionTypeDefinition) -> Self {
        let digest = test_definition_digest(&definition);
        Self {
            definition,
            digest,
            source_owner: None,
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn edit_for_testing(&mut self, edit: impl FnOnce(&mut MissionTypeDefinition)) {
        edit(&mut self.definition);
        self.digest = test_definition_digest(&self.definition);
    }

    /// Validate the complete semantic mission-type contract at the clock epoch
    /// where immutable effect deadlines will be derived. Bundle loading and
    /// direct engine creation use this same boundary.
    pub fn validate_at(&self, now_ms: i64) -> anyhow::Result<()> {
        self.default_team
            .validate_shape()
            .map_err(|error| anyhow::anyhow!("[team] {error}"))?;
        if self.recovery.max_attempts == 0 {
            anyhow::bail!("[recovery] max-attempts must be at least 1");
        }
        self.execution
            .validate_at(now_ms)
            .map_err(|error| anyhow::anyhow!("[execution] invalid execution policy: {error}"))?;
        validate_mission_environment(&self.environment)?;
        for (name, role) in &self.default_team.roles {
            crate::authority::validate_role_authority_request(role)?;
            if name != &role.id {
                anyhow::bail!(
                    "role map key '{name}' does not match role definition '{}'",
                    role.id
                );
            }
            if !role.grants.within(&self.ceilings) {
                anyhow::bail!(
                    "role instance '{}' exceeds mission authority ceilings",
                    role.id
                );
            }
            role.resources
                .within(&self.resource_ceilings)
                .map_err(|detail| {
                    anyhow::anyhow!(
                        "role '{}' requests resources outside mission ceilings: {detail}",
                        role.id
                    )
                })?;
            if let Some(timeout_secs) = role.deadline_secs {
                if timeout_secs == 0 {
                    anyhow::bail!("role '{}' timeout must be at least 1 second", role.id);
                }
                crate::model::resolve_execution_deadline_ms(now_ms, timeout_secs).map_err(
                    |error| anyhow::anyhow!("role '{}' deadline is invalid: {error}", role.id),
                )?;
            }
            for skill in &role.skills {
                if !self.skills.contains_key(skill) {
                    anyhow::bail!("role '{}' names missing skill '{skill}'", role.id);
                }
            }
            for input in &role.grants.inputs {
                if !self.inputs.contains_key(input) {
                    anyhow::bail!("role '{}' names missing prepared input '{input}'", role.id);
                }
            }
        }
        prepared_input::validate_prepared_inputs(&self.inputs)
            .map_err(|error| anyhow::anyhow!(error))?;
        if !self
            .ceilings
            .inputs
            .iter()
            .all(|input| self.inputs.contains_key(input))
        {
            anyhow::bail!("authority ceilings name an undeclared prepared input");
        }
        self.resource_ceilings
            .validate()
            .map_err(|detail| anyhow::anyhow!("resource ceilings are invalid: {detail}"))?;
        for (oracle, resources) in &self.oracle_resources {
            if !self.oracles.contains_key(oracle) {
                anyhow::bail!("oracle resources name undeclared oracle '{oracle}'");
            }
            resources
                .within(&self.resource_ceilings)
                .map_err(|detail| {
                    anyhow::anyhow!(
                        "oracle '{oracle}' requests resources outside mission ceilings: {detail}"
                    )
                })?;
        }
        for (oracle, devices) in &self.oracle_devices {
            if !self.oracles.contains_key(oracle) {
                anyhow::bail!("oracle devices name undeclared oracle '{oracle}'");
            }
            if !devices.is_subset(&self.ceilings.devices) {
                anyhow::bail!("oracle '{oracle}' requests devices outside mission ceilings");
            }
        }
        if self.requires_gap_review && self.default_team.gap_review_assignment.is_none() {
            anyhow::bail!("[team] requires-gap-review needs a gap-review assignment");
        }
        Ok(())
    }

    /// The complete revision-zero policy persisted at mission creation.
    /// Mission types are the sole source; replay reads the resolved copy from
    /// `MissionCreated` and never reopens bundle files.
    pub fn mission_config(&self) -> crate::model::MissionConfig {
        crate::model::MissionConfig {
            stop: self.stop,
            oracles: self.oracles.keys().cloned().collect(),
            ceilings: self.ceilings.clone(),
            resource_ceilings: self.resource_ceilings.clone(),
            oracle_resources: self.oracle_resources.clone(),
            oracle_devices: self.oracle_devices.clone(),
            requires_gap_review: self.requires_gap_review,
            recovery: self.recovery.clone(),
            execution: self.execution.clone(),
        }
    }
}

fn validate_mission_environment(environment: &BTreeMap<String, String>) -> anyhow::Result<()> {
    for (name, value) in environment {
        validate_environment_entry(name, value)
            .map_err(|detail| anyhow::anyhow!("[mission-type] environment {detail}"))?;
    }
    Ok(())
}

/// Compose one deterministic process environment. Prepared inputs may replace
/// domain policy, while kernel execution coordinates are always final.
pub(crate) fn execution_environment(
    kernel: impl IntoIterator<Item = (String, String)>,
    mission: &BTreeMap<String, String>,
    prepared_input: impl IntoIterator<Item = (String, String)>,
) -> Vec<(String, String)> {
    let mut environment = mission.clone();
    environment.extend(prepared_input);
    environment.extend(kernel);
    environment.into_iter().collect()
}

#[cfg(any(test, feature = "testing"))]
fn test_definition_digest(definition: &MissionTypeDefinition) -> String {
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(format!("{definition:#?}").as_bytes()))
}

#[cfg(test)]
mod environment_tests {
    use super::*;

    #[test]
    fn mission_environment_cannot_override_kernel_coordinates() {
        for name in crate::model::KERNEL_ENVIRONMENT_KEYS {
            let error = validate_mission_environment(&BTreeMap::from([(
                (*name).to_string(),
                "override".to_string(),
            )]))
            .expect_err("kernel coordinate must remain authoritative");
            assert!(error.to_string().contains("owned by the LionClaw kernel"));
        }
    }

    #[test]
    fn prepared_input_is_the_explicit_final_environment_overlay() {
        let composed = execution_environment(
            [("HOME".to_string(), "/runtime/home".to_string())],
            &BTreeMap::from([
                ("BUILD_CACHE".to_string(), "/scratch/cache".to_string()),
                ("TOOL_HOME".to_string(), "/scratch/tool".to_string()),
            ]),
            [
                ("HOME".to_string(), "/inputs/escape".to_string()),
                ("TOOL_HOME".to_string(), "/inputs/tool".to_string()),
            ],
        );
        let composed = BTreeMap::from_iter(composed);
        assert_eq!(composed["HOME"], "/runtime/home");
        assert_eq!(composed["BUILD_CACHE"], "/scratch/cache");
        assert_eq!(composed["TOOL_HOME"], "/inputs/tool");
    }
}
