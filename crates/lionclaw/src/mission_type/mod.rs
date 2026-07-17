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

mod bundled;
mod digest;
mod frontmatter;
mod home;
mod install;
mod loader;
mod locator;
mod manifest;
mod skill_install;
mod skills;

pub use bundled::BundledMissionTypes;
pub(crate) use digest::ContentDigest;
pub use home::Home;
pub use install::{install_mission_type, materialize_mission_type, InstallOutcome};
pub use loader::{load_mission_type, MissionTypeError};
pub use locator::MissionTypeLocator;
pub use skill_install::{add_skill, remove_skill, SkillChange, SkillSource};

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::model::{
    InputName, MissionTypeInventory, OracleName, OutputSemantics, PlanningDag, RoleName, StopBar,
    TerminalReviewConfig,
};

/// A role is property-composed data: open fields (name, prompt, runtime) plus
/// the closed engine-understood axes (`output`, and the plain `network`/
/// `secrets` flags). There is no role "kind".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDefinition {
    pub name: RoleName,
    pub output: OutputSemantics,
    /// Runtime profile name; `None` uses the mission default.
    pub runtime: Option<String>,
    pub timeout_secs: Option<u64>,
    pub network: bool,
    pub secrets: bool,
    /// Mission-owned skills projected for this role. Empty is valid.
    pub skills: Vec<String>,
    pub prompt_body: String,
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

#[derive(Debug, Clone)]
pub struct MissionType {
    pub name: String,
    /// Content digest over the loaded files (`loader::compute_digest`),
    /// recorded at start and verified on every engine open.
    pub digest: String,
    pub stop: StopBar,
    /// The confinement image every role and oracle runs in (from `mission.toml`).
    pub image: String,
    /// The planning DAG (how an objective becomes a proposed contract). Empty
    /// ⇒ no in-engine planning; a mission of this type awaits a proposed plan.
    pub planning: PlanningDag,
    /// Mission-level role recovery budget.
    pub recovery: crate::model::RecoveryConfig,
    pub execution: crate::model::ExecutionPolicy,
    /// The closing review (the pure-core config type, threaded verbatim into
    /// `MissionConfig` at mission start). Required when `stop = "reviewed"`.
    pub terminal_review: Option<TerminalReviewConfig>,
    pub playbook: Option<String>,
    pub roles: BTreeMap<RoleName, RoleDefinition>,
    pub skills: BTreeMap<String, SkillPackage>,
    pub inputs: BTreeMap<InputName, PreparedInput>,
    pub oracles: BTreeMap<OracleName, PathBuf>,
}

impl MissionType {
    /// The complete revision-zero policy persisted at mission creation.
    /// Mission types are the sole source; replay reads the resolved copy from
    /// `MissionCreated` and never reopens bundle files.
    pub fn mission_config(&self) -> crate::model::MissionConfig {
        crate::model::MissionConfig {
            stop: self.stop,
            planning: self.planning.clone(),
            recovery: self.recovery.clone(),
            execution: self.execution.clone(),
            terminal_review: self.terminal_review.clone(),
        }
    }

    /// The pure inventory plan validation runs against.
    pub fn inventory(&self) -> MissionTypeInventory {
        MissionTypeInventory {
            roles: self
                .roles
                .iter()
                .map(|(name, role)| (name.clone(), role.output))
                .collect(),
            oracles: self.oracles.keys().cloned().collect::<BTreeSet<_>>(),
            stop: self.stop,
        }
    }
}
