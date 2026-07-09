//! Mission types are data: a directory of prose the engine loads and validates
//! fail-closed. The engine imports no domain — adding one is authoring a
//! directory, no Rust.
//!
//! ```text
//! <domain>/
//! ├─ mission.toml            # identity + the honesty bar (stop)
//! ├─ playbook.md             # the method (optional in the walking skeleton)
//! ├─ roles/<name>.md         # frontmatter (output, network, secrets, runtime) + prompt
//! └─ oracles/<name>          # executable; exit 0 = pass
//! ```

mod frontmatter;
mod home;
mod loader;

pub use home::{bundled_mission_types_dir, Home};
pub use loader::{load_mission_type, MissionTypeError};

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::model::{
    MissionTypeInventory, OracleName, OutputSemantics, PlanningDag, RoleName, StopBar,
};

/// A role is property-composed data: open fields (name, prompt, skills,
/// runtime) plus the closed engine-understood axes (`output`, and the plain
/// `network`/`secrets` flags). There is no role "kind".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDefinition {
    pub name: RoleName,
    pub output: OutputSemantics,
    /// Runtime profile name; `None` uses the mission default.
    pub runtime: Option<String>,
    pub network: bool,
    pub secrets: bool,
    pub skills: Vec<String>,
    pub prompt_body: String,
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
    /// ⇒ no in-engine planning; a mission of this type awaits a submitted plan.
    pub planning: PlanningDag,
    pub root: PathBuf,
    pub playbook: Option<String>,
    pub roles: BTreeMap<RoleName, RoleDefinition>,
    pub oracles: BTreeMap<OracleName, PathBuf>,
}

impl MissionType {
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
