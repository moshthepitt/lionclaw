//! Plugins are data: a directory of prose the engine loads and validates
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
mod loader;

pub use loader::{load_plugin, PluginError};

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::model::{OracleName, OutputSemantics, PluginInventory, RoleName, StopBar};

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
pub struct LoadedPlugin {
    pub name: String,
    pub stop: StopBar,
    pub root: PathBuf,
    pub playbook: Option<String>,
    pub roles: BTreeMap<RoleName, RoleDefinition>,
    pub oracles: BTreeMap<OracleName, PathBuf>,
}

impl LoadedPlugin {
    /// The pure inventory plan validation runs against.
    pub fn inventory(&self) -> PluginInventory {
        PluginInventory {
            roles: self
                .roles
                .iter()
                .map(|(name, role)| (name.clone(), role.output))
                .collect(),
            oracles: self.oracles.keys().cloned().collect::<BTreeSet<_>>(),
        }
    }
}
