use serde::Serialize;

pub const PROJECT_INSTANCE_ENV: &str = "LIONCLAW_PROJECT_INSTANCE";
pub const PROJECT_INSTANCES_FILE_ENV: &str = "LIONCLAW_PROJECT_INSTANCES_FILE";
pub const PROJECT_INSTANCE_INVENTORY_DIR: &str = "/lionclaw/project";
pub const PROJECT_INSTANCES_FILE_NAME: &str = "instances.json";
pub const PROJECT_INSTANCES_FILE_PATH: &str = "/lionclaw/project/instances.json";

const PROJECT_INSTANCE_INVENTORY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectInstanceInventory {
    pub schema_version: u32,
    pub default_instance: Option<String>,
    pub instances: Vec<ProjectInstanceInventoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectInstanceInventoryEntry {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectInstanceRuntimeContext {
    pub instance_name: String,
    pub inventory: ProjectInstanceInventory,
}

impl ProjectInstanceInventory {
    pub fn new(default_instance: Option<String>, instance_names: Vec<String>) -> Self {
        Self {
            schema_version: PROJECT_INSTANCE_INVENTORY_SCHEMA_VERSION,
            default_instance,
            instances: instance_names
                .into_iter()
                .map(|name| ProjectInstanceInventoryEntry { name })
                .collect(),
        }
    }

    pub fn contains_instance(&self, name: &str) -> bool {
        self.instances.iter().any(|instance| instance.name == name)
    }

    pub fn to_pretty_json(&self) -> serde_json::Result<String> {
        let mut encoded = serde_json::to_string_pretty(self)?;
        encoded.push('\n');
        Ok(encoded)
    }
}
