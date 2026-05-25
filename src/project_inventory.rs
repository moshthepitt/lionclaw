use std::path::PathBuf;

use serde::Serialize;

pub const PROJECT_INSTANCE_ENV: &str = "LIONCLAW_PROJECT_INSTANCE";
pub const PROJECT_INSTANCES_FILE_ENV: &str = "LIONCLAW_PROJECT_INSTANCES_FILE";
pub const PROJECT_INSTANCE_INVENTORY_DIR: &str = "/lionclaw/project";
pub const PROJECT_INSTANCES_FILE_NAME: &str = "instances.json";
pub const PROJECT_INSTANCES_FILE_PATH: &str = "/lionclaw/project/instances.json";

const PROJECT_INSTANCE_INVENTORY_SCHEMA_VERSION: u32 = 1;
const PROJECT_INSTANCE_CONTACT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectInstanceInventory {
    pub schema_version: u32,
    pub default_instance: Option<String>,
    pub instances: Vec<ProjectInstanceInventoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectInstanceInventoryEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_send: Option<ProjectInstanceChannelSend>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectInstanceChannelSend {
    pub status: ProjectInstanceChannelSendStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_ref: Option<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectInstanceChannelSendStatus {
    Unconfigured,
    Configured,
    ChannelMissing,
    Misconfigured,
}

impl ProjectInstanceChannelSend {
    pub fn unconfigured() -> Self {
        Self {
            status: ProjectInstanceChannelSendStatus::Unconfigured,
            channel_id: None,
            conversation_ref: None,
            thread_ref: None,
        }
    }

    pub fn configured(
        channel_id: String,
        conversation_ref: String,
        thread_ref: Option<String>,
    ) -> Self {
        Self {
            status: ProjectInstanceChannelSendStatus::Configured,
            channel_id: Some(channel_id),
            conversation_ref: Some(conversation_ref),
            thread_ref: Some(thread_ref),
        }
    }

    pub fn channel_missing() -> Self {
        Self {
            status: ProjectInstanceChannelSendStatus::ChannelMissing,
            channel_id: None,
            conversation_ref: None,
            thread_ref: None,
        }
    }

    pub fn misconfigured() -> Self {
        Self {
            status: ProjectInstanceChannelSendStatus::Misconfigured,
            channel_id: None,
            conversation_ref: None,
            thread_ref: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectInstanceRuntimeContext {
    pub project_root: PathBuf,
    pub instance_name: String,
    pub inventory: ProjectInstanceInventory,
    pub channel_send_inventory: ProjectInstanceInventory,
}

impl ProjectInstanceInventory {
    pub fn new(default_instance: Option<String>, instance_names: Vec<String>) -> Self {
        Self {
            schema_version: PROJECT_INSTANCE_INVENTORY_SCHEMA_VERSION,
            default_instance,
            instances: instance_names
                .into_iter()
                .map(ProjectInstanceInventoryEntry::identity)
                .collect(),
        }
    }

    pub fn new_channel_send(
        default_instance: Option<String>,
        instances: Vec<ProjectInstanceInventoryEntry>,
    ) -> Self {
        Self {
            schema_version: PROJECT_INSTANCE_CONTACT_SCHEMA_VERSION,
            default_instance,
            instances,
        }
    }

    pub fn unconfigured_channel_send_projection(&self, selected_instance: &str) -> Self {
        Self::new_channel_send(
            self.default_instance.clone(),
            self.instances
                .iter()
                .map(|instance| {
                    if instance.name == selected_instance {
                        ProjectInstanceInventoryEntry::identity(instance.name.clone())
                    } else {
                        ProjectInstanceInventoryEntry::with_channel_send(
                            instance.name.clone(),
                            ProjectInstanceChannelSend::unconfigured(),
                        )
                    }
                })
                .collect(),
        )
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

impl ProjectInstanceInventoryEntry {
    pub fn identity(name: String) -> Self {
        Self {
            name,
            channel_send: None,
        }
    }

    pub fn with_channel_send(name: String, channel_send: ProjectInstanceChannelSend) -> Self {
        Self {
            name,
            channel_send: Some(channel_send),
        }
    }
}

impl ProjectInstanceRuntimeContext {
    pub fn new(
        project_root: PathBuf,
        instance_name: String,
        inventory: ProjectInstanceInventory,
    ) -> Self {
        let channel_send_inventory = inventory.unconfigured_channel_send_projection(&instance_name);
        Self {
            project_root,
            instance_name,
            inventory,
            channel_send_inventory,
        }
    }

    pub fn with_channel_send_inventory(
        mut self,
        channel_send_inventory: ProjectInstanceInventory,
    ) -> Self {
        self.channel_send_inventory = channel_send_inventory;
        self
    }
}
