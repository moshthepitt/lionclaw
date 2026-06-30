use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::protocol::CHANNEL_ID;

#[derive(Debug, Clone, Serialize)]
pub struct ListSummary {
    pub ok: bool,
    pub self_instance: String,
    pub members: Vec<MemberView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemberView {
    pub name: String,
    #[serde(rename = "self")]
    pub is_self: bool,
    pub route_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_send: Option<RouteView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolveSummary {
    pub ok: bool,
    pub recipient: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_send: Option<RouteView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RouteError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteView {
    pub channel_id: String,
    pub conversation_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct ProjectInventory {
    instances: Vec<ProjectInventoryEntry>,
}

#[derive(Debug, Deserialize)]
struct ProjectInventoryEntry {
    name: String,
    #[serde(default)]
    channel_send: Option<ProjectInventoryChannelSend>,
}

#[derive(Debug, Deserialize)]
struct ProjectInventoryChannelSend {
    status: String,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    conversation_ref: Option<String>,
    #[serde(default)]
    thread_ref: Option<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct RecipientRoute {
    pub channel_id: String,
    pub conversation_ref: String,
    pub thread_ref: Option<String>,
}

impl ProjectInventory {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    pub fn list_summary(&self, self_instance: &str) -> ListSummary {
        ListSummary {
            ok: true,
            self_instance: self_instance.to_string(),
            members: self
                .instances
                .iter()
                .map(|entry| entry.member_view(self_instance))
                .collect(),
        }
    }

    pub fn resolve_summary(&self, self_instance: &str, recipient: &str) -> ResolveSummary {
        match self.route_for(self_instance, recipient) {
            Ok(route) => ResolveSummary {
                ok: true,
                recipient: recipient.to_string(),
                status: "resolved".to_string(),
                channel_send: Some(route.view()),
                error: None,
            },
            Err(error) => ResolveSummary {
                ok: false,
                recipient: recipient.to_string(),
                status: "unroutable".to_string(),
                channel_send: None,
                error: Some(error),
            },
        }
    }

    pub fn route_for(
        &self,
        self_instance: &str,
        recipient: &str,
    ) -> std::result::Result<RecipientRoute, RouteError> {
        if recipient == self_instance {
            return Err(route_error(
                "self_recipient",
                "team-local cannot send a message to the selected instance itself",
            ));
        }
        let Some(entry) = self.instances.iter().find(|entry| entry.name == recipient) else {
            return Err(route_error(
                "unknown_recipient",
                format!("project instance '{recipient}' is not in instances.json"),
            ));
        };
        entry.route(recipient)
    }
}

impl ProjectInventoryEntry {
    fn member_view(&self, self_instance: &str) -> MemberView {
        let is_self = self.name == self_instance;
        MemberView {
            name: self.name.clone(),
            is_self,
            route_status: if is_self {
                "self".to_string()
            } else {
                self.channel_send
                    .as_ref()
                    .map(|channel_send| channel_send.status.clone())
                    .unwrap_or_else(|| "unconfigured".to_string())
            },
            channel_send: if is_self {
                None
            } else {
                self.route(&self.name).ok().map(|route| route.view())
            },
        }
    }

    fn route(&self, recipient: &str) -> std::result::Result<RecipientRoute, RouteError> {
        let Some(channel_send) = &self.channel_send else {
            return Err(route_error(
                "recipient_unconfigured",
                format!("project instance '{recipient}' has no channel_send route"),
            ));
        };
        if channel_send.status != "configured" {
            return Err(route_error(
                channel_send.status.as_str(),
                format!(
                    "project instance '{recipient}' channel_send status is '{}'",
                    channel_send.status
                ),
            ));
        }
        let channel_id =
            required_route_field(recipient, "channel_id", channel_send.channel_id.as_deref())?;
        if channel_id != CHANNEL_ID {
            return Err(route_error(
                "wrong_channel",
                format!("project instance '{recipient}' uses channel '{channel_id}'"),
            ));
        }
        let conversation_ref = required_route_field(
            recipient,
            "conversation_ref",
            channel_send.conversation_ref.as_deref(),
        )?;
        Ok(RecipientRoute {
            channel_id: channel_id.to_string(),
            conversation_ref: conversation_ref.to_string(),
            thread_ref: channel_send.thread_ref.clone().flatten(),
        })
    }
}

impl RecipientRoute {
    fn view(&self) -> RouteView {
        RouteView {
            channel_id: self.channel_id.clone(),
            conversation_ref: self.conversation_ref.clone(),
            thread_ref: self.thread_ref.clone(),
        }
    }
}

fn required_route_field<'a>(
    recipient: &str,
    field: &str,
    value: Option<&'a str>,
) -> std::result::Result<&'a str, RouteError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            route_error(
                "recipient_misconfigured",
                format!("project instance '{recipient}' route is missing {field}"),
            )
        })
}

pub fn ensure_single_recipient(recipient: Option<String>) -> Result<String> {
    let Some(recipient) = recipient.map(|value| value.trim().to_string()) else {
        bail!("team-local resolve requires a recipient");
    };
    if recipient.is_empty() {
        bail!("team-local recipient cannot be empty");
    }
    Ok(recipient)
}

fn route_error(code: impl Into<String>, message: impl Into<String>) -> RouteError {
    RouteError {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::ProjectInventory;

    #[test]
    fn lists_members_with_route_statuses() {
        let inventory = inventory(json!({
            "schema_version": 2,
            "default_instance": "main",
            "instances": [
                { "name": "main" },
                {
                    "name": "reviewer",
                    "channel_send": {
                        "status": "configured",
                        "channel_id": "team-local",
                        "conversation_ref": "team-local:peer:home-reviewer",
                        "thread_ref": null
                    }
                },
                {
                    "name": "qa",
                    "channel_send": { "status": "channel_missing" }
                }
            ]
        }));

        let summary = inventory.list_summary("main");

        assert!(summary.ok);
        assert_eq!(summary.members[0].name, "main");
        assert!(summary.members[0].is_self);
        assert_eq!(summary.members[0].route_status, "self");
        assert_eq!(summary.members[1].route_status, "configured");
        assert_eq!(
            summary.members[1]
                .channel_send
                .as_ref()
                .expect("route")
                .conversation_ref,
            "team-local:peer:home-reviewer"
        );
        assert_eq!(summary.members[2].route_status, "channel_missing");
    }

    #[test]
    fn resolves_configured_route_or_unroutable_reason() {
        let inventory = inventory(json!({
            "schema_version": 2,
            "default_instance": "main",
            "instances": [
                { "name": "main" },
                {
                    "name": "reviewer",
                    "channel_send": {
                        "status": "configured",
                        "channel_id": "team-local",
                        "conversation_ref": "team-local:peer:home-reviewer"
                    }
                }
            ]
        }));

        let resolved = inventory.resolve_summary("main", "reviewer");
        assert!(resolved.ok);
        assert_eq!(resolved.status, "resolved");
        assert_eq!(
            resolved.channel_send.expect("route").conversation_ref,
            "team-local:peer:home-reviewer"
        );

        let unresolved = inventory.resolve_summary("main", "missing");
        assert!(!unresolved.ok);
        assert_eq!(unresolved.status, "unroutable");
        assert_eq!(unresolved.error.expect("error").code, "unknown_recipient");
    }

    fn inventory(value: serde_json::Value) -> ProjectInventory {
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("instances.json");
        std::fs::write(&path, value.to_string()).expect("inventory");
        ProjectInventory::load(&path).expect("load inventory")
    }
}
