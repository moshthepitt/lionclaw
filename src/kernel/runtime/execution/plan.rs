use std::{collections::BTreeSet, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

/// Adapter-produced program invocation details, independent from how LionClaw
/// chooses to confine the process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeProgramSpec {
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub stdin: String,
}

/// User-facing coarse execution preset compiled before a turn starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionPreset {
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub secret_bindings: Vec<SecretBinding>,
    #[serde(default)]
    pub escape_classes: BTreeSet<EscapeClass>,
}

impl Default for ExecutionPreset {
    fn default() -> Self {
        Self {
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            secret_bindings: Vec::new(),
            escape_classes: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceAccess {
    ReadOnly,
    ReadWrite,
}

impl WorkspaceAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::ReadWrite => "read-write",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    None,
    On,
    Allowlist,
}

impl NetworkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::On => "on",
            Self::Allowlist => "allowlist",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SecretBinding {
    pub name: String,
    pub kind: SecretBindingKind,
    #[serde(default)]
    pub target_env: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretBindingKind {
    LaunchEnv,
    Brokered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum EscapeClass {
    ChannelSend,
    NetEgress,
    SecretRequest,
    SchedulerRun,
    ArtifactPublish,
}

impl EscapeClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ChannelSend => "channel-send",
            Self::NetEgress => "net-egress",
            Self::SecretRequest => "secret-request",
            Self::SchedulerRun => "scheduler-run",
            Self::ArtifactPublish => "artifact-publish",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MountSpec {
    pub source: PathBuf,
    pub target: String,
    pub access: MountAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MountAccess {
    ReadOnly,
    ReadWrite,
}

impl MountAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::ReadWrite => "read-write",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum ConfinementConfig {
    Oci(OciConfinementConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfinementBackend {
    Oci,
}

impl ConfinementConfig {
    pub fn backend(&self) -> ConfinementBackend {
        match self {
            Self::Oci(_) => ConfinementBackend::Oci,
        }
    }

    pub fn oci(&self) -> &OciConfinementConfig {
        match self {
            Self::Oci(config) => config,
        }
    }

    pub fn oci_mut(&mut self) -> &mut OciConfinementConfig {
        match self {
            Self::Oci(config) => config,
        }
    }
}

impl ConfinementBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Oci => "oci",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OciConfinementConfig {
    #[serde(default = "default_oci_engine")]
    pub engine: String,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub read_only_rootfs: bool,
    #[serde(default)]
    pub tmpfs: Vec<String>,
    #[serde(default)]
    pub additional_mounts: Vec<MountSpec>,
    #[serde(default)]
    pub limits: ExecutionLimits,
}

impl Default for OciConfinementConfig {
    fn default() -> Self {
        Self {
            engine: default_oci_engine(),
            image: None,
            read_only_rootfs: false,
            tmpfs: Vec::new(),
            additional_mounts: Vec::new(),
            limits: ExecutionLimits::default(),
        }
    }
}

fn default_oci_engine() -> String {
    "podman".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionLimits {
    pub memory_limit: Option<String>,
    pub cpu_limit: Option<String>,
    pub pids_limit: Option<u32>,
}

/// Kernel-compiled execution plan for a single runtime turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveExecutionPlan {
    pub runtime_id: String,
    pub preset_name: String,
    pub confinement: ConfinementConfig,
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub hard_timeout: Duration,
    pub mounts: Vec<MountSpec>,
    pub secret_bindings: Vec<SecretBinding>,
    pub escape_classes: BTreeSet<EscapeClass>,
    pub limits: ExecutionLimits,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ConfinementConfig, EscapeClass, ExecutionPreset, NetworkMode, SecretBinding,
        SecretBindingKind, WorkspaceAccess,
    };

    #[test]
    fn execution_preset_round_trips_without_embedded_name() {
        let preset = ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            secret_bindings: vec![SecretBinding {
                name: "github".to_string(),
                kind: SecretBindingKind::LaunchEnv,
                target_env: Some("GITHUB_TOKEN".to_string()),
            }],
            escape_classes: [EscapeClass::SecretRequest].into_iter().collect(),
        };

        let value = serde_json::to_value(&preset).expect("serialize preset");
        assert!(
            value.get("name").is_none(),
            "preset identity should stay external"
        );

        let round_trip: ExecutionPreset =
            serde_json::from_value(value).expect("deserialize preset");
        assert_eq!(round_trip, preset);
    }

    #[test]
    fn oci_confinement_uses_nested_limits_shape() {
        let config: ConfinementConfig = serde_json::from_value(json!({
            "backend": "oci",
            "read-only-rootfs": true,
            "limits": {
                "memory-limit": "4g",
                "cpu-limit": "2",
                "pids-limit": 512
            }
        }))
        .expect("deserialize confinement config");

        let value = serde_json::to_value(&config).expect("serialize confinement config");
        let limits = value
            .get("limits")
            .and_then(|raw| raw.as_object())
            .expect("limits object");
        assert_eq!(
            value.get("engine").and_then(|raw| raw.as_str()),
            Some("podman")
        );
        assert_eq!(value.get("image"), Some(&serde_json::Value::Null));
        assert_eq!(
            limits.get("memory-limit").and_then(|raw| raw.as_str()),
            Some("4g")
        );
        assert_eq!(
            limits.get("cpu-limit").and_then(|raw| raw.as_str()),
            Some("2")
        );
        assert_eq!(
            limits.get("pids-limit").and_then(|raw| raw.as_u64()),
            Some(512)
        );
    }
}
