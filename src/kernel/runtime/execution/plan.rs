use std::{collections::BTreeSet, fmt, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

/// Adapter-produced program invocation details, independent from how LionClaw
/// chooses to confine the process.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct RuntimeProgramSpec {
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub stdin: String,
    pub auth: Option<RuntimeAuthKind>,
}

impl fmt::Debug for RuntimeProgramSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeProgramSpec")
            .field("executable", &self.executable)
            .field("args", &self.args)
            .field("environment_count", &self.environment.len())
            .field("stdin_len", &self.stdin.len())
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAuthKind {
    Codex,
}

/// User-facing coarse execution preset compiled before a turn starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionPreset {
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub mount_runtime_secrets: bool,
    #[serde(default)]
    pub escape_classes: BTreeSet<EscapeClass>,
}

impl Default for ExecutionPreset {
    fn default() -> Self {
        Self {
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            mount_runtime_secrets: false,
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
}

impl NetworkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::On => "on",
        }
    }
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
#[serde(tag = "backend")]
pub enum ConfinementConfig {
    #[serde(rename = "podman")]
    Oci(OciConfinementConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfinementBackend {
    #[serde(rename = "podman")]
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
            Self::Oci => "podman",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OciConfinementConfig {
    #[serde(default = "default_podman_engine")]
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
            engine: default_podman_engine(),
            image: None,
            read_only_rootfs: false,
            tmpfs: Vec::new(),
            additional_mounts: Vec::new(),
            limits: ExecutionLimits::default(),
        }
    }
}

fn default_podman_engine() -> String {
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
#[derive(Clone, PartialEq, Eq)]
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
    pub mount_runtime_secrets: bool,
    pub escape_classes: BTreeSet<EscapeClass>,
    pub limits: ExecutionLimits,
}

impl fmt::Debug for EffectiveExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EffectiveExecutionPlan")
            .field("runtime_id", &self.runtime_id)
            .field("preset_name", &self.preset_name)
            .field("confinement", &self.confinement)
            .field("workspace_access", &self.workspace_access)
            .field("network_mode", &self.network_mode)
            .field("working_dir", &self.working_dir)
            .field("environment_count", &self.environment.len())
            .field("idle_timeout", &self.idle_timeout)
            .field("hard_timeout", &self.hard_timeout)
            .field("mounts", &self.mounts)
            .field("mount_runtime_secrets", &self.mount_runtime_secrets)
            .field("escape_classes", &self.escape_classes)
            .field("limits", &self.limits)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ConfinementConfig, EscapeClass, ExecutionPreset, NetworkMode, WorkspaceAccess};

    #[test]
    fn execution_preset_round_trips_without_embedded_name() {
        let preset = ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            mount_runtime_secrets: true,
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
    fn podman_confinement_uses_nested_limits_shape() {
        let config: ConfinementConfig = serde_json::from_value(json!({
            "backend": "podman",
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

    #[test]
    fn execution_preset_rejects_allowlist_network_mode() {
        let err = serde_json::from_value::<ExecutionPreset>(json!({
            "workspace-access": "read-write",
            "network-mode": "allowlist",
            "mount-runtime-secrets": false
        }))
        .expect_err("allowlist network mode should be rejected");

        assert!(err.to_string().contains("unknown variant"));
        assert!(err.to_string().contains("allowlist"));
    }

    #[test]
    fn runtime_program_spec_debug_redacts_environment_and_stdin_values() {
        let debug = format!(
            "{:?}",
            super::RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: vec!["exec".to_string()],
                environment: vec![("GITHUB_TOKEN".to_string(), "ghp_secret".to_string())],
                stdin: "hello".to_string(),
                auth: None,
            }
        );

        assert!(debug.contains("environment_count"));
        assert!(!debug.contains("ghp_secret"));
        assert!(!debug.contains("hello"));
    }
}
