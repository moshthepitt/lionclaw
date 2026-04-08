use std::{collections::BTreeSet, path::PathBuf};

use serde::{Deserialize, Serialize};

/// Adapter-produced program invocation details, independent from how LionClaw
/// chooses to confine the process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeProgramSpec {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub stdin: String,
}

/// User-facing coarse execution preset compiled before a turn starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionPreset {
    pub name: String,
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub secret_bindings: Vec<SecretBinding>,
    #[serde(default)]
    pub escape_classes: BTreeSet<EscapeClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    None,
    On,
    Allowlist,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SecretBinding {
    pub name: String,
    pub kind: SecretBindingKind,
    #[serde(default)]
    pub target_env: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OciConfinementConfig {
    pub engine: String,
    pub image: String,
    #[serde(default)]
    pub read_only_rootfs: bool,
    #[serde(default)]
    pub tmpfs: Vec<String>,
    #[serde(default)]
    pub additional_mounts: Vec<MountSpec>,
    #[serde(default)]
    pub memory_limit: Option<String>,
    #[serde(default)]
    pub cpu_limit: Option<String>,
    #[serde(default)]
    pub pids_limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    pub mounts: Vec<MountSpec>,
    pub secret_bindings: Vec<SecretBinding>,
    pub escape_classes: BTreeSet<EscapeClass>,
    pub limits: ExecutionLimits,
}
