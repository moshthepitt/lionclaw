use std::{
    collections::BTreeSet,
    fmt,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::skill_alias::validate_skill_alias;

pub use lionclaw_runtime_api::{NetworkMode, RuntimeAuthKind, RuntimeProgramSpec};

pub const WORKSPACE_MOUNT_TARGET: &str = "/workspace";
pub const RUNTIME_MOUNT_TARGET: &str = "/runtime";
pub const RUNTIME_HOME_MOUNT_TARGET: &str = "/runtime/home";
pub const DRAFTS_MOUNT_TARGET: &str = "/drafts";
pub const SKILLS_MOUNT_TARGET_ROOT: &str = "/lionclaw/skills";
pub const INHERITED_SKILLS_MOUNT_TARGET_ROOT: &str = "/lionclaw/inherited-skills";
pub const RUNTIME_INSTALL_ENV_DIR: &str = ".lionclaw";
pub const RUNTIME_INSTALL_ENV_FILE: &str = "install-env.sh";
pub const RUNTIME_INSTALL_ENV_PATH: &str = "/runtime/home/.lionclaw/install-env.sh";

pub fn skill_mount_target(alias: &str) -> String {
    format!("{SKILLS_MOUNT_TARGET_ROOT}/{alias}")
}

pub fn runtime_skill_mount_target_alias(target: &str) -> Option<&str> {
    target
        .strip_prefix(SKILLS_MOUNT_TARGET_ROOT)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .filter(|alias| !alias.contains('/') && validate_skill_alias(alias).is_ok())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RuntimeSkillProjectionConfig {
    NativeDir {
        root: String,
        #[serde(default)]
        format: RuntimeSkillProjectionFormat,
        /// Existing human-managed skill roots exposed alongside mission skills.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        inherit: Vec<InheritedSkillRoot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct InheritedSkillRoot {
    pub source: PathBuf,
    pub target: String,
    #[serde(default)]
    pub optional: bool,
}

impl RuntimeSkillProjectionConfig {
    pub fn native_dir(root: impl Into<String>) -> Self {
        let mut projection = Self::NativeDir {
            root: root.into(),
            format: RuntimeSkillProjectionFormat::SkillMd,
            inherit: Vec::new(),
        };
        projection.normalize();
        projection
    }

    pub fn native_dir_root(&self) -> &str {
        match self {
            Self::NativeDir { root, .. } => root,
        }
    }

    pub fn inherited_roots(&self) -> &[InheritedSkillRoot] {
        match self {
            Self::NativeDir { inherit, .. } => inherit,
        }
    }

    pub fn inherited_roots_mut(&mut self) -> &mut Vec<InheritedSkillRoot> {
        match self {
            Self::NativeDir { inherit, .. } => inherit,
        }
    }

    pub fn normalize(&mut self) {
        match self {
            Self::NativeDir { root, inherit, .. } => {
                *root = normalize_runtime_skill_projection_root(root);
                for inherited in inherit {
                    inherited.target = normalize_runtime_skill_projection_root(&inherited.target);
                }
            }
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::NativeDir { root, inherit, .. } => {
                validate_runtime_skill_projection_root(root)?;
                for inherited in inherit {
                    if !inherited.source.is_absolute() {
                        anyhow::bail!(
                            "inherited skill source '{}' must be absolute",
                            inherited.source.display()
                        );
                    }
                    validate_runtime_skill_projection_root(&inherited.target)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSkillProjectionFormat {
    #[default]
    SkillMd,
}

fn normalize_runtime_skill_projection_root(root: &str) -> String {
    root.trim().to_string()
}

fn validate_runtime_skill_projection_root(root: &str) -> Result<()> {
    let normalized = normalize_runtime_skill_projection_root(root);
    if normalized.is_empty() {
        anyhow::bail!("runtime skill projection root is required");
    }

    let path = Path::new(&normalized);
    if path.is_absolute() {
        anyhow::bail!("runtime skill projection root must be relative");
    }

    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                anyhow::bail!(
                    "runtime skill projection root must not contain traversal or absolute components"
                );
            }
        }
    }

    Ok(())
}

pub fn mount_source_for_target<'a>(mounts: &'a [MountSpec], target: &str) -> Option<&'a Path> {
    mounts
        .iter()
        .find(|mount| mount.target == target)
        .map(|mount| mount.source.as_path())
}

pub fn runtime_state_mount_source(mounts: &[MountSpec]) -> Option<&Path> {
    mount_source_for_target(mounts, RUNTIME_MOUNT_TARGET)
}

pub fn runtime_native_home_mount_source(mounts: &[MountSpec]) -> Option<&Path> {
    mount_source_for_target(mounts, RUNTIME_HOME_MOUNT_TARGET)
}

/// Maps a host-side path selected by execution policy into the path visible to
/// the runtime process through the configured mounts.
pub fn map_host_path_into_runtime_mount(
    host_path: &str,
    mounts: &[MountSpec],
    path_label: &str,
) -> Result<String> {
    let requested = PathBuf::from(host_path);
    let (mount, relative) = longest_mount_prefix(&requested, mounts).ok_or_else(|| {
        anyhow!(
            "{path_label} '{}' is not inside any configured runtime mount",
            requested.display()
        )
    })?;

    let runtime_root = Path::new(&mount.target);
    let mapped = if relative.as_os_str().is_empty() {
        runtime_root.to_path_buf()
    } else {
        runtime_root.join(relative)
    };

    Ok(mapped.to_string_lossy().to_string())
}

fn longest_mount_prefix<'a>(
    requested: &Path,
    mounts: &'a [MountSpec],
) -> Option<(&'a MountSpec, PathBuf)> {
    mounts
        .iter()
        .filter_map(|mount| {
            strip_mount_prefix(requested, &mount.source).map(|relative| (mount, relative))
        })
        .max_by_key(|(mount, _)| mount.source.components().count())
}

fn strip_mount_prefix(requested: &Path, source: &Path) -> Option<PathBuf> {
    if requested == source {
        return Some(PathBuf::new());
    }

    requested.strip_prefix(source).ok().map(Path::to_path_buf)
}

/// User-facing coarse execution preset compiled before a turn starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionPreset {
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    #[serde(default)]
    pub install_policy: InstallPolicy,
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
            install_policy: InstallPolicy::User,
            mount_runtime_secrets: false,
            escape_classes: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallPolicy {
    None,
    #[default]
    User,
    System,
}

impl InstallPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::User => "user",
            Self::System => "system",
        }
    }

    pub fn uses_user_install_helpers(self) -> bool {
        matches!(self, Self::User | Self::System)
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
    pub skill_projection: Option<RuntimeSkillProjectionConfig>,
    pub workspace_access: WorkspaceAccess,
    pub network_mode: NetworkMode,
    pub install_policy: InstallPolicy,
    pub root_in_userns: bool,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub mcp_servers: Vec<lionclaw_runtime_api::RuntimeMcpServerSpec>,
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
            .field("skill_projection", &self.skill_projection)
            .field("workspace_access", &self.workspace_access)
            .field("network_mode", &self.network_mode)
            .field("install_policy", &self.install_policy)
            .field("root_in_userns", &self.root_in_userns)
            .field("working_dir", &self.working_dir)
            .field("environment_count", &self.environment.len())
            .field("mcp_servers", &self.mcp_servers)
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

    use super::{
        runtime_skill_mount_target_alias, ConfinementConfig, EffectiveExecutionPlan, EscapeClass,
        ExecutionLimits, ExecutionPreset, InheritedSkillRoot, InstallPolicy, NetworkMode,
        OciConfinementConfig, RuntimeSkillProjectionConfig, WorkspaceAccess,
    };

    #[test]
    fn runtime_skill_mount_target_alias_accepts_only_exact_valid_alias_targets() {
        assert_eq!(
            runtime_skill_mount_target_alias("/lionclaw/skills/loopback"),
            Some("loopback")
        );
        assert_eq!(
            runtime_skill_mount_target_alias("/lionclaw/skills/channel.terminal_1"),
            Some("channel.terminal_1")
        );

        for target in [
            "/lionclaw/skills",
            "/lionclaw/skills/",
            "/lionclaw/skills/../custom",
            "/lionclaw/skills/loopback/extra",
            "/lionclaw/skills/.hidden",
            "/lionclaw/skillsfoo",
            "/other/skills/loopback",
        ] {
            assert_eq!(
                runtime_skill_mount_target_alias(target),
                None,
                "{target} should not be a managed runtime skill target"
            );
        }
    }

    #[test]
    fn execution_preset_round_trips_without_embedded_name() {
        let preset = ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            install_policy: InstallPolicy::User,
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
    fn install_policy_defaults_to_user_and_uses_kebab_case_values() {
        let defaulted: ExecutionPreset = serde_json::from_value(json!({
            "workspace-access": "read-write",
            "network-mode": "on",
            "mount-runtime-secrets": false
        }))
        .expect("deserialize preset without install policy");
        assert_eq!(defaulted.install_policy, InstallPolicy::User);

        for (raw, expected) in [
            ("none", InstallPolicy::None),
            ("user", InstallPolicy::User),
            ("system", InstallPolicy::System),
        ] {
            let preset = ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                install_policy: expected,
            };
            let value = serde_json::to_value(&preset).expect("serialize preset");
            assert_eq!(
                value.get("install-policy").and_then(|raw| raw.as_str()),
                Some(raw)
            );

            let round_trip: ExecutionPreset =
                serde_json::from_value(value).expect("deserialize preset");
            assert_eq!(round_trip.install_policy, expected);
        }

        let err = serde_json::from_value::<ExecutionPreset>(json!({
            "workspace-access": "read-write",
            "network-mode": "on",
            "install-policy": "global"
        }))
        .expect_err("invalid install policy should be rejected");
        assert!(err.to_string().contains("unknown variant"));
        assert!(err.to_string().contains("global"));
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

    #[test]
    fn install_policy_effective_execution_plan_debug_redacts_environment_values() {
        let debug = format!(
            "{:?}",
            EffectiveExecutionPlan {
                runtime_id: "codex".to_string(),
                preset_name: "team-local".to_string(),
                confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                skill_projection: None,
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                install_policy: InstallPolicy::User,
                root_in_userns: false,
                working_dir: None,
                environment: vec![("SECRET_ENV".to_string(), "sensitive-value".to_string())],
                mcp_servers: Vec::new(),
                hard_timeout: std::time::Duration::from_secs(1),
                mounts: Vec::new(),
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                limits: ExecutionLimits::default(),
            }
        );

        assert!(debug.contains("install_policy"));
        assert!(debug.contains("root_in_userns"));
        assert!(debug.contains("environment_count"));
        assert!(!debug.contains("SECRET_ENV"));
        assert!(!debug.contains("sensitive-value"));
    }

    #[test]
    fn runtime_skill_projection_root_must_be_safe_relative_path() {
        let valid = super::RuntimeSkillProjectionConfig::native_dir(" .config/runtime/skills ");
        assert_eq!(valid.native_dir_root(), ".config/runtime/skills");
        valid.validate().expect("valid projection root");

        for root in ["/absolute/skills", "../skills", "skills/../other", ""] {
            let projection = super::RuntimeSkillProjectionConfig::native_dir(root);
            assert!(
                projection.validate().is_err(),
                "{root:?} should be rejected"
            );
        }
    }

    #[test]
    fn inherited_skill_roots_require_absolute_sources_and_safe_targets() {
        let mut relative = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        relative.inherited_roots_mut().push(InheritedSkillRoot {
            source: "relative/skills".into(),
            target: ".native/skills".to_string(),
            optional: true,
        });
        assert!(relative.validate().is_err());

        let mut traversal = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        traversal.inherited_roots_mut().push(InheritedSkillRoot {
            source: "/home/user/skills".into(),
            target: "../skills".to_string(),
            optional: true,
        });
        assert!(traversal.validate().is_err());
    }
}
