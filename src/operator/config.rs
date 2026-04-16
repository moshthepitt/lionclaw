use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::{
    daemon_compat_partition_key, runtime_profile_partition_key, LionClawHome, DEFAULT_WORKSPACE,
};
use crate::kernel::runtime::{
    ConfinementConfig, ExecutionPreset, RuntimeAuthKind, RuntimeExecutionProfile,
};
use crate::kernel::skills::sanitize_skill_name;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorConfig {
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub defaults: OperatorDefaults,
    #[serde(default)]
    pub presets: BTreeMap<String, ExecutionPreset>,
    #[serde(default)]
    pub runtimes: BTreeMap<String, RuntimeProfileConfig>,
    #[serde(default)]
    pub skills: Vec<ManagedSkillConfig>,
    #[serde(default)]
    pub channels: Vec<ManagedChannelConfig>,
}

impl OperatorConfig {
    pub async fn load(home: &LionClawHome) -> Result<Self> {
        let path = home.config_path();
        if !tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?
        {
            return Ok(Self::default());
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        config.normalize();
        Ok(config)
    }

    pub async fn save(&self, home: &LionClawHome) -> Result<()> {
        let path = home.config_path();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut normalized = self.clone();
        normalized.normalize();
        let content =
            toml::to_string_pretty(&normalized).context("failed to encode operator config")?;
        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn upsert_skill(&mut self, skill: ManagedSkillConfig) {
        self.skills.retain(|existing| existing.alias != skill.alias);
        self.skills.push(skill);
        self.normalize();
    }

    pub fn remove_skill(&mut self, alias: &str) -> bool {
        let before = self.skills.len();
        self.skills.retain(|skill| skill.alias != alias);
        self.normalize();
        before != self.skills.len()
    }

    pub fn upsert_channel(&mut self, channel: ManagedChannelConfig) {
        self.channels.retain(|existing| existing.id != channel.id);
        self.channels.push(channel);
        self.normalize();
    }

    pub fn remove_channel(&mut self, id: &str) -> bool {
        let before = self.channels.len();
        self.channels.retain(|channel| channel.id != id);
        self.normalize();
        before != self.channels.len()
    }

    pub fn upsert_runtime(&mut self, id: String, runtime: RuntimeProfileConfig) {
        let should_set_default = self.defaults.runtime.is_none();
        self.runtimes.insert(id.clone(), runtime);
        if should_set_default {
            self.defaults.runtime = Some(id);
        }
        self.normalize();
    }

    pub fn remove_runtime(&mut self, id: &str) -> bool {
        let removed = self.runtimes.remove(id).is_some();
        if removed && self.defaults.runtime.as_deref() == Some(id) {
            self.defaults.runtime = None;
        }
        self.normalize();
        removed
    }

    pub fn runtime(&self, id: &str) -> Option<&RuntimeProfileConfig> {
        self.runtimes.get(id)
    }

    pub fn upsert_preset(&mut self, id: String, preset: ExecutionPreset) {
        let should_set_default = self.defaults.preset.is_none();
        self.presets.insert(id.clone(), preset);
        if should_set_default {
            self.defaults.preset = Some(id);
        }
        self.normalize();
    }

    pub fn remove_preset(&mut self, id: &str) -> bool {
        let removed = self.presets.remove(id).is_some();
        if removed && self.defaults.preset.as_deref() == Some(id) {
            self.defaults.preset = None;
        }
        self.normalize();
        removed
    }

    pub fn preset(&self, id: &str) -> Option<&ExecutionPreset> {
        self.presets.get(id)
    }

    pub fn resolve_runtime_id(&self, requested: Option<&str>) -> Result<String> {
        if let Some(runtime_id) = requested.map(str::trim).filter(|value| !value.is_empty()) {
            if !self.runtimes.contains_key(runtime_id) {
                return Err(anyhow!("runtime profile '{runtime_id}' is not configured"));
            }
            return Ok(runtime_id.to_string());
        }

        self.defaults
            .runtime
            .clone()
            .ok_or_else(|| anyhow!("runtime is required when no default runtime is configured"))
    }

    pub fn resolve_preset_id(&self, requested: Option<&str>) -> Result<String> {
        if let Some(preset_id) = requested.map(str::trim).filter(|value| !value.is_empty()) {
            return Ok(preset_id.to_string());
        }

        self.defaults
            .preset
            .clone()
            .ok_or_else(|| anyhow!("preset is required when no default preset is configured"))
    }

    pub fn set_default_runtime(&mut self, id: &str) -> Result<()> {
        if !self.runtimes.contains_key(id) {
            return Err(anyhow!("runtime profile '{id}' is not configured"));
        }
        self.defaults.runtime = Some(id.to_string());
        self.normalize();
        Ok(())
    }

    pub fn set_default_preset(&mut self, id: &str) -> Result<()> {
        if !self.presets.contains_key(id) {
            return Err(anyhow!("preset '{id}' is not configured"));
        }
        self.defaults.preset = Some(id.to_string());
        self.normalize();
        Ok(())
    }

    pub fn workspace_root(&self, home: &LionClawHome) -> PathBuf {
        home.workspace_dir(&self.daemon.workspace)
    }

    fn normalize(&mut self) {
        self.defaults.runtime = self
            .defaults
            .runtime
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self.defaults.preset = self
            .defaults
            .preset
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self.presets = std::mem::take(&mut self.presets)
            .into_iter()
            .filter_map(|(id, mut preset)| {
                let id = id.trim().to_string();
                if id.is_empty() {
                    return None;
                }
                normalize_execution_preset(&mut preset);
                Some((id, preset))
            })
            .collect();
        self.runtimes = std::mem::take(&mut self.runtimes)
            .into_iter()
            .filter_map(|(id, mut runtime)| {
                let id = id.trim().to_string();
                if id.is_empty() {
                    return None;
                }
                runtime.normalize();
                Some((id, runtime))
            })
            .collect();
        self.skills
            .sort_by(|left, right| left.alias.cmp(&right.alias));
        self.channels.sort_by(|left, right| left.id.cmp(&right.id));
        for channel in &mut self.channels {
            channel.required_env.sort();
            channel.required_env.dedup();
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct DaemonCompatConfig {
    version: u32,
    workspace_name: String,
    default_runtime_id: Option<String>,
    default_preset_name: Option<String>,
    codex_home_override: Option<String>,
    execution_presets: BTreeMap<String, ExecutionPreset>,
    runtime_profiles: BTreeMap<String, RuntimeProfileConfig>,
    runtime_image_identities: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeCompatConfig {
    version: u32,
    profile: RuntimeProfileConfig,
    runtime_auth_identity: Option<String>,
}

pub fn daemon_compat_fingerprint(config: &OperatorConfig) -> String {
    daemon_compat_fingerprint_with_runtime_context(config, None, &BTreeMap::new())
}

pub fn daemon_compat_fingerprint_with_runtime_context(
    config: &OperatorConfig,
    codex_home_override: Option<&Path>,
    runtime_image_identities: &BTreeMap<String, String>,
) -> String {
    let mut normalized = config.clone();
    normalized.normalize();
    let encoded = compatibility_digest_bytes(&DaemonCompatConfig {
        version: 1,
        workspace_name: normalized.daemon.workspace.clone(),
        default_runtime_id: normalized.defaults.runtime.clone(),
        default_preset_name: normalized.defaults.preset.clone(),
        codex_home_override: codex_home_override.map(|path| path.display().to_string()),
        execution_presets: normalized.presets,
        runtime_profiles: normalized.runtimes,
        runtime_image_identities: runtime_image_identities.clone(),
    });
    daemon_compat_partition_key(&encoded)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorDefaults {
    #[serde(default)]
    pub runtime: Option<String>,
    #[serde(default)]
    pub preset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_workspace")]
    pub workspace: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            workspace: default_workspace(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedSkillConfig {
    pub alias: String,
    pub source: String,
    #[serde(default = "default_reference")]
    pub reference: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedChannelConfig {
    pub id: String,
    pub skill: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub launch_mode: ChannelLaunchMode,
    #[serde(default)]
    pub required_env: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChannelLaunchMode {
    #[default]
    Service,
    Interactive,
}

impl ChannelLaunchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Interactive => "interactive",
        }
    }
}

impl std::str::FromStr for ChannelLaunchMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim() {
            "service" => Ok(Self::Service),
            "interactive" => Ok(Self::Interactive),
            other => Err(format!(
                "invalid channel launch mode '{other}'; expected 'service' or 'interactive'"
            )),
        }
    }
}

pub fn default_bind() -> String {
    "127.0.0.1:8979".to_string()
}

pub fn default_workspace() -> String {
    DEFAULT_WORKSPACE.to_string()
}

fn default_reference() -> String {
    "local".to_string()
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RuntimeProfileConfig {
    #[serde(rename = "codex")]
    Codex {
        executable: String,
        #[serde(default)]
        model: Option<String>,
        confinement: ConfinementConfig,
    },
    #[serde(rename = "opencode")]
    OpenCode {
        executable: String,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        agent: Option<String>,
        confinement: ConfinementConfig,
    },
}

impl RuntimeProfileConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Codex { .. } => "codex",
            Self::OpenCode { .. } => "opencode",
        }
    }

    pub fn executable(&self) -> &str {
        match self {
            Self::Codex { executable, .. } | Self::OpenCode { executable, .. } => executable,
        }
    }

    pub fn confinement(&self) -> &ConfinementConfig {
        match self {
            Self::Codex { confinement, .. } | Self::OpenCode { confinement, .. } => confinement,
        }
    }

    pub fn required_runtime_auth(&self) -> Option<RuntimeAuthKind> {
        match self {
            Self::Codex {
                confinement: ConfinementConfig::Oci(_),
                ..
            } => Some(RuntimeAuthKind::Codex),
            Self::OpenCode { .. } => None,
        }
    }

    pub fn execution_profile(&self) -> RuntimeExecutionProfile {
        self.execution_profile_with_runtime_context(None, None)
    }

    pub fn execution_profile_with_image_identity(
        &self,
        image_identity: Option<&str>,
    ) -> RuntimeExecutionProfile {
        self.execution_profile_with_runtime_context(image_identity, None)
    }

    pub fn execution_profile_with_runtime_context(
        &self,
        image_identity: Option<&str>,
        runtime_auth_identity: Option<&str>,
    ) -> RuntimeExecutionProfile {
        RuntimeExecutionProfile::new(
            self.confinement().clone(),
            self.compatibility_base_key(runtime_auth_identity),
            image_identity.map(str::to_string),
            self.required_runtime_auth(),
        )
    }

    pub fn compatibility_key(&self) -> String {
        self.compatibility_key_with_runtime_context(None, None)
    }

    pub fn compatibility_key_with_image_identity(&self, image_identity: Option<&str>) -> String {
        self.compatibility_key_with_runtime_context(image_identity, None)
    }

    pub fn compatibility_key_with_runtime_context(
        &self,
        image_identity: Option<&str>,
        runtime_auth_identity: Option<&str>,
    ) -> String {
        self.execution_profile_with_runtime_context(image_identity, runtime_auth_identity)
            .compatibility_key
    }

    fn compatibility_base_key(&self, runtime_auth_identity: Option<&str>) -> String {
        let mut normalized = self.clone();
        normalized.normalize();
        let encoded = compatibility_digest_bytes(&RuntimeCompatConfig {
            version: 1,
            profile: normalized,
            runtime_auth_identity: runtime_auth_identity.map(str::to_string),
        });
        runtime_profile_partition_key(&encoded)
    }

    pub fn validate(&self) -> Result<()> {
        validate_runtime_command(self.executable())?;

        match self.confinement() {
            ConfinementConfig::Oci(config) => {
                validate_podman_executable(&config.engine)?;
                if config
                    .image
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    return Err(anyhow!("Podman runtime image is required"));
                }
            }
        }

        Ok(())
    }

    fn normalize(&mut self) {
        match self {
            Self::Codex {
                executable,
                model,
                confinement,
            } => {
                *executable = executable.trim().to_string();
                *model = model
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
                normalize_confinement_config(confinement);
            }
            Self::OpenCode {
                executable,
                model,
                agent,
                confinement,
            } => {
                *executable = executable.trim().to_string();
                *model = model
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
                *agent = agent
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
                normalize_confinement_config(confinement);
            }
        }
    }
}

#[allow(
    clippy::expect_used,
    reason = "compatibility digest structs contain only infallibly serializable fields"
)]
fn compatibility_digest_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("compatibility digest inputs are infallibly serializable")
}

fn normalize_execution_preset(_preset: &mut ExecutionPreset) {}

fn normalize_confinement_config(config: &mut ConfinementConfig) {
    match config {
        ConfinementConfig::Oci(oci) => {
            oci.engine = oci.engine.trim().to_string();
            oci.image = oci
                .image
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            oci.tmpfs = std::mem::take(&mut oci.tmpfs)
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect();
            for mount in &mut oci.additional_mounts {
                mount.target = mount.target.trim().to_string();
            }
        }
    }
}

pub fn derive_skill_alias(source: &str) -> String {
    let raw = source
        .strip_prefix("local:")
        .unwrap_or(source)
        .trim_end_matches('/')
        .split('/')
        .next_back()
        .unwrap_or("skill");

    let alias = sanitize_skill_name(raw)
        .trim_start_matches("channel-")
        .to_string();
    if alias.is_empty() {
        "skill".to_string()
    } else {
        alias
    }
}

pub fn normalize_local_source(source: &str) -> Result<String> {
    let raw = source.strip_prefix("local:").unwrap_or(source);
    let absolute = std::fs::canonicalize(Path::new(raw))
        .with_context(|| format!("failed to resolve source '{source}'"))?;
    Ok(format!("local:{}", absolute.display()))
}

pub fn normalize_runtime_command(source: &str) -> Result<String> {
    let raw = source.trim();
    if raw.is_empty() {
        return Err(anyhow!("runtime command cannot be empty"));
    }

    Ok(raw.to_string())
}

pub fn validate_runtime_command(source: &str) -> Result<()> {
    if source.trim().is_empty() {
        return Err(anyhow!("runtime command cannot be empty"));
    }

    Ok(())
}

pub fn normalize_host_executable(source: &str) -> Result<String> {
    let raw = source.trim();
    if raw.is_empty() {
        return Err(anyhow!("host executable command or path cannot be empty"));
    }

    let path = if looks_like_path(raw) {
        normalize_executable_path(raw)?
    } else {
        which::which(raw)
            .with_context(|| format!("failed to resolve host executable '{source}'"))?
    };
    validate_executable_path(&path)?;
    Ok(path.to_string_lossy().to_string())
}

pub fn normalize_podman_executable(source: &str) -> Result<String> {
    let normalized = normalize_host_executable(source)?;
    ensure_podman_executable(Path::new(&normalized))?;
    Ok(normalized)
}

pub fn validate_executable_path(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Err(anyhow!("executable '{}' is not a file", path.display()));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(path)
            .with_context(|| format!("failed to read metadata for '{}'", path.display()))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err(anyhow!(
                "executable '{}' is not marked executable",
                path.display()
            ));
        }
    }

    Ok(())
}

pub fn validate_host_executable(source: &str) -> Result<()> {
    let raw = source.trim();
    if raw.is_empty() {
        return Err(anyhow!("host executable command or path cannot be empty"));
    }

    if !looks_like_path(raw) {
        return Err(anyhow!(
            "host executable '{source}' must be stored as an absolute or explicit path"
        ));
    }
    validate_executable_path(&normalize_executable_path(raw)?)
}

pub fn validate_podman_executable(source: &str) -> Result<()> {
    validate_host_executable(source)?;
    ensure_podman_executable(&normalize_executable_path(source.trim())?)
}

fn looks_like_path(raw: &str) -> bool {
    let path = Path::new(raw);
    path.is_absolute() || raw.contains('/') || raw.starts_with(".")
}

fn normalize_executable_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let current_dir = std::env::current_dir().context("failed to resolve current directory")?;
    Ok(current_dir.join(path))
}

fn ensure_podman_executable(path: &Path) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("podman engine path '{}' has no file name", path.display()))?;
    if file_name != "podman" {
        return Err(anyhow!(
            "Podman confinement requires a 'podman' executable path, got '{}'",
            path.display()
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use super::{
        daemon_compat_fingerprint, daemon_compat_fingerprint_with_runtime_context,
        derive_skill_alias, normalize_host_executable, normalize_local_source,
        normalize_runtime_command, validate_host_executable, ChannelLaunchMode,
        ManagedChannelConfig, OperatorConfig, RuntimeProfileConfig,
    };
    use crate::kernel::runtime::{
        ConfinementConfig, ExecutionPreset, NetworkMode, OciConfinementConfig, WorkspaceAccess,
    };

    #[test]
    fn derives_channel_alias_from_source_path() {
        assert_eq!(derive_skill_alias("skills/channel-telegram"), "telegram");
        assert_eq!(
            derive_skill_alias("local:/tmp/custom-skill"),
            "custom-skill"
        );
    }

    #[tokio::test]
    async fn missing_config_loads_default() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = OperatorConfig::load(&home).await.expect("load");
        assert!(config.skills.is_empty());
        assert!(config.channels.is_empty());
        assert!(config.runtimes.is_empty());
        assert!(config.presets.is_empty());
    }

    #[tokio::test]
    async fn channel_launch_mode_defaults_to_service() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "terminal".to_string(),
            skill: "terminal".to_string(),
            enabled: true,
            launch_mode: ChannelLaunchMode::default(),
            required_env: Vec::new(),
        });
        config.save(&home).await.expect("save config");

        let loaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(loaded.channels[0].launch_mode, ChannelLaunchMode::Service);
    }

    #[tokio::test]
    async fn channel_launch_mode_round_trips() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "terminal".to_string(),
            skill: "terminal".to_string(),
            enabled: true,
            launch_mode: ChannelLaunchMode::Interactive,
            required_env: Vec::new(),
        });
        config.save(&home).await.expect("save config");

        let loaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(
            loaded.channels[0].launch_mode,
            ChannelLaunchMode::Interactive
        );
    }

    #[test]
    fn normalizes_local_source_uri() {
        let absolute = normalize_local_source(".").expect("normalize");
        assert!(absolute.starts_with("local:/"));
    }

    #[test]
    fn first_runtime_becomes_default() {
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: sample_confinement(),
            },
        );

        assert_eq!(config.defaults.runtime.as_deref(), Some("codex"));
    }

    #[test]
    fn removing_default_runtime_clears_default() {
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: sample_confinement(),
            },
        );

        assert!(config.remove_runtime("codex"));
        assert!(config.defaults.runtime.is_none());
    }

    #[test]
    fn requested_runtime_must_be_configured() {
        let config = OperatorConfig::default();
        let err = config
            .resolve_runtime_id(Some("codex"))
            .expect_err("unconfigured runtime should fail");

        assert!(err
            .to_string()
            .contains("runtime profile 'codex' is not configured"));
    }

    #[test]
    fn runtime_compatibility_key_changes_when_profile_changes() {
        let left = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: Some("gpt-5".to_string()),
            confinement: sample_confinement(),
        };
        let right = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: Some("gpt-5.1".to_string()),
            confinement: sample_confinement(),
        };
        let normalized = RuntimeProfileConfig::Codex {
            executable: " codex ".to_string(),
            model: Some(" gpt-5 ".to_string()),
            confinement: sample_confinement(),
        };

        assert_ne!(left.compatibility_key(), right.compatibility_key());
        assert_eq!(left.compatibility_key(), normalized.compatibility_key());
    }

    #[test]
    fn runtime_compatibility_key_changes_when_image_identity_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };

        assert_ne!(
            runtime.compatibility_key_with_image_identity(Some("sha256:left")),
            runtime.compatibility_key_with_image_identity(Some("sha256:right"))
        );
    }

    #[test]
    fn runtime_compatibility_key_changes_when_codex_home_identity_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };

        assert_ne!(
            runtime.compatibility_key_with_runtime_context(
                Some("sha256:runtime"),
                Some("codex-home:/tmp/codex-a"),
            ),
            runtime.compatibility_key_with_runtime_context(
                Some("sha256:runtime"),
                Some("codex-home:/tmp/codex-b"),
            )
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_default_runtime_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };
        let mut left = OperatorConfig::default();
        left.upsert_runtime("codex".to_string(), runtime.clone());
        left.upsert_runtime("opencode".to_string(), runtime);
        left.set_default_runtime("codex")
            .expect("set default runtime");

        let mut right = left.clone();
        right
            .set_default_runtime("opencode")
            .expect("set second default runtime");

        assert_ne!(
            daemon_compat_fingerprint(&left),
            daemon_compat_fingerprint(&right)
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_workspace_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };
        let mut left = OperatorConfig::default();
        left.upsert_runtime("codex".to_string(), runtime);
        left.set_default_runtime("codex")
            .expect("set default runtime");

        let mut right = left.clone();
        right.daemon.workspace = "other-workspace".to_string();

        assert_ne!(
            daemon_compat_fingerprint(&left),
            daemon_compat_fingerprint(&right)
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_codex_home_override_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), runtime);

        assert_ne!(
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                Some(Path::new("/tmp/codex-a")),
                &BTreeMap::new(),
            ),
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                Some(Path::new("/tmp/codex-b")),
                &BTreeMap::new(),
            )
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_runtime_image_identity_changes() {
        let runtime = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: sample_confinement(),
        };
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), runtime);

        let mut left = BTreeMap::new();
        left.insert("codex".to_string(), "sha256:left".to_string());
        let mut right = BTreeMap::new();
        right.insert("codex".to_string(), "sha256:right".to_string());

        assert_ne!(
            daemon_compat_fingerprint_with_runtime_context(&config, None, &left),
            daemon_compat_fingerprint_with_runtime_context(&config, None, &right)
        );
    }

    #[test]
    fn first_preset_becomes_default() {
        let mut config = OperatorConfig::default();
        config.upsert_preset(
            "everyday".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
            },
        );

        assert_eq!(config.defaults.preset.as_deref(), Some("everyday"));
    }

    #[test]
    fn removing_default_preset_clears_default() {
        let mut config = OperatorConfig::default();
        config.upsert_preset(
            "everyday".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
            },
        );

        assert!(config.remove_preset("everyday"));
        assert!(config.defaults.preset.is_none());
    }

    #[test]
    fn preset_normalization_trims_name_and_preserves_secret_mount_toggle() {
        let mut config = OperatorConfig::default();
        config.upsert_preset(
            "  everyday  ".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                mount_runtime_secrets: true,
                escape_classes: Default::default(),
            },
        );

        assert!(config.preset("  everyday  ").is_none());
        let preset = config.preset("everyday").expect("normalized preset");
        assert!(preset.mount_runtime_secrets);
    }

    #[cfg(unix)]
    #[test]
    fn normalize_host_executable_rejects_non_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("not-executable");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

        let err =
            normalize_host_executable(path.to_str().expect("path utf8")).expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn normalize_host_executable_resolves_bare_commands_to_absolute_paths() {
        let normalized = normalize_host_executable("sh").expect("normalize");
        let expected = which::which("sh").expect("resolve sh");

        assert_eq!(normalized, expected.to_string_lossy());
    }

    #[cfg(unix)]
    #[test]
    fn validate_host_executable_rejects_bare_commands() {
        let err = validate_host_executable("sh").expect_err("bare command should fail");
        assert!(err
            .to_string()
            .contains("must be stored as an absolute or explicit path"));
    }

    #[test]
    fn normalize_runtime_command_trims_without_host_resolution() {
        let normalized = normalize_runtime_command("  /usr/local/bin/codex  ").expect("trim");
        assert_eq!(normalized, "/usr/local/bin/codex");
    }

    fn sample_confinement() -> ConfinementConfig {
        ConfinementConfig::Oci(OciConfinementConfig {
            engine: "/usr/bin/podman".to_string(),
            image: Some("ghcr.io/lionclaw/runtime:latest".to_string()),
            ..OciConfinementConfig::default()
        })
    }
}
