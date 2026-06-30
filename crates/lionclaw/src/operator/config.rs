use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::{
    daemon_compat_partition_key, runtime_profile_partition_key, LionClawHome, DEFAULT_WORKSPACE,
};
use crate::kernel::runtime::{
    execution::mount_validation::{normalize_runtime_mount_target, validate_configured_mounts},
    ConfinementConfig, ExecutionPreset, RuntimeAuthContext, RuntimeAuthKind,
    RuntimeExecutionProfile, RuntimeSkillProjectionConfig, RuntimeTerminalConfig,
};
use crate::kernel::skills::sanitize_skill_name;
use crate::operator::command_display::shell_quote_arg;

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
    pub channels: Vec<ManagedChannelConfig>,
    #[serde(default)]
    pub private_context: PrivateContextConfig,
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
        let mut config: Self = match toml::from_str(&content) {
            Ok(config) => config,
            Err(err) => {
                if let Some(message) = legacy_runtime_profile_config_error(&content) {
                    return Err(anyhow!("failed to parse {}: {message}", path.display()));
                }
                return Err(err).with_context(|| format!("failed to parse {}", path.display()));
            }
        };
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

    pub fn upsert_channel(&mut self, channel: ManagedChannelConfig) {
        if channel.contact.is_some() {
            for existing in &mut self.channels {
                if existing.id != channel.id {
                    existing.contact = None;
                }
            }
        }
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
        self.private_context.normalize();
        self.channels.sort_by_key(|channel| channel.id.clone());
        for channel in &mut self.channels {
            channel.id = channel.id.trim().to_string();
            channel.skill = channel.skill.trim().to_string();
            channel.worker = channel.worker.trim().to_string();
            if channel.worker.is_empty() {
                channel.worker = default_channel_worker();
            }
            channel.required_env.sort();
            channel.required_env.dedup();
            channel.optional_env.sort();
            channel.optional_env.dedup();
            channel
                .optional_env
                .retain(|key| channel.required_env.binary_search(key).is_err());
            if let Some(contact) = &mut channel.contact {
                contact.conversation_ref = contact
                    .conversation_ref
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
                contact.thread_ref = contact
                    .thread_ref
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct DaemonCompatConfig {
    version: u32,
    workspace_name: String,
    default_runtime_id: Option<String>,
    default_preset_name: Option<String>,
    runtime_auth_context: RuntimeAuthContext,
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
    daemon_compat_fingerprint_with_runtime_context(
        config,
        &RuntimeAuthContext::default(),
        &BTreeMap::new(),
    )
}

pub fn daemon_compat_fingerprint_with_runtime_context(
    config: &OperatorConfig,
    runtime_auth_context: &RuntimeAuthContext,
    runtime_image_identities: &BTreeMap<String, String>,
) -> String {
    let mut normalized = config.clone();
    normalized.normalize();
    let encoded = compatibility_digest_bytes(&DaemonCompatConfig {
        version: 1,
        workspace_name: normalized.daemon.workspace.clone(),
        default_runtime_id: normalized.defaults.runtime.clone(),
        default_preset_name: normalized.defaults.preset.clone(),
        runtime_auth_context: runtime_auth_context.clone(),
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivateContextConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projector_skill: Option<String>,
}

impl PrivateContextConfig {
    fn normalize(&mut self) {
        self.projector_skill = self
            .projector_skill
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub bind_configured: bool,
    #[serde(default = "default_workspace")]
    pub workspace: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            bind_configured: false,
            workspace: default_workspace(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedChannelConfig {
    pub id: String,
    pub skill: String,
    #[serde(default)]
    pub launch_mode: ChannelLaunchMode,
    #[serde(default = "default_channel_worker")]
    pub worker: String,
    #[serde(default)]
    pub required_env: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optional_env: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<ChannelContactConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelContactConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_ref: Option<String>,
}

impl ChannelContactConfig {
    pub fn new(conversation_ref: String, thread_ref: Option<String>) -> Self {
        Self {
            conversation_ref: Some(conversation_ref),
            thread_ref,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChannelLaunchMode {
    #[default]
    Background,
    Interactive,
}

impl ChannelLaunchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Background => "background",
            Self::Interactive => "interactive",
        }
    }
}

impl std::str::FromStr for ChannelLaunchMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim() {
            "background" => Ok(Self::Background),
            "interactive" => Ok(Self::Interactive),
            other => Err(format!(
                "invalid channel launch mode '{other}'; expected 'background' or 'interactive'"
            )),
        }
    }
}

fn legacy_runtime_profile_config_error(content: &str) -> Option<String> {
    let value = toml::from_str::<toml::Value>(content).ok()?;
    let runtimes = value.get("runtimes")?.as_table()?;
    for (runtime_id, runtime) in runtimes {
        let Some(profile) = runtime.as_table() else {
            continue;
        };
        if profile.contains_key("kind") || profile.contains_key("executable") {
            return Some(format!(
                "runtime profile '{runtime_id}' uses the removed pre-#159 config format (kind/executable); update it to driver/command, for example driver = \"acp\" and command = \"opencode\""
            ));
        }
    }
    None
}

pub fn default_bind() -> String {
    "127.0.0.1:8979".to_string()
}

pub fn default_workspace() -> String {
    DEFAULT_WORKSPACE.to_string()
}

pub fn default_channel_worker() -> String {
    crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileConfig {
    pub driver: String,
    #[serde(rename = "command")]
    pub executable: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<RuntimeAuthKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_projection: Option<RuntimeSkillProjectionConfig>,
    #[serde(default, skip_serializing_if = "RuntimeTerminalConfig::is_empty")]
    pub terminal: RuntimeTerminalConfig,
    pub confinement: ConfinementConfig,
}

impl RuntimeProfileConfig {
    pub fn new(
        driver: impl Into<String>,
        executable: impl Into<String>,
        confinement: ConfinementConfig,
    ) -> Self {
        Self {
            driver: driver.into(),
            executable: executable.into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            model: None,
            mode: None,
            auth: None,
            skill_projection: None,
            terminal: RuntimeTerminalConfig::default(),
            confinement,
        }
    }

    pub fn with_auth(mut self, auth: RuntimeAuthKind) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_skill_projection(mut self, projection: RuntimeSkillProjectionConfig) -> Self {
        self.skill_projection = Some(projection);
        self
    }

    pub fn driver(&self) -> &str {
        &self.driver
    }

    pub fn executable(&self) -> &str {
        &self.executable
    }

    pub fn confinement(&self) -> &ConfinementConfig {
        &self.confinement
    }

    pub fn confinement_mut(&mut self) -> &mut ConfinementConfig {
        &mut self.confinement
    }

    pub fn required_runtime_auth(&self) -> Option<RuntimeAuthKind> {
        self.auth.clone()
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
            self.confinement.clone(),
            self.compatibility_base_key(runtime_auth_identity),
            image_identity.map(str::to_string),
            self.required_runtime_auth(),
        )
        .with_skill_projection(self.skill_projection.clone())
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
        // Native terminal argv changes how an operator attaches to an existing
        // runtime state root; it must not repartition that state root.
        normalized.terminal = RuntimeTerminalConfig::default();
        let encoded = compatibility_digest_bytes(&RuntimeCompatConfig {
            version: 1,
            profile: normalized,
            runtime_auth_identity: runtime_auth_identity.map(str::to_string),
        });
        runtime_profile_partition_key(&encoded)
    }

    pub fn validate(&self) -> Result<()> {
        if self.driver.trim().is_empty() {
            return Err(anyhow!("runtime driver is required"));
        }
        validate_runtime_command(self.executable())?;
        if let Some(skill_projection) = &self.skill_projection {
            skill_projection.validate()?;
        }

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
                validate_configured_mounts(&config.additional_mounts, &[])
                    .map_err(anyhow::Error::msg)?;
            }
        }

        Ok(())
    }

    fn normalize(&mut self) {
        self.driver = self.driver.trim().to_string();
        self.executable = self.executable.trim().to_string();
        self.args = self
            .args
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        self.environment = std::mem::take(&mut self.environment)
            .into_iter()
            .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
            .filter(|(key, _)| !key.is_empty())
            .collect();
        self.model = self
            .model
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self.mode = self
            .mode
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(skill_projection) = &mut self.skill_projection {
            skill_projection.normalize();
        }
        normalize_runtime_terminal_config(&mut self.terminal);
        normalize_confinement_config(&mut self.confinement);
    }
}

fn normalize_runtime_terminal_config(config: &mut RuntimeTerminalConfig) {
    config.args = std::mem::take(&mut config.args)
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
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
                mount.target = normalize_runtime_mount_target(&mount.target)
                    .unwrap_or_else(|_| mount.target.trim().to_string());
            }
            oci.additional_mounts.sort_by(|left, right| {
                left.target
                    .cmp(&right.target)
                    .then_with(|| left.source.cmp(&right.source))
            });
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
    let normalized = normalize_host_executable(source)
        .map_err(|err| podman_executable_error(PodmanExecutableContext::Requested, source, err))?;
    ensure_podman_executable(Path::new(&normalized))
        .map_err(|err| podman_executable_error(PodmanExecutableContext::Requested, source, err))?;
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

    let path = Path::new(raw);
    if !path.is_absolute() {
        return Err(anyhow!(
            "host executable '{source}' must be stored as an absolute path"
        ));
    }
    validate_executable_path(path)
}

pub fn validate_podman_executable(source: &str) -> Result<()> {
    let raw = source.trim();
    validate_host_executable(raw)
        .map_err(|err| podman_executable_error(PodmanExecutableContext::Stored, raw, err))?;
    ensure_podman_executable(Path::new(raw))
        .map_err(|err| podman_executable_error(PodmanExecutableContext::Stored, raw, err))
}

#[derive(Debug, Clone, Copy)]
enum PodmanExecutableContext {
    Requested,
    Stored,
}

fn podman_executable_error(
    context: PodmanExecutableContext,
    source: &str,
    err: anyhow::Error,
) -> anyhow::Error {
    anyhow!(podman_executable_error_message(context, source, &err))
}

fn podman_executable_error_message(
    context: PodmanExecutableContext,
    source: &str,
    err: &anyhow::Error,
) -> String {
    let source = source.trim();
    let guidance = podman_executable_repair_note();
    match context {
        PodmanExecutableContext::Requested if looks_like_path(source) => format!(
            "Podman is required for OCI confinement, but {} is not a usable Podman executable: {err}. {guidance}",
            shell_quote_arg(source)
        ),
        PodmanExecutableContext::Requested => format!(
            "Podman is required for OCI confinement, but host executable `{source}` is not available in the environment running LionClaw: {err}. {guidance}"
        ),
        PodmanExecutableContext::Stored => format!(
            "configured Podman engine {} is invalid or unavailable in the environment running LionClaw: {err}. {guidance}",
            shell_quote_arg(source)
        ),
    }
}

pub fn podman_executable_repair_note() -> &'static str {
    "Install Podman or run LionClaw where Podman is available"
}

pub fn podman_executable_inspect_command(source: &str) -> String {
    let source = source.trim();
    if looks_like_path(source) {
        format!("ls -l {}", shell_quote_arg(source))
    } else {
        format!("command -v {}", shell_quote_arg(source))
    }
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
        default_channel_worker, derive_skill_alias, normalize_host_executable,
        normalize_local_source, normalize_podman_executable, normalize_runtime_command,
        podman_executable_inspect_command, podman_executable_repair_note, validate_host_executable,
        validate_podman_executable, ChannelLaunchMode, ManagedChannelConfig, OperatorConfig,
        RuntimeProfileConfig,
    };
    use crate::kernel::runtime::{
        ConfinementConfig, ExecutionPreset, MountAccess, MountSpec, NetworkMode,
        OciConfinementConfig, RuntimeAuthContext, RuntimeTerminalConfig, WorkspaceAccess,
    };

    fn runtime_profile(
        executable: impl Into<String>,
        model: Option<&str>,
        confinement: ConfinementConfig,
    ) -> RuntimeProfileConfig {
        let mut profile = RuntimeProfileConfig::new("codex", executable, confinement);
        profile.model = model.map(str::to_string);
        profile
    }

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
        assert!(config.channels.is_empty());
        assert!(config.runtimes.is_empty());
        assert!(config.presets.is_empty());
        assert!(config.private_context.projector_skill.is_none());
    }

    #[tokio::test]
    async fn legacy_runtime_profile_shape_reports_explicit_break() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        tokio::fs::create_dir_all(home.config_dir())
            .await
            .expect("config dir");
        tokio::fs::write(
            home.config_path(),
            r#"
[runtimes.opencode]
kind = "opencode"
executable = "opencode"
"#,
        )
        .await
        .expect("write config");

        let err = OperatorConfig::load(&home)
            .await
            .expect_err("legacy runtime profile shape should fail clearly");
        let message = err.to_string();
        assert!(message.contains("pre-#159 config format"), "{message}");
        assert!(message.contains("kind/executable"), "{message}");
        assert!(message.contains("driver/command"), "{message}");
    }

    #[tokio::test]
    async fn private_context_projector_skill_is_trimmed_and_optional() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        tokio::fs::create_dir_all(home.config_dir())
            .await
            .expect("config dir");
        tokio::fs::write(
            home.config_path(),
            "[private_context]\nprojector_skill = \" private-context-core \"\n",
        )
        .await
        .expect("write config");

        let config = OperatorConfig::load(&home).await.expect("load config");

        assert_eq!(
            config.private_context.projector_skill.as_deref(),
            Some("private-context-core")
        );
    }

    #[tokio::test]
    async fn legacy_memory_projector_config_does_not_activate_projector() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        tokio::fs::create_dir_all(home.config_dir())
            .await
            .expect("config dir");
        tokio::fs::write(
            home.config_path(),
            "[memory]\nprojector_skill = \"private-context-core\"\n",
        )
        .await
        .expect("write config");

        let config = OperatorConfig::load(&home).await.expect("load config");

        assert!(config.private_context.projector_skill.is_none());
    }

    #[tokio::test]
    async fn channel_launch_mode_defaults_to_background() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "loopback".to_string(),
            skill: "loopback".to_string(),
            launch_mode: ChannelLaunchMode::default(),
            worker: default_channel_worker(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: None,
        });
        config.save(&home).await.expect("save config");

        let loaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(
            loaded.channels[0].launch_mode,
            ChannelLaunchMode::Background
        );
    }

    #[tokio::test]
    async fn channel_launch_mode_round_trips() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "loopback".to_string(),
            skill: "loopback".to_string(),
            launch_mode: ChannelLaunchMode::Interactive,
            worker: default_channel_worker(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: None,
        });
        config.save(&home).await.expect("save config");

        let loaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(loaded.channels.len(), 1);
        assert_eq!(
            loaded.channels[0].launch_mode,
            ChannelLaunchMode::Interactive
        );
    }

    #[tokio::test]
    async fn contact_channel_upsert_clears_previous_contact_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "team-local".to_string(),
            skill: "team-local".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: default_channel_worker(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: Some(super::ChannelContactConfig::new(
                "member:reviewer".to_string(),
                None,
            )),
        });
        config.upsert_channel(ManagedChannelConfig {
            id: "email".to_string(),
            skill: "email".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: default_channel_worker(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: Some(super::ChannelContactConfig::new(
                "reviewer@example.com".to_string(),
                None,
            )),
        });
        config.save(&home).await.expect("save config");

        let loaded = OperatorConfig::load(&home).await.expect("load config");

        assert_eq!(loaded.channels.len(), 2);
        assert!(loaded
            .channels
            .iter()
            .find(|channel| channel.id == "team-local")
            .expect("team-local")
            .contact
            .is_none());
        assert_eq!(
            loaded
                .channels
                .iter()
                .find(|channel| channel.id == "email")
                .expect("email")
                .contact
                .as_ref()
                .and_then(|contact| contact.conversation_ref.as_deref()),
            Some("reviewer@example.com")
        );
    }

    #[tokio::test]
    async fn absent_contact_thread_ref_is_omitted_from_config_toml() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "team-local".to_string(),
            skill: "team-local".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: default_channel_worker(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: Some(super::ChannelContactConfig::new(
                "member:reviewer".to_string(),
                None,
            )),
        });

        config.save(&home).await.expect("save config");
        let content = tokio::fs::read_to_string(home.config_path())
            .await
            .expect("read config");

        assert!(content.contains("conversation_ref = \"member:reviewer\""));
        assert!(!content.contains("thread_ref"));
    }

    #[test]
    fn normalizes_local_source_uri() {
        let absolute = normalize_local_source(".").expect("normalize");
        assert!(absolute.starts_with("local:/"));
    }

    #[test]
    fn bare_podman_engine_is_rejected_for_stored_profiles() {
        let err = validate_podman_executable("podman").expect_err("bare podman engine");

        assert!(err
            .to_string()
            .contains("must be stored as an absolute path"));
    }

    #[test]
    fn relative_podman_engine_is_rejected_for_stored_profiles() {
        let err = validate_podman_executable("./podman").expect_err("relative podman engine");

        assert!(err
            .to_string()
            .contains("must be stored as an absolute path"));
    }

    #[test]
    fn missing_requested_podman_reports_actionable_environment_guidance() {
        let missing = "lionclaw-definitely-missing-podman";
        let err = normalize_podman_executable(missing).expect_err("missing podman");
        let message = err.to_string();

        assert!(message.contains("Podman is required for OCI confinement"));
        assert!(message.contains("environment running LionClaw"));
        assert!(message.contains(podman_executable_repair_note()));
        assert_eq!(
            podman_executable_inspect_command(missing),
            format!("command -v {missing}")
        );
    }

    #[test]
    fn first_runtime_becomes_default() {
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            runtime_profile("codex", None, sample_confinement()),
        );

        assert_eq!(config.defaults.runtime.as_deref(), Some("codex"));
    }

    #[test]
    fn removing_default_runtime_clears_default() {
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            runtime_profile("codex", None, sample_confinement()),
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
        let left = runtime_profile("codex", Some("gpt-5"), sample_confinement());
        let right = runtime_profile("codex", Some("gpt-5.1"), sample_confinement());
        let normalized = runtime_profile(" codex ", Some(" gpt-5 "), sample_confinement());

        assert_ne!(left.compatibility_key(), right.compatibility_key());
        assert_eq!(left.compatibility_key(), normalized.compatibility_key());
    }

    #[test]
    fn runtime_compatibility_key_changes_when_mount_config_changes() {
        let left_confinement = sample_confinement();
        let mut right_confinement = sample_confinement();
        let ConfinementConfig::Oci(oci) = &mut right_confinement;
        oci.additional_mounts.push(MountSpec {
            source: Path::new("/var/tmp/lionclaw-docs").to_path_buf(),
            target: "/mnt/docs".to_string(),
            access: MountAccess::ReadOnly,
        });

        let left = runtime_profile("codex", Some("gpt-5"), left_confinement);
        let right = runtime_profile("codex", Some("gpt-5"), right_confinement);

        assert_ne!(left.compatibility_key(), right.compatibility_key());
    }

    #[test]
    fn runtime_compatibility_key_ignores_native_terminal_profile() {
        let left = RuntimeProfileConfig::new("acp", "opencode", sample_confinement());
        let mut right = left.clone();
        right.terminal = RuntimeTerminalConfig {
            args: vec!["--native".to_string()],
        };

        assert_eq!(left.compatibility_key(), right.compatibility_key());
    }

    #[test]
    fn runtime_compatibility_key_changes_when_image_identity_changes() {
        let runtime = runtime_profile("codex", None, sample_confinement());

        assert_ne!(
            runtime.compatibility_key_with_image_identity(Some("sha256:left")),
            runtime.compatibility_key_with_image_identity(Some("sha256:right"))
        );
    }

    #[test]
    fn runtime_compatibility_key_changes_when_codex_home_identity_changes() {
        let runtime = runtime_profile("codex", None, sample_confinement());

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
    fn daemon_compat_fingerprint_tracks_native_terminal_profile() {
        let left_runtime = RuntimeProfileConfig::new("acp", "opencode", sample_confinement());
        let mut right_runtime = left_runtime.clone();
        right_runtime.terminal = RuntimeTerminalConfig {
            args: vec!["--native".to_string()],
        };
        let mut left = OperatorConfig::default();
        left.upsert_runtime("opencode".to_string(), left_runtime);
        let mut right = OperatorConfig::default();
        right.upsert_runtime("opencode".to_string(), right_runtime);

        assert_ne!(
            daemon_compat_fingerprint(&left),
            daemon_compat_fingerprint(&right)
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_default_runtime_changes() {
        let runtime = runtime_profile("codex", None, sample_confinement());
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
        let runtime = runtime_profile("codex", None, sample_confinement());
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
    fn daemon_compat_fingerprint_changes_when_runtime_auth_context_changes() {
        let runtime = runtime_profile("codex", None, sample_confinement());
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), runtime);
        let left_context = RuntimeAuthContext::new().with_home_override("codex", "/tmp/codex-a");
        let right_context = RuntimeAuthContext::new().with_home_override("codex", "/tmp/codex-b");

        assert_ne!(
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                &left_context,
                &BTreeMap::new(),
            ),
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                &right_context,
                &BTreeMap::new(),
            )
        );
    }

    #[test]
    fn daemon_compat_fingerprint_changes_when_runtime_image_identity_changes() {
        let runtime = runtime_profile("codex", None, sample_confinement());
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), runtime);

        let mut left = BTreeMap::new();
        left.insert("codex".to_string(), "sha256:left".to_string());
        let mut right = BTreeMap::new();
        right.insert("codex".to_string(), "sha256:right".to_string());

        assert_ne!(
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                &RuntimeAuthContext::default(),
                &left
            ),
            daemon_compat_fingerprint_with_runtime_context(
                &config,
                &RuntimeAuthContext::default(),
                &right
            )
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
            .contains("must be stored as an absolute path"));
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
