use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::{LionClawHome, DEFAULT_WORKSPACE};
use crate::kernel::runtime::{ConfinementConfig, ExecutionPreset};
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
            return Err(anyhow!("runtime profile '{}' is not configured", id));
        }
        self.defaults.runtime = Some(id.to_string());
        self.normalize();
        Ok(())
    }

    pub fn set_default_preset(&mut self, id: &str) -> Result<()> {
        if !self.presets.contains_key(id) {
            return Err(anyhow!("preset '{}' is not configured", id));
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
        self.skills
            .sort_by(|left, right| left.alias.cmp(&right.alias));
        self.channels.sort_by(|left, right| left.id.cmp(&right.id));
        for channel in &mut self.channels {
            channel.required_env.sort();
            channel.required_env.dedup();
        }
    }
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
                "invalid channel launch mode '{}'; expected 'service' or 'interactive'",
                other
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

fn default_opencode_format() -> String {
    "json".to_string()
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
        #[serde(default)]
        confinement: Option<ConfinementConfig>,
    },
    #[serde(rename = "opencode")]
    OpenCode {
        executable: String,
        #[serde(default = "default_opencode_format")]
        format: String,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        xdg_data_home: Option<String>,
        #[serde(default)]
        continue_last_session: bool,
        #[serde(default)]
        confinement: Option<ConfinementConfig>,
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

    pub fn confinement(&self) -> Option<&ConfinementConfig> {
        match self {
            Self::Codex { confinement, .. } | Self::OpenCode { confinement, .. } => {
                confinement.as_ref()
            }
        }
    }
}

fn normalize_execution_preset(preset: &mut ExecutionPreset) {
    preset.secret_bindings = std::mem::take(&mut preset.secret_bindings)
        .into_iter()
        .filter_map(|mut binding| {
            binding.name = binding.name.trim().to_string();
            binding.target_env = binding
                .target_env
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            if binding.name.is_empty() {
                return None;
            }
            Some(binding)
        })
        .collect();
    preset.secret_bindings.sort();
    preset.secret_bindings.dedup();
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
        .with_context(|| format!("failed to resolve source '{}'", source))?;
    Ok(format!("local:{}", absolute.display()))
}

pub fn normalize_executable(source: &str) -> Result<String> {
    let raw = source.trim();
    if raw.is_empty() {
        return Err(anyhow!("runtime command or path cannot be empty"));
    }

    if looks_like_path(raw) {
        let path = normalize_executable_path(raw)?;
        validate_executable_path(&path)?;
        return Ok(path.to_string_lossy().to_string());
    }

    let resolved = which::which(raw)
        .with_context(|| format!("failed to resolve runtime command '{}'", source))?;
    validate_executable_path(&resolved)?;
    Ok(raw.to_string())
}

pub fn validate_executable_path(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Err(anyhow!(
            "runtime executable '{}' is not a file",
            path.display()
        ));
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
                "runtime executable '{}' is not marked executable",
                path.display()
            ));
        }
    }

    Ok(())
}

pub fn validate_executable(source: &str) -> Result<()> {
    let raw = source.trim();
    if raw.is_empty() {
        return Err(anyhow!("runtime command or path cannot be empty"));
    }

    if looks_like_path(raw) {
        return validate_executable_path(&normalize_executable_path(raw)?);
    }

    let resolved = which::which(raw)
        .with_context(|| format!("failed to resolve runtime command '{}'", source))?;
    validate_executable_path(&resolved)
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

#[cfg(test)]
mod tests {
    use super::{
        derive_skill_alias, normalize_executable, normalize_local_source, validate_executable,
        ChannelLaunchMode, ManagedChannelConfig, OperatorConfig, RuntimeProfileConfig,
    };
    use crate::kernel::runtime::{
        ExecutionPreset, NetworkMode, SecretBinding, SecretBindingKind, WorkspaceAccess,
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
                executable: "/tmp/codex".to_string(),
                model: None,
                confinement: None,
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
                executable: "/tmp/codex".to_string(),
                model: None,
                confinement: None,
            },
        );

        assert!(config.remove_runtime("codex"));
        assert!(config.defaults.runtime.is_none());
    }

    #[test]
    fn first_preset_becomes_default() {
        let mut config = OperatorConfig::default();
        config.upsert_preset(
            "everyday".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                secret_bindings: Vec::new(),
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
                secret_bindings: Vec::new(),
                escape_classes: Default::default(),
            },
        );

        assert!(config.remove_preset("everyday"));
        assert!(config.defaults.preset.is_none());
    }

    #[test]
    fn preset_normalization_trims_keys_and_dedupes_secret_bindings() {
        let mut config = OperatorConfig::default();
        config.upsert_preset(
            "  everyday  ".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                secret_bindings: vec![
                    SecretBinding {
                        name: "github".to_string(),
                        kind: SecretBindingKind::LaunchEnv,
                        target_env: Some("GITHUB_TOKEN".to_string()),
                    },
                    SecretBinding {
                        name: "github".to_string(),
                        kind: SecretBindingKind::LaunchEnv,
                        target_env: Some("GITHUB_TOKEN".to_string()),
                    },
                ],
                escape_classes: Default::default(),
            },
        );

        assert!(config.preset("  everyday  ").is_none());
        let preset = config.preset("everyday").expect("normalized preset");
        assert_eq!(preset.secret_bindings.len(), 1);
    }

    #[test]
    fn legacy_codex_runtime_fields_are_ignored_on_load() {
        let config: OperatorConfig = toml::from_str(
            r#"
                [runtimes.codex]
                kind = "codex"
                executable = "codex"
                model = "gpt-5-codex"
                sandbox = "danger-full-access"
                skip_git_repo_check = false
                ephemeral = false
            "#,
        )
        .expect("parse config");

        let runtime = config.runtime("codex").expect("codex runtime");
        match runtime {
            RuntimeProfileConfig::Codex {
                executable,
                model,
                confinement,
            } => {
                assert_eq!(executable, "codex");
                assert_eq!(model.as_deref(), Some("gpt-5-codex"));
                assert!(confinement.is_none());
            }
            other => panic!("unexpected runtime profile: {}", other.kind()),
        }
    }

    #[cfg(unix)]
    #[test]
    fn normalize_executable_rejects_non_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("not-executable");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

        let err = normalize_executable(path.to_str().expect("path utf8")).expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn normalize_executable_keeps_bare_command_names() {
        let normalized = normalize_executable("sh").expect("normalize");

        assert_eq!(normalized, "sh");
    }

    #[cfg(unix)]
    #[test]
    fn validate_executable_resolves_bare_commands_via_path() {
        validate_executable("sh").expect("bare command should validate");
    }
}
