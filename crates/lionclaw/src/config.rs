//! Data-driven mission runtime profiles. Runtime products are configuration;
//! Rust extension points are conversation drivers, auth providers, and generic
//! confinement/projection mechanisms.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use lionclaw_confinement::{
    ConfinementConfig, ExecutionLimits, OciConfinementConfig, RuntimeSkillProjectionConfig,
};
use serde::Deserialize;

use crate::mission_type::Home;

const DEFAULT_HARD_TIMEOUT_SECS: u64 = 30 * 60;
const DEFAULT_ORACLE_TIMEOUT_SECS: u64 = 15 * 60;

const DEFAULT_RUNTIMES_TOML: &str = r#"
[runtimes.codex]
driver = "codex"
command = "codex"
auth = "codex"
skill-projection = { kind = "native-dir", root = ".agents/skills", format = "skill-md", inherit = [
  { source = "~/.agents/skills", target = ".agents/skills", optional = true },
  { source = "~/.codex/skills", target = ".codex/skills", optional = true },
] }
confinement = { backend = "podman", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=512m"] }

[runtimes.opencode]
driver = "acp"
command = "opencode"
args = ["acp"]
environment = { OPENCODE_DISABLE_AUTOUPDATE = "1" }
skill-projection = { kind = "native-dir", root = ".agents/skills", format = "skill-md", inherit = [
  { source = "~/.agents/skills", target = ".agents/skills", optional = true },
  { source = "~/.config/opencode/skills", target = ".config/opencode/skills", optional = true },
] }
confinement = { backend = "podman", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=512m"] }
"#;

#[derive(Debug, Clone)]
pub struct MissionRuntimeProfile {
    pub name: String,
    pub driver: String,
    pub command: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub auth: Option<String>,
    pub skill_projection: Option<RuntimeSkillProjectionConfig>,
    pub confinement: ConfinementConfig,
    pub hard_timeout: Duration,
    pub oracle_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct RuntimeProfiles {
    profiles: BTreeMap<String, MissionRuntimeProfile>,
}

impl RuntimeProfiles {
    pub fn built_in() -> Result<Self> {
        Self::from_toml(DEFAULT_RUNTIMES_TOML, &user_home_from_env()?)
            .context("invalid built-in runtime configuration")
    }

    pub fn load(home: &Home) -> Result<Self> {
        let path = home.runtimes_file();
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::from_toml(&text, &user_home_from_env()?)
                .with_context(|| format!("invalid runtime configuration '{}'", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Self::built_in(),
            Err(err) => Err(err).with_context(|| format!("reading '{}'", path.display())),
        }
    }

    pub fn from_toml(text: &str, user_home: &Path) -> Result<Self> {
        let file: RuntimeProfilesFile = toml::from_str(text).context("invalid runtimes TOML")?;
        let mut profiles = BTreeMap::new();
        for (name, config) in file.runtimes {
            validate_runtime_name(&name)?;
            let profile = config.apply(name.clone(), user_home)?;
            profiles.insert(name, profile);
        }
        if profiles.is_empty() {
            return Err(anyhow!("runtime configuration has no profiles"));
        }
        Ok(Self { profiles })
    }

    pub fn get(&self, name: &str) -> Result<MissionRuntimeProfile> {
        self.profiles.get(name).cloned().ok_or_else(|| {
            anyhow!(
                "unknown runtime '{name}' (configured: {})",
                self.profiles.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.profiles.keys().map(String::as_str)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeProfilesFile {
    runtimes: BTreeMap<String, RuntimeProfileFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RuntimeProfileFile {
    driver: String,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    environment: BTreeMap<String, String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    auth: Option<String>,
    #[serde(default)]
    skill_projection: Option<RuntimeSkillProjectionConfig>,
    #[serde(default = "default_confinement")]
    confinement: ConfinementConfig,
    #[serde(default = "default_hard_timeout_secs")]
    hard_timeout_secs: u64,
    #[serde(default = "default_oracle_timeout_secs")]
    oracle_timeout_secs: u64,
}

impl RuntimeProfileFile {
    fn apply(mut self, name: String, user_home: &Path) -> Result<MissionRuntimeProfile> {
        self.driver = required_trimmed("driver", self.driver)?;
        self.command = required_trimmed("command", self.command)?;
        self.auth = self
            .auth
            .map(|value| required_trimmed("auth", value))
            .transpose()?;
        if self.hard_timeout_secs == 0 || self.oracle_timeout_secs == 0 {
            return Err(anyhow!("runtime timeouts must be greater than zero"));
        }
        if let Some(projection) = &mut self.skill_projection {
            for inherited in projection.inherited_roots_mut() {
                inherited.source = expand_home(&inherited.source, user_home)?;
            }
            projection.normalize();
            projection.validate()?;
        }
        Ok(MissionRuntimeProfile {
            name,
            driver: self.driver,
            command: self.command,
            args: self.args,
            environment: self.environment.into_iter().collect(),
            model: self.model,
            auth: self.auth,
            skill_projection: self.skill_projection,
            confinement: self.confinement,
            hard_timeout: Duration::from_secs(self.hard_timeout_secs),
            oracle_timeout: Duration::from_secs(self.oracle_timeout_secs),
        })
    }
}

fn required_trimmed(label: &str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("runtime {label} is required"));
    }
    Ok(trimmed.to_string())
}

fn validate_runtime_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    if !chars.next().is_some_and(|ch| ch.is_ascii_lowercase())
        || !chars
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_'))
    {
        return Err(anyhow!(
            "runtime name '{name}' must start lowercase and contain only lowercase ASCII letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

fn expand_home(path: &Path, home: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let text = path.to_string_lossy();
    if text == "~" {
        return Ok(home.to_path_buf());
    }
    if let Some(relative) = text.strip_prefix("~/") {
        return Ok(home.join(relative));
    }
    Err(anyhow!(
        "inherited skill source '{}' must be absolute or start with '~/'",
        path.display()
    ))
}

fn user_home_from_env() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is required to resolve runtime skill roots"))
}

fn default_confinement() -> ConfinementConfig {
    ConfinementConfig::Oci(OciConfinementConfig {
        read_only_rootfs: true,
        tmpfs: vec!["/tmp:rw,size=512m".to_string()],
        limits: ExecutionLimits::default(),
        ..OciConfinementConfig::default()
    })
}

const fn default_hard_timeout_secs() -> u64 {
    DEFAULT_HARD_TIMEOUT_SECS
}

const fn default_oracle_timeout_secs() -> u64 {
    DEFAULT_ORACLE_TIMEOUT_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_acp_runtime_is_profile_data() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.hermes]
            driver = "acp"
            command = "hermes"
            args = ["acp"]
            skill-projection = { kind = "native-dir", root = ".agents/skills", inherit = [
              { source = "~/.hermes/skills", target = ".hermes/skills", optional = true }
            ] }
            "#,
            Path::new("/home/alice"),
        )
        .expect("valid profiles");

        let profile = profiles.get("hermes").expect("hermes profile");
        assert_eq!(profile.driver, "acp");
        assert_eq!(profile.command, "hermes");
        assert_eq!(profile.args, ["acp"]);
        let projection = profile.skill_projection.expect("skill projection");
        assert_eq!(projection.native_dir_root(), ".agents/skills");
        assert_eq!(
            projection.inherited_roots()[0].source,
            PathBuf::from("/home/alice/.hermes/skills")
        );
    }

    #[test]
    fn built_in_profiles_use_the_same_toml_loader() {
        let profiles = RuntimeProfiles::from_toml(DEFAULT_RUNTIMES_TOML, Path::new("/home/alice"))
            .expect("built-in profiles");
        assert_eq!(profiles.names().collect::<Vec<_>>(), ["codex", "opencode"]);
        assert_eq!(profiles.get("codex").unwrap().driver, "codex");
        assert_eq!(profiles.get("opencode").unwrap().driver, "acp");
    }

    #[test]
    fn invalid_projection_is_rejected_while_loading_configuration() {
        let err = RuntimeProfiles::from_toml(
            r#"
            [runtimes.bad]
            driver = "acp"
            command = "bad"
            skill-projection = { kind = "native-dir", root = "../escape" }
            "#,
            Path::new("/home/alice"),
        )
        .expect_err("invalid root");
        assert!(err.to_string().contains("traversal"), "got {err:#}");
    }
}
