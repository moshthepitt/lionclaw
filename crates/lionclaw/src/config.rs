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
        let user_home = user_home_from_env()
            .ok_or_else(|| anyhow!("HOME is required to resolve built-in runtime skill roots"))?;
        Self::from_toml(DEFAULT_RUNTIMES_TOML, &user_home)
            .context("invalid built-in runtime configuration")
    }

    pub fn load(home: &Home) -> Result<Self> {
        let path = home.runtimes_file();
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::from_toml_with_home(&text, user_home_from_env().as_deref())
                .with_context(|| format!("invalid runtime configuration '{}'", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Self::built_in(),
            Err(err) => Err(err).with_context(|| format!("reading '{}'", path.display())),
        }
    }

    pub fn from_toml(text: &str, user_home: &Path) -> Result<Self> {
        Self::from_toml_with_home(text, Some(user_home))
    }

    fn from_toml_with_home(text: &str, user_home: Option<&Path>) -> Result<Self> {
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
    fn apply(mut self, name: String, user_home: Option<&Path>) -> Result<MissionRuntimeProfile> {
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
        lionclaw_confinement::mount_validation::validate_configured_mounts(
            &self.confinement.oci().additional_mounts,
            &[],
        )
        .map_err(anyhow::Error::msg)?;
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

fn expand_home(path: &Path, home: Option<&Path>) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let text = path.to_string_lossy();
    if text == "~" {
        return home
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow!("HOME is required to resolve inherited skill source '~'"));
    }
    if let Some(relative) = text.strip_prefix("~/") {
        return home.map(|home| home.join(relative)).ok_or_else(|| {
            anyhow!(
                "HOME is required to resolve inherited skill source '{}'",
                path.display()
            )
        });
    }
    Err(anyhow!(
        "inherited skill source '{}' must be absolute or start with '~/'",
        path.display()
    ))
}

fn user_home_from_env() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
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

    #[test]
    fn custom_profiles_need_home_only_when_they_use_tilde_paths() {
        let absolute = RuntimeProfiles::from_toml_with_home(
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            skill-projection = { kind = "native-dir", root = ".agents/skills", inherit = [
              { source = "/opt/custom/skills", target = ".custom/skills", optional = true }
            ] }
            "#,
            None,
        )
        .expect("absolute roots do not need HOME");
        assert_eq!(absolute.get("custom").unwrap().driver, "acp");

        let err = RuntimeProfiles::from_toml_with_home(
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            skill-projection = { kind = "native-dir", root = ".agents/skills", inherit = [
              { source = "~/.custom/skills", target = ".custom/skills", optional = true }
            ] }
            "#,
            None,
        )
        .expect_err("tilde roots require HOME");
        assert!(err.to_string().contains("HOME is required"), "got {err:#}");
    }

    #[test]
    fn configured_additional_mounts_are_validated_and_retained() {
        let source = tempfile::tempdir().unwrap();
        let profiles = RuntimeProfiles::from_toml(
            &format!(
                r#"
                [runtimes.custom]
                driver = "acp"
                command = "custom"
                confinement = {{ backend = "podman", additional-mounts = [
                  {{ source = {:?}, target = "/opt/custom", access = "read-only" }}
                ] }}
                "#,
                source.path()
            ),
            Path::new("/home/alice"),
        )
        .expect("valid additional mount");
        assert_eq!(
            profiles
                .get("custom")
                .unwrap()
                .confinement
                .oci()
                .additional_mounts[0]
                .target,
            "/opt/custom"
        );

        let err = RuntimeProfiles::from_toml(
            &format!(
                r#"
                [runtimes.custom]
                driver = "acp"
                command = "custom"
                confinement = {{ backend = "podman", additional-mounts = [
                  {{ source = {:?}, target = "/lionclaw/skills/shadow", access = "read-only" }}
                ] }}
                "#,
                source.path()
            ),
            Path::new("/home/alice"),
        )
        .expect_err("reserved additional mount");
        assert!(err.to_string().contains("reserved runtime path"));
    }
}
