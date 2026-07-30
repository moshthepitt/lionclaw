//! Data-driven mission runtime profiles. Runtime products are configuration;
//! Rust extension points are conversation drivers, auth providers, and generic
//! confinement mechanisms.

use std::collections::BTreeMap;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use lionclaw_confinement::{ConfinementConfig, ExecutionLimits, OciConfinementConfig};
use lionclaw_runtime_api::{RuntimeTerminalConfig, MAX_RUNTIME_CREDENTIAL_PROJECTIONS};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::mission_type::Home;

const DEFAULT_RUNTIMES_TOML: &str = r#"
[runtimes.codex]
driver = "codex"
command = "codex"
native-resume = true
auth = "codex"
skills-dir = ".agents/skills"
confinement = { backend = "podman", image = "localhost/lionclaw-runtime-dev:v1", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=512m"] }

[runtimes.opencode]
driver = "acp"
command = "opencode"
args = ["acp"]
native-resume = true
terminal = { resume-args = ["--continue"], message-arg = "--prompt" }
environment = { OPENCODE_DISABLE_AUTOUPDATE = "1", OPENCODE_CONFIG_CONTENT = '{"permission":{"*":"allow"}}' }
model = "opencode/big-pickle"
mode = "build"
auth = { kind = "native-home", source = "~/.local/share/opencode", target = ".local/share/opencode", required-files = ["auth.json"] }
skills-dir = ".agents/skills"
confinement = { backend = "podman", image = "localhost/lionclaw-runtime-dev:v1", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=512m"] }

[runtimes.hermes]
driver = "acp"
command = "hermes"
args = ["acp"]
native-resume = true
terminal = { args = ["--tui", "--skills", "lionclaw"], resume-args = ["--continue"] }
environment = { HERMES_HOME = "/runtime/home/.hermes" }
mode = "dont_ask"
auth = { kind = "native-home", source = "~/.hermes", target = ".hermes", required-files = ["config.yaml"], optional-files = [".env", "auth.json", ".anthropic_oauth.json"] }
skills-dir = ".hermes/skills"
confinement = { backend = "podman", image = "localhost/lionclaw-runtime-dev:v1", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=512m"] }
"#;

#[derive(Debug, Clone)]
pub struct MissionRuntimeProfile {
    pub name: String,
    pub driver: String,
    pub command: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub auth: Option<RuntimeAuthConfig>,
    pub skills_dir: Option<RuntimeSkillsDir>,
    pub terminal: RuntimeTerminalConfig,
    /// Whether this profile can retain and reopen a native conversation.
    pub native_resume: bool,
    pub confinement: ConfinementConfig,
}

impl MissionRuntimeProfile {
    /// Stable non-secret identity for native state that may be reopened.
    /// Changing an execution or auth coordinate selects a fresh state scope.
    pub(crate) fn native_state_key(&self, auth_identity: Option<&str>) -> String {
        let mut digest = Sha256::new();
        digest_field(&mut digest, b"name", self.name.as_bytes());
        digest_field(&mut digest, b"driver", self.driver.as_bytes());
        digest_field(&mut digest, b"command", self.command.as_bytes());
        for arg in &self.args {
            digest_field(&mut digest, b"arg", arg.as_bytes());
        }
        let mut environment = self.environment.iter().collect::<Vec<_>>();
        environment.sort();
        for (name, value) in environment {
            digest_field(&mut digest, b"env-name", name.as_bytes());
            digest_field(&mut digest, b"env-value", value.as_bytes());
        }
        digest_optional(&mut digest, b"model", self.model.as_deref());
        digest_optional(&mut digest, b"mode", self.mode.as_deref());
        match &self.skills_dir {
            Some(skills_dir) => digest_field(
                &mut digest,
                b"skills-dir",
                skills_dir.0.as_os_str().as_bytes(),
            ),
            None => digest_field(&mut digest, b"skills-dir", b"<none>"),
        }
        for arg in &self.terminal.args {
            digest_field(&mut digest, b"terminal-arg", arg.as_bytes());
        }
        for arg in &self.terminal.resume_args {
            digest_field(&mut digest, b"terminal-resume-arg", arg.as_bytes());
        }
        digest_optional(
            &mut digest,
            b"terminal-message-arg",
            self.terminal.message_arg.as_deref(),
        );
        digest_field(
            &mut digest,
            b"native-resume",
            if self.native_resume {
                b"true"
            } else {
                b"false"
            },
        );
        match &self.auth {
            None => digest_field(&mut digest, b"auth", b"none"),
            Some(RuntimeAuthConfig::Provider(kind)) => {
                digest_field(&mut digest, b"auth-kind", kind.as_bytes());
            }
            Some(RuntimeAuthConfig::NativeHome(config)) => {
                digest_field(&mut digest, b"auth-kind", b"native-home");
                digest_field(
                    &mut digest,
                    b"auth-source",
                    config.source.as_os_str().as_bytes(),
                );
                digest_field(
                    &mut digest,
                    b"auth-target",
                    config.target.as_os_str().as_bytes(),
                );
                digest_paths(&mut digest, b"auth-required", &config.required_files);
                digest_paths(&mut digest, b"auth-optional", &config.optional_files);
            }
        }
        digest_optional(&mut digest, b"auth-identity", auth_identity);
        digest_optional(
            &mut digest,
            b"image",
            self.confinement.oci().image.as_deref(),
        );
        hex::encode(digest.finalize())
    }
}

fn digest_field(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update(label.len().to_be_bytes());
    digest.update(label);
    digest.update(value.len().to_be_bytes());
    digest.update(value);
}

fn digest_optional(digest: &mut Sha256, label: &[u8], value: Option<&str>) {
    match value {
        Some(value) => digest_field(digest, label, value.as_bytes()),
        None => digest_field(digest, label, b"<none>"),
    }
}

fn digest_paths(digest: &mut Sha256, label: &[u8], paths: &[PathBuf]) {
    let mut paths = paths.iter().collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        left.as_os_str()
            .as_bytes()
            .cmp(right.as_os_str().as_bytes())
    });
    for path in paths {
        digest_field(digest, label, path.as_os_str().as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeAuthConfig {
    Provider(String),
    NativeHome(NativeHomeAuthConfig),
}

impl RuntimeAuthConfig {
    pub fn kind(&self) -> &str {
        match self {
            Self::Provider(kind) => kind,
            Self::NativeHome(_) => "native-home",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeHomeAuthConfig {
    pub source: PathBuf,
    pub target: PathBuf,
    pub required_files: Vec<PathBuf>,
    pub optional_files: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeProfiles {
    profiles: BTreeMap<String, MissionRuntimeProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSkillsDir(PathBuf);

impl RuntimeSkillsDir {
    fn new(path: PathBuf) -> Result<Self> {
        validate_relative_path(&path, "runtime skills-dir")?;
        Ok(Self(path))
    }

    pub(crate) fn relative_skill_path(&self, skill_name: &str) -> Result<PathBuf> {
        lionclaw_confinement::validate_skill_alias(skill_name)?;
        Ok(self.0.join(skill_name))
    }

    pub fn mount_target(&self, skill_name: &str) -> Result<String> {
        Ok(Path::new(lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET)
            .join(self.relative_skill_path(skill_name)?)
            .to_string_lossy()
            .into_owned())
    }
}

impl RuntimeProfiles {
    pub(crate) fn single(profile: MissionRuntimeProfile) -> Self {
        Self {
            profiles: BTreeMap::from([(profile.name.clone(), profile)]),
        }
    }

    pub fn built_in() -> Result<Self> {
        let user_home = user_home_from_env()
            .ok_or_else(|| anyhow!("HOME is required to resolve built-in runtime auth paths"))?;
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

    pub(crate) fn instrument_identities(
        &self,
    ) -> BTreeMap<String, crate::model::RuntimeInstrumentIdentity> {
        self.profiles
            .iter()
            .map(|(name, profile)| {
                (
                    name.clone(),
                    crate::model::RuntimeInstrumentIdentity {
                        runtime: profile.name.clone(),
                        model: profile.model.clone(),
                        mode: profile.mode.clone(),
                    },
                )
            })
            .collect()
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
    mode: Option<String>,
    #[serde(default)]
    auth: Option<RuntimeAuthConfigFile>,
    #[serde(default)]
    skills_dir: Option<PathBuf>,
    #[serde(default)]
    terminal: RuntimeTerminalConfig,
    #[serde(default)]
    native_resume: bool,
    #[serde(default = "default_confinement")]
    confinement: ConfinementConfig,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RuntimeAuthConfigFile {
    Provider(String),
    Structured(StructuredRuntimeAuthConfigFile),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum StructuredRuntimeAuthConfigFile {
    NativeHome {
        source: PathBuf,
        target: PathBuf,
        #[serde(default, rename = "required-files")]
        required_files: Vec<PathBuf>,
        #[serde(default, rename = "optional-files")]
        optional_files: Vec<PathBuf>,
    },
}

impl RuntimeProfileFile {
    fn apply(mut self, name: String, user_home: Option<&Path>) -> Result<MissionRuntimeProfile> {
        self.driver = required_trimmed("driver", self.driver)?;
        self.command = required_trimmed("command", self.command)?;
        self.model = self
            .model
            .map(|value| runtime_selection("model", value))
            .transpose()?;
        self.mode = self
            .mode
            .map(|value| runtime_selection("mode", value))
            .transpose()?;
        let auth = self
            .auth
            .map(|config| config.apply(user_home))
            .transpose()?;
        let skills_dir = self.skills_dir.map(RuntimeSkillsDir::new).transpose()?;
        self.terminal.validate()?;
        self.confinement.oci_mut().tmpfs = self
            .confinement
            .oci()
            .tmpfs
            .iter()
            .map(|entry| {
                lionclaw_confinement::parse_runtime_tmpfs_entry(entry)
                    .map(lionclaw_confinement::RuntimeTmpfsEntry::into_argument)
                    .map_err(anyhow::Error::msg)
            })
            .collect::<Result<Vec<_>>>()?;
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
            mode: self.mode,
            auth,
            skills_dir,
            terminal: self.terminal,
            native_resume: self.native_resume,
            confinement: self.confinement,
        })
    }
}

impl RuntimeAuthConfigFile {
    fn apply(self, user_home: Option<&Path>) -> Result<RuntimeAuthConfig> {
        match self {
            Self::Provider(kind) => {
                Ok(RuntimeAuthConfig::Provider(required_trimmed("auth", kind)?))
            }
            Self::Structured(StructuredRuntimeAuthConfigFile::NativeHome {
                source,
                target,
                required_files,
                optional_files,
            }) => {
                let declared_files = required_files
                    .len()
                    .checked_add(optional_files.len())
                    .ok_or_else(|| anyhow!("native-home auth credential file count overflowed"))?;
                if declared_files > MAX_RUNTIME_CREDENTIAL_PROJECTIONS {
                    return Err(anyhow!(
                        "native-home auth declares {declared_files} credential files; limit is {MAX_RUNTIME_CREDENTIAL_PROJECTIONS}"
                    ));
                }
                let source = expand_home(&source, user_home, "native-home auth source")?;
                validate_relative_path(&target, "native-home auth target")?;
                if required_files.is_empty() && optional_files.is_empty() {
                    return Err(anyhow!(
                        "native-home auth must declare at least one required or optional file"
                    ));
                }
                let mut paths = std::collections::BTreeSet::new();
                for path in required_files.iter().chain(&optional_files) {
                    validate_relative_path(path, "native-home auth file")?;
                    if !paths.insert(path.clone()) {
                        return Err(anyhow!(
                            "native-home auth file '{}' is declared more than once",
                            path.display()
                        ));
                    }
                }
                Ok(RuntimeAuthConfig::NativeHome(NativeHomeAuthConfig {
                    source,
                    target,
                    required_files,
                    optional_files,
                }))
            }
        }
    }
}

fn required_trimmed(label: &str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("runtime {label} is required"));
    }
    Ok(trimmed.to_string())
}

fn runtime_selection(label: &str, value: String) -> Result<String> {
    let value = required_trimmed(label, value)?;
    if value.len() > lionclaw_runtime_api::FAILURE_TEXT_LIMIT {
        return Err(anyhow!(
            "runtime {label} exceeds the {} byte evidence limit",
            lionclaw_runtime_api::FAILURE_TEXT_LIMIT
        ));
    }
    Ok(value)
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

fn expand_home(path: &Path, home: Option<&Path>, label: &str) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let text = path.to_string_lossy();
    if text == "~" {
        return home
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow!("HOME is required to resolve {label} '~'"));
    }
    if let Some(relative) = text.strip_prefix("~/") {
        return home
            .map(|home| home.join(relative))
            .ok_or_else(|| anyhow!("HOME is required to resolve {label} '{}'", path.display()));
    }
    Err(anyhow!(
        "{label} '{}' must be absolute or start with '~/'",
        path.display()
    ))
}

fn validate_relative_path(path: &Path, label: &str) -> Result<()> {
    use std::path::Component;

    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(anyhow!(
            "{label} '{}' must be a clean non-empty relative path without traversal",
            path.display()
        ));
    }
    Ok(())
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
            skills-dir = ".agents/skills"
            "#,
            Path::new("/home/alice"),
        )
        .expect("valid profiles");

        let profile = profiles.get("hermes").expect("hermes profile");
        assert_eq!(profile.driver, "acp");
        assert_eq!(profile.command, "hermes");
        assert_eq!(profile.args, ["acp"]);
        assert_eq!(
            profile
                .skills_dir
                .expect("skills dir")
                .mount_target("fixture")
                .unwrap(),
            "/runtime/home/.agents/skills/fixture"
        );
    }

    #[test]
    fn acp_session_mode_is_runtime_profile_data() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.example]
            driver = "acp"
            command = "example"
            mode = "autonomous"
            "#,
            Path::new("/home/alice"),
        )
        .expect("profile");

        assert_eq!(
            profiles.get("example").unwrap().mode.as_deref(),
            Some("autonomous")
        );
    }

    #[test]
    fn native_resume_is_profile_declared_and_defaults_to_reconstruction() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.native]
            driver = "acp"
            command = "native"
            native-resume = true

            [runtimes.reconstructed]
            driver = "acp"
            command = "reconstructed"
            "#,
            Path::new("/home/alice"),
        )
        .expect("profiles");

        assert!(profiles.get("native").unwrap().native_resume);
        assert!(!profiles.get("reconstructed").unwrap().native_resume);
    }

    #[test]
    fn requested_runtime_selections_must_fit_the_evidence_contract() {
        let oversized = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
        for field in ["model", "mode"] {
            let error = RuntimeProfiles::from_toml(
                &format!(
                    "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\n{field} = \"{oversized}\"\n"
                ),
                Path::new("/home/alice"),
            )
            .expect_err("oversized selection must fail at profile load");
            assert!(
                error.to_string().contains(field),
                "unexpected error: {error:#}"
            );
        }

        let maximum = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
        let profiles = RuntimeProfiles::from_toml(
            &format!(
                "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\nmodel = \"{maximum}\"\nmode = \"{maximum}\"\n"
            ),
            Path::new("/home/alice"),
        )
        .expect("exact evidence boundary is representable");
        let profile = profiles.get("example").unwrap();
        assert_eq!(profile.model.as_deref(), Some(maximum.as_str()));
        assert_eq!(profile.mode.as_deref(), Some(maximum.as_str()));
    }

    #[test]
    fn native_home_auth_is_runtime_profile_data() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.example]
            driver = "acp"
            command = "example-agent"
            auth = { kind = "native-home", source = "~/.example-agent", target = ".example-agent", required-files = ["config.toml"], optional-files = ["auth.json", ".env"] }
            "#,
            Path::new("/home/alice"),
        )
        .expect("valid native-home auth");

        let profile = profiles.get("example").expect("example profile");
        let RuntimeAuthConfig::NativeHome(config) = profile.auth.expect("auth config") else {
            panic!("expected native-home auth");
        };
        assert_eq!(config.source, PathBuf::from("/home/alice/.example-agent"));
        assert_eq!(config.target, PathBuf::from(".example-agent"));
        assert_eq!(config.required_files, [PathBuf::from("config.toml")]);
        assert_eq!(
            config.optional_files,
            [PathBuf::from("auth.json"), PathBuf::from(".env")]
        );
    }

    #[test]
    fn native_home_auth_enforces_the_declared_credential_count_boundary() {
        let declared_files = |count: usize| {
            (0..count)
                .map(|index| format!(r#""credential-{index}""#))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let profile = |count: usize| {
            format!(
                "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\nauth = {{ kind = \"native-home\", source = \"~/.example\", target = \".example\", required-files = [{}] }}\n",
                declared_files(count)
            )
        };

        RuntimeProfiles::from_toml(
            &profile(MAX_RUNTIME_CREDENTIAL_PROJECTIONS),
            Path::new("/home/alice"),
        )
        .expect("the exact credential projection count limit must be accepted");

        let error = RuntimeProfiles::from_toml(
            &profile(MAX_RUNTIME_CREDENTIAL_PROJECTIONS + 1),
            Path::new("/home/alice"),
        )
        .expect_err("a native-home declaration above the projection limit must fail");
        assert!(
            error.to_string().contains(&format!(
                "declares {} credential files; limit is {}",
                MAX_RUNTIME_CREDENTIAL_PROJECTIONS + 1,
                MAX_RUNTIME_CREDENTIAL_PROJECTIONS
            )),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn native_home_auth_rejects_paths_outside_its_roots() {
        for auth in [
            r#"{ kind = "native-home", source = "~/.example", target = "../escape", required-files = ["config.toml"] }"#,
            r#"{ kind = "native-home", source = "~/.example", target = ".example", required-files = ["../secret"] }"#,
            r#"{ kind = "native-home", source = "relative", target = ".example", required-files = ["config.toml"] }"#,
        ] {
            let err = RuntimeProfiles::from_toml(
                &format!(
                    "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\nauth = {auth}\n"
                ),
                Path::new("/home/alice"),
            )
            .expect_err("unsafe native-home path");
            assert!(
                err.to_string().contains("native-home"),
                "unexpected error: {err:#}"
            );
        }
    }

    #[test]
    fn built_in_profiles_use_the_same_toml_loader() {
        let profiles = RuntimeProfiles::from_toml(DEFAULT_RUNTIMES_TOML, Path::new("/home/alice"))
            .expect("built-in profiles");
        assert_eq!(
            profiles.names().collect::<Vec<_>>(),
            ["codex", "hermes", "opencode"]
        );
        assert_eq!(profiles.get("codex").unwrap().driver, "codex");
        let hermes = profiles.get("hermes").unwrap();
        assert_eq!(hermes.driver, "acp");
        assert_eq!(hermes.mode.as_deref(), Some("dont_ask"));
        assert!(hermes.native_resume);
        assert_eq!(hermes.terminal.args, ["--tui", "--skills", "lionclaw"]);
        assert_eq!(hermes.terminal.resume_args, ["--continue"]);
        let opencode = profiles.get("opencode").unwrap();
        assert_eq!(opencode.driver, "acp");
        assert_eq!(opencode.model.as_deref(), Some("opencode/big-pickle"));
        assert_eq!(opencode.mode.as_deref(), Some("build"));
        assert!(opencode.native_resume);
        assert_eq!(opencode.terminal.resume_args, ["--continue"]);
        assert_eq!(opencode.terminal.message_arg.as_deref(), Some("--prompt"));
        assert!(opencode.environment.contains(&(
            "OPENCODE_CONFIG_CONTENT".to_string(),
            r#"{"permission":{"*":"allow"}}"#.to_string(),
        )));
        assert_eq!(
            opencode.auth,
            Some(RuntimeAuthConfig::NativeHome(NativeHomeAuthConfig {
                source: PathBuf::from("/home/alice/.local/share/opencode"),
                target: PathBuf::from(".local/share/opencode"),
                required_files: vec![PathBuf::from("auth.json")],
                optional_files: Vec::new(),
            }))
        );
    }

    #[test]
    fn invalid_skills_dir_is_rejected_while_loading_configuration() {
        for path in ["../escape", "./skills", "/absolute"] {
            let err = RuntimeProfiles::from_toml(
                &format!(
                    "[runtimes.bad]\ndriver = \"acp\"\ncommand = \"bad\"\nskills-dir = \"{path}\"\n"
                ),
                Path::new("/home/alice"),
            )
            .expect_err("invalid root");
            assert!(err.to_string().contains("traversal"), "got {err:#}");
        }
    }

    #[test]
    fn skills_dir_does_not_depend_on_the_host_home() {
        let profiles = RuntimeProfiles::from_toml_with_home(
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            skills-dir = ".custom/skills"
            "#,
            None,
        )
        .expect("native skill paths do not need HOME");
        assert_eq!(profiles.get("custom").unwrap().driver, "acp");

        let err = RuntimeProfiles::from_toml_with_home(
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            auth = { kind = "native-home", source = "~/.custom", target = ".custom", required-files = ["config.toml"] }
            "#,
            None,
        )
        .expect_err("native-home auth requires HOME");
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
                  {{ source = {:?}, target = "/lionclaw/internal", access = "read-only" }}
                ] }}
                "#,
                source.path()
            ),
            Path::new("/home/alice"),
        )
        .expect_err("reserved additional mount");
        assert!(err.to_string().contains("reserved runtime path"));
    }

    #[test]
    fn nested_runtime_configuration_rejects_unknown_fields() {
        for invalid in [
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            confinement = { backend = "podman", read-only-rootf = true }
            "#,
            r#"
            [runtimes.custom]
            driver = "acp"
            command = "custom"
            skills-dirr = ".agents/skills"
            "#,
        ] {
            let err = RuntimeProfiles::from_toml(invalid, Path::new("/home/alice"))
                .expect_err("unknown nested field");
            assert!(format!("{err:#}").contains("unknown field"), "got {err:#}");
        }
    }

    #[test]
    fn native_state_key_tracks_effective_reopen_compatibility() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.example]
            driver = "acp"
            command = "agent-a"
            args = ["serve"]
            environment = { B = "two", A = "one" }
            native-resume = true
            auth = { kind = "native-home", source = "/auth/a", target = ".agent", required-files = ["auth.json", "config.json"], optional-files = ["token.json", "oauth.json"] }
            "#,
            Path::new("/home/alice"),
        )
        .unwrap();
        let original = profiles.get("example").unwrap();
        let mut changed = original.clone();
        changed.command = "agent-b".to_string();

        assert_eq!(original.native_state_key(None).len(), 64);
        assert_ne!(
            original.native_state_key(None),
            changed.native_state_key(None)
        );

        changed = original.clone();
        let Some(RuntimeAuthConfig::NativeHome(auth)) = changed.auth.as_mut() else {
            panic!("native-home auth");
        };
        auth.source = PathBuf::from("/auth/b");
        assert_ne!(
            original.native_state_key(None),
            changed.native_state_key(None)
        );

        changed = original.clone();
        changed.environment.reverse();
        assert_eq!(
            original.native_state_key(None),
            changed.native_state_key(None)
        );

        changed = original.clone();
        changed.skills_dir = Some(RuntimeSkillsDir::new(PathBuf::from(".agent/skills")).unwrap());
        assert_ne!(
            original.native_state_key(None),
            changed.native_state_key(None)
        );

        changed = original.clone();
        let Some(RuntimeAuthConfig::NativeHome(auth)) = changed.auth.as_mut() else {
            panic!("native-home auth");
        };
        auth.required_files.reverse();
        auth.optional_files.reverse();
        assert_eq!(
            original.native_state_key(None),
            changed.native_state_key(None)
        );
        assert_ne!(
            original.native_state_key(Some("principal:a")),
            original.native_state_key(Some("principal:b"))
        );
    }
}
