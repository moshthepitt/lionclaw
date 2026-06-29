use anyhow::{anyhow, bail, Result};

use crate::kernel::runtime::{ConfinementConfig, OciConfinementConfig};

use super::config::{normalize_podman_executable, OperatorConfig, RuntimeProfileConfig};

pub const CODEX_RUNTIME_ID: &str = "codex";
pub const CODEX_RUNTIME_DRIVER: &str = "codex";
pub const CODEX_DEFAULT_EXECUTABLE: &str = "codex";
pub const DEFAULT_OCI_ENGINE: &str = "podman";
pub const DEFAULT_RUNTIME_IMAGE: &str = "lionclaw-runtime:v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigureRuntimeOutcome {
    pub runtime_id: String,
    pub created_profile: bool,
    pub default_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProfileFacts {
    pub driver: String,
    pub executable: String,
    pub confinement: String,
    pub engine: Option<String>,
    pub image: Option<String>,
    pub validation_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigureRuntime {
    Codex,
}

impl ConfigureRuntime {
    fn parse(requested: &str) -> Result<Self> {
        match requested.trim() {
            CODEX_RUNTIME_ID => Ok(Self::Codex),
            "" => Err(anyhow!(configure_runtime_required_message())),
            other => Err(anyhow!(unsupported_configure_runtime_message(other))),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Codex => CODEX_RUNTIME_ID,
        }
    }

    fn driver(self) -> &'static str {
        match self {
            Self::Codex => CODEX_RUNTIME_DRIVER,
        }
    }

    fn default_profile(self, oci_engine: String) -> RuntimeProfileConfig {
        match self {
            Self::Codex => RuntimeProfileConfig::Codex {
                executable: CODEX_DEFAULT_EXECUTABLE.to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: oci_engine,
                    image: Some(DEFAULT_RUNTIME_IMAGE.to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        }
    }
}

pub fn configure_runtime_profile(
    config: &mut OperatorConfig,
    requested_runtime: &str,
) -> Result<ConfigureRuntimeOutcome> {
    configure_runtime_profile_with_engine_resolver(config, requested_runtime, |engine| {
        normalize_podman_executable(engine)
    })
}

pub(crate) fn configure_runtime_profile_with_engine_resolver<F>(
    config: &mut OperatorConfig,
    requested_runtime: &str,
    mut resolve_engine: F,
) -> Result<ConfigureRuntimeOutcome>
where
    F: FnMut(&str) -> Result<String>,
{
    let runtime = ConfigureRuntime::parse(requested_runtime)?;
    let runtime_id = runtime.id();
    let previous_default = config.defaults.runtime.clone();
    let mut created_profile = false;

    match config.runtimes.get(runtime_id) {
        Some(profile) if profile.driver() == runtime.driver() => {}
        Some(profile) => bail!(
            "runtime profile \"{runtime_id}\" already exists with driver \"{}\"; expected driver \"{}\"",
            profile.driver(),
            runtime.driver()
        ),
        None => {
            let oci_engine = resolve_engine(DEFAULT_OCI_ENGINE)?;
            config
                .runtimes
                .insert(runtime_id.to_string(), runtime.default_profile(oci_engine));
            created_profile = true;
        }
    }

    config.set_default_runtime(runtime_id)?;

    Ok(ConfigureRuntimeOutcome {
        runtime_id: runtime_id.to_string(),
        created_profile,
        default_changed: previous_default.as_deref() != Some(runtime_id),
    })
}

pub fn runtime_profile_facts(profile: &RuntimeProfileConfig) -> RuntimeProfileFacts {
    let (engine, image) = match profile.confinement() {
        ConfinementConfig::Oci(oci) => (Some(oci.engine.clone()), oci.image.clone()),
    };
    RuntimeProfileFacts {
        driver: profile.driver().to_string(),
        executable: profile.executable().to_string(),
        confinement: profile.confinement().backend().as_str().to_string(),
        engine,
        image,
        validation_error: profile.validate().err().map(|err| err.to_string()),
    }
}

pub fn configure_runtime_required_message() -> &'static str {
    "runtime is required\nRun:\n  lionclaw configure --runtime codex"
}

pub fn unsupported_configure_runtime_message(runtime_id: &str) -> String {
    format!(
        "runtime \"{runtime_id}\" is not supported by configure yet\nExisting low-level runtime commands may still be available."
    )
}

pub fn runtime_auth_guidance(profile: &RuntimeProfileConfig) -> Option<&'static str> {
    match profile {
        RuntimeProfileConfig::Codex { .. } => Some("Codex auth is checked at launch; run `codex login` on the host if launch reports missing auth."),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::config::OperatorConfig;

    const RESOLVED_PODMAN: &str = "/usr/bin/podman";

    fn configure_with_resolved_engine(
        config: &mut OperatorConfig,
        requested_runtime: &str,
    ) -> Result<ConfigureRuntimeOutcome> {
        configure_runtime_profile_with_engine_resolver(config, requested_runtime, |engine| {
            assert_eq!(engine, "podman");
            Ok(RESOLVED_PODMAN.to_string())
        })
    }

    #[test]
    fn configure_codex_creates_profile_with_resolved_engine_and_sets_default() {
        let mut config = OperatorConfig::default();

        let outcome =
            configure_with_resolved_engine(&mut config, "codex").expect("configure codex");

        assert!(outcome.created_profile);
        assert_eq!(config.defaults.runtime.as_deref(), Some("codex"));
        let RuntimeProfileConfig::Codex {
            executable,
            confinement,
            ..
        } = config.runtimes.get("codex").expect("codex profile")
        else {
            panic!("expected codex profile");
        };
        assert_eq!(executable, "codex");
        let ConfinementConfig::Oci(oci) = confinement;
        assert_eq!(oci.engine, RESOLVED_PODMAN);
        assert_eq!(oci.image.as_deref(), Some("lionclaw-runtime:v1"));
    }

    #[test]
    fn configure_codex_is_idempotent_and_preserves_existing_fields() {
        let mut config = OperatorConfig::default();
        config.runtimes.insert(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "custom-codex".to_string(),
                model: Some("gpt-5.2".to_string()),
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: "/usr/bin/podman".to_string(),
                    image: Some("custom-runtime:v2".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let outcome = configure_runtime_profile_with_engine_resolver(&mut config, "codex", |_| {
            panic!("engine resolver should not run for an existing profile")
        })
        .expect("configure codex");

        assert!(!outcome.created_profile);
        assert_eq!(config.defaults.runtime.as_deref(), Some("codex"));
        let RuntimeProfileConfig::Codex {
            executable,
            model,
            confinement,
        } = config.runtimes.get("codex").expect("codex profile")
        else {
            panic!("expected codex profile");
        };
        assert_eq!(executable, "custom-codex");
        assert_eq!(model.as_deref(), Some("gpt-5.2"));
        let ConfinementConfig::Oci(oci) = confinement;
        assert_eq!(oci.engine, "/usr/bin/podman");
        assert_eq!(oci.image.as_deref(), Some("custom-runtime:v2"));
    }

    #[test]
    fn configure_rejects_unsupported_runtime() {
        let mut config = OperatorConfig::default();

        let err = configure_with_resolved_engine(&mut config, "opencode")
            .expect_err("opencode is not a configure happy path yet");

        assert!(err
            .to_string()
            .contains("runtime \"opencode\" is not supported by configure yet"));
    }

    #[test]
    fn configure_rejects_wrong_driver_existing_profile() {
        let mut config = OperatorConfig::default();
        config
            .runtimes
            .insert("codex".to_string(), test_acp_profile());

        let err = configure_runtime_profile_with_engine_resolver(&mut config, "codex", |_| {
            panic!("engine resolver should not run for a wrong-kind profile")
        })
        .expect_err("wrong driver should fail");

        assert!(err
            .to_string()
            .contains("already exists with driver \"acp\""));
    }

    #[test]
    fn configure_surfaces_engine_resolution_errors_without_creating_profile() {
        let mut config = OperatorConfig::default();

        let err = configure_runtime_profile_with_engine_resolver(&mut config, "codex", |_| {
            Err(anyhow!("podman not found"))
        })
        .expect_err("missing podman should fail");

        assert!(err.to_string().contains("podman not found"));
        assert!(!config.runtimes.contains_key("codex"));
        assert!(config.defaults.runtime.is_none());
    }

    #[test]
    fn auth_guidance_follows_profile_kind() {
        let codex = RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
        };
        let acp = test_acp_profile();

        assert!(runtime_auth_guidance(&codex)
            .expect("codex guidance")
            .contains("codex login"));
        assert!(runtime_auth_guidance(&acp).is_none());
    }

    fn test_acp_profile() -> RuntimeProfileConfig {
        RuntimeProfileConfig::Acp {
            executable: "opencode".to_string(),
            args: vec!["acp".to_string()],
            environment: std::collections::BTreeMap::new(),
            model: None,
            mode: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
        }
    }
}
