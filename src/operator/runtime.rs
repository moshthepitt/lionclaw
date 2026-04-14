use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::home::LionClawHome;
use crate::kernel::{
    runtime::{
        CodexRuntimeAdapter, CodexRuntimeConfig, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig,
        RuntimeExecutionProfile,
    },
    Kernel,
};

use super::config::{OperatorConfig, RuntimeProfileConfig};

pub async fn register_configured_runtimes(kernel: &Kernel, config: &OperatorConfig) -> Result<()> {
    validate_configured_runtimes(config)?;

    for (id, runtime) in &config.runtimes {
        match runtime {
            RuntimeProfileConfig::Codex {
                executable,
                model,
                confinement: _,
            } => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(CodexRuntimeAdapter::new(CodexRuntimeConfig {
                            executable: executable.clone(),
                            model: model.clone(),
                        })),
                    )
                    .await;
            }
            RuntimeProfileConfig::OpenCode {
                executable,
                model,
                agent,
                confinement: _,
            } => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
                            executable: executable.clone(),
                            model: model.clone(),
                            agent: agent.clone(),
                        })),
                    )
                    .await;
            }
        }
    }

    Ok(())
}

pub fn configured_runtime_execution_profiles(
    config: &OperatorConfig,
) -> BTreeMap<String, RuntimeExecutionProfile> {
    config
        .runtimes
        .iter()
        .map(|(id, runtime)| (id.clone(), runtime.execution_profile()))
        .collect()
}

pub fn resolve_runtime_id(config: &OperatorConfig, requested: Option<&str>) -> Result<String> {
    config.resolve_runtime_id(requested)
}

pub fn validate_configured_runtimes(config: &OperatorConfig) -> Result<()> {
    for runtime_id in config.runtimes.keys() {
        validate_runtime_availability(config, runtime_id)?;
    }

    Ok(())
}

pub fn validate_runtime_availability(config: &OperatorConfig, runtime_id: &str) -> Result<()> {
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{}' is not configured", runtime_id))?;
    profile.validate().map_err(|err| {
        anyhow!(
            "configured runtime profile '{}' is invalid: {}",
            runtime_id,
            err
        )
    })?;
    Ok(())
}

pub async fn validate_runtime_launch_prerequisites(
    home: &LionClawHome,
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<()> {
    validate_runtime_availability(config, runtime_id)?;
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{}' is not configured", runtime_id))?;
    let Some(required_var) = profile.required_runtime_auth_var() else {
        return Ok(());
    };
    let auth_path = home.runtime_auth_env_path();
    home.resolve_runtime_auth_file()
        .await?
        .ok_or_else(|| {
            anyhow!(
                "configured runtime profile '{}' requires host runtime auth file '{}' with {} configured",
                runtime_id,
                auth_path.display(),
                required_var
            )
        })?;
    home.read_runtime_auth_var(required_var)
        .await?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!(
                "configured runtime profile '{}' requires {} in '{}'",
                runtime_id,
                required_var,
                auth_path.display()
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_configured_runtimes, validate_runtime_availability,
        validate_runtime_launch_prerequisites,
    };
    use crate::home::LionClawHome;
    use crate::kernel::runtime::{ConfinementConfig, OciConfinementConfig};
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

    #[cfg(unix)]
    fn fake_podman() -> (tempfile::TempDir, String) {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("podman");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        (temp_dir, path.to_string_lossy().to_string())
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_requires_usable_host_engine() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("podman");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: path.to_string_lossy().to_string(),
                    image: Some("ghcr.io/lionclaw/codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_profile_accepts_container_command_with_valid_engine() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine,
                    image: Some("ghcr.io/lionclaw/codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );

        validate_runtime_availability(&config, "codex").expect("runtime command should validate");
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_requires_image() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine,
                    image: None,
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("Podman runtime image is required"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_validation_rejects_any_invalid_profile() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.clone(),
                    image: Some("ghcr.io/lionclaw/codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        config.upsert_runtime(
            "broken".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine,
                    image: None,
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let err = validate_configured_runtimes(&config).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("configured runtime profile 'broken' is invalid"));
    }

    fn codex_runtime_profile(engine: String) -> RuntimeProfileConfig {
        RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine,
                image: Some("ghcr.io/lionclaw/codex-runtime:latest".to_string()),
                ..OciConfinementConfig::default()
            }),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_require_runtime_auth_file() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("missing runtime auth file should fail");
        assert!(err.to_string().contains("runtime-auth.env"));
        assert!(err.to_string().contains("OPENAI_API_KEY"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_require_openai_api_key() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(home.runtime_auth_env_path(), "# no key\n")
            .await
            .expect("write runtime auth");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("missing auth var should fail");
        assert!(err.to_string().contains("OPENAI_API_KEY"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_accept_runtime_auth_file() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(home.runtime_auth_env_path(), "OPENAI_API_KEY=sk-test\n")
            .await
            .expect("write runtime auth");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect("runtime auth should validate");
    }
}
