use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::kernel::{
    runtime::{
        CodexRuntimeAdapter, CodexRuntimeConfig, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig,
    },
    Kernel,
};

use super::config::{validate_executable, OperatorConfig, RuntimeProfileConfig};

pub async fn register_configured_runtimes(kernel: &Kernel, config: &OperatorConfig) -> Result<()> {
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
                format,
                model,
                agent,
                xdg_data_home,
                continue_last_session,
                confinement: _,
            } => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
                            executable: executable.clone(),
                            format: format.clone(),
                            model: model.clone(),
                            agent: agent.clone(),
                            xdg_data_home: xdg_data_home.clone(),
                            continue_last_session: *continue_last_session,
                        })),
                    )
                    .await;
            }
        }
    }

    Ok(())
}

pub fn resolve_runtime_id(config: &OperatorConfig, requested: Option<&str>) -> Result<String> {
    config.resolve_runtime_id(requested)
}

pub fn validate_runtime_availability(config: &OperatorConfig, runtime_id: &str) -> Result<()> {
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{}' is not configured", runtime_id))?;
    validate_executable(profile.executable()).map_err(|err| {
        anyhow!(
            "configured runtime command '{}' is invalid: {}",
            profile.executable(),
            err
        )
    })?;
    Ok(())
}

pub fn runtime_service_env(
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<Vec<(String, String)>> {
    config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{}' is not configured", runtime_id))?;
    Ok(current_process_path_env())
}

fn current_process_path_env() -> Vec<(String, String)> {
    std::env::var("PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| vec![("PATH".to_string(), value)])
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::validate_runtime_availability;
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

    #[test]
    fn runtime_service_env_requires_configured_runtime() {
        let config = OperatorConfig::default();
        let err = super::runtime_service_env(&config, "custom-runtime").expect_err("should fail");
        assert!(err
            .to_string()
            .contains("runtime profile 'custom-runtime' is not configured"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_requires_executable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("codex");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: path.to_string_lossy().to_string(),
                model: None,
                confinement: None,
            },
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_command_is_validated_via_path() {
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "sh".to_string(),
                model: None,
                confinement: None,
            },
        );

        validate_runtime_availability(&config, "codex").expect("runtime command should validate");
    }

    #[test]
    fn configured_runtime_service_env_carries_path() {
        let path = std::env::var("PATH").expect("path");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: None,
            },
        );

        let env = super::runtime_service_env(&config, "codex").expect("service env");
        assert_eq!(env, vec![("PATH".to_string(), path)]);
    }
}
