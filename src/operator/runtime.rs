use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::kernel::{
    runtime::{
        CodexRuntimeAdapter, CodexRuntimeConfig, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig,
        RuntimeExecutionProfile,
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

#[cfg(test)]
mod tests {
    use super::validate_runtime_availability;
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

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
        let executable = which::which("sh").expect("resolve sh");
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: executable.to_string_lossy().to_string(),
                model: None,
                confinement: None,
            },
        );

        validate_runtime_availability(&config, "codex").expect("runtime command should validate");
    }
}
