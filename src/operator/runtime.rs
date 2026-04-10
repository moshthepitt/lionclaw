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

#[cfg(test)]
mod tests {
    use super::{validate_configured_runtimes, validate_runtime_availability};
    use crate::kernel::runtime::{ConfinementConfig, OciConfinementConfig};
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

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
        let engine = std::env::current_exe().expect("current exe");
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.to_string_lossy().to_string(),
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
        let engine = std::env::current_exe().expect("current exe");
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.to_string_lossy().to_string(),
                    image: None,
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err
            .to_string()
            .contains("OCI confinement image is required"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_validation_rejects_any_invalid_profile() {
        let engine = std::env::current_exe().expect("current exe");
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.to_string_lossy().to_string(),
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
                    engine: engine.to_string_lossy().to_string(),
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

    #[cfg(unix)]
    #[test]
    fn configured_opencode_runtime_requires_json_format() {
        let engine = std::env::current_exe().expect("current exe");
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "opencode".to_string(),
            RuntimeProfileConfig::OpenCode {
                executable: "opencode".to_string(),
                format: "default".to_string(),
                model: None,
                agent: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.to_string_lossy().to_string(),
                    image: Some("ghcr.io/lionclaw/opencode-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );

        let err = validate_runtime_availability(&config, "opencode").expect_err("should fail");
        assert!(err
            .to_string()
            .contains("OpenCode runtime format must be 'json'"));
    }
}
