use std::sync::Arc;
use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{anyhow, Result};

use crate::home::LionClawHome;
use crate::kernel::{
    runtime::{
        validate_runtime_auth_prerequisites, CodexRuntimeAdapter, CodexRuntimeConfig,
        OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig, RuntimeExecutionProfile,
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
    validate_runtime_auth_prerequisites(
        runtime_id,
        profile.required_runtime_auth(),
        operator_codex_home_override(home).as_deref(),
    )
    .await
}

pub(crate) fn operator_codex_home_override(home: &LionClawHome) -> Option<PathBuf> {
    #[cfg(test)]
    {
        Some(home.root().join(".codex"))
    }
    #[cfg(not(test))]
    {
        let _ = home;
        None
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use chrono::{Duration as ChronoDuration, Utc};
    use serde_json::json;

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

    async fn write_test_codex_auth(home: &LionClawHome, auth: serde_json::Value) {
        let codex_home = home.root().join(".codex");
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        tokio::fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&auth).expect("encode auth"),
        )
        .await
        .expect("write auth file");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_require_host_codex_auth() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("missing host Codex auth should fail");
        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_reject_unusable_codex_auth() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        write_test_codex_auth(&home, json!({ "OPENAI_API_KEY": null, "tokens": {} })).await;
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("missing host auth should fail");
        assert!(err.to_string().contains("codex login"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_accept_host_codex_auth() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        write_test_codex_auth(
            &home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": Utc::now().to_rfc3339(),
                "tokens": {
                    "access_token": format!("a.{}.c", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                        serde_json::to_vec(&json!({ "exp": (Utc::now() + ChronoDuration::hours(1)).timestamp() }))
                            .expect("jwt payload")
                    )),
                    "refresh_token": "refresh-test"
                }
            }),
        )
        .await;
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect("runtime auth should validate");
    }
}
