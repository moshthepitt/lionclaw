use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::kernel::{
    runtime::{
        CodexRuntimeAdapter, CodexRuntimeConfig, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig,
        BUILTIN_RUNTIME_CODEX, BUILTIN_RUNTIME_OPENCODE,
    },
    Kernel,
};

use super::config::{
    normalize_executable, validate_executable, OperatorConfig, RuntimeProfileConfig,
};

pub async fn register_configured_runtimes(kernel: &Kernel, config: &OperatorConfig) -> Result<()> {
    for (id, runtime) in &config.runtimes {
        match runtime {
            RuntimeProfileConfig::Codex {
                executable,
                model,
                sandbox,
                skip_git_repo_check,
                ephemeral,
            } => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(CodexRuntimeAdapter::new(CodexRuntimeConfig {
                            executable: executable.clone(),
                            model: model.clone(),
                            sandbox_mode: sandbox.clone(),
                            skip_git_repo_check: *skip_git_repo_check,
                            ephemeral: *ephemeral,
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
    if let Some(profile) = config.runtime(runtime_id) {
        validate_executable(profile.executable()).map_err(|err| {
            anyhow!(
                "configured runtime command '{}' is invalid: {}",
                profile.executable(),
                err
            )
        })?;
        return Ok(());
    }

    let _ = build_runtime_fallback_env(runtime_id)?;
    Ok(())
}

pub fn runtime_service_env(
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<Vec<(String, String)>> {
    let mut env = current_process_path_env();
    if config.runtime(runtime_id).is_some() {
        return Ok(env);
    }

    env.extend(build_runtime_fallback_env(runtime_id)?);
    Ok(env)
}

fn build_runtime_fallback_env(runtime_id: &str) -> Result<Vec<(String, String)>> {
    match runtime_id {
        BUILTIN_RUNTIME_CODEX => {
            let executable = std::env::var("LIONCLAW_CODEX_BIN")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| normalize_executable(&value))
                .transpose()?
                .unwrap_or(normalize_executable("codex")?);

            let mut env = vec![("LIONCLAW_CODEX_BIN".to_string(), executable)];
            copy_if_present(&mut env, "LIONCLAW_CODEX_MODEL");
            copy_if_present(&mut env, "LIONCLAW_CODEX_SANDBOX");
            copy_if_present(&mut env, "LIONCLAW_CODEX_SKIP_GIT_REPO_CHECK");
            copy_if_present(&mut env, "LIONCLAW_CODEX_EPHEMERAL");
            Ok(env)
        }
        BUILTIN_RUNTIME_OPENCODE => {
            let executable = std::env::var("LIONCLAW_OPENCODE_BIN")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| normalize_executable(&value))
                .transpose()?
                .unwrap_or(normalize_executable("opencode")?);

            let mut env = vec![("LIONCLAW_OPENCODE_BIN".to_string(), executable)];
            copy_if_present(&mut env, "LIONCLAW_OPENCODE_FORMAT");
            copy_if_present(&mut env, "LIONCLAW_OPENCODE_MODEL");
            copy_if_present(&mut env, "LIONCLAW_OPENCODE_AGENT");
            copy_if_present(&mut env, "LIONCLAW_OPENCODE_XDG_DATA_HOME");
            copy_if_present(&mut env, "LIONCLAW_OPENCODE_CONTINUE_LAST_SESSION");
            Ok(env)
        }
        _ => Err(anyhow!(
            "runtime '{}' is not configured; add it with 'lionclaw runtime add {} --kind <codex|opencode> --bin <command-or-path>'",
            runtime_id, runtime_id
        )),
    }
}

fn copy_if_present(out: &mut Vec<(String, String)>, key: &str) {
    if let Ok(value) = std::env::var(key) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            out.push((key.to_string(), trimmed.to_string()));
        }
    }
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
    use super::{build_runtime_fallback_env, validate_runtime_availability};
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

    #[test]
    fn unsupported_runtime_requires_configuration() {
        let err = build_runtime_fallback_env("custom-runtime").expect_err("should fail");
        assert!(
            err.to_string().contains("lionclaw runtime add"),
            "error should guide the user toward configuring a runtime profile"
        );
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
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_command_is_validated_via_path() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let bin_dir = temp_dir.path().join("bin");
        std::fs::create_dir_all(&bin_dir).expect("mkdir");
        let path = bin_dir.join("codex");
        std::fs::write(&path, "#!/usr/bin/env bash\n").expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        let original_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("PATH", bin_dir.as_os_str());
        }

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );

        let result = validate_runtime_availability(&config, "codex");
        match original_path {
            Some(value) => unsafe {
                std::env::set_var("PATH", value);
            },
            None => unsafe {
                std::env::remove_var("PATH");
            },
        }

        result.expect("runtime command should validate");
    }

    #[test]
    fn configured_runtime_service_env_carries_path() {
        let original_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("PATH", "/tmp/lionclaw-bin");
        }

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );

        let env = super::runtime_service_env(&config, "codex").expect("service env");
        match original_path {
            Some(value) => unsafe {
                std::env::set_var("PATH", value);
            },
            None => unsafe {
                std::env::remove_var("PATH");
            },
        }

        assert_eq!(
            env,
            vec![("PATH".to_string(), "/tmp/lionclaw-bin".to_string())]
        );
    }
}
