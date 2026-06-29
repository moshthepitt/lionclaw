use std::sync::Arc;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use lionclaw_runtime_acp::{AcpRuntimeAdapter, AcpRuntimeConfig};
use lionclaw_runtime_api::RuntimeAuthProvider;
use lionclaw_runtime_codex::{CodexRuntimeAdapter, CodexRuntimeConfig};
use lionclaw_runtime_codex::{CodexRuntimeAuthProvider, CODEX_RUNTIME_AUTH_KIND};

use crate::home::LionClawHome;
use crate::kernel::{
    runtime::execution::planner::resolve_execution_network_mode,
    runtime::{
        resolve_oci_image_compatibility_identity, validate_runtime_execution_prerequisites,
        ExecutionPlanPurpose, NetworkMode, RuntimeAuthContext, RuntimeAuthRegistry,
        RuntimeExecutionProfile,
    },
    Kernel,
};
use tracing::warn;

use super::config::{
    daemon_compat_fingerprint_with_runtime_context, validate_runtime_command, OperatorConfig,
    RuntimeProfileConfig,
};
use super::runtime_mounts::validate_configured_runtime_mounts;

#[derive(Debug, Clone)]
pub struct ResolvedRuntimeExecutionContext {
    pub codex_home_override: Option<PathBuf>,
    pub runtime_auth_registry: RuntimeAuthRegistry,
    pub runtime_auth_context: RuntimeAuthContext,
    pub daemon_config_fingerprint: String,
    pub execution_profiles: BTreeMap<String, RuntimeExecutionProfile>,
}

pub async fn register_configured_runtimes(kernel: &Kernel, config: &OperatorConfig) -> Result<()> {
    for (id, runtime) in &config.runtimes {
        if let Err(err) = validate_runtime_command(runtime.executable()) {
            warn!(
                ?err,
                runtime_id = %id,
                "skipping runtime adapter registration for invalid runtime command"
            );
            continue;
        }
        match runtime.driver() {
            "codex" => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(CodexRuntimeAdapter::new(CodexRuntimeConfig {
                            executable: runtime.executable.clone(),
                            model: runtime.model.clone(),
                        })),
                    )
                    .await;
            }
            "acp" => {
                kernel
                    .register_runtime_adapter(
                        id.clone(),
                        Arc::new(AcpRuntimeAdapter::new(AcpRuntimeConfig {
                            runtime_id: id.clone(),
                            executable: runtime.executable.clone(),
                            args: runtime.args.clone(),
                            environment: runtime
                                .environment
                                .iter()
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect(),
                            model: runtime.model.clone(),
                            mode: runtime.mode.clone(),
                            session_id_state_file: ".lionclaw-acp-session-id".to_string(),
                            default_working_dir: "/workspace".to_string(),
                        })),
                    )
                    .await;
            }
            other => warn!(
                runtime_id = %id,
                driver = %other,
                "skipping runtime adapter registration for unsupported runtime driver"
            ),
        }
    }

    Ok(())
}

pub async fn resolve_runtime_execution_context(
    home: &LionClawHome,
    config: &OperatorConfig,
    selected_runtime_id: Option<&str>,
) -> Result<ResolvedRuntimeExecutionContext> {
    let codex_home_override = operator_codex_home_override(home)?;
    let runtime_auth_context = runtime_auth_context(codex_home_override.clone());
    let runtime_auth_registry = runtime_auth_registry();
    let runtime_image_identities =
        resolve_runtime_image_identities(config, selected_runtime_id).await?;
    let execution_profiles = config
        .runtimes
        .iter()
        .map(|(id, runtime)| {
            let runtime_auth_identity =
                runtime_auth_identity(runtime, &runtime_auth_registry, &runtime_auth_context)?;
            let profile = if selected_runtime_id == Some(id.as_str()) {
                runtime.execution_profile_with_runtime_context(
                    runtime_image_identities.get(id).map(String::as_str),
                    runtime_auth_identity.as_deref(),
                )
            } else {
                runtime
                    .execution_profile_with_runtime_context(None, runtime_auth_identity.as_deref())
            };
            Ok((id.clone(), profile))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let daemon_config_fingerprint = daemon_compat_fingerprint_with_runtime_context(
        config,
        &runtime_auth_context,
        &runtime_image_identities,
    );

    Ok(ResolvedRuntimeExecutionContext {
        codex_home_override,
        runtime_auth_registry,
        runtime_auth_context,
        daemon_config_fingerprint,
        execution_profiles,
    })
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
        .ok_or_else(|| anyhow!("runtime profile '{runtime_id}' is not configured"))?;
    profile
        .validate()
        .map_err(|err| anyhow!("configured runtime profile '{runtime_id}' is invalid: {err}"))?;
    match profile.driver() {
        "codex" | "acp" => {}
        other => {
            return Err(anyhow!(
            "configured runtime profile '{runtime_id}' uses unsupported runtime driver '{other}'"
        ))
        }
    }
    Ok(())
}

pub async fn validate_runtime_launch_prerequisites(
    home: &LionClawHome,
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<()> {
    validate_runtime_launch_prerequisites_for_work_root(home, config, runtime_id, None, None).await
}

pub async fn validate_runtime_launch_prerequisites_for_work_root(
    home: &LionClawHome,
    config: &OperatorConfig,
    runtime_id: &str,
    project_root: Option<&Path>,
    work_root: Option<&Path>,
) -> Result<()> {
    validate_runtime_availability(config, runtime_id)?;
    validate_configured_runtime_mounts(home, project_root, work_root, config, runtime_id).map_err(
        |err| anyhow!("configured runtime profile '{runtime_id}' has invalid mounts: {err}"),
    )?;
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{runtime_id}' is not configured"))?;
    let codex_home_override = operator_codex_home_override(home)?;
    let runtime_auth_context = runtime_auth_context(codex_home_override);
    let runtime_auth_registry = runtime_auth_registry();
    validate_runtime_execution_prerequisites(
        runtime_id,
        profile.confinement(),
        profile.required_runtime_auth(),
        &runtime_auth_registry,
        &runtime_auth_context,
        interactive_network_mode(config)?,
    )
    .await
}

pub(crate) fn operator_codex_home_override(_home: &LionClawHome) -> Result<Option<PathBuf>> {
    #[cfg(test)]
    {
        Ok(Some(_home.root().join(".codex")))
    }
    #[cfg(not(test))]
    {
        crate::config::resolve_optional_env_override_path("CODEX_HOME").map_err(Into::into)
    }
}

async fn resolve_runtime_image_identities(
    config: &OperatorConfig,
    selected_runtime_id: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    let mut identities = BTreeMap::new();
    let Some(selected_runtime_id) = selected_runtime_id else {
        return Ok(identities);
    };
    let Some(runtime) = config.runtime(selected_runtime_id) else {
        return Ok(identities);
    };
    let crate::kernel::runtime::ConfinementConfig::Oci(oci) = runtime.confinement();
    let Some(image) = oci.image.as_deref() else {
        return Ok(identities);
    };
    let identity = resolve_oci_image_compatibility_identity(&oci.engine, image)
        .await
        .map_err(|err| {
            anyhow!(
                "failed to resolve local OCI image identity for runtime '{selected_runtime_id}': {err}"
            )
        })?;
    identities.insert(selected_runtime_id.to_string(), identity);

    Ok(identities)
}

fn runtime_auth_identity(
    runtime: &RuntimeProfileConfig,
    auth_registry: &RuntimeAuthRegistry,
    auth_context: &RuntimeAuthContext,
) -> Result<Option<String>> {
    match runtime.required_runtime_auth() {
        Some(auth) => {
            let provider = auth_registry.get(&auth).ok_or_else(|| {
                anyhow!(
                    "configured runtime requires unsupported runtime auth kind '{}'",
                    auth.as_str()
                )
            })?;
            provider.identity(auth_context)
        }
        None => Ok(None),
    }
}

pub(crate) fn runtime_auth_registry() -> RuntimeAuthRegistry {
    RuntimeAuthRegistry::new([Arc::new(CodexRuntimeAuthProvider) as Arc<dyn RuntimeAuthProvider>])
}

fn runtime_auth_context(codex_home_override: Option<PathBuf>) -> RuntimeAuthContext {
    match codex_home_override {
        Some(path) => RuntimeAuthContext::new().with_home_override(CODEX_RUNTIME_AUTH_KIND, path),
        None => RuntimeAuthContext::new(),
    }
}

fn interactive_network_mode(config: &OperatorConfig) -> Result<NetworkMode> {
    resolve_execution_network_mode(
        ExecutionPlanPurpose::Interactive,
        None,
        config.defaults.preset.as_deref(),
        &config.presets,
    )
    .map_err(|err| anyhow!(err))
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use chrono::{Duration as ChronoDuration, Utc};
    use lionclaw_runtime_codex::codex_runtime_auth_kind;
    use serde_json::json;

    use super::{
        register_configured_runtimes, resolve_runtime_execution_context,
        validate_configured_runtimes, validate_runtime_availability,
        validate_runtime_launch_prerequisites, validate_runtime_launch_prerequisites_for_work_root,
    };
    use crate::home::LionClawHome;
    use crate::kernel::{
        runtime::{
            ConfinementConfig, ExecutionPreset, MountAccess, MountSpec, NetworkMode,
            OciConfinementConfig, RuntimeSkillProjectionConfig, WorkspaceAccess,
        },
        Kernel,
    };
    use crate::operator::config::{OperatorConfig, RuntimeProfileConfig};

    #[cfg(unix)]
    fn fake_podman_with_body(body: &str) -> (tempfile::TempDir, String) {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("podman");
        std::fs::write(&path, body).expect("write file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        (temp_dir, path.to_string_lossy().to_string())
    }

    #[cfg(unix)]
    fn fake_podman() -> (tempfile::TempDir, String) {
        fake_podman_with_body(
            "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
        )
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
            codex_runtime_profile(path.to_string_lossy().to_string()),
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("not marked executable"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_profile_accepts_container_command_with_valid_engine() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        validate_runtime_availability(&config, "codex").expect("runtime command should validate");
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_requires_image() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            codex_runtime_profile_with_image(engine, None),
        );

        let err = validate_runtime_availability(&config, "codex").expect_err("should fail");
        assert!(err.to_string().contains("Podman runtime image is required"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_runtime_validation_rejects_any_invalid_profile() {
        let (_temp_dir, engine) = fake_podman();
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine.clone()));
        config.upsert_runtime(
            "broken".to_string(),
            codex_runtime_profile_with_image(engine, None),
        );

        let err = validate_configured_runtimes(&config).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("configured runtime profile 'broken' is invalid"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_adapter_registration_does_not_require_every_profile_to_be_launchable() {
        let (_temp_dir, engine) = fake_podman();
        let home = tempfile::tempdir().expect("home");
        let kernel = Kernel::new(&home.path().join("lionclaw.db"))
            .await
            .expect("kernel");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine.clone()));
        config.upsert_runtime(
            "broken".to_string(),
            codex_runtime_profile_with_image(engine, None),
        );

        register_configured_runtimes(&kernel, &config)
            .await
            .expect("non-launchable profile should not block kernel registration");
    }

    fn codex_runtime_profile(engine: String) -> RuntimeProfileConfig {
        codex_runtime_profile_with_image(
            engine,
            Some("ghcr.io/lionclaw/codex-runtime:latest".to_string()),
        )
    }

    fn codex_runtime_profile_with_image(
        engine: String,
        image: Option<String>,
    ) -> RuntimeProfileConfig {
        RuntimeProfileConfig::new(
            "codex",
            "codex",
            ConfinementConfig::Oci(OciConfinementConfig {
                engine,
                image,
                ..OciConfinementConfig::default()
            }),
        )
        .with_auth(codex_runtime_auth_kind())
        .with_skill_projection(RuntimeSkillProjectionConfig::native_dir(".codex/skills"))
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
    async fn launch_prereqs_reject_project_metadata_mount_sources() {
        let (_temp_dir, engine) = fake_podman();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let project_root = temp_dir.path().join("project");
        let work_root = temp_dir.path().join("work");
        let metadata_source = project_root.join(".lionclaw/private");
        std::fs::create_dir_all(&metadata_source).expect("metadata source");
        std::fs::create_dir_all(&work_root).expect("work root");
        home.ensure_base_dirs().await.expect("base dirs");
        let mut profile = codex_runtime_profile(engine);
        let ConfinementConfig::Oci(oci) = profile.confinement_mut();
        oci.additional_mounts.push(MountSpec {
            source: metadata_source,
            target: "/mnt/private".to_string(),
            access: MountAccess::ReadOnly,
        });
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), profile);

        let err = validate_runtime_launch_prerequisites_for_work_root(
            &home,
            &config,
            "codex",
            Some(&project_root),
            Some(&work_root),
        )
        .await
        .expect_err("project metadata mount source");

        assert!(err.to_string().contains("project metadata"));
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

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_fail_early_when_private_network_is_unavailable() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let log_path = temp_dir.path().join("podman.log");
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"${{1:-}}\" = \"image\" ] && [ \"${{2:-}}\" = \"exists\" ]; then\n  exit 0\nfi\nif [ \"${{1:-}}\" = \"run\" ]; then\n  cat >&2 <<'EOF'\nError: pasta failed with exit code 1:\nFailed to open() /dev/net/tun: No such device\nFailed to set up tap device in namespace\nEOF\n  exit 125\nfi\nexit 0\n",
            log_path.display()
        );
        let (_podman_dir, engine) = fake_podman_with_body(&script);
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

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("unavailable private network should fail before startup");
        assert!(err.to_string().contains("requires network-mode 'on'"));
        assert!(err
            .to_string()
            .contains("could not start a private network"));
        assert!(err.to_string().contains("/dev/net/tun"));

        let log = std::fs::read_to_string(&log_path).expect("read podman log");
        assert!(log.contains("image exists ghcr.io/lionclaw/codex-runtime:latest"));
        assert!(log.contains(
            "run --rm --pull=never --network private --entrypoint /bin/sh ghcr.io/lionclaw/codex-runtime:latest -lc :"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_skip_private_network_probe_when_default_preset_is_offline() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let log_path = temp_dir.path().join("podman.log");
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"${{1:-}}\" = \"image\" ] && [ \"${{2:-}}\" = \"exists\" ]; then\n  exit 0\nfi\nif [ \"${{1:-}}\" = \"run\" ]; then\n  exit 0\nfi\nexit 0\n",
            log_path.display()
        );
        let (_podman_dir, engine) = fake_podman_with_body(&script);
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
        config.defaults.preset = Some("offline".to_string());
        config.upsert_preset(
            "offline".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::None,
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
            },
        );
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect("offline preset should skip private-network probe");

        let log = std::fs::read_to_string(&log_path).expect("read podman log");
        assert!(log.contains("image exists ghcr.io/lionclaw/codex-runtime:latest"));
        assert!(!log.contains("run --rm --pull=never --network private"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_do_not_pull_hidden_second_image() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let log_path = temp_dir.path().join("podman.log");
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> \"{}\"\nif [ \"${{1:-}}\" = \"image\" ] && [ \"${{2:-}}\" = \"exists\" ]; then\n  exit 0\nfi\nexit 0\n",
            log_path.display()
        );
        let (_podman_dir, engine) = fake_podman_with_body(&script);
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
            .expect("runtime image probe should validate");

        let log = std::fs::read_to_string(&log_path).expect("read podman log");
        assert!(log.contains("image exists ghcr.io/lionclaw/codex-runtime:latest"));
        assert!(!log.contains("pull "));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_fail_when_runtime_image_is_missing_locally() {
        let script = "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"exists\" ]; then\n  exit 1\nfi\nexit 0\n";
        let (_podman_dir, engine) = fake_podman_with_body(script);
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

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("missing runtime image should fail before launch");
        assert!(err
            .to_string()
            .contains("configured runtime image 'ghcr.io/lionclaw/codex-runtime:latest' for runtime 'codex' is not available locally"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_do_not_hide_runtime_image_probe_errors_with_exit_one() {
        let script = "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"exists\" ]; then\n  echo 'failed to read storage state' >&2\n  exit 1\nfi\nexit 0\n";
        let (_podman_dir, engine) = fake_podman_with_body(script);
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

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("probe stderr should surface as an engine error");
        assert!(err.to_string().contains(
            "failed to inspect OCI image 'ghcr.io/lionclaw/codex-runtime:latest': failed to read storage state"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_launch_prereqs_surface_runtime_image_probe_errors_honestly() {
        let script = "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"exists\" ]; then\n  echo 'storage denied' >&2\n  exit 125\nfi\nexit 0\n";
        let (_podman_dir, engine) = fake_podman_with_body(script);
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

        let err = validate_runtime_launch_prerequisites(&home, &config, "codex")
            .await
            .expect_err("probe failure should surface engine error");
        assert!(err.to_string().contains(
            "failed to inspect OCI image 'ghcr.io/lionclaw/codex-runtime:latest': storage denied"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_execution_context_changes_when_local_image_identity_changes() {
        let (_podman_dir, engine) = fake_podman_with_body(
            "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:runtime-a\\n'\n  exit 0\nfi\nexit 0\n",
        );
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let left = resolve_runtime_execution_context(&home, &config, Some("codex"))
            .await
            .expect("resolve left context");

        let (_podman_dir, engine) = fake_podman_with_body(
            "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:runtime-b\\n'\n  exit 0\nfi\nexit 0\n",
        );
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let right = resolve_runtime_execution_context(&home, &config, Some("codex"))
            .await
            .expect("resolve right context");

        assert_ne!(
            left.daemon_config_fingerprint,
            right.daemon_config_fingerprint
        );
        assert_ne!(
            left.execution_profiles["codex"].compatibility_key,
            right.execution_profiles["codex"].compatibility_key
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_execution_context_changes_when_codex_home_identity_changes() {
        let (_podman_dir, engine) = fake_podman();
        let left_temp = tempfile::tempdir().expect("left temp dir");
        let right_temp = tempfile::tempdir().expect("right temp dir");
        let left_home = LionClawHome::new(left_temp.path().join(".lionclaw"));
        let right_home = LionClawHome::new(right_temp.path().join(".lionclaw"));
        left_home.ensure_base_dirs().await.expect("left dirs");
        right_home.ensure_base_dirs().await.expect("right dirs");
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), codex_runtime_profile(engine));

        let left = resolve_runtime_execution_context(&left_home, &config, Some("codex"))
            .await
            .expect("resolve left context");
        let right = resolve_runtime_execution_context(&right_home, &config, Some("codex"))
            .await
            .expect("resolve right context");

        assert_ne!(
            left.execution_profiles["codex"].compatibility_key,
            right.execution_profiles["codex"].compatibility_key
        );
    }
}
