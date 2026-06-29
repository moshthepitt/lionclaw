use anyhow::{anyhow, Result};

pub mod builtins;
pub mod execution;
mod skill_projection;

pub use builtins::{register_builtin_runtime_adapters, BUILTIN_RUNTIME_MOCK};
pub use execution::{
    execute_attached, execute_captured, execute_streaming, map_host_path_into_runtime_mount,
    resolve_oci_image_compatibility_identity, runtime_native_home_mount_source,
    runtime_skill_mount_target_alias, runtime_state_mount_source, skill_mount_target,
    spawn_interactive, validate_oci_launch_prerequisites, ConfinementBackend, ConfinementConfig,
    EffectiveExecutionPlan, EscapeClass, ExecutionBackend, ExecutionLimits, ExecutionPlanPurpose,
    ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig, ExecutionPreset,
    ExecutionRequest, ExecutionSession, MountAccess, MountSpec, OciConfinementConfig,
    OciExecutionBackend, RuntimeExecutionProfile, RuntimeExecutionSession, RuntimeSecretsMount,
    RuntimeSkillProjectionConfig, RuntimeSkillProjectionFormat, WorkspaceAccess,
    BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION, DRAFTS_MOUNT_TARGET,
    RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET, SKILLS_MOUNT_TARGET_ROOT,
};
pub use lionclaw_runtime_api::*;
pub use lionclaw_runtime_mock::MockRuntimeAdapter;
pub use skill_projection::project_runtime_skills;

async fn validate_runtime_auth_prerequisites(
    runtime_id: &str,
    required_auth: Option<&RuntimeAuthKind>,
    auth_registry: &RuntimeAuthRegistry,
    auth_context: &RuntimeAuthContext,
) -> Result<()> {
    let Some(required_auth) = required_auth else {
        return Ok(());
    };

    let provider = auth_registry.get(required_auth).ok_or_else(|| {
        anyhow!(
            "configured runtime profile '{runtime_id}' requires unsupported runtime auth kind '{required_auth}'"
        )
    })?;
    provider
        .validate(auth_context)
        .await
        .map_err(|err| anyhow!("configured runtime profile '{runtime_id}' requires {err}"))
}

pub async fn validate_runtime_launch_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    auth_registry: &RuntimeAuthRegistry,
    auth_context: &RuntimeAuthContext,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        auth_registry,
        auth_context,
        None,
    )
    .await
}

pub async fn validate_runtime_execution_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    auth_registry: &RuntimeAuthRegistry,
    auth_context: &RuntimeAuthContext,
    network_mode: NetworkMode,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        auth_registry,
        auth_context,
        Some(network_mode),
    )
    .await
}

async fn validate_runtime_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    auth_registry: &RuntimeAuthRegistry,
    auth_context: &RuntimeAuthContext,
    network_mode: Option<NetworkMode>,
) -> Result<()> {
    validate_runtime_auth_prerequisites(
        runtime_id,
        required_auth.as_ref(),
        auth_registry,
        auth_context,
    )
    .await?;

    match confinement {
        ConfinementConfig::Oci(config) => {
            validate_oci_launch_prerequisites(runtime_id, config, required_auth).await?;
            if network_mode == Some(NetworkMode::On) {
                execution::oci::validate_oci_private_network_prerequisites(runtime_id, config)
                    .await?;
            }
            Ok(())
        }
    }
}
