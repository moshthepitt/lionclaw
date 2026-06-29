use std::path::Path;

use anyhow::{anyhow, Result};

pub mod builtins;
mod codex_host_auth;
pub mod execution;
mod skill_projection;

pub use builtins::{register_builtin_runtime_adapters, BUILTIN_RUNTIME_MOCK};
pub use codex_host_auth::{ensure_codex_host_auth_ready, sync_codex_home_into_runtime_home};
pub use execution::{
    execute_attached, execute_captured, execute_streaming, map_host_path_into_runtime_mount,
    resolve_oci_image_compatibility_identity, runtime_native_home_mount_source,
    runtime_skill_mount_target_alias, runtime_state_mount_source, skill_mount_target,
    spawn_interactive, validate_oci_launch_prerequisites, ConfinementBackend, ConfinementConfig,
    EffectiveExecutionPlan, EscapeClass, ExecutionBackend, ExecutionLimits, ExecutionPlanPurpose,
    ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig, ExecutionPreset,
    ExecutionRequest, ExecutionSession, MountAccess, MountSpec, OciConfinementConfig,
    OciExecutionBackend, RuntimeExecutionProfile, RuntimeExecutionSession, RuntimeSecretsMount,
    WorkspaceAccess, BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION,
    DRAFTS_MOUNT_TARGET, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET, SKILLS_MOUNT_TARGET_ROOT,
};
pub use lionclaw_runtime_acp::{AcpRuntimeAdapter, AcpRuntimeConfig};
pub use lionclaw_runtime_api::*;
pub use lionclaw_runtime_codex::{CodexRuntimeAdapter, CodexRuntimeConfig};
pub use lionclaw_runtime_mock::MockRuntimeAdapter;
pub use skill_projection::project_runtime_skills;

async fn validate_runtime_auth_prerequisites(
    runtime_id: &str,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    let Some(required_auth) = required_auth else {
        return Ok(());
    };

    match required_auth {
        RuntimeAuthKind::Codex => ensure_codex_host_auth_ready(codex_home_override)
            .await
            .map_err(|err| anyhow!("configured runtime profile '{runtime_id}' requires {err}")),
    }
}

pub async fn validate_runtime_launch_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        codex_home_override,
        None,
    )
    .await
}

pub async fn validate_runtime_execution_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
    network_mode: NetworkMode,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        codex_home_override,
        Some(network_mode),
    )
    .await
}

async fn validate_runtime_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
    network_mode: Option<NetworkMode>,
) -> Result<()> {
    validate_runtime_auth_prerequisites(runtime_id, required_auth, codex_home_override).await?;

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
