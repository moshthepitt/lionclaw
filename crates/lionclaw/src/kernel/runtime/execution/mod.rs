//! Execution planning for runtime launches.
//!
//! The confinement contracts and backends live in the `lionclaw-confinement`
//! crate; this module re-exports them for kernel callers and keeps the
//! kernel-specific execution planner.

pub mod planner;

pub use lionclaw_confinement::{backend, mount_validation, oci, plan, process, runtime_auth};
pub use lionclaw_confinement::{
    execute_attached, execute_captured, execute_streaming, map_host_path_into_runtime_mount,
    mount_source_for_target, resolve_oci_image_compatibility_identity,
    runtime_native_home_mount_source, runtime_skill_mount_target_alias,
    runtime_state_mount_source, skill_mount_target, spawn_interactive,
    validate_oci_launch_prerequisites, ConfinementBackend, ConfinementConfig,
    EffectiveExecutionPlan, EscapeClass, ExecutionBackend, ExecutionLimits, ExecutionOutput,
    ExecutionPreset, ExecutionRequest, ExecutionSession, InstallPolicy, MountAccess, MountSpec,
    NetworkMode, OciConfinementConfig, OciExecutionBackend, RuntimeAuthKind, RuntimeProgramSpec,
    RuntimeExecutionSession, RuntimeSecretsMount, RuntimeSkillProjectionConfig,
    RuntimeSkillProjectionFormat, WorkspaceAccess, DRAFTS_MOUNT_TARGET, RUNTIME_HOME_MOUNT_TARGET,
    RUNTIME_INSTALL_ENV_DIR, RUNTIME_INSTALL_ENV_FILE, RUNTIME_INSTALL_ENV_PATH,
    RUNTIME_MOUNT_TARGET, SKILLS_MOUNT_TARGET_ROOT, WORKSPACE_MOUNT_TARGET,
};
pub use planner::{
    ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig,
    RuntimeExecutionProfile, BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION,
};
