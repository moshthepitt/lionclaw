//! Execution planning and confinement contracts for runtime launches.
//!
//! This module owns execution planning, backend launch contracts, and shared
//! process utilities so confinement policy stays out of individual adapters.

pub mod backend;
pub mod mount_validation;
pub mod oci;
pub mod plan;
pub mod planner;
pub(crate) mod process;
pub(crate) mod runtime_auth;
pub use backend::{
    execute_attached, execute_captured, execute_streaming, spawn_interactive, ExecutionBackend,
    ExecutionOutput, ExecutionRequest, ExecutionSession, RuntimeExecutionSession,
    RuntimeSecretsMount,
};
pub use oci::{
    resolve_oci_image_compatibility_identity, validate_oci_launch_prerequisites,
    OciExecutionBackend,
};
pub use plan::{
    mount_source_for_target, runtime_native_home_mount_source, runtime_skill_mount_target_alias,
    runtime_state_mount_source, skill_mount_target, ConfinementBackend, ConfinementConfig,
    EffectiveExecutionPlan, EscapeClass, ExecutionLimits, ExecutionPreset, MountAccess, MountSpec,
    NetworkMode, OciConfinementConfig, RuntimeAuthKind, RuntimeProgramSpec, WorkspaceAccess,
    DRAFTS_MOUNT_TARGET, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET, SKILLS_MOUNT_TARGET_ROOT,
    WORKSPACE_MOUNT_TARGET,
};
pub use planner::{
    ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig,
    RuntimeExecutionProfile, BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION,
};
