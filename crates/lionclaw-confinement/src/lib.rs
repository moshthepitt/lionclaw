//! Execution confinement contracts and backends, extracted from the LionClaw
//! kernel so confinement policy can be consumed as a narrow library without
//! pulling in the kernel itself.
//!
//! Owns the authority vocabulary (`ExecutionPreset`, `EscapeClass`, mounts),
//! the compiled `EffectiveExecutionPlan`, and the OCI (podman) execution
//! backend that enforces it.

pub mod backend;
pub mod mount_validation;
pub mod oci;
pub mod plan;
pub mod process;
pub mod runtime_auth;
pub mod skill_alias;

pub use backend::{
    execute_attached, execute_captured, execute_streaming, spawn_interactive, ExecutionBackend,
    ExecutionOutput, ExecutionRequest, ExecutionSession, RuntimeExecutionSession,
    RuntimeSecretsMount,
};
pub use mount_validation::{parse_runtime_tmpfs_entry, RuntimeTmpfsEntry};
pub use oci::{
    remove_oci_container, remove_oci_secret, resolve_oci_image_compatibility_identity,
    validate_oci_launch_prerequisites, OciExecutionBackend,
};
pub use plan::{
    map_host_path_into_runtime_mount, mount_source_for_target, runtime_native_home_mount_source,
    runtime_state_mount_source, ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan,
    EscapeClass, ExecutionLimits, ExecutionPreset, InstallPolicy, MountAccess, MountSpec,
    NetworkMode, OciConfinementConfig, RuntimeAuthKind, RuntimeProgramSpec, WorkspaceAccess,
    DRAFTS_MOUNT_TARGET, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_INSTALL_ENV_DIR,
    RUNTIME_INSTALL_ENV_FILE, RUNTIME_INSTALL_ENV_PATH, RUNTIME_MOUNT_TARGET,
    WORKSPACE_MOUNT_TARGET,
};
pub use skill_alias::{validate_skill_alias, SkillAliasValidationError};
