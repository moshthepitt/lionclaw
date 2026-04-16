//! Execution planning and confinement contracts for runtime launches.
//!
//! This module owns execution planning, backend launch contracts, and shared
//! process utilities so confinement policy stays out of individual adapters.

pub mod backend;
pub mod oci;
pub mod plan;
pub mod planner;
pub(crate) mod process;
pub(crate) mod runtime_auth;
pub use backend::{
    execute_streaming, ExecutionBackend, ExecutionOutput, ExecutionRequest, RuntimeSecretsMount,
};
pub use oci::{
    resolve_oci_image_compatibility_identity, validate_oci_launch_prerequisites,
    OciExecutionBackend,
};
pub use plan::{
    ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionLimits,
    ExecutionPreset, MountAccess, MountSpec, NetworkMode, OciConfinementConfig, RuntimeAuthKind,
    RuntimeProgramSpec, WorkspaceAccess,
};
pub use planner::{
    ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig,
    RuntimeExecutionProfile, BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION,
};
