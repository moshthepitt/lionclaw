//! Execution planning and confinement contracts for runtime launches.
//!
//! This module owns execution planning, backend launch contracts, and shared
//! process utilities so confinement policy stays out of individual adapters.

pub(crate) mod auth_sidecar;
pub mod backend;
pub mod oci;
pub mod plan;
pub mod planner;
pub(crate) mod process;

pub use backend::{
    execute_streaming, ExecutionBackend, ExecutionOutput, ExecutionRequest, RuntimeSecretsMount,
};
pub use oci::OciExecutionBackend;
pub use plan::{
    ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionLimits,
    ExecutionPreset, MountAccess, MountSpec, NetworkMode, OciConfinementConfig, RuntimeAuthKind,
    RuntimeProgramSpec, WorkspaceAccess,
};
pub use planner::{
    ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig,
    RuntimeExecutionProfile, BUILTIN_PRESET_EVERYDAY, BUILTIN_PRESET_HIDDEN_COMPACTION,
};
