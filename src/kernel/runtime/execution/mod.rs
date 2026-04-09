//! Execution planning and confinement contracts for runtime launches.
//!
//! This module owns execution planning, backend launch contracts, and shared
//! process utilities so confinement policy stays out of individual adapters.

pub mod backend;
pub mod oci;
pub mod plan;
pub mod planner;
pub(crate) mod process;

pub use backend::{execute_streaming, ExecutionBackend, ExecutionOutput, ExecutionRequest};
pub use oci::OciExecutionBackend;
pub use plan::{
    ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionLimits,
    ExecutionPreset, MountAccess, MountSpec, NetworkMode, OciConfinementConfig, RuntimeProgramSpec,
    SecretBinding, SecretBindingKind, WorkspaceAccess,
};
pub use planner::{
    ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig, RuntimeExecutionProfile,
    BUILTIN_PRESET_EVERYDAY,
};
