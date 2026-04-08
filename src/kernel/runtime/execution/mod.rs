//! Execution planning and confinement contracts for runtime launches.
//!
//! Phase 0 introduces these types without changing runtime behavior so later
//! phases can move launch policy out of individual adapters.

pub mod backend;
pub mod oci;
pub mod plan;

pub use backend::{ExecutionBackend, ExecutionOutput, ExecutionRequest};
pub use oci::OciExecutionBackend;
pub use plan::{
    ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionLimits,
    ExecutionPreset, MountAccess, MountSpec, NetworkMode, OciConfinementConfig, RuntimeProgramSpec,
    SecretBinding, SecretBindingKind, WorkspaceAccess,
};
