//! The pure deterministic core: identifiers, plan vocabulary, events, state,
//! the fold, validation, and the step decision function.
//!
//! Dependency wall: everything in this module imports only `std`, `serde`,
//! and `thiserror` (error derives). No I/O, no clock, no RNG, no async — the
//! fold-litmus test depends on it.

pub mod event;
pub mod fold;
pub mod gate;
pub mod ids;
pub mod plan;
pub mod plan_validation;
pub mod state;
pub mod step;
pub mod verdict;

pub use event::{
    ArtifactOutcome, BlobRef, EventEnvelope, Handoff, IdemClass, MissionConfig, MissionEvent,
    PayloadRef, RunErrorKind, StopBar, ValidationItem, VersionStamps, SCHEMA_VERSION,
};
pub use fold::{apply, fold, REDUCER_VERSION};
pub use gate::{evaluate_gate, GateResult};
pub use ids::{AssertionId, IdError, MissionId, OracleName, RoleName, TaskId};
pub use plan::{derive_kind, Assertion, OutputSemantics, PlanSubmission, Task, TaskKind};
pub use plan_validation::{validate_plan_submission, PlanValidationError, PluginInventory};
pub use state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, TaskRuntimeState, TaskStatus,
};
pub use step::{step, OracleDispatchIntent, RoleDispatchIntent, StepDecision};
pub use verdict::{classify_finish, AuthoritativeVerdict, FinishClass};
