//! The pure deterministic core: identifiers, plan vocabulary, events, state,
//! the fold, validation, and the step decision function.
//!
//! Dependency wall: everything in this module imports only `std`, `serde`,
//! and `thiserror` (error derives). No I/O, no clock, no RNG, no async — the
//! fold-litmus test depends on it.

pub mod decision;
pub mod event;
pub mod fold;
pub mod gate;
pub mod ids;
pub mod plan;
pub mod plan_validation;
pub mod state;
pub mod step;
pub mod verdict;

pub use decision::{validate_decision, DecisionError};
pub use event::{
    AmendmentOps, ArtifactOutcome, BlobRef, DecisionAction, EventEnvelope, Gap, GapSeverity,
    Handoff, IdemClass, MissionConfig, MissionEvent, MissionTypeRef, OracleBinding, PayloadRef,
    RunErrorKind, StopBar, Supersession, TerminalReviewConfig, ValidationItem, VersionStamps,
    SCHEMA_VERSION,
};
pub use fold::{apply, fold, REDUCER_VERSION};
pub use gate::{evaluate_gate, GateResult};
pub use ids::{short_hex, AssertionId, IdError, MissionId, OracleName, RoleName, TaskId};
pub use plan::{
    Assertion, OutputSemantics, PlanSubmission, PlanningDag, PlanningTask, Task, TaskKind,
};
pub use plan_validation::{
    validate_plan_amendment, validate_plan_submission, validate_planning_dag, AmendmentError,
    MissionTypeInventory, PlanValidationError,
};
pub use state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, PlanningState, ReviewAcceptance, ReviewAcceptanceKind, ReviewOutcome,
    TaskRuntimeState, TaskStatus, TerminalReviewState, TerminalReviewVerdict,
};
pub use step::{
    step, OracleDispatchIntent, RoleDispatchIntent, StepDecision, TerminalReviewDispatchIntent,
};
pub use verdict::{classify_finish, AuthoritativeVerdict, FinishClass};
