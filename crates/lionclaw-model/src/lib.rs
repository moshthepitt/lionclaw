//! The pure deterministic core: identifiers, plan vocabulary, events, state,
//! the fold, validation, and the step decision function.
//!
//! Dependency wall: this crate has only deterministic value/hash dependencies.
//! No I/O, clock, RNG, or async dependencies may enter the kernel model.

#![no_std]

extern crate alloc;

#[cfg(test)]
extern crate std;

mod prelude {
    pub(crate) use alloc::{
        boxed::Box,
        collections::{BTreeMap, BTreeSet},
        format,
        string::{String, ToString},
        vec,
        vec::Vec,
    };
}

pub mod decision;
pub mod event;
pub mod failure;
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
    resolve_execution_deadline_ms, role_success_contract_error, ArtifactOutcome, BlobRef,
    ControlAction, DecisionAction, EffectEventClass, EffectResource, EventEnvelope,
    ExecutionPolicy, Gap, GapSeverity, Handoff, MissionConfig, MissionEvent, MissionTypeRef,
    OracleRunSuccess, PayloadRef, PreparedInputRef, RecoveryConfig, RoleRunSuccess,
    RuntimeConfigurationEvidence, StopBar, TaskNamespace, TerminalReviewConfig,
    TerminalReviewSuccess, ValidationItem, VersionStamps, MAX_EXECUTION_DURATION_SECS,
    SCHEMA_VERSION,
};
pub use failure::{
    bounded_text as bounded_failure_text, AppliedRuntimeConfiguration,
    RuntimeConfigurationConfirmation, TypedFailure, TypedFailureEvidence, FAILURE_TEXT_LIMIT,
};
pub use fold::{apply, fold, REDUCER_VERSION};
pub use gate::{evaluate_gate, GateResult};
pub use ids::{
    short_hex, AssertionId, EffectId, IdError, InputName, MissionId, OracleName, RequirementId,
    RoleName, TaskId, TERMINAL_REVIEW_TASK_TAG,
};
pub use plan::{
    Assertion, OutputSemantics, Plan, PlanProposal, PlanningDag, PlanningTask, Requirement,
    RequirementDisposition, RequirementKind, Task, TaskKind,
};
pub use plan_validation::{
    validate_plan, validate_plan_proposal, validate_planning_dag, MissionTypeInventory,
    PlanValidationError, ProposalError,
};
pub use state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, EffectCleanupFailure,
    FailureEvidence, FailureFeedback, InflightEffect, MissionPhase, MissionState, ParkedEffect,
    PlanningInput, PlanningRefinement, PlanningState, ReviewAcceptance, ReviewAcceptanceKind,
    ReviewOutcome, TaskAddress, TaskRuntimeState, TaskStatus, TerminalReviewState,
    TerminalReviewVerdict,
};
pub use step::{
    step, OracleDispatchIntent, RoleDispatchIntent, StepDecision, TerminalReviewDispatchIntent,
};
pub use verdict::{classify_finish, AuthoritativeVerdict, FinishClass};
