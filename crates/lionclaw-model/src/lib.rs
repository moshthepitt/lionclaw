//! The pure deterministic core: identifiers, plan vocabulary, events, state,
//! the fold, validation, and the `next` workflow projection.
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

pub mod child;
pub mod decision;
mod digest;
pub mod event;
pub mod failure;
pub mod fold;
pub mod ids;
pub mod network;
pub mod oracle;
pub mod plan;
pub mod plan_validation;
pub mod state;
pub mod team;
pub mod verdict;
pub mod workflow;

pub use child::{
    ChildMissionAssignment, ChildMissionOutput, ChildMissionReceipt, ChildMissionRequest,
    ChildProofSummary, MissionLineage, MAX_CHILD_MISSION_DEPTH, MAX_CHILD_MISSION_DESCENDANTS,
    MAX_CHILD_MISSION_OBJECTIVE_BYTES, MAX_CHILD_MISSION_REQUEST_BYTES,
};
pub use decision::{validate_decision, DecisionError};
pub use event::{
    resolve_execution_deadline_ms, role_assignment_prompt_template, role_prompt_template,
    role_success_contract_error, ArtifactOutcome, BlobRef, ContinueMode, ControlAction,
    DecisionAction, EffectEventClass, EffectResource, EnvironmentAssignment, EnvironmentPreflight,
    EventEnvelope, ExecutionPolicy, Gap, GapSeverity, Handoff, MessageReference, MissionConfig,
    MissionEvent, MissionProposal, MissionSkill, MissionTypeRef, OracleRunSuccess, PayloadRef,
    PreparedInputRef, RecoveryConfig, ReportEvidenceRef, RoleInstrumentIdentity,
    RolePromptTemplate, RoleProofFreshness, RoleTurnSuccess, RuntimeConfigurationEvidence,
    RuntimeInstrumentIdentity, SkillInstrumentIdentity, StopBar, TaskCandidateRef,
    UnavailableReferenceCause, ValidationItem, VersionStamps, WorkspacePreparation,
    MAX_EXECUTION_DURATION_SECS, MAX_FINAL_RESPONSE_BYTES, MAX_MESSAGE_BYTES,
    MAX_MESSAGE_RECIPIENTS, MAX_MESSAGE_REFERENCES, MAX_QUEUED_MESSAGES_PER_CONVERSATION,
    MAX_ROLE_REPORT_BYTES, SCHEMA_VERSION,
};
pub use failure::{
    bounded_text as bounded_failure_text, AppliedRuntimeConfiguration,
    RuntimeConfigurationConfirmation, RuntimeUsage, RuntimeUsageCost, RuntimeUsageCostScope,
    RuntimeUsageDetails, TypedFailure, TypedFailureEvidence, FAILURE_TEXT_LIMIT,
};
pub use fold::{apply, fold, REDUCER_VERSION};
pub use ids::{
    short_hex, AssertionId, EffectId, IdError, InputName, MissionId, OracleName, RequirementId,
    RoleInstanceId, TaskId,
};
pub use network::{
    canonical_host, Destination, NetworkGrant, NetworkGrantError, MAX_DESTINATION_HOST_BYTES,
    MAX_DESTINATION_PORTS, MAX_NETWORK_DESTINATIONS,
};
pub use oracle::{
    CommandOracle, ExternalOracle, ExternalOracleDriverAuthIdentity, ExternalOracleDriverId,
    ExternalOracleDriverIdentity, OracleSpec, OracleSpecError, WorkspaceRelativeDir,
    MAX_EXTERNAL_ORACLE_DRIVER_ID_BYTES, MAX_EXTERNAL_ORACLE_REQUEST_BYTES,
    MAX_EXTERNAL_ORACLE_REQUEST_FIELDS, MAX_EXTERNAL_ORACLE_REQUEST_KEY_BYTES,
    MAX_EXTERNAL_ORACLE_REQUEST_VALUE_BYTES, MAX_ORACLE_ARGV_BYTES, MAX_ORACLE_ARG_BYTES,
    MAX_ORACLE_ARG_COUNT, MAX_ORACLE_CWD_BYTES, MAX_ORACLE_ENVIRONMENT_BYTES,
    MAX_ORACLE_ENVIRONMENT_ENTRIES,
};
pub use plan::{
    Assertion, AssertionSupersession, OutputSemantics, Plan, PlanProposal, Requirement,
    RequirementDisposition, RequirementKind, RoleResourceLifetime, Task,
};
pub use plan_validation::{
    validate_mission_proposal, validate_plan, validate_plan_proposal, PlanValidationError,
    ProposalError, MAX_TASK_DEPENDENCIES,
};
pub use state::{
    resolve_role_assignment, resolve_task_assignment, AdvisoryStatus, AppliedResult,
    AssertionState, ConversationLifecycle, ConversationState, DecisionEvidence, DeliveryMarker,
    DurableCancellation, EffectCleanupFailure, FailureFeedback, InflightEffect, MissionState,
    ParkedEffect, PlanningInput, PlanningRefinement, QueuedMessage, ReferenceRecipientPolicy,
    RoleAssignment, RoleAssignmentContext, RoleAttemptAuthority, RoleAttemptDisposition,
    RoleAttemptEvidenceUse, RoleAttemptGeneration, RoleAttemptReceipt, RoleEffectSource,
    RoleTurnProvenance, SettledHandoff, SupersededAssertion, TaskAttemptOutcome,
    TaskRoleAssignment, TaskRuntimeState, TaskStatus, TaskWorkspaceProvenance, TerminalState,
};
pub use team::{
    parse_confinement_size_bytes, validate_environment_entry, AuthorityCeilings, AuthorityGrants,
    ConfinementResources, ConfinementTmpfsResource, MissionGuidance, RoleInstance, TaskAssignment,
    TeamRevision, KERNEL_ENVIRONMENT_KEYS, MAX_GUIDANCE_BYTES, MAX_TMPFS_RESOURCE_OVERRIDES,
};
pub use verdict::{AuthoritativeVerdict, FinishClass};
pub use workflow::{next, Choice, EffectIntent, Next, OracleDispatchIntent, RoleDispatchIntent};
