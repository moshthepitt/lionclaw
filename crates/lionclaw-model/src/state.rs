//! `MissionState` — a pure fold over the event log. Deterministic containers
//! only (`BTreeMap`), derives `PartialEq` so the fold-litmus test can assert
//! rebuilt state equality. Wall-clock time never enters this type.

use serde::{Deserialize, Serialize};

use super::digest::CanonicalDigest;
use super::event::{
    EffectResource, EnvironmentAssignment, MissionConfig, MissionTypeRef, PayloadRef,
    PreparedInputRef, ReportEvidenceRef, RoleInstrumentIdentity, RoleProofFreshness,
    RuntimeConfigurationEvidence, RuntimeInstrumentIdentity, SkillInstrumentIdentity,
    TaskCandidateRef,
};
use super::ids::{AssertionId, MissionId, OracleName, RoleInstanceId, TaskId};
use super::plan::{Assertion, OutputSemantics, Plan};
use super::verdict::{AuthoritativeVerdict, FinishClass};
use crate::prelude::*;
use crate::{RuntimeUsage, TypedFailure, TypedFailureEvidence};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationLifecycle {
    Ready,
    Running,
    AwaitingLead,
    ReworkingInvalidHandoff,
    Completed,
    /// This generation was permanently replaced or its owning work ended.
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMarker {
    Queued,
    PreviouslyDelivered,
    PossiblyDelivered,
    /// The conversation lost delivery authority before this message settled.
    Undeliverable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedMessage {
    pub sequence_no: u64,
    pub body: String,
    pub references: Vec<super::MessageReference>,
    pub marker: DeliveryMarker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationState {
    pub role_instance: RoleInstanceId,
    pub lifecycle: ConversationLifecycle,
    /// Exact role attempt whose disposable conversation scratch still needs
    /// reconciliation after this conversation settles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposable_resource_owner: Option<super::EffectId>,
    pub queued: Vec<QueuedMessage>,
    pub consumed_through: u64,
    pub active_delivery: Option<ActiveDelivery>,
    /// Latest response produced by this exact conversation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_response: Option<super::PayloadRef>,
    pub invalid_handoff_reworks: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveDelivery {
    pub effect_id: super::EffectId,
    pub message_boundary: u64,
    pub presented_messages: Vec<u64>,
}

/// A durable cancellation fact that dominates any later effect outcome.
/// Event order chooses one cause; both the live engine and pure replay use
/// this value so the shell cannot grant success that the reducer rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableCancellation {
    Aborted { reason: String },
    Stopped { reason: String },
    DeadlineReached { deadline_ms: i64 },
}

impl DurableCancellation {
    pub fn into_failure(self, mut evidence: TypedFailureEvidence) -> TypedFailure {
        let (code, detail, reason) = match &self {
            Self::Aborted { reason } => (
                "control.aborted_before_settlement",
                "mission abort became durable before the effect outcome",
                reason.clone(),
            ),
            Self::Stopped { reason } => (
                "control.stopped_before_settlement",
                "operator stop became durable before the effect outcome",
                reason.clone(),
            ),
            Self::DeadlineReached { deadline_ms } => (
                "control.deadline_before_settlement",
                "the recorded effect deadline became durable before the effect outcome",
                format!("deadline reached at {deadline_ms}"),
            ),
        };
        evidence.code = Some(code.into());
        evidence.detail = detail.into();
        evidence.stop_reason = Some(reason);
        match self {
            Self::Aborted { .. } => TypedFailure::OperatorAborted {
                evidence: Box::new(evidence),
            },
            Self::Stopped { .. } => TypedFailure::OperatorStopped {
                evidence: Box::new(evidence),
            },
            Self::DeadlineReached { .. } => TypedFailure::DeadlineExhausted {
                evidence: Box::new(evidence),
            },
        }
        .projected()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TerminalState {
    Done { finish: FinishClass },
    Aborted { reason: String },
}

impl TerminalState {
    /// The finish grade, if this is a completed terminal state.
    pub const fn finish(&self) -> Option<FinishClass> {
        match self {
            Self::Done { finish } => Some(*finish),
            _ => None,
        }
    }

    /// The stable snake_case terminal variant name. The `Done`/`Aborted`
    /// payloads are not part of the slug; callers that want the finish grade
    /// compose it from [`FinishClass::slug`].
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Done { .. } => "done",
            Self::Aborted { .. } => "aborted",
        }
    }

    pub fn display_slug(&self) -> String {
        match self {
            Self::Done { finish } => format!("done:{}", finish.slug()),
            Self::Aborted { .. } => "aborted".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedResult {
    pub branch: String,
    pub sha: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Cleared,
    Failed,
    /// Retired by a later plan revision. A tombstone: the
    /// task is removed from the live `plan.tasks`, so no derivation dispatches
    /// or judges it; this row survives in `tasks` (with its `attempts`) for
    /// audit. Never transitions to any other status.
    Superseded,
}

/// Exact folded provenance and lifecycle evidence for one role attempt.
///
/// The fold creates one receipt from the matching active request, then only
/// advances its observation and disposition. Tasks, advisories, reviews, and
/// operator projections retain the effect ID and resolve this record rather
/// than copying evidence into mutable "latest" slots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleAttemptReceipt {
    pub effect_id: super::EffectId,
    pub source: RoleEffectSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_configuration: Option<RuntimeConfigurationEvidence>,
    #[serde(default)]
    pub runtime_usage: RuntimeUsage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_inputs: Vec<PreparedInputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_response: Option<PayloadRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<super::Handoff>,
    pub disposition: RoleAttemptDisposition,
}

/// Whether current folded state still uses a role attempt as evidence.
///
/// This is deliberately distinct from the role generation's lifecycle: a
/// superseded generation can remain the current evidence for replanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleAttemptEvidenceUse {
    Current,
    Historical,
}

impl RoleAttemptEvidenceUse {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Historical => "historical",
        }
    }
}

/// Whether the role attempt belongs to the mission's current contract
/// generation or to a retired/superseded generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleAttemptGeneration {
    Current,
    Superseded,
}

impl RoleAttemptGeneration {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAttemptAuthority {
    pub evidence_use: RoleAttemptEvidenceUse,
    pub generation: RoleAttemptGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoleEffectSource {
    Turn {
        request: Box<RoleTurnProvenance>,
        plan_revision: u32,
    },
}

/// Folded provenance copied only from an accepted role-turn request. Outcome
/// events carry the effect id and result; they cannot restate this authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleTurnProvenance {
    pub role_instance: RoleInstanceId,
    pub team_revision: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertion_ids: Vec<AssertionId>,
    pub attempt_no: u32,
    pub assignment_epoch: u32,
    pub prompt_template: super::RolePromptTemplate,
    pub prompt_hash: String,
    pub base_sha: String,
    pub environment_digest: String,
    pub instrument_identity: RoleInstrumentIdentity,
    pub dependency_refs: Vec<TaskCandidateRef>,
    pub report_refs: Vec<ReportEvidenceRef>,
    pub workspace_preparation: super::WorkspacePreparation,
    pub message_boundary: u64,
    pub presented_messages: Vec<u64>,
}

impl RoleTurnProvenance {
    pub fn freshness(&self) -> RoleProofFreshness {
        RoleProofFreshness {
            judged_sha: self.base_sha.clone(),
            environment_digest: self.environment_digest.clone(),
            instrument_identity: self.instrument_identity.clone(),
        }
    }

    pub fn is_fresh_at(&self, state: &MissionState) -> bool {
        self.freshness().is_fresh_at(state)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoleAttemptDisposition {
    Active,
    Retired,
    Succeeded {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        handoff: Option<Box<SettledHandoff>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact: Option<super::ArtifactOutcome>,
    },
    Failed {
        failure: TypedFailure,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SettledHandoff {
    Work {
        request_attention: bool,
    },
    Validate {
        items: Vec<super::ValidationItem>,
        passed: bool,
        request_attention: bool,
    },
    Review {
        passed: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        gaps: Vec<super::Gap>,
    },
    Plan {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proposal: Option<Box<super::MissionProposal>>,
        request_attention: bool,
    },
}

impl SettledHandoff {
    pub fn from_handoff(handoff: &super::Handoff) -> Self {
        match handoff {
            super::Handoff::Work {
                request_attention, ..
            } => Self::Work {
                request_attention: *request_attention,
            },
            super::Handoff::Validate {
                items,
                passed,
                request_attention,
                ..
            } => Self::Validate {
                items: items.clone(),
                passed: *passed,
                request_attention: *request_attention,
            },
            super::Handoff::Review { passed, gaps, .. } => Self::Review {
                passed: *passed,
                gaps: gaps.clone(),
            },
            super::Handoff::Plan {
                proposal,
                request_attention,
                ..
            } => Self::Plan {
                proposal: proposal.clone(),
                request_attention: *request_attention,
            },
        }
    }
}

impl RoleAttemptReceipt {
    /// Canonical folded runtime configuration for this attempt.
    ///
    /// Canonical folded runtime configuration from `RoleTurnCompleted`.
    pub fn effective_runtime_configuration(&self) -> Option<&RuntimeConfigurationEvidence> {
        self.runtime_configuration.as_ref()
    }

    pub fn accepted_report(&self) -> Option<&PayloadRef> {
        self.handoff.as_ref().map(super::Handoff::report)
    }

    pub fn rejection(&self) -> Option<&TypedFailure> {
        self.failure().filter(|failure| failure.is_invalid_output())
    }

    pub fn failure(&self) -> Option<&TypedFailure> {
        match &self.disposition {
            RoleAttemptDisposition::Failed { failure } => Some(failure),
            RoleAttemptDisposition::Active
            | RoleAttemptDisposition::Retired
            | RoleAttemptDisposition::Succeeded { .. } => None,
        }
    }

    pub fn settled_handoff(&self) -> Option<&SettledHandoff> {
        match &self.disposition {
            RoleAttemptDisposition::Succeeded { handoff, .. } => handoff.as_deref(),
            RoleAttemptDisposition::Active
            | RoleAttemptDisposition::Retired
            | RoleAttemptDisposition::Failed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskAttemptOutcome {
    Accepted { effect_id: super::EffectId },
    Failed { effect_id: super::EffectId },
}

impl TaskAttemptOutcome {
    pub const fn effect_id(&self) -> &super::EffectId {
        match self {
            Self::Accepted { effect_id } | Self::Failed { effect_id } => effect_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRuntimeState {
    pub status: TaskStatus,
    /// Monotonic dispatch identity. Successful reruns never reset it.
    pub attempts: u32,
    /// Failures since the last successful outcome or explicit recovery
    /// decision. This, not the dispatch identity, bounds automatic recovery.
    pub consecutive_failures: u32,
    /// The latest settled attempt. Accepted report authority and failed
    /// evidence are mutually exclusive by construction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<TaskAttemptOutcome>,
    /// Current candidate commit for this task lineage. A task that completed
    /// with no captured commit inherits its assignment base. A downstream
    /// repair clears this field until the task is re-owed and reruns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_sha: Option<String>,
    /// Exact base required for a repaired task's next non-retry attempt. This
    /// is set from the deliverable head that made the failed proof current and
    /// cleared once the replacement candidate settles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_base_sha: Option<String>,
    /// Engine-routed repair feedback for this task's next attempt.
    #[serde(default)]
    pub feedback: Vec<FailureFeedback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_assignment: Option<TaskRoleAssignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_provenance: Option<TaskWorkspaceProvenance>,
    /// Exact parked writer effect whose retained checkout the next request must
    /// archive before rebuilding. Cleared only by matching preparation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_workspace_recreation: Option<super::EffectId>,
}

/// The sole folded authority for one task's current role generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRoleAssignment {
    pub role_instance: RoleInstanceId,
    pub team_revision: u32,
    pub base_sha: String,
    pub assignment_epoch: u32,
}

/// Atomic retained authority for one writer checkout. The fold replaces this
/// record only after the exact active effect confirms preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskWorkspaceProvenance {
    pub effect_id: super::EffectId,
    pub base_sha: String,
    pub assignment_epoch: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_effect_id: Option<super::EffectId>,
}

/// Resolve one fresh or retry assignment from durable task state. The engine
/// and replay fold share this candidate; only an exact validated request fold
/// installs it as the task's role-assignment authority.
pub fn resolve_task_assignment(
    previous: Option<&TaskRuntimeState>,
    required_base: &str,
    lifecycle_generation: u32,
    retrying_failure: bool,
) -> (String, u32, super::WorkspacePreparation) {
    let previous_assignment = previous.and_then(|task| task.role_assignment.as_ref());
    let base_sha = if retrying_failure {
        previous_assignment
            .map(|assignment| assignment.base_sha.clone())
            .unwrap_or_else(|| required_base.to_string())
    } else {
        required_base.to_string()
    };
    let workspace_preparation =
        match previous.and_then(|task| task.pending_workspace_recreation.as_ref()) {
            Some(parked_effect_id) => super::WorkspacePreparation::ArchiveAndReset {
                parked_effect_id: parked_effect_id.clone(),
            },
            None if previous
                .and_then(|task| task.workspace_provenance.as_ref())
                .map(|workspace| workspace.base_sha.as_str())
                != Some(base_sha.as_str()) =>
            {
                super::WorkspacePreparation::ResetForAssignment
            }
            None => super::WorkspacePreparation::Preserve,
        };
    let generation_floor = lifecycle_generation.max(1);
    let epoch = match previous_assignment {
        None => generation_floor,
        Some(previous) if previous.base_sha != base_sha => previous
            .assignment_epoch
            .saturating_add(1)
            .max(generation_floor),
        Some(previous) => previous.assignment_epoch.max(generation_floor),
    };
    (base_sha, epoch, workspace_preparation)
}

/// The single authoritative identity derivation for a role assignment.
/// Dispatch, prompt materialization, and replay all consume this value rather
/// than independently inferring a current conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAssignment {
    pub role_instance: RoleInstanceId,
    pub team_revision: u32,
    pub base_sha: String,
    pub dependency_refs: Vec<TaskCandidateRef>,
    pub generation: u32,
    pub workspace_preparation: super::WorkspacePreparation,
}

#[derive(Debug, Clone, Copy)]
pub struct RoleAssignmentContext<'a> {
    pub previous: Option<&'a TaskRuntimeState>,
    pub required_base: &'a str,
    pub dependency_refs: &'a [TaskCandidateRef],
    pub lifecycle_generation: u32,
    pub retrying_failure: bool,
    pub output: super::OutputSemantics,
}

pub fn resolve_role_assignment(
    role_instance: &RoleInstanceId,
    team_revision: u32,
    context: RoleAssignmentContext<'_>,
) -> RoleAssignment {
    let (base_sha, generation, proposed_workspace_preparation) = resolve_task_assignment(
        context.previous,
        context.required_base,
        context.lifecycle_generation,
        context.retrying_failure,
    );
    let workspace_preparation = if context.output == super::OutputSemantics::ProducesArtifact {
        proposed_workspace_preparation
    } else {
        super::WorkspacePreparation::Preserve
    };
    RoleAssignment {
        role_instance: role_instance.clone(),
        team_revision,
        base_sha,
        dependency_refs: context.dependency_refs.to_vec(),
        generation,
        workspace_preparation,
    }
}

impl TaskRuntimeState {
    /// The exact outcome made deliverable by task clearance. This includes a
    /// failed attempt explicitly accepted by the lead; callers must preserve
    /// that failure classification rather than presenting it as success.
    pub fn cleared_outcome(&self) -> Option<&TaskAttemptOutcome> {
        (self.status == TaskStatus::Cleared)
            .then_some(self.last_outcome.as_ref())
            .flatten()
    }
}

/// Exact evidence behind an operator decision or replanning input.
///
/// Role-derived facts retain only receipt identities and resolve their content
/// through `MissionState::role_attempt_receipts`. Completed oracle runs resolve
/// through `MissionState::authoritative_receipts`; runtime failures retain
/// their typed payload because they did not mint an authoritative receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionEvidence {
    None,
    RoleAttempts { effect_ids: Vec<super::EffectId> },
    AuthoritativeReceipts { effect_ids: Vec<super::EffectId> },
    OracleRuntimeFailure { failure: TypedFailure },
}

impl DecisionEvidence {
    pub fn from_role_attempts(effect_ids: Vec<super::EffectId>) -> Self {
        if effect_ids.is_empty() {
            Self::None
        } else {
            Self::RoleAttempts { effect_ids }
        }
    }

    pub fn role_attempts(&self) -> &[super::EffectId] {
        match self {
            Self::RoleAttempts { effect_ids } => effect_ids,
            Self::None | Self::AuthoritativeReceipts { .. } | Self::OracleRuntimeFailure { .. } => {
                &[]
            }
        }
    }

    pub fn authoritative_receipts(&self) -> &[super::EffectId] {
        match self {
            Self::AuthoritativeReceipts { effect_ids } => effect_ids,
            Self::None | Self::RoleAttempts { .. } | Self::OracleRuntimeFailure { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureFeedback {
    pub summary: String,
    pub evidence: DecisionEvidence,
    pub justification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningRefinement {
    Guidance(String),
    FailureEvidence(Vec<FailureFeedback>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningInput {
    /// Latest complete candidate rejected during plan ratification. Kept as
    /// planning input until a candidate is approved.
    pub latest_rejected_proposal: Option<super::MissionProposal>,
    /// The single active refinement input for the next planning pass.
    pub refinement: Option<PlanningRefinement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectCleanupFailure {
    pub effect_id: super::EffectId,
    pub resource: EffectResource,
    pub failure: TypedFailure,
}

/// Fresh per-assertion judged status. Omission counts as pending; a later
/// non-pass receipt downgrades the assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisoryStatus {
    Pending,
    Passed,
    Failed,
}

impl AdvisoryStatus {
    /// The stable snake_case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionState {
    /// The engine-run oracle binding, if any (copied from the plan).
    pub oracle: Option<OracleName>,
    /// Last evidence-bearing verdict per assigned judgment role instance.
    pub last_advisory: BTreeMap<RoleInstanceId, super::EffectId>,
    /// Current engine-run receipt. Only the fold can mint its referenced
    /// verdict, and only from `OracleRunCompleted`.
    pub last_authoritative_receipt: Option<super::EffectId>,
}

/// An assertion receipt retired by an explicit correction. It remains
/// inspectable evidence but cannot satisfy the active contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupersededAssertion {
    pub assertion: Assertion,
    pub state: AssertionState,
    pub replacement_ids: Vec<AssertionId>,
    pub superseded_at_revision: u32,
}

/// Exact failed effect generation that may be reopened by `continue`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParkedEffect {
    RoleTurn {
        role_instance: RoleInstanceId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<TaskId>,
    },
    OracleRun {
        oracle: OracleName,
    },
}

/// Fold-authoritative policy for attaching durable evidence to lead messages.
/// Recipient identity is validated separately; this closed result keeps output
/// semantics identical at live ingress and replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferenceRecipientPolicy {
    Permitted,
    Disallowed {
        role_instance: RoleInstanceId,
        output: super::OutputSemantics,
    },
    Mixed,
    Invalid,
}

/// A `…Requested` event without a recorded outcome. The active driver executes
/// it; a later driver cleans and marks it interrupted rather than replaying it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InflightEffect {
    RoleTurn {
        role_instance: RoleInstanceId,
        team_revision: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<TaskId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        assertion_ids: Vec<AssertionId>,
        attempt_no: u32,
        output: super::OutputSemantics,
        runtime: String,
        prompt_template: super::RolePromptTemplate,
        prompt_hash: String,
        base_sha: String,
        environment_digest: String,
        instrument_identity: Box<RoleInstrumentIdentity>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dependency_refs: Vec<TaskCandidateRef>,
        report_refs: Vec<ReportEvidenceRef>,
        assignment_epoch: u32,
        message_boundary: u64,
        presented_messages: Vec<u64>,
        workspace_preparation: super::WorkspacePreparation,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        budget_deadline_ms: i64,
        requested_seq: u64,
    },
    OracleRun {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        spec_digest: String,
        judged_sha: String,
        environment_digest: String,
        attempt_no: u32,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        requested_seq: u64,
    },
    ChildMission {
        request: Box<super::ChildMissionRequest>,
        #[serde(default)]
        bound: bool,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        budget_deadline_ms: i64,
        requested_seq: u64,
    },
}

impl InflightEffect {
    /// Bind one receipt to the exact active request that produced it. The fold
    /// supplies plan revision and validator targets from authoritative state;
    /// events cannot choose any provenance field.
    pub fn role_attempt_receipt(
        &self,
        effect_id: &super::EffectId,
        plan_revision: u32,
    ) -> Option<RoleAttemptReceipt> {
        let source = match self {
            Self::RoleTurn { .. } => RoleEffectSource::Turn {
                request: Box::new(self.role_turn_provenance()?),
                plan_revision,
            },
            Self::OracleRun { .. } | Self::ChildMission { .. } => return None,
        };
        Some(RoleAttemptReceipt {
            effect_id: effect_id.clone(),
            source,
            runtime_configuration: None,
            runtime_usage: RuntimeUsage::NotReported,
            prepared_inputs: Vec::new(),
            final_response: None,
            handoff: None,
            disposition: RoleAttemptDisposition::Active,
        })
    }

    pub fn role_turn_provenance(&self) -> Option<RoleTurnProvenance> {
        let Self::RoleTurn {
            role_instance,
            team_revision,
            task_id,
            assertion_ids,
            attempt_no,
            prompt_template,
            prompt_hash,
            base_sha,
            environment_digest,
            instrument_identity,
            dependency_refs,
            report_refs,
            assignment_epoch,
            message_boundary,
            presented_messages,
            workspace_preparation,
            ..
        } = self
        else {
            return None;
        };
        Some(RoleTurnProvenance {
            role_instance: role_instance.clone(),
            team_revision: *team_revision,
            task_id: task_id.clone(),
            assertion_ids: assertion_ids.clone(),
            attempt_no: *attempt_no,
            assignment_epoch: *assignment_epoch,
            prompt_template: *prompt_template,
            prompt_hash: prompt_hash.clone(),
            base_sha: base_sha.clone(),
            environment_digest: environment_digest.clone(),
            instrument_identity: instrument_identity.as_ref().clone(),
            dependency_refs: dependency_refs.clone(),
            report_refs: report_refs.clone(),
            workspace_preparation: workspace_preparation.clone(),
            message_boundary: *message_boundary,
            presented_messages: presented_messages.clone(),
        })
    }

    pub fn set_deadline_ms(&mut self, new_deadline_ms: i64) {
        match self {
            Self::RoleTurn { deadline_ms, .. }
            | Self::OracleRun { deadline_ms, .. }
            | Self::ChildMission { deadline_ms, .. } => *deadline_ms = new_deadline_ms,
        }
    }

    pub fn deadline_ms(&self) -> i64 {
        match self {
            Self::RoleTurn { deadline_ms, .. }
            | Self::OracleRun { deadline_ms, .. }
            | Self::ChildMission { deadline_ms, .. } => *deadline_ms,
        }
    }

    pub fn not_before_ms(&self) -> i64 {
        match self {
            Self::RoleTurn { not_before_ms, .. }
            | Self::OracleRun { not_before_ms, .. }
            | Self::ChildMission { not_before_ms, .. } => *not_before_ms,
        }
    }

    pub fn budget_deadline_ms(&self) -> Option<i64> {
        match self {
            Self::RoleTurn {
                budget_deadline_ms, ..
            }
            | Self::ChildMission {
                budget_deadline_ms, ..
            } => Some(*budget_deadline_ms),
            Self::OracleRun { .. } => None,
        }
    }

    /// Capacity reserved at every ancestor while this effect is active.
    pub fn capacity_reservation(&self) -> u32 {
        match self {
            Self::RoleTurn { .. } | Self::OracleRun { .. } => 1,
            Self::ChildMission { request, .. } => {
                request.assignment.config.execution.effect_capacity
            }
        }
    }

    /// Build the inflight entry for a `…Requested` event.
    pub fn from_request(
        event: &super::event::MissionEvent,
        requested_seq: u64,
        teams: &BTreeMap<u32, super::TeamRevision>,
        not_before_ms: i64,
    ) -> Option<(super::EffectId, Self)> {
        use super::event::MissionEvent;
        match event {
            MissionEvent::RoleTurnRequested {
                role_instance,
                team_revision,
                task_id,
                assertion_ids,
                attempt_no,
                effect_id,
                prompt_template,
                prompt_hash,
                base_sha,
                environment_digest,
                instrument_identity,
                dependency_refs,
                report_refs,
                assignment_epoch,
                message_boundary,
                presented_messages,
                workspace_preparation,
                requested_at_ms,
                deadline_ms,
                budget_deadline_ms,
            } => {
                let role = teams.get(team_revision)?.role(role_instance)?;
                Some((
                    effect_id.clone(),
                    Self::RoleTurn {
                        role_instance: role_instance.clone(),
                        team_revision: *team_revision,
                        task_id: task_id.clone(),
                        assertion_ids: assertion_ids.clone(),
                        attempt_no: *attempt_no,
                        output: role.output,
                        runtime: role.runtime.clone(),
                        prompt_template: *prompt_template,
                        prompt_hash: prompt_hash.clone(),
                        base_sha: base_sha.clone(),
                        environment_digest: environment_digest.clone(),
                        instrument_identity: Box::new(instrument_identity.clone()),
                        dependency_refs: dependency_refs.clone(),
                        report_refs: report_refs.clone(),
                        assignment_epoch: *assignment_epoch,
                        message_boundary: *message_boundary,
                        presented_messages: presented_messages.clone(),
                        workspace_preparation: workspace_preparation.clone(),
                        requested_at_ms: *requested_at_ms,
                        not_before_ms,
                        deadline_ms: *deadline_ms,
                        budget_deadline_ms: *budget_deadline_ms,
                        requested_seq,
                    },
                ))
            }
            MissionEvent::OracleRunRequested {
                assertion_ids,
                oracle,
                spec_digest,
                judged_sha,
                environment_digest,
                attempt_no,
                effect_id,
                requested_at_ms,
                deadline_ms,
            } => Some((
                effect_id.clone(),
                Self::OracleRun {
                    assertion_ids: assertion_ids.clone(),
                    oracle: oracle.clone(),
                    spec_digest: spec_digest.clone(),
                    judged_sha: judged_sha.clone(),
                    environment_digest: environment_digest.clone(),
                    attempt_no: *attempt_no,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms,
                    deadline_ms: *deadline_ms,
                    requested_seq,
                },
            )),
            MissionEvent::ChildMissionRequested {
                request,
                requested_at_ms,
                deadline_ms,
                budget_deadline_ms,
            } => Some((
                request.parent_effect_id.clone(),
                Self::ChildMission {
                    request: request.clone(),
                    bound: false,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms,
                    deadline_ms: *deadline_ms,
                    budget_deadline_ms: *budget_deadline_ms,
                    requested_seq,
                },
            )),
            // Exhaustive on purpose: every new `…Requested` event must build
            // its inflight entry here.
            MissionEvent::MissionCreated { .. }
            | MissionEvent::MissionInputRecorded { .. }
            | MissionEvent::ProposalRecorded { .. }
            | MissionEvent::TeamConfigured { .. }
            | MissionEvent::SkillAdded { .. }
            | MissionEvent::EnvironmentAssigned { .. }
            | MissionEvent::ConversationResourcesCleaned { .. }
            | MissionEvent::MessageSent { .. }
            | MissionEvent::RoleTurnCompleted { .. }
            | MissionEvent::OracleRunCompleted { .. }
            | MissionEvent::ChildMissionBound { .. }
            | MissionEvent::ChildMissionCompleted { .. }
            | MissionEvent::ChildMissionCleaned { .. }
            | MissionEvent::MissionAborted { .. }
            | MissionEvent::MissionFinished { .. }
            | MissionEvent::ResultApplied { .. }
            | MissionEvent::DecisionRecorded { .. }
            | MissionEvent::ControlRequested { .. }
            | MissionEvent::EffectCleanupFailed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionState {
    pub mission_id: MissionId,
    pub objective: String,
    /// The mission type, pinned by content digest (verified on every open).
    pub mission_type: MissionTypeRef,
    /// The confinement image, resolved to a content id at start.
    pub image_id: String,
    #[serde(default)]
    pub environment_history: Vec<EnvironmentAssignment>,
    pub workspace_dir: String,
    /// Target repo HEAD at mission creation.
    pub base_sha: String,
    pub config: MissionConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<super::MissionLineage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mission_inputs: Vec<super::MissionDependencyInput>,
    pub team: Option<super::TeamRevision>,
    pub team_history: BTreeMap<u32, super::TeamRevision>,
    pub runtime_identity_history:
        BTreeMap<u32, BTreeMap<RoleInstanceId, RuntimeInstrumentIdentity>>,
    #[serde(default)]
    pub skills: BTreeMap<String, super::MissionSkill>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<TerminalState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_result: Option<AppliedResult>,
    pub plan: Option<Plan>,
    pub contract: BTreeMap<AssertionId, AssertionState>,
    #[serde(default)]
    pub superseded_assertions: Vec<SupersededAssertion>,
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
    /// Active candidate/guidance/evidence input for the next planning pass.
    pub planning_input: PlanningInput,
    /// Complete plan proposal awaiting approval or automatic promotion.
    pub proposal: Option<super::MissionProposal>,
    /// Current mission-local command oracle set.
    pub oracles: BTreeMap<OracleName, super::OracleSpec>,
    /// Latest recorded artifact head (starts at `base_sha`). Oracle verdicts
    /// are fresh only when judged at this commit.
    pub current_sha: String,
    /// Per-oracle dispatch counter (attempt numbering).
    /// Dispatch counters scoped by oracle name and canonical spec digest.
    /// Retaining prior digests prevents effect-id reuse if a spec is restored.
    pub oracle_attempts: BTreeMap<OracleName, BTreeMap<String, u32>>,
    pub inflight: BTreeMap<super::EffectId, InflightEffect>,
    /// One append-only, fold-owned receipt for every role effect. Mutable task,
    /// advisory, review, attention, and feedback state retains only effect IDs
    /// and resolves evidence through this map.
    #[serde(default)]
    pub role_attempt_receipts: BTreeMap<super::EffectId, RoleAttemptReceipt>,
    /// Mission-private dialogue authority, keyed by stable role-instance id.
    #[serde(default)]
    pub conversations: BTreeMap<RoleInstanceId, ConversationState>,
    /// Monotonic evidence of retained writer archives, keyed by task.
    #[serde(default)]
    pub retained_workspace_archives: BTreeMap<TaskId, BTreeSet<super::EffectId>>,
    /// Append-only authoritative command receipts. Assertions retain current
    /// receipt IDs and resolve verdicts through this map.
    #[serde(default)]
    pub authoritative_receipts: BTreeMap<super::EffectId, AuthoritativeVerdict>,
    /// Child receipts remain audit/task evidence and never enter the
    /// authoritative parent-proof map above.
    #[serde(default)]
    pub child_mission_receipts: BTreeMap<super::EffectId, super::ChildMissionReceipt>,
    #[serde(default)]
    pub cleaned_child_missions: BTreeSet<super::EffectId>,
    /// Commits established by mission creation or accepted artifact outcomes.
    #[serde(default)]
    pub reachable_commits: BTreeSet<String>,
    #[serde(default)]
    pub stop_requests: BTreeMap<super::EffectId, String>,
    #[serde(default)]
    pub reached_deadlines: BTreeMap<super::EffectId, i64>,
    #[serde(default)]
    pub parked_effects: BTreeMap<super::EffectId, ParkedEffect>,
    /// Latest cleanup failure for an unfinished effect. Cleared only when that
    /// effect's outcome is durably recorded.
    #[serde(default)]
    pub cleanup_failure: Option<EffectCleanupFailure>,
    /// Plan revision: the initial proposal promotes to 1, each later proposal
    /// to the next. Used for the proposal staleness guard (`base_revision`) and
    /// status display; the initial `MissionCreated` state (no plan) is 0.
    pub revision: u32,
    /// Gate checkpoints the human approved — the mission
    /// proceeds past them without re-raising the checkpoint.
    pub acknowledged_gates: BTreeSet<TaskId>,
    /// Tasks whose handoff asked for a human look (`request_attention`), until
    /// a decision clears them. Namespaced because planning and execution may
    /// legitimately use the same task id.
    pub flagged_tasks: BTreeSet<TaskId>,
    /// Oracles that failed to *run* (infrastructure failure, distinct from a
    /// nonzero exit) → mapped to the failure detail, until a decision clears
    /// them. Prevents a broken oracle from re-requesting forever.
    pub oracle_failures: BTreeMap<OracleName, TypedFailure>,
    /// Sequence number of the last folded event (optimistic-concurrency head).
    pub head: u64,
}

fn role_instrument_digest(role: &super::RoleInstance) -> String {
    let mut digest = CanonicalDigest::new("lionclaw.role-instrument.v1");
    digest.str("output", output_slug(role.output));
    digest.str("instructions", &role.instructions);
    digest.map("environment", role.environment.iter());
    digest.bool("grants.secrets", role.grants.secrets);
    role.grants
        .network
        .feed_digest(&mut digest, "grants.network");
    digest.bool("grants.install", role.grants.install);
    digest.bool("grants.writes", role.grants.writes);
    digest.set("grants.devices", role.grants.devices.iter());
    digest.set(
        "grants.inputs",
        role.grants.inputs.iter().map(ToString::to_string),
    );
    digest.set("resources.tmpfs", role.resources.tmpfs.iter());
    digest.option_u64("deadline_secs", role.deadline_secs);
    digest.finish()
}

fn output_slug(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::ProducesReport => "produces-report",
        OutputSemantics::ProducesArtifact => "produces-artifact",
        OutputSemantics::EmitsVerdict => "emits-verdict",
        OutputSemantics::EmitsGapVerdict => "emits-gap-verdict",
        OutputSemantics::ProposesPlan => "proposes-plan",
    }
}

impl MissionState {
    /// Classify one folded role receipt for every projection surface.
    ///
    /// Evidence use and generation lifecycle are orthogonal. For example, a
    /// failed execution generation can be superseded while its exact receipt
    /// remains the current input to replanning.
    pub fn role_attempt_authority(&self, receipt: &RoleAttemptReceipt) -> RoleAttemptAuthority {
        let effect_id = &receipt.effect_id;
        let superseded_assertion = self
            .superseded_assertions
            .iter()
            .any(|entry| entry.state.last_advisory.values().any(|id| id == effect_id));
        let RoleEffectSource::Turn {
            request,
            plan_revision,
        } = &receipt.source;
        let superseded_task = *plan_revision < self.revision
            || request.task_id.as_ref().is_some_and(|task_id| {
                self.tasks.get(task_id).is_some_and(|task| {
                    task.status == TaskStatus::Superseded
                        && task
                            .last_outcome
                            .as_ref()
                            .is_some_and(|outcome| outcome.effect_id() == effect_id)
                })
            });
        let generation = if receipt.disposition == RoleAttemptDisposition::Retired
            || superseded_assertion
            || superseded_task
        {
            RoleAttemptGeneration::Superseded
        } else {
            RoleAttemptGeneration::Current
        };

        let current_task = request
            .task_id
            .as_ref()
            .and_then(|task_id| self.tasks.get(task_id))
            .and_then(|task| task.last_outcome.as_ref())
            .is_some_and(|outcome| outcome.effect_id() == effect_id);
        let current_advisory = self.contract.iter().any(|(assertion_id, assertion)| {
            assertion.last_advisory.iter().any(|(validator, id)| {
                id == effect_id && self.advisory_receipt(assertion_id, validator, id).is_some()
            })
        });
        let current_review = self
            .team
            .as_ref()
            .and_then(|team| team.gap_review_assignment.as_ref())
            .and_then(|role| self.latest_taskless_assignment_receipt(role, &[]))
            .is_some_and(|latest| {
                if &latest.effect_id != effect_id {
                    return false;
                }
                if !self.role_attempt_is_fresh(latest) {
                    return false;
                }
                let clean_review = matches!(
                    &latest.disposition,
                    RoleAttemptDisposition::Succeeded {
                        handoff: Some(handoff),
                        ..
                    } if matches!(
                        handoff.as_ref(),
                        SettledHandoff::Review { passed: true, gaps }
                            if !gaps.iter().any(|gap| gap.severity == super::GapSeverity::Blocking)
                    )
                );
                clean_review || self.parked_effects.contains_key(effect_id)
            });
        let current_feedback = self
            .planning_input
            .refinement
            .as_ref()
            .and_then(|refinement| match refinement {
                PlanningRefinement::FailureEvidence(feedback) => Some(feedback.as_slice()),
                PlanningRefinement::Guidance(_) => None,
            })
            .is_some_and(|feedback| {
                feedback
                    .iter()
                    .any(|item| item.evidence.role_attempts().contains(effect_id))
            });
        let current_taskless_failure = match &receipt.source {
            RoleEffectSource::Turn {
                request,
                plan_revision,
            } if request.task_id.is_none()
                && *plan_revision == self.revision
                && receipt.failure().is_some()
                && self.parked_effects.contains_key(effect_id) =>
            {
                self.taskless_assignment_failure(&request.role_instance, &request.assertion_ids)
                    .is_some_and(|(parked_id, _, _)| parked_id == effect_id)
            }
            _ => false,
        };
        let inflight = self.inflight.contains_key(effect_id)
            && receipt.disposition == RoleAttemptDisposition::Active;
        let evidence_use = if current_task
            || current_advisory
            || current_review
            || current_feedback
            || current_taskless_failure
            || inflight
        {
            RoleAttemptEvidenceUse::Current
        } else {
            RoleAttemptEvidenceUse::Historical
        };

        RoleAttemptAuthority {
            evidence_use,
            generation,
        }
    }

    pub fn task_last_role_attempt(&self, task_id: &TaskId) -> Option<&RoleAttemptReceipt> {
        let effect_id = self.tasks.get(task_id)?.last_outcome.as_ref()?.effect_id();
        let receipt = self.role_attempt_receipts.get(effect_id)?;
        matches!(
            &receipt.source,
            RoleEffectSource::Turn { request, .. }
                if request.task_id.as_ref() == Some(task_id)
        )
        .then_some(receipt)
    }

    pub fn task_last_failure(&self, task_id: &TaskId) -> Option<&TypedFailure> {
        let effect_id = self.tasks.get(task_id)?.last_outcome.as_ref()?.effect_id();
        self.task_attempt_failure(effect_id)
    }

    pub fn task_automatic_retry_remaining(&self, task_id: &TaskId) -> bool {
        let Some(task) = self.tasks.get(task_id) else {
            return false;
        };
        task.status == TaskStatus::Failed
            && task.consecutive_failures < self.config.recovery.max_attempts
            && self
                .task_last_failure(task_id)
                .is_some_and(|failure| failure.is_transient() || failure.is_invalid_output())
    }

    pub fn advisory_receipt(
        &self,
        assertion_id: &AssertionId,
        validator: &RoleInstanceId,
        effect_id: &super::EffectId,
    ) -> Option<(&RoleAttemptReceipt, bool)> {
        let receipt = self.role_attempt_receipts.get(effect_id)?;
        let RoleEffectSource::Turn { request, .. } = &receipt.source;
        if &request.role_instance != validator
            || !request.assertion_ids.contains(assertion_id)
            || request.task_id.is_some()
        {
            return None;
        }
        let current_team = self.team.as_ref()?;
        let role = current_team.role(&request.role_instance)?;
        if role.output != super::OutputSemantics::EmitsVerdict
            || !self.role_attempt_is_fresh(receipt)
            || !self
                .team
                .as_ref()
                .and_then(|team| team.judgment_assignments.get(assertion_id))
                .is_some_and(|panel| panel.contains(validator))
        {
            return None;
        }
        let SettledHandoff::Validate { items, .. } = receipt.settled_handoff()? else {
            return None;
        };
        items
            .iter()
            .find(|item| &item.item_id == assertion_id)
            .map(|item| (receipt, item.passed))
    }

    pub fn advisory_status(&self, assertion_id: &AssertionId) -> AdvisoryStatus {
        let Some(assertion) = self.contract.get(assertion_id) else {
            return AdvisoryStatus::Pending;
        };
        let Some(panel) = self
            .team
            .as_ref()
            .and_then(|team| team.judgment_assignments.get(assertion_id))
            .filter(|panel| !panel.is_empty())
        else {
            return AdvisoryStatus::Pending;
        };
        let mut saw_failure = false;
        for validator in panel {
            let Some(effect_id) = assertion.last_advisory.get(validator) else {
                return AdvisoryStatus::Pending;
            };
            match self.advisory_receipt(assertion_id, validator, effect_id) {
                Some((_, true)) => {}
                Some((_, false)) => saw_failure = true,
                None => return AdvisoryStatus::Pending,
            }
        }
        if saw_failure {
            AdvisoryStatus::Failed
        } else {
            AdvisoryStatus::Passed
        }
    }

    pub(crate) fn taskless_assignment_receipts<'a>(
        &'a self,
        role_instance: &RoleInstanceId,
        assertion_ids: &[AssertionId],
    ) -> Vec<&'a RoleAttemptReceipt> {
        let Some(team_revision) = self.team.as_ref().map(|team| team.revision) else {
            return Vec::new();
        };
        let mut receipts: Vec<_> = self
            .role_attempt_receipts
            .values()
            .filter(|receipt| {
                matches!(
                    &receipt.source,
                    RoleEffectSource::Turn { request, .. }
                        if request.role_instance == *role_instance
                            && request.team_revision == team_revision
                            && request.task_id.is_none()
                            && request.assertion_ids == assertion_ids
                )
            })
            .collect();
        receipts.sort_by_key(|receipt| match &receipt.source {
            RoleEffectSource::Turn { request, .. } => request.attempt_no,
        });
        receipts
    }

    pub fn latest_taskless_assignment_receipt(
        &self,
        role_instance: &RoleInstanceId,
        assertion_ids: &[AssertionId],
    ) -> Option<&RoleAttemptReceipt> {
        self.taskless_assignment_receipts(role_instance, assertion_ids)
            .into_iter()
            .last()
    }

    pub fn role_attempt_is_fresh(&self, receipt: &RoleAttemptReceipt) -> bool {
        let RoleEffectSource::Turn {
            request,
            plan_revision,
        } = &receipt.source;
        *plan_revision == self.revision
            && request.is_fresh_at(self)
            && self.role_report_refs_match(
                &request.role_instance,
                request.team_revision,
                request.task_id.as_ref(),
                &request.assertion_ids,
                &request.report_refs,
            )
    }

    pub(crate) fn taskless_assignment_failure(
        &self,
        role_instance: &RoleInstanceId,
        assertion_ids: &[AssertionId],
    ) -> Option<(&super::EffectId, &TypedFailure, u32)> {
        let receipts = self.taskless_assignment_receipts(role_instance, assertion_ids);
        let latest = *receipts.last()?;
        let failure = latest.failure()?;
        if !self.parked_effects.contains_key(&latest.effect_id) {
            return None;
        }
        let consecutive = receipts
            .iter()
            .rev()
            .take_while(|receipt| {
                receipt.failure().is_some() && self.parked_effects.contains_key(&receipt.effect_id)
            })
            .count() as u32;
        Some((&latest.effect_id, failure, consecutive))
    }

    pub(crate) fn taskless_assignment_dispatchable(
        &self,
        role_instance: &RoleInstanceId,
        assertion_ids: &[AssertionId],
    ) -> bool {
        self.taskless_assignment_failure(role_instance, assertion_ids)
            .is_none_or(|(_, failure, consecutive)| {
                failure.automatically_retryable() && consecutive < self.config.recovery.max_attempts
            })
    }

    /// Closed role contract shared by request folding, dispatch, and live
    /// workspace observation.
    pub fn role_dispatch_contract_matches(
        &self,
        role_instance: &RoleInstanceId,
        team_revision: u32,
        task_id: Option<&TaskId>,
        assertion_ids: &[AssertionId],
    ) -> bool {
        let Some(team) = self.team_history.get(&team_revision) else {
            return false;
        };
        let Some(role) = team.role(role_instance) else {
            return false;
        };
        match (role.output, task_id) {
            (output, Some(task_id)) if output.produces_task_output() => {
                team.task_assignments
                    .get(task_id)
                    .and_then(super::TaskAssignment::role_instance)
                    == Some(role_instance)
                    && self
                        .plan
                        .as_ref()
                        .is_some_and(|plan| plan.tasks.iter().any(|task| &task.id == task_id))
            }
            (super::OutputSemantics::ProposesPlan, None) => {
                team.planning_assignment == *role_instance && assertion_ids.is_empty()
            }
            (super::OutputSemantics::EmitsVerdict, None) => {
                !assertion_ids.is_empty()
                    && assertion_ids.iter().all(|assertion| {
                        team.judgment_assignments
                            .get(assertion)
                            .is_some_and(|panel| panel.contains(role_instance))
                    })
            }
            (super::OutputSemantics::EmitsGapVerdict, None) => {
                team.gap_review_assignment.as_ref() == Some(role_instance)
                    && assertion_ids.is_empty()
            }
            (super::OutputSemantics::ProducesReport, None) => assertion_ids.is_empty(),
            _ => false,
        }
    }
}

impl MissionState {
    pub fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    pub fn finish(&self) -> Option<FinishClass> {
        self.terminal.as_ref().and_then(TerminalState::finish)
    }

    pub fn task_workspace_role(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<(&RoleInstanceId, &ConversationState)>, &'static str> {
        let Some(task) = self.tasks.get(task_id) else {
            return Ok(None);
        };
        let Some(assignment) = task.role_assignment.as_ref() else {
            return Ok(None);
        };
        let Some(conversation) = self.conversations.get(&assignment.role_instance) else {
            return Err("assigned role instance has no conversation");
        };
        Ok(Some((&assignment.role_instance, conversation)))
    }

    pub fn active_role_conversation(
        &self,
        effect_id: &super::EffectId,
    ) -> Result<(&RoleInstanceId, &ConversationState), &'static str> {
        let Some(InflightEffect::RoleTurn {
            role_instance,
            team_revision,
            task_id,
            assertion_ids,
            attempt_no,
            assignment_epoch,
            message_boundary,
            presented_messages,
            requested_seq,
            ..
        }) = self.inflight.get(effect_id)
        else {
            return Err("effect is not an active role turn");
        };
        if !self.role_dispatch_contract_matches(
            role_instance,
            *team_revision,
            task_id.as_ref(),
            assertion_ids,
        ) {
            return Err("active role turn no longer matches its team assignment");
        }
        let Some(conversation) = self.conversations.get(role_instance) else {
            return Err("active role instance conversation is absent");
        };
        let expected_presented = conversation
            .queued
            .iter()
            .filter(|message| {
                message.sequence_no <= *message_boundary
                    && message.marker != DeliveryMarker::Undeliverable
            })
            .map(|message| message.sequence_no)
            .collect::<Vec<_>>();
        let delivery_agrees = conversation
            .active_delivery
            .as_ref()
            .is_some_and(|delivery| {
                &delivery.effect_id == effect_id
                    && delivery.message_boundary == *message_boundary
                    && delivery.presented_messages == *presented_messages
            });
        let effect_is_canonical = effect_id
            == &super::EffectId::for_role_turn(
                &self.mission_id,
                role_instance,
                *team_revision,
                task_id.as_ref(),
                *attempt_no,
                *assignment_epoch,
                self.inflight
                    .get(effect_id)
                    .and_then(InflightEffect::role_turn_provenance)
                    .as_ref()
                    .map_or("", |request| request.prompt_hash.as_str()),
            );
        if !effect_is_canonical
            || *requested_seq == 0
            || *requested_seq > self.head
            || expected_presented != *presented_messages
            || conversation.role_instance != *role_instance
            || conversation.lifecycle != ConversationLifecycle::Running
            || !delivery_agrees
        {
            return Err("active role turn and conversation authority disagree");
        }
        Ok((role_instance, conversation))
    }

    pub fn expected_active_workspace_provenance(
        &self,
        effect_id: &super::EffectId,
    ) -> Result<(TaskId, TaskWorkspaceProvenance), &'static str> {
        self.active_role_conversation(effect_id)?;
        let Some(InflightEffect::RoleTurn {
            task_id: Some(task_id),
            output,
            base_sha,
            assignment_epoch,
            workspace_preparation,
            ..
        }) = self.inflight.get(effect_id)
        else {
            return Err("effect is not an assigned work turn");
        };
        if *output != super::OutputSemantics::ProducesArtifact {
            return Err("active effect is not an artifact-producing role");
        }
        Ok((
            task_id.clone(),
            TaskWorkspaceProvenance {
                effect_id: effect_id.clone(),
                base_sha: base_sha.clone(),
                assignment_epoch: *assignment_epoch,
                archived_effect_id: workspace_preparation.archived_effect().cloned(),
            },
        ))
    }

    pub fn active_workspace_task(
        &self,
        effect_id: &super::EffectId,
    ) -> Result<(&TaskId, &TaskRuntimeState, TaskWorkspaceProvenance), &'static str> {
        let (task_id, expected) = self.expected_active_workspace_provenance(effect_id)?;
        let task = self
            .tasks
            .get(&task_id)
            .ok_or("active effect task is absent from folded state")?;
        let Some(assignment) = task.role_assignment.as_ref() else {
            return Err("active effect task has no role assignment authority");
        };
        if assignment.base_sha != expected.base_sha
            || assignment.assignment_epoch != expected.assignment_epoch
            || task.pending_workspace_recreation.as_ref() != expected.archived_effect_id.as_ref()
        {
            return Err("active effect and task workspace authority disagree");
        }
        Ok((
            self.tasks.get_key_value(&task_id).expect("task exists").0,
            task,
            expected,
        ))
    }

    pub fn is_safe_snapshot_seed(&self) -> bool {
        !self.tasks.values().any(|task| {
            task.pending_workspace_recreation.is_some()
                || (task.status == TaskStatus::Pending
                    && task.attempts > 0
                    && task.workspace_provenance.is_some())
        }) && self.inflight.iter().all(|(effect_id, effect)| {
            !matches!(
                effect,
                InflightEffect::RoleTurn {
                    output: super::OutputSemantics::ProducesArtifact,
                    ..
                }
            ) || self.active_role_conversation(effect_id).is_ok()
        })
    }

    pub fn reference_recipient_policy(
        &self,
        recipients: &[RoleInstanceId],
    ) -> ReferenceRecipientPolicy {
        if recipients.is_empty() {
            return ReferenceRecipientPolicy::Invalid;
        }
        let Some(team) = self.team.as_ref() else {
            return ReferenceRecipientPolicy::Invalid;
        };
        let outputs = recipients
            .iter()
            .map(|recipient| team.role(recipient).map(|role| (recipient, role.output)))
            .collect::<Option<Vec<_>>>();
        let Some(outputs) = outputs else {
            return ReferenceRecipientPolicy::Invalid;
        };
        let permitted = outputs
            .iter()
            .filter(|(_, output)| output.permits_message_references())
            .count();
        if permitted == outputs.len() {
            ReferenceRecipientPolicy::Permitted
        } else if permitted != 0 {
            ReferenceRecipientPolicy::Mixed
        } else {
            let (role_instance, output) = outputs[0];
            ReferenceRecipientPolicy::Disallowed {
                role_instance: role_instance.clone(),
                output,
            }
        }
    }

    pub fn conversation_is_messageable(&self, role_instance: &RoleInstanceId) -> bool {
        !self.is_terminal()
            && self
                .conversations
                .get(role_instance)
                .is_some_and(|conversation| {
                    !matches!(
                        conversation.lifecycle,
                        ConversationLifecycle::Completed | ConversationLifecycle::Retired
                    )
                })
    }

    pub fn conversation_accepts_message(&self, role_instance: &RoleInstanceId) -> bool {
        self.conversation_is_messageable(role_instance)
            && self.conversations[role_instance].queued.len()
                < crate::MAX_QUEUED_MESSAGES_PER_CONVERSATION
    }

    pub fn deliverable_head(&self) -> &str {
        &self.current_sha
    }

    pub fn environment_digest(&self) -> &str {
        &self.image_id
    }

    pub fn role_instrument_identity(
        &self,
        role_instance: &RoleInstanceId,
    ) -> Option<RoleInstrumentIdentity> {
        let team_revision = self.team.as_ref()?.revision;
        self.role_instrument_identity_for_revision(role_instance, team_revision)
    }

    pub fn role_instrument_identity_for_revision(
        &self,
        role_instance: &RoleInstanceId,
        team_revision: u32,
    ) -> Option<RoleInstrumentIdentity> {
        let team = self.team_history.get(&team_revision)?;
        let role = team.role(role_instance)?;
        let runtime = self
            .runtime_identity_history
            .get(&team_revision)?
            .get(role_instance)?
            .clone();
        if runtime.runtime != role.runtime {
            return None;
        }
        let mut skills = role
            .skills
            .iter()
            .map(|name| {
                self.skills
                    .get(name)
                    .or_else(|| self.config.skills.get(name))
                    .map(|skill| SkillInstrumentIdentity {
                        name: skill.name.clone(),
                        digest: skill.digest.clone(),
                    })
            })
            .collect::<Option<Vec<_>>>()?;
        skills.sort_by(|left, right| left.name.cmp(&right.name));
        Some(RoleInstrumentIdentity {
            role_instance: role_instance.clone(),
            role_digest: role_instrument_digest(role),
            runtime,
            skills,
        })
    }

    pub fn deliverable_task_id(&self) -> Option<&TaskId> {
        let plan = self.plan.as_ref()?;
        let depended_on: BTreeSet<_> = plan
            .tasks
            .iter()
            .flat_map(|task| task.depends_on.iter())
            .collect();
        plan.tasks
            .iter()
            .find(|task| !depended_on.contains(&task.id))
            .map(|task| &task.id)
    }

    pub fn task_dependency_refs(&self, task_id: &TaskId) -> Option<Vec<TaskCandidateRef>> {
        let task = self
            .plan
            .as_ref()?
            .tasks
            .iter()
            .find(|task| &task.id == task_id)?;
        task.depends_on
            .iter()
            .map(|dependency| {
                let runtime = self.tasks.get(dependency)?;
                (runtime.status == TaskStatus::Cleared)
                    .then_some(runtime.candidate_sha.as_ref())
                    .flatten()
                    .map(|sha| TaskCandidateRef {
                        task_id: dependency.clone(),
                        sha: sha.clone(),
                    })
            })
            .collect()
    }

    pub fn child_mission_dependency_refs(
        &self,
        task_id: &TaskId,
    ) -> Option<Vec<super::ChildMissionDependencyRef>> {
        let task = self
            .plan
            .as_ref()?
            .tasks
            .iter()
            .find(|task| &task.id == task_id)?;
        let team = self.team.as_ref()?;
        task.depends_on
            .iter()
            .map(|dependency| {
                let outcome = self.tasks.get(dependency)?.cleared_outcome()?;
                if !self.task_effect_is_secret_free(outcome.effect_id()) {
                    return None;
                }
                let candidate_sha = self.tasks.get(dependency)?.candidate_sha.as_ref()?.clone();
                let report = self.accepted_task_report(outcome.effect_id());
                if team.task_output(dependency) == Some(OutputSemantics::ProducesReport)
                    && report.is_none()
                {
                    return None;
                }
                let failure_sha256 = match self.task_attempt_failure(outcome.effect_id()) {
                    Some(failure) => Some(super::child_failure_digest(failure)?),
                    None => None,
                };
                Some(super::ChildMissionDependencyRef {
                    task_id: dependency.clone(),
                    effect_id: outcome.effect_id().clone(),
                    candidate_sha,
                    report_sha256: report.and_then(PayloadRef::content_sha256),
                    failure_sha256,
                })
            })
            .collect()
    }

    fn task_effect_is_secret_free(&self, effect_id: &super::EffectId) -> bool {
        if let Some(receipt) = self.role_attempt_receipts.get(effect_id) {
            let RoleEffectSource::Turn { request, .. } = &receipt.source;
            return self
                .team_history
                .get(&request.team_revision)
                .and_then(|team| team.role(&request.role_instance))
                .is_some_and(|role| !role.grants.secrets);
        }
        self.child_mission_receipts.contains_key(effect_id)
    }

    pub fn mission_input_dependency_refs(&self) -> Vec<TaskCandidateRef> {
        self.mission_inputs
            .iter()
            .map(super::MissionDependencyInput::candidate_ref)
            .collect()
    }

    pub fn descendant_count(&self) -> u64 {
        self.child_mission_receipts
            .values()
            .map(|receipt| u64::from(receipt.descendant_count).saturating_add(1))
            .sum()
    }

    /// Current report deliverables relevant to one judgment assignment, in
    /// plan-authored task order. Only cleared report-producing tasks enter the
    /// set; their exact accepted payload digest and effect identity are bound.
    pub fn judgment_report_refs(
        &self,
        assertion_ids: &[AssertionId],
    ) -> Option<Vec<ReportEvidenceRef>> {
        let plan = self.plan.as_ref()?;
        let team = self.team.as_ref()?;
        plan.tasks
            .iter()
            .filter(|task| {
                task.targets
                    .iter()
                    .any(|target| assertion_ids.contains(target))
            })
            .filter(|task| {
                team.task_output(&task.id) == Some(super::OutputSemantics::ProducesReport)
            })
            .map(|task| {
                let outcome = self.tasks.get(&task.id)?.cleared_outcome()?;
                let report = self.accepted_task_report(outcome.effect_id())?;
                Some(ReportEvidenceRef {
                    task_id: task.id.clone(),
                    effect_id: outcome.effect_id().clone(),
                    report_sha256: report.content_sha256()?,
                })
            })
            .collect()
    }

    pub fn accepted_task_report(&self, effect_id: &super::EffectId) -> Option<&PayloadRef> {
        self.role_attempt_receipts
            .get(effect_id)
            .and_then(RoleAttemptReceipt::accepted_report)
            .or_else(|| {
                self.child_mission_receipts
                    .get(effect_id)?
                    .output
                    .as_ref()?
                    .report()
            })
    }

    pub fn task_attempt_failure(&self, effect_id: &super::EffectId) -> Option<&TypedFailure> {
        self.role_attempt_receipts
            .get(effect_id)
            .and_then(RoleAttemptReceipt::failure)
            .or_else(|| {
                self.child_mission_receipts
                    .get(effect_id)
                    .and_then(|receipt| receipt.failure.as_ref())
            })
    }

    pub(crate) fn role_report_refs_match(
        &self,
        role_instance: &RoleInstanceId,
        team_revision: u32,
        task_id: Option<&TaskId>,
        assertion_ids: &[AssertionId],
        report_refs: &[ReportEvidenceRef],
    ) -> bool {
        let Some(role) = self
            .team_history
            .get(&team_revision)
            .and_then(|team| team.role(role_instance))
        else {
            return false;
        };
        if role.output == super::OutputSemantics::EmitsVerdict && task_id.is_none() {
            return self.judgment_report_refs(assertion_ids).as_deref() == Some(report_refs);
        }
        report_refs.is_empty()
    }

    pub fn task_required_base(&self, task_id: &TaskId) -> Option<String> {
        if let Some(base_sha) = self
            .tasks
            .get(task_id)
            .and_then(|runtime| runtime.pending_base_sha.as_ref())
        {
            return Some(base_sha.clone());
        }
        let refs = self.task_dependency_refs(task_id)?;
        Some(
            refs.first()
                .map(|candidate| candidate.sha.clone())
                .unwrap_or_else(|| self.base_sha.clone()),
        )
    }

    pub fn task_lineage_request_matches(
        &self,
        task_id: &TaskId,
        base_sha: &str,
        dependency_refs: &[TaskCandidateRef],
    ) -> bool {
        self.task_required_base(task_id).as_deref() == Some(base_sha)
            && self.task_dependency_refs(task_id).as_deref() == Some(dependency_refs)
    }

    pub fn durable_cancellation(&self, effect_id: &super::EffectId) -> Option<DurableCancellation> {
        if let Some(TerminalState::Aborted { reason }) = &self.terminal {
            return Some(DurableCancellation::Aborted {
                reason: reason.clone(),
            });
        }
        if let Some(reason) = self.stop_requests.get(effect_id) {
            return Some(DurableCancellation::Stopped {
                reason: reason.clone(),
            });
        }
        self.reached_deadlines
            .get(effect_id)
            .copied()
            .map(|deadline_ms| DurableCancellation::DeadlineReached { deadline_ms })
    }

    pub fn parked_effect_is_continuable(&self, effect_id: &super::EffectId) -> bool {
        !self.is_terminal()
            && self
                .parked_effects
                .get(effect_id)
                .is_some_and(|effect| self.parked_effect_remains_continuable(effect_id, effect))
    }

    pub fn parked_workspace_recreation(&self, effect_id: &super::EffectId) -> Option<TaskId> {
        if !self.parked_effect_is_continuable(effect_id) {
            return None;
        }
        let ParkedEffect::RoleTurn {
            task_id: Some(task_id),
            ..
        } = self.parked_effects.get(effect_id)?
        else {
            return None;
        };
        self.tasks
            .get(task_id)
            .and_then(|task| task.workspace_provenance.as_ref())
            .map(|_| task_id.clone())
    }

    pub fn parked_continue_is_legal(
        &self,
        effect_id: &super::EffectId,
        mode: super::ContinueMode,
    ) -> bool {
        match mode {
            super::ContinueMode::Preserve => {
                self.parked_effect_is_continuable(effect_id)
                    && self
                        .parked_workspace_recreation(effect_id)
                        .and_then(|task_id| {
                            self.tasks
                                .get(&task_id)
                                .and_then(|task| task.pending_workspace_recreation.as_ref())
                        })
                        .is_none()
            }
            super::ContinueMode::RecreateWorkspace => {
                self.parked_workspace_recreation(effect_id).is_some()
            }
        }
    }

    pub fn continue_is_legal(
        &self,
        effect_id: &super::EffectId,
        automatic: bool,
        mode: super::ContinueMode,
    ) -> bool {
        (!automatic || mode == super::ContinueMode::Preserve)
            && self.parked_continue_is_legal(effect_id, mode)
    }

    pub(crate) fn parked_effect_remains_continuable(
        &self,
        effect_id: &super::EffectId,
        effect: &ParkedEffect,
    ) -> bool {
        match effect {
            ParkedEffect::RoleTurn {
                role_instance,
                task_id,
            } => match task_id {
                Some(task_id) => {
                    self.team.as_ref().is_some_and(|team| {
                        team.task_assignments
                            .get(task_id)
                            .and_then(super::TaskAssignment::role_instance)
                            == Some(role_instance)
                    }) && self
                        .tasks
                        .get(task_id)
                        .is_some_and(|task| task.status == TaskStatus::Failed)
                }
                None => {
                    self.conversation_is_messageable(role_instance)
                        && self
                            .role_attempt_receipts
                            .get(effect_id)
                            .is_some_and(|receipt| {
                                matches!(
                                    &receipt.source,
                                    RoleEffectSource::Turn { request, .. }
                                        if request.task_id.is_none()
                                            && self.team.as_ref().is_some_and(|team| {
                                                request.team_revision == team.revision
                                            })
                                ) && self.role_attempt_is_fresh(receipt)
                            })
                }
            },
            ParkedEffect::OracleRun { oracle } => self.oracle_failures.contains_key(oracle),
        }
    }

    pub fn active_tasks(&self) -> &BTreeMap<TaskId, TaskRuntimeState> {
        &self.tasks
    }

    pub fn authoritative_verdict(
        &self,
        assertion: &AssertionState,
    ) -> Option<&AuthoritativeVerdict> {
        assertion
            .last_authoritative_receipt
            .as_ref()
            .and_then(|effect_id| self.authoritative_receipts.get(effect_id))
    }

    pub(crate) fn oracle_automatic_retry_remaining(&self, oracle: &OracleName) -> bool {
        let Some(spec_digest) = self.oracles.get(oracle).map(super::OracleSpec::digest) else {
            return false;
        };
        self.oracle_failures
            .get(oracle)
            .is_some_and(TypedFailure::is_transient)
            && self
                .oracle_attempts
                .get(oracle)
                .and_then(|attempts| attempts.get(&spec_digest))
                .copied()
                .unwrap_or_default()
                < self.config.recovery.max_attempts
    }

    pub(crate) fn next_oracle_attempt(&self, oracle: &OracleName) -> Option<u32> {
        let spec_digest = self.oracles.get(oracle)?.digest();
        self.oracle_attempts
            .get(oracle)
            .and_then(|attempts| attempts.get(&spec_digest))
            .copied()
            .unwrap_or_default()
            .checked_add(1)
    }

    pub(crate) fn owed_assertions_for_oracle(&self, oracle: &OracleName) -> Vec<AssertionId> {
        let Some(plan) = &self.plan else {
            return Vec::new();
        };
        self.contract
            .iter()
            .filter(|(assertion_id, assertion)| {
                assertion.oracle.as_ref() == Some(oracle)
                    && plan.assertion_requires_confined_proof(assertion_id)
                    && self
                        .authoritative_verdict(assertion)
                        .is_none_or(|verdict| !verdict.is_fresh_at(self))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub(crate) fn oracle_dispatchable(&self, oracle: &OracleName) -> bool {
        !self.oracle_failures.contains_key(oracle) || self.oracle_automatic_retry_remaining(oracle)
    }
}
