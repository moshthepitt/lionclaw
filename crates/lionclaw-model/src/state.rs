//! `MissionState` — a pure fold over the event log. Deterministic containers
//! only (`BTreeMap`), derives `PartialEq` so the fold-litmus test can assert
//! rebuilt state equality. Wall-clock time never enters this type.

use serde::{Deserialize, Serialize};

use super::event::{
    EffectResource, MissionConfig, MissionTypeRef, PayloadRef, RuntimeConfigurationEvidence,
};
use super::ids::{AssertionId, MissionId, OracleName, RoleInstanceId, TaskId};
use super::plan::{Assertion, Plan};
use super::verdict::{AuthoritativeVerdict, FinishClass};
use crate::prelude::*;
use crate::{TypedFailure, TypedFailureEvidence};

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
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum MissionPhase {
    /// No accepted plan yet: dispatches the team's planning assignment toward
    /// a joint plan/team proposal.
    Planning,
    Running,
    /// Open attention items — parked at zero compute (durable interrupt).
    AttentionNeeded,
    Done {
        finish: FinishClass,
    },
    Aborted {
        reason: String,
    },
}

impl MissionPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Aborted { .. })
    }

    /// The finish grade, if this is a `Done` phase.
    pub const fn finish(&self) -> Option<FinishClass> {
        match self {
            Self::Done { finish } => Some(*finish),
            _ => None,
        }
    }

    /// The stable snake_case variant name (matches the serde `phase` tag). The
    /// `Done`/`Aborted` payloads are not part of the slug — a caller that wants
    /// the finish grade composes it from [`FinishClass::slug`].
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Running => "running",
            Self::AttentionNeeded => "attention_needed",
            Self::Done { .. } => "done",
            Self::Aborted { .. } => "aborted",
        }
    }
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
    pub workspace_preparation: super::WorkspacePreparation,
    pub message_boundary: u64,
    pub presented_messages: Vec<u64>,
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
    pub generation: u32,
    pub workspace_preparation: super::WorkspacePreparation,
}

#[derive(Debug, Clone, Copy)]
pub struct RoleAssignmentContext<'a> {
    pub previous: Option<&'a TaskRuntimeState>,
    pub required_base: &'a str,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureEvidence {
    pub exit_code: i32,
    pub exit_signal: Option<i32>,
    pub stdout: PayloadRef,
    pub stderr: PayloadRef,
}

/// Exact evidence behind an operator decision or replanning input.
///
/// Role-derived facts retain only receipt identities and resolve their content
/// through `MissionState::role_attempt_receipts`. Oracle failures are not role
/// attempts and retain their own typed payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionEvidence {
    None,
    RoleAttempts { effect_ids: Vec<super::EffectId> },
    OracleRuntimeFailure { failure: TypedFailure },
    OracleVerdict { evidence: FailureEvidence },
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
            Self::None | Self::OracleRuntimeFailure { .. } | Self::OracleVerdict { .. } => &[],
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
    FailureEvidence(Box<FailureFeedback>),
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
    /// Only the fold can mint this, and only from `OracleRunCompleted`.
    pub last_authoritative: Option<AuthoritativeVerdict>,
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

/// The gap-review ledger: fold-owned, advisory-only (never read by
/// `classify_finish`). All-default == "no review has run".
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GapReviewState {
    /// Dispatch counter (mirrors `oracle_attempts`): folded from
    /// The assigned gap role's request attempt; the next dispatch and its
    /// effect ID ride on it, so a retry re-rolls under a fresh identity.
    #[serde(default)]
    pub attempts: u32,
    /// Failures since the last completed review or explicit recovery decision.
    #[serde(default)]
    pub consecutive_failures: u32,
    /// The last attempt's result. A fresh verdict and a pending failure
    /// cannot coexist: a failure only follows a dispatch, and dispatch only
    /// happens without a fresh verdict (history lives in the event log).
    #[serde(default)]
    pub outcome: Option<ReviewOutcome>,
    /// The one human-acceptance fact ("accept closure despite the review").
    #[serde(default)]
    pub accepted: Option<ReviewAcceptance>,
}

impl GapReviewState {
    /// The acceptance, if it still holds at the current head — the ONE
    /// freshness-law site the fold's derivations and the CLI's summaries all
    /// share, so they can never disagree about whether the mission may close.
    pub fn fresh_acceptance(&self, current_sha: &str) -> Option<&ReviewAcceptance> {
        self.accepted
            .as_ref()
            .filter(|a| a.is_fresh_at(current_sha))
    }

    /// Whether a fresh waiver stands at the current head (closure permitted
    /// without a verdict).
    pub fn waived_at(&self, current_sha: &str) -> bool {
        self.fresh_acceptance(current_sha)
            .is_some_and(|a| a.kind == ReviewAcceptanceKind::Waived)
    }

    /// Whether this verdict's blocking gaps were acknowledged (the
    /// acknowledgment is keyed to the verdict's own sha).
    pub fn acknowledges_sha(&self, judged_sha: &str) -> bool {
        self.accepted.as_ref().is_some_and(|a| {
            a.kind == ReviewAcceptanceKind::AcknowledgedGaps && a.judged_sha == judged_sha
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReviewOutcome {
    Verdict {
        effect_id: super::EffectId,
    },
    /// The reviewer failed to run or hand off a verdict (infrastructure),
    /// until a decision clears it. Prevents a broken reviewer from
    /// re-requesting forever (mirrors `oracle_failures`).
    Failed {
        effect_id: super::EffectId,
    },
}

impl ReviewOutcome {
    pub const fn effect_id(&self) -> &super::EffectId {
        match self {
            Self::Verdict { effect_id } | Self::Failed { effect_id } => effect_id,
        }
    }
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

/// How a human accepted closure despite the review: `accept` on a gap park
/// acknowledges the blocking verdict, while `accept` on a failure park waives
/// the review outright. One value, so waived-and-acknowledged is unrepresentable;
/// the receipt distinguishes the kinds and cites why it was accepted.
///
/// Both kinds are keyed to the head they were granted at: a later artifact
/// commit stales the acceptance and re-opens the review, so neither an
/// acknowledgment nor a waiver is ever inherited by work the human never saw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewAcceptance {
    pub kind: ReviewAcceptanceKind,
    /// `current_sha` at the moment of acceptance (for an acknowledgment this
    /// is also the verdict's `judged_sha` — the gap item only raises fresh).
    pub judged_sha: String,
    pub justification: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAcceptanceKind {
    /// A blocking verdict the human closed over; its gaps stay on record.
    AcknowledgedGaps,
    /// The review failed to run and the human closed without a verdict.
    Waived,
}

impl ReviewAcceptance {
    /// Same freshness law as verdicts: an acceptance holds only at the head
    /// it was granted at.
    pub fn is_fresh_at(&self, current_sha: &str) -> bool {
        self.judged_sha == current_sha
    }
}

impl ReviewAcceptanceKind {
    /// The stable snake_case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::AcknowledgedGaps => "acknowledged_gaps",
            Self::Waived => "waived",
        }
    }
}

/// A gap reviewer's verdict. Plain public data — deliberately NOT an
/// `AuthoritativeVerdict` (private-field mint, `verdict.rs`): this verdict
/// is advisory, mints nothing, and gates closure only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    NodeFailed,
    NodeAttention,
    /// An oracle failed to *run* (infrastructure), distinct from a nonzero
    /// exit (which is a valid verdict).
    OracleFailed,
    /// An oracle ran and returned an authoritative nonzero verdict.
    OracleVerdictFailed,
    GateFailed,
    GateCheckpoint,
    /// A complete plan proposal awaits approval before promotion.
    PlanProposal,
    /// The gap review's blocking verdict awaits a human (revise to
    /// remediate / retry to re-run / accept to acknowledge-and-close /
    /// abort). Raised only when the mission would otherwise close, so
    /// remediation work auto-clears it.
    GapReviewGaps,
    /// The gap reviewer failed to run or hand off a verdict
    /// (infrastructure), distinct from a verdict with gaps.
    GapReviewFailed,
}

impl AttentionKind {
    /// The stable snake_case name (matches the serde repr). Used both to derive
    /// the durable attention-item id in the fold and to render it in the CLI, so
    /// the id a user reads is exactly the id they pass back to `decide`.
    pub const fn slug(self) -> &'static str {
        match self {
            Self::NodeFailed => "node_failed",
            Self::NodeAttention => "node_attention",
            Self::OracleFailed => "oracle_failed",
            Self::OracleVerdictFailed => "oracle_verdict_failed",
            Self::GateFailed => "gate_failed",
            Self::GateCheckpoint => "gate_checkpoint",
            Self::PlanProposal => "plan_proposal",
            Self::GapReviewGaps => "gap_review_gaps",
            Self::GapReviewFailed => "gap_review_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttentionItem {
    /// Stable across refolds: `{kind}:{anchor}` where the anchor is the task,
    /// oracle, or "mission".
    pub id: String,
    pub kind: AttentionKind,
    /// The task this item is about, if any.
    pub task_id: Option<TaskId>,
    /// The oracle this item is about, if any (oracle infra failures).
    #[serde(default)]
    pub oracle: Option<OracleName>,
    #[serde(default)]
    pub assertion_ids: Vec<AssertionId>,
    pub evidence: DecisionEvidence,
    pub report: String,
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
        judged_sha: String,
        attempt_no: u32,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
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
            Self::OracleRun { .. } => return None,
        };
        Some(RoleAttemptReceipt {
            effect_id: effect_id.clone(),
            source,
            runtime_configuration: None,
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
            workspace_preparation: workspace_preparation.clone(),
            message_boundary: *message_boundary,
            presented_messages: presented_messages.clone(),
        })
    }

    pub fn set_deadline_ms(&mut self, new_deadline_ms: i64) {
        match self {
            Self::RoleTurn { deadline_ms, .. } | Self::OracleRun { deadline_ms, .. } => {
                *deadline_ms = new_deadline_ms
            }
        }
    }

    pub fn deadline_ms(&self) -> i64 {
        match self {
            Self::RoleTurn { deadline_ms, .. } | Self::OracleRun { deadline_ms, .. } => {
                *deadline_ms
            }
        }
    }

    pub fn not_before_ms(&self) -> i64 {
        match self {
            Self::RoleTurn { not_before_ms, .. } | Self::OracleRun { not_before_ms, .. } => {
                *not_before_ms
            }
        }
    }

    pub fn budget_deadline_ms(&self) -> Option<i64> {
        match self {
            Self::RoleTurn {
                budget_deadline_ms, ..
            } => Some(*budget_deadline_ms),
            Self::OracleRun { .. } => None,
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
                judged_sha,
                attempt_no,
                effect_id,
                requested_at_ms,
                deadline_ms,
            } => Some((
                effect_id.clone(),
                Self::OracleRun {
                    assertion_ids: assertion_ids.clone(),
                    oracle: oracle.clone(),
                    judged_sha: judged_sha.clone(),
                    attempt_no: *attempt_no,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms,
                    deadline_ms: *deadline_ms,
                    requested_seq,
                },
            )),
            // Exhaustive on purpose: every new `…Requested` event must build
            // its inflight entry here.
            MissionEvent::MissionCreated { .. }
            | MissionEvent::ProposalRecorded { .. }
            | MissionEvent::TeamConfigured { .. }
            | MissionEvent::SkillAdded { .. }
            | MissionEvent::MessageSent { .. }
            | MissionEvent::RoleTurnCompleted { .. }
            | MissionEvent::OracleRunCompleted { .. }
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
    pub workspace_dir: String,
    /// Target repo HEAD at mission creation.
    pub base_sha: String,
    pub config: MissionConfig,
    pub delegation: super::DelegationSet,
    pub team: Option<super::TeamRevision>,
    pub team_history: BTreeMap<u32, super::TeamRevision>,
    #[serde(default)]
    pub skills: BTreeMap<String, super::MissionSkill>,
    pub phase: MissionPhase,
    pub plan: Option<Plan>,
    pub contract: BTreeMap<AssertionId, AssertionState>,
    #[serde(default)]
    pub superseded_assertions: Vec<SupersededAssertion>,
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
    /// Active candidate/guidance/evidence input for the next planning pass.
    pub planning_input: PlanningInput,
    /// Complete plan proposal awaiting approval or automatic promotion.
    pub proposal: Option<super::MissionProposal>,
    /// Latest recorded artifact head (starts at `base_sha`). Oracle verdicts
    /// are fresh only when judged at this commit.
    pub current_sha: String,
    /// Per-oracle dispatch counter (attempt numbering).
    pub oracle_attempts: BTreeMap<OracleName, u32>,
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
    /// Exact same-mission evidence identities eligible for message references.
    #[serde(default)]
    pub authoritative_receipts: BTreeSet<super::EffectId>,
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
    /// Derived each fold from failed nodes, gate results, and the
    /// approval gate, minus anything a decision has resolved.
    pub open_attention: BTreeMap<String, AttentionItem>,
    /// The pending proposal was approved (the durable approval gate was answered).
    /// Cleared on every new proposal when the gate is on, so approval of
    /// one plan revision never authorizes the next (ADR 0006).
    pub proposal_approved: bool,
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
    /// Oracles whose obligation a human waived (`accept` on an oracle
    /// failure): the mission may finish, but never *verified* — there is no
    /// authoritative verdict.
    pub waived_oracles: BTreeSet<OracleName>,
    /// Terminal-review runtime (config-gated; default-empty for every
    /// pre-feature mission and snapshot).
    #[serde(default)]
    pub gap_review: GapReviewState,
    /// Sequence number of the last folded event (optimistic-concurrency head).
    pub head: u64,
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
            .gap_review
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.effect_id() == effect_id);
        let current_feedback = self
            .planning_input
            .refinement
            .as_ref()
            .and_then(|refinement| match refinement {
                PlanningRefinement::FailureEvidence(feedback) => Some(feedback),
                PlanningRefinement::Guidance(_) => None,
            })
            .is_some_and(|feedback| feedback.evidence.role_attempts().contains(effect_id));
        let current_attention = self
            .open_attention
            .values()
            .any(|item| item.evidence.role_attempts().contains(effect_id));
        let inflight = self.inflight.contains_key(effect_id)
            && receipt.disposition == RoleAttemptDisposition::Active;
        let evidence_use = if current_task
            || current_advisory
            || current_review
            || current_feedback
            || current_attention
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
        self.task_last_role_attempt(task_id)?.failure()
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
        let RoleEffectSource::Turn {
            request,
            plan_revision,
        } = &receipt.source;
        if &request.role_instance != validator
            || !request.assertion_ids.contains(assertion_id)
            || request.task_id.is_some()
        {
            return None;
        }
        let role = self
            .team_history
            .get(&request.team_revision)?
            .role(&request.role_instance)?;
        if role.output != super::OutputSemantics::EmitsVerdict
            || request.team_revision != self.team.as_ref()?.revision
            || *plan_revision != self.revision
            || request.base_sha != self.deliverable_head()
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

    fn taskless_assignment_receipts<'a>(
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

    pub fn gap_review_receipt(&self) -> Option<&RoleAttemptReceipt> {
        self.gap_review
            .outcome
            .as_ref()
            .and_then(|outcome| self.role_attempt_receipts.get(outcome.effect_id()))
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
            (super::OutputSemantics::ProducesArtifact, Some(task_id)) => {
                team.task_assignments.get(task_id) == Some(role_instance)
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
        !self.phase.is_terminal()
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

    pub fn conversation_legal_actions(&self, role_instance: &RoleInstanceId) -> Vec<&'static str> {
        if !self.conversation_is_messageable(role_instance) {
            return Vec::new();
        }
        let mut actions = match self.conversations[role_instance].lifecycle {
            ConversationLifecycle::AwaitingLead => Vec::new(),
            ConversationLifecycle::Running => vec!["mission status"],
            ConversationLifecycle::Ready | ConversationLifecycle::ReworkingInvalidHandoff => {
                vec!["mission advance"]
            }
            ConversationLifecycle::Completed | ConversationLifecycle::Retired => return Vec::new(),
        };
        if self.conversation_accepts_message(role_instance) {
            actions.push("mission send");
        }
        actions
    }

    pub fn deliverable_head(&self) -> &str {
        &self.current_sha
    }

    pub fn durable_cancellation(&self, effect_id: &super::EffectId) -> Option<DurableCancellation> {
        if let MissionPhase::Aborted { reason } = &self.phase {
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
        !self.phase.is_terminal()
            && self
                .parked_effects
                .get(effect_id)
                .is_some_and(|effect| self.parked_effect_remains_continuable(effect))
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

    pub(crate) fn parked_effect_remains_continuable(&self, effect: &ParkedEffect) -> bool {
        match effect {
            ParkedEffect::RoleTurn {
                role_instance,
                task_id,
            } => match task_id {
                Some(task_id) => {
                    self.team.as_ref().is_some_and(|team| {
                        team.task_assignments.get(task_id) == Some(role_instance)
                    }) && self
                        .tasks
                        .get(task_id)
                        .is_some_and(|task| task.status == TaskStatus::Failed)
                }
                None => self.conversation_is_messageable(role_instance),
            },
            ParkedEffect::OracleRun { oracle } => self.oracle_failures.contains_key(oracle),
        }
    }

    pub fn active_tasks(&self) -> &BTreeMap<TaskId, TaskRuntimeState> {
        &self.tasks
    }

    pub(crate) fn oracle_automatic_retry_remaining(&self, oracle: &OracleName) -> bool {
        self.oracle_failures
            .get(oracle)
            .is_some_and(TypedFailure::is_transient)
            && self
                .oracle_attempts
                .get(oracle)
                .copied()
                .unwrap_or_default()
                < self.config.recovery.max_attempts
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
                    && assertion
                        .last_authoritative
                        .as_ref()
                        .is_none_or(|verdict| !verdict.is_fresh_at(self.deliverable_head()))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub(crate) fn oracle_dispatchable(&self, oracle: &OracleName) -> bool {
        !self.waived_oracles.contains(oracle)
            && (!self.oracle_failures.contains_key(oracle)
                || self.oracle_automatic_retry_remaining(oracle))
    }
}
