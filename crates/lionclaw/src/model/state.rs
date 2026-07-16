//! `MissionState` — a pure fold over the event log. Deterministic containers
//! only (`BTreeMap`), derives `PartialEq` so the fold-litmus test can assert
//! rebuilt state equality. Wall-clock time never enters this type.

use std::collections::BTreeMap;

use lionclaw_runtime_api::TypedFailure;
use serde::{Deserialize, Serialize};

use super::event::{
    EffectResource, Gap, GapSeverity, MissionConfig, MissionTypeRef, PayloadRef,
    RuntimeConfigurationEvidence,
};
use super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
use super::plan::{Plan, PlanProposal};
use super::verdict::{AuthoritativeVerdict, FinishClass};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum MissionPhase {
    /// No execution plan yet: drives the in-engine planning DAG (research →
    /// red-team → author) toward a proposal, or — with an empty planning DAG —
    /// idles awaiting a manually proposed plan.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRuntimeState {
    pub status: TaskStatus,
    /// Monotonic dispatch identity. Successful reruns never reset it.
    pub attempts: u32,
    /// Failures since the last successful outcome or explicit recovery
    /// decision. This, not the dispatch identity, bounds automatic recovery.
    pub consecutive_failures: u32,
    /// The latest handoff report, for threading into downstream prompts.
    #[serde(default)]
    pub last_report: Option<PayloadRef>,
    #[serde(default)]
    pub last_failure: Option<TypedFailure>,
    /// Engine-routed repair feedback for this task's next attempt.
    #[serde(default)]
    pub feedback: Vec<FailureFeedback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_runtime_configuration: Option<RuntimeConfigurationEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_base_sha: Option<String>,
    #[serde(default)]
    pub assignment_epoch: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_response: Option<PayloadRef>,
}

impl TaskRuntimeState {
    pub fn automatic_retry_remaining(&self, max_attempts: u32) -> bool {
        self.status == TaskStatus::Failed
            && self.consecutive_failures < max_attempts
            && self
                .last_failure
                .as_ref()
                .is_some_and(|failure| failure.is_transient() || failure.is_invalid_output())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureEvidence {
    pub exit_code: i32,
    pub exit_signal: Option<i32>,
    pub stdout: PayloadRef,
    pub stderr: PayloadRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureFeedback {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<FailureEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<PayloadRef>,
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
    pub latest_rejected_proposal: Option<PlanProposal>,
    /// The single active refinement input for the next planning pass.
    pub refinement: Option<PlanningRefinement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectCleanupFailure {
    pub effect_id: super::EffectId,
    pub resource: EffectResource,
    pub failure: TypedFailure,
}

/// Runtime status of the contract-free planning DAG. A separate map from the
/// execution `tasks` so a planning id can never satisfy execution coverage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlanningState {
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
}

/// Zenith's sticky per-assertion advisory status: `pending → passed` is
/// sticky; anything else that reports non-pass lands `failed`.
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
    pub advisory: AdvisoryStatus,
    /// Last verdict per validator task (gate evaluation input).
    pub last_advisory: BTreeMap<TaskId, bool>,
    /// Only the fold can mint this, and only from `OracleRunCompleted`.
    pub last_authoritative: Option<AuthoritativeVerdict>,
}

/// The terminal-review ledger: fold-owned, advisory-only (never read by
/// `classify_finish`). All-default == "no review has run".
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TerminalReviewState {
    /// Dispatch counter (mirrors `oracle_attempts`): folded from
    /// `TerminalReviewRequested.attempt_no`; the next dispatch and its
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

impl TerminalReviewState {
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
    pub fn acknowledges(&self, verdict: &TerminalReviewVerdict) -> bool {
        self.accepted.as_ref().is_some_and(|a| {
            a.kind == ReviewAcceptanceKind::AcknowledgedGaps && a.judged_sha == verdict.judged_sha
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReviewOutcome {
    Verdict(TerminalReviewVerdict),
    /// The reviewer failed to run or hand off a verdict (infrastructure),
    /// until a decision clears it. Prevents a broken reviewer from
    /// re-requesting forever (mirrors `oracle_failures`).
    Failed {
        failure: TypedFailure,
    },
}

/// Exact failed effect generation that may be reopened by `continue`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParkedEffect {
    RoleRun { task_id: TaskId },
    OracleRun { oracle: OracleName },
    TerminalReview,
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

/// A terminal reviewer's verdict. Plain public data — deliberately NOT an
/// `AuthoritativeVerdict` (private-field mint, `verdict.rs`): this verdict
/// is advisory, mints nothing, and gates closure only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalReviewVerdict {
    pub judged_sha: String,
    pub passed: bool,
    #[serde(default)]
    pub gaps: Vec<Gap>,
    pub report: PayloadRef,
}

impl TerminalReviewVerdict {
    /// Same freshness law as `AuthoritativeVerdict::is_fresh_at`.
    pub fn is_fresh_at(&self, current_sha: &str) -> bool {
        self.judged_sha == current_sha
    }

    /// Fail-closed: a blocking gap dominates the reviewer's own summary bit,
    /// and a fail with no structured gaps still blocks (the report is the
    /// evidence). Closure is clean iff passed AND no blocking gap.
    pub fn blocking(&self) -> bool {
        !self.passed
            || self
                .gaps
                .iter()
                .any(|g| g.severity == GapSeverity::Blocking)
    }
}

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
    /// The terminal review's blocking verdict awaits a human (revise to
    /// remediate / retry to re-run / accept to acknowledge-and-close /
    /// abort). Raised only when the mission would otherwise close, so
    /// remediation work auto-clears it.
    TerminalReviewGaps,
    /// The terminal reviewer failed to run or hand off a verdict
    /// (infrastructure), distinct from a verdict with gaps.
    TerminalReviewFailed,
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
            Self::TerminalReviewGaps => "terminal_review_gaps",
            Self::TerminalReviewFailed => "terminal_review_failed",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<FailureEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<PayloadRef>,
    pub report: String,
}

/// A `…Requested` event without a recorded outcome. The active driver executes
/// it; a later driver cleans and marks it interrupted rather than replaying it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InflightEffect {
    RoleRun {
        task_id: TaskId,
        attempt_no: u32,
        role: RoleName,
        runtime: String,
        prompt: PayloadRef,
        base_sha: String,
        assignment_epoch: u32,
        recreate_workspace: bool,
        runtime_configuration: Option<super::RuntimeConfigurationEvidence>,
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
    TerminalReview {
        attempt_no: u32,
        role: RoleName,
        runtime: String,
        prompt: PayloadRef,
        judged_sha: String,
        /// Carried from the event so the runner's handoff-forgery check
        /// still has its expected token after a crash/resume.
        nonce: String,
        runtime_configuration: Option<super::RuntimeConfigurationEvidence>,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        budget_deadline_ms: i64,
        requested_seq: u64,
    },
}

impl InflightEffect {
    pub fn set_deadline_ms(&mut self, new_deadline_ms: i64) {
        match self {
            Self::RoleRun { deadline_ms, .. }
            | Self::OracleRun { deadline_ms, .. }
            | Self::TerminalReview { deadline_ms, .. } => *deadline_ms = new_deadline_ms,
        }
    }

    pub fn deadline_ms(&self) -> i64 {
        match self {
            Self::RoleRun { deadline_ms, .. }
            | Self::OracleRun { deadline_ms, .. }
            | Self::TerminalReview { deadline_ms, .. } => *deadline_ms,
        }
    }

    pub fn not_before_ms(&self) -> i64 {
        match self {
            Self::RoleRun { not_before_ms, .. }
            | Self::OracleRun { not_before_ms, .. }
            | Self::TerminalReview { not_before_ms, .. } => *not_before_ms,
        }
    }

    pub fn budget_deadline_ms(&self) -> Option<i64> {
        match self {
            Self::RoleRun {
                budget_deadline_ms, ..
            }
            | Self::TerminalReview {
                budget_deadline_ms, ..
            } => Some(*budget_deadline_ms),
            Self::OracleRun { .. } => None,
        }
    }

    /// Build the inflight entry for a `…Requested` event.
    pub fn from_request(
        event: &super::event::MissionEvent,
        requested_seq: u64,
    ) -> Option<(super::EffectId, Self)> {
        use super::event::MissionEvent;
        match event {
            MissionEvent::RoleRunRequested {
                task_id,
                attempt_no,
                effect_id,
                role,
                runtime,
                prompt,
                base_sha,
                assignment_epoch,
                recreate_workspace,
                requested_at_ms,
                not_before_ms,
                deadline_ms,
                budget_deadline_ms,
            } => Some((
                effect_id.clone(),
                Self::RoleRun {
                    task_id: task_id.clone(),
                    attempt_no: *attempt_no,
                    role: role.clone(),
                    runtime: runtime.clone(),
                    prompt: prompt.clone(),
                    base_sha: base_sha.clone(),
                    assignment_epoch: *assignment_epoch,
                    recreate_workspace: *recreate_workspace,
                    runtime_configuration: None,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms: *not_before_ms,
                    deadline_ms: *deadline_ms,
                    budget_deadline_ms: *budget_deadline_ms,
                    requested_seq,
                },
            )),
            MissionEvent::OracleRunRequested {
                assertion_ids,
                oracle,
                judged_sha,
                attempt_no,
                effect_id,
                requested_at_ms,
                not_before_ms,
                deadline_ms,
            } => Some((
                effect_id.clone(),
                Self::OracleRun {
                    assertion_ids: assertion_ids.clone(),
                    oracle: oracle.clone(),
                    judged_sha: judged_sha.clone(),
                    attempt_no: *attempt_no,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms: *not_before_ms,
                    deadline_ms: *deadline_ms,
                    requested_seq,
                },
            )),
            MissionEvent::TerminalReviewRequested {
                attempt_no,
                effect_id,
                role,
                runtime,
                prompt,
                judged_sha,
                nonce,
                requested_at_ms,
                not_before_ms,
                deadline_ms,
                budget_deadline_ms,
            } => Some((
                effect_id.clone(),
                Self::TerminalReview {
                    attempt_no: *attempt_no,
                    role: role.clone(),
                    runtime: runtime.clone(),
                    prompt: prompt.clone(),
                    judged_sha: judged_sha.clone(),
                    nonce: nonce.clone(),
                    runtime_configuration: None,
                    requested_at_ms: *requested_at_ms,
                    not_before_ms: *not_before_ms,
                    deadline_ms: *deadline_ms,
                    budget_deadline_ms: *budget_deadline_ms,
                    requested_seq,
                },
            )),
            // Exhaustive on purpose: every new `…Requested` event must build
            // its inflight entry here.
            MissionEvent::MissionCreated { .. }
            | MissionEvent::PlanProposed { .. }
            | MissionEvent::TaskWorkspacePrepared { .. }
            | MissionEvent::EffectRuntimeConfigured { .. }
            | MissionEvent::RoleRunCompleted { .. }
            | MissionEvent::OracleRunCompleted { .. }
            | MissionEvent::TerminalReviewCompleted { .. }
            | MissionEvent::MissionAborted { .. }
            | MissionEvent::DecisionRecorded { .. }
            | MissionEvent::ControlRequested { .. }
            | MissionEvent::EffectDeadlineReached { .. }
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
    /// The runtime profile id roles run under.
    pub runtime: String,
    /// The confinement image, resolved to a content id at start.
    pub image_id: String,
    pub workspace_dir: String,
    /// Target repo HEAD at mission creation.
    pub base_sha: String,
    pub config: MissionConfig,
    pub phase: MissionPhase,
    pub plan: Option<Plan>,
    pub contract: BTreeMap<AssertionId, AssertionState>,
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
    /// The contract-free planning phase: the runtime status of the mission
    /// type's planning DAG. Disjoint from `tasks` (execution); the active era
    /// decides which map receives role events and operator decisions.
    pub planning: PlanningState,
    /// Revision the active planning DAG is authoring against. `None` means the
    /// planning DAG is idle; this is independent of whether an accepted plan
    /// already exists, so the same DAG can author repairs.
    pub planning_base_revision: Option<u32>,
    /// Active candidate/guidance/evidence input for the next planning pass.
    pub planning_input: PlanningInput,
    /// Complete plan proposal awaiting approval or automatic promotion.
    pub proposal: Option<PlanProposal>,
    /// Latest recorded artifact head (starts at `base_sha`). Oracle verdicts
    /// are fresh only when judged at this commit.
    pub current_sha: String,
    /// Per-oracle dispatch counter (attempt numbering).
    pub oracle_attempts: BTreeMap<OracleName, u32>,
    pub inflight: BTreeMap<super::EffectId, InflightEffect>,
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
    pub acknowledged_gates: std::collections::BTreeSet<TaskId>,
    /// Nodes whose handoff asked for a human look (`request_attention`),
    /// until a decision clears them.
    pub flagged_nodes: std::collections::BTreeSet<TaskId>,
    /// Oracles that failed to *run* (infrastructure failure, distinct from a
    /// nonzero exit) → mapped to the failure detail, until a decision clears
    /// them. Prevents a broken oracle from re-requesting forever.
    pub oracle_failures: BTreeMap<OracleName, TypedFailure>,
    /// Oracles whose obligation a human waived (`accept` on an oracle
    /// failure): the mission may finish, but never *verified* — there is no
    /// authoritative verdict.
    pub waived_oracles: std::collections::BTreeSet<OracleName>,
    /// Terminal-review runtime (config-gated; default-empty for every
    /// pre-feature mission and snapshot).
    #[serde(default)]
    pub terminal_review: TerminalReviewState,
    /// Sequence number of the last folded event (optimistic-concurrency head).
    pub head: u64,
}

impl MissionState {
    /// The authoritative serial artifact head. Later slices may change how
    /// this value is produced; proof and closure consumers use this boundary.
    pub fn deliverable_head(&self) -> &str {
        &self.current_sha
    }

    /// Runtime state for the task era currently allowed to dispatch roles.
    pub(crate) fn active_tasks(&self) -> &BTreeMap<TaskId, TaskRuntimeState> {
        if self.planning_base_revision.is_some() {
            &self.planning.tasks
        } else {
            &self.tasks
        }
    }

    pub(crate) fn active_tasks_mut(&mut self) -> &mut BTreeMap<TaskId, TaskRuntimeState> {
        if self.planning_base_revision.is_some() {
            &mut self.planning.tasks
        } else {
            &mut self.tasks
        }
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
}

#[cfg(test)]
mod slug_tests {
    use super::*;
    use crate::model::{FinishClass, OutputSemantics, StopBar};

    /// Every `slug()` must equal the enum's serde repr — the single source that
    /// keeps the CLI, the fold's attention ids, and the wire format from
    /// drifting (`InternallyConsistent` → `internally_consistent`, not
    /// `internallyconsistent`).
    fn assert_slug<T: serde::Serialize>(value: &T, slug: &str) {
        let serde = serde_json::to_value(value).unwrap();
        // Unit enums serialize to a bare string; the internally-tagged
        // `MissionPhase` to an object whose tag field is the variant name.
        let repr = serde
            .as_str()
            .or_else(|| serde.get("phase").and_then(|v| v.as_str()))
            .expect("a string or a tagged object");
        assert_eq!(repr, slug, "slug drifted from the serde repr");
    }

    #[test]
    fn slugs_match_the_serde_repr() {
        for k in [
            AttentionKind::NodeFailed,
            AttentionKind::NodeAttention,
            AttentionKind::OracleFailed,
            AttentionKind::OracleVerdictFailed,
            AttentionKind::GateFailed,
            AttentionKind::GateCheckpoint,
            AttentionKind::PlanProposal,
            AttentionKind::TerminalReviewGaps,
            AttentionKind::TerminalReviewFailed,
        ] {
            assert_slug(&k, k.slug());
        }
        for g in [
            GapSeverity::Blocking,
            GapSeverity::Major,
            GapSeverity::Minor,
        ] {
            assert_slug(&g, g.slug());
        }
        for k in [
            ReviewAcceptanceKind::AcknowledgedGaps,
            ReviewAcceptanceKind::Waived,
        ] {
            assert_slug(&k, k.slug());
        }
        for s in [
            AdvisoryStatus::Pending,
            AdvisoryStatus::Passed,
            AdvisoryStatus::Failed,
        ] {
            assert_slug(&s, s.slug());
        }
        for f in [
            FinishClass::Verified,
            FinishClass::InternallyConsistent,
            FinishClass::Unverified,
        ] {
            assert_slug(&f, f.slug());
        }
        for b in [StopBar::Verified, StopBar::Reviewed] {
            assert_slug(&b, b.slug());
        }
        for o in [
            OutputSemantics::ProducesReport,
            OutputSemantics::ProducesArtifact,
            OutputSemantics::EmitsVerdict,
            OutputSemantics::EmitsGapVerdict,
            OutputSemantics::ProposesPlan,
        ] {
            assert_slug(&o, o.slug());
        }
        for p in [
            MissionPhase::Planning,
            MissionPhase::Running,
            MissionPhase::AttentionNeeded,
            MissionPhase::Done {
                finish: FinishClass::Verified,
            },
            MissionPhase::Aborted {
                reason: String::new(),
            },
        ] {
            assert_slug(&p, p.slug());
        }
    }
}
