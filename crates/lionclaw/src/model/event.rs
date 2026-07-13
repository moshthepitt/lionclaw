//! The mission event log vocabulary.
//!
//! An event is a **fact the fold cannot compute**: a recorded outcome, a
//! proposed plan, a human decision. Everything the engine can derive
//! (task/assertion status, gate results, attention, phase, finish class) is
//! fold-derived and never stored, so state/log divergence is unrepresentable.
//!
//! Non-deterministic or side-effecting steps are two events: `…Requested`
//! (intent + content-derived effect ID; consumed by the effect driver)
//! then `…Completed`/`…Failed` (outcome fact; consumed by the fold). The log
//! stores outcomes, never executable intentions.
//!
//! Events are additive-only and version-stamped; never rewrite history.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, InputName, MissionId, OracleName, RoleName, TaskId};
use super::plan::{PlanProposal, PlanningDag};

pub const SCHEMA_VERSION: u32 = 3;

/// Reference to a content-addressed blob on durable-fs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub algo: String,
    pub hex: String,
    pub len: u64,
}

/// Payload data: inline for small values, blob reference above the
/// externalization threshold (enforced by the store at append time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadRef {
    Inline { text: String },
    Blob(BlobRef),
}

impl PayloadRef {
    pub fn inline(text: impl Into<String>) -> Self {
        Self::Inline { text: text.into() }
    }
}

/// The honesty bar a mission type declares: what "finished" must mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopBar {
    Verified,
    Reviewed,
}

impl StopBar {
    /// The stable snake_case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Reviewed => "reviewed",
        }
    }
}

/// The mission type a mission was created against, pinned by content digest.
/// The digest is verified on every engine open, so the instrument of judgment
/// (roles, oracles) cannot be swapped after the mission starts. Plain data —
/// the shell computes the digest (`mission_type::load_mission_type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionTypeRef {
    pub name: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionConfig {
    pub stop: StopBar,
    /// The mission type's planning DAG (how an objective becomes a proposed
    /// contract). Empty ⇒ no in-engine planning; the mission awaits a manually
    /// proposed plan.
    #[serde(default)]
    pub planning: PlanningDag,
    #[serde(default)]
    pub recovery: RecoveryConfig,
    /// The mission type's closing review (a fresh-context judge of the final
    /// tree against the objective). `None` ⇒ feature off: every derivation
    /// short-circuits, so pre-feature event logs re-derive identically.
    /// Skipped when absent so non-review missions stay byte-identical on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_review: Option<TerminalReviewConfig>,
}

impl Default for MissionConfig {
    fn default() -> Self {
        Self {
            stop: StopBar::Verified,
            planning: PlanningDag::default(),
            recovery: RecoveryConfig::default(),
            terminal_review: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RecoveryConfig {
    pub max_attempts: u32,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

/// The closing review a mission type declares: an `emits-gap-verdict` role the
/// engine dispatches contract-blind once work and oracle obligations settle.
/// Engine-owned structure (declared in `mission.toml`), never plan-authored,
/// so a planner cannot omit or weaken it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalReviewConfig {
    pub role: RoleName,
}

/// Provenance stamps carried by every envelope.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VersionStamps {
    pub schema_version: u32,
    pub engine_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_hash: Option<String>,
}

/// What a role's agent handed back. Written by the agent as
/// `/mission/handoff/handoff.json`, parsed strictly by the runner.
/// Mirrors Zenith's `WorkHandoff`/`ValidateHandoff` (Apache-2.0,
/// Intelligent Internet, `models.py`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Handoff {
    Work {
        done: bool,
        report: PayloadRef,
        request_attention: bool,
    },
    Validate {
        done: bool,
        report: PayloadRef,
        items: Vec<ValidationItem>,
        passed: bool,
        request_attention: bool,
    },
    /// The engine-owned terminal review's contract-blind product verdict.
    /// Separate from `Validate` so per-assertion items and objective-level
    /// gaps cannot be accepted on the wrong path and silently discarded.
    Review {
        done: bool,
        report: PayloadRef,
        passed: bool,
        gaps: Vec<Gap>,
        /// Echo of the per-attempt nonce the engine put in the prompt.
        nonce: String,
    },
    /// The planning author's deliverable: a proposed contract + task DAG. It has
    /// **no verdict field** — a proposal is gradeless and can never mint
    /// authority; it becomes `state.contract` only after approval.
    Plan {
        done: bool,
        report: PayloadRef,
        #[serde(default)]
        proposal: Option<PlanProposal>,
        request_attention: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationItem {
    pub item_id: AssertionId,
    pub passed: bool,
}

/// One typed product gap from a terminal review. `severity` is the only
/// field the engine branches on; the rest is structured evidence for the
/// human and for remediation revisions. Strict fields (`deny_unknown_fields`,
/// required prose) force the reviewer to decompose instead of hand-waving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gap {
    /// Reviewer-chosen stable label within one verdict (e.g. "GAP-1").
    /// Display/reference only — never an engine key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub severity: GapSeverity,
    /// The objective requirement this gap is against (reviewer prose — the
    /// reviewer is contract-blind, so this is never an assertion id).
    pub requirement: String,
    pub expected: String,
    pub observed: String,
    /// Observed-behavior evidence (commands run, output seen, file paths).
    /// The runner rejects a handoff whose gap leaves this (or any prose
    /// field) empty — a gap is a falsifiable claim, never a bare assertion.
    #[serde(default)]
    pub evidence: String,
}

/// How bad a gap is, grounded in the objective. Only `Blocking` gates
/// closure; the rest is triage information for the receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapSeverity {
    /// The objective is not met without this; parks closure.
    Blocking,
    /// A real product defect, but the core objective still holds.
    Major,
    /// Polish or hardening beyond what the objective asks.
    Minor,
}

impl GapSeverity {
    /// The stable snake_case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Blocking => "blocking",
            Self::Major => "major",
            Self::Minor => "minor",
        }
    }
}

/// Runner-computed artifact fact: the commits that now exist in the target
/// repo. Recorded by the engine from the worktree, never claimed by the
/// agent — git is content-addressed, so `rev-parse` reconciles on resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOutcome {
    pub base_sha: String,
    pub head_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedInputRef {
    pub name: InputName,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunErrorKind {
    Launch,
    TurnFailed,
    Timeout,
    HandoffMissing,
    HandoffInvalid,
    DirtyWorktree,
    Infra,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectResource {
    Container,
    RuntimeSecret,
    AttemptDirectory,
    WriterRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MissionEvent {
    MissionCreated {
        objective: String,
        /// The mission type, pinned by content digest (verified on every open).
        mission_type: MissionTypeRef,
        /// The runtime profile id roles run under (recorded so later commands
        /// need no `--runtime`).
        runtime: String,
        /// The confinement image resolved to a content id at start, so a
        /// rebuilt tag can't silently change the instrument mid-mission.
        image_id: String,
        workspace_dir: String,
        /// HEAD of the target repo when the mission was created.
        base_sha: String,
        config: MissionConfig,
    },
    PlanProposed {
        proposal: PlanProposal,
        /// sha256 of the canonical plan JSON.
        plan_hash: String,
        actor: String,
        justification: String,
    },
    RoleRunRequested {
        task_id: TaskId,
        attempt_no: u32,
        effect_id: super::EffectId,
        role: RoleName,
        /// Effective runtime profile, resolved before the request is recorded.
        runtime: String,
        /// Assembled prompt, persisted before the request is recorded so a
        /// resume re-dispatches byte-identical input.
        prompt: PayloadRef,
        /// Commit the role's workspace is created at.
        base_sha: String,
    },
    RoleRunCompleted {
        task_id: TaskId,
        attempt_no: u32,
        effect_id: super::EffectId,
        handoff: Handoff,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact: Option<ArtifactOutcome>,
    },
    RoleRunFailed {
        task_id: TaskId,
        attempt_no: u32,
        effect_id: super::EffectId,
        failure: super::RunFailure,
    },
    OracleRunRequested {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        effect_id: super::EffectId,
    },
    OracleRunCompleted {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        effect_id: super::EffectId,
        exit_code: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_signal: Option<i32>,
        stdout: PayloadRef,
        stderr: PayloadRef,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        prepared_inputs: Vec<PreparedInputRef>,
        duration_ms: u64,
    },
    OracleRunFailed {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        effect_id: super::EffectId,
        failure: super::RunFailure,
    },
    /// The closing review was dispatched: a fresh-context `emits-gap-verdict`
    /// role judging the tree at `judged_sha` against the objective,
    /// contract-blind. Config-declared (`MissionConfig::terminal_review`),
    /// never a plan task — hence no `task_id`.
    TerminalReviewRequested {
        attempt_no: u32,
        effect_id: super::EffectId,
        role: RoleName,
        /// Effective runtime profile, resolved before the request is recorded.
        runtime: String,
        /// Assembled prompt, persisted before the request is recorded so a
        /// resume re-dispatches byte-identical input.
        prompt: PayloadRef,
        /// The commit under review; the verdict is stamped at this sha.
        judged_sha: String,
        /// Per-attempt random token the prompt tells the reviewer to echo in
        /// its handoff. Rides the event so the runner's forgery check
        /// survives crash/resume (the inflight effect rebuilds from here).
        nonce: String,
    },
    /// The reviewer's verdict — advisory by construction: the fold stores it
    /// in `terminal_review`, never in any assertion's `last_authoritative`,
    /// and `classify_finish` never reads it. It gates closure only.
    TerminalReviewCompleted {
        attempt_no: u32,
        effect_id: super::EffectId,
        judged_sha: String,
        /// The reviewer's own summary bit. A blocking gap dominates it
        /// (fail-closed) — see `TerminalReviewVerdict::blocking`.
        passed: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        gaps: Vec<Gap>,
        report: PayloadRef,
    },
    /// The reviewer failed to *run or hand off a verdict* (infrastructure or
    /// an unfinished review), distinct from a verdict with gaps.
    TerminalReviewFailed {
        attempt_no: u32,
        effect_id: super::EffectId,
        judged_sha: String,
        failure: super::RunFailure,
    },
    /// Cleanup failed without settling the original request. The next driver
    /// retries the same exact resource operation before any new dispatch.
    EffectCleanupFailed {
        effect_id: super::EffectId,
        resource: EffectResource,
        failure: super::RunFailure,
    },
    MissionAborted {
        reason: String,
        actor: String,
    },
    /// A human/orchestrator decision resolving an open attention item (a
    /// durable interrupt). The fold applies the action and marks the item
    /// resolved. Ported from Zenith's `decide_attention` (Apache-2.0,
    /// Intelligent Internet, `controller.py`).
    DecisionRecorded {
        attention_id: String,
        action: DecisionAction,
        justification: String,
        actor: String,
    },
}

/// The actions a decision can take on an open attention item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum DecisionAction {
    /// Approve a proposed plan or a cleared gate checkpoint.
    Approve,
    /// Re-run the same failed attempt without changing the plan.
    Retry,
    /// Reopen the work that owns a failed authoritative assertion.
    Repair,
    /// Re-enter the planning DAG to propose a complete next plan revision.
    Revise,
    /// Accept a below-bar outcome and proceed, with explicit justification.
    Accept,
    /// Abort the mission.
    Abort,
}

impl DecisionAction {
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Retry => "retry",
            Self::Repair => "repair",
            Self::Revise => "revise",
            Self::Accept => "accept",
            Self::Abort => "abort",
        }
    }
}

/// Role of an event within a two-event (request/outcome) effect pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectEventClass {
    Request,
    Outcome,
}

impl MissionEvent {
    /// Stable type tag (matches the serde tag; persisted for queries).
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::MissionCreated { .. } => "mission_created",
            Self::PlanProposed { .. } => "plan_proposed",
            Self::RoleRunRequested { .. } => "role_run_requested",
            Self::RoleRunCompleted { .. } => "role_run_completed",
            Self::RoleRunFailed { .. } => "role_run_failed",
            Self::OracleRunRequested { .. } => "oracle_run_requested",
            Self::OracleRunCompleted { .. } => "oracle_run_completed",
            Self::OracleRunFailed { .. } => "oracle_run_failed",
            Self::TerminalReviewRequested { .. } => "terminal_review_requested",
            Self::TerminalReviewCompleted { .. } => "terminal_review_completed",
            Self::TerminalReviewFailed { .. } => "terminal_review_failed",
            Self::EffectCleanupFailed { .. } => "effect_cleanup_failed",
            Self::MissionAborted { .. } => "mission_aborted",
            Self::DecisionRecorded { .. } => "decision_recorded",
        }
    }

    /// The effect ID and its class, for events participating in a
    /// request/outcome pair.
    pub fn effect_identity(&self) -> Option<(EffectEventClass, &str)> {
        match self {
            Self::RoleRunRequested { effect_id, .. }
            | Self::OracleRunRequested { effect_id, .. }
            | Self::TerminalReviewRequested { effect_id, .. } => {
                Some((EffectEventClass::Request, effect_id.as_str()))
            }
            Self::RoleRunCompleted { effect_id, .. }
            | Self::RoleRunFailed { effect_id, .. }
            | Self::OracleRunCompleted { effect_id, .. }
            | Self::OracleRunFailed { effect_id, .. }
            | Self::TerminalReviewCompleted { effect_id, .. }
            | Self::TerminalReviewFailed { effect_id, .. } => {
                Some((EffectEventClass::Outcome, effect_id.as_str()))
            }
            // Fact events carry no effect ID. Exhaustive on purpose: a new
            // effect-style event must decide its class here.
            Self::MissionCreated { .. }
            | Self::PlanProposed { .. }
            | Self::MissionAborted { .. }
            | Self::DecisionRecorded { .. }
            | Self::EffectCleanupFailed { .. } => None,
        }
    }
}

/// A persisted event with its log position and provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub mission_id: MissionId,
    pub sequence_no: u64,
    /// Wall-clock metadata only — the fold never branches on it.
    pub recorded_at_ms: i64,
    pub stamps: VersionStamps,
    pub event: MissionEvent,
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    /// The optional review configuration stays absent from the ordinary wire
    /// shape when it is not configured.
    #[test]
    fn optional_terminal_review_stays_out_of_the_default_wire_shape() {
        let config = MissionConfig::default();
        assert_eq!(config.terminal_review, None);
        let json = serde_json::to_string(&config).expect("serialize");
        assert!(!json.contains("terminal_review"));
        assert!(!json.contains("approval_required"));

        // A pre-feature Validate handoff keeps its exact shape; terminal
        // review uses a separate handoff variant rather than widening it.
        let old_validate = r#"{"type":"validate","done":true,
                               "report":{"kind":"inline","text":"r"},
                               "items":[],"passed":true,"request_attention":false}"#;
        let handoff: Handoff = serde_json::from_str(old_validate).expect("old handoff parses");
        let Handoff::Validate { .. } = &handoff else {
            panic!("expected validate");
        };
        // A validate serializes without terminal-review keys.
        let json = serde_json::to_string(&handoff).expect("serialize");
        assert!(!json.contains("gaps") && !json.contains("nonce"));
    }

    #[test]
    fn plan_proposal_round_trips_strict_requirement_dispositions() {
        use crate::model::{
            Assertion, Plan, Requirement, RequirementDisposition, RequirementId, RequirementKind,
        };

        let event = MissionEvent::PlanProposed {
            proposal: PlanProposal {
                base_revision: 0,
                plan: Plan {
                    requirements: vec![Requirement {
                        id: RequirementId::new("OBJECTIVE-MET").unwrap(),
                        kind: RequirementKind::Capability,
                        prose: "the objective is met".into(),
                        disposition: RequirementDisposition::Covered {
                            assertion_ids: vec![AssertionId::new("VAL-OBJECTIVE").unwrap()],
                        },
                    }],
                    assertions: vec![Assertion {
                        id: AssertionId::new("VAL-OBJECTIVE").unwrap(),
                        prose: "the objective is demonstrably met".into(),
                        oracle: None,
                    }],
                    tasks: vec![],
                },
            },
            plan_hash: "hash".into(),
            actor: "author".into(),
            justification: "initial proposal".into(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<MissionEvent>(&json).unwrap(), event);
    }
}
