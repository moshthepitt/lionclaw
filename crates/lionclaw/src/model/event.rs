//! The mission event log vocabulary.
//!
//! An event is a **fact the fold cannot compute**: a recorded outcome, a
//! submitted plan, a human decision. Everything the engine can derive
//! (task/assertion status, gate results, attention, phase, finish class) is
//! fold-derived and never stored, so state/log divergence is unrepresentable.
//!
//! Non-deterministic or side-effecting steps are two events: `…Requested`
//! (intent + content-derived idempotency key; consumed by the effect driver)
//! then `…Completed`/`…Failed` (outcome fact; consumed by the fold). The log
//! stores outcomes, never executable intentions.
//!
//! Events are additive-only and version-stamped; never rewrite history.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
use super::plan::{Assertion, PlanSubmission, PlanningDag, Task};

pub const SCHEMA_VERSION: u32 = 2;

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
    pub ratification_gate: bool,
    pub stop: StopBar,
    /// The mission type's planning DAG (how an objective becomes a proposed
    /// contract). Empty ⇒ no in-engine planning; the mission awaits a manually
    /// submitted plan.
    #[serde(default)]
    pub planning: PlanningDag,
}

impl Default for MissionConfig {
    fn default() -> Self {
        Self {
            ratification_gate: true,
            stop: StopBar::Verified,
            planning: PlanningDag::default(),
        }
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_schema_hash: Option<String>,
}

/// What a role's agent handed back. Written by the agent as
/// `/mission/handoff/handoff.json`, parsed strictly by the runner.
/// Mirrors Zenith's `WorkHandoff`/`ValidateHandoff` (Apache-2.0,
/// Intelligent Internet, `models.py`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
    /// The planning author's deliverable: a proposed contract + task DAG. It has
    /// **no verdict field** — a proposal is gradeless and can never mint
    /// authority; it becomes `state.contract` only after a human ratifies it.
    Plan {
        done: bool,
        report: PayloadRef,
        #[serde(default)]
        proposal: Option<PlanSubmission>,
        request_attention: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationItem {
    pub item_id: AssertionId,
    pub passed: bool,
}

/// Runner-computed artifact fact: the commits that now exist in the target
/// repo. Recorded by the engine from the worktree, never claimed by the
/// agent — git is content-addressed, so `rev-parse` reconciles on resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOutcome {
    pub base_sha: String,
    pub head_sha: String,
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
    PlanSubmitted {
        plan: PlanSubmission,
        /// sha256 of the canonical plan JSON.
        plan_hash: String,
    },
    RoleRunRequested {
        task_id: TaskId,
        attempt_no: u32,
        idempotency_key: String,
        role: RoleName,
        /// Assembled prompt, persisted before the request is recorded so a
        /// resume re-dispatches byte-identical input.
        prompt: PayloadRef,
        /// Commit the role's workspace is created at.
        base_sha: String,
    },
    RoleRunCompleted {
        task_id: TaskId,
        attempt_no: u32,
        idempotency_key: String,
        handoff: Handoff,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact: Option<ArtifactOutcome>,
    },
    RoleRunFailed {
        task_id: TaskId,
        attempt_no: u32,
        idempotency_key: String,
        error_kind: RunErrorKind,
        detail: String,
        /// True when the engine synthesized this outcome on resume for a
        /// run whose real outcome is unknowable.
        synthesized: bool,
    },
    OracleRunRequested {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        idempotency_key: String,
    },
    OracleRunCompleted {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        idempotency_key: String,
        exit_code: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_signal: Option<i32>,
        stdout: PayloadRef,
        stderr: PayloadRef,
        duration_ms: u64,
    },
    OracleRunFailed {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        idempotency_key: String,
        detail: String,
        synthesized: bool,
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
    /// A mid-mission amendment: add / supersede / cancel tasks and *strengthen*
    /// the contract (add assertions, bind oracles), applied atomically as one
    /// transition between two valid plan revisions (ADRs 0003–0011). A fact
    /// event like `DecisionRecorded` — no idempotency key, no effect-ledger
    /// row. `base_revision` is the plan revision the amendment was authored
    /// against; the fold no-ops the whole event if it no longer matches
    /// (never trust the writer).
    PlanAmended {
        base_revision: u32,
        ops: AmendmentOps,
        actor: String,
        justification: String,
    },
    /// A plan proposed *manually* (not by the in-engine author) — the escape
    /// hatch for a host that hand-authors a `PlanSubmission`. A fact event: the
    /// fold sets `proposal` (gradeless), and it seeds the contract only after
    /// ratification, exactly like an engine-authored proposal.
    PlanProposed {
        plan: PlanSubmission,
        actor: String,
    },
}

/// The operation set of one amendment. All fields default-empty, so an
/// amendment carries only the ops it uses. Task ops are legal on any live
/// task; contract ops are strengthen-only (never remove/unbind/weaken).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AmendmentOps {
    /// New tasks, seeded `Pending`.
    #[serde(default)]
    pub add: Vec<Task>,
    /// Replace a live task: `old → Superseded`, downstream `depends_on`
    /// rewritten `old → new`. `new` must be among `add`.
    #[serde(default)]
    pub supersede: Vec<Supersession>,
    /// Retire a live task without replacement: `old → Superseded`, downstream
    /// `depends_on` drops `old`.
    #[serde(default)]
    pub cancel: Vec<TaskId>,
    /// New contract assertions (must arrive with a covering work task in the
    /// same amendment — coverage is re-validated over the whole plan).
    #[serde(default)]
    pub add_assertion: Vec<Assertion>,
    /// Bind an oracle to a currently-unbound assertion (strengthening). Never
    /// replaces or removes an existing binding.
    #[serde(default)]
    pub bind_oracle: Vec<OracleBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Supersession {
    pub old: TaskId,
    pub new: TaskId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleBinding {
    pub assertion: AssertionId,
    pub oracle: OracleName,
}

/// The actions a decision can take on an open attention item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum DecisionAction {
    /// Approve the plan/contract at the ratification gate.
    Ratify,
    /// Re-dispatch a failed node (valid only for `node_failed`).
    Retry,
    /// Accept the current situation and proceed (accept a node failure, or
    /// confirm a cleared gate checkpoint).
    Continue,
    /// Abort the mission.
    Abort,
}

/// Idempotency role of an event within a two-event (request/outcome) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdemClass {
    Request,
    Outcome,
}

impl MissionEvent {
    /// Stable type tag (matches the serde tag; persisted for queries).
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::MissionCreated { .. } => "mission_created",
            Self::PlanSubmitted { .. } => "plan_submitted",
            Self::RoleRunRequested { .. } => "role_run_requested",
            Self::RoleRunCompleted { .. } => "role_run_completed",
            Self::RoleRunFailed { .. } => "role_run_failed",
            Self::OracleRunRequested { .. } => "oracle_run_requested",
            Self::OracleRunCompleted { .. } => "oracle_run_completed",
            Self::OracleRunFailed { .. } => "oracle_run_failed",
            Self::MissionAborted { .. } => "mission_aborted",
            Self::DecisionRecorded { .. } => "decision_recorded",
            Self::PlanAmended { .. } => "plan_amended",
            Self::PlanProposed { .. } => "plan_proposed",
        }
    }

    /// The idempotency key and its class, for events participating in a
    /// request/outcome pair.
    pub fn idempotency(&self) -> Option<(IdemClass, &str)> {
        match self {
            Self::RoleRunRequested {
                idempotency_key, ..
            }
            | Self::OracleRunRequested {
                idempotency_key, ..
            } => Some((IdemClass::Request, idempotency_key)),
            Self::RoleRunCompleted {
                idempotency_key, ..
            }
            | Self::RoleRunFailed {
                idempotency_key, ..
            }
            | Self::OracleRunCompleted {
                idempotency_key, ..
            }
            | Self::OracleRunFailed {
                idempotency_key, ..
            } => Some((IdemClass::Outcome, idempotency_key)),
            // Fact events carry no idempotency key. Exhaustive on purpose: a new
            // effect-style event must decide its class here, never silently skip
            // the ledger (the bug the deleted terminal-review scaffolding had).
            Self::MissionCreated { .. }
            | Self::PlanSubmitted { .. }
            | Self::MissionAborted { .. }
            | Self::DecisionRecorded { .. }
            | Self::PlanAmended { .. }
            | Self::PlanProposed { .. } => None,
        }
    }

    /// Whether an outcome event records a success (`done`) or failure
    /// (`failed`) for its effect ledger row. Exhaustive on purpose (see
    /// `idempotency`).
    pub fn outcome_succeeded(&self) -> Option<bool> {
        match self {
            Self::RoleRunCompleted { .. } | Self::OracleRunCompleted { .. } => Some(true),
            Self::RoleRunFailed { .. } | Self::OracleRunFailed { .. } => Some(false),
            Self::MissionCreated { .. }
            | Self::PlanSubmitted { .. }
            | Self::RoleRunRequested { .. }
            | Self::OracleRunRequested { .. }
            | Self::MissionAborted { .. }
            | Self::DecisionRecorded { .. }
            | Self::PlanAmended { .. }
            | Self::PlanProposed { .. } => None,
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
