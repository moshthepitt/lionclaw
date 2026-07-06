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
use super::plan::PlanSubmission;

pub const SCHEMA_VERSION: u32 = 1;

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

/// The honesty bar a plugin declares: what "finished" must mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopBar {
    Verified,
    Reviewed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionConfig {
    pub max_parallel: u32,
    pub ratification_gate: bool,
    pub stop: StopBar,
}

impl Default for MissionConfig {
    fn default() -> Self {
        Self {
            max_parallel: 4,
            ratification_gate: true,
            stop: StopBar::Verified,
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
        plugin_name: String,
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
    TerminalReviewRequested {
        attempt_no: u32,
        idempotency_key: String,
    },
    TerminalReviewCompleted {
        attempt_no: u32,
        idempotency_key: String,
        done: bool,
        report: PayloadRef,
    },
    MissionAborted {
        reason: String,
        actor: String,
    },
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
