//! `MissionState` — a pure fold over the event log. Deterministic containers
//! only (`BTreeMap`), derives `PartialEq` so the fold-litmus test can assert
//! rebuilt state equality. Wall-clock time never enters this type.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::event::{MissionConfig, PayloadRef};
use super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
use super::plan::PlanSubmission;
use super::verdict::{AuthoritativeVerdict, FinishClass};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum MissionPhase {
    /// Created, awaiting a plan submission.
    Planning,
    Running,
    /// Open attention items — parked at zero compute (durable interrupt).
    AttentionNeeded,
    Done { finish: FinishClass },
    Aborted { reason: String },
}

impl MissionPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Aborted { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Cleared,
    Failed,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRuntimeState {
    pub status: TaskStatus,
    pub attempts: u32,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    NodeFailed,
    NodeAttention,
    GateFailed,
    GateCheckpoint,
    TerminalReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttentionItem {
    /// Stable across refolds: embeds the triggering sequence number.
    pub id: String,
    pub kind: AttentionKind,
    pub task_id: Option<TaskId>,
    pub report: String,
}

/// A `…Requested` event without a recorded outcome. Drives reconcile on
/// resume and the derived effects-queue rebuild; keyed by idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InflightEffect {
    RoleRun {
        task_id: TaskId,
        attempt_no: u32,
        role: RoleName,
        prompt: PayloadRef,
        base_sha: String,
        requested_seq: u64,
    },
    OracleRun {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        requested_seq: u64,
    },
    TerminalReview {
        attempt_no: u32,
        requested_seq: u64,
    },
}

impl InflightEffect {
    /// Build the inflight entry for a `…Requested` event. Single source of
    /// truth shared by the fold and the effect-ledger enqueue.
    pub fn from_request(
        event: &super::event::MissionEvent,
        requested_seq: u64,
    ) -> Option<(String, Self)> {
        use super::event::MissionEvent;
        match event {
            MissionEvent::RoleRunRequested {
                task_id,
                attempt_no,
                idempotency_key,
                role,
                prompt,
                base_sha,
            } => Some((
                idempotency_key.clone(),
                Self::RoleRun {
                    task_id: task_id.clone(),
                    attempt_no: *attempt_no,
                    role: role.clone(),
                    prompt: prompt.clone(),
                    base_sha: base_sha.clone(),
                    requested_seq,
                },
            )),
            MissionEvent::OracleRunRequested {
                assertion_ids,
                oracle,
                judged_sha,
                attempt_no,
                idempotency_key,
            } => Some((
                idempotency_key.clone(),
                Self::OracleRun {
                    assertion_ids: assertion_ids.clone(),
                    oracle: oracle.clone(),
                    judged_sha: judged_sha.clone(),
                    attempt_no: *attempt_no,
                    requested_seq,
                },
            )),
            MissionEvent::TerminalReviewRequested {
                attempt_no,
                idempotency_key,
            } => Some((
                idempotency_key.clone(),
                Self::TerminalReview {
                    attempt_no: *attempt_no,
                    requested_seq,
                },
            )),
            _ => None,
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::RoleRun { .. } => "role_run",
            Self::OracleRun { .. } => "oracle_run",
            Self::TerminalReview { .. } => "terminal_review",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionState {
    pub mission_id: MissionId,
    pub objective: String,
    pub plugin_name: String,
    pub workspace_dir: String,
    /// Target repo HEAD at mission creation.
    pub base_sha: String,
    pub config: MissionConfig,
    pub phase: MissionPhase,
    pub plan: Option<PlanSubmission>,
    pub contract: BTreeMap<AssertionId, AssertionState>,
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
    /// Latest recorded artifact head (starts at `base_sha`). Oracle verdicts
    /// are fresh only when judged at this commit.
    pub current_sha: String,
    /// Per-oracle dispatch counter (attempt numbering).
    pub oracle_attempts: BTreeMap<OracleName, u32>,
    pub terminal_review_attempts: u32,
    /// Outcome of the latest terminal review, if any.
    pub terminal_review_done: Option<bool>,
    pub inflight: BTreeMap<String, InflightEffect>,
    pub open_attention: BTreeMap<String, AttentionItem>,
    /// Sequence number of the last folded event (optimistic-concurrency head).
    pub head: u64,
}
