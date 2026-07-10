//! `MissionState` — a pure fold over the event log. Deterministic containers
//! only (`BTreeMap`), derives `PartialEq` so the fold-litmus test can assert
//! rebuilt state equality. Wall-clock time never enters this type.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::event::{Gap, GapSeverity, MissionConfig, MissionTypeRef, PayloadRef};
use super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
use super::plan::PlanSubmission;
use super::verdict::{AuthoritativeVerdict, FinishClass};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum MissionPhase {
    /// No execution plan yet: drives the in-engine planning DAG (research →
    /// red-team → author) toward a proposal, or — with an empty planning DAG —
    /// idles awaiting a manually submitted plan.
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
    /// Retired by an amendment (superseded or cancelled). A tombstone: the
    /// task is removed from the live `plan.tasks`, so no derivation dispatches
    /// or judges it; this row survives in `tasks` (with its `attempts`) for
    /// audit. Never transitions to any other status.
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRuntimeState {
    pub status: TaskStatus,
    pub attempts: u32,
    /// The latest handoff report, for threading into downstream prompts.
    #[serde(default)]
    pub last_report: Option<PayloadRef>,
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
    /// idempotency key ride on it, so a retry re-rolls under a fresh key.
    #[serde(default)]
    pub attempts: u32,
    /// The last attempt's result. A fresh verdict and a pending failure
    /// cannot coexist: a failure only follows a dispatch, and dispatch only
    /// happens without a fresh verdict (history lives in the event log).
    #[serde(default)]
    pub outcome: Option<ReviewOutcome>,
    /// The one human-acceptance fact ("accept closure despite the review").
    #[serde(default)]
    pub accepted: Option<ReviewAcceptance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReviewOutcome {
    Verdict(TerminalReviewVerdict),
    /// The reviewer failed to run or hand off a verdict (infrastructure),
    /// until a decision clears it. Prevents a broken reviewer from
    /// re-requesting forever (mirrors `oracle_failures`).
    Failed {
        detail: String,
    },
}

/// How a human accepted closure despite the review. `continue` on a gap park
/// acknowledges the verdict at its sha; `continue` on a failure park waives
/// the review outright. One enum, so waived-and-acknowledged is
/// unrepresentable and the receipt distinguishes the two by variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "acceptance", rename_all = "snake_case")]
pub enum ReviewAcceptance {
    /// Keyed to the verdict's sha: a later head move re-opens the review;
    /// the acknowledgment is never inherited.
    AcknowledgedGaps { judged_sha: String },
    /// Sticky (the failure is about the instrument, not the tree): the
    /// mission may close, but no verdict was ever recorded.
    Waived,
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
    /// The default-on ratification gate: approve the plan before work runs.
    Ratify,
    NodeFailed,
    NodeAttention,
    /// An oracle failed to *run* (infrastructure), distinct from a nonzero
    /// exit (which is a valid verdict).
    OracleFailed,
    GateFailed,
    GateCheckpoint,
    /// The in-engine author's proposal awaits a human's ratification before it
    /// seeds the contract.
    RatifyProposal,
    /// The terminal review's blocking verdict awaits a human (amend to
    /// remediate / retry to re-roll / continue to acknowledge-and-close /
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
            Self::Ratify => "ratify",
            Self::NodeFailed => "node_failed",
            Self::NodeAttention => "node_attention",
            Self::OracleFailed => "oracle_failed",
            Self::GateFailed => "gate_failed",
            Self::GateCheckpoint => "gate_checkpoint",
            Self::RatifyProposal => "ratify_proposal",
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
        role: RoleName,
        prompt: PayloadRef,
        judged_sha: String,
        /// Carried from the event so the runner's handoff-forgery check
        /// still has its expected token after a crash/resume.
        nonce: String,
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
                role,
                prompt,
                judged_sha,
                nonce,
            } => Some((
                idempotency_key.clone(),
                Self::TerminalReview {
                    attempt_no: *attempt_no,
                    role: role.clone(),
                    prompt: prompt.clone(),
                    judged_sha: judged_sha.clone(),
                    nonce: nonce.clone(),
                    requested_seq,
                },
            )),
            // Exhaustive on purpose: a new `…Requested` event must build its
            // inflight entry here, never silently skip the effect ledger.
            MissionEvent::MissionCreated { .. }
            | MissionEvent::PlanSubmitted { .. }
            | MissionEvent::RoleRunCompleted { .. }
            | MissionEvent::RoleRunFailed { .. }
            | MissionEvent::OracleRunCompleted { .. }
            | MissionEvent::OracleRunFailed { .. }
            | MissionEvent::TerminalReviewCompleted { .. }
            | MissionEvent::TerminalReviewFailed { .. }
            | MissionEvent::MissionAborted { .. }
            | MissionEvent::DecisionRecorded { .. }
            | MissionEvent::PlanAmended { .. } => None,
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
    pub plan: Option<PlanSubmission>,
    pub contract: BTreeMap<AssertionId, AssertionState>,
    pub tasks: BTreeMap<TaskId, TaskRuntimeState>,
    /// The contract-free planning phase: the runtime status of the mission
    /// type's planning DAG. Disjoint from `tasks` (execution) — planning and
    /// execution ids never coexist, since `plan` goes monotonically `None → Some`.
    pub planning: PlanningState,
    /// The author's proposed plan, awaiting ratification. Gradeless: it becomes
    /// `contract`/`tasks` only via `derive_promotion` once ratified. `None`
    /// before a proposal and after promotion. Every proposal is engine-authored
    /// and always requires a human `Ratify` (the manual path is `PlanSubmitted`,
    /// which seeds the contract directly and never populates this field).
    pub proposal: Option<PlanSubmission>,
    /// Latest recorded artifact head (starts at `base_sha`). Oracle verdicts
    /// are fresh only when judged at this commit.
    pub current_sha: String,
    /// Per-oracle dispatch counter (attempt numbering).
    pub oracle_attempts: BTreeMap<OracleName, u32>,
    pub inflight: BTreeMap<String, InflightEffect>,
    /// Derived each fold from failed nodes, gate results, and the
    /// ratification gate, minus anything a decision has resolved.
    pub open_attention: BTreeMap<String, AttentionItem>,
    /// The plan was ratified (the durable ratification gate was answered).
    /// Cleared on any accepted amendment when the gate is on, so approval of
    /// one plan revision never authorizes the next (ADR 0006).
    pub ratified: bool,
    /// Plan revision: the initial submission is 1, each accepted amendment the
    /// next. Used for the amendment staleness guard (`base_revision`) and
    /// status display; the initial `MissionCreated` state (no plan) is 0.
    pub revision: u32,
    /// Gate checkpoints the human confirmed (`continue`) — the mission
    /// proceeds past them without re-raising the checkpoint.
    pub acknowledged_gates: std::collections::BTreeSet<TaskId>,
    /// Nodes whose handoff asked for a human look (`request_attention`),
    /// until a decision clears them.
    pub flagged_nodes: std::collections::BTreeSet<TaskId>,
    /// Oracles that failed to *run* (infrastructure failure, distinct from a
    /// nonzero exit) → mapped to the failure detail, until a decision clears
    /// them. Prevents a broken oracle from re-requesting forever.
    pub oracle_failures: BTreeMap<OracleName, String>,
    /// Oracles whose obligation a human waived (`continue` on an oracle
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
            AttentionKind::Ratify,
            AttentionKind::NodeFailed,
            AttentionKind::NodeAttention,
            AttentionKind::OracleFailed,
            AttentionKind::GateFailed,
            AttentionKind::GateCheckpoint,
            AttentionKind::RatifyProposal,
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
