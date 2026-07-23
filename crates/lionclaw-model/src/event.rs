//! The mission event log vocabulary.
//!
//! An event is a **fact the fold cannot compute**: a recorded outcome, a
//! proposed plan, a human decision. Everything the engine can derive
//! (task/assertion status, gate results, attention, phase, finish class) is
//! fold-derived and never stored, so state/log divergence is unrepresentable.
//!
//! Non-deterministic or side-effecting steps are two events: `…Requested`
//! (intent + content-derived effect ID; consumed by the effect driver)
//! then `…Completed` with a typed success/failure result (outcome fact;
//! consumed by the fold). The log
//! stores outcomes, never executable intentions.
//!
//! Events are additive-only and version-stamped; never rewrite history.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, ConversationId, InputName, MissionId, OracleName, RoleName, TaskId};
use super::plan::{OutputSemantics, PlanInventory, PlanProposal, PlanningDag};
use crate::prelude::*;
use crate::{AppliedRuntimeConfiguration, TypedFailure, TypedFailureEvidence};

/// Version 23 records complete role-turn and accepted/rejected handoff
/// observations before effect cleanup. Unreleased older logs intentionally
/// fail loudly rather than invent role-attempt provenance.
pub const SCHEMA_VERSION: u32 = 23;

/// Maximum durable message body. Reference expansion is deliberately not
/// represented here: the shell resolves it transiently for a turn.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024;
pub const MAX_MESSAGE_RECIPIENTS: usize = 64;
pub const MAX_MESSAGE_REFERENCES: usize = 32;
/// Maximum narrative evidence retained from one role handoff.
pub const MAX_ROLE_REPORT_BYTES: usize = 256 * 1024;
/// Maximum retained message records in one conversation generation. At the
/// current body and expansion bounds this keeps queued dialogue below the same
/// aggregate scale as the existing 16-way upstream-context ceiling.
pub const MAX_QUEUED_MESSAGES_PER_CONVERSATION: usize = 16;
pub const MAX_FINAL_RESPONSE_BYTES: u64 = 64 * 1024;

/// Closed renderer identity for a durable role request.  The rendered turn is
/// deliberately shell-owned and reconstructed from the request's log prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolePromptTemplate {
    Execution,
    Planning,
    Judgment,
}

/// The one renderer branch compatible with a durable role contract.
pub const fn role_prompt_template(
    namespace: TaskNamespace,
    output: OutputSemantics,
) -> Option<RolePromptTemplate> {
    match (namespace, output) {
        (
            TaskNamespace::Planning,
            OutputSemantics::ProducesReport | OutputSemantics::ProposesPlan,
        ) => Some(RolePromptTemplate::Planning),
        (TaskNamespace::Execution, OutputSemantics::ProducesArtifact) => {
            Some(RolePromptTemplate::Execution)
        }
        (TaskNamespace::Execution, OutputSemantics::EmitsVerdict) => {
            Some(RolePromptTemplate::Judgment)
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageReference {
    AuthoritativeReceipt { effect_id: super::EffectId },
    ParkEvidence { effect_id: super::EffectId },
    ReachableCommit { sha: String },
}

/// Runtime-neutral reason that an ingress-authorized reference could not be
/// materialized at its immutable delivery boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReferenceCause {
    SourceMissing,
    SourceUnreadable,
    InvalidContent,
    ExpansionLimitExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationRecipient {
    pub conversation_id: ConversationId,
    pub role: RoleName,
    pub namespace: TaskNamespace,
    pub task_id: TaskId,
    pub assignment_epoch: u32,
}

impl ConversationRecipient {
    pub fn validate(&self, mission_id: &MissionId) -> bool {
        self.assignment_epoch > 0
            && self.conversation_id
                == ConversationId::for_role_instance(
                    mission_id,
                    self.namespace,
                    &self.task_id,
                    &self.role,
                    self.assignment_epoch,
                )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskNamespace {
    Planning,
    Execution,
}

/// Largest whole-second duration that has an exact positive `i64`
/// millisecond representation for an immutable effect request.
pub const MAX_EXECUTION_DURATION_SECS: u64 = i64::MAX as u64 / 1_000;

/// Resolve one immutable absolute deadline without truncation or saturation.
/// Callers validate stored duration shape separately, then use this exact
/// check at the epoch where an effect can actually be scheduled.
pub fn resolve_execution_deadline_ms(
    requested_at_ms: i64,
    duration_secs: u64,
) -> Result<i64, String> {
    let duration_ms = duration_secs
        .checked_mul(1_000)
        .ok_or_else(|| "execution duration overflows milliseconds".to_string())?;
    let duration_ms =
        i64::try_from(duration_ms).map_err(|_| "execution duration is too large".to_string())?;
    requested_at_ms
        .checked_add(duration_ms)
        .ok_or_else(|| "execution deadline overflows epoch milliseconds".to_string())
}

impl TaskNamespace {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Execution => "execution",
        }
    }
}

/// Reference to a content-addressed blob on durable-fs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub algo: String,
    pub hex: String,
    pub len: u64,
}

/// Payload data: inline for small values, blob reference above the
/// engine-owned externalization threshold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadRef {
    Inline { text: String },
    Blob(BlobRef),
}

/// The bounded handoff fact observed at the exact role-effect boundary.
///
/// Provenance is deliberately absent here: the fold derives it from the
/// matching inflight effect so event authors cannot choose a conversation,
/// role, task, generation, contract, or judged head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoleHandoffObservation {
    Accepted { report: PayloadRef },
    Rejected { failure: TypedFailure },
}

/// The complete runtime turn result observed by the host before handoff
/// parsing, artifact capture, or disposable effect cleanup.
///
/// Provenance is deliberately absent for the same reason as
/// `RoleHandoffObservation`: the fold binds this fact to the exact active role
/// effect. Recording failures as well as successes preserves useful response
/// and configuration evidence without treating either as task settlement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoleTurnObservation {
    Completed {
        final_response: PayloadRef,
        runtime_configuration: RuntimeConfigurationEvidence,
    },
    Failed {
        failure: TypedFailure,
    },
}

impl PayloadRef {
    pub fn inline(text: impl Into<String>) -> Self {
        Self::Inline { text: text.into() }
    }

    /// SHA-256 identity of the bytes the runner will resolve. Blob contents
    /// are verified against their reference by the durable blob store.
    pub fn content_sha256(&self) -> Option<String> {
        match self {
            Self::Inline { text } => {
                use sha2::{Digest, Sha256};

                Some(super::ids::lowercase_hex(&Sha256::digest(text.as_bytes())))
            }
            Self::Blob(blob)
                if blob.algo == "sha256"
                    && blob.hex.len() == 64
                    && blob
                        .hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
            {
                Some(blob.hex.clone())
            }
            Self::Blob(_) => None,
        }
    }

    pub const fn declared_len(&self) -> u64 {
        match self {
            Self::Inline { text } => text.len() as u64,
            Self::Blob(blob) => blob.len,
        }
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
    /// Resolved role outputs and oracle names available to every plan. This
    /// immutable copy makes complete plan validation replayable.
    pub plan_inventory: PlanInventory,
    /// The mission type's planning DAG (how an objective becomes a proposed
    /// contract). Empty ⇒ no in-engine planning; the mission awaits a manually
    /// proposed plan.
    #[serde(default)]
    pub planning: PlanningDag,
    #[serde(default)]
    pub recovery: RecoveryConfig,
    #[serde(default)]
    pub execution: ExecutionPolicy,
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
            plan_inventory: PlanInventory::default(),
            planning: PlanningDag::default(),
            recovery: RecoveryConfig::default(),
            execution: ExecutionPolicy::default(),
            terminal_review: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub default_timeout_secs: u64,
    pub max_task_time_secs: u64,
    pub extension_step_secs: u64,
    #[serde(default)]
    pub auto_continue_candidate: bool,
    #[serde(default)]
    pub auto_continue_proof: bool,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            default_timeout_secs: 30 * 60,
            max_task_time_secs: 30 * 60,
            extension_step_secs: 5 * 60,
            auto_continue_candidate: false,
            auto_continue_proof: false,
        }
    }
}

impl ExecutionPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.default_timeout_secs == 0 || self.extension_step_secs == 0 {
            return Err("execution durations must be greater than zero".into());
        }
        if self.max_task_time_secs < self.default_timeout_secs {
            return Err("max-task-time-secs must be at least default-timeout-secs".into());
        }
        if [
            self.default_timeout_secs,
            self.max_task_time_secs,
            self.extension_step_secs,
        ]
        .into_iter()
        .any(|duration| duration > MAX_EXECUTION_DURATION_SECS)
        {
            return Err(format!(
                "execution durations must not exceed {MAX_EXECUTION_DURATION_SECS} seconds"
            ));
        }
        Ok(())
    }

    pub fn validate_at(&self, requested_at_ms: i64) -> Result<(), String> {
        self.validate()?;
        resolve_execution_deadline_ms(requested_at_ms, self.max_task_time_secs).map(|_| ())
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
    pub prompt_hash: Option<String>,
}

pub type RuntimeConfigurationEvidence = AppliedRuntimeConfiguration;

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

impl Handoff {
    /// Whether this payload has the exact schema promised by a role effect.
    /// The predicate lives in the model so parsing, execution, and replay use
    /// one closed contract rather than independently matching handoff tags.
    pub const fn matches_output(&self, output: OutputSemantics) -> bool {
        matches!(
            (self, output),
            (Self::Validate { .. }, OutputSemantics::EmitsVerdict)
                | (Self::Review { .. }, OutputSemantics::EmitsGapVerdict)
                | (Self::Plan { .. }, OutputSemantics::ProposesPlan)
                | (
                    Self::Work { .. },
                    OutputSemantics::ProducesReport | OutputSemantics::ProducesArtifact
                )
        )
    }

    /// Narrative report carried by every role handoff variant.
    pub const fn report(&self) -> &PayloadRef {
        match self {
            Self::Work { report, .. }
            | Self::Validate { report, .. }
            | Self::Review { report, .. }
            | Self::Plan { report, .. } => report,
        }
    }
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

/// Validate the parts of a role success that are authoritative at the effect
/// boundary. Writers may legitimately report no artifact when the requested
/// work was already satisfied, but only writers may return one and its base
/// must be the immutable assignment base.
pub fn role_success_contract_error(
    output: OutputSemantics,
    handoff: &Handoff,
    artifact: Option<&ArtifactOutcome>,
    requested_base_sha: &str,
) -> Option<&'static str> {
    if !handoff.matches_output(output) {
        return Some("role handoff does not match the effect output contract");
    }
    match handoff {
        Handoff::Work { done: false, .. } => return Some("role reported done=false"),
        Handoff::Plan { done: false, .. } => return Some("planning author reported done=false"),
        _ => {}
    }
    let artifact = artifact?;
    if output != OutputSemantics::ProducesArtifact {
        return Some("only a produces-artifact role may return an artifact");
    }
    if artifact.base_sha != requested_base_sha {
        return Some("artifact base does not match the effect assignment base");
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedInputRef {
    pub name: InputName,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleRunSuccess {
    /// `None` is a dialogue checkpoint only when the pinned output semantics
    /// makes its handoff optional; required-output absence is invalid output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Handoff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactOutcome>,
    pub final_response: PayloadRef,
    pub runtime_configuration: RuntimeConfigurationEvidence,
}

/// Immutable authority carried from a role request into its completion.
/// The reducer accepts an outcome only when every field still matches the
/// active request; an effect id alone is not evidence of what was executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleRunRequestIdentity {
    pub conversation_id: ConversationId,
    pub namespace: TaskNamespace,
    pub task_id: TaskId,
    pub attempt_no: u32,
    pub assignment_epoch: u32,
    pub role: RoleName,
    pub output: OutputSemantics,
    pub runtime: String,
    pub prompt_template: RolePromptTemplate,
    pub prompt_hash: String,
    pub base_sha: String,
    pub workspace_preparation: WorkspacePreparation,
    pub message_boundary: u64,
    pub presented_messages: Vec<u64>,
}

impl RoleRunRequestIdentity {
    /// Validate the content-derived effect identity, canonical conversation,
    /// and immutable message boundary shared by fold and live dispatch.
    pub fn has_canonical_coordinates(
        &self,
        mission_id: &MissionId,
        effect_id: &super::EffectId,
        requested_seq: u64,
    ) -> bool {
        self.conversation_id
            == ConversationId::for_role_instance(
                mission_id,
                self.namespace,
                &self.task_id,
                &self.role,
                self.assignment_epoch,
            )
            && effect_id
                == &super::EffectId::for_role_request(
                    self.namespace,
                    mission_id,
                    &self.task_id,
                    self.attempt_no,
                    self.assignment_epoch,
                    &self.prompt_hash,
                )
            && requested_seq > 0
            && self.message_boundary == requested_seq.saturating_sub(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleRunSuccess {
    pub exit_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<i32>,
    pub stdout: PayloadRef,
    pub stderr: PayloadRef,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_inputs: Vec<PreparedInputRef>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalReviewSuccess {
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<Gap>,
    pub report: PayloadRef,
    pub final_response: PayloadRef,
    pub runtime_configuration: RuntimeConfigurationEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectResource {
    Container,
    RuntimeSecret,
    EffectDirectory,
    WriterRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
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
    },
    RoleRunRequested {
        /// Exact durable dialogue instance receiving this turn.
        conversation_id: ConversationId,
        namespace: TaskNamespace,
        task_id: TaskId,
        attempt_no: u32,
        effect_id: super::EffectId,
        role: RoleName,
        /// Closed output contract resolved from the pinned mission type.
        output: OutputSemantics,
        /// Effective runtime profile, resolved before the request is recorded.
        runtime: String,
        /// Closed renderer branch and hash of the canonical transient turn.
        /// Turn prose is never durable request authority.
        prompt_template: RolePromptTemplate,
        prompt_hash: String,
        /// Commit the role's workspace is created at.
        base_sha: String,
        /// Monotonic identity for a fresh task assignment. Retries and
        /// continues retain the epoch and workspace.
        assignment_epoch: u32,
        /// Immutable log boundary and exact message identities presented by
        /// this request. Messages appended later belong to the next turn.
        message_boundary: u64,
        presented_messages: Vec<u64>,
        /// Exact fold-derived handling for the retained conversation checkout.
        workspace_preparation: WorkspacePreparation,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        budget_deadline_ms: i64,
    },
    /// Sender-free dialogue routed atomically to role instances as they
    /// existed at this exact log position.
    MessageSent {
        recipients: Vec<ConversationRecipient>,
        body: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        references: Vec<MessageReference>,
    },
    /// Fail-closed settlement for one whole queued message whose referenced
    /// source disappeared after ingress validation and before dispatch.
    MessageReferenceUnavailable {
        conversation_id: ConversationId,
        assignment_epoch: u32,
        message_sequence: u64,
        reference: MessageReference,
        cause: UnavailableReferenceCause,
    },
    /// Kernel-observed confirmation that the conversation-owned writable
    /// checkout exists at the assignment base. Request intent never updates
    /// workspace provenance; only this post-materialization fact does.
    TaskWorkspacePrepared {
        task_id: TaskId,
        effect_id: super::EffectId,
        base_sha: String,
        assignment_epoch: u32,
    },
    /// Structured adapter evidence for the exact active effect. This is the
    /// one runtime journal fact promoted into mission authority so crash
    /// recovery can report configuration truth without trusting activity.json.
    EffectRuntimeConfigured {
        effect_id: super::EffectId,
        configuration: RuntimeConfigurationEvidence,
    },
    /// The role adapter's complete turn result was durably observed. This fact
    /// does not settle the role effect or grant handoff authority.
    RoleTurnObserved {
        effect_id: super::EffectId,
        observation: RoleTurnObservation,
    },
    /// A role handoff was accepted or rejected for an exact active effect.
    /// This fact preserves evidence before cleanup; it does not settle the run
    /// or grant task/verdict authority.
    RoleHandoffObserved {
        effect_id: super::EffectId,
        observation: RoleHandoffObservation,
    },
    RoleRunCompleted {
        effect_id: super::EffectId,
        request: Box<RoleRunRequestIdentity>,
        outcome: Result<RoleRunSuccess, TypedFailure>,
    },
    OracleRunRequested {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        effect_id: super::EffectId,
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
    },
    OracleRunCompleted {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        attempt_no: u32,
        effect_id: super::EffectId,
        outcome: Result<OracleRunSuccess, TypedFailure>,
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
        requested_at_ms: i64,
        not_before_ms: i64,
        deadline_ms: i64,
        budget_deadline_ms: i64,
    },
    /// The reviewer's verdict — advisory by construction: the fold stores it
    /// in `terminal_review`, never in any assertion's `last_authoritative`,
    /// and `classify_finish` never reads it. It gates closure only.
    TerminalReviewCompleted {
        attempt_no: u32,
        effect_id: super::EffectId,
        judged_sha: String,
        /// The reviewer's own summary bit. A blocking gap dominates it
        /// when the fold settles the receipt.
        outcome: Result<TerminalReviewSuccess, TypedFailure>,
    },
    /// A durable control for one exact effect generation.
    ControlRequested {
        effect_id: super::EffectId,
        action: ControlAction,
        reason: String,
    },
    /// The driver durably won the exact deadline race and may now cancel this
    /// generation. Later extensions are stale; an earlier extension changes
    /// the deadline and makes this fact inapplicable in the fold.
    EffectDeadlineReached {
        effect_id: super::EffectId,
        deadline_ms: i64,
    },
    /// Cleanup failed without settling the original request. The next driver
    /// retries the same exact resource operation before any new dispatch.
    EffectCleanupFailed {
        effect_id: super::EffectId,
        resource: EffectResource,
        failure: TypedFailure,
    },
    MissionAborted {
        reason: String,
    },
    /// A human/orchestrator decision resolving an open attention item (a
    /// durable interrupt). The fold applies the action and marks the item
    /// resolved. Ported from Zenith's `decide_attention` (Apache-2.0,
    /// Intelligent Internet, `controller.py`).
    DecisionRecorded {
        attention_id: String,
        action: DecisionAction,
        justification: String,
        /// Covered requirements explicitly changed by plan approval.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        requirement_changes: Vec<super::RequirementId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    Stop,
    ExtendDeadline {
        old_deadline_ms: i64,
        new_deadline_ms: i64,
        #[serde(default)]
        automatic: bool,
    },
    Continue {
        #[serde(default)]
        automatic: bool,
        mode: ContinueMode,
    },
}

/// The exact operator intent applied when reopening one parked effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinueMode {
    Preserve,
    RecreateWorkspace,
}

/// Fold-authoritative preparation for one artifact-producing role checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspacePreparation {
    Preserve,
    ResetForAssignment,
    ArchiveAndReset { parked_effect_id: super::EffectId },
}

impl WorkspacePreparation {
    pub const fn resets_workspace(&self) -> bool {
        !matches!(self, Self::Preserve)
    }

    pub const fn archived_effect(&self) -> Option<&super::EffectId> {
        match self {
            Self::ArchiveAndReset { parked_effect_id } => Some(parked_effect_id),
            Self::Preserve | Self::ResetForAssignment => None,
        }
    }
}

/// The actions a decision can take on an open attention item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
}

impl DecisionAction {
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Retry => "retry",
            Self::Repair => "repair",
            Self::Revise => "revise",
            Self::Accept => "accept",
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
            Self::MessageSent { .. } => "message_sent",
            Self::MessageReferenceUnavailable { .. } => "message_reference_unavailable",
            Self::TaskWorkspacePrepared { .. } => "task_workspace_prepared",
            Self::EffectRuntimeConfigured { .. } => "effect_runtime_configured",
            Self::RoleTurnObserved { .. } => "role_turn_observed",
            Self::RoleHandoffObserved { .. } => "role_handoff_observed",
            Self::RoleRunCompleted { .. } => "role_run_completed",
            Self::OracleRunRequested { .. } => "oracle_run_requested",
            Self::OracleRunCompleted { .. } => "oracle_run_completed",
            Self::TerminalReviewRequested { .. } => "terminal_review_requested",
            Self::TerminalReviewCompleted { .. } => "terminal_review_completed",
            Self::ControlRequested { .. } => "control_requested",
            Self::EffectDeadlineReached { .. } => "effect_deadline_reached",
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
            | Self::OracleRunCompleted { effect_id, .. }
            | Self::TerminalReviewCompleted { effect_id, .. } => {
                Some((EffectEventClass::Outcome, effect_id.as_str()))
            }
            // Facts are not members of the request/outcome pair, even when
            // they identify the effect they describe. Exhaustive on purpose.
            Self::MissionCreated { .. }
            | Self::PlanProposed { .. }
            | Self::MessageSent { .. }
            | Self::MessageReferenceUnavailable { .. }
            | Self::TaskWorkspacePrepared { .. }
            | Self::EffectRuntimeConfigured { .. }
            | Self::RoleTurnObserved { .. }
            | Self::RoleHandoffObserved { .. }
            | Self::ControlRequested { .. }
            | Self::EffectDeadlineReached { .. }
            | Self::MissionAborted { .. }
            | Self::DecisionRecorded { .. }
            | Self::EffectCleanupFailed { .. } => None,
        }
    }

    pub(crate) fn outcome_effect_id(&self) -> Option<&super::EffectId> {
        match self {
            Self::RoleRunCompleted { effect_id, .. }
            | Self::OracleRunCompleted { effect_id, .. }
            | Self::TerminalReviewCompleted { effect_id, .. } => Some(effect_id),
            _ => None,
        }
    }

    /// The typed failure already carried by an outcome, if it has one.
    pub fn outcome_failure(&self) -> Option<&TypedFailure> {
        match self {
            Self::RoleRunCompleted {
                outcome: Err(failure),
                ..
            }
            | Self::OracleRunCompleted {
                outcome: Err(failure),
                ..
            }
            | Self::TerminalReviewCompleted {
                outcome: Err(failure),
                ..
            } => Some(failure),
            _ => None,
        }
    }

    /// Canonical bounded evidence available at an effect outcome boundary.
    /// Blob payloads stay referenced by the success event; pure replay cannot
    /// resolve storage and therefore does not duplicate them into a failure.
    pub fn outcome_failure_evidence(&self) -> Option<TypedFailureEvidence> {
        match self {
            Self::RoleRunCompleted { outcome, .. } => Some(match outcome {
                Ok(success) => TypedFailureEvidence {
                    final_response: inline_payload(&success.final_response),
                    configuration: success.runtime_configuration.clone(),
                    ..Default::default()
                },
                Err(failure) => failure.evidence().clone(),
            }),
            Self::OracleRunCompleted { outcome, .. } => Some(match outcome {
                Ok(success) => TypedFailureEvidence {
                    exit_code: Some(success.exit_code),
                    stderr: inline_payload(&success.stderr),
                    ..Default::default()
                },
                Err(failure) => failure.evidence().clone(),
            }),
            Self::TerminalReviewCompleted { outcome, .. } => Some(match outcome {
                Ok(success) => TypedFailureEvidence {
                    final_response: inline_payload(&success.final_response),
                    configuration: success.runtime_configuration.clone(),
                    ..Default::default()
                },
                Err(failure) => failure.evidence().clone(),
            }),
            _ => None,
        }
    }
}

fn inline_payload(payload: &PayloadRef) -> String {
    match payload {
        PayloadRef::Inline { text } => text.clone(),
        PayloadRef::Blob(_) => String::new(),
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
        use crate::{
            Assertion, Plan, Requirement, RequirementDisposition, RequirementId, RequirementKind,
        };

        let event = MissionEvent::PlanProposed {
            proposal: PlanProposal {
                base_revision: 0,
                requirement_changes: vec![],
                assertion_supersessions: vec![],
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
        };

        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<MissionEvent>(&json).unwrap(), event);
    }

    fn empty_plan_proposal() -> PlanProposal {
        PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: crate::Plan {
                requirements: vec![],
                assertions: vec![],
                tasks: vec![],
            },
        }
    }

    #[test]
    fn plan_proposed_serializes_with_only_proposal_and_engine_hash() {
        let event = MissionEvent::PlanProposed {
            proposal: empty_plan_proposal(),
            plan_hash: "engine-hash".into(),
        };

        let json = serde_json::to_value(&event).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "type": "plan_proposed",
                "proposal": {
                    "base_revision": 0,
                    "plan": {
                        "requirements": [],
                        "assertions": [],
                        "tasks": []
                    }
                },
                "plan_hash": "engine-hash"
            })
        );
        assert_eq!(serde_json::from_value::<MissionEvent>(json).unwrap(), event);
    }

    #[test]
    fn mission_aborted_serializes_with_only_reason() {
        let event = MissionEvent::MissionAborted {
            reason: "not worth continuing".into(),
        };

        let json = serde_json::to_value(&event).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "type": "mission_aborted",
                "reason": "not worth continuing"
            })
        );
        assert_eq!(serde_json::from_value::<MissionEvent>(json).unwrap(), event);
    }

    #[test]
    fn role_effect_task_namespace_and_output_contract_are_required_on_the_wire() {
        let event = MissionEvent::RoleRunRequested {
            conversation_id: ConversationId::for_role_instance(
                &MissionId::from_digest_prefix("abcdef0123456789"),
                TaskNamespace::Planning,
                &TaskId::new("author").unwrap(),
                &RoleName::new("planner").unwrap(),
                1,
            ),
            namespace: TaskNamespace::Planning,
            task_id: TaskId::new("author").unwrap(),
            attempt_no: 1,
            effect_id: crate::EffectId::for_parts(&["test", "author"]),
            role: RoleName::new("planner").unwrap(),
            output: OutputSemantics::ProposesPlan,
            runtime: "codex".into(),
            prompt_template: RolePromptTemplate::Planning,
            prompt_hash: "0".repeat(64),
            base_sha: "base".into(),
            assignment_epoch: 1,
            message_boundary: 0,
            presented_messages: vec![],
            workspace_preparation: WorkspacePreparation::ResetForAssignment,
            requested_at_ms: 1,
            not_before_ms: 1,
            deadline_ms: 2,
            budget_deadline_ms: 3,
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["namespace"], "planning");
        assert_eq!(json["output"], "proposes-plan");
        assert_eq!(json["prompt_template"], "planning");
        assert!(json.get("prompt").is_none());
        assert!(!json.to_string().contains("assembled prompt"));
        assert_eq!(
            serde_json::from_value::<MissionEvent>(json.clone()).unwrap(),
            event
        );
        for field in ["namespace", "output"] {
            let mut missing = json.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<MissionEvent>(missing).is_err(),
                "missing {field} must fail closed"
            );
        }
    }

    #[test]
    fn role_observations_have_strict_effect_bound_wire_shapes() {
        let effect_id = crate::EffectId::for_parts(&["test", "observation"]);
        let turn = MissionEvent::RoleTurnObserved {
            effect_id: effect_id.clone(),
            observation: RoleTurnObservation::Completed {
                final_response: PayloadRef::inline("done"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            },
        };
        let handoff = MissionEvent::RoleHandoffObserved {
            effect_id: effect_id.clone(),
            observation: RoleHandoffObservation::Accepted {
                report: PayloadRef::inline("evidence"),
            },
        };

        for event in [turn, handoff] {
            let json = serde_json::to_value(&event).unwrap();
            assert_eq!(json["effect_id"], effect_id.as_str());
            assert!(
                json.get("conversation_id").is_none()
                    && json.get("task_id").is_none()
                    && json.get("role").is_none(),
                "observation provenance must come only from the matching request"
            );
            assert_eq!(serde_json::from_value::<MissionEvent>(json).unwrap(), event);
        }

        for raw in [
            serde_json::json!({
                "type": "role_turn_observed",
                "effect_id": effect_id,
                "observation": {
                    "outcome": "completed",
                    "final_response": {"kind": "inline", "text": "done"},
                    "runtime_configuration": {},
                    "conversation_id": "forged"
                }
            }),
            serde_json::json!({
                "type": "role_handoff_observed",
                "effect_id": crate::EffectId::for_parts(&["test", "observation"]),
                "observation": {
                    "outcome": "accepted",
                    "report": {"kind": "inline", "text": "evidence"},
                    "role": "forged"
                }
            }),
        ] {
            assert!(
                serde_json::from_value::<MissionEvent>(raw).is_err(),
                "observation payloads must reject caller-selected provenance"
            );
        }
    }

    #[test]
    fn execution_policy_rejects_the_first_unrepresentable_duration() {
        let invalid = ExecutionPolicy {
            max_task_time_secs: super::MAX_EXECUTION_DURATION_SECS + 1,
            ..ExecutionPolicy::default()
        };
        assert!(invalid.validate().is_err());

        let maximum = ExecutionPolicy {
            max_task_time_secs: super::MAX_EXECUTION_DURATION_SECS,
            ..ExecutionPolicy::default()
        };
        assert!(maximum.validate().is_ok());
    }

    #[test]
    fn every_decision_action_uses_the_same_strict_wire_shape() {
        let cases = [
            DecisionAction::Approve,
            DecisionAction::Revise,
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Accept,
        ];

        for action in cases {
            let event = MissionEvent::DecisionRecorded {
                attention_id: format!("attn-{}", action.slug()),
                action: action.clone(),
                justification: format!("because {}", action.slug()),
                requirement_changes: vec![],
            };

            let json = serde_json::to_value(&event).unwrap();

            assert_eq!(
                json,
                serde_json::json!({
                    "type": "decision_recorded",
                    "attention_id": format!("attn-{}", action.slug()),
                    "action": action.slug(),
                    "justification": format!("because {}", action.slug())
                })
            );
            assert_eq!(serde_json::from_value::<MissionEvent>(json).unwrap(), event);
        }
    }

    #[test]
    fn removed_event_fields_and_unknown_nested_fields_are_rejected() {
        let rejected = [
            r#"{"type":"plan_proposed","proposal":{"base_revision":0,"plan":{"requirements":[],"assertions":[],"tasks":[]}},"plan_hash":"hash","actor":"caller"}"#,
            r#"{"type":"plan_proposed","proposal":{"base_revision":0,"plan":{"requirements":[],"assertions":[],"tasks":[]}},"plan_hash":"hash","justification":"dead prose"}"#,
            r#"{"type":"plan_proposed","proposal":{"base_revision":0,"plan":{"requirements":[],"assertions":[],"tasks":[]},"unexpected":"nested"}},"plan_hash":"hash"}"#,
            r#"{"type":"decision_recorded","attention_id":"a","action":"approve","justification":"ok","actor":"caller"}"#,
            r#"{"type":"decision_recorded","attention_id":"a","action":"approve","justification":"ok","unexpected":"field"}"#,
            r#"{"type":"decision_recorded","attention_id":"a","action":{"action":"approve"},"justification":"ok"}"#,
            r#"{"type":"decision_recorded","attention_id":"a","action":"unknown","justification":"ok"}"#,
            r#"{"type":"mission_aborted","reason":"stop","actor":"caller"}"#,
            r#"{"type":"mission_aborted","reason":"stop","unexpected":"field"}"#,
        ];

        for raw in rejected {
            assert!(
                serde_json::from_str::<MissionEvent>(raw).is_err(),
                "unexpectedly accepted {raw}"
            );
        }
    }

    #[test]
    fn action_specific_decision_event_types_are_not_in_the_wire_vocabulary() {
        for event_type in [
            "plan_approved",
            "plan_revised",
            "retry_recorded",
            "repair_recorded",
            "accept_recorded",
            "abort_recorded",
        ] {
            let raw = serde_json::json!({
                "type": event_type,
                "attention_id": "attn",
                "justification": "because"
            });
            assert!(serde_json::from_value::<MissionEvent>(raw).is_err());
        }
    }
}
