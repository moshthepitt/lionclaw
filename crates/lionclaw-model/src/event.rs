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

use super::ids::{AssertionId, InputName, MissionId, OracleName, RoleInstanceId, TaskId};
use super::plan::{OutputSemantics, PlanProposal};
use super::verdict::FinishClass;
use crate::prelude::*;
use crate::{AppliedRuntimeConfiguration, RuntimeUsage, TypedFailure, TypedFailureEvidence};

/// Version 30 is the Slice 9.5 plan-contract closeout surface: role successes
/// carry prepared-input digests, missions can durably assign a digest-pinned
/// runtime environment after OCI preflight, and effect requests bind the
/// resolved environment digest they ran under.
pub const SCHEMA_VERSION: u32 = 30;

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
    GapReview,
}

/// The one renderer branch compatible with a durable role contract.
pub const fn role_prompt_template(output: OutputSemantics) -> RolePromptTemplate {
    match output {
        OutputSemantics::ProducesArtifact => RolePromptTemplate::Execution,
        OutputSemantics::ProducesReport | OutputSemantics::ProposesPlan => {
            RolePromptTemplate::Planning
        }
        OutputSemantics::EmitsVerdict => RolePromptTemplate::Judgment,
        OutputSemantics::EmitsGapVerdict => RolePromptTemplate::GapReview,
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
    Attested,
}

impl StopBar {
    /// The stable snake_case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Attested => "attested",
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
    pub oracles: BTreeSet<OracleName>,
    #[serde(default)]
    pub ceilings: super::AuthorityCeilings,
    #[serde(default, skip_serializing_if = "super::ConfinementResources::is_empty")]
    pub resource_ceilings: super::ConfinementResources,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub oracle_resources: BTreeMap<OracleName, super::ConfinementResources>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub oracle_devices: BTreeMap<OracleName, BTreeSet<String>>,
    #[serde(default)]
    pub requires_gap_review: bool,
    #[serde(default)]
    pub recovery: RecoveryConfig,
    #[serde(default)]
    pub execution: ExecutionPolicy,
}

impl Default for MissionConfig {
    fn default() -> Self {
        Self {
            stop: StopBar::Verified,
            oracles: BTreeSet::new(),
            ceilings: super::AuthorityCeilings::default(),
            resource_ceilings: super::ConfinementResources::default(),
            oracle_resources: BTreeMap::new(),
            oracle_devices: BTreeMap::new(),
            requires_gap_review: false,
            recovery: RecoveryConfig::default(),
            execution: ExecutionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub default_timeout_secs: u64,
    pub max_task_time_secs: u64,
    pub extension_step_secs: u64,
    #[serde(default = "default_effect_capacity")]
    pub effect_capacity: u32,
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
            effect_capacity: default_effect_capacity(),
            auto_continue_candidate: false,
            auto_continue_proof: false,
        }
    }
}

const fn default_effect_capacity() -> u32 {
    4
}

impl ExecutionPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.default_timeout_secs == 0 || self.extension_step_secs == 0 {
            return Err("execution durations must be greater than zero".into());
        }
        if self.effect_capacity == 0 || self.effect_capacity > 64 {
            return Err("effect-capacity must be between 1 and 64".into());
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
    /// The engine-owned gap review's contract-blind product verdict.
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
    /// The planning author's joint plan/team deliverable. It has **no verdict
    /// field** — a proposal is gradeless and can never mint authority; its
    /// accepted revisions become active only after approval.
    Plan {
        done: bool,
        report: PayloadRef,
        #[serde(default)]
        proposal: Option<Box<MissionProposal>>,
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

/// One typed product gap from a gap review. `severity` is the only
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

/// Exact candidate commits from a task's dependency lineages, copied onto a
/// writer request so replay can reject stale integration work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCandidateRef {
    pub task_id: TaskId,
    pub sha: String,
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
pub struct EnvironmentPreflight {
    pub engine: String,
    pub image_ref: String,
    pub image_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentAssignment {
    pub revision: u32,
    pub image_ref: String,
    pub image_id: String,
    pub preflight: EnvironmentPreflight,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_revision: Option<u32>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleTurnSuccess {
    /// `None` is a dialogue checkpoint only when the pinned output semantics
    /// makes its handoff optional; required-output absence is invalid output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Handoff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactOutcome>,
    pub final_response: PayloadRef,
    pub runtime_configuration: RuntimeConfigurationEvidence,
    #[serde(default)]
    pub runtime_usage: RuntimeUsage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_inputs: Vec<PreparedInputRef>,
}

/// Immutable authority carried from a role request into its completion.
/// The reducer accepts an outcome only when every field still matches the
/// active request; an effect id alone is not evidence of what was executed.
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
pub struct MissionProposal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<PlanProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<super::TeamRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionSkill {
    pub name: String,
    pub digest: String,
    pub description: String,
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
#[serde(deny_unknown_fields)]
pub struct DelegationSet {
    #[serde(default)]
    pub ratification: bool,
    #[serde(default)]
    pub proof_bar_weakening: bool,
    #[serde(default)]
    pub finish: bool,
    #[serde(default)]
    pub abort: bool,
    #[serde(default)]
    pub apply: bool,
}

impl DelegationSet {
    pub const fn none() -> Self {
        Self {
            ratification: false,
            proof_bar_weakening: false,
            finish: false,
            abort: false,
            apply: false,
        }
    }

    pub const fn fully_delegated() -> Self {
        Self {
            ratification: true,
            proof_bar_weakening: true,
            finish: true,
            abort: true,
            apply: true,
        }
    }
}

impl Default for DelegationSet {
    fn default() -> Self {
        Self::none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum MissionEvent {
    MissionCreated {
        objective: String,
        /// The mission type, pinned by content digest (verified on every open).
        mission_type: MissionTypeRef,
        /// The confinement image resolved to a content id at start, so a
        /// rebuilt tag can't silently change the instrument mid-mission.
        image_id: String,
        workspace_dir: String,
        /// HEAD of the target repo when the mission was created.
        base_sha: String,
        config: MissionConfig,
        delegation: DelegationSet,
    },
    ProposalRecorded {
        proposal: Box<MissionProposal>,
        /// sha256 of the canonical proposal JSON.
        proposal_hash: String,
    },
    TeamConfigured {
        team: super::TeamRevision,
    },
    SkillAdded {
        skill: MissionSkill,
    },
    EnvironmentAssigned {
        /// Digest-pinned user input, either `sha256:<hex>` or
        /// `<name>@sha256:<hex>`. Tags are deliberately not accepted.
        image_ref: String,
        /// OCI engine's immutable image identity from preflight.
        image_id: String,
        preflight: EnvironmentPreflight,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        team_revision: Option<u32>,
        reason: String,
    },
    RoleTurnRequested {
        role_instance: RoleInstanceId,
        team_revision: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<TaskId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        assertion_ids: Vec<AssertionId>,
        attempt_no: u32,
        effect_id: super::EffectId,
        /// Closed renderer branch and hash of the canonical transient turn.
        /// Turn prose is never durable request authority.
        prompt_template: RolePromptTemplate,
        prompt_hash: String,
        /// Commit the role's workspace is created at.
        base_sha: String,
        /// Resolved immutable environment digest at dispatch time.
        environment_digest: String,
        /// Candidate commits this task depends on, in plan-authored dependency
        /// order. Empty for root tasks and taskless turns.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dependency_refs: Vec<TaskCandidateRef>,
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
        deadline_ms: i64,
        budget_deadline_ms: i64,
    },
    /// Sender-free dialogue routed atomically to role instances as they
    /// existed at this exact log position.
    MessageSent {
        recipients: Vec<RoleInstanceId>,
        body: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        references: Vec<MessageReference>,
    },
    RoleTurnCompleted {
        effect_id: super::EffectId,
        outcome: Result<RoleTurnSuccess, TypedFailure>,
    },
    OracleRunRequested {
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        judged_sha: String,
        /// Resolved immutable environment digest at dispatch time.
        environment_digest: String,
        attempt_no: u32,
        effect_id: super::EffectId,
        requested_at_ms: i64,
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
    /// A durable control for one exact effect generation.
    ControlRequested {
        effect_id: super::EffectId,
        action: ControlAction,
        reason: String,
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
    MissionFinished {
        finish: FinishClass,
        reason: String,
    },
    ResultApplied {
        branch: String,
        sha: String,
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
    DeadlineReached {
        deadline_ms: i64,
    },
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
    /// Re-enter planning to propose a complete next plan/team revision.
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
            Self::ProposalRecorded { .. } => "proposal_recorded",
            Self::TeamConfigured { .. } => "team_configured",
            Self::SkillAdded { .. } => "skill_added",
            Self::EnvironmentAssigned { .. } => "environment_assigned",
            Self::RoleTurnRequested { .. } => "role_turn_requested",
            Self::MessageSent { .. } => "message_sent",
            Self::RoleTurnCompleted { .. } => "role_turn_completed",
            Self::OracleRunRequested { .. } => "oracle_run_requested",
            Self::OracleRunCompleted { .. } => "oracle_run_completed",
            Self::ControlRequested { .. } => "control_requested",
            Self::EffectCleanupFailed { .. } => "effect_cleanup_failed",
            Self::MissionAborted { .. } => "mission_aborted",
            Self::MissionFinished { .. } => "mission_finished",
            Self::ResultApplied { .. } => "result_applied",
            Self::DecisionRecorded { .. } => "decision_recorded",
        }
    }

    /// The effect ID and its class, for events participating in a
    /// request/outcome pair.
    pub fn effect_identity(&self) -> Option<(EffectEventClass, &str)> {
        match self {
            Self::RoleTurnRequested { effect_id, .. }
            | Self::OracleRunRequested { effect_id, .. } => {
                Some((EffectEventClass::Request, effect_id.as_str()))
            }
            Self::RoleTurnCompleted { effect_id, .. }
            | Self::OracleRunCompleted { effect_id, .. } => {
                Some((EffectEventClass::Outcome, effect_id.as_str()))
            }
            // Facts are not members of the request/outcome pair, even when
            // they identify the effect they describe. Exhaustive on purpose.
            Self::MissionCreated { .. }
            | Self::ProposalRecorded { .. }
            | Self::TeamConfigured { .. }
            | Self::SkillAdded { .. }
            | Self::EnvironmentAssigned { .. }
            | Self::MessageSent { .. }
            | Self::ControlRequested { .. }
            | Self::MissionAborted { .. }
            | Self::MissionFinished { .. }
            | Self::ResultApplied { .. }
            | Self::DecisionRecorded { .. }
            | Self::EffectCleanupFailed { .. } => None,
        }
    }

    /// The typed failure already carried by an outcome, if it has one.
    pub fn outcome_failure(&self) -> Option<&TypedFailure> {
        match self {
            Self::RoleTurnCompleted {
                outcome: Err(failure),
                ..
            }
            | Self::OracleRunCompleted {
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
            Self::RoleTurnCompleted { outcome, .. } => Some(match outcome {
                Ok(success) => TypedFailureEvidence {
                    final_response: inline_payload(&success.final_response),
                    configuration: success.runtime_configuration.clone(),
                    runtime_usage: success.runtime_usage.clone(),
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
