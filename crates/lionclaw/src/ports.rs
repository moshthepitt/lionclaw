//! Port seams between the deterministic engine and the world. All types are
//! plain data; implementations live in `runner`/`oracle` (confined, real)
//! and `testing` (mocks). Zenith's `NodeDispatcher` protocol is the
//! ancestor of `RoleRunner` (Apache-2.0, Intelligent Internet).
//!
//! `RoleRunner` and `OracleRunner` are deliberately separate traits:
//! authoritative verdicts exist only on the oracle path, structurally.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;

use async_trait::async_trait;
use lionclaw_runtime_api::TypedFailure;
use tokio::sync::watch;

use crate::mission_type::{PreparedInput, SkillPackage};
use crate::model::{
    ConfinementResources, EffectId, EffectResource, Handoff, MissionId, OracleName,
    PreparedInputRef, RoleInstance, RuntimeConfigurationEvidence, RuntimeUsage, TaskCandidateRef,
    TaskId,
};
pub use crate::workspace::{ArtifactCapture, CapturedArtifact};

/// One full autonomous agent run — the engine never micromanages how a role
/// works. The engine guarantees an effect ID with a recorded outcome
/// is never re-invoked. A runner must close its adapter and stop mutating
/// retained role state before requesting an accepted handoff acknowledgement;
/// a successful acknowledgement is the post-turn quiescence boundary.
#[async_trait]
pub trait RoleRunner: Send + Sync {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure>;
}

#[derive(Debug, Clone)]
pub struct RoleTurnRequest {
    pub mission_id: MissionId,
    pub task_id: Option<TaskId>,
    /// Exact assertion set assigned to this turn. Empty for non-judgment roles.
    pub assertion_ids: Vec<crate::model::AssertionId>,
    pub attempt_no: u32,
    pub effect_id: EffectId,
    pub role: RoleInstance,
    /// Domain policy from the pinned mission type. Kernel-owned coordinates
    /// are added by the runner and cannot be declared here.
    pub environment: BTreeMap<String, String>,
    /// Mission-owned skill packages resolved from the pinned mission type.
    pub skills: Vec<SkillPackage>,
    /// Prepared inputs explicitly granted by the pinned role contract.
    pub prepared_inputs: Vec<PreparedInput>,
    /// Mission resource ceilings applied to the role's resource overrides.
    pub resource_ceilings: ConfinementResources,
    /// Fully assembled prompt (already persisted in the request event).
    pub prompt: String,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
    /// Resolved immutable environment digest this effect is authorized under.
    pub environment_digest: String,
    /// Exact upstream task candidates this turn must incorporate.
    pub dependency_refs: Vec<TaskCandidateRef>,
    pub assignment_epoch: u32,
    pub workspace_preparation: crate::model::WorkspacePreparation,
    pub deadline_ms: i64,
    pub control: watch::Receiver<ExecutionControl>,
    /// Coalesced, non-authoritative runtime telemetry. Slow observers retain
    /// only the latest event and can never backpressure runtime execution.
    pub activity: watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    /// The target repository the mission operates on.
    pub workspace_dir: PathBuf,
    /// Mission state root for durable conversation and disposable effect resources.
    pub state_dir: PathBuf,
    /// Present only for artifact-producing roles and bound to this request's
    /// exact conversation checkout and durable capture ref.
    pub artifact_capture: Option<ArtifactCapture>,
}

#[derive(Debug, Clone)]
pub struct RoleTurnOutcome {
    /// An agent may finish a dialogue turn without declaring task completion.
    /// That is an ordinary checkpoint whose final response is presented to the
    /// lead; a present handoff remains subject to the role's closed contract.
    pub handoff: Option<Handoff>,
    /// Engine-observed commits (never agent-claimed). Writers return `Some`
    /// only when a clean committed head was captured; work that was already
    /// satisfied may legitimately return `None`. Read-only roles never return
    /// an artifact.
    pub artifact: Option<CapturedArtifact>,
    /// Prepared inputs actually published and mounted for this role turn.
    pub prepared_inputs: Vec<PreparedInputRef>,
    pub runtime_configuration: RuntimeConfigurationEvidence,
    pub runtime_usage: RuntimeUsage,
    pub final_response: String,
}

impl RoleTurnOutcome {
    pub(crate) fn projected(mut self) -> Self {
        self.runtime_configuration = self.runtime_configuration.projected();
        self.runtime_usage = self.runtime_usage.projected();
        self.final_response = lionclaw_runtime_api::bounded_text(&self.final_response);
        self
    }
}

/// An engine-run, worker-independent, reproducible check. Exit 0 = pass.
#[async_trait]
pub trait OracleRunner: Send + Sync {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure>;
}

#[derive(Debug, Clone)]
pub struct OracleRunRequest {
    pub mission_id: MissionId,
    pub effect_id: EffectId,
    pub oracle: OracleName,
    /// Resolved oracle executable (engine resolves from the mission type; the
    /// runner stays domain-blind — it never sees which assertions it judges).
    pub oracle_path: PathBuf,
    pub judged_sha: String,
    /// Resolved immutable environment digest this effect is authorized under.
    pub environment_digest: String,
    pub workspace_dir: PathBuf,
    pub state_dir: PathBuf,
    pub prepared_inputs: Vec<PreparedInput>,
    /// Domain policy from the pinned mission type.
    pub environment: BTreeMap<String, String>,
    pub devices: BTreeSet<String>,
    /// Per-oracle resource overrides declared by the pinned mission type.
    pub resources: ConfinementResources,
    pub resource_ceilings: ConfinementResources,
    pub deadline_ms: i64,
    pub control: watch::Receiver<ExecutionControl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionControl {
    RunUntil(i64),
    DeadlineExhausted,
    Stop(String),
    Abort(String),
}

#[derive(Debug, Clone)]
pub struct EffectCleanupRequest {
    pub mission_id: MissionId,
    pub effect_id: EffectId,
    pub workspace_dir: PathBuf,
    pub state_dir: PathBuf,
    pub discard_artifact: bool,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("failed to clean up {resource:?}: {detail}")]
pub struct EffectCleanupFailure {
    pub resource: EffectResource,
    pub detail: String,
}

#[async_trait]
pub trait EffectCleaner: Send + Sync {
    /// Stop the exact effect process while preserving every resource from
    /// which durable outcome evidence may still need to be recovered.
    async fn quiesce(&self, request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure>;

    /// Remove disposable effect resources after evidence recovery is durable.
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure>;
}

#[derive(Debug, Clone)]
pub struct OracleOutcome {
    pub exit_code: i32,
    pub exit_signal: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub prepared_inputs: Vec<PreparedInputRef>,
    pub duration_ms: u64,
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

/// A synchronous, non-blocking observer of committed events. The store fires it
/// *after* `tx.commit()`, so a sink never sees a phantom event from a rolled-back
/// append (a `Conflict`/`Duplicate`). The contract is "return fast, don't block":
/// the CLI sink prints one line to stderr; a future daemon sink pushes into an
/// async channel and returns immediately (so a long-running driver stays live).
///
/// Deliberately not a `broadcast::Sender`: `advance` is one sequential loop with
/// one in-process consumer, so pub/sub fan-out (lag handling, subscribe/select)
/// would be dead weight.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: &crate::model::EventEnvelope);
}

/// The crate's single wall-clock call site; everything else takes time
/// through this port.
pub struct SystemClock;

impl Clock for SystemClock {
    #[expect(clippy::disallowed_methods)]
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}
