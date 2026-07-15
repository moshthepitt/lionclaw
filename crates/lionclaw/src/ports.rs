//! Port seams between the deterministic engine and the world. All types are
//! plain data; implementations live in `runner`/`oracle` (confined, real)
//! and `testing` (mocks). Zenith's `NodeDispatcher` protocol is the
//! ancestor of `RoleRunner` (Apache-2.0, Intelligent Internet).
//!
//! `RoleRunner` and `OracleRunner` are deliberately separate traits:
//! authoritative verdicts exist only on the oracle path, structurally.

use std::path::PathBuf;

use async_trait::async_trait;
use lionclaw_runtime_api::TypedFailure;

use crate::mission_type::{PreparedInput, RoleDefinition, SkillPackage};
use crate::model::{
    ArtifactOutcome, EffectId, EffectResource, Handoff, MissionId, OracleName, PreparedInputRef,
    RuntimeConfigurationEvidence, TaskId,
};

/// One full autonomous agent run — the engine never micromanages how a role
/// works. The engine guarantees an effect ID with a recorded outcome
/// is never re-invoked.
#[async_trait]
pub trait RoleRunner: Send + Sync {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure>;
}

#[derive(Debug, Clone)]
pub struct RoleRunRequest {
    pub mission_id: MissionId,
    pub task_id: TaskId,
    pub attempt_no: u32,
    pub effect_id: EffectId,
    pub role: RoleDefinition,
    /// Runtime profile resolved when the request event was recorded.
    pub runtime: String,
    /// Mission-owned skill packages resolved from the pinned mission type.
    pub skills: Vec<SkillPackage>,
    /// Fully assembled prompt (already persisted in the request event).
    pub prompt: String,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
    pub assignment_epoch: u32,
    pub recreate_workspace: bool,
    /// The target repository the mission operates on.
    pub workspace_dir: PathBuf,
    /// Mission state root (attempt dirs, worktrees) — `<workspace>/.lionclaw`.
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RoleRunOutcome {
    pub handoff: Handoff,
    /// Engine-observed commits (never agent-claimed); `Some` for every
    /// artifact-producing role, even one that committed nothing — then
    /// `head_sha == base_sha`. Readers/judges/planners are never captured, so
    /// this is `None` for them.
    pub artifact: Option<ArtifactOutcome>,
    pub runtime_configuration: RuntimeConfigurationEvidence,
    pub final_response: String,
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
    pub workspace_dir: PathBuf,
    pub state_dir: PathBuf,
    pub prepared_inputs: Vec<PreparedInput>,
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
