//! Port seams between the deterministic engine and the world. All types are
//! plain data; implementations live in `runner`/`oracle` (confined, real)
//! and `testing` (mocks). Zenith's `NodeDispatcher` protocol is the
//! ancestor of `RoleRunner` (Apache-2.0, Intelligent Internet).
//!
//! `RoleRunner` and `OracleRunner` are deliberately separate traits:
//! authoritative verdicts exist only on the oracle path, structurally.

use std::path::PathBuf;

use async_trait::async_trait;

use crate::mission_type::RoleDefinition;
use crate::model::{
    ArtifactOutcome, AssertionId, Handoff, MissionId, OracleName, RunErrorKind, TaskId,
};

/// One full autonomous agent run — the engine never micromanages how a role
/// works. The engine guarantees an idempotency key with a recorded outcome
/// is never re-invoked.
#[async_trait]
pub trait RoleRunner: Send + Sync {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure>;
}

#[derive(Debug, Clone)]
pub struct RoleRunRequest {
    pub mission_id: MissionId,
    pub task_id: TaskId,
    pub attempt_no: u32,
    pub idempotency_key: String,
    pub role: RoleDefinition,
    /// Fully assembled prompt (already persisted in the request event).
    pub prompt: String,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
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
    /// `head_sha == base_sha`. Readers/judges/planners have no clone, so `None`.
    pub artifact: Option<ArtifactOutcome>,
    pub model_id: Option<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("role run failed ({kind:?}): {detail}")]
pub struct RoleRunFailure {
    pub kind: RunErrorKind,
    pub detail: String,
}

/// An engine-run, worker-independent, reproducible check. Exit 0 = pass.
#[async_trait]
pub trait OracleRunner: Send + Sync {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, OracleFailure>;
}

#[derive(Debug, Clone)]
pub struct OracleRunRequest {
    pub mission_id: MissionId,
    pub oracle: OracleName,
    /// Resolved oracle executable (engine resolves from the mission type; the
    /// runner stays domain-blind).
    pub oracle_path: PathBuf,
    pub assertion_ids: Vec<AssertionId>,
    pub judged_sha: String,
    pub workspace_dir: PathBuf,
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct OracleOutcome {
    pub exit_code: i32,
    pub exit_signal: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration_ms: u64,
}

/// Infrastructure failure — distinct from a nonzero exit (which is a valid,
/// recorded verdict).
#[derive(Debug, Clone, thiserror::Error)]
#[error("oracle failed to run: {detail}")]
pub struct OracleFailure {
    pub detail: String,
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
