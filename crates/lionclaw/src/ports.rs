//! Port seams between the deterministic engine and the world. All types are
//! plain data; implementations live in `runner`/`oracle` (confined, real)
//! and `testing` (mocks). Zenith's `NodeDispatcher` protocol is the
//! ancestor of `RoleRunner` (Apache-2.0, Intelligent Internet).
//!
//! `RoleRunner` and `OracleRunner` are deliberately separate traits:
//! authoritative verdicts exist only on the oracle path, structurally.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use lionclaw_runtime_api::TypedFailure;
use tokio::sync::{mpsc, oneshot, watch};

use crate::mission_type::{PreparedInput, RoleDefinition, SkillPackage};
use crate::model::{
    EffectId, EffectResource, Handoff, MissionId, OracleName, PreparedInputRef,
    RoleHandoffObservation, RoleTurnObservation, RuntimeConfigurationEvidence, TaskId,
    TaskNamespace,
};
pub use crate::workspace::{ArtifactCapture, CapturedArtifact};

/// One full autonomous agent run — the engine never micromanages how a role
/// works. The engine guarantees an effect ID with a recorded outcome
/// is never re-invoked. A runner must close its adapter and stop mutating
/// retained role state before requesting an accepted handoff acknowledgement;
/// a successful acknowledgement is the post-turn quiescence boundary.
#[async_trait]
pub trait RoleRunner: Send + Sync {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure>;
}

#[derive(Debug, Clone)]
pub struct RoleRunRequest {
    pub mission_id: MissionId,
    pub namespace: TaskNamespace,
    pub task_id: TaskId,
    pub attempt_no: u32,
    pub effect_id: EffectId,
    pub role: RoleDefinition,
    /// Domain policy from the pinned mission type. Kernel-owned coordinates
    /// are added by the runner and cannot be declared here.
    pub environment: BTreeMap<String, String>,
    /// Runtime profile resolved when the request event was recorded.
    pub runtime: String,
    /// Mission-owned skill packages resolved from the pinned mission type.
    pub skills: Vec<SkillPackage>,
    /// Fully assembled prompt (already persisted in the request event).
    pub prompt: String,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
    pub assignment_epoch: u32,
    pub workspace_preparation: crate::model::WorkspacePreparation,
    pub deadline_ms: i64,
    pub control: watch::Receiver<ExecutionControl>,
    /// Lossless, low-volume facts that may affect durable mission evidence.
    pub updates: mpsc::Sender<RoleRunUpdate>,
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

impl RoleRunRequest {
    async fn confirm_update(
        &self,
        code: &'static str,
        subject: &'static str,
        make_update: impl FnOnce(oneshot::Sender<Result<(), String>>) -> RoleRunUpdate,
    ) -> Result<(), TypedFailure> {
        let (acknowledge, acknowledged) = oneshot::channel();
        self.updates
            .send(make_update(acknowledge))
            .await
            .map_err(|_| {
                TypedFailure::permanent(
                    code,
                    format!("kernel role update receiver closed before {subject}"),
                )
            })?;
        acknowledged
            .await
            .map_err(|_| {
                TypedFailure::permanent(
                    code,
                    format!("kernel closed before acknowledging {subject}"),
                )
            })?
            .map_err(|detail| TypedFailure::permanent(code, detail))
    }

    /// Block writer launch until the engine durably accepts and reloads the
    /// exact workspace preparation fact.
    pub async fn confirm_workspace_prepared(&self) -> Result<(), TypedFailure> {
        if self.role.output != crate::model::OutputSemantics::ProducesArtifact {
            return Err(TypedFailure::invalid(
                "workspace.preparation",
                "only an artifact-producing role may prepare a retained workspace",
            ));
        }
        self.confirm_update(
            "workspace.preparation",
            "workspace preparation",
            |acknowledge| RoleRunUpdate::WorkspacePrepared {
                base_sha: self.base_sha.clone(),
                assignment_epoch: self.assignment_epoch,
                acknowledge,
            },
        )
        .await
    }

    /// Block role completion until the engine durably records and reloads the
    /// accepted or rejected handoff for this exact effect.
    pub async fn confirm_handoff_observed(
        &self,
        observation: RoleHandoffObservation,
    ) -> Result<(), TypedFailure> {
        let (acknowledge, acknowledged) = oneshot::channel();
        self.updates
            .send(RoleRunUpdate::HandoffObserved {
                observation,
                acknowledge,
            })
            .await
            .map_err(|_| {
                TypedFailure::permanent(
                    "role.handoff_observation",
                    "kernel role update receiver closed before durable handoff observation",
                )
            })?;
        acknowledged.await.map_err(|_| {
            TypedFailure::permanent(
                "role.handoff_observation",
                "kernel closed before acknowledging durable handoff observation",
            )
        })?
    }

    /// Block further effect processing until the engine durably records and
    /// reloads the adapter's complete turn result for this exact effect.
    pub async fn confirm_turn_observed(
        &self,
        observation: RoleTurnObservation,
    ) -> Result<(), TypedFailure> {
        self.confirm_update(
            "role.turn_observation",
            "durable role turn observation",
            |acknowledge| RoleRunUpdate::TurnObserved {
                observation,
                acknowledge,
            },
        )
        .await
    }
}

#[derive(Debug)]
pub enum RoleRunUpdate {
    WorkspacePrepared {
        base_sha: String,
        assignment_epoch: u32,
        acknowledge: oneshot::Sender<Result<(), String>>,
    },
    HandoffObserved {
        observation: RoleHandoffObservation,
        acknowledge: oneshot::Sender<Result<(), TypedFailure>>,
    },
    TurnObserved {
        observation: RoleTurnObservation,
        acknowledge: oneshot::Sender<Result<(), String>>,
    },
    RuntimeConfigured {
        configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration,
        acknowledge: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone)]
pub struct RoleRunOutcome {
    /// An agent may finish a dialogue turn without declaring task completion.
    /// That is an ordinary checkpoint whose final response is presented to the
    /// lead; a present handoff remains subject to the role's closed contract.
    pub handoff: Option<Handoff>,
    /// Engine-observed commits (never agent-claimed). Writers return `Some`
    /// only when a clean committed head was captured; work that was already
    /// satisfied may legitimately return `None`. Read-only roles never return
    /// an artifact.
    pub artifact: Option<CapturedArtifact>,
    pub runtime_configuration: RuntimeConfigurationEvidence,
    pub final_response: String,
}

impl RoleRunOutcome {
    pub(crate) fn projected(mut self) -> Self {
        self.runtime_configuration = self.runtime_configuration.projected();
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
    pub workspace_dir: PathBuf,
    pub state_dir: PathBuf,
    pub prepared_inputs: Vec<PreparedInput>,
    /// Domain policy from the pinned mission type.
    pub environment: BTreeMap<String, String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mission_type::RoleDefinition;
    use crate::model::{OutputSemantics, RoleName, WorkspacePreparation};

    fn role_request(updates: mpsc::Sender<RoleRunUpdate>) -> RoleRunRequest {
        let (_, control) = watch::channel(ExecutionControl::RunUntil(i64::MAX));
        let (activity, _) = watch::channel(None);
        RoleRunRequest {
            mission_id: MissionId::for_creation("/workspace", "report-observation", 1),
            namespace: TaskNamespace::Execution,
            task_id: TaskId::new("review").unwrap(),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["report-observation"]),
            role: RoleDefinition {
                name: RoleName::new("reviewer").unwrap(),
                output: OutputSemantics::EmitsVerdict,
                runtime: None,
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: String::new(),
            },
            environment: BTreeMap::new(),
            runtime: "codex".into(),
            skills: Vec::new(),
            prompt: String::new(),
            base_sha: "0123456789abcdef".into(),
            assignment_epoch: 1,
            workspace_preparation: WorkspacePreparation::Preserve,
            deadline_ms: i64::MAX,
            control,
            updates,
            activity,
            workspace_dir: PathBuf::from("/workspace"),
            state_dir: PathBuf::from("/state"),
            artifact_capture: None,
        }
    }

    #[tokio::test]
    async fn report_observation_waits_for_durable_acknowledgement() {
        let (updates, mut receiver) = mpsc::channel(1);
        let observation = RoleHandoffObservation::Accepted {
            report: crate::model::PayloadRef::inline("authoritative review evidence"),
        };
        let confirmation = tokio::spawn({
            let request = role_request(updates);
            let observation = observation.clone();
            async move { request.confirm_handoff_observed(observation).await }
        });

        let update = receiver.recv().await.unwrap();
        let RoleRunUpdate::HandoffObserved {
            observation: observed,
            acknowledge,
        } = update
        else {
            panic!("expected a report observation")
        };
        assert_eq!(observed, observation);
        assert!(!confirmation.is_finished());

        acknowledge.send(Ok(())).unwrap();
        confirmation.await.unwrap().unwrap();
    }
}
