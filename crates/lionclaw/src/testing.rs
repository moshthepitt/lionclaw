//! Mock port implementations for the deterministic core's tests
//! (`feature = "testing"`). Scripted responders with call logs and per-key
//! invocation counters — the resume tests assert an effect ID is never
//! executed twice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use lionclaw_runtime_api::TypedFailure;

use crate::model::{Gap, Handoff, PayloadRef, TaskId};
use crate::ports::{
    CapturedArtifact, Clock, EffectCleaner, EffectCleanupFailure, EffectCleanupRequest,
    OracleOutcome, OracleRunRequest, OracleRunner, RoleRunOutcome, RoleRunRequest, RoleRunner,
};

#[derive(Default)]
pub struct NoopEffectCleaner;

#[async_trait]
impl EffectCleaner for NoopEffectCleaner {
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

    async fn cleanup(&self, _request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }
}

/// A terminal-review verdict outcome that echoes the request prompt's nonce
/// (judges are never captured, so `artifact` is always `None`).
pub fn review_verdict(request: &RoleRunRequest, passed: bool, gaps: Vec<Gap>) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Some(Handoff::Review {
            done: true,
            report: PayloadRef::inline("requirement map + observations"),
            passed,
            gaps,
            nonce: crate::prompt::handoff_nonce(&request.prompt)
                .expect("terminal-review prompt has a nonce")
                .to_string(),
        }),
        artifact: None,
        runtime_configuration: crate::model::RuntimeConfigurationEvidence {
            requested_model: Some("mock-model".to_string()),
            applied_model: Some("mock-model".to_string()),
            ..Default::default()
        },
        final_response: "requirement map + observations".to_string(),
    }
}

/// Materialize and capture a deterministic artifact through the exact
/// authority issued with a test role request.
pub async fn capture_test_artifact(
    request: &RoleRunRequest,
    head_sha: &str,
) -> Result<CapturedArtifact, TypedFailure> {
    prepare_test_workspace(request).await?;
    capture_prepared_test_artifact(request, head_sha).await
}

/// Prepare a deterministic writer checkout and cross the same durable
/// authority barrier as a production runner.
pub async fn prepare_test_workspace(request: &RoleRunRequest) -> Result<(), TypedFailure> {
    let capture = request.artifact_capture.as_ref().ok_or_else(|| {
        TypedFailure::permanent(
            "testing.capture_authority",
            "artifact-producing test request has no capture authority",
        )
    })?;
    capture
        .prepare_for_testing(&request.workspace_preparation)
        .await
        .map_err(|error| TypedFailure::permanent("testing.workspace", error.to_string()))?;
    request.confirm_workspace_prepared().await
}

/// Capture a deterministic artifact after [`prepare_test_workspace`] has
/// established authority.
pub async fn capture_prepared_test_artifact(
    request: &RoleRunRequest,
    head_sha: &str,
) -> Result<CapturedArtifact, TypedFailure> {
    let capture = request.artifact_capture.as_ref().ok_or_else(|| {
        TypedFailure::permanent(
            "testing.capture_authority",
            "artifact-producing test request has no capture authority",
        )
    })?;
    if head_sha == request.base_sha {
        capture.capture().await
    } else {
        capture.capture_test_commit(head_sha).await
    }
    .map_err(|error| TypedFailure::permanent("testing.capture", error.to_string()))
}

/// Deterministic monotonic clock — proves nothing depends on real time.
#[derive(Default)]
pub struct MockClock {
    now: AtomicI64,
}

impl Clock for MockClock {
    fn now_ms(&self) -> i64 {
        self.now.fetch_add(1, Ordering::SeqCst) + 1_000_000
    }
}

type RoleScript =
    Box<dyn Fn(&RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> + Send + Sync>;

pub struct MockRoleRunner {
    script: RoleScript,
    pub calls: Mutex<Vec<(TaskId, u32, String)>>,
    pub invocations_by_key: Mutex<BTreeMap<String, u32>>,
}

impl MockRoleRunner {
    pub fn new(script: RoleScript) -> Self {
        Self {
            script,
            calls: Mutex::new(Vec::new()),
            invocations_by_key: Mutex::new(BTreeMap::new()),
        }
    }

    /// A worker that reports done and "commits" a deterministic new sha.
    pub fn happy(head_sha: &str) -> Self {
        let head_sha = head_sha.to_string();
        Self::new(Box::new(move |request| {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("did the work"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    head_sha.clone(),
                )),
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    requested_model: Some("mock-model".to_string()),
                    applied_model: Some("mock-model".to_string()),
                    ..Default::default()
                },
                final_response: "did the work".to_string(),
            })
        }))
    }

    pub fn max_invocations_per_key(&self) -> u32 {
        self.invocations_by_key
            .lock()
            .expect("lock")
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }
}

#[async_trait]
impl RoleRunner for MockRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        self.calls.lock().expect("lock").push((
            request.task_id.clone(),
            request.attempt_no,
            request.effect_id.to_string(),
        ));
        *self
            .invocations_by_key
            .lock()
            .expect("lock")
            .entry(request.effect_id.to_string())
            .or_insert(0) += 1;
        if request.role.output == crate::model::OutputSemantics::ProducesArtifact
            && request.artifact_capture.is_some()
        {
            prepare_test_workspace(&request).await?;
        }
        let mut outcome = match (self.script)(&request) {
            Ok(outcome) => {
                let outcome = outcome.projected();
                request
                    .confirm_turn_observed(crate::model::RoleTurnObservation::Completed {
                        final_response: crate::model::PayloadRef::inline(
                            outcome.final_response.clone(),
                        ),
                        runtime_configuration: outcome.runtime_configuration.clone(),
                    })
                    .await?;
                outcome
            }
            Err(failure) => {
                let failure = failure.projected();
                request
                    .confirm_turn_observed(crate::model::RoleTurnObservation::Failed {
                        failure: failure.clone(),
                    })
                    .await?;
                return Err(failure);
            }
        };
        if let Some(handoff) = &outcome.handoff {
            match crate::runner::validate_handoff(handoff, request.role.output) {
                Ok(()) => {
                    request
                        .confirm_handoff_observed(crate::model::RoleHandoffObservation::Accepted {
                            report: handoff.report().clone(),
                        })
                        .await?;
                }
                Err(mut failure) => {
                    failure.evidence_mut().final_response = outcome.final_response.clone();
                    failure.evidence_mut().configuration = outcome.runtime_configuration.clone();
                    let failure = failure.projected();
                    request
                        .confirm_handoff_observed(crate::model::RoleHandoffObservation::Rejected {
                            failure: failure.clone(),
                        })
                        .await?;
                    return Err(failure);
                }
            }
        }
        if let Some(test_request) = outcome
            .artifact
            .as_ref()
            .and_then(CapturedArtifact::test_request)
            .cloned()
        {
            if test_request.base_sha != request.base_sha {
                return Err(TypedFailure::invalid(
                    "testing.artifact_base",
                    "test artifact request names a different assignment base",
                ));
            }
            if request.artifact_capture.is_some() {
                outcome.artifact =
                    Some(capture_prepared_test_artifact(&request, &test_request.head_sha).await?);
            }
        }
        Ok(outcome)
    }
}

type OracleScript =
    Box<dyn Fn(&OracleRunRequest) -> Result<OracleOutcome, TypedFailure> + Send + Sync>;

pub struct MockOracleRunner {
    script: OracleScript,
    pub calls: Mutex<Vec<(String, String)>>,
}

impl MockOracleRunner {
    pub fn new(script: OracleScript) -> Self {
        Self {
            script,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// An oracle with a fixed exit code.
    pub fn exiting(exit_code: i32) -> Self {
        Self::new(Box::new(move |_| {
            Ok(OracleOutcome {
                exit_code,
                exit_signal: None,
                stdout: format!("oracle exit {exit_code}").into_bytes(),
                stderr: Vec::new(),
                prepared_inputs: Vec::new(),
                duration_ms: 42,
            })
        }))
    }
}

#[async_trait]
impl OracleRunner for MockOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        self.calls
            .lock()
            .expect("lock")
            .push((request.oracle.to_string(), request.judged_sha.clone()));
        (self.script)(&request)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

    use tokio::sync::{mpsc, watch};

    use super::*;
    use crate::mission_type::RoleDefinition;
    use crate::model::{
        EffectId, MissionId, OutputSemantics, RoleHandoffObservation, RoleName, TaskNamespace,
        WorkspacePreparation,
    };
    use crate::ports::{ExecutionControl, RoleRunUpdate};

    fn role_request(
        updates: mpsc::Sender<RoleRunUpdate>,
        output: OutputSemantics,
    ) -> RoleRunRequest {
        let (_, control) = watch::channel(ExecutionControl::RunUntil(i64::MAX));
        let (activity, _) = watch::channel(None);
        RoleRunRequest {
            mission_id: MissionId::for_creation("/workspace", "mock-handoff-boundary", 1),
            namespace: TaskNamespace::Execution,
            task_id: TaskId::new("review").unwrap(),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["mock-handoff-boundary"]),
            role: RoleDefinition {
                name: RoleName::new("reviewer").unwrap(),
                output,
                runtime: None,
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: String::new(),
            },
            environment: BTreeMap::new(),
            runtime: "mock".into(),
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
    async fn mock_runner_acknowledges_only_valid_exact_handoffs() {
        let accepted = Handoff::Validate {
            done: true,
            report: PayloadRef::inline("validated"),
            items: Vec::new(),
            passed: true,
            request_attention: false,
        };
        let runner = Arc::new(MockRoleRunner::new(Box::new({
            let accepted = accepted.clone();
            move |_| {
                Ok(RoleRunOutcome {
                    handoff: Some(accepted.clone()),
                    artifact: None,
                    runtime_configuration: Default::default(),
                    final_response: "validated".into(),
                })
            }
        })));
        let (updates, mut receiver) = mpsc::channel(1);
        let run = tokio::spawn({
            let runner = runner.clone();
            async move {
                runner
                    .run(role_request(updates, OutputSemantics::EmitsVerdict))
                    .await
            }
        });

        let RoleRunUpdate::TurnObserved {
            observation,
            acknowledge,
        } = receiver.recv().await.unwrap()
        else {
            panic!("expected turn observation")
        };
        assert_eq!(
            observation,
            crate::model::RoleTurnObservation::Completed {
                final_response: PayloadRef::inline("validated"),
                runtime_configuration: Default::default(),
            }
        );
        assert!(!run.is_finished());
        acknowledge.send(Ok(())).unwrap();

        let RoleRunUpdate::HandoffObserved {
            observation,
            acknowledge,
        } = receiver.recv().await.unwrap()
        else {
            panic!("expected handoff observation")
        };
        assert_eq!(
            observation,
            RoleHandoffObservation::Accepted {
                report: accepted.report().clone()
            }
        );
        assert!(!run.is_finished());
        acknowledge.send(Ok(())).unwrap();
        assert_eq!(run.await.unwrap().unwrap().handoff, Some(accepted));
    }

    #[tokio::test]
    async fn mock_runner_durably_rejects_invalid_handoff_with_exact_outcome_evidence() {
        let runner = Arc::new(MockRoleRunner::new(Box::new(|_| {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrong contract"),
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    applied_model: Some("mock-model".into()),
                    ..Default::default()
                },
                final_response: "wrong contract response".into(),
            })
        })));
        let (updates, mut receiver) = mpsc::channel(1);
        let run = tokio::spawn({
            let runner = runner.clone();
            async move {
                runner
                    .run(role_request(updates, OutputSemantics::EmitsVerdict))
                    .await
            }
        });

        let RoleRunUpdate::TurnObserved {
            observation,
            acknowledge,
        } = receiver.recv().await.unwrap()
        else {
            panic!("expected turn observation")
        };
        assert_eq!(
            observation,
            crate::model::RoleTurnObservation::Completed {
                final_response: PayloadRef::inline("wrong contract response"),
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    applied_model: Some("mock-model".into()),
                    ..Default::default()
                },
            }
        );
        assert!(!run.is_finished());
        acknowledge.send(Ok(())).unwrap();

        let RoleRunUpdate::HandoffObserved {
            observation,
            acknowledge,
        } = receiver.recv().await.unwrap()
        else {
            panic!("expected handoff observation")
        };
        let RoleHandoffObservation::Rejected { failure } = observation else {
            panic!("invalid handoff must not be accepted")
        };
        assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
        assert_eq!(failure.evidence().final_response, "wrong contract response");
        assert_eq!(
            failure.evidence().configuration.applied_model.as_deref(),
            Some("mock-model")
        );
        assert!(!run.is_finished());
        acknowledge.send(Ok(())).unwrap();
        assert_eq!(run.await.unwrap().unwrap_err(), failure);
    }
}
