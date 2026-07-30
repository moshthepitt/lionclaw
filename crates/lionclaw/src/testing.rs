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
    OracleOutcome, OracleRunRequest, OracleRunStatus, OracleRunner, RoleRunner, RoleTurnOutcome,
    RoleTurnRequest,
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

/// A gap-review verdict outcome that echoes the request prompt's nonce
/// (judges are never captured, so `artifact` is always `None`).
pub fn review_verdict(request: &RoleTurnRequest, passed: bool, gaps: Vec<Gap>) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: Some(Handoff::Review {
            done: true,
            report: PayloadRef::inline("requirement map + observations"),
            passed,
            gaps,
            nonce: crate::prompt::handoff_nonce(&request.prompt)
                .expect("gap-review prompt has a nonce")
                .to_string(),
        }),
        artifact: None,
        prepared_inputs: Vec::new(),
        runtime_configuration: crate::model::RuntimeConfigurationEvidence {
            requested_model: Some("mock-model".to_string()),
            applied_model: Some("mock-model".to_string()),
            ..Default::default()
        },
        runtime_usage: Default::default(),
        final_response: "requirement map + observations".to_string(),
    }
}

/// Materialize and capture a deterministic artifact through the exact
/// authority issued with a test role request.
pub async fn capture_test_artifact(
    request: &RoleTurnRequest,
    head_sha: &str,
) -> Result<CapturedArtifact, TypedFailure> {
    prepare_test_workspace(request).await?;
    capture_prepared_test_artifact(request, head_sha).await
}

/// Prepare a deterministic writer checkout.
pub async fn prepare_test_workspace(request: &RoleTurnRequest) -> Result<(), TypedFailure> {
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
    Ok(())
}

/// Capture a deterministic artifact after [`prepare_test_workspace`] has
/// established authority.
pub async fn capture_prepared_test_artifact(
    request: &RoleTurnRequest,
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
    Box<dyn Fn(&RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> + Send + Sync>;

pub struct MockRoleRunner {
    script: RoleScript,
    pub calls: Mutex<Vec<(Option<TaskId>, u32, String)>>,
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
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("did the work"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    head_sha.clone(),
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    requested_model: Some("mock-model".to_string()),
                    applied_model: Some("mock-model".to_string()),
                    ..Default::default()
                },
                runtime_usage: Default::default(),
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
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
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
        let mut outcome = (self.script)(&request)?.projected();
        if let Some(handoff) = &outcome.handoff {
            match crate::runner::validate_handoff(handoff, request.role.output) {
                Ok(()) => {}
                Err(mut failure) => {
                    failure.evidence_mut().final_response = outcome.final_response.clone();
                    failure.evidence_mut().configuration = outcome.runtime_configuration.clone();
                    let failure = failure.projected();
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

type OracleStatusScript =
    Box<dyn Fn(&OracleRunRequest) -> Result<OracleRunStatus, TypedFailure> + Send + Sync>;
type OracleOutcomeScript =
    Box<dyn Fn(&OracleRunRequest) -> Result<OracleOutcome, TypedFailure> + Send + Sync>;

pub struct MockOracleRunner {
    script: OracleStatusScript,
    pub calls: Mutex<Vec<(String, String)>>,
}

impl MockOracleRunner {
    pub fn new(script: OracleOutcomeScript) -> Self {
        Self::new_status(Box::new(move |request| {
            script(request).map(OracleRunStatus::Complete)
        }))
    }

    pub fn new_status(script: OracleStatusScript) -> Self {
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
    async fn run(&self, request: OracleRunRequest) -> Result<OracleRunStatus, TypedFailure> {
        self.calls
            .lock()
            .expect("lock")
            .push((request.oracle.to_string(), request.judged_sha.clone()));
        (self.script)(&request)
    }
}
