//! The engine-run oracle: a worker-independent, reproducible check the
//! engine executes itself. An agent never self-reports an authoritative
//! pass — this path is the only source of `OracleRunCompleted`, and it runs
//! against a complete Git checkout of the judged commit mounted read-only.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{RuntimeProgramExecutor, TypedFailure, TypedFailureEvidence};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::authority::{
    compile_role_plan, oracle_authority_with_network, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::ports::{
    ExecutionControl, ExternalOracleDriver, ExternalOracleDriverContext,
    ExternalOracleDriverRegistry, ExternalOraclePoll, ExternalOraclePollRequest,
    ExternalOracleProof, ExternalOracleSubmission, ExternalOracleSubmitRequest, OracleOutcome,
    OracleRunRequest, OracleRunStatus, OracleRunner,
};
use crate::resources::MissionDirs;
use crate::runner::{
    await_controlled, prepare_inputs, MissionProgramExecutor, PreparedInputs, SCRATCH_MOUNT_TARGET,
};
use crate::workspace;

pub struct OciOracleRunner {
    profile: MissionRuntimeProfile,
    external_drivers: ExternalOracleDriverRegistry,
    repo_lock: Arc<Mutex<()>>,
    input_lock: Arc<Mutex<()>>,
}

impl OciOracleRunner {
    pub fn new(profile: MissionRuntimeProfile) -> Self {
        let external_drivers = profile.external_oracle_drivers.iter().fold(
            ExternalOracleDriverRegistry::new(),
            |registry, (id, driver)| {
                registry.with_driver(
                    id.clone(),
                    Arc::new(ProfileExternalOracleDriver {
                        profile: profile.clone(),
                        driver: driver.clone(),
                    }),
                )
            },
        );
        Self {
            profile,
            external_drivers,
            repo_lock: Arc::new(Mutex::new(())),
            input_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_external_drivers(mut self, external_drivers: ExternalOracleDriverRegistry) -> Self {
        self.external_drivers = external_drivers;
        self
    }

    fn profile_for(&self, environment_digest: &str) -> MissionRuntimeProfile {
        let mut profile = self.profile.clone();
        profile.confinement.oci_mut().image = Some(environment_digest.to_string());
        profile
    }
}

const EXTERNAL_ORACLE_DRIVER_EXECUTABLE: &str = "lionclaw-external-oracle-driver";

#[derive(Clone)]
struct ProfileExternalOracleDriver {
    profile: MissionRuntimeProfile,
    driver: crate::config::ExternalOracleDriverProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalOracleState {
    driver: crate::model::ExternalOracleDriverId,
    oracle: crate::model::OracleName,
    spec_digest: String,
    request_digest: String,
    idempotency_key: String,
    artifact_digest: String,
    job_id: String,
    submitted_at_ms: i64,
    next_poll_after_ms: i64,
    poll_count: u32,
}

fn fail(detail: impl Into<String>) -> TypedFailure {
    TypedFailure::permanent("oracle.infrastructure", detail)
}

enum ControlFailureKind {
    Deadline,
    Stop,
    Abort,
}

fn control_failure(control: &ExecutionControl) -> Option<TypedFailure> {
    let (kind, code, detail, reason) = match control {
        ExecutionControl::RunUntil(_) => return None,
        ExecutionControl::DeadlineExhausted => (
            ControlFailureKind::Deadline,
            "oracle.deadline",
            "oracle exceeded its recorded effect deadline",
            "effect deadline exhausted".to_string(),
        ),
        ExecutionControl::Stop(reason) => (
            ControlFailureKind::Stop,
            "oracle.stopped",
            "oracle stopped by operator",
            reason.clone(),
        ),
        ExecutionControl::Abort(reason) => (
            ControlFailureKind::Abort,
            "oracle.aborted",
            "oracle cancelled because the mission was aborted",
            reason.clone(),
        ),
    };
    let mut evidence = TypedFailureEvidence::new(Some(code.to_string()), detail);
    evidence.stop_reason = Some(reason);
    Some(match kind {
        ControlFailureKind::Deadline => TypedFailure::DeadlineExhausted {
            evidence: Box::new(evidence),
        },
        ControlFailureKind::Stop => TypedFailure::OperatorStopped {
            evidence: Box::new(evidence),
        },
        ControlFailureKind::Abort => TypedFailure::OperatorAborted {
            evidence: Box::new(evidence),
        },
    })
}

#[async_trait]
impl ExternalOracleDriver for ProfileExternalOracleDriver {
    async fn submit(
        &self,
        request: ExternalOracleSubmitRequest,
        context: ExternalOracleDriverContext,
    ) -> Result<ExternalOracleSubmission, TypedFailure> {
        self.run_driver("submit", request, &context).await
    }

    async fn poll(
        &self,
        request: ExternalOraclePollRequest,
        context: ExternalOracleDriverContext,
    ) -> Result<ExternalOraclePoll, TypedFailure> {
        self.run_driver("poll", request, &context).await
    }
}

impl ProfileExternalOracleDriver {
    async fn run_driver<I, O>(
        &self,
        operation: &str,
        request: I,
        context: &ExternalOracleDriverContext,
    ) -> Result<O, TypedFailure>
    where
        I: Serialize + DriverRequestIdentity + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let mission_id = request.mission_id().clone();
        let effect_id = request.effect_id().clone();
        let environment_digest = request.environment_digest().to_string();
        let dirs = MissionDirs::new(&context.state_dir, &mission_id)
            .effect(&effect_id)
            .oracle();
        dirs.prepare()
            .map_err(|error| fail(format!("failed to prepare external oracle dirs: {error}")))?;

        let mut profile = self.profile.clone();
        profile.confinement.oci_mut().image = Some(environment_digest);
        let authority = oracle_authority_with_network(
            &format!("external-driver:{}", self.driver.id),
            BTreeSet::new(),
            self.driver.network.clone(),
        );
        let compiled = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: profile.name.clone(),
            confinement: profile.confinement,
            mounts: MissionMounts {
                workspace: dirs.scratch().to_path_buf(),
                extras: Vec::new(),
            },
            working_dir: dirs.scratch().to_path_buf(),
            judged_roots: &[],
            environment: external_driver_environment(),
            resources: Default::default(),
            resource_ceilings: &context.resource_ceilings,
            runtime_network: crate::model::NetworkGrant::Deny,
        })
        .map_err(|error| {
            fail(format!(
                "external oracle driver plan refused to compile: {error}"
            ))
        })?;

        let stdin = serde_json::to_string(&request).map_err(|error| {
            TypedFailure::permanent(
                "oracle.external_driver",
                format!("failed to encode external oracle driver request: {error}"),
            )
        })?;
        let program = RuntimeProgramSpec {
            executable: EXTERNAL_ORACLE_DRIVER_EXECUTABLE.to_string(),
            args: vec![self.driver.id.as_str().to_string(), operation.to_string()],
            environment: Vec::new(),
            stdin,
            auth: None,
        };
        let mut executor =
            MissionProgramExecutor::new(compiled.plan().clone(), None, &effect_id, None);
        let run = async move {
            let output = executor
                .execute_captured(program)
                .await
                .map_err(|error| fail(format!("external oracle driver failed to run: {error}")))?;
            if output.exit_code != Some(0) || output.exit_signal.is_some() {
                return Err(TypedFailure::permanent(
                    "oracle.external_driver",
                    "external oracle driver exited without a successful result",
                ));
            }
            serde_json::from_slice(&output.stdout).map_err(|error| {
                TypedFailure::permanent(
                    "oracle.external_result",
                    format!("external oracle driver returned malformed JSON: {error}"),
                )
            })
        };
        await_controlled(Box::pin(run), context.control.clone(), control_failure).await
    }
}

trait DriverRequestIdentity {
    fn mission_id(&self) -> &crate::model::MissionId;
    fn effect_id(&self) -> &crate::model::EffectId;
    fn environment_digest(&self) -> &str;
}

impl DriverRequestIdentity for ExternalOracleSubmitRequest {
    fn mission_id(&self) -> &crate::model::MissionId {
        &self.mission_id
    }

    fn effect_id(&self) -> &crate::model::EffectId {
        &self.effect_id
    }

    fn environment_digest(&self) -> &str {
        &self.environment_digest
    }
}

impl DriverRequestIdentity for ExternalOraclePollRequest {
    fn mission_id(&self) -> &crate::model::MissionId {
        &self.mission_id
    }

    fn effect_id(&self) -> &crate::model::EffectId {
        &self.effect_id
    }

    fn environment_digest(&self) -> &str {
        &self.environment_digest
    }
}

#[async_trait]
impl OracleRunner for OciOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleRunStatus, TypedFailure> {
        if let Some(failure) = control_failure(&request.control.borrow().clone()) {
            return Err(failure);
        }
        let actual_digest = request.spec.digest();
        if actual_digest != request.spec_digest {
            return Err(TypedFailure::permanent(
                "oracle.spec_mismatch",
                "resolved oracle spec does not match the recorded oracle spec digest",
            ));
        }
        match request.spec.clone() {
            crate::model::OracleSpec::Command(command) => self
                .run_command(request, command)
                .await
                .map(OracleRunStatus::Complete),
            crate::model::OracleSpec::External(external) => {
                self.run_external(request, external).await
            }
        }
    }
}

impl OciOracleRunner {
    async fn run_command(
        &self,
        request: OracleRunRequest,
        command: crate::model::CommandOracle,
    ) -> Result<OracleOutcome, TypedFailure> {
        let dirs = MissionDirs::new(&request.state_dir, &request.mission_id)
            .effect(&request.effect_id)
            .oracle();
        let profile = self.profile_for(&request.environment_digest);
        dirs.prepare()
            .map_err(|e| fail(format!("failed to prepare oracle dirs: {e}")))?;

        // Keep every fallible stage in one result. The engine owns the one
        // cleanup path after this runner returns, including failures before a
        // run starts and recovery after this process exits.
        let checkout = dirs.work().to_path_buf();
        let result = async {
            // Complete checkout of the judged commit. The oracle receives it
            // read-only, never a worker's live checkout.
            {
                let _guard = self.repo_lock.lock().await;
                workspace::create_checkout(&request.workspace_dir, &checkout, &request.judged_sha)
                    .await
                    .map_err(|e| fail(format!("failed to create judged checkout: {e}")))?;
            }

            // Preparation may use its declaration's explicit network policy,
            // but only publishes an immutable cache directory. The oracle
            // below remains on its separate network-off authority and sees
            // those directories read-only.
            let prepared = if request.prepared_inputs.is_empty() {
                PreparedInputs {
                    mounts: Vec::new(),
                    environment: Vec::new(),
                    refs: Vec::new(),
                }
            } else {
                let _guard = self.input_lock.lock().await;
                prepare_inputs(
                    &profile,
                    &request.state_dir,
                    dirs.root(),
                    &checkout,
                    &request.prepared_inputs,
                    &request.effect_id,
                )
                .await
                .map_err(|error| fail(format!("failed to prepare mission inputs: {error:#}")))?
            };

            let authority = oracle_authority_with_network(
                request.oracle.as_str(),
                command.grants.devices.clone(),
                command.grants.network.clone(),
            );
            let mut extras = vec![MountSpec {
                source: dirs.scratch().to_path_buf(),
                target: SCRATCH_MOUNT_TARGET.to_string(),
                access: MountAccess::ReadWrite,
            }];
            extras.extend(prepared.mounts);
            let environment = oracle_environment(&command.environment, prepared.environment);
            let working_dir = if command.cwd.as_str() == "." {
                checkout.clone()
            } else {
                checkout.join(command.cwd.as_str())
            };
            let metadata = tokio::fs::metadata(&working_dir).await.map_err(|error| {
                fail(format!(
                    "oracle working directory '{}' is unavailable: {error}",
                    command.cwd.as_str()
                ))
            })?;
            if !metadata.is_dir() {
                return Err(fail(format!(
                    "oracle working directory '{}' is not a directory",
                    command.cwd.as_str()
                )));
            }
            let judged_roots = [crate::authority::canonical_or_lexical(&checkout)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: profile.name.clone(),
                confinement: profile.confinement.clone(),
                mounts: MissionMounts {
                    workspace: checkout.clone(),
                    extras,
                },
                working_dir,
                judged_roots: &judged_roots,
                environment,
                resources: command.resources.clone(),
                resource_ceilings: &request.resource_ceilings,
                runtime_network: crate::model::NetworkGrant::Deny,
            })
            .map_err(|e| fail(format!("oracle plan refused to compile (moat): {e}")))?;

            let program = command_program(&command)?;
            let mut executor = MissionProgramExecutor::new(
                compiled.plan().clone(),
                None,
                &request.effect_id,
                None,
            );
            // Wall-clock duration is recorded evidence, not fold state; measuring
            // it here is a runner concern that never threatens fold purity.
            #[expect(clippy::disallowed_methods)]
            let started = Instant::now();
            let run = executor
                .execute_captured(program)
                .await
                .map_err(|error| fail(format!("oracle failed to run: {error}")));
            let duration_ms = started.elapsed().as_millis() as u64;
            match run {
                Err(failure) => Err(failure),
                Ok(output) => Ok(OracleOutcome {
                    exit_code: output.exit_code.unwrap_or(-1),
                    exit_signal: output.exit_signal,
                    stdout: output.stdout,
                    stderr: output.stderr,
                    prepared_inputs: prepared.refs,
                    duration_ms,
                }),
            }
        };
        await_controlled(Box::pin(result), request.control.clone(), control_failure).await
    }

    async fn run_external(
        &self,
        request: OracleRunRequest,
        external: crate::model::ExternalOracle,
    ) -> Result<OracleRunStatus, TypedFailure> {
        let driver = self.external_drivers.get(&external.driver).ok_or_else(|| {
            TypedFailure::permanent(
                "oracle.external_driver_missing",
                format!(
                    "external oracle driver '{}' is not installed in the runtime profile",
                    external.driver
                ),
            )
        })?;
        let request_digest = request.spec.request_digest().ok_or_else(|| {
            TypedFailure::permanent(
                "oracle.external_spec",
                "external oracle request digest is unavailable for a command spec",
            )
        })?;
        let idempotency_key = external_idempotency_key(&request, &external, &request_digest);
        let artifact_digest = external_artifact_digest(&request.judged_sha);
        let driver_context = ExternalOracleDriverContext {
            state_dir: request.state_dir.clone(),
            resource_ceilings: request.resource_ceilings.clone(),
            control: request.control.clone(),
        };
        let dirs = MissionDirs::new(&request.state_dir, &request.mission_id)
            .effect(&request.effect_id)
            .oracle();
        dirs.prepare()
            .map_err(|error| fail(format!("failed to prepare oracle dirs: {error}")))?;
        let state_path = dirs.root().join("external-oracle-submission.json");
        let mut state = match read_external_state(&state_path).await? {
            Some(state) => {
                validate_external_state(
                    &state,
                    &request,
                    &external,
                    &request_digest,
                    &idempotency_key,
                )?;
                state
            }
            None => {
                let submit = ExternalOracleSubmitRequest {
                    mission_id: request.mission_id.clone(),
                    effect_id: request.effect_id.clone(),
                    oracle: request.oracle.clone(),
                    driver: external.driver.clone(),
                    spec_digest: request.spec_digest.clone(),
                    request_digest: request_digest.clone(),
                    idempotency_key: idempotency_key.clone(),
                    artifact_digest: artifact_digest.clone(),
                    request: external.request.clone(),
                    judged_sha: request.judged_sha.clone(),
                    environment_digest: request.environment_digest.clone(),
                    attempt_no: request.attempt_no,
                    deadline_ms: request.deadline_ms,
                };
                let submission = driver.submit(submit, driver_context.clone()).await?;
                validate_external_submission(
                    &submission,
                    &request,
                    &external,
                    &request_digest,
                    &idempotency_key,
                )?;
                let state = ExternalOracleState {
                    driver: submission.driver,
                    oracle: request.oracle.clone(),
                    spec_digest: submission.spec_digest,
                    request_digest: submission.request_digest,
                    idempotency_key: submission.idempotency_key,
                    artifact_digest: artifact_digest.clone(),
                    job_id: submission.job_id,
                    submitted_at_ms: request.now_ms,
                    next_poll_after_ms: request.now_ms,
                    poll_count: 0,
                };
                write_external_state(&state_path, &state).await?;
                state
            }
        };

        if request.now_ms < state.next_poll_after_ms {
            return Ok(OracleRunStatus::Pending {
                next_poll_after_ms: state.next_poll_after_ms,
            });
        }
        if state.poll_count >= external_max_polls(&external) {
            return Ok(OracleRunStatus::Complete(external_outcome(
                1,
                None,
                "external oracle polling exceeded its bounded attempt budget",
            )?));
        }

        let poll = ExternalOraclePollRequest {
            mission_id: request.mission_id.clone(),
            effect_id: request.effect_id.clone(),
            oracle: request.oracle.clone(),
            driver: external.driver.clone(),
            spec_digest: request.spec_digest.clone(),
            request_digest: request_digest.clone(),
            idempotency_key: idempotency_key.clone(),
            artifact_digest,
            job_id: state.job_id.clone(),
            judged_sha: request.judged_sha.clone(),
            environment_digest: request.environment_digest.clone(),
            attempt_no: request.attempt_no,
            deadline_ms: request.deadline_ms,
        };
        match driver.poll(poll, driver_context).await? {
            ExternalOraclePoll::Pending { retry_after_ms } => {
                state.poll_count = state.poll_count.saturating_add(1);
                state.next_poll_after_ms =
                    next_external_poll_after_ms(&request, &external, retry_after_ms);
                write_external_state(&state_path, &state).await?;
                Ok(OracleRunStatus::Pending {
                    next_poll_after_ms: state.next_poll_after_ms,
                })
            }
            ExternalOraclePoll::Passed { proof } => {
                validate_external_proof(&proof, &state)?;
                Ok(OracleRunStatus::Complete(external_outcome(
                    0,
                    Some(&proof),
                    "external oracle passed",
                )?))
            }
            ExternalOraclePoll::Failed { proof, detail } => {
                validate_external_proof(&proof, &state)?;
                Ok(OracleRunStatus::Complete(external_outcome(
                    1,
                    Some(&proof),
                    &detail,
                )?))
            }
        }
    }
}

fn external_idempotency_key(
    request: &OracleRunRequest,
    external: &crate::model::ExternalOracle,
    request_digest: &str,
) -> String {
    let mut digest = Sha256::new();
    feed_digest_field(
        &mut digest,
        "domain",
        "lionclaw.external-oracle.idempotency.v1",
    );
    feed_digest_field(&mut digest, "mission_id", request.mission_id.as_str());
    feed_digest_field(&mut digest, "effect_id", request.effect_id.as_str());
    feed_digest_field(&mut digest, "oracle", request.oracle.as_str());
    feed_digest_field(&mut digest, "driver", external.driver.as_str());
    feed_digest_field(&mut digest, "spec_digest", &request.spec_digest);
    feed_digest_field(&mut digest, "request_digest", request_digest);
    feed_digest_field(&mut digest, "judged_sha", &request.judged_sha);
    feed_digest_field(
        &mut digest,
        "environment_digest",
        &request.environment_digest,
    );
    feed_digest_field(&mut digest, "attempt_no", &request.attempt_no.to_string());
    feed_digest_field(&mut digest, "deadline_ms", &request.deadline_ms.to_string());
    hex::encode(digest.finalize())
}

fn external_artifact_digest(judged_sha: &str) -> String {
    let mut digest = Sha256::new();
    feed_digest_field(
        &mut digest,
        "domain",
        "lionclaw.external-oracle-artifact.v1",
    );
    feed_digest_field(&mut digest, "judged_sha", judged_sha);
    format!("sha256:{}", hex::encode(digest.finalize()))
}

fn feed_digest_field(digest: &mut Sha256, label: &str, value: &str) {
    digest.update((label.len() as u64).to_be_bytes());
    digest.update(label.as_bytes());
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

async fn read_external_state(path: &Path) -> Result<Option<ExternalOracleState>, TypedFailure> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            TypedFailure::permanent(
                "oracle.external_state",
                format!("external oracle submission state is malformed: {error}"),
            )
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(TypedFailure::permanent(
            "oracle.external_state",
            format!("failed to read external oracle submission state: {error}"),
        )),
    }
}

async fn write_external_state(
    path: &Path,
    state: &ExternalOracleState,
) -> Result<(), TypedFailure> {
    let path = path.to_path_buf();
    let bytes = serde_json::to_vec(state).map_err(|error| {
        TypedFailure::permanent(
            "oracle.external_state",
            format!("failed to encode external oracle submission state: {error}"),
        )
    })?;
    tokio::task::spawn_blocking(move || write_external_state_sync(&path, &bytes))
        .await
        .map_err(|error| {
            TypedFailure::permanent(
                "oracle.external_state",
                format!("external oracle state writer panicked: {error}"),
            )
        })?
        .map_err(|error| {
            TypedFailure::permanent(
                "oracle.external_state",
                format!("failed to durably record external oracle submission: {error}"),
            )
        })
}

fn write_external_state_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "external oracle state path has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn validate_external_state(
    state: &ExternalOracleState,
    request: &OracleRunRequest,
    external: &crate::model::ExternalOracle,
    request_digest: &str,
    idempotency_key: &str,
) -> Result<(), TypedFailure> {
    if state.driver != external.driver
        || state.oracle != request.oracle
        || state.spec_digest != request.spec_digest
        || state.request_digest != request_digest
        || state.idempotency_key != idempotency_key
        || state.artifact_digest != external_artifact_digest(&request.judged_sha)
        || !valid_external_job_id(&state.job_id)
    {
        return Err(TypedFailure::permanent(
            "oracle.external_state",
            "external oracle submission state does not match the active effect",
        ));
    }
    Ok(())
}

fn validate_external_submission(
    submission: &ExternalOracleSubmission,
    request: &OracleRunRequest,
    external: &crate::model::ExternalOracle,
    request_digest: &str,
    idempotency_key: &str,
) -> Result<(), TypedFailure> {
    if submission.driver != external.driver
        || submission.spec_digest != request.spec_digest
        || submission.request_digest != request_digest
        || submission.idempotency_key != idempotency_key
        || !valid_external_job_id(&submission.job_id)
    {
        return Err(TypedFailure::permanent(
            "oracle.external_result",
            "external oracle driver returned a submission for a different request",
        ));
    }
    Ok(())
}

fn validate_external_proof(
    proof: &ExternalOracleProof,
    state: &ExternalOracleState,
) -> Result<(), TypedFailure> {
    if proof.driver != state.driver
        || proof.spec_digest != state.spec_digest
        || proof.request_digest != state.request_digest
        || proof.idempotency_key != state.idempotency_key
        || proof.job_id != state.job_id
        || proof.artifact_digest != state.artifact_digest
    {
        return Err(TypedFailure::permanent(
            "oracle.external_result",
            "external oracle driver returned proof for a different submission",
        ));
    }
    if !valid_artifact_digest(&proof.artifact_digest) {
        return Err(TypedFailure::permanent(
            "oracle.external_result",
            "external oracle driver returned an invalid artifact digest",
        ));
    }
    Ok(())
}

fn valid_external_job_id(job_id: &str) -> bool {
    !job_id.is_empty()
        && job_id.len() <= 256
        && job_id
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/' && byte != b'\\')
}

fn valid_artifact_digest(digest: &str) -> bool {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn external_max_polls(external: &crate::model::ExternalOracle) -> u32 {
    let polls = external
        .timeout_secs
        .saturating_add(external.poll_secs.saturating_sub(1))
        / external.poll_secs.max(1);
    polls.saturating_add(2).min(u64::from(u32::MAX)) as u32
}

fn next_external_poll_after_ms(
    request: &OracleRunRequest,
    external: &crate::model::ExternalOracle,
    retry_after_ms: Option<u64>,
) -> i64 {
    let default_ms = external.poll_secs.saturating_mul(1_000);
    let requested_ms = retry_after_ms.unwrap_or(default_ms);
    let timeout_ms = external.timeout_secs.saturating_mul(1_000);
    let remaining_ms = request.deadline_ms.saturating_sub(request.now_ms).max(0) as u64;
    let delay_ms = requested_ms
        .min(default_ms.saturating_mul(8))
        .min(timeout_ms)
        .min(remaining_ms);
    request
        .now_ms
        .saturating_add(delay_ms.min(i64::MAX as u64) as i64)
}

fn external_outcome(
    exit_code: i32,
    proof: Option<&ExternalOracleProof>,
    detail: &str,
) -> Result<OracleOutcome, TypedFailure> {
    let stdout = match proof {
        Some(proof) => serde_json::to_vec(&serde_json::json!({
            "driver": proof.driver.as_str(),
            "idempotency_key": proof.idempotency_key,
            "spec_digest": proof.spec_digest,
            "request_digest": proof.request_digest,
            "job_id": proof.job_id,
            "artifact_digest": proof.artifact_digest,
            "summary": lionclaw_runtime_api::bounded_text(&proof.summary),
        }))
        .map_err(|error| {
            TypedFailure::permanent(
                "oracle.external_result",
                format!("failed to encode external oracle proof: {error}"),
            )
        })?,
        None => Vec::new(),
    };
    Ok(OracleOutcome {
        exit_code,
        exit_signal: None,
        stdout,
        stderr: if exit_code == 0 {
            Vec::new()
        } else {
            lionclaw_runtime_api::bounded_text(detail).into_bytes()
        },
        prepared_inputs: Vec::new(),
        duration_ms: 0,
    })
}

fn external_driver_environment() -> Vec<(String, String)> {
    vec![
        ("HOME".to_string(), "/tmp".to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
    ]
}

/// Kernel-owned execution coordinates for an oracle. Prepared inputs are the
/// final explicit overlay because they may publish a content-addressed cache.
fn oracle_environment(
    declared: &std::collections::BTreeMap<String, String>,
    prepared_input: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut environment = declared.clone();
    environment.extend(prepared_input);
    environment.extend([
        ("HOME".to_string(), SCRATCH_MOUNT_TARGET.to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
    ]);
    environment.into_iter().collect()
}

fn command_program(
    command: &crate::model::CommandOracle,
) -> Result<RuntimeProgramSpec, TypedFailure> {
    let Some((executable, args)) = command.argv.split_first() else {
        return Err(fail("oracle argv is empty"));
    };
    Ok(RuntimeProgramSpec {
        executable: executable.clone(),
        args: args.to_vec(),
        environment: Vec::new(),
        stdin: String::new(),
        auth: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeProfiles;
    use crate::model::{
        AuthorityGrants, CommandOracle, EffectId, MissionId, OracleName, OracleSpec,
        WorkspaceRelativeDir,
    };
    use std::collections::BTreeMap;

    #[test]
    fn oracle_prepared_input_overrides_domain_cache_location() {
        let environment = BTreeMap::from_iter(oracle_environment(
            &BTreeMap::from([
                ("BUILD_OUTPUT".to_string(), "/scratch/build".to_string()),
                ("TOOL_HOME".to_string(), "/scratch/tool".to_string()),
            ]),
            vec![("TOOL_HOME".to_string(), "/inputs/tool".to_string())],
        ));
        assert_eq!(environment["HOME"], "/scratch");
        assert_eq!(environment["BUILD_OUTPUT"], "/scratch/build");
        assert_eq!(environment["TOOL_HOME"], "/inputs/tool");
    }

    #[test]
    fn shell_syntax_is_a_literal_argument() {
        let program = command_program(&CommandOracle {
            argv: vec![
                "printf".into(),
                "$(touch /workspace/escaped)".into(),
                "a; echo b".into(),
            ],
            cwd: WorkspaceRelativeDir::new(".").unwrap(),
            environment: BTreeMap::new(),
            timeout_secs: 30,
            grants: AuthorityGrants::default(),
            resources: Default::default(),
        })
        .unwrap();

        assert_eq!(program.executable, "printf");
        assert_eq!(
            program.args,
            vec!["$(touch /workspace/escaped)", "a; echo b"]
        );
    }

    #[test]
    fn external_proof_requires_exact_judged_artifact_digest() {
        let state = ExternalOracleState {
            driver: crate::model::ExternalOracleDriverId::new("local-ci").unwrap(),
            oracle: OracleName::new("ci").unwrap(),
            spec_digest: "spec-digest".to_string(),
            request_digest: "request-digest".to_string(),
            idempotency_key: "idem".to_string(),
            artifact_digest: external_artifact_digest("judged-sha"),
            job_id: "job-1".to_string(),
            submitted_at_ms: 0,
            next_poll_after_ms: 0,
            poll_count: 0,
        };
        let mut proof = ExternalOracleProof {
            driver: state.driver.clone(),
            idempotency_key: state.idempotency_key.clone(),
            spec_digest: state.spec_digest.clone(),
            request_digest: state.request_digest.clone(),
            job_id: state.job_id.clone(),
            artifact_digest: String::new(),
            summary: String::new(),
        };

        let missing =
            validate_external_proof(&proof, &state).expect_err("missing artifact digest must fail");
        assert_eq!(
            missing.evidence().code.as_deref(),
            Some("oracle.external_result")
        );

        proof.artifact_digest = format!("sha256:{}", "0".repeat(64));
        let wrong = validate_external_proof(&proof, &state)
            .expect_err("digest for a different judged artifact must fail");
        assert_eq!(
            wrong.evidence().code.as_deref(),
            Some("oracle.external_result")
        );

        proof.artifact_digest = state.artifact_digest.clone();
        validate_external_proof(&proof, &state).expect("matching proof");
    }

    #[tokio::test]
    async fn production_oracle_observes_stop_before_checkout_or_runtime_launch() {
        let temp = tempfile::tempdir().unwrap();
        let profile = RuntimeProfiles::from_toml(
            "[runtimes.test]\ndriver = \"acp\"\ncommand = \"never-launched\"\n",
            temp.path(),
        )
        .unwrap()
        .get("test")
        .unwrap();
        let runner = OciOracleRunner::new(profile);
        let (_control_tx, control) =
            tokio::sync::watch::channel(ExecutionControl::Stop("operator stop".into()));
        let spec = OracleSpec::Command(CommandOracle {
            argv: vec!["true".into()],
            cwd: WorkspaceRelativeDir::new(".").unwrap(),
            environment: BTreeMap::new(),
            timeout_secs: 30,
            grants: AuthorityGrants::default(),
            resources: Default::default(),
        });
        let result = runner
            .run(OracleRunRequest {
                mission_id: MissionId::parse("m123456789abc").unwrap(),
                effect_id: EffectId::for_parts(&["oracle", "pre-start-stop"]),
                oracle: OracleName::new("checks").unwrap(),
                spec_digest: spec.digest(),
                spec,
                judged_sha: "must-not-be-resolved".into(),
                environment_digest: "sha256:oracle-test".into(),
                attempt_no: 1,
                now_ms: 0,
                workspace_dir: temp.path().join("must-not-be-cloned"),
                state_dir: temp.path().join("state"),
                prepared_inputs: Vec::new(),
                resource_ceilings: Default::default(),
                deadline_ms: 10,
                control,
            })
            .await;

        let failure = result.expect_err("pre-start stop wins before setup");
        assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
        assert_eq!(
            failure.evidence().stop_reason.as_deref(),
            Some("operator stop")
        );
    }
}
