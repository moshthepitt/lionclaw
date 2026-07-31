mod common;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lionclaw::config::RuntimeProfiles;
use lionclaw::model::{
    Choice, EffectId, ExternalOracle, ExternalOracleDriverId, ExternalOracleDriverIdentity,
    MissionId, MissionProposal, NetworkGrant, OracleName, OracleSpec,
};
use lionclaw::oracle::OciOracleRunner;
use lionclaw::ports::{
    ExternalOracleDriver, ExternalOracleDriverContext, ExternalOracleDriverRegistry,
    ExternalOraclePoll, ExternalOraclePollRequest, ExternalOracleProof, ExternalOracleSubmission,
    ExternalOracleSubmitRequest, OracleOutcome, OracleRunRequest, OracleRunStatus, OracleRunner,
};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::TypedFailure;

use common::{approve_plan, proposal, simple_plan, BASE_SHA, HEAD_SHA};

fn external_spec(driver: &str) -> OracleSpec {
    OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new(driver).unwrap(),
        driver_identity: Some(ExternalOracleDriverIdentity {
            driver: ExternalOracleDriverId::new(driver).unwrap(),
            image_id: "sha256:external-test".to_string(),
            network: NetworkGrant::Deny,
            auth: None,
        }),
        request: BTreeMap::from([
            ("suite".to_string(), "cargo-test".to_string()),
            ("artifact".to_string(), "workspace".to_string()),
        ]),
        timeout_secs: 120,
        poll_secs: 5,
    })
}

fn unresolved_external_spec(driver: &str) -> OracleSpec {
    OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new(driver).unwrap(),
        driver_identity: None,
        request: BTreeMap::from([
            ("suite".to_string(), "cargo-test".to_string()),
            ("artifact".to_string(), "workspace".to_string()),
        ]),
        timeout_secs: 120,
        poll_secs: 5,
    })
}

fn external_proposal(driver: &str) -> MissionProposal {
    let mut proposal = proposal(0, simple_plan());
    proposal.oracles = Some(BTreeMap::from([(
        OracleName::new("cargo-test").unwrap(),
        unresolved_external_spec(driver),
    )]));
    proposal
}

fn external_driver_identity(
    driver: &str,
    network: NetworkGrant,
) -> (ExternalOracleDriverId, ExternalOracleDriverIdentity) {
    let id = ExternalOracleDriverId::new(driver).unwrap();
    (
        id.clone(),
        ExternalOracleDriverIdentity {
            driver: id,
            image_id: "localhost/lionclaw-runtime-dev:v1".to_string(),
            network,
            auth: None,
        },
    )
}

async fn harness_with_external_driver(
    workspace: &std::path::Path,
    identity: ExternalOracleDriverIdentity,
    oracle_runner: MockOracleRunner,
) -> common::TestHarness {
    let driver_id = identity.driver.clone();
    let mut runtime_identities = common::default_runtime_identities();
    runtime_identities
        .get_mut("codex")
        .unwrap()
        .external_oracle_drivers
        .insert(driver_id.clone(), identity.clone());
    let role_runner = Arc::new(MockRoleRunner::happy(HEAD_SHA));
    let oracle_runner = Arc::new(oracle_runner);
    common::initialize_repository(workspace);
    let engine = common::engine_with_runtime_and_external_driver_identities(
        workspace,
        common::test_mission_type(),
        role_runner.clone(),
        oracle_runner.clone(),
        runtime_identities,
        BTreeMap::from([(driver_id, identity)]),
    )
    .await;
    common::TestHarness {
        engine,
        role_runner,
        oracle_runner,
    }
}

fn passing_outcome() -> OracleOutcome {
    OracleOutcome {
        exit_code: 0,
        exit_signal: None,
        stdout: b"external proof accepted".to_vec(),
        stderr: Vec::new(),
        prepared_inputs: Vec::new(),
        duration_ms: 10,
    }
}

#[tokio::test]
async fn pending_external_oracle_stays_in_resolve_effect_until_polled_complete() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(Mutex::new(0usize));
    let oracle = MockOracleRunner::new_status(Box::new({
        let calls = calls.clone();
        move |request| {
            assert!(matches!(request.spec, OracleSpec::External(_)));
            let mut calls = calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Ok(OracleRunStatus::Pending {
                    next_poll_after_ms: request.now_ms + 5_000,
                })
            } else {
                Ok(OracleRunStatus::Complete(passing_outcome()))
            }
        }
    }));
    let (_driver_id, identity) = external_driver_identity("local-ci", NetworkGrant::Deny);
    let h = harness_with_external_driver(dir.path(), identity, oracle).await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "external proof", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, external_proposal("local-ci"))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;

    let pending = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(pending.state.inflight.len(), 1);
    assert!(pending
        .next
        .effects
        .iter()
        .any(|effect| matches!(effect, lionclaw::model::EffectIntent::ResolveEffect { .. })));
    assert!(h
        .engine
        .finish(&mission_id, "must not finish while external job is pending")
        .await
        .is_err());

    let ready = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(*calls.lock().unwrap(), 2);
    assert!(ready.state.inflight.is_empty());
    assert!(ready
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Finish { .. })));
    assert!(
        ready.state.terminal.is_none(),
        "external pass never auto-finishes"
    );
}

#[tokio::test]
async fn plan_admission_resolves_external_driver_identity_before_recording() {
    let dir = tempfile::tempdir().unwrap();
    let (_driver_id, identity) = external_driver_identity("local-ci", NetworkGrant::Deny);
    let h =
        harness_with_external_driver(dir.path(), identity.clone(), MockOracleRunner::exiting(0))
            .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "external oracle identity",
            BASE_SHA,
        )
        .await
        .unwrap();

    h.engine
        .propose_plan(&mission_id, external_proposal("local-ci"))
        .await
        .unwrap();
    let state = h.engine.store().require_state(&mission_id).await.unwrap();
    let recorded = state
        .proposal
        .as_ref()
        .and_then(|proposal| proposal.oracles.as_ref())
        .and_then(|oracles| oracles.get(&OracleName::new("cargo-test").unwrap()))
        .expect("recorded oracle");
    let OracleSpec::External(external) = recorded else {
        panic!("expected external oracle");
    };

    assert_eq!(external.driver_identity.as_ref(), Some(&identity));
}

#[tokio::test]
async fn plan_admission_rejects_external_driver_network_above_mission_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let (_driver_id, identity) = external_driver_identity(
        "local-ci",
        NetworkGrant::allow_single("ci.example.com", 443).unwrap(),
    );
    let h = harness_with_external_driver(dir.path(), identity, MockOracleRunner::exiting(0)).await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "external oracle ceiling",
            BASE_SHA,
        )
        .await
        .unwrap();

    let error = h
        .engine
        .propose_plan(&mission_id, external_proposal("local-ci"))
        .await
        .expect_err("driver network above mission ceiling must be rejected");
    assert!(error
        .to_string()
        .contains("oracle authority exceeds mission ceilings"));
}

#[derive(Default)]
struct LocalExternalDriver {
    submissions: Mutex<Vec<ExternalOracleSubmitRequest>>,
    polls: Mutex<Vec<ExternalOraclePollRequest>>,
    polls_to_return: Mutex<VecDeque<ExternalOraclePoll>>,
}

impl LocalExternalDriver {
    fn with_polls(polls: Vec<ExternalOraclePoll>) -> Self {
        Self {
            polls_to_return: Mutex::new(polls.into()),
            ..Default::default()
        }
    }
}

#[async_trait]
impl ExternalOracleDriver for LocalExternalDriver {
    async fn submit(
        &self,
        request: ExternalOracleSubmitRequest,
        _context: ExternalOracleDriverContext,
    ) -> Result<ExternalOracleSubmission, TypedFailure> {
        let job_id = format!("job-{}", &request.idempotency_key[..16]);
        self.submissions.lock().unwrap().push(request.clone());
        Ok(ExternalOracleSubmission {
            driver: request.driver,
            idempotency_key: request.idempotency_key,
            spec_digest: request.spec_digest,
            request_digest: request.request_digest,
            job_id,
        })
    }

    async fn poll(
        &self,
        request: ExternalOraclePollRequest,
        _context: ExternalOracleDriverContext,
    ) -> Result<ExternalOraclePoll, TypedFailure> {
        self.polls.lock().unwrap().push(request.clone());
        let Some(mut poll) = self.polls_to_return.lock().unwrap().pop_front() else {
            return Ok(ExternalOraclePoll::Pending {
                retry_after_ms: Some(5_000),
            });
        };
        if let ExternalOraclePoll::Passed { proof } | ExternalOraclePoll::Failed { proof, .. } =
            &mut poll
        {
            if proof.driver.as_str() == "local-ci"
                && proof.idempotency_key.is_empty()
                && proof.spec_digest.is_empty()
                && proof.request_digest.is_empty()
                && proof.job_id.is_empty()
                && proof.artifact_digest.is_empty()
            {
                proof.driver = request.driver;
                proof.idempotency_key = request.idempotency_key;
                proof.spec_digest = request.spec_digest;
                proof.request_digest = request.request_digest;
                proof.job_id = request.job_id;
                proof.artifact_digest = request.artifact_digest;
            }
        }
        Ok(poll)
    }
}

fn external_request(temp: &tempfile::TempDir, spec: OracleSpec) -> OracleRunRequest {
    std::fs::create_dir_all(temp.path().join("state")).unwrap();
    let (control_tx, control) =
        tokio::sync::watch::channel(lionclaw::ports::ExecutionControl::RunUntil(2_000_000));
    drop(control_tx);
    OracleRunRequest {
        mission_id: MissionId::parse("m123456789abc").unwrap(),
        effect_id: EffectId::for_parts(&["external-oracle", "request"]),
        oracle: OracleName::new("cargo-test").unwrap(),
        spec_digest: spec.digest(),
        spec,
        judged_sha: HEAD_SHA.to_string(),
        environment_digest: "sha256:external-test".to_string(),
        attempt_no: 1,
        now_ms: 1_000_000,
        workspace_dir: temp.path().join("workspace"),
        state_dir: temp.path().join("state"),
        prepared_inputs: Vec::new(),
        resource_ceilings: Default::default(),
        deadline_ms: 2_000_000,
        control,
    }
}

fn runner(driver: Arc<LocalExternalDriver>) -> OciOracleRunner {
    let temp = tempfile::tempdir().unwrap();
    let profile = RuntimeProfiles::from_toml(
        "[runtimes.test]\ndriver = \"acp\"\ncommand = \"unused-for-external\"\n",
        temp.path(),
    )
    .unwrap()
    .get("test")
    .unwrap();
    OciOracleRunner::new(profile).with_external_drivers(
        ExternalOracleDriverRegistry::new()
            .with_driver(ExternalOracleDriverId::new("local-ci").unwrap(), driver),
    )
}

#[tokio::test]
async fn external_submission_is_idempotent_across_crash_before_recording() {
    let temp = tempfile::tempdir().unwrap();
    let driver = Arc::new(LocalExternalDriver::with_polls(vec![
        ExternalOraclePoll::Pending {
            retry_after_ms: Some(5_000),
        },
        ExternalOraclePoll::Pending {
            retry_after_ms: Some(5_000),
        },
    ]));
    let runner = runner(driver.clone());
    let request = external_request(&temp, external_spec("local-ci"));

    let first = runner.run(request.clone()).await.unwrap();
    assert!(matches!(first, OracleRunStatus::Pending { .. }));
    let state_file = temp
        .path()
        .join("state/missions/m123456789abc/effects")
        .join(request.effect_id.as_str())
        .join("external-oracle-submission.json");
    std::fs::remove_file(&state_file).unwrap();

    let second = runner.run(request).await.unwrap();
    assert!(matches!(second, OracleRunStatus::Pending { .. }));
    let submissions = driver.submissions.lock().unwrap();
    assert_eq!(submissions.len(), 2);
    assert_eq!(
        submissions[0].idempotency_key,
        submissions[1].idempotency_key
    );
}

#[tokio::test]
async fn external_submission_record_survives_crash_after_recording() {
    let temp = tempfile::tempdir().unwrap();
    let driver = Arc::new(LocalExternalDriver::with_polls(vec![
        ExternalOraclePoll::Pending {
            retry_after_ms: Some(0),
        },
        ExternalOraclePoll::Passed {
            proof: ExternalOracleProof::empty_for_testing(),
        },
    ]));
    let runner = runner(driver.clone());
    let request = external_request(&temp, external_spec("local-ci"));

    assert!(matches!(
        runner.run(request.clone()).await.unwrap(),
        OracleRunStatus::Pending { .. }
    ));
    assert!(matches!(
        runner.run(OracleRunRequest {
            now_ms: request.now_ms + 1,
            ..request
        })
        .await
        .unwrap(),
        OracleRunStatus::Complete(outcome) if outcome.exit_code == 0
    ));
    assert_eq!(driver.submissions.lock().unwrap().len(), 1);
    assert_eq!(driver.polls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn external_driver_result_must_match_submission_identity() {
    let temp = tempfile::tempdir().unwrap();
    let driver = Arc::new(LocalExternalDriver::with_polls(vec![
        ExternalOraclePoll::Passed {
            proof: ExternalOracleProof {
                driver: ExternalOracleDriverId::new("other-driver").unwrap(),
                ..ExternalOracleProof::empty_for_testing()
            },
        },
    ]));
    let runner = runner(driver);
    let failure = runner
        .run(external_request(&temp, external_spec("local-ci")))
        .await
        .expect_err("forged driver result is rejected");

    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("oracle.external_result")
    );
}

#[tokio::test]
async fn external_driver_result_must_match_recorded_spec_digest() {
    let temp = tempfile::tempdir().unwrap();
    let driver = Arc::new(LocalExternalDriver::with_polls(vec![
        ExternalOraclePoll::Passed {
            proof: ExternalOracleProof {
                spec_digest: "wrong-spec".to_string(),
                ..ExternalOracleProof::empty_for_testing()
            },
        },
    ]));
    let runner = runner(driver);
    let failure = runner
        .run(external_request(&temp, external_spec("local-ci")))
        .await
        .expect_err("wrong spec result is rejected");

    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("oracle.external_result")
    );
}

#[tokio::test]
async fn external_driver_result_must_carry_a_valid_artifact_digest() {
    let temp = tempfile::tempdir().unwrap();
    let driver = Arc::new(LocalExternalDriver::with_polls(vec![
        ExternalOraclePoll::Passed {
            proof: ExternalOracleProof {
                artifact_digest: "sha256:not-lowercase-hex".to_string(),
                ..ExternalOracleProof::empty_for_testing()
            },
        },
    ]));
    let runner = runner(driver);
    let failure = runner
        .run(external_request(&temp, external_spec("local-ci")))
        .await
        .expect_err("wrong artifact digest is rejected");

    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("oracle.external_result")
    );
}
