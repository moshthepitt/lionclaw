mod common;

use clap::Parser;
use common::{BASE_SHA, HEAD_SHA};
use lionclaw::config::RuntimeProfiles;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use lionclaw::cli;
use lionclaw::model::{
    resolve_role_assignment, role_prompt_template, ArtifactOutcome, AssertionId, EffectId,
    EnvironmentPreflight, FinishClass, Handoff, MissionEvent, OracleName, OracleRunSuccess,
    OutputSemantics, PayloadRef, RoleAssignmentContext, RoleInstanceId, RoleTurnSuccess, TaskId,
    WorkspacePreparation,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverProvider, RuntimeDriverRegistry,
    TypedFailure,
};
use lionclaw_runtime_codex::{CodexRuntimeAuthProvider, CodexRuntimeDriver};

fn digest(ch: char) -> String {
    ch.to_string().repeat(64)
}

fn environment_event(
    image_ref: impl Into<String>,
    image_id: impl Into<String>,
    team_revision: Option<u32>,
) -> MissionEvent {
    let image_ref = image_ref.into();
    let image_id = image_id.into();
    MissionEvent::EnvironmentAssigned {
        preflight: EnvironmentPreflight {
            engine: "podman".to_string(),
            image_ref: image_ref.clone(),
            image_id: image_id.clone(),
        },
        image_ref,
        image_id,
        team_revision,
        reason: "pin benchmark environment".to_string(),
    }
}

fn guide_json(repo: &Path, mission_id: &lionclaw::model::MissionId) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "guide", mission_id.as_str(), "--json"])
        .arg("--repo")
        .arg(repo)
        .output()
        .expect("run production mission guide");
    assert!(
        output.status.success(),
        "guide failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("guide JSON")
}

fn assert_guide_projection(
    repo: &Path,
    mission_id: &lionclaw::model::MissionId,
    terminal: Option<&str>,
) {
    let guide = guide_json(repo, mission_id);
    assert_eq!(guide["mission_id"], mission_id.as_str());
    assert!(guide["environment"]["image_id"].is_string());
    assert!(guide["next"]["effects"].is_array());
    assert!(guide["next"]["choices"].is_array());
    assert_eq!(
        guide["terminal"]["kind"].as_str(),
        terminal,
        "unexpected terminal projection: {guide:#}"
    );
}

fn transports_with_oci(root: &Path, script: &str) -> cli::MissionTransports {
    let fake_oci = root.join("fake-oci");
    std::fs::write(&fake_oci, script).expect("write fake OCI");
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake OCI");
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
model = "test-model"
auth = "codex"
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}

[runtimes.opencode]
driver = "acp"
command = "external-opencode"
model = "test-model"
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display(),
            fake_oci.display()
        ),
        root,
    )
    .unwrap();
    cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([
            Arc::new(CodexRuntimeDriver) as Arc<dyn RuntimeDriverProvider>,
            Arc::new(AcpRuntimeDriver) as Arc<dyn RuntimeDriverProvider>,
        ]),
        RuntimeAuthRegistry::new([
            Arc::new(CodexRuntimeAuthProvider) as Arc<dyn RuntimeAuthProvider>
        ]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
}

#[tokio::test]
async fn environment_assignment_is_digest_pinned_and_folded_into_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = common::harness(
        dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "pin environment", BASE_SHA)
        .await
        .unwrap();
    let state = h.engine.store().require_state(&mission_id).await.unwrap();
    let team_revision = state.team.as_ref().map(|team| team.revision);
    let image_ref = format!("localhost/lionclaw@sha256:{}", digest('a'));
    let image_id = format!("sha256:{}", digest('b'));

    common::fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(environment_event(
            image_ref.clone(),
            image_id.clone(),
            team_revision,
        ))],
        10,
    )
    .await;

    let updated = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(updated.image_id, image_id);
    assert_eq!(updated.environment_history.len(), 1);
    assert_eq!(updated.environment_history[0].revision, 1);
    assert_eq!(updated.environment_history[0].image_ref, image_ref);
}

#[tokio::test]
async fn environment_use_preflights_and_records_only_verified_digests() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = common::harness(
        dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "cli environment", BASE_SHA)
        .await
        .unwrap();
    let image_id = format!("sha256:{}", digest('b'));
    let transports = transports_with_oci(
        dir.path(),
        &format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo {image_id}; exit 0; fi\nexit 2\n"
        ),
    );
    let image_ref = format!("sha256:{}", digest('a'));
    let code = cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "environment",
            "use",
            image_ref.as_str(),
            "--mission-id",
            mission_id.as_str(),
            "--reason",
            "benchmark image",
            "--repo",
            dir.path().to_str().unwrap(),
            "--json",
        ])
        .unwrap(),
        transports,
    )
    .await
    .unwrap();
    assert_eq!(code, std::process::ExitCode::SUCCESS);
    let updated = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(updated.image_id, image_id);
    assert_eq!(updated.environment_history.len(), 1);

    let failing_dir = tempfile::tempdir().expect("tempdir");
    let failing = common::harness(
        failing_dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let failing_id = failing
        .engine
        .create_mission(
            failing_dir.path().to_str().unwrap(),
            "failed preflight",
            BASE_SHA,
        )
        .await
        .unwrap();
    let failing_state = failing
        .engine
        .store()
        .require_state(&failing_id)
        .await
        .unwrap();
    let failing_transports = transports_with_oci(
        failing_dir.path(),
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo refused >&2; exit 42; fi\nexit 2\n",
    );
    let code = cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "environment",
            "use",
            image_ref.as_str(),
            "--mission-id",
            failing_id.as_str(),
            "--reason",
            "benchmark image",
            "--repo",
            failing_dir.path().to_str().unwrap(),
            "--json",
        ])
        .unwrap(),
        failing_transports,
    )
    .await
    .unwrap();
    assert_eq!(code, std::process::ExitCode::FAILURE);
    let after_failure = failing
        .engine
        .store()
        .require_state(&failing_id)
        .await
        .unwrap();
    assert_eq!(after_failure.image_id, failing_state.image_id);
    assert!(after_failure.environment_history.is_empty());
}

#[tokio::test]
async fn environment_digest_change_stales_authoritative_proof_and_reruns_oracle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = common::harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "environment proof freshness",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&h.engine, &mission_id).await;

    let running = h.engine.store().require_state(&mission_id).await.unwrap();
    let team_revision = running.team.as_ref().map(|team| team.revision);
    let image_a = format!("sha256:{}", digest('a'));
    common::fault_append_events(
        dir.path(),
        &mission_id,
        running.head,
        &[NewEvent::new(environment_event(
            image_a.clone(),
            image_a.clone(),
            team_revision,
        ))],
        20,
    )
    .await;
    let image_a_state = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(image_a_state.environment_digest(), image_a);

    let task_id = TaskId::new("fix").unwrap();
    let assertion_id = AssertionId::new("TESTS-PASS").unwrap();
    let role_instance = RoleInstanceId::new("implementer").unwrap();
    let team = image_a_state.team.as_ref().expect("team");
    let role = team.role(&role_instance).expect("implementer");
    let prompt_hash = digest('c');
    let assignment = resolve_role_assignment(
        &role_instance,
        team.revision,
        RoleAssignmentContext {
            previous: image_a_state.tasks.get(&task_id),
            required_base: BASE_SHA,
            dependency_refs: &[],
            lifecycle_generation: image_a_state.revision.max(1),
            retrying_failure: false,
            output: role.output,
        },
    );
    let role_effect = EffectId::for_role_turn(
        &mission_id,
        &role_instance,
        team.revision,
        Some(&task_id),
        1,
        assignment.generation,
        &prompt_hash,
    );
    let oracle = OracleName::new("cargo-test").unwrap();
    let oracle_spec_digest = image_a_state.oracles[&oracle].digest();
    let oracle_deadline_ms = lionclaw::model::resolve_execution_deadline_ms(
        22,
        image_a_state.oracles[&oracle].as_command().timeout_secs,
    )
    .unwrap();
    let oracle_effect =
        EffectId::for_oracle_request(&mission_id, &oracle, &oracle_spec_digest, HEAD_SHA, 1);
    common::fault_append_events(
        dir.path(),
        &mission_id,
        image_a_state.head,
        &[
            NewEvent::new(MissionEvent::RoleTurnRequested {
                role_instance: role_instance.clone(),
                team_revision: team.revision,
                task_id: Some(task_id),
                assertion_ids: vec![assertion_id.clone()],
                attempt_no: 1,
                effect_id: role_effect.clone(),
                prompt_template: role_prompt_template(OutputSemantics::ProducesArtifact),
                prompt_hash: prompt_hash.clone(),
                base_sha: assignment.base_sha.clone(),
                environment_digest: image_a_state.environment_digest().to_string(),
                instrument_identity: image_a_state
                    .role_instrument_identity_for_revision(&role_instance, team.revision)
                    .expect("role instrument identity"),
                dependency_refs: assignment.dependency_refs.clone(),
                report_refs: Vec::new(),
                assignment_epoch: assignment.generation,
                message_boundary: image_a_state.head,
                presented_messages: Vec::new(),
                workspace_preparation: assignment.workspace_preparation,
                requested_at_ms: 21,
                deadline_ms: 30_000,
                budget_deadline_ms: 30_000,
            })
            .with_prompt_hash(prompt_hash),
            NewEvent::new(MissionEvent::RoleTurnCompleted {
                effect_id: role_effect.clone(),
                outcome: Ok(RoleTurnSuccess {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report: PayloadRef::inline("implemented"),
                        request_attention: false,
                    }),
                    artifact: Some(ArtifactOutcome {
                        base_sha: BASE_SHA.to_string(),
                        head_sha: HEAD_SHA.to_string(),
                    }),
                    final_response: PayloadRef::inline("done"),
                    runtime_configuration: Default::default(),
                    runtime_usage: Default::default(),
                    prepared_inputs: Vec::new(),
                }),
            }),
            NewEvent::new(MissionEvent::ConversationResourcesCleaned {
                role_instance,
                effect_id: role_effect,
            }),
            NewEvent::new(MissionEvent::OracleRunRequested {
                assertion_ids: vec![assertion_id.clone()],
                oracle: oracle.clone(),
                spec_digest: oracle_spec_digest.clone(),
                judged_sha: HEAD_SHA.to_string(),
                environment_digest: image_a_state.environment_digest().to_string(),
                attempt_no: 1,
                effect_id: oracle_effect.clone(),
                requested_at_ms: 22,
                deadline_ms: oracle_deadline_ms,
            }),
            NewEvent::new(MissionEvent::OracleRunCompleted {
                assertion_ids: vec![assertion_id.clone()],
                oracle: oracle.clone(),
                spec_digest: oracle_spec_digest,
                judged_sha: HEAD_SHA.to_string(),
                attempt_no: 1,
                effect_id: oracle_effect,
                outcome: Ok(OracleRunSuccess {
                    exit_code: 0,
                    exit_signal: None,
                    stdout: PayloadRef::inline("pass"),
                    stderr: PayloadRef::inline(""),
                    prepared_inputs: Vec::new(),
                    duration_ms: 1,
                }),
            }),
        ],
        21,
    )
    .await;
    let proved = h.engine.store().require_state(&mission_id).await.unwrap();
    let verdict = proved
        .authoritative_verdict(&proved.contract[&assertion_id])
        .expect("proof under image A");
    assert_eq!(verdict.environment_digest(), image_a);
    assert!(verdict.is_fresh_at(&proved));
    assert_eq!(common::finish_choice(&proved), Some(FinishClass::Verified));

    let image_b = format!("sha256:{}", digest('b'));
    common::fault_append_events(
        dir.path(),
        &mission_id,
        proved.head,
        &[NewEvent::new(environment_event(
            image_b.clone(),
            image_b.clone(),
            team_revision,
        ))],
        30,
    )
    .await;
    let stale = h.engine.store().require_state(&mission_id).await.unwrap();
    let stale_verdict = stale
        .authoritative_verdict(&stale.contract[&assertion_id])
        .expect("historical proof remains inspectable");
    assert_eq!(stale_verdict.environment_digest(), image_a);
    assert!(!stale_verdict.is_fresh_at(&stale));
    assert_eq!(common::finish_choice(&stale), None);

    let rerun = common::advance_to_finished(&h.engine, &mission_id).await;
    assert_eq!(rerun.state.finish(), Some(FinishClass::Verified));
    {
        let rerun_calls = h.oracle_runner.calls.lock().expect("oracle calls");
        assert_eq!(
            rerun_calls.as_slice(),
            [("cargo-test".to_string(), HEAD_SHA.to_string())]
        );
    }

    let events = h.engine.store().load(&mission_id).await.unwrap();
    let requests = events
        .iter()
        .filter_map(|envelope| match &envelope.event {
            MissionEvent::OracleRunRequested {
                attempt_no,
                environment_digest,
                ..
            } => Some((*attempt_no, environment_digest.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(requests, vec![(1, image_a.as_str()), (2, image_b.as_str())]);
    let final_verdict = rerun
        .state
        .authoritative_verdict(&rerun.state.contract[&assertion_id])
        .expect("proof under image B");
    assert_eq!(final_verdict.environment_digest(), image_b);
    assert!(final_verdict.is_fresh_at(&rerun.state));
}

#[tokio::test]
async fn mission_guide_renders_every_phase() {
    let planning_dir = tempfile::tempdir().expect("tempdir");
    let planning = common::harness(
        planning_dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let planning_id = planning
        .engine
        .create_mission(
            planning_dir.path().to_str().unwrap(),
            "guide planning",
            BASE_SHA,
        )
        .await
        .unwrap();
    assert_guide_projection(planning_dir.path(), &planning_id, None);

    let running_dir = tempfile::tempdir().expect("tempdir");
    let running = common::harness(
        running_dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let running_id = running
        .engine
        .create_mission(
            running_dir.path().to_str().unwrap(),
            "guide running",
            BASE_SHA,
        )
        .await
        .unwrap();
    running
        .engine
        .propose_plan(&running_id, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&running.engine, &running_id).await;
    assert_guide_projection(running_dir.path(), &running_id, None);

    let attention_dir = tempfile::tempdir().expect("tempdir");
    let failing = MockRoleRunner::new(Box::new(|_request| {
        Err(TypedFailure::permanent("test.failure", "role failed"))
    }));
    let attention =
        common::harness(attention_dir.path(), failing, MockOracleRunner::exiting(0)).await;
    let attention_id = attention
        .engine
        .create_mission(
            attention_dir.path().to_str().unwrap(),
            "guide attention",
            BASE_SHA,
        )
        .await
        .unwrap();
    attention
        .engine
        .propose_plan(&attention_id, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&attention.engine, &attention_id).await;
    attention.engine.advance(&attention_id).await.unwrap();
    assert_guide_projection(attention_dir.path(), &attention_id, None);

    let done_dir = tempfile::tempdir().expect("tempdir");
    let done = common::harness(
        done_dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let done_id = done
        .engine
        .create_mission(done_dir.path().to_str().unwrap(), "guide done", BASE_SHA)
        .await
        .unwrap();
    done.engine
        .propose_plan(&done_id, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&done.engine, &done_id).await;
    common::advance_to_finished(&done.engine, &done_id).await;
    assert_guide_projection(done_dir.path(), &done_id, Some("done"));

    let aborted_dir = tempfile::tempdir().expect("tempdir");
    let aborted = common::harness(
        aborted_dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let aborted_id = aborted
        .engine
        .create_mission(
            aborted_dir.path().to_str().unwrap(),
            "guide aborted",
            BASE_SHA,
        )
        .await
        .unwrap();
    aborted.engine.abort(&aborted_id, "stop").await.unwrap();
    assert_guide_projection(aborted_dir.path(), &aborted_id, Some("aborted"));
}

#[tokio::test]
async fn environment_assignment_rejects_tags_and_inflight_races() {
    use rustix::fs::{flock, FlockOperation};
    use std::fs::OpenOptions;

    let dir = tempfile::tempdir().expect("tempdir");
    let h = common::harness(
        dir.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "guard environment", BASE_SHA)
        .await
        .unwrap();
    let initial = h.engine.store().require_state(&mission_id).await.unwrap();
    let team = initial.team.as_ref().expect("default team");
    let team_revision = Some(team.revision);
    common::fault_append_events(
        dir.path(),
        &mission_id,
        initial.head,
        &[NewEvent::new(environment_event(
            "localhost/lionclaw:latest",
            format!("sha256:{}", digest('c')),
            team_revision,
        ))],
        11,
    )
    .await;
    let after_tag = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(after_tag.image_id, initial.image_id);
    assert!(after_tag.environment_history.is_empty());

    let lock_path = dir
        .path()
        .join(".lionclaw/missions")
        .join(mission_id.as_str())
        .join("driver.lock");
    std::fs::create_dir_all(lock_path.parent().expect("lock parent")).expect("lock dir");
    let held_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .expect("open driver lock");
    flock(&held_lock, FlockOperation::LockExclusive).expect("hold driver lock");

    let locked_ref = format!("sha256:{}", digest('1'));
    let locked_id = format!("sha256:{}", digest('2'));
    let locked_transports = transports_with_oci(
        dir.path(),
        &format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo {locked_id}; exit 0; fi\nexit 2\n"
        ),
    );
    let code = cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "environment",
            "use",
            locked_ref.as_str(),
            "--mission-id",
            mission_id.as_str(),
            "--reason",
            "locked image",
            "--repo",
            dir.path().to_str().unwrap(),
            "--json",
        ])
        .unwrap(),
        locked_transports,
    )
    .await
    .unwrap();
    assert_eq!(code, std::process::ExitCode::FAILURE);
    flock(&held_lock, FlockOperation::Unlock).expect("release driver lock");
    let after_locked_cli = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(after_locked_cli.image_id, initial.image_id);
    assert!(after_locked_cli.environment_history.is_empty());

    let planner = team.planning_assignment.clone();
    let prompt_hash = digest('d');
    let effect_id = EffectId::for_role_turn(
        &mission_id,
        &planner,
        team.revision,
        None,
        1,
        1,
        &prompt_hash,
    );
    common::fault_append_events(
        dir.path(),
        &mission_id,
        after_tag.head,
        &[NewEvent::new(MissionEvent::RoleTurnRequested {
            role_instance: RoleInstanceId::new(planner.as_str()).unwrap(),
            team_revision: team.revision,
            task_id: None,
            assertion_ids: Vec::new(),
            attempt_no: 1,
            effect_id,
            prompt_template: role_prompt_template(OutputSemantics::ProposesPlan),
            prompt_hash,
            base_sha: after_tag.deliverable_head().to_string(),
            environment_digest: after_tag.environment_digest().to_string(),
            instrument_identity: after_tag
                .role_instrument_identity_for_revision(&planner, team.revision)
                .expect("role instrument identity"),
            dependency_refs: Vec::new(),
            report_refs: Vec::new(),
            assignment_epoch: 1,
            message_boundary: after_tag.head,
            presented_messages: Vec::new(),
            workspace_preparation: WorkspacePreparation::Preserve,
            requested_at_ms: 12,
            deadline_ms: 30,
            budget_deadline_ms: 30,
        })],
        12,
    )
    .await;
    let inflight = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(inflight.inflight.len(), 1);

    let race_ref = format!("sha256:{}", digest('e'));
    let race_id = format!("sha256:{}", digest('f'));
    let transports = transports_with_oci(
        dir.path(),
        &format!(
            "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo {race_id}; exit 0; fi\nexit 2\n"
        ),
    );
    let code = cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "environment",
            "use",
            race_ref.as_str(),
            "--mission-id",
            mission_id.as_str(),
            "--reason",
            "race image",
            "--repo",
            dir.path().to_str().unwrap(),
            "--json",
        ])
        .unwrap(),
        transports,
    )
    .await
    .unwrap();
    assert_eq!(code, std::process::ExitCode::FAILURE);
    let after_cli_race = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(after_cli_race.image_id, initial.image_id);
    assert!(after_cli_race.environment_history.is_empty());

    common::fault_append_events(
        dir.path(),
        &mission_id,
        after_cli_race.head,
        &[NewEvent::new(environment_event(
            race_ref,
            race_id,
            team_revision,
        ))],
        13,
    )
    .await;
    let after_race = h.engine.store().require_state(&mission_id).await.unwrap();
    assert_eq!(after_race.image_id, initial.image_id);
    assert!(after_race.environment_history.is_empty());
}
