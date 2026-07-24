//! Production engine reconciliation of disposable conversation resources.

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::{approve_plan, harness, harness_with_type, proposal, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::MissionDisposition;
use lionclaw::mission_type::{load_mission_type, materialize_mission_type};
use lionclaw::model::{
    ConversationLifecycle, FinishClass, Handoff, MissionId, MissionPhase, OutputSemantics,
    RoleInstanceId,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn write_cli_test_mission_type(root: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::create_dir_all(root.join("roles")).expect("roles");
    std::fs::create_dir_all(root.join("oracles")).expect("oracles");
    std::fs::write(
        root.join("playbook.md"),
        "Conversation resource lifecycle fixture.\n",
    )
    .expect("playbook");
    std::fs::write(
        root.join("mission.toml"),
        r#"[mission-type]
name = "conversation-resource-lifecycle"
stop = "verified"
image = "localhost/lionclaw-runtime-dev:v1"

[execution]
default-timeout-secs = 60
max-task-time-secs = 120
extension-step-secs = 30
auto-continue-candidate = true
auto-continue-proof = true

[team]
planning-assignment = "strategist"

[ceilings]
network = true
writes = true
install = true
"#,
    )
    .expect("manifest");
    std::fs::write(
        root.join("roles/strategist.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nPlan the change.\n",
    )
    .expect("strategist role");
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nImplement the change.\n",
    )
    .expect("implementer role");
    std::fs::write(
        root.join("roles/reviewer.md"),
        "---\noutput: emits-verdict\nruntime: codex\n---\nReview the change.\n",
    )
    .expect("reviewer role");
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, "#!/bin/sh\nexit 0\n").expect("oracle");
    std::fs::set_permissions(&oracle, std::fs::Permissions::from_mode(0o755))
        .expect("oracle permissions");
}

fn task_work(role_root: &std::path::Path) -> PathBuf {
    role_root
        .parent()
        .and_then(std::path::Path::parent)
        .expect("mission resource root")
        .join("tasks/fix/work")
}

async fn awaiting_lead_with_resources(
    repo: &std::path::Path,
) -> (common::TestHarness, MissionId, PathBuf) {
    let mission_type_source = repo.join("test-mission-type");
    write_cli_test_mission_type(&mission_type_source);
    let mission_type = load_mission_type(&mission_type_source, &AuthorityCeiling::default())
        .expect("load test mission type");
    let observed_root = Arc::new(Mutex::new(None::<PathBuf>));
    let runner_root = observed_root.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("validated"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| lionclaw::model::ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "validated".into(),
            });
        }
        let root = request
            .state_dir
            .join("missions")
            .join(request.mission_id.as_str())
            .join("conversations")
            .join(request.role.id.as_str());
        std::fs::create_dir_all(root.join("scratch")).expect("create conversation scratch");
        std::fs::create_dir_all(task_work(&root)).expect("create retained work");
        std::fs::create_dir_all(root.join("runtime")).expect("create retained runtime state");
        std::fs::write(root.join("scratch/build-output"), "discard\n")
            .expect("write disposable build output");
        std::fs::write(task_work(&root).join("retained"), "preserve\n")
            .expect("write retained workspace evidence");
        std::fs::write(root.join("runtime/native-session"), "preserve\n")
            .expect("write retained runtime state");
        std::fs::write(root.join("observer.index"), "preserve\n")
            .expect("write retained observer evidence");
        *runner_root.lock().expect("lock") = Some(root);

        Ok(RoleTurnOutcome {
            handoff: None,
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: "Which behavior should I preserve?".into(),
        })
    }));
    let harness = harness_with_type(repo, mission_type, runner, MockOracleRunner::exiting(0)).await;
    let mission_id = harness
        .engine
        .create_mission(
            repo.to_str().expect("utf8 repo path"),
            "retain a question until the lead answers",
            BASE_SHA,
        )
        .await
        .expect("create mission");
    std::fs::create_dir_all(repo.join(".lionclaw/missions").join(mission_id.as_str()))
        .expect("mission resource directory");
    materialize_mission_type(
        &mission_type_source,
        &repo
            .join(".lionclaw/missions")
            .join(mission_id.as_str())
            .join("mission-type"),
        &AuthorityCeiling::default(),
    )
    .expect("materialize mission type snapshot");
    harness
        .engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose plan");
    approve_plan(&harness.engine, &mission_id).await;
    let checkpoint = harness
        .engine
        .advance(&mission_id)
        .await
        .expect("advance to question");
    assert_eq!(checkpoint.disposition, MissionDisposition::AwaitingLead);
    let root = observed_root
        .lock()
        .expect("lock")
        .clone()
        .expect("role ran");
    (harness, mission_id, root)
}

#[tokio::test]
async fn successful_settlement_removes_only_the_exact_conversation_scratch() {
    let repo = tempfile::tempdir().expect("tempdir");
    let observed_root = Arc::new(Mutex::new(None::<PathBuf>));
    let runner_root = observed_root.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("validated"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| lionclaw::model::ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "validated".into(),
            });
        }
        let root = request
            .state_dir
            .join("missions")
            .join(request.mission_id.as_str())
            .join("conversations")
            .join(request.role.id.as_str());
        std::fs::create_dir_all(root.join("scratch")).expect("create conversation scratch");
        std::fs::create_dir_all(root.join("runtime")).expect("create retained runtime state");
        std::fs::write(root.join("scratch/build-output"), "discard\n")
            .expect("write disposable build output");
        std::fs::write(root.join("runtime/native-session"), "preserve\n")
            .expect("write retained runtime state");
        *runner_root.lock().expect("lock") = Some(root);

        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: lionclaw::model::PayloadRef::inline("completed"),
                request_attention: false,
            }),
            artifact: Some(CapturedArtifact::for_testing(
                request.base_sha.clone(),
                HEAD_SHA,
            )),
            runtime_configuration: Default::default(),
            final_response: "completed".into(),
        })
    }));
    let harness = harness(repo.path(), runner, MockOracleRunner::exiting(0)).await;
    let mission_id = harness
        .engine
        .create_mission(
            repo.path().to_str().expect("utf8 repo path"),
            "clean settled conversation scratch",
            BASE_SHA,
        )
        .await
        .expect("create mission");
    harness
        .engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose plan");
    approve_plan(&harness.engine, &mission_id).await;

    let settled = harness.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(
        settled.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    assert!(settled.state.inflight.is_empty());
    assert!(settled.state.conversations.values().all(|conversation| {
        matches!(
            conversation.lifecycle,
            ConversationLifecycle::Completed | ConversationLifecycle::Retired
        )
    }));

    let root = observed_root
        .lock()
        .expect("lock")
        .clone()
        .expect("role ran");
    assert!(!root.join("scratch").exists());
    assert!(task_work(&root).is_dir());
    assert_eq!(
        std::fs::read_to_string(root.join("runtime/native-session")).unwrap(),
        "preserve\n"
    );

    harness
        .engine
        .advance(&mission_id)
        .await
        .expect("idempotent terminal advance");
    assert!(!root.join("scratch").exists());
    assert!(task_work(&root).is_dir());
    assert!(root.join("runtime/native-session").is_file());
}

#[tokio::test]
async fn aborting_an_idle_question_reconciles_only_disposable_scratch() {
    let repo = tempfile::tempdir().expect("tempdir");
    let (harness, mission_id, root) = awaiting_lead_with_resources(repo.path()).await;

    harness
        .engine
        .abort(&mission_id, "the lead ended this mission")
        .await
        .expect("abort and reconcile");

    let state = harness.engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(state.phase, MissionPhase::Aborted { .. }));
    assert!(!root.join("scratch").exists());
    assert!(task_work(&root).join("retained").is_file());
    assert!(root.join("runtime/native-session").is_file());
    assert!(root.join("observer.index").is_file());
}

#[tokio::test]
async fn abort_reloads_state_after_waiting_for_the_driver_lock() {
    use rustix::fs::{flock, FlockOperation};
    use std::fs::OpenOptions;
    use std::time::Duration;

    let repo = tempfile::tempdir().expect("tempdir");
    let (harness, mission_id, root) = awaiting_lead_with_resources(repo.path()).await;
    let lock_path = repo
        .path()
        .join(".lionclaw/missions")
        .join(mission_id.as_str())
        .join("driver.lock");
    let held_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .expect("open driver lock");
    flock(&held_lock, FlockOperation::LockExclusive).expect("hold driver lock");

    let store = harness.engine.store().clone();
    let observing_store = store.clone();
    let abort_mission = mission_id.clone();
    let abort = tokio::spawn(async move {
        lionclaw::engine::record_abort(
            &store,
            42,
            &abort_mission,
            "abort after the driver's final fold",
        )
        .await
    });
    let aborted = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = observing_store
                .require_state(&mission_id)
                .await
                .expect("observe state");
            if state.phase.is_terminal() {
                break state;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort event must commit before lock acquisition");
    assert!(matches!(aborted.phase, MissionPhase::Aborted { .. }));
    assert!(
        !abort.is_finished(),
        "cleanup must wait for driver ownership"
    );
    assert!(root.join("scratch/build-output").is_file());

    flock(&held_lock, FlockOperation::Unlock).expect("release driver lock");
    abort
        .await
        .expect("join abort")
        .expect("reconcile after lock release");

    assert!(!root.join("scratch").exists());
    assert!(task_work(&root).join("retained").is_file());
    assert!(root.join("runtime/native-session").is_file());
    assert!(root.join("observer.index").is_file());
    let reconciled = observing_store
        .require_state(&mission_id)
        .await
        .expect("reconciled state");
    assert_eq!(
        reconciled.head, aborted.head,
        "cleanup must append no events"
    );
}

#[tokio::test]
async fn accepting_a_replacement_preserves_the_team_owned_conversation() {
    let repo = tempfile::tempdir().expect("tempdir");
    let (harness, mission_id, root) = awaiting_lead_with_resources(repo.path()).await;
    let old_conversation = RoleInstanceId::new(
        root.file_name()
            .and_then(|name| name.to_str())
            .expect("conversation directory name"),
    )
    .expect("conversation id");
    let mut replacement = simple_plan();
    replacement.tasks[0].id = lionclaw::model::TaskId::new("replacement").expect("task id");
    replacement.tasks[0].body = "implement the corrected assignment".into();

    harness
        .engine
        .propose_plan(&mission_id, proposal(1, replacement))
        .await
        .expect("propose replacement");
    approve_plan(&harness.engine, &mission_id).await;

    let state = harness.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(
        state.conversations[&old_conversation].lifecycle,
        ConversationLifecycle::AwaitingLead
    );
    assert!(root.join("scratch/build-output").is_file());
    assert!(task_work(&root).join("retained").is_file());
    assert!(root.join("runtime/native-session").is_file());
    assert!(root.join("observer.index").is_file());
}

#[tokio::test]
async fn terminal_advance_retries_a_failed_abort_cleanup_without_rerunning_the_role() {
    use std::os::unix::fs::symlink;

    let repo = tempfile::tempdir().expect("tempdir");
    let (harness, mission_id, root) = awaiting_lead_with_resources(repo.path()).await;
    let retained = repo.path().join("retained-conversation");
    let outside = repo.path().join("outside");
    std::fs::create_dir(&outside).expect("outside");
    std::fs::write(outside.join("sentinel"), "preserve\n").expect("outside sentinel");
    std::fs::rename(&root, &retained).expect("hold the real conversation tree");
    symlink(&outside, &root).expect("install unsafe conversation parent");

    let error = harness
        .engine
        .abort(&mission_id, "abort before cleanup can run")
        .await
        .expect_err("unsafe cleanup must fail closed after durable abort");
    assert!(error.to_string().contains("was aborted durably"));
    let state = harness.engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(state.phase, MissionPhase::Aborted { .. }));
    let aborted_head = state.head;
    assert!(outside.join("sentinel").is_file());

    std::fs::remove_file(&root).expect("remove unsafe link");
    std::fs::rename(&retained, &root).expect("restore conversation tree");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args([
            "mission",
            "advance",
            mission_id.as_str(),
            "--wait",
            "--repo",
        ])
        .arg(repo.path())
        .output()
        .expect("run terminal advance through the production CLI");
    assert!(
        output.status.success(),
        "terminal advance failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(!root.join("scratch").exists());
    assert!(task_work(&root).join("retained").is_file());
    assert!(root.join("runtime/native-session").is_file());
    assert!(root.join("observer.index").is_file());
    assert!(outside.join("sentinel").is_file());
    let retried = harness
        .engine
        .load_state(&mission_id)
        .await
        .expect("state after cleanup retry");
    assert_eq!(retried.head, aborted_head, "cleanup must append no events");
    assert!(matches!(retried.phase, MissionPhase::Aborted { .. }));
}
