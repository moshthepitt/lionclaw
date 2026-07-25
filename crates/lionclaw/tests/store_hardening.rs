//! The event log is the only durable effect queue. There is no second ledger
//! to lease, reseed, or reconcile.

mod common;

use common::{
    advisory_plan, approve_plan, fault_append_events, harness, proposal, simple_plan, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::model::{
    fold, ControlAction, EffectId, Handoff, MissionEvent, MissionState, PayloadRef, RoleInstanceId,
    RolePromptTemplate, RoleTurnSuccess, RuntimeConfigurationEvidence, TaskId, TaskStatus,
    TypedFailure, WorkspacePreparation, REDUCER_VERSION,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

const PROMPT_HASH: &str = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

fn role_request(
    state: &MissionState,
    role_instance: RoleInstanceId,
    task_id: Option<TaskId>,
    assertion_ids: Vec<lionclaw::model::AssertionId>,
) -> NewEvent {
    let team = state.team.as_ref().expect("active team");
    let role = team.role(&role_instance).expect("assigned role");
    let previous = task_id
        .as_ref()
        .and_then(|task_id| state.tasks.get(task_id));
    let assignment = lionclaw::model::resolve_role_assignment(
        &role_instance,
        team.revision,
        lionclaw::model::RoleAssignmentContext {
            previous,
            required_base: &state.current_sha,
            dependency_refs: &[],
            lifecycle_generation: state.revision.max(1),
            retrying_failure: task_id
                .as_ref()
                .is_some_and(|task_id| state.task_automatic_retry_remaining(task_id)),
            output: role.output,
        },
    );
    let attempt_no = previous.map_or(1, |task| task.attempts + 1);
    let effect_id = EffectId::for_role_turn(
        &state.mission_id,
        &role_instance,
        team.revision,
        task_id.as_ref(),
        attempt_no,
        assignment.generation,
        PROMPT_HASH,
    );
    let presented_messages =
        state
            .conversations
            .get(&role_instance)
            .map_or_else(Vec::new, |conversation| {
                conversation
                    .queued
                    .iter()
                    .filter(|message| {
                        message.sequence_no <= state.head
                            && message.marker != lionclaw::model::DeliveryMarker::Undeliverable
                    })
                    .map(|message| message.sequence_no)
                    .collect()
            });
    NewEvent::new(MissionEvent::RoleTurnRequested {
        role_instance,
        team_revision: team.revision,
        task_id,
        assertion_ids,
        attempt_no,
        effect_id,
        prompt_template: lionclaw::model::role_prompt_template(role.output),
        prompt_hash: PROMPT_HASH.into(),
        base_sha: assignment.base_sha,
        dependency_refs: assignment.dependency_refs,
        assignment_epoch: assignment.generation,
        message_boundary: state.head,
        presented_messages,
        workspace_preparation: assignment.workspace_preparation,
        requested_at_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    })
    .with_prompt_hash(PROMPT_HASH)
}

fn writer_request(state: &MissionState, task: &str) -> NewEvent {
    let task_id = TaskId::new(task).expect("task");
    let targets = state
        .plan
        .as_ref()
        .and_then(|plan| plan.tasks.iter().find(|task| task.id == task_id))
        .expect("planned task")
        .targets
        .clone();
    role_request(
        state,
        RoleInstanceId::new("implementer").unwrap(),
        Some(task_id),
        targets,
    )
}

fn reviewer_request(state: &MissionState, assertion: &str) -> NewEvent {
    role_request(
        state,
        RoleInstanceId::new("reviewer").unwrap(),
        None,
        vec![lionclaw::model::AssertionId::new(assertion).unwrap()],
    )
}

fn effect_id(event: &NewEvent) -> EffectId {
    let MissionEvent::RoleTurnRequested { effect_id, .. } = &event.event else {
        panic!("expected role request")
    };
    effect_id.clone()
}

fn success(handoff: Option<Handoff>) -> NewEvent {
    NewEvent::new(MissionEvent::RoleTurnCompleted {
        effect_id: EffectId::for_parts(&["replaced-by-caller"]),
        outcome: Ok(RoleTurnSuccess {
            handoff,
            artifact: None,
            final_response: PayloadRef::inline("role response"),
            runtime_configuration: RuntimeConfigurationEvidence::default(),
            prepared_inputs: Vec::new(),
            runtime_usage: Default::default(),
        }),
    })
}

fn completion(effect_id: EffectId, handoff: Option<Handoff>) -> NewEvent {
    let mut event = success(handoff);
    let MissionEvent::RoleTurnCompleted {
        effect_id: event_id,
        ..
    } = &mut event.event
    else {
        unreachable!()
    };
    *event_id = effect_id;
    event
}

fn work_handoff(report: &str) -> Handoff {
    Handoff::Work {
        done: true,
        report: PayloadRef::inline(report),
        request_attention: false,
    }
}

fn assert_only_head_advanced(before: &MissionState, after: &MissionState) {
    let mut normalized = after.clone();
    normalized.head = before.head;
    assert_eq!(&normalized, before);
}

async fn mutate_snapshot_state(
    repo: &std::path::Path,
    mission_id: &lionclaw::model::MissionId,
    mutate: impl FnOnce(&mut MissionState),
) {
    let options = SqliteConnectOptions::new()
        .filename(repo.join(".lionclaw/mission.db"))
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open snapshot database");
    let json: String =
        sqlx::query_scalar("SELECT state_json FROM mission_snapshots WHERE mission_id = ?1")
            .bind(mission_id.as_str())
            .fetch_one(&pool)
            .await
            .expect("current snapshot");
    let mut state: MissionState = serde_json::from_str(&json).expect("snapshot state");
    mutate(&mut state);
    sqlx::query("UPDATE mission_snapshots SET state_json = ?1 WHERE mission_id = ?2")
        .bind(serde_json::to_string(&state).unwrap())
        .bind(mission_id.as_str())
        .execute(&pool)
        .await
        .expect("replace snapshot state");
    pool.close().await;
}

async fn set_snapshot_reducer(
    repo: &std::path::Path,
    mission_id: &lionclaw::model::MissionId,
    reducer: u32,
) {
    let options = SqliteConnectOptions::new()
        .filename(repo.join(".lionclaw/mission.db"))
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open snapshot database");
    sqlx::query("UPDATE mission_snapshots SET reducer_version = ?1 WHERE mission_id = ?2")
        .bind(i64::from(reducer))
        .bind(mission_id.as_str())
        .execute(&pool)
        .await
        .expect("replace snapshot reducer");
    pool.close().await;
}

async fn approved_simple_mission(
    dir: &tempfile::TempDir,
) -> (common::TestHarness, lionclaw::model::MissionId) {
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    (h, mission_id)
}

#[tokio::test]
async fn role_request_tuple_is_validated_before_durable_fold_mutation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = approved_simple_mission(&dir).await;

    for corrupt in 0..6 {
        let before = h.engine.load_state(&mission_id).await.expect("prestate");
        let mut request = writer_request(&before, "fix");
        let MissionEvent::RoleTurnRequested {
            role_instance,
            team_revision,
            effect_id,
            prompt_template,
            assignment_epoch,
            message_boundary,
            presented_messages,
            ..
        } = &mut request.event
        else {
            unreachable!()
        };
        match corrupt {
            0 => *role_instance = RoleInstanceId::new("ghost").unwrap(),
            1 => *team_revision += 100,
            2 => *effect_id = EffectId::for_parts(&["forged"]),
            3 => *prompt_template = RolePromptTemplate::Judgment,
            4 => *assignment_epoch += 1,
            5 => {
                *message_boundary += 1;
                presented_messages.push(*message_boundary);
            }
            _ => unreachable!(),
        }
        fault_append_events(
            dir.path(),
            &mission_id,
            before.head,
            &[request],
            corrupt + 10,
        )
        .await;
        let after = h.engine.load_state(&mission_id).await.expect("state");
        assert_only_head_advanced(&before, &after);
    }

    let before = h.engine.load_state(&mission_id).await.expect("prestate");
    let request = writer_request(&before, "fix");
    let id = effect_id(&request);
    fault_append_events(dir.path(), &mission_id, before.head, &[request], 20).await;
    let accepted = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(
        accepted.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Running
    );
    assert!(accepted.inflight.contains_key(&id));
    assert_eq!(
        accepted
            .conversations
            .get(&RoleInstanceId::new("implementer").unwrap())
            .and_then(|conversation| conversation.active_delivery.as_ref())
            .map(|delivery| &delivery.effect_id),
        Some(&id)
    );
}

#[tokio::test]
async fn workspace_recreation_authority_is_store_replay_and_snapshot_tail_safe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = approved_simple_mission(&dir).await;
    let initial = h.engine.load_state(&mission_id).await.expect("initial");
    let first_request = writer_request(&initial, "fix");
    let first_effect = effect_id(&first_request);
    fault_append_events(
        dir.path(),
        &mission_id,
        initial.head,
        &[
            first_request,
            completion(
                first_effect.clone(),
                Some(work_handoff("initial workspace")),
            ),
        ],
        49,
    )
    .await;
    let established = h.engine.load_state(&mission_id).await.expect("established");
    assert!(established.tasks[&TaskId::new("fix").unwrap()]
        .workspace_provenance
        .is_some());
    let failed_request = writer_request(&established, "fix");
    let failed_effect = effect_id(&failed_request);
    fault_append_events(
        dir.path(),
        &mission_id,
        established.head,
        &[
            failed_request,
            NewEvent::new(MissionEvent::RoleTurnCompleted {
                effect_id: failed_effect.clone(),
                outcome: Err(TypedFailure::permanent(
                    "workspace.history",
                    "retained checkout diverged",
                )),
            }),
        ],
        50,
    )
    .await;

    lionclaw::engine::record_control(
        h.engine.store(),
        51,
        &mission_id,
        &failed_effect,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::RecreateWorkspace,
        },
        "archive the divergent checkout",
    )
    .await
    .expect("continue");
    let continued = h.engine.load_state(&mission_id).await.expect("continued");
    let retry_request = writer_request(&continued, "fix");
    let retry_effect = effect_id(&retry_request);
    let MissionEvent::RoleTurnRequested {
        assignment_epoch,
        workspace_preparation,
        ..
    } = &retry_request.event
    else {
        unreachable!()
    };
    assert_eq!(
        workspace_preparation,
        &WorkspacePreparation::ArchiveAndReset {
            parked_effect_id: failed_effect.clone()
        }
    );
    let assignment_epoch = *assignment_epoch;
    fault_append_events(
        dir.path(),
        &mission_id,
        continued.head,
        &[
            retry_request,
            completion(
                retry_effect.clone(),
                Some(work_handoff("recreated workspace")),
            ),
        ],
        52,
    )
    .await;

    let live = h.engine.load_state(&mission_id).await.expect("live");
    let provenance = live.tasks[&TaskId::new("fix").unwrap()]
        .workspace_provenance
        .as_ref()
        .expect("workspace provenance");
    assert_eq!(provenance.effect_id, retry_effect);
    assert_eq!(provenance.assignment_epoch, assignment_epoch);
    assert_eq!(provenance.archived_effect_id.as_ref(), Some(&failed_effect));
    let replayed = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(live, replayed);
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 53)
        .await
        .expect("rebuild");
    assert_eq!(live, rebuilt);
}

#[tokio::test]
async fn active_writer_snapshot_cannot_forge_a_missing_preparation_event() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = approved_simple_mission(&dir).await;
    let initial = h.engine.load_state(&mission_id).await.expect("initial");
    let request = writer_request(&initial, "fix");
    let id = effect_id(&request);
    let MissionEvent::RoleTurnRequested {
        base_sha,
        assignment_epoch,
        ..
    } = &request.event
    else {
        unreachable!()
    };
    let base_sha = base_sha.clone();
    let assignment_epoch = *assignment_epoch;
    fault_append_events(dir.path(), &mission_id, initial.head, &[request], 70).await;
    let requested = h.engine.load_state(&mission_id).await.expect("requested");
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 71)
        .await
        .expect("snapshot request");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&TaskId::new("fix").unwrap())
            .unwrap()
            .workspace_provenance = Some(lionclaw::model::TaskWorkspaceProvenance {
            effect_id: EffectId::for_parts(&["forged-preparation"]),
            base_sha: "forged-base".into(),
            assignment_epoch: assignment_epoch + 10,
            archived_effect_id: None,
        });
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[completion(id.clone(), Some(work_handoff("complete")))],
        72,
    )
    .await;

    let replayed = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    let snapshotted = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshotted, replayed);
    let provenance = replayed.tasks[&TaskId::new("fix").unwrap()]
        .workspace_provenance
        .as_ref()
        .expect("canonical preparation");
    assert_eq!(provenance.effect_id, id);
    assert_eq!(provenance.base_sha, base_sha);
    assert_eq!(provenance.assignment_epoch, assignment_epoch);
}

#[tokio::test]
async fn forged_missing_verdict_agrees_across_live_replay_and_snapshot_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .expect("create");
    let mut plan = advisory_plan();
    plan.assertions[0].oracle = Some(lionclaw::model::OracleName::new("cargo-test").unwrap());
    plan.requirements[0].disposition = lionclaw::model::RequirementDisposition::ConfinedProvable {
        assertion_ids: vec![lionclaw::model::AssertionId::new("STYLE-OK").unwrap()],
    };
    h.engine
        .propose_plan(&mission_id, proposal(0, plan))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let initial = h.engine.load_state(&mission_id).await.expect("initial");
    let writer = writer_request(&initial, "write");
    let writer_effect = effect_id(&writer);
    fault_append_events(
        dir.path(),
        &mission_id,
        initial.head,
        &[
            writer,
            completion(writer_effect, Some(work_handoff("writer complete"))),
        ],
        40,
    )
    .await;
    let after_writer = h.engine.load_state(&mission_id).await.expect("writer");
    let verdict = reviewer_request(&after_writer, "STYLE-OK");
    let verdict_effect = effect_id(&verdict);
    fault_append_events(dir.path(), &mission_id, after_writer.head, &[verdict], 41).await;
    let requested = h.engine.load_state(&mission_id).await.expect("request");
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 42)
        .await
        .expect("snapshot request");
    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[completion(verdict_effect.clone(), None)],
        43,
    )
    .await;

    let live = h.engine.load_state(&mission_id).await.expect("live");
    let replayed = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    let snapshotted = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live, replayed);
    assert_eq!(live, snapshotted);
    let failure = live
        .role_attempt_receipts
        .get(&verdict_effect)
        .and_then(lionclaw::model::RoleAttemptReceipt::failure)
        .expect("missing verdict fails");
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.contract"));

    set_snapshot_reducer(dir.path(), &mission_id, REDUCER_VERSION - 1).await;
    let rebuilt_from_events = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebuilt_from_events, replayed);
}

#[tokio::test]
async fn unfinished_request_is_rebuilt_from_the_log_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = approved_simple_mission(&dir).await;
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let request = writer_request(&state, "fix");
    let id = effect_id(&request);
    fault_append_events(dir.path(), &mission_id, state.head, &[request], 1).await;

    let before = h.engine.load_state(&mission_id).await.expect("state");
    assert!(before.inflight.contains_key(&id));
    assert!(before.active_role_conversation(&id).is_ok());
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 2)
        .await
        .expect("rebuild");
    assert_eq!(rebuilt, before);

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open schema");
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'mission_effects'",
    )
    .fetch_all(&database)
    .await
    .expect("schema query");
    assert!(tables.is_empty(), "parallel effect ledger must not exist");
}

#[tokio::test]
async fn an_old_event_schema_is_refused_instead_of_accepted_by_accident() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (h, mission_id) = approved_simple_mission(&dir).await;
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let mut old = NewEvent::new(MissionEvent::MissionAborted {
        reason: "old writer".to_string(),
    });
    old.stamps.schema_version = lionclaw::model::SCHEMA_VERSION - 1;
    fault_append_events(dir.path(), &mission_id, state.head, &[old], 2).await;

    let error = h
        .engine
        .store()
        .load(&mission_id)
        .await
        .expect_err("reader must reject old schema");
    assert!(
        error.to_string().contains("unsupported schema version"),
        "got: {error:#}"
    );

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open database");
    sqlx::query(
        "UPDATE mission_events SET schema_version = ?1 \
         WHERE mission_id = ?2 AND sequence_no = ?3",
    )
    .bind(i64::from(lionclaw::model::SCHEMA_VERSION))
    .bind(mission_id.as_str())
    .bind((state.head + 1) as i64)
    .execute(&database)
    .await
    .expect("corrupt redundant stamp");
    let error = h
        .engine
        .store()
        .load(&mission_id)
        .await
        .expect_err("conflicting stamps must be rejected");
    assert!(
        error.to_string().contains("conflicting schema stamps"),
        "got: {error:#}"
    );
}
