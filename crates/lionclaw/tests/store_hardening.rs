//! The event log is the only durable effect queue. There is no second ledger
//! to lease, reseed, or reconcile.

mod common;

use common::{
    advisory_plan, approve_plan, fault_append_events, harness, proposal, simple_plan, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::model::{
    apply, fold, ControlAction, ConversationId, ConversationLifecycle, ConversationRecipient,
    Handoff, MissionEvent, MissionState, OutputSemantics, ParkedEffect, PayloadRef, RoleName,
    RoleRunRequestIdentity, RoleRunSuccess, RoleTurnObservation, RuntimeConfigurationEvidence,
    TaskId, TaskNamespace, TaskStatus, TypedFailure, REDUCER_VERSION,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

const PROMPT_HASH: &str = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

fn role_request(state: &MissionState) -> NewEvent {
    let task_id = TaskId::new("fix").unwrap();
    let role = RoleName::new("implementer").unwrap();
    let assignment = lionclaw::model::resolve_role_assignment(
        &state.mission_id,
        TaskNamespace::Execution,
        &task_id,
        &role,
        lionclaw::model::RoleAssignmentContext {
            previous: state.tasks.get(&task_id),
            required_base: BASE_SHA,
            lifecycle_generation: state.role_lifecycle_generation(TaskNamespace::Execution),
            retrying_failure: state
                .task_automatic_retry_remaining(TaskNamespace::Execution, &task_id),
            output: OutputSemantics::ProducesArtifact,
        },
    );
    let attempt_no = state
        .tasks
        .get(&task_id)
        .map_or(1, |task| task.attempts + 1);
    let effect_id = lionclaw::model::EffectId::for_role_request(
        TaskNamespace::Execution,
        &state.mission_id,
        &task_id,
        attempt_no,
        assignment.generation,
        PROMPT_HASH,
    );
    let presented_messages = state
        .conversations
        .get(&assignment.conversation_id)
        .map_or_else(Vec::new, |conversation| {
            conversation
                .queued
                .iter()
                .map(|message| message.sequence_no)
                .collect()
        });
    NewEvent::new(MissionEvent::RoleRunRequested {
        conversation_id: assignment.conversation_id,
        namespace: TaskNamespace::Execution,
        task_id,
        attempt_no,
        effect_id,
        role,
        output: OutputSemantics::ProducesArtifact,
        runtime: "codex".into(),
        prompt_template: lionclaw::model::RolePromptTemplate::Execution,
        prompt_hash: PROMPT_HASH.into(),
        base_sha: assignment.base_sha,
        assignment_epoch: assignment.generation,
        message_boundary: state.head,
        presented_messages,
        workspace_preparation: assignment.workspace_preparation,
        requested_at_ms: 0,
        not_before_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    })
    .with_prompt_hash(PROMPT_HASH)
}

fn execution_role_request(
    state: &MissionState,
    task_id: TaskId,
    role: RoleName,
    output: OutputSemantics,
) -> NewEvent {
    let assignment = lionclaw::model::resolve_role_assignment(
        &state.mission_id,
        TaskNamespace::Execution,
        &task_id,
        &role,
        lionclaw::model::RoleAssignmentContext {
            previous: state.tasks.get(&task_id),
            required_base: &state.current_sha,
            lifecycle_generation: state.role_lifecycle_generation(TaskNamespace::Execution),
            retrying_failure: state
                .task_automatic_retry_remaining(TaskNamespace::Execution, &task_id),
            output,
        },
    );
    let attempt_no = state
        .tasks
        .get(&task_id)
        .map_or(1, |task| task.attempts + 1);
    let effect_id = lionclaw::model::EffectId::for_role_request(
        TaskNamespace::Execution,
        &state.mission_id,
        &task_id,
        attempt_no,
        assignment.generation,
        PROMPT_HASH,
    );
    NewEvent::new(MissionEvent::RoleRunRequested {
        conversation_id: assignment.conversation_id,
        namespace: TaskNamespace::Execution,
        task_id,
        attempt_no,
        effect_id,
        role,
        output,
        runtime: "codex".into(),
        prompt_template: lionclaw::model::RolePromptTemplate::Execution,
        prompt_hash: PROMPT_HASH.into(),
        base_sha: assignment.base_sha,
        assignment_epoch: assignment.generation,
        message_boundary: state.head,
        presented_messages: vec![],
        workspace_preparation: assignment.workspace_preparation,
        requested_at_ms: 0,
        not_before_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    })
    .with_prompt_hash(PROMPT_HASH)
}

fn assert_only_head_advanced(before: &MissionState, after: &MissionState) {
    let mut normalized = after.clone();
    normalized.head = before.head;
    assert_eq!(&normalized, before);
}

fn request_identity(event: &MissionEvent) -> RoleRunRequestIdentity {
    let MissionEvent::RoleRunRequested {
        conversation_id,
        namespace,
        task_id,
        attempt_no,
        role,
        output,
        runtime,
        prompt_template,
        prompt_hash,
        base_sha,
        assignment_epoch,
        message_boundary,
        presented_messages,
        workspace_preparation,
        ..
    } = event
    else {
        panic!("expected role request")
    };
    RoleRunRequestIdentity {
        conversation_id: conversation_id.clone(),
        namespace: *namespace,
        task_id: task_id.clone(),
        attempt_no: *attempt_no,
        assignment_epoch: *assignment_epoch,
        role: role.clone(),
        output: *output,
        runtime: runtime.clone(),
        prompt_template: *prompt_template,
        prompt_hash: prompt_hash.clone(),
        base_sha: base_sha.clone(),
        workspace_preparation: workspace_preparation.clone(),
        message_boundary: *message_boundary,
        presented_messages: presented_messages.clone(),
    }
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

#[tokio::test]
async fn role_request_tuple_is_validated_before_durable_fold_mutation() {
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
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    for corrupt in 0..4 {
        let before = h.engine.load_state(&mission_id).await.expect("prestate");
        let mut request = role_request(&before);
        let MissionEvent::RoleRunRequested {
            conversation_id,
            namespace,
            task_id,
            attempt_no,
            effect_id,
            prompt_hash,
            assignment_epoch,
            message_boundary,
            ..
        } = &mut request.event
        else {
            unreachable!()
        };
        match corrupt {
            0 => *conversation_id = ConversationId::parse("f".repeat(64)).unwrap(),
            1 => {
                *assignment_epoch += 1;
                *effect_id = lionclaw::model::EffectId::for_role_request(
                    *namespace,
                    &before.mission_id,
                    task_id,
                    *attempt_no,
                    *assignment_epoch,
                    prompt_hash,
                );
            }
            2 => *message_boundary = message_boundary.saturating_sub(1),
            3 => *message_boundary += 1,
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
        let after = h
            .engine
            .load_state(&mission_id)
            .await
            .expect("rejected state");
        assert_only_head_advanced(&before, &after);
    }

    let before = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("canonical prestate");
    let request = role_request(&before);
    let identity = request_identity(&request.event);
    let MissionEvent::RoleRunRequested { effect_id, .. } = &request.event else {
        unreachable!()
    };
    let effect_id = effect_id.clone();
    fault_append_events(dir.path(), &mission_id, before.head, &[request], 20).await;
    let accepted = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("accepted state");
    assert_eq!(
        accepted.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Running
    );
    assert!(accepted.inflight.contains_key(&effect_id));
    assert!(accepted
        .conversations
        .values()
        .any(|conversation| conversation
            .active_delivery
            .as_ref()
            .is_some_and(|delivery| delivery.effect_id == effect_id)));

    let failed_head = fault_append_events(
        dir.path(),
        &mission_id,
        accepted.head,
        &[NewEvent::new(MissionEvent::RoleRunCompleted {
            effect_id: effect_id.clone(),
            request: Box::new(identity.clone()),
            outcome: Err(TypedFailure::permanent(
                "runtime.unavailable",
                "operator must continue the parked assignment",
            )),
        })],
        21,
    )
    .await;
    let failed = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("parked prestate");
    assert_eq!(failed.head, failed_head);
    assert_eq!(
        failed.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Failed
    );
    assert_eq!(
        failed.parked_effects.get(&effect_id),
        Some(&ParkedEffect::RoleRun {
            namespace: TaskNamespace::Execution,
            task_id: TaskId::new("fix").unwrap(),
        })
    );
    let conversation = &failed.conversations[&identity.conversation_id];
    assert_eq!(conversation.lifecycle, ConversationLifecycle::Ready);
    assert!(conversation.active_delivery.is_none());
    let parked_effect_id = effect_id.clone();
    let recipient = ConversationRecipient {
        conversation_id: identity.conversation_id.clone(),
        role: conversation.role.clone(),
        namespace: conversation.namespace,
        task_id: conversation.task_id.clone(),
        assignment_epoch: conversation.assignment_epoch,
    };
    fault_append_events(
        dir.path(),
        &mission_id,
        failed.head,
        &[
            NewEvent::new(MissionEvent::MessageSent {
                recipients: vec![recipient.clone()],
                body: "first queued message".into(),
                references: vec![],
            }),
            NewEvent::new(MissionEvent::MessageSent {
                recipients: vec![recipient],
                body: "second queued message".into(),
                references: vec![],
            }),
        ],
        22,
    )
    .await;
    for corrupt in 0..5 {
        let before = h
            .engine
            .load_state(&mission_id)
            .await
            .expect("queued prestate");
        let mut request = role_request(&before);
        let MissionEvent::RoleRunRequested {
            conversation_id,
            effect_id,
            assignment_epoch,
            presented_messages,
            ..
        } = &mut request.event
        else {
            unreachable!()
        };
        *effect_id = parked_effect_id.clone();
        match corrupt {
            0 => {
                presented_messages.pop();
            }
            1 => presented_messages.push(before.head),
            2 => presented_messages.swap(0, 1),
            3 => {
                *assignment_epoch = assignment_epoch.saturating_sub(1);
            }
            4 => *conversation_id = ConversationId::parse("e".repeat(64)).unwrap(),
            _ => unreachable!(),
        }
        fault_append_events(
            dir.path(),
            &mission_id,
            before.head,
            &[request],
            23 + corrupt,
        )
        .await;
        let after = h
            .engine
            .load_state(&mission_id)
            .await
            .expect("queued rejection");
        assert_only_head_advanced(&before, &after);
    }
    let retry_prestate = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("parked rejection state");
    assert_eq!(
        retry_prestate.parked_effects.get(&effect_id),
        Some(&ParkedEffect::RoleRun {
            namespace: TaskNamespace::Execution,
            task_id: TaskId::new("fix").unwrap(),
        })
    );
    assert_eq!(
        retry_prestate.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Failed
    );
    assert_eq!(
        retry_prestate.conversations[&identity.conversation_id].lifecycle,
        ConversationLifecycle::Ready
    );
    assert!(retry_prestate.conversations[&identity.conversation_id]
        .active_delivery
        .is_none());
    assert!(retry_prestate.inflight.is_empty());

    fault_append_events(
        dir.path(),
        &mission_id,
        retry_prestate.head,
        &[NewEvent::new(MissionEvent::ControlRequested {
            effect_id: effect_id.clone(),
            action: ControlAction::Continue {
                automatic: false,
                mode: lionclaw::model::ContinueMode::Preserve,
            },
            reason: "resume the supported parked assignment".into(),
        })],
        29,
    )
    .await;
    let mut continued = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("continued prestate");
    assert!(!continued.parked_effects.contains_key(&effect_id));
    assert_eq!(
        continued.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Pending
    );
    assert_eq!(
        continued.conversations[&identity.conversation_id].lifecycle,
        ConversationLifecycle::Ready
    );
    for corrupt in 0..3 {
        let before = continued;
        let mut request = role_request(&before);
        let MissionEvent::RoleRunRequested {
            presented_messages, ..
        } = &mut request.event
        else {
            unreachable!()
        };
        match corrupt {
            0 => presented_messages.swap(0, 1),
            1 => {
                presented_messages.pop();
            }
            2 => presented_messages.push(before.head),
            _ => unreachable!(),
        }
        fault_append_events(
            dir.path(),
            &mission_id,
            before.head,
            &[request],
            30 + corrupt,
        )
        .await;
        continued = h
            .engine
            .load_state(&mission_id)
            .await
            .expect("canonical-effect tuple rejection");
        assert_only_head_advanced(&before, &continued);
    }
    assert_eq!(
        h.engine
            .store()
            .rebuild_cursors(&mission_id, 34)
            .await
            .expect("seed snapshot-tail cursor"),
        continued
    );
    let retry = role_request(&continued);
    let retry_identity = request_identity(&retry.event);
    let retry_effect = match &retry.event {
        MissionEvent::RoleRunRequested {
            effect_id,
            assignment_epoch,
            workspace_preparation,
            ..
        } => {
            assert_eq!(*assignment_epoch, 1);
            assert_eq!(
                workspace_preparation,
                &lionclaw::model::WorkspacePreparation::ResetForAssignment
            );
            effect_id.clone()
        }
        _ => unreachable!(),
    };
    fault_append_events(dir.path(), &mission_id, continued.head, &[retry], 35).await;
    let accepted = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("accepted retry");
    assert_eq!(
        accepted.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Running
    );
    assert!(accepted.inflight.contains_key(&retry_effect));
    let task = &accepted.tasks[&TaskId::new("fix").unwrap()];
    assert_eq!(task.attempts, retry_identity.attempt_no);
    let conversation = &accepted.conversations[&retry_identity.conversation_id];
    assert_eq!(
        conversation.assignment_epoch,
        retry_identity.assignment_epoch
    );
    assert_eq!(conversation.workspace_base_sha, retry_identity.base_sha);
    assert_eq!(
        conversation.lifecycle,
        lionclaw::model::ConversationLifecycle::Running
    );
    let delivery = conversation.active_delivery.as_ref().unwrap();
    assert_eq!(delivery.effect_id, retry_effect);
    assert_eq!(delivery.message_boundary, retry_identity.message_boundary);
    assert_eq!(
        delivery.presented_messages,
        retry_identity.presented_messages
    );
    assert_eq!(conversation.queued.len(), 2);

    let store = h.engine.store();
    let events = store.load(&mission_id).await.expect("events");
    let replayed = fold(events.clone()).expect("full replay");
    assert_eq!(replayed, accepted);
    let (snapshot_head, _) = store
        .snapshot_meta(&mission_id)
        .await
        .expect("snapshot metadata")
        .expect("live snapshot");
    assert!(
        snapshot_head < replayed.head,
        "snapshot load must apply a tail"
    );
    let snapshotted = store
        .load_state_snapshotted(&mission_id)
        .await
        .expect("snapshot-tail load")
        .expect("state");
    assert_eq!(snapshotted, replayed);
    let rebuilt = store
        .rebuild_cursors(&mission_id, 30)
        .await
        .expect("cursor rebuild");
    assert_eq!(rebuilt, replayed);
    for split in 1..events.len() {
        let mut cursor = fold(events[..split].to_vec()).expect("prefix fold");
        for event in &events[split..] {
            apply(&mut cursor, event);
        }
        assert_eq!(cursor, replayed, "cursor split {split}");
    }
}

#[tokio::test]
async fn workspace_recreation_authority_is_store_replay_and_snapshot_tail_safe() {
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
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let initial = h.engine.load_state(&mission_id).await.expect("initial");
    let first_request = role_request(&initial);
    let first_request_seq = initial.head + 1;
    let first_request_event = first_request.event.clone();
    let first_identity = request_identity(&first_request.event);
    let MissionEvent::RoleRunRequested {
        effect_id: first_effect,
        ..
    } = &first_request.event
    else {
        unreachable!()
    };
    let first_effect = first_effect.clone();
    fault_append_events(
        dir.path(),
        &mission_id,
        initial.head,
        &[
            first_request,
            NewEvent::new(MissionEvent::TaskWorkspacePrepared {
                task_id: first_identity.task_id.clone(),
                effect_id: first_effect.clone(),
                base_sha: first_identity.base_sha.clone(),
                assignment_epoch: first_identity.assignment_epoch,
            }),
            NewEvent::new(MissionEvent::RoleRunCompleted {
                effect_id: first_effect.clone(),
                request: Box::new(first_identity.clone()),
                outcome: Err(TypedFailure::permanent(
                    "workspace.history",
                    "retained checkout diverged",
                )),
            }),
        ],
        50,
    )
    .await;
    let parked = h.engine.load_state(&mission_id).await.expect("parked");
    assert!(parked.parked_workspace_recreation(&first_effect).is_some());

    lionclaw::engine::record_control(
        h.engine.store(),
        51,
        &mission_id,
        &first_effect,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::RecreateWorkspace,
        },
        "archive the divergent checkout",
    )
    .await
    .expect("exact parked writer may be recreated");
    assert!(lionclaw::engine::record_control(
        h.engine.store(),
        52,
        &mission_id,
        &first_effect,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::RecreateWorkspace,
        },
        "stale duplicate",
    )
    .await
    .is_err());

    let continued = h.engine.load_state(&mission_id).await.expect("continued");
    let retry_request = role_request(&continued);
    let retry_identity = request_identity(&retry_request.event);
    assert_eq!(
        retry_identity.assignment_epoch,
        first_identity.assignment_epoch
    );
    assert_eq!(
        retry_identity.workspace_preparation,
        lionclaw::model::WorkspacePreparation::ArchiveAndReset {
            parked_effect_id: first_effect.clone(),
        }
    );
    let MissionEvent::RoleRunRequested {
        effect_id: retry_effect,
        ..
    } = &retry_request.event
    else {
        unreachable!()
    };
    let retry_effect = retry_effect.clone();
    fault_append_events(
        dir.path(),
        &mission_id,
        continued.head,
        &[retry_request],
        53,
    )
    .await;
    let requested = h.engine.load_state(&mission_id).await.expect("requested");
    assert_eq!(
        h.engine
            .store()
            .rebuild_cursors(&mission_id, 54)
            .await
            .expect("snapshot pending preparation"),
        requested
    );

    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[NewEvent::new(MissionEvent::RoleRunCompleted {
            effect_id: retry_effect.clone(),
            request: Box::new(retry_identity.clone()),
            outcome: Err(TypedFailure::permanent(
                "runtime.setup",
                "failed after archival but before durable preparation",
            )),
        })],
        55,
    )
    .await;
    let retry_parked = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("retry parked");
    assert_eq!(
        retry_parked.tasks[&retry_identity.task_id]
            .pending_workspace_recreation
            .as_ref(),
        Some(&first_effect)
    );
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 56)
        .await
        .expect("snapshot parked recreation retry");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&retry_identity.task_id)
            .unwrap()
            .pending_workspace_recreation = None;
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        retry_parked.head,
        &[NewEvent::new(MissionEvent::ControlRequested {
            effect_id: retry_effect.clone(),
            action: ControlAction::Continue {
                automatic: false,
                mode: lionclaw::model::ContinueMode::Preserve,
            },
            reason: "forged downgrade against a corrupted snapshot".into(),
        })],
        57,
    )
    .await;
    let after_forged_downgrade = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        after_forged_downgrade
    );
    assert_eq!(
        after_forged_downgrade.tasks[&retry_identity.task_id]
            .pending_workspace_recreation
            .as_ref(),
        Some(&first_effect)
    );
    assert!(after_forged_downgrade
        .parked_effects
        .contains_key(&retry_effect));

    lionclaw::engine::record_control(
        h.engine.store(),
        58,
        &mission_id,
        &retry_effect,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::RecreateWorkspace,
        },
        "retry the original archive operation",
    )
    .await
    .expect("repeated recreation preserves the original archive identity");
    let continued = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("continued retry");
    let retry_request = role_request(&continued);
    let retry_identity = request_identity(&retry_request.event);
    assert_eq!(
        retry_identity.workspace_preparation,
        lionclaw::model::WorkspacePreparation::ArchiveAndReset {
            parked_effect_id: first_effect.clone(),
        }
    );
    let MissionEvent::RoleRunRequested {
        effect_id: retry_effect,
        ..
    } = &retry_request.event
    else {
        unreachable!()
    };
    let retry_effect = retry_effect.clone();
    fault_append_events(
        dir.path(),
        &mission_id,
        continued.head,
        &[retry_request],
        59,
    )
    .await;
    let requested = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("replacement retry requested");

    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[NewEvent::new(MissionEvent::TaskWorkspacePrepared {
            task_id: retry_identity.task_id.clone(),
            effect_id: retry_effect.clone(),
            base_sha: retry_identity.base_sha.clone(),
            assignment_epoch: retry_identity.assignment_epoch,
        })],
        60,
    )
    .await;
    let prepared = h.engine.load_state(&mission_id).await.expect("prepared");
    let replayed = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(prepared, replayed);
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        replayed
    );
    assert!(prepared
        .active_workspace_conversation(&retry_effect)
        .is_ok());
    assert!(
        prepared.retained_workspace_archives[&retry_identity.conversation_id]
            .contains(&first_effect)
    );

    h.engine
        .store()
        .rebuild_cursors(&mission_id, 61)
        .await
        .expect("snapshot prepared authority before stale request forgery");
    let (_, settled_request) =
        lionclaw::model::InflightEffect::from_request(&first_request_event, first_request_seq)
            .expect("first request identity");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot.inflight.clear();
        snapshot
            .inflight
            .insert(first_effect.clone(), settled_request.clone());
        let task = snapshot.tasks.get_mut(&first_identity.task_id).unwrap();
        task.status = TaskStatus::Running;
        task.attempts = first_identity.attempt_no;
        task.role_assignment = Some(lionclaw::model::TaskRoleAssignment {
            base_sha: first_identity.base_sha.clone(),
            assignment_epoch: first_identity.assignment_epoch,
        });
        task.workspace_provenance = Some(lionclaw::model::TaskWorkspaceProvenance {
            effect_id: first_effect.clone(),
            conversation_id: first_identity.conversation_id.clone(),
            base_sha: first_identity.base_sha.clone(),
            assignment_epoch: first_identity.assignment_epoch,
            archived_effect_id: None,
        });
        task.pending_workspace_recreation = None;
        let conversation = snapshot
            .conversations
            .get_mut(&first_identity.conversation_id)
            .unwrap();
        conversation.lifecycle = ConversationLifecycle::Running;
        let delivery = conversation
            .active_delivery
            .as_mut()
            .expect("current retry has active delivery");
        delivery.effect_id = first_effect.clone();
        delivery.message_boundary = first_identity.message_boundary;
        delivery
            .presented_messages
            .clone_from(&first_identity.presented_messages);
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        prepared.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id: retry_effect.clone(),
            configuration: RuntimeConfigurationEvidence {
                requested_model: Some("settled-request-forgery-tail".into()),
                ..Default::default()
            },
        })],
        62,
    )
    .await;
    let after_settled_request_forgery =
        fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        after_settled_request_forgery,
        "an authentic request with a prior durable outcome is not active snapshot authority"
    );

    h.engine
        .store()
        .rebuild_cursors(&mission_id, 63)
        .await
        .expect("snapshot prepared authority");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&retry_identity.task_id)
            .unwrap()
            .workspace_provenance
            .as_mut()
            .unwrap()
            .archived_effect_id = None;
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        after_settled_request_forgery.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id: retry_effect.clone(),
            configuration: RuntimeConfigurationEvidence {
                requested_model: Some("archive-corruption-tail".into()),
                ..Default::default()
            },
        })],
        64,
    )
    .await;
    let after_archive_corruption = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        after_archive_corruption,
        "a structurally corrupt current snapshot must rebuild before applying its tail"
    );

    h.engine
        .store()
        .rebuild_cursors(&mission_id, 58)
        .await
        .expect("snapshot exact authority");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        let lionclaw::model::InflightEffect::RoleRun {
            workspace_preparation,
            ..
        } = snapshot.inflight.get_mut(&retry_effect).unwrap()
        else {
            unreachable!()
        };
        *workspace_preparation = lionclaw::model::WorkspacePreparation::Preserve;
        snapshot
            .tasks
            .get_mut(&retry_identity.task_id)
            .unwrap()
            .workspace_provenance
            .as_mut()
            .unwrap()
            .archived_effect_id = None;
        snapshot
            .retained_workspace_archives
            .get_mut(&retry_identity.conversation_id)
            .unwrap()
            .clear();
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        after_archive_corruption.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id: retry_effect.clone(),
            configuration: RuntimeConfigurationEvidence {
                applied_model: Some("coordinated-forgery-tail".into()),
                ..Default::default()
            },
        })],
        59,
    )
    .await;
    let after_coordinated_forgery =
        fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        after_coordinated_forgery,
        "active writer snapshots must remain bound to the immutable request event"
    );

    h.engine
        .store()
        .rebuild_cursors(&mission_id, 60)
        .await
        .expect("snapshot exact authority after coordinated forgery");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&retry_identity.task_id)
            .unwrap()
            .pending_workspace_recreation = Some(first_effect.clone());
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        after_coordinated_forgery.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id: retry_effect.clone(),
            configuration: RuntimeConfigurationEvidence {
                requested_mode: Some("pending-corruption-tail".into()),
                ..Default::default()
            },
        })],
        61,
    )
    .await;
    let after_pending_corruption = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        after_pending_corruption
    );

    h.engine
        .store()
        .rebuild_cursors(&mission_id, 60)
        .await
        .expect("snapshot at the current reducer");
    set_snapshot_reducer(dir.path(), &mission_id, REDUCER_VERSION - 1).await;
    fault_append_events(
        dir.path(),
        &mission_id,
        after_pending_corruption.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id: retry_effect.clone(),
            configuration: RuntimeConfigurationEvidence {
                applied_mode: Some("old-reducer-tail".into()),
                ..Default::default()
            },
        })],
        61,
    )
    .await;
    let old_reducer_rebuild = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        old_reducer_rebuild
    );

    let snapshotted = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 62)
        .await
        .expect("restore current reducer snapshot");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&retry_identity.task_id)
            .unwrap()
            .workspace_provenance = None;
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        snapshotted.head,
        &[NewEvent::new(MissionEvent::RoleRunCompleted {
            effect_id: retry_effect,
            request: Box::new(retry_identity.clone()),
            outcome: Ok(RoleRunSuccess {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("workspace recreated"),
                    request_attention: false,
                }),
                artifact: None,
                final_response: PayloadRef::inline("workspace recreated"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            }),
        })],
        63,
    )
    .await;
    let complete = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    let (snapshot_head, reducer) = h
        .engine
        .store()
        .snapshot_meta(&mission_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reducer, REDUCER_VERSION);
    assert!(snapshot_head < complete.head);
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        complete
    );
    assert!(
        complete.retained_workspace_archives[&retry_identity.conversation_id]
            .contains(&first_effect)
    );
}

#[tokio::test]
async fn active_writer_snapshot_cannot_forge_a_missing_preparation_event() {
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
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let initial = h.engine.load_state(&mission_id).await.expect("initial");
    let request = role_request(&initial);
    let identity = request_identity(&request.event);
    let MissionEvent::RoleRunRequested { effect_id, .. } = &request.event else {
        unreachable!()
    };
    let effect_id = effect_id.clone();
    fault_append_events(dir.path(), &mission_id, initial.head, &[request], 70).await;
    let requested = h.engine.load_state(&mission_id).await.expect("requested");
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 71)
        .await
        .expect("snapshot pre-preparation writer");
    mutate_snapshot_state(dir.path(), &mission_id, |snapshot| {
        snapshot
            .tasks
            .get_mut(&identity.task_id)
            .unwrap()
            .workspace_provenance = Some(lionclaw::model::TaskWorkspaceProvenance {
            effect_id: effect_id.clone(),
            conversation_id: identity.conversation_id.clone(),
            base_sha: identity.base_sha.clone(),
            assignment_epoch: identity.assignment_epoch,
            archived_effect_id: None,
        });
    })
    .await;
    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[NewEvent::new(MissionEvent::RoleRunCompleted {
            effect_id: effect_id.clone(),
            request: Box::new(identity),
            outcome: Ok(RoleRunSuccess {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("forged preparation"),
                    request_attention: false,
                }),
                artifact: None,
                final_response: PayloadRef::inline("forged preparation"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            }),
        })],
        72,
    )
    .await;

    let replayed = fold(h.engine.store().load(&mission_id).await.unwrap()).unwrap();
    assert!(replayed.inflight.contains_key(&effect_id));
    assert_eq!(
        replayed.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Running
    );
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        replayed,
        "active writer snapshots cannot establish preparation authority"
    );
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
    h.engine
        .propose_plan(&mission_id, proposal(0, plan))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let initial = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("initial state");
    let writer_request = execution_role_request(
        &initial,
        TaskId::new("write").unwrap(),
        RoleName::new("implementer").unwrap(),
        OutputSemantics::ProducesArtifact,
    );
    let writer_identity = request_identity(&writer_request.event);
    let MissionEvent::RoleRunRequested {
        effect_id: writer_effect,
        ..
    } = &writer_request.event
    else {
        unreachable!()
    };
    let writer_completion = NewEvent::new(MissionEvent::RoleRunCompleted {
        effect_id: writer_effect.clone(),
        request: Box::new(writer_identity.clone()),
        outcome: Ok(RoleRunSuccess {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("writer complete"),
                request_attention: false,
            }),
            artifact: None,
            final_response: PayloadRef::inline("writer complete"),
            runtime_configuration: RuntimeConfigurationEvidence::default(),
        }),
    });
    let writer_turn_observed = NewEvent::new(MissionEvent::RoleTurnObserved {
        effect_id: writer_effect.clone(),
        observation: RoleTurnObservation::Completed {
            final_response: PayloadRef::inline("writer complete"),
            runtime_configuration: RuntimeConfigurationEvidence::default(),
        },
    });
    let writer_report_observed = NewEvent::new(MissionEvent::RoleHandoffObserved {
        effect_id: writer_effect.clone(),
        observation: lionclaw::model::RoleHandoffObservation::Accepted {
            report: PayloadRef::inline("writer complete"),
        },
    });
    let writer_prepared = NewEvent::new(MissionEvent::TaskWorkspacePrepared {
        task_id: writer_identity.task_id.clone(),
        effect_id: writer_effect.clone(),
        base_sha: writer_identity.base_sha.clone(),
        assignment_epoch: writer_identity.assignment_epoch,
    });
    fault_append_events(
        dir.path(),
        &mission_id,
        initial.head,
        &[
            writer_request,
            writer_prepared,
            writer_turn_observed,
            writer_report_observed,
            writer_completion,
        ],
        40,
    )
    .await;

    let after_writer = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("writer state");
    let verdict_request = execution_role_request(
        &after_writer,
        TaskId::new("review").unwrap(),
        RoleName::new("reviewer").unwrap(),
        OutputSemantics::EmitsVerdict,
    );
    let verdict_identity = request_identity(&verdict_request.event);
    let conversation_id = verdict_identity.conversation_id.clone();
    let MissionEvent::RoleRunRequested {
        effect_id: verdict_effect,
        ..
    } = &verdict_request.event
    else {
        unreachable!()
    };
    let verdict_effect = verdict_effect.clone();
    fault_append_events(
        dir.path(),
        &mission_id,
        after_writer.head,
        &[verdict_request],
        41,
    )
    .await;
    let requested = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("request state");
    let snapshotted_request = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 9_000_000)
        .await
        .expect("seed current-reducer request snapshot");
    assert_eq!(snapshotted_request, requested);

    let runtime_configuration = RuntimeConfigurationEvidence {
        requested_model: Some("forged-but-pinned".into()),
        ..RuntimeConfigurationEvidence::default()
    };
    let final_response = PayloadRef::inline("I cannot supply the required verdict.");
    fault_append_events(
        dir.path(),
        &mission_id,
        requested.head,
        &[
            NewEvent::new(MissionEvent::RoleTurnObserved {
                effect_id: verdict_effect.clone(),
                observation: RoleTurnObservation::Completed {
                    final_response: final_response.clone(),
                    runtime_configuration: runtime_configuration.clone(),
                },
            }),
            NewEvent::new(MissionEvent::RoleRunCompleted {
                effect_id: verdict_effect.clone(),
                request: Box::new(verdict_identity),
                outcome: Ok(RoleRunSuccess {
                    handoff: None,
                    artifact: None,
                    final_response: final_response.clone(),
                    runtime_configuration: runtime_configuration.clone(),
                }),
            }),
        ],
        42,
    )
    .await;

    let live = h.engine.load_state(&mission_id).await.expect("live fold");
    let events = h.engine.store().load(&mission_id).await.expect("events");
    let replayed = fold(events).expect("full replay");
    assert_eq!(live, replayed);
    let (snapshot_head, reducer_version) = h
        .engine
        .store()
        .snapshot_meta(&mission_id)
        .await
        .expect("snapshot metadata")
        .expect("snapshot");
    assert_eq!(reducer_version, REDUCER_VERSION);
    assert!(
        snapshot_head < live.head,
        "completion must be a nonempty tail"
    );
    assert_eq!(
        h.engine
            .store()
            .load_state_snapshotted(&mission_id)
            .await
            .expect("snapshot-tail load")
            .expect("state"),
        replayed
    );
    assert_eq!(
        h.engine
            .store()
            .rebuild_cursors(&mission_id, 9_000_000)
            .await
            .expect("rebuild from durable events"),
        replayed
    );

    let review = &live.tasks[&TaskId::new("review").unwrap()];
    assert_eq!(review.status, TaskStatus::Failed);
    let receipt = live
        .task_last_role_attempt(TaskNamespace::Execution, &TaskId::new("review").unwrap())
        .expect("failed verdict receipt");
    assert!(matches!(
        receipt.failure(),
        Some(TypedFailure::InvalidOutput { .. })
    ));
    assert_eq!(
        receipt.runtime_configuration.as_ref(),
        Some(&runtime_configuration)
    );
    let conversation = &live.conversations[&conversation_id];
    assert_eq!(
        conversation.lifecycle,
        ConversationLifecycle::ReworkingInvalidHandoff
    );
    assert_eq!(conversation.invalid_handoff_reworks, 1);
    assert_eq!(conversation.final_response, Some(final_response));
    assert!(conversation.active_delivery.is_none());
    assert!(!live.inflight.contains_key(&verdict_effect));
}

#[tokio::test]
async fn unfinished_request_is_rebuilt_from_the_log_alone() {
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
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let prompt_hash = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";
    let task_id = TaskId::new("fix").unwrap();
    let id = lionclaw::model::EffectId::for_role_request(
        lionclaw::model::TaskNamespace::Execution,
        &mission_id,
        &task_id,
        1,
        1,
        prompt_hash,
    );
    let state = h.engine.load_state(&mission_id).await.expect("state");
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(MissionEvent::RoleRunRequested {
            conversation_id: lionclaw::model::ConversationId::for_role_instance(
                &mission_id,
                lionclaw::model::TaskNamespace::Execution,
                &task_id,
                &RoleName::new("implementer").unwrap(),
                1,
            ),
            namespace: lionclaw::model::TaskNamespace::Execution,
            task_id,
            attempt_no: 1,
            effect_id: id.clone(),
            role: RoleName::new("implementer").unwrap(),
            output: OutputSemantics::ProducesArtifact,
            runtime: "codex".to_string(),
            prompt_template: lionclaw::model::RolePromptTemplate::Execution,
            prompt_hash: prompt_hash.into(),
            base_sha: BASE_SHA.to_string(),
            assignment_epoch: 1,
            message_boundary: state.head,
            presented_messages: vec![],
            workspace_preparation: lionclaw::model::WorkspacePreparation::ResetForAssignment,
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        })
        .with_prompt_hash(prompt_hash)],
        1,
    )
    .await;

    let before = h.engine.load_state(&mission_id).await.expect("state");
    assert!(before.inflight.contains_key(&id));
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
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
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
        .expect_err("reader must reject the old schema");
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
