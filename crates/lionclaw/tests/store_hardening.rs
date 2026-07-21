//! The event log is the only durable effect queue. There is no second ledger
//! to lease, reseed, or reconcile.

mod common;

use common::{
    approve_plan, fault_append_events, harness, proposal, simple_plan, BASE_SHA, HEAD_SHA,
};
use lionclaw::model::{
    apply, fold, ControlAction, ConversationId, ConversationLifecycle, ConversationRecipient,
    MissionEvent, MissionState, OutputSemantics, ParkedEffect, RoleName, RoleRunRequestIdentity,
    TaskId, TaskNamespace, TaskStatus, TypedFailure,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

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
            max_attempts: state.config.recovery.max_attempts,
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
        recreate_workspace: assignment.recreate_workspace,
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
        recreate_workspace,
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
        recreate_workspace: *recreate_workspace,
        message_boundary: *message_boundary,
        presented_messages: presented_messages.clone(),
    }
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
            action: ControlAction::Continue { automatic: false },
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
            recreate_workspace,
            ..
        } => {
            assert_eq!(*assignment_epoch, 1);
            assert!(recreate_workspace);
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
            recreate_workspace: true,
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
