use std::collections::{BTreeMap, BTreeSet};

use lionclaw_model::{
    apply, fold, next, resolve_execution_deadline_ms, ArtifactOutcome, Assertion, AssertionId,
    AuthorityCeilings, AuthorityGrants, ChildMissionAssignment, ChildMissionOutput,
    ChildMissionReceipt, ChildProofSummary, ControlAction, DecisionAction, EffectId, EffectIntent,
    EventEnvelope, ExecutionPolicy, FinishClass, MissionConfig, MissionEvent, MissionId,
    MissionProposal, MissionState, MissionTypeRef, NetworkGrant, OutputSemantics, PayloadRef, Plan,
    PlanProposal, RecoveryConfig, Requirement, RequirementDisposition, RequirementId,
    RequirementKind, RoleInstance, RoleInstanceId, RuntimeInstrumentIdentity, StopBar, Task,
    TaskAssignment, TaskId, TaskStatus, TeamRevision, TerminalState, TypedFailure, VersionStamps,
    SCHEMA_VERSION,
};

fn id(raw: &str) -> RoleInstanceId {
    RoleInstanceId::new(raw).expect("valid role id")
}

fn role(raw: &str, output: OutputSemantics) -> RoleInstance {
    RoleInstance {
        id: id(raw),
        purpose: format!("{raw} purpose"),
        output,
        runtime: "codex".into(),
        instructions: format!("{raw} instructions"),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            writes: output == OutputSemantics::ProducesArtifact,
            ..Default::default()
        },
        resources: Default::default(),
        deadline_secs: None,
    }
}

fn plan(task: &str) -> Plan {
    let upper = task.to_ascii_uppercase();
    let assertion = AssertionId::new(format!("A-{upper}")).unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new(format!("REQ-{upper}")).unwrap(),
            kind: RequirementKind::Capability,
            prose: format!("{task} is delivered"),
            disposition: RequirementDisposition::ReviewerCheckable {
                assertion_ids: vec![assertion.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: format!("{task} is acceptable"),
            oracle: None,
        }],
        tasks: vec![Task {
            id: TaskId::new(task).unwrap(),
            body: format!("complete {task}"),
            targets: vec![assertion],
            depends_on: Vec::new(),
        }],
    }
}

fn execution(
    timeout: u64,
    maximum: u64,
    capacity: u32,
    depth: u32,
    descendants: u32,
) -> ExecutionPolicy {
    ExecutionPolicy {
        default_timeout_secs: timeout,
        max_task_time_secs: maximum,
        extension_step_secs: timeout,
        effect_capacity: capacity,
        max_child_depth: depth,
        max_descendants: descendants,
        auto_continue_candidate: false,
        auto_continue_proof: false,
    }
}

fn child_assignment(output: OutputSemantics) -> ChildMissionAssignment {
    let planner = role("child-planner", OutputSemantics::ProposesPlan);
    let worker = role("child-worker", output);
    let reviewer = role("child-reviewer", OutputSemantics::EmitsVerdict);
    let task_id = TaskId::new("child-work").unwrap();
    let assertion_id = AssertionId::new("A-CHILD-WORK").unwrap();
    let team = TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner),
            (worker.id.clone(), worker),
            (reviewer.id.clone(), reviewer),
        ]),
        planning_assignment: id("child-planner"),
        task_assignments: BTreeMap::from([(task_id, id("child-worker").into())]),
        judgment_assignments: BTreeMap::from([(assertion_id, vec![id("child-reviewer")])]),
        gap_review_assignment: None,
        guidance: None,
    };
    ChildMissionAssignment {
        objective: "produce the delegated deliverable".into(),
        output,
        config: MissionConfig {
            stop: StopBar::Attested,
            ceilings: AuthorityCeilings {
                writes: output == OutputSemantics::ProducesArtifact,
                ..Default::default()
            },
            runtime_ceilings: BTreeSet::from(["codex".into()]),
            recovery: RecoveryConfig { max_attempts: 2 },
            execution: execution(60, 120, 2, 3, 8),
            ..Default::default()
        },
        proposal: Box::new(MissionProposal {
            plan: Some(PlanProposal {
                base_revision: 0,
                requirement_changes: Vec::new(),
                assertion_supersessions: Vec::new(),
                plan: plan("child-work"),
            }),
            team: Some(team),
            oracles: Some(BTreeMap::new()),
        }),
        deadline_secs: 120,
    }
}

fn runtime_identities(team: &TeamRevision) -> BTreeMap<RoleInstanceId, RuntimeInstrumentIdentity> {
    team.roles
        .iter()
        .map(|(role_id, role)| {
            (
                role_id.clone(),
                RuntimeInstrumentIdentity {
                    runtime: role.runtime.clone(),
                    model: None,
                    mode: None,
                    model_network: NetworkGrant::Deny,
                    external_oracle_drivers: BTreeMap::new(),
                },
            )
        })
        .collect()
}

fn parent_events(output: OutputSemantics) -> Vec<EventEnvelope> {
    let planner = role("planner", OutputSemantics::ProposesPlan);
    let reviewer = role("reviewer", OutputSemantics::EmitsVerdict);
    let team_zero = TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner),
            (reviewer.id.clone(), reviewer),
        ]),
        planning_assignment: id("planner"),
        task_assignments: BTreeMap::new(),
        judgment_assignments: BTreeMap::new(),
        gap_review_assignment: None,
        guidance: None,
    };
    let mut team_one = team_zero.clone();
    team_one.revision = 1;
    team_one.task_assignments.insert(
        TaskId::new("parent-work").unwrap(),
        TaskAssignment::ChildMission {
            mission: Box::new(child_assignment(output)),
        },
    );
    team_one.judgment_assignments.insert(
        AssertionId::new("A-PARENT-WORK").unwrap(),
        vec![id("reviewer")],
    );
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan("parent-work"),
        }),
        team: Some(team_one.clone()),
        oracles: Some(BTreeMap::new()),
    };
    vec![
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "parent objective".into(),
                mission_type: MissionTypeRef {
                    name: "software-dev".into(),
                    digest: "mission-type".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config: MissionConfig {
                    stop: StopBar::Attested,
                    ceilings: AuthorityCeilings {
                        writes: true,
                        ..Default::default()
                    },
                    resource_ceilings: Default::default(),
                    runtime_ceilings: BTreeSet::from(["codex".into()]),
                    recovery: RecoveryConfig { max_attempts: 3 },
                    execution: execution(120, 300, 4, 4, 32),
                    ..Default::default()
                },
                lineage: None,
            },
        ),
        event(
            2,
            MissionEvent::TeamConfigured {
                runtime_identities: runtime_identities(&team_zero),
                team: team_zero,
            },
        ),
        event(
            3,
            MissionEvent::ProposalRecorded {
                proposal: Box::new(proposal),
                proposal_hash: "proposal".into(),
            },
        ),
        event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "approve typed child work".into(),
                requirement_changes: Vec::new(),
                proposal_runtime_identities: runtime_identities(&team_one),
            },
        ),
    ]
}

fn event(sequence_no: u64, event: MissionEvent) -> EventEnvelope {
    EventEnvelope {
        mission_id: MissionId::parse("mabc123abc123").unwrap(),
        sequence_no,
        recorded_at_ms: sequence_no as i64,
        stamps: VersionStamps {
            schema_version: SCHEMA_VERSION,
            engine_version: "test".into(),
            prompt_hash: None,
        },
        event,
    }
}

fn projected_request(state: &MissionState) -> lionclaw_model::ChildMissionRequest {
    next(state)
        .effects
        .into_iter()
        .find_map(|effect| match effect {
            EffectIntent::ChildMission(request) => Some(request),
            _ => None,
        })
        .expect("child mission is a visible next effect")
}

#[test]
fn next_visibly_derives_stable_child_identity_and_reserves_capacity() {
    let state = fold(parent_events(OutputSemantics::ProducesReport)).unwrap();
    let first = projected_request(&state);
    let replayed =
        projected_request(&fold(parent_events(OutputSemantics::ProducesReport)).unwrap());

    assert_eq!(first, replayed);
    assert_eq!(first.parent_mission_id, state.mission_id);
    assert_eq!(first.attempt_no, 1);
    assert_eq!(first.input_artifact, "base");
    assert_eq!(first.assignment.config.execution.effect_capacity, 2);
    assert_eq!(next(&state).effects.len(), 1);
}

#[test]
fn canonical_child_digest_binds_nested_contract_and_enforces_size_bound() {
    let original = child_assignment(OutputSemantics::ProducesReport);
    let digest = original.digest().unwrap();
    assert_eq!(digest, original.clone().digest().unwrap());

    let mut changed = original.clone();
    changed
        .proposal
        .team
        .as_mut()
        .unwrap()
        .roles
        .get_mut(&id("child-worker"))
        .unwrap()
        .instructions
        .push_str(" with a changed contract");
    assert_ne!(digest, changed.digest().unwrap());

    let mut oversized = original;
    oversized
        .proposal
        .team
        .as_mut()
        .unwrap()
        .roles
        .get_mut(&id("child-worker"))
        .unwrap()
        .instructions = "x".repeat(lionclaw_model::MAX_CHILD_MISSION_REQUEST_BYTES);
    assert!(oversized.digest().is_none());
}

#[test]
fn child_subtree_reservation_prevents_parent_local_capacity_multiplication() {
    let mut events = parent_events(OutputSemantics::ProducesReport);
    let local_task = TaskId::new("parent-local-work").unwrap();
    let join_task = TaskId::new("parent-integrate").unwrap();
    let local_role = role("local-worker", OutputSemantics::ProducesReport);
    let local_role_id = local_role.id.clone();
    let target = AssertionId::new("A-PARENT-LOCAL-WORK").unwrap();
    let join_target = AssertionId::new("A-PARENT-INTEGRATE").unwrap();
    if let MissionEvent::MissionCreated { config, .. } = &mut events[0].event {
        config.execution.effect_capacity = 2;
    }
    let MissionEvent::ProposalRecorded { proposal, .. } = &mut events[2].event else {
        unreachable!()
    };
    let plan = &mut proposal.plan.as_mut().unwrap().plan;
    plan.requirements.push(Requirement {
        id: RequirementId::new("REQ-PARENT-LOCAL-WORK").unwrap(),
        kind: RequirementKind::Capability,
        prose: "parent-local work is delivered".into(),
        disposition: RequirementDisposition::ReviewerCheckable {
            assertion_ids: vec![target.clone()],
        },
    });
    plan.assertions.push(Assertion {
        id: target.clone(),
        prose: "parent-local work is acceptable".into(),
        oracle: None,
    });
    plan.requirements.push(Requirement {
        id: RequirementId::new("REQ-PARENT-INTEGRATE").unwrap(),
        kind: RequirementKind::Capability,
        prose: "parent work is integrated".into(),
        disposition: RequirementDisposition::ReviewerCheckable {
            assertion_ids: vec![join_target.clone()],
        },
    });
    plan.assertions.push(Assertion {
        id: join_target.clone(),
        prose: "parent work is integrated acceptably".into(),
        oracle: None,
    });
    plan.tasks.push(Task {
        id: local_task.clone(),
        body: "complete parent-local work".into(),
        targets: vec![target.clone()],
        depends_on: Vec::new(),
    });
    plan.tasks.push(Task {
        id: join_task.clone(),
        body: "integrate parent and delegated work".into(),
        targets: vec![join_target.clone()],
        depends_on: vec![TaskId::new("parent-work").unwrap(), local_task.clone()],
    });
    let team = proposal.team.as_mut().unwrap();
    team.roles.insert(local_role_id.clone(), local_role);
    team.task_assignments
        .insert(local_task, local_role_id.clone().into());
    team.task_assignments
        .insert(join_task, local_role_id.into());
    team.judgment_assignments
        .insert(target, vec![id("reviewer")]);
    team.judgment_assignments
        .insert(join_target, vec![id("reviewer")]);
    let identities = runtime_identities(team);
    let MissionEvent::DecisionRecorded {
        proposal_runtime_identities,
        ..
    } = &mut events[3].event
    else {
        unreachable!()
    };
    *proposal_runtime_identities = identities;

    let initial = fold(events[..2].iter().cloned()).unwrap();
    let MissionEvent::ProposalRecorded { proposal, .. } = &events[2].event else {
        unreachable!()
    };
    lionclaw_model::validate_mission_proposal(&initial, proposal).unwrap();

    let state = fold(events.clone()).unwrap();
    let effects = next(&state).effects;
    assert_eq!(effects.len(), 1);
    assert!(
        matches!(effects[0], EffectIntent::ChildMission(_)),
        "unexpected effects: {effects:?}; proposal={:?}",
        state.proposal
    );
    let EffectIntent::ChildMission(request) = &effects[0] else {
        unreachable!()
    };
    assert_eq!(
        request.assignment.config.execution.effect_capacity,
        state.config.execution.effect_capacity
    );

    let MissionEvent::ProposalRecorded { proposal, .. } = &mut events[2].event else {
        unreachable!()
    };
    let TaskAssignment::ChildMission { mission } = proposal
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .get_mut(&TaskId::new("parent-work").unwrap())
        .unwrap()
    else {
        unreachable!()
    };
    mission.config.execution.effect_capacity = 1;

    let effects = next(&fold(events).unwrap()).effects;
    assert_eq!(effects.len(), 2);
    let reserved = effects
        .iter()
        .map(|effect| match effect {
            EffectIntent::ChildMission(request) => {
                request.assignment.config.execution.effect_capacity
            }
            EffectIntent::DispatchRole(_) | EffectIntent::DispatchOracle(_) => 1,
            _ => 0,
        })
        .sum::<u32>();
    assert_eq!(reserved, 2);
}

#[test]
fn child_request_schema_carries_secret_authority_without_secret_material_fields() {
    let mut events = parent_events(OutputSemantics::ProducesReport);
    if let MissionEvent::MissionCreated { config, .. } = &mut events[0].event {
        config.ceilings.secrets = true;
    }
    let MissionEvent::ProposalRecorded { proposal, .. } = &mut events[2].event else {
        unreachable!()
    };
    let TaskAssignment::ChildMission { mission } = proposal
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .get_mut(&TaskId::new("parent-work").unwrap())
        .unwrap()
    else {
        unreachable!()
    };
    mission.config.ceilings.secrets = true;
    let child_team = mission.proposal.team.as_mut().unwrap();
    child_team
        .roles
        .get_mut(&id("child-planner"))
        .unwrap()
        .grants
        .secrets = true;
    child_team
        .roles
        .get_mut(&id("child-worker"))
        .unwrap()
        .grants
        .secrets = true;
    let request = projected_request(&fold(events).unwrap());
    let projected_team = request.assignment.proposal.team.as_ref().unwrap();
    let serialized = serde_json::to_string(&request).unwrap();

    assert!(
        projected_team
            .roles
            .get(&id("child-planner"))
            .unwrap()
            .grants
            .secrets
    );
    assert!(
        projected_team
            .roles
            .get(&id("child-worker"))
            .unwrap()
            .grants
            .secrets
    );
    assert!(serialized.contains("\"secrets\":true"));
    assert!(!serialized.contains("secret_values"));
    assert!(!serialized.contains("lineage"));
    assert!(!serialized.contains("ancestry"));
}

#[test]
fn child_secret_authority_must_follow_the_typed_parent_subset() {
    let mut state = fold(
        parent_events(OutputSemantics::ProducesReport)[..2]
            .iter()
            .cloned(),
    )
    .unwrap();
    state.config.ceilings.secrets = true;
    let mut candidate = match &parent_events(OutputSemantics::ProducesReport)[2].event {
        MissionEvent::ProposalRecorded { proposal, .. } => (**proposal).clone(),
        _ => unreachable!(),
    };
    {
        let TaskAssignment::ChildMission { mission } = candidate
            .team
            .as_mut()
            .unwrap()
            .task_assignments
            .get_mut(&TaskId::new("parent-work").unwrap())
            .unwrap()
        else {
            unreachable!()
        };
        mission.config.ceilings.secrets = true;
        let child_team = mission.proposal.team.as_mut().unwrap();
        child_team
            .roles
            .get_mut(&id("child-planner"))
            .unwrap()
            .grants
            .secrets = true;
        child_team
            .roles
            .get_mut(&id("child-worker"))
            .unwrap()
            .grants
            .secrets = true;
    }

    state
        .team
        .as_mut()
        .unwrap()
        .roles
        .get_mut(&id("planner"))
        .unwrap()
        .grants
        .secrets = true;
    candidate
        .team
        .as_mut()
        .unwrap()
        .roles
        .get_mut(&id("planner"))
        .unwrap()
        .grants
        .secrets = true;

    lionclaw_model::validate_mission_proposal(&state, &candidate).unwrap();

    state.config.ceilings.secrets = false;
    let error = lionclaw_model::validate_mission_proposal(&state, &candidate).unwrap_err();
    assert!(error.to_string().contains("within ceilings"));
}

#[test]
fn request_bind_receipt_and_cleanup_replay_without_promoting_child_proof() {
    let mut events = parent_events(OutputSemantics::ProducesReport);
    let state = fold(events.clone()).unwrap();
    let request = projected_request(&state);
    let requested_at_ms = 1_000;
    events.push(event(
        5,
        MissionEvent::ChildMissionRequested {
            request: Box::new(request.clone()),
            requested_at_ms,
            deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
            budget_deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
        },
    ));
    events.push(event(
        6,
        MissionEvent::ChildMissionBound {
            effect_id: request.parent_effect_id.clone(),
            child_mission_id: request.child_mission_id.clone(),
        },
    ));
    let report = PayloadRef::inline("child report");
    let report_sha256 = report.content_sha256().unwrap();
    let receipt = ChildMissionReceipt {
        parent_mission_id: request.parent_mission_id.clone(),
        parent_effect_id: request.parent_effect_id.clone(),
        child_mission_id: request.child_mission_id.clone(),
        request_digest: request.request_digest.clone(),
        input_artifact: request.input_artifact.clone(),
        terminal: TerminalState::Done {
            finish: FinishClass::Attested,
        },
        descendant_count: 0,
        output: Some(ChildMissionOutput::Report {
            report,
            report_sha256,
        }),
        proof: ChildProofSummary {
            finish: Some(FinishClass::Verified),
            authoritative_receipt_digests: vec!["forged-child-proof".into()],
            advisory_receipt_digests: Vec::new(),
        },
        failure: None,
    };
    events.push(event(
        7,
        MissionEvent::ChildMissionCompleted {
            effect_id: request.parent_effect_id.clone(),
            receipt: Box::new(receipt),
        },
    ));

    let before_cleanup = fold(events.clone()).unwrap();
    assert_eq!(
        before_cleanup.tasks[&request.task_id].status,
        TaskStatus::Cleared
    );
    assert!(before_cleanup.authoritative_receipts.is_empty());
    assert!(!next(&before_cleanup)
        .choices
        .iter()
        .any(|choice| matches!(choice, lionclaw_model::Choice::Finish { .. })));
    assert_eq!(
        next(&before_cleanup).effects,
        vec![EffectIntent::CleanupChildMission {
            effect_id: request.parent_effect_id.clone(),
            child_mission_id: request.child_mission_id.clone(),
        }]
    );

    events.push(event(
        8,
        MissionEvent::ChildMissionCleaned {
            effect_id: request.parent_effect_id.clone(),
            child_mission_id: request.child_mission_id.clone(),
        },
    ));
    let replayed = fold(events.clone()).unwrap();
    let mut incrementally_folded = fold(events[..4].iter().cloned()).unwrap();
    for envelope in &events[4..] {
        apply(&mut incrementally_folded, envelope);
    }
    assert_eq!(replayed, incrementally_folded);
    assert!(next(&replayed).effects.iter().any(|effect| matches!(
        effect,
        EffectIntent::DispatchRole(turn) if turn.output == OutputSemantics::EmitsVerdict
    )));
}

#[test]
fn durable_parent_stop_dominates_a_later_successful_child_receipt() {
    let mut events = parent_events(OutputSemantics::ProducesReport);
    let request = projected_request(&fold(events.clone()).unwrap());
    let requested_at_ms = 1_000;
    let report = PayloadRef::inline("child report");
    let report_sha256 = report.content_sha256().unwrap();
    events.extend([
        event(
            5,
            MissionEvent::ChildMissionRequested {
                request: Box::new(request.clone()),
                requested_at_ms,
                deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
                budget_deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
            },
        ),
        event(
            6,
            MissionEvent::ChildMissionBound {
                effect_id: request.parent_effect_id.clone(),
                child_mission_id: request.child_mission_id.clone(),
            },
        ),
        event(
            7,
            MissionEvent::ControlRequested {
                effect_id: request.parent_effect_id.clone(),
                action: ControlAction::Stop,
                reason: "operator stopped delegated work".into(),
            },
        ),
        event(
            8,
            MissionEvent::ChildMissionCompleted {
                effect_id: request.parent_effect_id.clone(),
                receipt: Box::new(ChildMissionReceipt {
                    parent_mission_id: request.parent_mission_id.clone(),
                    parent_effect_id: request.parent_effect_id.clone(),
                    child_mission_id: request.child_mission_id.clone(),
                    request_digest: request.request_digest.clone(),
                    input_artifact: request.input_artifact.clone(),
                    terminal: TerminalState::Done {
                        finish: FinishClass::Attested,
                    },
                    descendant_count: 0,
                    output: Some(ChildMissionOutput::Report {
                        report,
                        report_sha256,
                    }),
                    proof: ChildProofSummary {
                        finish: Some(FinishClass::Verified),
                        authoritative_receipt_digests: Vec::new(),
                        advisory_receipt_digests: Vec::new(),
                    },
                    failure: None,
                }),
            },
        ),
    ]);

    let settled = fold(events).unwrap();
    assert_eq!(settled.tasks[&request.task_id].status, TaskStatus::Failed);
    assert_eq!(
        settled
            .task_last_failure(&request.task_id)
            .unwrap()
            .evidence()
            .code
            .as_deref(),
        Some("control.stopped_before_settlement")
    );
}

#[test]
fn descendant_admission_reserves_every_legal_child_attempt() {
    let state = fold(
        parent_events(OutputSemantics::ProducesReport)[..2]
            .iter()
            .cloned(),
    )
    .unwrap();
    let mut candidate = match &parent_events(OutputSemantics::ProducesReport)[2].event {
        MissionEvent::ProposalRecorded { proposal, .. } => (**proposal).clone(),
        _ => unreachable!(),
    };
    let TaskAssignment::ChildMission { mission } = candidate
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .get_mut(&TaskId::new("parent-work").unwrap())
        .unwrap()
    else {
        unreachable!()
    };
    mission.config.execution.max_descendants = 0;
    mission.config.execution.max_child_depth = 0;
    mission.config.recovery.max_attempts = 3;
    candidate.plan.as_mut().unwrap().plan.tasks[0].body =
        "complete parent work with bounded retries".into();

    let mut limit_two = state.clone();
    limit_two.config.execution.max_descendants = 2;
    let error = lionclaw_model::validate_mission_proposal(&limit_two, &candidate).unwrap_err();
    assert!(error.to_string().contains("reserves 3 more"));

    let mut limit_three = state;
    limit_three.config.execution.max_descendants = 3;
    lionclaw_model::validate_mission_proposal(&limit_three, &candidate).unwrap();

    let historical_effect = EffectId::for_parts(&["historical-child"]);
    limit_three.child_mission_receipts.insert(
        historical_effect.clone(),
        ChildMissionReceipt {
            parent_mission_id: limit_three.mission_id.clone(),
            parent_effect_id: historical_effect,
            child_mission_id: MissionId::parse("m111111111111").unwrap(),
            request_digest: "historical-request".into(),
            input_artifact: "parent-base".into(),
            terminal: TerminalState::Aborted {
                reason: "historical child failed".into(),
            },
            descendant_count: 0,
            output: None,
            proof: ChildProofSummary {
                finish: None,
                authoritative_receipt_digests: Vec::new(),
                advisory_receipt_digests: Vec::new(),
            },
            failure: Some(TypedFailure::permanent(
                "test.historical_child",
                "historical child failed",
            )),
        },
    );
    let error = lionclaw_model::validate_mission_proposal(&limit_three, &candidate).unwrap_err();
    assert!(error.to_string().contains("has 1 descendants"));
}

#[test]
fn descendant_replan_reserves_only_unspent_attempts_for_retained_tasks() {
    let mut events = parent_events(OutputSemantics::ProducesReport);
    if let MissionEvent::MissionCreated { config, .. } = &mut events[0].event {
        config.recovery.max_attempts = 2;
        config.execution.max_descendants = 2;
    }
    let MissionEvent::ProposalRecorded { proposal, .. } = &mut events[2].event else {
        unreachable!()
    };
    let TaskAssignment::ChildMission { mission } = proposal
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .get_mut(&TaskId::new("parent-work").unwrap())
        .unwrap()
    else {
        unreachable!()
    };
    mission.config.execution.max_descendants = 0;

    let request = projected_request(&fold(events.clone()).unwrap());
    let requested_at_ms = 1_000;
    events.extend([
        event(
            5,
            MissionEvent::ChildMissionRequested {
                request: Box::new(request.clone()),
                requested_at_ms,
                deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
                budget_deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
            },
        ),
        event(
            6,
            MissionEvent::ChildMissionBound {
                effect_id: request.parent_effect_id.clone(),
                child_mission_id: request.child_mission_id.clone(),
            },
        ),
        event(
            7,
            MissionEvent::ChildMissionCompleted {
                effect_id: request.parent_effect_id.clone(),
                receipt: Box::new(ChildMissionReceipt {
                    parent_mission_id: request.parent_mission_id.clone(),
                    parent_effect_id: request.parent_effect_id.clone(),
                    child_mission_id: request.child_mission_id.clone(),
                    request_digest: request.request_digest.clone(),
                    input_artifact: request.input_artifact.clone(),
                    terminal: TerminalState::Aborted {
                        reason: "first child attempt failed".into(),
                    },
                    descendant_count: 0,
                    output: None,
                    proof: ChildProofSummary {
                        finish: None,
                        authoritative_receipt_digests: Vec::new(),
                        advisory_receipt_digests: Vec::new(),
                    },
                    failure: Some(TypedFailure::permanent(
                        "test.first_child_failed",
                        "first child attempt failed",
                    )),
                }),
            },
        ),
        event(
            8,
            MissionEvent::ChildMissionCleaned {
                effect_id: request.parent_effect_id,
                child_mission_id: request.child_mission_id,
            },
        ),
    ]);
    let state = fold(events).unwrap();
    assert_eq!(state.descendant_count(), 1);
    assert_eq!(state.tasks[&request.task_id].attempts, 1);

    let mut next_team = state.team.clone().unwrap();
    next_team.revision += 1;
    lionclaw_model::validate_mission_proposal(
        &state,
        &MissionProposal {
            plan: None,
            team: Some(next_team),
            oracles: None,
        },
    )
    .unwrap();
}

#[test]
fn artifact_child_settles_through_the_normal_task_candidate() {
    let mut events = parent_events(OutputSemantics::ProducesArtifact);
    let state = fold(events.clone()).unwrap();
    let request = projected_request(&state);
    let requested_at_ms = 2_000;
    events.extend([
        event(
            5,
            MissionEvent::ChildMissionRequested {
                request: Box::new(request.clone()),
                requested_at_ms,
                deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
                budget_deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
            },
        ),
        event(
            6,
            MissionEvent::ChildMissionBound {
                effect_id: request.parent_effect_id.clone(),
                child_mission_id: request.child_mission_id.clone(),
            },
        ),
        event(
            7,
            MissionEvent::ChildMissionCompleted {
                effect_id: request.parent_effect_id.clone(),
                receipt: Box::new(ChildMissionReceipt {
                    parent_mission_id: request.parent_mission_id.clone(),
                    parent_effect_id: request.parent_effect_id.clone(),
                    child_mission_id: request.child_mission_id.clone(),
                    request_digest: request.request_digest.clone(),
                    input_artifact: request.input_artifact.clone(),
                    terminal: TerminalState::Done {
                        finish: FinishClass::Attested,
                    },
                    descendant_count: 0,
                    output: Some(ChildMissionOutput::Artifact {
                        artifact: ArtifactOutcome {
                            base_sha: request.input_artifact.clone(),
                            head_sha: "child-head".into(),
                        },
                    }),
                    proof: ChildProofSummary {
                        finish: Some(FinishClass::Attested),
                        authoritative_receipt_digests: Vec::new(),
                        advisory_receipt_digests: Vec::new(),
                    },
                    failure: None,
                }),
            },
        ),
    ]);
    let settled = fold(events).unwrap();

    assert_eq!(settled.tasks[&request.task_id].status, TaskStatus::Cleared);
    assert_eq!(
        settled.tasks[&request.task_id].candidate_sha.as_deref(),
        Some("child-head")
    );
}

#[test]
fn child_authority_network_runtime_depth_deadline_and_resources_are_bounded() {
    let state = fold(
        parent_events(OutputSemantics::ProducesReport)[..2]
            .iter()
            .cloned(),
    )
    .unwrap();
    let mut candidate = match &parent_events(OutputSemantics::ProducesReport)[2].event {
        MissionEvent::ProposalRecorded { proposal, .. } => (**proposal).clone(),
        _ => unreachable!(),
    };
    let TaskAssignment::ChildMission { mission } = candidate
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .get_mut(&TaskId::new("parent-work").unwrap())
        .unwrap()
    else {
        unreachable!()
    };
    mission.config.ceilings.network = NetworkGrant::allow_single("example.com", 443).unwrap();
    mission.config.runtime_ceilings.insert("hermes".into());
    mission.config.resource_ceilings.tmpfs = vec!["/tmp:rw,size=1g".into()];
    mission.config.execution.max_child_depth = 4;
    mission.config.execution.max_descendants = 32;
    mission.config.execution.effect_capacity = 5;
    mission.deadline_secs = 301;

    let error = lionclaw_model::validate_mission_proposal(&state, &candidate)
        .expect_err("a child cannot widen any parent authority dimension");
    let detail = error.to_string();
    assert!(detail.contains("invalid_child_mission"));
    assert!(detail.contains("authority ceilings exceed"));
    assert!(detail.contains("runtime ceilings"));
    assert!(detail.contains("resource ceilings"));
    assert!(detail.contains("execution authority"));
    assert!(detail.contains("descendant_limit_exceeded"));
    assert!(detail.contains("deadline"));
}
