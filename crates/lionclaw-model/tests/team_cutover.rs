use std::collections::{BTreeMap, BTreeSet};

use lionclaw_model::{
    apply, fold, ready_to_finish, step, AdvisoryStatus, Assertion, AssertionId, AssertionState,
    AttentionKind, AuthorityCeilings, AuthorityGrants, ConfinementResources, ConversationLifecycle,
    ConversationState, DecisionAction, DeliveryMarker, EffectId, EventEnvelope, ExecutionPolicy,
    Handoff, MissionConfig, MissionEvent, MissionGuidance, MissionId, MissionPhase,
    MissionProposal, MissionState, MissionTypeRef, OracleName, OutputSemantics, PayloadRef, Plan,
    PlanProposal, QueuedMessage, RecoveryConfig, Requirement, RequirementDisposition,
    RequirementId, RequirementKind, RoleAttemptDisposition, RoleInstance, RoleInstanceId,
    RoleInstrumentIdentity, RolePromptTemplate, RoleTurnSuccess, RuntimeConfigurationEvidence,
    RuntimeInstrumentIdentity, StepDecision, StopBar, Task, TaskId, TaskRoleAssignment,
    TaskRuntimeState, TaskStatus, TeamRevision, TypedFailure, ValidationItem, VersionStamps,
    WorkspacePreparation, SCHEMA_VERSION,
};

const BASE_ENVIRONMENT_DIGEST: &str = "image";

fn instance(raw: &str) -> RoleInstanceId {
    RoleInstanceId::new(raw).expect("valid role instance id")
}

fn role(id: &str, output: OutputSemantics) -> RoleInstance {
    RoleInstance {
        id: instance(id),
        purpose: format!("{id} purpose"),
        output,
        runtime: "codex".to_string(),
        instructions: format!("{id} instructions"),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants::default(),
        resources: Default::default(),
        deadline_secs: None,
    }
}

fn runtime_identities(team: &TeamRevision) -> BTreeMap<RoleInstanceId, RuntimeInstrumentIdentity> {
    team.roles
        .iter()
        .map(|(id, role)| {
            (
                id.clone(),
                RuntimeInstrumentIdentity {
                    runtime: role.runtime.clone(),
                    model: None,
                    mode: None,
                },
            )
        })
        .collect()
}

fn team_event(team: TeamRevision) -> MissionEvent {
    let runtime_identities = runtime_identities(&team);
    MissionEvent::TeamConfigured {
        team,
        runtime_identities,
    }
}

fn role_instrument_from_prefix(
    events: &[EventEnvelope],
    role_instance: &RoleInstanceId,
    team_revision: u32,
) -> RoleInstrumentIdentity {
    fold(events.iter().cloned())
        .expect("prefix folds")
        .role_instrument_identity_for_revision(role_instance, team_revision)
        .expect("role instrument identity")
}

fn role_instrument_for_revision(
    team_revision: u32,
    role_instance: &RoleInstanceId,
) -> RoleInstrumentIdentity {
    let mut events = vec![
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "instrument helper".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config: MissionConfig {
                    ceilings: AuthorityCeilings {
                        writes: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
        ),
        event(2, team_event(team(0, false))),
    ];
    if team_revision != 0 {
        events.push(event(3, team_event(team(team_revision, true))));
    }
    role_instrument_from_prefix(&events, role_instance, team_revision)
}

#[test]
fn role_instance_identity_survives_assignment_and_team_revisions() {
    let engineer = instance("engineer");
    let planner = role("planner", OutputSemantics::ProposesPlan);
    let mut original = role("engineer", OutputSemantics::ProducesArtifact);
    original.grants.writes = true;
    let mut revised = original.clone();
    revised.runtime = "hermes".to_string();
    let task = TaskId::new("implement").unwrap();
    let revision_zero = TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner.clone()),
            (engineer.clone(), original),
        ]),
        planning_assignment: planner.id.clone(),
        task_assignments: BTreeMap::from([(task.clone(), engineer.clone())]),
        judgment_assignments: BTreeMap::new(),
        gap_review_assignment: None,
        guidance: None,
    };
    let revision_one = TeamRevision {
        revision: 1,
        roles: BTreeMap::from([(planner.id.clone(), planner), (engineer.clone(), revised)]),
        ..revision_zero.clone()
    };
    assert_eq!(revision_one.task_assignments[&task], engineer);
    assert_eq!(revision_one.roles[&engineer].runtime, "hermes");
}

#[test]
fn team_owns_contracts_assignments_and_guidance() {
    let planner = role("planner", OutputSemantics::ProposesPlan);
    let engineer = role("engineer", OutputSemantics::ProducesArtifact);
    let reviewer = role("reviewer", OutputSemantics::EmitsVerdict);
    let gap = role("gap-reviewer", OutputSemantics::EmitsGapVerdict);
    let task = TaskId::new("implement").expect("task");
    let assertion = AssertionId::new("A-1").expect("assertion");

    let team = TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner),
            (engineer.id.clone(), engineer),
            (reviewer.id.clone(), reviewer),
            (gap.id.clone(), gap),
        ]),
        planning_assignment: instance("planner"),
        task_assignments: BTreeMap::from([(task, instance("engineer"))]),
        judgment_assignments: BTreeMap::from([(assertion, vec![instance("reviewer")])]),
        gap_review_assignment: Some(instance("gap-reviewer")),
        guidance: Some(MissionGuidance::new("Prefer the smallest correct change.")),
    };

    assert_eq!(
        team.role(&instance("engineer")).expect("engineer").output,
        OutputSemantics::ProducesArtifact
    );
    team.validate_shape().expect("valid team shape");
}

#[test]
fn team_role_environment_cannot_override_kernel_coordinates() {
    let mut proposed = team(0, false);
    proposed
        .roles
        .get_mut(&instance("planner"))
        .unwrap()
        .environment
        .insert("HOME".into(), "/tmp/escape".into());
    let error = proposed
        .validate_shape()
        .expect_err("kernel-owned environment must be rejected");
    assert!(error.contains("owned by the LionClaw kernel"));
}

#[test]
fn role_resource_overrides_are_bounded_by_mission_resource_ceilings() {
    let mut proposed = team(1, true);
    proposed
        .roles
        .get_mut(&instance("engineer"))
        .unwrap()
        .resources = ConfinementResources {
        tmpfs: vec!["/tmp:rw,size=3g".into()],
    };
    let config = MissionConfig {
        resource_ceilings: ConfinementResources {
            tmpfs: vec!["/tmp:rw,size=2g".into()],
        },
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let state = fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test resource ceilings".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, true))),
    ])
    .unwrap();
    let proposal = MissionProposal {
        plan: None,
        team: Some(proposed),
    };
    let error = lionclaw_model::validate_mission_proposal(&state, &proposal)
        .expect_err("over-ceiling role resources must reject the team proposal");
    assert!(format!("{error:?}").contains("invalid_team_revision"));
}

#[test]
fn over_ceiling_team_configured_event_is_ignored_during_replay() {
    let mut forged = team(1, true);
    forged
        .roles
        .get_mut(&instance("engineer"))
        .unwrap()
        .resources = ConfinementResources {
        tmpfs: vec!["/tmp:rw,size=3g".into()],
    };
    let config = MissionConfig {
        resource_ceilings: ConfinementResources {
            tmpfs: vec!["/tmp:rw,size=2g".into()],
        },
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let state = fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test direct replay guard".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, true))),
        event(3, team_event(forged)),
    ])
    .unwrap();

    assert_eq!(state.team.as_ref().unwrap().revision, 0);
    assert!(!state.team_history.contains_key(&1));
}

#[test]
fn sunset_wire_shapes_have_no_planning_or_role_bridges() {
    let config = MissionConfig {
        stop: StopBar::Verified,
        oracles: BTreeSet::from([OracleName::new("cargo-test").expect("oracle")]),
        skills: BTreeMap::new(),
        ceilings: AuthorityCeilings::default(),
        resource_ceilings: Default::default(),
        oracle_resources: Default::default(),
        oracle_devices: Default::default(),
        requires_gap_review: true,
        recovery: RecoveryConfig::default(),
        execution: ExecutionPolicy::default(),
    };
    let config_json = serde_json::to_value(config).expect("serialize config");
    assert!(config_json.get("plan_inventory").is_none());
    assert!(config_json.get("planning").is_none());
    assert!(config_json.get("gap_review").is_none());

    let task = Task {
        id: TaskId::new("implement").expect("task"),
        body: "Implement the contract".to_string(),
        targets: vec![AssertionId::new("A-1").expect("assertion")],
        depends_on: Vec::new(),
    };
    let task_json = serde_json::to_value(task).expect("serialize task");
    assert!(task_json.get("kind").is_none());
    assert!(task_json.get("role").is_none());

    let effect_id = EffectId::for_role_turn(
        &MissionId::parse("mabc123abc123").unwrap(),
        &instance("engineer"),
        1,
        Some(&TaskId::new("implement").unwrap()),
        1,
        1,
        "prompt",
    );
    let request_json = serde_json::to_value(MissionEvent::RoleTurnRequested {
        role_instance: instance("engineer"),
        team_revision: 1,
        task_id: Some(TaskId::new("implement").unwrap()),
        assertion_ids: vec![AssertionId::new("A-1").unwrap()],
        attempt_no: 1,
        effect_id,
        prompt_template: RolePromptTemplate::Execution,
        prompt_hash: "prompt".into(),
        base_sha: "base".into(),
        environment_digest: BASE_ENVIRONMENT_DIGEST.into(),
        instrument_identity: role_instrument_for_revision(1, &instance("engineer")),
        dependency_refs: Vec::new(),
        assignment_epoch: 1,
        message_boundary: 0,
        presented_messages: Vec::new(),
        workspace_preparation: WorkspacePreparation::ResetForAssignment,
        requested_at_ms: 1,
        deadline_ms: 2,
        budget_deadline_ms: 3,
    })
    .unwrap();
    assert_eq!(request_json["type"], "role_turn_requested");
    assert!(request_json.get("output").is_none());
    assert!(request_json.get("runtime").is_none());
    assert!(request_json.get("conversation_id").is_none());
    assert!(request_json.get("namespace").is_none());
}

fn plan() -> Plan {
    let assertion = AssertionId::new("A-1").unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("REQ-1").unwrap(),
            kind: RequirementKind::Capability,
            prose: "the requested behavior works".into(),
            disposition: RequirementDisposition::ConfinedProvable {
                assertion_ids: vec![assertion.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: "the behavior is verified".into(),
            oracle: Some(OracleName::new("test").unwrap()),
        }],
        tasks: vec![Task {
            id: TaskId::new("implement").unwrap(),
            body: "implement the behavior".into(),
            targets: vec![assertion],
            depends_on: Vec::new(),
        }],
    }
}

fn two_oracle_plan() -> Plan {
    let left = AssertionId::new("A-LEFT").unwrap();
    let right = AssertionId::new("A-RIGHT").unwrap();
    Plan {
        requirements: vec![
            Requirement {
                id: RequirementId::new("REQ-LEFT").unwrap(),
                kind: RequirementKind::Capability,
                prose: "left behavior works".into(),
                disposition: RequirementDisposition::ConfinedProvable {
                    assertion_ids: vec![left.clone()],
                },
            },
            Requirement {
                id: RequirementId::new("REQ-RIGHT").unwrap(),
                kind: RequirementKind::Capability,
                prose: "right behavior works".into(),
                disposition: RequirementDisposition::ConfinedProvable {
                    assertion_ids: vec![right.clone()],
                },
            },
        ],
        assertions: vec![
            Assertion {
                id: left,
                prose: "left behavior is verified".into(),
                oracle: Some(OracleName::new("left").unwrap()),
            },
            Assertion {
                id: right,
                prose: "right behavior is verified".into(),
                oracle: Some(OracleName::new("right").unwrap()),
            },
        ],
        tasks: Vec::new(),
    }
}

fn team(revision: u32, assigned: bool) -> TeamRevision {
    let planner = role("planner", OutputSemantics::ProposesPlan);
    let mut engineer = role("engineer", OutputSemantics::ProducesArtifact);
    engineer.grants.writes = true;
    let reviewer = role("reviewer", OutputSemantics::EmitsVerdict);
    TeamRevision {
        revision,
        roles: BTreeMap::from([
            (planner.id.clone(), planner),
            (engineer.id.clone(), engineer),
            (reviewer.id.clone(), reviewer),
        ]),
        planning_assignment: instance("planner"),
        task_assignments: if assigned {
            [(TaskId::new("implement").unwrap(), instance("engineer"))].into()
        } else {
            BTreeMap::new()
        },
        judgment_assignments: if assigned {
            [(AssertionId::new("A-1").unwrap(), vec![instance("reviewer")])].into()
        } else {
            BTreeMap::new()
        },
        gap_review_assignment: None,
        guidance: None,
    }
}

#[test]
fn oracle_dispatch_is_bounded_by_remaining_effect_capacity() {
    let config = MissionConfig {
        stop: StopBar::Verified,
        oracles: BTreeSet::from([OracleName::new("test").unwrap()]),
        skills: BTreeMap::new(),
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let accepted_team = team(1, true);
    let mut state = fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test oracle capacity".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, false))),
        event(
            3,
            MissionEvent::ProposalRecorded {
                proposal: Box::new(MissionProposal {
                    plan: Some(PlanProposal {
                        base_revision: 0,
                        requirement_changes: Vec::new(),
                        assertion_supersessions: Vec::new(),
                        plan: plan(),
                    }),
                    team: Some(accepted_team.clone()),
                }),
                proposal_hash: "proposal".into(),
            },
        ),
        event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "ratified".into(),
                requirement_changes: Vec::new(),
            },
        ),
        event(5, team_event(accepted_team)),
    ])
    .expect("accepted mission plan");
    let left = AssertionId::new("A-LEFT").unwrap();
    let right = AssertionId::new("A-RIGHT").unwrap();
    state.phase = MissionPhase::Running;
    state.config.oracles = BTreeSet::from([
        OracleName::new("left").unwrap(),
        OracleName::new("right").unwrap(),
    ]);
    state.config.execution.effect_capacity = 1;
    state.tasks.clear();
    state.plan = Some(two_oracle_plan());
    state.contract = BTreeMap::from([
        (
            left.clone(),
            AssertionState {
                oracle: Some(OracleName::new("left").unwrap()),
                last_advisory: BTreeMap::new(),
                last_authoritative_receipt: None,
            },
        ),
        (
            right.clone(),
            AssertionState {
                oracle: Some(OracleName::new("right").unwrap()),
                last_advisory: BTreeMap::new(),
                last_authoritative_receipt: None,
            },
        ),
    ]);
    let plan = state.plan.as_ref().expect("plan");
    assert!(plan.assertion_requires_confined_proof(&left));
    assert!(plan.assertion_requires_confined_proof(&right));
    assert!(state.inflight.is_empty());
    assert_eq!(
        state
            .plan
            .as_ref()
            .unwrap()
            .tasks
            .iter()
            .filter(|task| state.tasks.get(&task.id).map(|task| task.status)
                != Some(TaskStatus::Cleared))
            .count(),
        0
    );
    assert_eq!(
        state
            .contract
            .iter()
            .filter(|(assertion_id, assertion)| {
                plan.assertion_requires_confined_proof(assertion_id)
                    && assertion.oracle.as_ref().is_some_and(|_| {
                        state
                            .authoritative_verdict(assertion)
                            .is_none_or(|verdict| !verdict.is_fresh_at(&state))
                    })
            })
            .count(),
        2
    );

    let StepDecision::RunOracles(intents) = step(&state) else {
        panic!("expected oracle dispatch, got {:?}", step(&state))
    };
    assert_eq!(intents.len(), 1);
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

#[test]
fn accepted_joint_proposal_promotes_the_plan_and_exact_team_revision() {
    let config = MissionConfig {
        stop: StopBar::Verified,
        oracles: BTreeSet::from([OracleName::new("test").unwrap()]),
        skills: BTreeMap::new(),
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        resource_ceilings: Default::default(),
        oracle_resources: Default::default(),
        oracle_devices: Default::default(),
        requires_gap_review: false,
        recovery: RecoveryConfig::default(),
        execution: ExecutionPolicy::default(),
    };
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: Some(team(1, true)),
    };
    let state = fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test team cutover".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, false))),
        event(
            3,
            MissionEvent::ProposalRecorded {
                proposal: Box::new(proposal.clone()),
                proposal_hash: "recorded".into(),
            },
        ),
        event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "ratified".into(),
                requirement_changes: Vec::new(),
            },
        ),
        event(5, team_event(proposal.team.unwrap())),
    ])
    .unwrap();

    assert_eq!(state.revision, 1);
    assert_eq!(state.team.unwrap().revision, 1);
    assert_eq!(state.plan.unwrap(), plan());
}

#[test]
fn a_skipped_team_revision_is_ignored_during_replay() {
    let config = MissionConfig {
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let state = fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test replay".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, false))),
        event(3, team_event(team(2, false))),
    ])
    .unwrap();
    assert_eq!(state.team.unwrap().revision, 0);
}

#[test]
fn role_completion_cannot_override_the_team_owned_output_contract() {
    let config = MissionConfig {
        stop: StopBar::Verified,
        oracles: BTreeSet::from([OracleName::new("test").unwrap()]),
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let accepted_team = team(1, true);
    let effect_id = EffectId::for_role_turn(
        &MissionId::parse("mabc123abc123").unwrap(),
        &instance("engineer"),
        1,
        Some(&TaskId::new("implement").unwrap()),
        1,
        1,
        "prompt",
    );
    let mut events = vec![
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test role authority".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config,
            },
        ),
        event(2, team_event(team(0, false))),
        event(
            3,
            MissionEvent::ProposalRecorded {
                proposal: Box::new(MissionProposal {
                    plan: Some(PlanProposal {
                        base_revision: 0,
                        requirement_changes: Vec::new(),
                        assertion_supersessions: Vec::new(),
                        plan: plan(),
                    }),
                    team: Some(accepted_team.clone()),
                }),
                proposal_hash: "proposal".into(),
            },
        ),
        event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "ratified".into(),
                requirement_changes: Vec::new(),
            },
        ),
        event(5, team_event(accepted_team)),
    ];
    let instrument_identity = role_instrument_from_prefix(&events, &instance("engineer"), 1);
    events.push(event(
        6,
        MissionEvent::RoleTurnRequested {
            role_instance: instance("engineer"),
            team_revision: 1,
            task_id: Some(TaskId::new("implement").unwrap()),
            assertion_ids: vec![AssertionId::new("A-1").unwrap()],
            attempt_no: 1,
            effect_id: effect_id.clone(),
            prompt_template: RolePromptTemplate::Execution,
            prompt_hash: "prompt".into(),
            base_sha: "base".into(),
            environment_digest: BASE_ENVIRONMENT_DIGEST.into(),
            instrument_identity,
            dependency_refs: Vec::new(),
            assignment_epoch: 1,
            message_boundary: 5,
            presented_messages: Vec::new(),
            workspace_preparation: WorkspacePreparation::ResetForAssignment,
            requested_at_ms: 6,
            deadline_ms: 60,
            budget_deadline_ms: 120,
        },
    ));
    events.push(event(
        7,
        MissionEvent::RoleTurnCompleted {
            effect_id: effect_id.clone(),
            outcome: Ok(RoleTurnSuccess {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("forged judge output"),
                    items: Vec::new(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                final_response: PayloadRef::inline("done"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
                prepared_inputs: Vec::new(),
                runtime_usage: Default::default(),
            }),
        },
    ));

    let state = fold(events).unwrap();
    assert!(matches!(
        state.role_attempt_receipts[&effect_id].disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    assert_ne!(
        state.tasks[&TaskId::new("implement").unwrap()].status,
        lionclaw_model::TaskStatus::Cleared
    );
}

fn accepted_advisory_state() -> MissionState {
    let mut advisory_plan = plan();
    advisory_plan.assertions[0].oracle = None;
    advisory_plan.requirements[0].disposition = RequirementDisposition::ReviewerCheckable {
        assertion_ids: vec![AssertionId::new("A-1").unwrap()],
    };
    let accepted_team = team(1, true);
    fold([
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "test advisory closure".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
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
                    ..Default::default()
                },
            },
        ),
        event(2, team_event(team(0, false))),
        event(
            3,
            MissionEvent::ProposalRecorded {
                proposal: Box::new(MissionProposal {
                    plan: Some(PlanProposal {
                        base_revision: 0,
                        requirement_changes: Vec::new(),
                        assertion_supersessions: Vec::new(),
                        plan: advisory_plan,
                    }),
                    team: Some(accepted_team.clone()),
                }),
                proposal_hash: "proposal".into(),
            },
        ),
        event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "ratified".into(),
                requirement_changes: Vec::new(),
            },
        ),
        event(5, team_event(accepted_team)),
    ])
    .unwrap()
}

fn reviewer_request(
    sequence_no: u64,
    attempt_no: u32,
    prompt_hash: &str,
) -> (EffectId, EventEnvelope) {
    let role = instance("reviewer");
    let assertion = AssertionId::new("A-1").unwrap();
    let effect_id = EffectId::for_role_turn(
        &MissionId::parse("mabc123abc123").unwrap(),
        &role,
        1,
        None,
        attempt_no,
        1,
        prompt_hash,
    );
    (
        effect_id.clone(),
        event(
            sequence_no,
            MissionEvent::RoleTurnRequested {
                role_instance: role.clone(),
                team_revision: 1,
                task_id: None,
                assertion_ids: vec![assertion],
                attempt_no,
                effect_id,
                prompt_template: RolePromptTemplate::Judgment,
                prompt_hash: prompt_hash.into(),
                base_sha: "base".into(),
                environment_digest: BASE_ENVIRONMENT_DIGEST.into(),
                instrument_identity: accepted_advisory_state()
                    .role_instrument_identity_for_revision(&role, 1)
                    .unwrap(),
                dependency_refs: Vec::new(),
                assignment_epoch: 1,
                message_boundary: sequence_no - 1,
                presented_messages: Vec::new(),
                workspace_preparation: WorkspacePreparation::Preserve,
                requested_at_ms: sequence_no as i64,
                deadline_ms: 60,
                budget_deadline_ms: 120,
            },
        ),
    )
}

fn validate_success(items: Vec<ValidationItem>, passed: bool) -> RoleTurnSuccess {
    RoleTurnSuccess {
        handoff: Some(Handoff::Validate {
            done: true,
            report: PayloadRef::inline("judgment"),
            items,
            passed,
            request_attention: false,
        }),
        artifact: None,
        final_response: PayloadRef::inline("done"),
        runtime_configuration: RuntimeConfigurationEvidence::default(),
        prepared_inputs: Vec::new(),
        runtime_usage: Default::default(),
    }
}

#[test]
fn attested_closure_waits_for_every_assigned_judge() {
    let mut state = accepted_advisory_state();
    state
        .tasks
        .get_mut(&TaskId::new("implement").unwrap())
        .unwrap()
        .status = TaskStatus::Cleared;
    apply(
        &mut state,
        &event(
            6,
            MissionEvent::MessageSent {
                recipients: vec![instance("reviewer")],
                body: "ignored before a conversation exists".into(),
                references: Vec::new(),
            },
        ),
    );
    assert_eq!(
        state.advisory_status(&AssertionId::new("A-1").unwrap()),
        AdvisoryStatus::Pending
    );
    assert_eq!(state.phase, MissionPhase::Running);

    let (effect_id, request) = reviewer_request(7, 1, "judge");
    apply(&mut state, &request);
    apply(
        &mut state,
        &event(
            8,
            MissionEvent::RoleTurnCompleted {
                effect_id,
                outcome: Ok(validate_success(
                    vec![ValidationItem {
                        item_id: AssertionId::new("A-1").unwrap(),
                        passed: true,
                    }],
                    true,
                )),
            },
        ),
    );
    assert_eq!(
        ready_to_finish(&state),
        Some(lionclaw_model::FinishClass::Attested)
    );
    apply(
        &mut state,
        &event(
            9,
            MissionEvent::MissionFinished {
                finish: lionclaw_model::FinishClass::Attested,
                reason: "attested proof bar satisfied".into(),
            },
        ),
    );
    assert_eq!(
        state.phase,
        MissionPhase::Done {
            finish: lionclaw_model::FinishClass::Attested
        }
    );
}

fn failed_required_judgment_state() -> MissionState {
    let mut state = accepted_advisory_state();
    state
        .tasks
        .get_mut(&TaskId::new("implement").unwrap())
        .unwrap()
        .status = TaskStatus::Cleared;
    let (effect_id, request) = reviewer_request(6, 1, "failed-judgment");
    apply(&mut state, &request);
    apply(
        &mut state,
        &event(
            7,
            MissionEvent::RoleTurnCompleted {
                effect_id,
                outcome: Ok(validate_success(
                    vec![ValidationItem {
                        item_id: AssertionId::new("A-1").unwrap(),
                        passed: false,
                    }],
                    false,
                )),
            },
        ),
    );
    state
}

#[test]
fn failed_required_judgment_parks_and_rejects_below_bar_finish() {
    let state = failed_required_judgment_state();
    let assertion_id = AssertionId::new("A-1").unwrap();
    assert_eq!(state.advisory_status(&assertion_id), AdvisoryStatus::Failed);
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert_eq!(ready_to_finish(&state), None);
    assert_eq!(step(&state), StepDecision::Park);

    let attention = &state.open_attention["proof_failed:judgment:reviewer:A-1"];
    assert_eq!(attention.kind, AttentionKind::ProofFailed);
    assert_eq!(
        attention.assertion_ids.as_slice(),
        core::slice::from_ref(&assertion_id)
    );
    assert_eq!(
        lionclaw_model::decision::legal_actions(&state, attention),
        [
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Revise
        ]
    );
    assert!(matches!(
        &attention.evidence,
        lionclaw_model::DecisionEvidence::RoleAttempts { effect_ids }
            if effect_ids.len() == 1
    ));

    for finish in [
        lionclaw_model::FinishClass::Unverified,
        lionclaw_model::FinishClass::Attested,
    ] {
        let mut forged = state.clone();
        apply(
            &mut forged,
            &event(
                8,
                MissionEvent::MissionFinished {
                    finish,
                    reason: "forged below-bar finish".into(),
                },
            ),
        );
        assert_eq!(forged.phase, MissionPhase::AttentionNeeded);
    }

    let failed_effect = attention
        .evidence
        .role_attempts()
        .first()
        .expect("failed judgment receipt")
        .clone();
    let mut forged = state;
    apply(
        &mut forged,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: "proof_failed:judgment:reviewer:A-1".into(),
                action: DecisionAction::Accept,
                justification: "forged proof waiver".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert_eq!(forged.phase, MissionPhase::AttentionNeeded);
    assert!(forged
        .open_attention
        .contains_key("proof_failed:judgment:reviewer:A-1"));
    assert_eq!(
        forged.contract[&assertion_id].last_advisory[&instance("reviewer")],
        failed_effect
    );
}

#[test]
fn failed_required_judgment_recovery_retries_or_replans() {
    let state = failed_required_judgment_state();
    let attention_id = "proof_failed:judgment:reviewer:A-1";
    let failed_effect = state.open_attention[attention_id]
        .evidence
        .role_attempts()
        .first()
        .expect("failed judgment receipt")
        .clone();

    let mut retry = state.clone();
    apply(
        &mut retry,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Retry,
                justification: "rerun the failed required judgment".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert!(retry.role_attempt_receipts.contains_key(&failed_effect));
    assert_eq!(
        retry.advisory_status(&AssertionId::new("A-1").unwrap()),
        AdvisoryStatus::Pending
    );
    assert!(matches!(step(&retry), StepDecision::DispatchRole(_)));

    let mut repair = state.clone();
    apply(
        &mut repair,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Repair,
                justification: "repair the work using the failed judgment".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert!(repair.role_attempt_receipts.contains_key(&failed_effect));
    let repaired_task = &repair.tasks[&TaskId::new("implement").unwrap()];
    assert_eq!(repaired_task.status, TaskStatus::Pending);
    assert!(matches!(
        repaired_task.feedback.last().map(|feedback| &feedback.evidence),
        Some(lionclaw_model::DecisionEvidence::RoleAttempts { effect_ids })
            if effect_ids == core::slice::from_ref(&failed_effect)
    ));

    let mut revise = state;
    apply(
        &mut revise,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Revise,
                justification: "revise the work using the failed judgment".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert_eq!(revise.phase, MissionPhase::Planning);
    let Some(lionclaw_model::PlanningRefinement::FailureEvidence(feedback)) =
        &revise.planning_input.refinement
    else {
        panic!("failed judgment evidence must reach replanning");
    };
    assert!(matches!(
        &feedback.evidence,
        lionclaw_model::DecisionEvidence::RoleAttempts { effect_ids }
            if effect_ids == core::slice::from_ref(&failed_effect)
    ));
    assert!(revise.role_attempt_receipts.contains_key(&failed_effect));
}

#[test]
fn repeated_identical_required_judgment_suppresses_retry() {
    let mut state = failed_required_judgment_state();
    let attention_id = "proof_failed:judgment:reviewer:A-1";
    apply(
        &mut state,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Retry,
                justification: "one manual retry".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    let (second_effect, request) = reviewer_request(9, 2, "failed-judgment");
    apply(&mut state, &request);
    apply(
        &mut state,
        &event(
            10,
            MissionEvent::RoleTurnCompleted {
                effect_id: second_effect.clone(),
                outcome: Ok(validate_success(
                    vec![ValidationItem {
                        item_id: AssertionId::new("A-1").unwrap(),
                        passed: false,
                    }],
                    false,
                )),
            },
        ),
    );

    let attention = &state.open_attention[attention_id];
    assert_eq!(
        lionclaw_model::decision::legal_actions(&state, attention),
        [DecisionAction::Repair, DecisionAction::Revise]
    );
    assert!(state.role_attempt_receipts.contains_key(&second_effect));
    let current = state.contract[&AssertionId::new("A-1").unwrap()].last_advisory
        [&instance("reviewer")]
        .clone();

    apply(
        &mut state,
        &event(
            11,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Retry,
                justification: "forged repeated retry".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert_eq!(
        state.contract[&AssertionId::new("A-1").unwrap()].last_advisory[&instance("reviewer")],
        current
    );
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
}

#[test]
fn changed_judgment_identity_offers_a_new_retry() {
    let mut state = failed_required_judgment_state();
    let attention_id = "proof_failed:judgment:reviewer:A-1";
    apply(
        &mut state,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Retry,
                justification: "retry with a changed prompt".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    let (second_effect, request) = reviewer_request(9, 2, "changed-judgment-prompt");
    apply(&mut state, &request);
    apply(
        &mut state,
        &event(
            10,
            MissionEvent::RoleTurnCompleted {
                effect_id: second_effect,
                outcome: Ok(validate_success(
                    vec![ValidationItem {
                        item_id: AssertionId::new("A-1").unwrap(),
                        passed: false,
                    }],
                    false,
                )),
            },
        ),
    );
    assert_eq!(
        lionclaw_model::decision::legal_actions(&state, &state.open_attention[attention_id]),
        [
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Revise
        ]
    );
}

#[test]
fn validator_handoff_requires_the_exact_assertion_set_and_consistent_summary() {
    for (suffix, items, passed) in [
        ("missing", Vec::new(), true),
        (
            "duplicate",
            vec![
                ValidationItem {
                    item_id: AssertionId::new("A-1").unwrap(),
                    passed: true,
                },
                ValidationItem {
                    item_id: AssertionId::new("A-1").unwrap(),
                    passed: true,
                },
            ],
            true,
        ),
        (
            "summary",
            vec![ValidationItem {
                item_id: AssertionId::new("A-1").unwrap(),
                passed: false,
            }],
            true,
        ),
    ] {
        let mut state = accepted_advisory_state();
        let (effect_id, request) = reviewer_request(6, 1, suffix);
        apply(&mut state, &request);
        apply(
            &mut state,
            &event(
                7,
                MissionEvent::RoleTurnCompleted {
                    effect_id: effect_id.clone(),
                    outcome: Ok(validate_success(items, passed)),
                },
            ),
        );
        assert!(matches!(
            state.role_attempt_receipts[&effect_id].disposition,
            RoleAttemptDisposition::Failed { .. }
        ));
        assert_eq!(
            state.advisory_status(&AssertionId::new("A-1").unwrap()),
            AdvisoryStatus::Pending
        );
    }
}

#[test]
fn permanent_taskless_role_failure_parks_until_an_explicit_retry() {
    let mut state = accepted_advisory_state();
    state
        .tasks
        .get_mut(&TaskId::new("implement").unwrap())
        .unwrap()
        .status = TaskStatus::Cleared;
    let (effect_id, request) = reviewer_request(6, 1, "failure");
    apply(&mut state, &request);
    apply(
        &mut state,
        &event(
            7,
            MissionEvent::RoleTurnCompleted {
                effect_id: effect_id.clone(),
                outcome: Err(TypedFailure::permanent("judge.failed", "fault injected")),
            },
        ),
    );
    let attention_id = "node_failed:reviewer:A-1";
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert_eq!(
        state.open_attention[attention_id].kind,
        AttentionKind::NodeFailed
    );
    assert_eq!(step(&state), StepDecision::Park);

    apply(
        &mut state,
        &event(
            8,
            MissionEvent::DecisionRecorded {
                attention_id: attention_id.into(),
                action: DecisionAction::Retry,
                justification: "retry after correcting the runtime".into(),
                requirement_changes: Vec::new(),
            },
        ),
    );
    assert_eq!(state.phase, MissionPhase::Running);
    assert!(matches!(step(&state), StepDecision::DispatchRole(_)));
    assert!(!state.parked_effects.contains_key(&effect_id));
}

#[test]
fn queued_continuation_uses_the_roles_running_serial_task() {
    let mut state = accepted_advisory_state();
    let earlier = TaskId::new("implement").unwrap();
    let active = TaskId::new("verify").unwrap();
    state.tasks.get_mut(&earlier).unwrap().status = TaskStatus::Cleared;
    state.tasks.get_mut(&earlier).unwrap().candidate_sha = Some("earlier-sha".into());
    state.plan.as_mut().unwrap().tasks.push(Task {
        id: active.clone(),
        body: "finish the later serial task".into(),
        targets: Vec::new(),
        depends_on: vec![earlier],
    });
    state
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .insert(active.clone(), instance("engineer"));
    state.tasks.insert(
        active.clone(),
        TaskRuntimeState {
            status: TaskStatus::Running,
            attempts: 1,
            consecutive_failures: 0,
            last_outcome: None,
            candidate_sha: None,
            pending_base_sha: None,
            feedback: Vec::new(),
            role_assignment: Some(TaskRoleAssignment {
                role_instance: instance("engineer"),
                team_revision: 1,
                base_sha: "base".into(),
                assignment_epoch: 1,
            }),
            workspace_provenance: None,
            pending_workspace_recreation: None,
        },
    );
    state.conversations.insert(
        instance("engineer"),
        ConversationState {
            role_instance: instance("engineer"),
            lifecycle: ConversationLifecycle::AwaitingLead,
            queued: vec![QueuedMessage {
                sequence_no: 6,
                body: "continue".into(),
                references: Vec::new(),
                marker: DeliveryMarker::Queued,
            }],
            consumed_through: 0,
            active_delivery: None,
            final_response: None,
            invalid_handoff_reworks: 0,
        },
    );

    let StepDecision::DispatchRole(intent) = step(&state) else {
        panic!("queued continuation should dispatch");
    };
    assert_eq!(intent.task_id, Some(active));
    assert_eq!(intent.body, "finish the later serial task");
}
