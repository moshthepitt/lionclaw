use std::collections::{BTreeMap, BTreeSet};

use lionclaw_model::{
    fold, Assertion, AssertionId, AuthorityCeilings, AuthorityGrants, DecisionAction, EffectId,
    EventEnvelope, ExecutionPolicy, Handoff, MissionConfig, MissionEvent, MissionGuidance,
    MissionId, MissionProposal, MissionTypeRef, OracleName, OutputSemantics, PayloadRef, Plan,
    PlanProposal, RecoveryConfig, Requirement, RequirementDisposition, RequirementId,
    RequirementKind, RoleAttemptDisposition, RoleInstance, RoleInstanceId, RolePromptTemplate,
    RoleTurnSuccess, RuntimeConfigurationEvidence, StopBar, Task, TaskId, TeamRevision,
    VersionStamps, WorkspacePreparation, SCHEMA_VERSION,
};

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
        deadline_secs: None,
    }
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
fn sunset_wire_shapes_have_no_planning_or_role_bridges() {
    let config = MissionConfig {
        stop: StopBar::Verified,
        oracles: BTreeSet::from([OracleName::new("cargo-test").expect("oracle")]),
        ceilings: AuthorityCeilings::default(),
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
            disposition: RequirementDisposition::Covered {
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
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
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
        event(
            2,
            MissionEvent::TeamConfigured {
                team: team(0, false),
            },
        ),
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
        event(
            5,
            MissionEvent::TeamConfigured {
                team: proposal.team.unwrap(),
            },
        ),
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
        event(
            2,
            MissionEvent::TeamConfigured {
                team: team(0, false),
            },
        ),
        event(
            3,
            MissionEvent::TeamConfigured {
                team: team(2, false),
            },
        ),
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
        event(
            2,
            MissionEvent::TeamConfigured {
                team: team(0, false),
            },
        ),
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
        event(
            5,
            MissionEvent::TeamConfigured {
                team: accepted_team,
            },
        ),
        event(
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
                assignment_epoch: 1,
                message_boundary: 5,
                presented_messages: Vec::new(),
                workspace_preparation: WorkspacePreparation::ResetForAssignment,
                requested_at_ms: 6,
                deadline_ms: 60,
                budget_deadline_ms: 120,
            },
        ),
    ];
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
