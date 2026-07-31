use std::collections::BTreeMap;

use lionclaw_model::{
    apply, fold, next, validate_mission_proposal, Assertion, AssertionId, AuthorityCeilings,
    AuthorityGrants, Choice, CommandOracle, ConfinementResources, DecisionAction, EffectId,
    EffectIntent, EventEnvelope, ExternalOracle, ExternalOracleDriverId,
    ExternalOracleDriverIdentity, MissionConfig, MissionEvent, MissionProposal, MissionTypeRef,
    NetworkGrant, OracleName, OracleRunSuccess, OracleSpec, OutputSemantics, PayloadRef, Plan,
    PlanProposal, Requirement, RequirementDisposition, RequirementId, RequirementKind,
    RoleInstance, RoleInstanceId, RuntimeInstrumentIdentity, StopBar, Task, TaskId, TaskStatus,
    TeamRevision, VersionStamps, WorkspaceRelativeDir, SCHEMA_VERSION,
};

fn role(id: &str, output: OutputSemantics) -> RoleInstance {
    RoleInstance {
        id: RoleInstanceId::new(id).unwrap(),
        purpose: id.to_string(),
        output,
        runtime: "codex".to_string(),
        instructions: id.to_string(),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            writes: output == OutputSemantics::ProducesArtifact,
            ..Default::default()
        },
        resources: ConfinementResources::default(),
        deadline_secs: None,
    }
}

fn team() -> TeamRevision {
    let planner = role("planner", OutputSemantics::ProposesPlan);
    let worker = role("worker", OutputSemantics::ProducesArtifact);
    let reviewer = role("reviewer", OutputSemantics::EmitsVerdict);
    let task = TaskId::new("implement").unwrap();
    let assertion = AssertionId::new("TESTS-PASS").unwrap();
    TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner.clone()),
            (worker.id.clone(), worker.clone()),
            (reviewer.id.clone(), reviewer.clone()),
        ]),
        planning_assignment: planner.id,
        task_assignments: BTreeMap::from([(task, worker.id)]),
        judgment_assignments: BTreeMap::from([(assertion, vec![reviewer.id])]),
        gap_review_assignment: None,
        guidance: None,
    }
}

fn event(sequence_no: u64, event: MissionEvent) -> EventEnvelope {
    EventEnvelope {
        mission_id: lionclaw_model::MissionId::parse("mabc123abc123").unwrap(),
        sequence_no,
        recorded_at_ms: sequence_no as i64,
        stamps: VersionStamps {
            schema_version: SCHEMA_VERSION,
            engine_version: "test".to_string(),
            prompt_hash: None,
        },
        event,
    }
}

fn state() -> lionclaw_model::MissionState {
    fold(bootstrap_events()).unwrap()
}

fn bootstrap_events() -> Vec<EventEnvelope> {
    let team = team();
    let runtime_identities = team
        .roles
        .iter()
        .map(|(id, role)| {
            (
                id.clone(),
                RuntimeInstrumentIdentity {
                    runtime: role.runtime.clone(),
                    model: None,
                    mode: None,
                    model_network: NetworkGrant::Deny,
                    external_oracle_drivers: BTreeMap::new(),
                },
            )
        })
        .collect();
    vec![
        event(
            1,
            MissionEvent::MissionCreated {
                objective: "dynamic oracle".to_string(),
                mission_type: MissionTypeRef {
                    name: "software-dev".to_string(),
                    digest: "mission-type".to_string(),
                },
                image_id: "image".to_string(),
                workspace_dir: "/workspace".to_string(),
                base_sha: "base".to_string(),
                config: MissionConfig {
                    stop: StopBar::Verified,
                    ceilings: AuthorityCeilings {
                        writes: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
        ),
        event(
            2,
            MissionEvent::TeamConfigured {
                team,
                runtime_identities,
            },
        ),
    ]
}

fn accepted_events(spec: OracleSpec) -> Vec<EventEnvelope> {
    let mut events = bootstrap_events();
    events.push(event(
        3,
        MissionEvent::ProposalRecorded {
            proposal: Box::new(MissionProposal {
                plan: Some(PlanProposal {
                    base_revision: 0,
                    requirement_changes: Vec::new(),
                    assertion_supersessions: Vec::new(),
                    plan: plan(),
                }),
                team: None,
                oracles: Some(BTreeMap::from([(OracleName::new("tests").unwrap(), spec)])),
            }),
            proposal_hash: "proposal".to_string(),
        },
    ));
    events.push(event(
        4,
        MissionEvent::DecisionRecorded {
            attention_id: "plan_proposal:mission".to_string(),
            action: DecisionAction::Approve,
            justification: "approve the complete mission shape".to_string(),
            requirement_changes: Vec::new(),
            proposal_runtime_identities: BTreeMap::new(),
        },
    ));
    events
}

fn plan() -> Plan {
    let assertion = AssertionId::new("TESTS-PASS").unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("GREEN-TESTS").unwrap(),
            kind: RequirementKind::Validation,
            prose: "tests pass".to_string(),
            disposition: RequirementDisposition::ConfinedProvable {
                assertion_ids: vec![assertion.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: "tests pass".to_string(),
            oracle: Some(OracleName::new("tests").unwrap()),
        }],
        tasks: vec![Task {
            id: TaskId::new("implement").unwrap(),
            body: "make tests pass".to_string(),
            targets: vec![assertion],
            depends_on: Vec::new(),
        }],
    }
}

fn command(argv: &[&str]) -> OracleSpec {
    OracleSpec::Command(CommandOracle {
        argv: argv.iter().map(|arg| (*arg).to_string()).collect(),
        cwd: WorkspaceRelativeDir::new(".").unwrap(),
        environment: BTreeMap::new(),
        timeout_secs: 300,
        grants: AuthorityGrants::default(),
        resources: ConfinementResources::default(),
    })
}

fn command_mut(spec: &mut OracleSpec) -> &mut CommandOracle {
    match spec {
        OracleSpec::Command(command) => command,
        OracleSpec::External(_) => panic!("test helper expected a command oracle"),
    }
}

fn external_oracle() -> OracleSpec {
    OracleSpec::External(ExternalOracle {
        driver: ExternalOracleDriverId::new("local-ci").unwrap(),
        driver_identity: Some(ExternalOracleDriverIdentity {
            driver: ExternalOracleDriverId::new("local-ci").unwrap(),
            image_id: "image".to_string(),
            network: NetworkGrant::Deny,
            auth: None,
        }),
        request: BTreeMap::from([("suite".to_string(), "cargo-test".to_string())]),
        timeout_secs: 300,
        poll_secs: 5,
    })
}

#[test]
fn joint_proposal_validates_plan_against_new_oracle_map() {
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: None,
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            command(&["cargo", "test"]),
        )])),
    };

    validate_mission_proposal(&state(), &proposal)
        .expect("plan may bind an oracle introduced by the same proposal");
}

#[test]
fn oracle_replacement_requires_plan_proposal_for_revision_guard() {
    let proposal = MissionProposal {
        plan: None,
        team: None,
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            command(&["cargo", "test"]),
        )])),
    };

    let error = validate_mission_proposal(&state(), &proposal)
        .expect_err("oracle replacement without a base revision must fail");
    assert!(error
        .to_string()
        .contains("oracle change requires a plan proposal"));
}

#[test]
fn pending_external_oracle_does_not_advertise_deadline_extension() {
    let spec = external_oracle();
    let spec_digest = spec.digest();
    let mut events = accepted_events(spec);
    let oracle = OracleName::new("tests").unwrap();
    let effect_id = EffectId::for_oracle_request(
        &lionclaw_model::MissionId::parse("mabc123abc123").unwrap(),
        &oracle,
        &spec_digest,
        "base",
        1,
    );
    events.push(event(
        5,
        MissionEvent::OracleRunRequested {
            assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
            oracle,
            spec_digest,
            judged_sha: "base".to_string(),
            environment_digest: "image".to_string(),
            attempt_no: 1,
            effect_id,
            requested_at_ms: 5,
            deadline_ms: lionclaw_model::resolve_execution_deadline_ms(5, 300).unwrap(),
        },
    ));
    let state = fold(events).expect("pending external oracle state");
    let choices = next(&state).choices;

    assert!(
        choices
            .iter()
            .any(|choice| matches!(choice, Choice::Stop { .. })),
        "pending external oracle can still be stopped"
    );
    assert!(
        !choices
            .iter()
            .any(|choice| matches!(choice, Choice::ExtendDeadline { .. })),
        "external oracle idempotency identity has an immutable deadline"
    );
}

#[test]
fn unchanged_oracle_map_does_not_turn_a_team_revision_into_an_oracle_change() {
    let mut next_team = team();
    next_team.revision = 1;
    let proposal = MissionProposal {
        plan: None,
        team: Some(next_team),
        oracles: Some(BTreeMap::new()),
    };

    validate_mission_proposal(&state(), &proposal)
        .expect("an identical complete oracle map does not need a plan revision guard");
}

#[test]
fn declared_shell_executables_are_rejected() {
    let state = state();
    for executable in ["sh", "/bin/bash", "dash", "/usr/bin/zsh"] {
        let spec = command(&[executable, "-c", "printf injected"]);
        assert!(
            matches!(
                spec.validate(
                    &state.config.ceilings,
                    &state.config.resource_ceilings,
                    &state.config.execution,
                ),
                Err(lionclaw_model::OracleSpecError::ShellExecutable(_))
            ),
            "{executable} must not cross the command-oracle boundary"
        );
    }
}

#[test]
fn command_oracles_may_request_only_destination_scoped_network() {
    let ceilings = AuthorityCeilings {
        secrets: true,
        network: NetworkGrant::allow_single("api.example.com", 443).unwrap(),
        install: true,
        writes: true,
        ..Default::default()
    };
    let mut spec = command(&["curl", "https://api.example.com/health"]);
    command_mut(&mut spec).grants.network =
        NetworkGrant::allow_single("api.example.com", 443).unwrap();

    spec.validate(
        &ceilings,
        &ConfinementResources::default(),
        &Default::default(),
    )
    .expect("explicit network destination is allowed under ceiling");

    for mutate in [
        |grants: &mut AuthorityGrants| grants.secrets = true,
        |grants: &mut AuthorityGrants| grants.install = true,
        |grants: &mut AuthorityGrants| grants.writes = true,
    ] {
        let mut forbidden = spec.clone();
        mutate(&mut command_mut(&mut forbidden).grants);
        assert!(matches!(
            forbidden.validate(
                &ceilings,
                &ConfinementResources::default(),
                &Default::default(),
            ),
            Err(lionclaw_model::OracleSpecError::AuthorityViolatesProofFloor)
        ));
    }
}

#[test]
fn unchanged_plan_can_replace_oracles_using_the_plan_revision_guard() {
    let initial = command(&["cargo", "test"]);
    let replacement = command(&["cargo", "test", "--all-targets"]);
    let mut events = accepted_events(initial);
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 1,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: None,
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            replacement.clone(),
        )])),
    };
    validate_mission_proposal(&fold(events.clone()).unwrap(), &proposal)
        .expect("a real oracle replacement may carry the unchanged current plan");
    events.push(event(
        5,
        MissionEvent::ProposalRecorded {
            proposal: Box::new(proposal),
            proposal_hash: "replacement".to_string(),
        },
    ));
    events.push(event(
        6,
        MissionEvent::DecisionRecorded {
            attention_id: "plan_proposal:mission".to_string(),
            action: DecisionAction::Approve,
            justification: "replace the oracle map".to_string(),
            requirement_changes: Vec::new(),
            proposal_runtime_identities: BTreeMap::new(),
        },
    ));

    let promoted = fold(events).unwrap();
    assert_eq!(promoted.revision, 2);
    assert_eq!(promoted.plan, Some(plan()));
    assert_eq!(
        promoted.oracles[&OracleName::new("tests").unwrap()],
        replacement
    );
}

#[test]
fn oracle_replacement_is_stale_when_its_plan_base_revision_is_stale() {
    let current = fold(accepted_events(command(&["cargo", "test"]))).unwrap();
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: None,
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            command(&["cargo", "test", "--all-targets"]),
        )])),
    };

    let error =
        validate_mission_proposal(&current, &proposal).expect_err("stale oracle replacement");
    assert_eq!(
        error,
        lionclaw_model::ProposalError::Stale {
            base_revision: 0,
            current_revision: 1,
        }
    );
}

#[test]
fn oracle_attempt_counters_are_scoped_to_spec_digest() {
    let original = command(&["cargo", "test"]);
    let replacement = command(&["cargo", "test", "--all-targets"]);
    let original_digest = original.digest();
    let replacement_digest = replacement.digest();
    let oracle = OracleName::new("tests").unwrap();
    let mut state = fold(accepted_events(original.clone())).unwrap();
    for task in state.tasks.values_mut() {
        task.status = TaskStatus::Cleared;
    }

    let assertion_ids = vec![AssertionId::new("TESTS-PASS").unwrap()];
    let judged_sha = state.deliverable_head().to_string();
    let environment_digest = state.environment_digest().to_string();
    let effect_id =
        EffectId::for_oracle_request(&state.mission_id, &oracle, &original_digest, &judged_sha, 1);
    apply(
        &mut state,
        &event(
            5,
            MissionEvent::OracleRunRequested {
                assertion_ids: assertion_ids.clone(),
                oracle: oracle.clone(),
                spec_digest: original_digest.clone(),
                judged_sha: judged_sha.clone(),
                environment_digest,
                attempt_no: 1,
                effect_id: effect_id.clone(),
                requested_at_ms: 0,
                deadline_ms: 300_000,
            },
        ),
    );
    apply(
        &mut state,
        &event(
            6,
            MissionEvent::OracleRunCompleted {
                assertion_ids,
                oracle: oracle.clone(),
                spec_digest: original_digest,
                judged_sha,
                attempt_no: 1,
                effect_id,
                outcome: Err(lionclaw_model::TypedFailure::permanent(
                    "oracle.failed",
                    "fault injected",
                )),
            },
        ),
    );

    for (sequence_no, base_revision, spec) in
        [(7, 1, replacement.clone()), (9, 2, original.clone())]
    {
        apply(
            &mut state,
            &event(
                sequence_no,
                MissionEvent::ProposalRecorded {
                    proposal: Box::new(MissionProposal {
                        plan: Some(PlanProposal {
                            base_revision,
                            requirement_changes: Vec::new(),
                            assertion_supersessions: Vec::new(),
                            plan: plan(),
                        }),
                        team: None,
                        oracles: Some(BTreeMap::from([(oracle.clone(), spec)])),
                    }),
                    proposal_hash: format!("proposal-{sequence_no}"),
                },
            ),
        );
        apply(
            &mut state,
            &event(
                sequence_no + 1,
                MissionEvent::DecisionRecorded {
                    attention_id: "plan_proposal:mission".to_string(),
                    action: DecisionAction::Approve,
                    justification: "replace the command instrument".to_string(),
                    requirement_changes: Vec::new(),
                    proposal_runtime_identities: BTreeMap::new(),
                },
            ),
        );
        for task in state.tasks.values_mut() {
            task.status = TaskStatus::Cleared;
        }

        let dispatch = next(&state)
            .effects
            .into_iter()
            .find_map(|effect| match effect {
                EffectIntent::DispatchOracle(intent) => Some(intent),
                _ => None,
            })
            .expect("replacement oracle dispatch");
        if base_revision == 1 {
            assert_eq!(dispatch.spec_digest, replacement_digest);
            assert_eq!(dispatch.attempt_no, 1);
        } else {
            assert_eq!(dispatch.spec_digest, original.digest());
            assert_eq!(
                dispatch.attempt_no, 2,
                "restoring a prior digest must not reuse its first effect id"
            );
        }
    }
}

#[test]
fn command_digest_covers_behavior_and_authority() {
    let baseline = command(&["cargo", "test"]);
    let baseline_digest = baseline.digest();

    let mut variants = Vec::new();
    variants.push(command(&["cargo", "test", "--all-targets"]));

    let mut cwd = command(&["cargo", "test"]);
    command_mut(&mut cwd).cwd = WorkspaceRelativeDir::new("crates/kernel").unwrap();
    variants.push(cwd);

    let mut environment = command(&["cargo", "test"]);
    command_mut(&mut environment)
        .environment
        .insert("RUSTFLAGS".into(), "-Dwarnings".into());
    variants.push(environment);

    let mut timeout = command(&["cargo", "test"]);
    command_mut(&mut timeout).timeout_secs += 1;
    variants.push(timeout);

    let mut grants = command(&["cargo", "test"]);
    command_mut(&mut grants)
        .grants
        .devices
        .insert("nvidia.com/gpu=all".to_string());
    variants.push(grants);

    let mut resources = command(&["cargo", "test"]);
    command_mut(&mut resources)
        .resources
        .tmpfs
        .push("/tmp:rw,size=1g".to_string());
    variants.push(resources);

    for variant in variants {
        assert_ne!(variant.digest(), baseline_digest);
    }

    let mut resources = command(&["cargo", "test"]);
    command_mut(&mut resources).resources.tmpfs =
        vec!["/tmp:rw,size=1g".into(), "/cache:rw,size=2g".into()];
    let mut reordered = resources.clone();
    command_mut(&mut reordered).resources.tmpfs.reverse();
    assert_eq!(
        resources.digest(),
        reordered.digest(),
        "resource declaration order is not command behavior"
    );
}

#[test]
fn workspace_directory_rejects_noncanonical_paths() {
    for invalid in [
        "",
        "/workspace",
        "crates/../src",
        "./src",
        "src//lib",
        r"src\lib",
    ] {
        assert!(
            WorkspaceRelativeDir::new(invalid).is_err(),
            "{invalid:?} must not be admitted"
        );
    }
    assert_eq!(WorkspaceRelativeDir::new(".").unwrap().as_str(), ".");
    assert_eq!(
        WorkspaceRelativeDir::new("crates/lionclaw")
            .unwrap()
            .as_str(),
        "crates/lionclaw"
    );
}

#[test]
fn one_approval_event_promotes_plan_and_oracles_together() {
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: None,
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            command(&["cargo", "test"]),
        )])),
    };
    let mut events = bootstrap_events();
    events.push(event(
        3,
        MissionEvent::ProposalRecorded {
            proposal: Box::new(proposal),
            proposal_hash: "proposal".to_string(),
        },
    ));
    let pending = fold(events.clone()).unwrap();
    assert!(next(&pending).choices.contains(&Choice::Decide {
        id: "plan_proposal:mission".to_string(),
        action: DecisionAction::Approve,
    }));

    events.push(event(
        4,
        MissionEvent::DecisionRecorded {
            attention_id: "plan_proposal:mission".to_string(),
            action: DecisionAction::Approve,
            justification: "approve the complete mission shape".to_string(),
            requirement_changes: Vec::new(),
            proposal_runtime_identities: BTreeMap::new(),
        },
    ));
    let accepted = fold(events).unwrap();
    assert_eq!(accepted.revision, 1);
    assert!(accepted.proposal.is_none());
    assert_eq!(accepted.plan, Some(plan()));
    assert_eq!(
        accepted.oracles[&OracleName::new("tests").unwrap()],
        command(&["cargo", "test"])
    );
}

#[test]
fn invalid_team_identity_keeps_the_whole_proposal_pending() {
    let mut proposed_team = team();
    proposed_team.revision = 1;
    let proposal = MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan(),
        }),
        team: Some(proposed_team),
        oracles: Some(BTreeMap::from([(
            OracleName::new("tests").unwrap(),
            command(&["cargo", "test"]),
        )])),
    };
    let mut pending_events = bootstrap_events();
    pending_events.push(event(
        3,
        MissionEvent::ProposalRecorded {
            proposal: Box::new(proposal),
            proposal_hash: "proposal".to_string(),
        },
    ));
    let pending = fold(pending_events.clone()).unwrap();

    let invalid_identities = [
        BTreeMap::new(),
        BTreeMap::from([(
            RoleInstanceId::new("planner").unwrap(),
            RuntimeInstrumentIdentity {
                runtime: "wrong-runtime".to_string(),
                model: None,
                mode: None,
                model_network: NetworkGrant::Deny,
                external_oracle_drivers: BTreeMap::new(),
            },
        )]),
    ];
    for proposal_runtime_identities in invalid_identities {
        let mut events = pending_events.clone();
        events.push(event(
            4,
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".to_string(),
                action: DecisionAction::Approve,
                justification: "invalid runtime identity set".to_string(),
                requirement_changes: Vec::new(),
                proposal_runtime_identities,
            },
        ));
        let rejected = fold(events).unwrap();
        assert_eq!(rejected.revision, pending.revision);
        assert_eq!(rejected.team, pending.team);
        assert_eq!(rejected.plan, pending.plan);
        assert_eq!(rejected.oracles, pending.oracles);
        assert_eq!(rejected.proposal, pending.proposal);
    }
}

#[test]
fn old_spec_requests_and_outcomes_are_inert_and_spec_changes_stale_receipts() {
    let old_spec = command(&["cargo", "test"]);
    let current_spec = command(&["cargo", "test", "--all-targets"]);
    let old_digest = old_spec.digest();
    let current_digest = current_spec.digest();
    let mut state = fold(accepted_events(current_spec.clone())).unwrap();
    let oracle = OracleName::new("tests").unwrap();
    let assertion_ids = vec![AssertionId::new("TESTS-PASS").unwrap()];
    let judged_sha = state.deliverable_head().to_string();
    let environment_digest = state.environment_digest().to_string();
    for task in state.tasks.values_mut() {
        task.status = TaskStatus::Cleared;
    }

    let dispatch = next(&state)
        .effects
        .into_iter()
        .find_map(|effect| match effect {
            EffectIntent::DispatchOracle(intent) => Some(intent),
            _ => None,
        })
        .expect("current oracle dispatch");
    assert_eq!(dispatch.spec_digest, current_digest);

    let old_effect =
        EffectId::for_oracle_request(&state.mission_id, &oracle, &old_digest, &judged_sha, 1);
    apply(
        &mut state,
        &event(
            5,
            MissionEvent::OracleRunRequested {
                assertion_ids: assertion_ids.clone(),
                oracle: oracle.clone(),
                spec_digest: old_digest.clone(),
                judged_sha: judged_sha.clone(),
                environment_digest: environment_digest.clone(),
                attempt_no: 1,
                effect_id: old_effect.clone(),
                requested_at_ms: 0,
                deadline_ms: 30_000,
            },
        ),
    );
    assert!(!state.inflight.contains_key(&old_effect));

    let current_effect =
        EffectId::for_oracle_request(&state.mission_id, &oracle, &current_digest, &judged_sha, 1);
    apply(
        &mut state,
        &event(
            6,
            MissionEvent::OracleRunRequested {
                assertion_ids: assertion_ids.clone(),
                oracle: oracle.clone(),
                spec_digest: current_digest.clone(),
                judged_sha: judged_sha.clone(),
                environment_digest: environment_digest.clone(),
                attempt_no: 1,
                effect_id: current_effect.clone(),
                requested_at_ms: 0,
                deadline_ms: 1,
            },
        ),
    );
    assert!(
        !state.inflight.contains_key(&current_effect),
        "the recorded deadline may not redefine the digested timeout"
    );

    apply(
        &mut state,
        &event(
            7,
            MissionEvent::OracleRunRequested {
                assertion_ids: assertion_ids.clone(),
                oracle: oracle.clone(),
                spec_digest: current_digest.clone(),
                judged_sha: judged_sha.clone(),
                environment_digest,
                attempt_no: 1,
                effect_id: current_effect.clone(),
                requested_at_ms: 0,
                deadline_ms: 300_000,
            },
        ),
    );
    assert!(state.inflight.contains_key(&current_effect));

    let success = OracleRunSuccess {
        exit_code: 0,
        exit_signal: None,
        stdout: PayloadRef::inline("pass"),
        stderr: PayloadRef::inline(""),
        prepared_inputs: Vec::new(),
        duration_ms: 1,
    };
    apply(
        &mut state,
        &event(
            8,
            MissionEvent::OracleRunCompleted {
                assertion_ids: assertion_ids.clone(),
                oracle: oracle.clone(),
                spec_digest: old_digest,
                judged_sha: judged_sha.clone(),
                attempt_no: 1,
                effect_id: current_effect.clone(),
                outcome: Ok(success.clone()),
            },
        ),
    );
    assert!(state.inflight.contains_key(&current_effect));
    assert!(state.authoritative_receipts.is_empty());

    apply(
        &mut state,
        &event(
            9,
            MissionEvent::OracleRunCompleted {
                assertion_ids,
                oracle: oracle.clone(),
                spec_digest: current_digest.clone(),
                judged_sha,
                attempt_no: 1,
                effect_id: current_effect.clone(),
                outcome: Ok(success),
            },
        ),
    );
    let receipt = state.authoritative_receipts[&current_effect].clone();
    assert_eq!(receipt.spec_digest(), current_digest);
    assert!(receipt.is_fresh_at(&state));

    let mut variants = vec![old_spec];
    let mut cwd = current_spec.clone();
    command_mut(&mut cwd).cwd = WorkspaceRelativeDir::new("crates/lionclaw").unwrap();
    variants.push(cwd);
    let mut environment = current_spec.clone();
    command_mut(&mut environment)
        .environment
        .insert("RUSTFLAGS".into(), "-Dwarnings".into());
    variants.push(environment);
    let mut timeout = current_spec.clone();
    command_mut(&mut timeout).timeout_secs += 1;
    variants.push(timeout);
    let mut grants = current_spec.clone();
    command_mut(&mut grants).grants.devices.insert("gpu".into());
    variants.push(grants);
    let mut resources = current_spec.clone();
    command_mut(&mut resources)
        .resources
        .tmpfs
        .push("/tmp:rw,size=1g".into());
    variants.push(resources);

    for variant in variants {
        state.oracles.insert(oracle.clone(), variant);
        assert!(!receipt.is_fresh_at(&state));
    }

    state.oracles.insert(oracle, current_spec);
    let current_image = state.image_id.clone();
    state.image_id = "different-runtime-image".to_string();
    assert!(!receipt.is_fresh_at(&state));
    state.image_id = current_image;

    state.current_sha = "different-artifact".to_string();
    assert!(!receipt.is_fresh_at(&state));
}
