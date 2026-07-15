//! Gate evaluation, ported from Zenith (Apache-2.0, Intelligent Internet)
//! `coordinator.py::_evaluate_gate` / `_upstream_validators`.
//!
//! A gate aggregates advisory verdicts with **AND semantics**: it clears
//! only when every one of its targets is covered by at least one upstream
//! validator and every covering validator's most recent verdict for that
//! target is a pass. An uncovered target, a validator that never reported a
//! target, or any single dissent blocks the gate.
//!
//! Gate status is **derived** — a pure function of task statuses and advisory
//! verdicts — never an event. A cleared gate still pauses for a human
//! checkpoint (zenith's discipline); a failed gate raises `gate_failed`.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::TaskId;
use super::plan::{Plan, TaskKind};
use super::state::MissionState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateResult {
    Cleared,
    Blocked { reason: String },
}

/// Evaluate a gate given the current advisory state. `gate` must be a gate
/// task in the plan.
pub fn evaluate_gate(state: &MissionState, plan: &Plan, gate_id: &TaskId) -> GateResult {
    let by_id: BTreeMap<&TaskId, &super::plan::Task> =
        plan.tasks.iter().map(|t| (&t.id, t)).collect();
    let Some(gate) = by_id.get(gate_id) else {
        return blocked(format!("gate '{gate_id}' is not in the plan"));
    };
    let validators = upstream_validators(&by_id, gate_id);

    let mut uncovered = Vec::new();
    let mut failed = Vec::new();
    for target in &gate.targets {
        // Covering validators = upstream validators whose targets include
        // this gate target.
        let covering: Vec<&TaskId> = validators
            .iter()
            .filter(|v| by_id.get(*v).is_some_and(|t| t.targets.contains(target)))
            .copied()
            .collect();
        if covering.is_empty() {
            uncovered.push(target.to_string());
            continue;
        }
        let advisory = state.contract.get(target).map(|a| &a.last_advisory);
        let all_pass = covering.iter().all(|v| {
            advisory
                .and_then(|verdicts| verdicts.get(*v))
                .copied()
                .unwrap_or(false) // never-reported counts as fail
        });
        if !all_pass {
            failed.push(target.to_string());
        }
    }

    if !uncovered.is_empty() {
        return blocked(format!("no validator covers: {}", uncovered.join(", ")));
    }
    if !failed.is_empty() {
        return blocked(format!("failed or unproven targets: {}", failed.join(", ")));
    }
    GateResult::Cleared
}

/// Transitive validate-type predecessors of a task (DFS over `depends_on`).
/// Shared with plan validation so a gate with an uncovered target is rejected
/// at author time rather than parking the mission at run time.
pub(crate) fn upstream_validators<'a>(
    by_id: &BTreeMap<&'a TaskId, &'a super::plan::Task>,
    start: &'a TaskId,
) -> BTreeSet<&'a TaskId> {
    let mut validators = BTreeSet::new();
    let mut stack = vec![start];
    let mut seen = BTreeSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(task) = by_id.get(id) else { continue };
        for dep in &task.depends_on {
            if let Some(dep_task) = by_id.get(dep) {
                if dep_task.kind == TaskKind::Validate {
                    validators.insert(dep);
                }
                stack.push(dep);
            }
        }
    }
    validators
}

fn blocked(reason: String) -> GateResult {
    GateResult::Blocked { reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::event::{
        EventEnvelope, Handoff, MissionEvent, PayloadRef, ValidationItem, VersionStamps,
    };
    use crate::model::fold::fold;
    use crate::model::ids::{AssertionId, EffectId, MissionId, RoleName};
    use crate::model::plan::{Assertion, Task};
    use crate::model::MissionConfig;

    fn aid(s: &str) -> AssertionId {
        AssertionId::new(s).unwrap()
    }
    fn tid(s: &str) -> TaskId {
        TaskId::new(s).unwrap()
    }

    fn plan_with(assertions: Vec<Assertion>, tasks: Vec<Task>) -> Plan {
        Plan {
            requirements: vec![],
            assertions,
            tasks,
        }
    }

    fn work(id: &str, targets: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Work,
            body: "b".into(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: Some(RoleName::new("implementer").unwrap()),
            depends_on: vec![],
        }
    }
    fn validate(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Validate,
            body: "b".into(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: Some(RoleName::new("reviewer").unwrap()),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }
    fn gate(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Gate,
            body: "".into(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: None,
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    /// Build a state by folding: create → accept a plan → validators report
    /// the given verdicts.
    fn state_with_verdicts(plan: Plan, verdicts: &[(&str, &[(&str, bool)])]) -> MissionState {
        let mission_id = MissionId::from_digest_prefix("abcdef0123456789");
        let mut events = vec![
            env(
                &mission_id,
                1,
                MissionEvent::MissionCreated {
                    objective: "o".into(),
                    mission_type: crate::model::MissionTypeRef {
                        name: "p".into(),
                        digest: "d".into(),
                    },
                    runtime: "codex".into(),
                    image_id: "img".into(),
                    workspace_dir: "/w".into(),
                    base_sha: "s0".into(),
                    config: MissionConfig {
                        ..Default::default()
                    },
                },
            ),
            env(
                &mission_id,
                2,
                MissionEvent::PlanProposed {
                    proposal: crate::model::PlanProposal {
                        base_revision: 0,
                        plan: plan.clone(),
                    },
                    plan_hash: "h".into(),
                },
            ),
            env(
                &mission_id,
                3,
                MissionEvent::DecisionRecorded {
                    attention_id: "plan_proposal:mission".into(),
                    action: crate::model::DecisionAction::Approve,
                    justification: "test fixture approves the plan".into(),
                },
            ),
        ];
        let mut seq = 4;
        for (validator, items) in verdicts {
            events.push(env(
                &mission_id,
                seq,
                MissionEvent::RoleRunRequested {
                    task_id: tid(validator),
                    attempt_no: 1,
                    effect_id: EffectId::for_parts(&["test", validator]),
                    role: RoleName::new("reviewer").unwrap(),
                    runtime: "codex".into(),
                    prompt: PayloadRef::inline("p"),
                    base_sha: "s0".into(),
                    assignment_epoch: 1,
                    recreate_workspace: true,
                    requested_at_ms: 0,
                    deadline_ms: 100_000,
                    budget_deadline_ms: 100_000,
                },
            ));
            seq += 1;
            events.push(env(
                &mission_id,
                seq,
                MissionEvent::RoleRunCompleted {
                    task_id: tid(validator),
                    attempt_no: 1,
                    effect_id: EffectId::for_parts(&["test", validator]),
                    outcome: Ok(crate::model::RoleRunSuccess {
                        handoff: Handoff::Validate {
                            done: true,
                            report: PayloadRef::inline("r"),
                            items: items
                                .iter()
                                .map(|(a, p)| ValidationItem {
                                    item_id: aid(a),
                                    passed: *p,
                                })
                                .collect(),
                            passed: items.iter().all(|(_, p)| *p),
                            request_attention: false,
                        },
                        artifact: None,
                        final_response: PayloadRef::inline("reviewed"),
                        runtime_configuration: crate::model::RuntimeConfigurationEvidence::default(
                        ),
                    }),
                },
            ));
            seq += 1;
        }
        fold(events).unwrap()
    }

    fn env(mission_id: &MissionId, seq: u64, event: MissionEvent) -> EventEnvelope {
        EventEnvelope {
            mission_id: mission_id.clone(),
            sequence_no: seq,
            recorded_at_ms: 0,
            stamps: VersionStamps::default(),
            event,
        }
    }

    #[test]
    fn unanimous_pass_clears() {
        let plan = plan_with(
            vec![Assertion {
                id: aid("AA"),
                prose: "a".into(),
                oracle: None,
            }],
            vec![
                work("w", &["AA"]),
                validate("v", &["AA"], &["w"]),
                gate("g", &["AA"], &["v"]),
            ],
        );
        let state = state_with_verdicts(plan.clone(), &[("v", &[("AA", true)])]);
        assert_eq!(evaluate_gate(&state, &plan, &tid("g")), GateResult::Cleared);
    }

    #[test]
    fn any_dissent_blocks() {
        let plan = plan_with(
            vec![Assertion {
                id: aid("AA"),
                prose: "a".into(),
                oracle: None,
            }],
            vec![
                work("w", &["AA"]),
                validate("v", &["AA"], &["w"]),
                gate("g", &["AA"], &["v"]),
            ],
        );
        let state = state_with_verdicts(plan.clone(), &[("v", &[("AA", false)])]);
        assert!(matches!(
            evaluate_gate(&state, &plan, &tid("g")),
            GateResult::Blocked { .. }
        ));
    }

    #[test]
    fn uncovered_target_blocks() {
        // Gate targets A and B, but only A has a covering validator.
        let plan = plan_with(
            vec![
                Assertion {
                    id: aid("AA"),
                    prose: "a".into(),
                    oracle: None,
                },
                Assertion {
                    id: aid("BB"),
                    prose: "b".into(),
                    oracle: None,
                },
            ],
            vec![
                work("wa", &["AA"]),
                work("wb", &["BB"]),
                validate("v", &["AA"], &["wa"]),
                gate("g", &["AA", "BB"], &["v"]),
            ],
        );
        let state = state_with_verdicts(plan.clone(), &[("v", &[("AA", true)])]);
        let result = evaluate_gate(&state, &plan, &tid("g"));
        assert!(matches!(&result, GateResult::Blocked { reason } if reason.contains("BB")));
    }

    #[test]
    fn never_reported_target_counts_as_fail() {
        // Validator covers A (declares it a target) but its handoff omitted A.
        let plan = plan_with(
            vec![Assertion {
                id: aid("AA"),
                prose: "a".into(),
                oracle: None,
            }],
            vec![
                work("w", &["AA"]),
                validate("v", &["AA"], &["w"]),
                gate("g", &["AA"], &["v"]),
            ],
        );
        let state = state_with_verdicts(plan.clone(), &[("v", &[])]);
        assert!(matches!(
            evaluate_gate(&state, &plan, &tid("g")),
            GateResult::Blocked { .. }
        ));
    }

    #[test]
    fn two_validators_and_semantics() {
        // Both validators cover A; one dissents → blocked.
        let plan = plan_with(
            vec![Assertion {
                id: aid("AA"),
                prose: "a".into(),
                oracle: None,
            }],
            vec![
                work("w", &["AA"]),
                validate("v1", &["AA"], &["w"]),
                validate("v2", &["AA"], &["w"]),
                gate("g", &["AA"], &["v1", "v2"]),
            ],
        );
        let pass = state_with_verdicts(
            plan.clone(),
            &[("v1", &[("AA", true)]), ("v2", &[("AA", true)])],
        );
        assert_eq!(evaluate_gate(&pass, &plan, &tid("g")), GateResult::Cleared);
        let dissent = state_with_verdicts(
            plan.clone(),
            &[("v1", &[("AA", true)]), ("v2", &[("AA", false)])],
        );
        assert!(matches!(
            evaluate_gate(&dissent, &plan, &tid("g")),
            GateResult::Blocked { .. }
        ));
    }
}
