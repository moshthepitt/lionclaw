//! Fail-closed structural validation of a plan, ported from
//! Zenith (Apache-2.0, Intelligent Internet) `task_validation.py`.
//!
//! Check groups run in zenith's order and short-circuit per group: an id
//! error suppresses shape errors, and so on. A plan that fails any
//! group produces no event — the mission never advances on an invalid plan.
//!
//! Divergences: contract + task list validate together (one plan);
//! roles replace skills, and a task's role must carry compatible output
//! semantics (an artifact role works, a verdict role validates; the read-only
//! planning roles — report/proposal — are rejected on every execution task);
//! assertion oracle bindings must exist in the mission-type inventory. Id charset
//! rules are enforced by the id newtypes at every deserialization boundary, so
//! only duplicates are checked here.

use super::event::StopBar;
use super::ids::{OracleName, RoleName, TaskId};
use super::plan::{
    OutputSemantics, Plan, PlanProposal, PlanningDag, RequirementDisposition, TaskKind,
};
use super::state::MissionState;
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub struct PlanValidationError {
    pub code: &'static str,
    pub detail: String,
}

fn err(code: &'static str, detail: impl Into<String>) -> PlanValidationError {
    PlanValidationError {
        code,
        detail: detail.into(),
    }
}

/// The mission-type-supplied inventory the plan is validated against.
pub struct MissionTypeInventory {
    pub roles: BTreeMap<RoleName, OutputSemantics>,
    pub oracles: BTreeSet<OracleName>,
    /// The honesty bar the mission type declares — plans under `Verified` must
    /// be provable (every assertion bound to an oracle).
    pub stop: StopBar,
}

pub fn validate_plan(plan: &Plan, inventory: &MissionTypeInventory) -> Vec<PlanValidationError> {
    // Group 0: emptiness (zenith empty_contract / empty_task_list).
    if plan.assertions.is_empty() {
        return vec![err(
            "empty_contract",
            "mission has no contract assertions; a plan must state falsifiable claims",
        )];
    }
    if plan.tasks.is_empty() {
        return vec![err("empty_task_list", "plan has no tasks")];
    }
    if plan.requirements.is_empty() {
        return vec![err(
            "empty_requirements",
            "plan has no objective requirements",
        )];
    }

    // Group 1: id uniqueness (charset enforced by the newtypes).
    let errors = check_unique_ids(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 2: every objective requirement is explicitly covered or accepted
    // as a limitation, and every assertion proves at least one requirement.
    let errors = check_requirements(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 3: per-kind shape + role/oracle inventory resolution.
    let errors = check_shape(plan, inventory);
    if !errors.is_empty() {
        return errors;
    }
    // Group 4: dependency resolution.
    let errors = check_deps_resolve(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 5: acyclicity (Kahn).
    let errors = check_acyclic(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 6: coverage.
    let errors = check_coverage(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 7: every gate target has an upstream validator (else the gate can
    // never clear — reject at author time instead of parking at run time).
    let errors = check_gate_coverage(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 8: the declared stop bar is reachable. A `Verified` mission must
    // launch fully provable — every assertion bound to an oracle as proposed.
    // An oracle-less assertion is rejected at author time. (A later complete
    // proposal *could* bind one, so this is a
    // launch-time policy, not a permanence claim; a domain with genuinely
    // unprovable claims declares `stop = reviewed`.)
    check_stop_bar_reachable(plan, inventory.stop)
}

/// Validate a mission type's planning DAG (at load, fail-closed): unique ids,
/// resolvable acyclic deps, every role a planning role (`ProducesReport` or the
/// single `ProposesPlan` author), and the author is the unique sink — so the
/// DAG fully drains into the proposer with no orphan island.
pub fn validate_planning_dag(
    dag: &PlanningDag,
    inventory: &MissionTypeInventory,
) -> Vec<PlanValidationError> {
    // An empty DAG is valid: it means "no in-engine planning" (the mission
    // awaits a manually proposed plan).
    if dag.tasks.is_empty() {
        return Vec::new();
    }
    let mut ids = BTreeSet::new();
    for t in &dag.tasks {
        if !ids.insert(&t.id) {
            return vec![err(
                "duplicate_task_id",
                format!("planning task '{}' is declared twice", t.id),
            )];
        }
        if t.id.as_str() == super::ids::TERMINAL_REVIEW_TASK_TAG {
            return vec![err(
                "reserved_task_id",
                format!(
                    "planning task id '{}' is reserved for the terminal reviewer",
                    t.id
                ),
            )];
        }
    }

    let mut errors = Vec::new();
    let mut proposers = 0;
    for t in &dag.tasks {
        for dep in &t.depends_on {
            if dep == &t.id {
                errors.push(err(
                    "self_loop",
                    format!("planning task '{}' depends on itself", t.id),
                ));
            } else if !ids.contains(dep) {
                errors.push(err(
                    "dep_unknown_task",
                    format!("planning task '{}' depends on unknown '{dep}'", t.id),
                ));
            }
        }
        match inventory.roles.get(&t.role) {
            None => errors.push(err(
                "unknown_role",
                format!(
                    "planning task '{}' names role '{}' which the mission type does not provide",
                    t.id, t.role
                ),
            )),
            Some(OutputSemantics::ProposesPlan) => proposers += 1,
            Some(OutputSemantics::ProducesReport) => {}
            Some(other) => errors.push(err(
                "role_output_mismatch",
                format!(
                    "planning role '{}' must be produces-report or proposes-plan, not {other:?}",
                    t.role
                ),
            )),
        }
    }
    if !errors.is_empty() {
        return errors;
    }

    if proposers != 1 {
        return vec![err(
            "planning_author",
            format!("a planning DAG must have exactly one proposes-plan author, found {proposers}"),
        )];
    }
    if planning_has_cycle(dag) {
        return vec![err(
            "cycle_detected",
            "the planning DAG has a dependency cycle",
        )];
    }
    // The author must be the unique sink: no node depends on it, and it is the
    // only node nothing depends on — every path drains into the proposer.
    let has_successor: BTreeSet<&TaskId> = dag.tasks.iter().flat_map(|t| &t.depends_on).collect();
    let sinks: Vec<_> = dag
        .tasks
        .iter()
        .filter(|t| !has_successor.contains(&t.id))
        .collect();
    let author_is_unique_sink = sinks.len() == 1
        && inventory.roles.get(&sinks[0].role) == Some(&OutputSemantics::ProposesPlan);
    if !author_is_unique_sink {
        return vec![err(
            "planning_sink",
            "the proposes-plan author must be the unique sink of the planning DAG \
             (every node drains into it)",
        )];
    }
    Vec::new()
}

fn planning_has_cycle(dag: &PlanningDag) -> bool {
    // Kahn: repeatedly remove nodes whose deps are all removed; a remainder is a
    // cycle.
    let mut remaining: BTreeSet<&TaskId> = dag.tasks.iter().map(|t| &t.id).collect();
    loop {
        let ready: Vec<&TaskId> = dag
            .tasks
            .iter()
            .filter(|t| remaining.contains(&t.id))
            .filter(|t| t.depends_on.iter().all(|d| !remaining.contains(d)))
            .map(|t| &t.id)
            .collect();
        if ready.is_empty() {
            return !remaining.is_empty();
        }
        for id in ready {
            remaining.remove(id);
        }
    }
}

fn check_stop_bar_reachable(plan: &Plan, stop: StopBar) -> Vec<PlanValidationError> {
    if stop != StopBar::Verified {
        return Vec::new();
    }
    plan.assertions
        .iter()
        .filter(|a| a.oracle.is_none())
        .map(|a| {
            err(
                "assertion_unprovable",
                format!(
                    "assertion '{}' binds no oracle, so it can never be authoritatively \
                     Verified; bind an oracle, or declare `stop = reviewed`",
                    a.id
                ),
            )
        })
        .collect()
}

/// Why a complete plan proposal is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProposalError {
    #[error(
        "proposal targets revision {base_revision}, but the current revision is {current_revision}"
    )]
    Stale {
        base_revision: u32,
        current_revision: u32,
    },
    #[error("proposal is immaterial (it leaves the plan unchanged)")]
    Immaterial,
    #[error("task '{task}' changes an existing task; retained task ids are immutable")]
    TaskChanged { task: String },
    #[error("new task '{task}' reuses a retired task id")]
    TaskIdReused { task: String },
    #[error("requirement '{requirement}' was removed or weakened")]
    RequirementWeakened { requirement: String },
    #[error("assertion '{assertion}' was removed or weakened")]
    AssertionWeakened { assertion: String },
    #[error("the resulting plan is invalid:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Invalid(Vec<PlanValidationError>),
}

/// Validate the complete candidate plan at the proposal boundary. The normal
/// whole-plan validator owns structural correctness; this function owns only
/// revision monotonicity and id history.
pub fn validate_plan_proposal(
    state: &MissionState,
    proposal: &PlanProposal,
    inventory: &MissionTypeInventory,
) -> Result<(), ProposalError> {
    validate_plan_transition(state, proposal)?;
    let errors = validate_plan(&proposal.plan, inventory);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ProposalError::Invalid(errors))
    }
}

/// Validate the revision relationship without mission-type inventory. Shared
/// with the fold so a malformed persisted proposal cannot weaken the contract
/// even if an event bypassed the engine boundary.
pub(crate) fn validate_plan_transition(
    state: &MissionState,
    proposal: &PlanProposal,
) -> Result<(), ProposalError> {
    if proposal.base_revision != state.revision {
        return Err(ProposalError::Stale {
            base_revision: proposal.base_revision,
            current_revision: state.revision,
        });
    }
    let Some(current) = state.plan.as_ref() else {
        return Ok(());
    };
    if proposal.plan == *current {
        return Err(ProposalError::Immaterial);
    }

    let next_requirements: BTreeMap<_, _> = proposal
        .plan
        .requirements
        .iter()
        .map(|r| (&r.id, r))
        .collect();
    for old in &current.requirements {
        let Some(new) = next_requirements.get(&old.id) else {
            return Err(ProposalError::RequirementWeakened {
                requirement: old.id.to_string(),
            });
        };
        let disposition_strengthens = match (&old.disposition, &new.disposition) {
            (
                RequirementDisposition::Covered {
                    assertion_ids: old_ids,
                },
                RequirementDisposition::Covered {
                    assertion_ids: new_ids,
                },
            ) => old_ids.iter().all(|id| new_ids.contains(id)),
            (
                RequirementDisposition::Limitation {
                    rationale: old_reason,
                },
                RequirementDisposition::Limitation {
                    rationale: new_reason,
                },
            ) => old_reason == new_reason,
            (RequirementDisposition::Limitation { .. }, RequirementDisposition::Covered { .. }) => {
                true
            }
            (RequirementDisposition::Covered { .. }, RequirementDisposition::Limitation { .. }) => {
                false
            }
        };
        if old.kind != new.kind || old.prose != new.prose || !disposition_strengthens {
            return Err(ProposalError::RequirementWeakened {
                requirement: old.id.to_string(),
            });
        }
    }

    let next_assertions: BTreeMap<_, _> = proposal
        .plan
        .assertions
        .iter()
        .map(|a| (&a.id, a))
        .collect();
    for old in &current.assertions {
        let strengthens = next_assertions.get(&old.id).is_some_and(|new| {
            old.prose == new.prose
                && (old.oracle == new.oracle || (old.oracle.is_none() && new.oracle.is_some()))
        });
        if !strengthens {
            return Err(ProposalError::AssertionWeakened {
                assertion: old.id.to_string(),
            });
        }
    }

    let current_tasks: BTreeMap<_, _> = current.tasks.iter().map(|t| (&t.id, t)).collect();
    for task in &proposal.plan.tasks {
        match current_tasks.get(&task.id) {
            Some(old) if *old != task => {
                return Err(ProposalError::TaskChanged {
                    task: task.id.to_string(),
                });
            }
            Some(_) => {}
            None if state.tasks.contains_key(&task.id) => {
                return Err(ProposalError::TaskIdReused {
                    task: task.id.to_string(),
                });
            }
            None => {}
        }
    }
    Ok(())
}

fn check_requirements(plan: &Plan) -> Vec<PlanValidationError> {
    let assertion_ids: BTreeSet<_> = plan.assertions.iter().map(|a| &a.id).collect();
    let mut referenced = BTreeSet::new();
    let mut errors = Vec::new();
    for requirement in &plan.requirements {
        if requirement.prose.trim().is_empty() {
            errors.push(err(
                "empty_requirement",
                format!("requirement '{}' has empty prose", requirement.id),
            ));
        }
        match &requirement.disposition {
            RequirementDisposition::Covered { assertion_ids: ids } => {
                if ids.is_empty() {
                    errors.push(err(
                        "requirement_uncovered",
                        format!("requirement '{}' covers no assertions", requirement.id),
                    ));
                }
                for id in ids {
                    if !assertion_ids.contains(id) {
                        errors.push(err(
                            "requirement_unknown_assertion",
                            format!(
                                "requirement '{}' references unknown assertion '{id}'",
                                requirement.id
                            ),
                        ));
                    }
                    referenced.insert(id);
                }
            }
            RequirementDisposition::Limitation { rationale } if rationale.trim().is_empty() => {
                errors.push(err(
                    "empty_limitation",
                    format!("requirement '{}' has an empty limitation", requirement.id),
                ));
            }
            RequirementDisposition::Limitation { .. } => {}
        }
    }
    for assertion in &plan.assertions {
        if !referenced.contains(&assertion.id) {
            errors.push(err(
                "assertion_without_requirement",
                format!(
                    "assertion '{}' does not cover an objective requirement",
                    assertion.id
                ),
            ));
        }
    }
    errors
}
fn check_gate_coverage(plan: &Plan) -> Vec<PlanValidationError> {
    let by_id: BTreeMap<&TaskId, &super::plan::Task> =
        plan.tasks.iter().map(|t| (&t.id, t)).collect();
    let mut errors = Vec::new();
    for gate in plan.tasks.iter().filter(|t| t.kind == TaskKind::Gate) {
        let validators = super::gate::upstream_validators(&by_id, &gate.id);
        for target in &gate.targets {
            let covered = validators
                .iter()
                .any(|v| by_id.get(*v).is_some_and(|t| t.targets.contains(target)));
            if !covered {
                errors.push(err(
                    "gate_target_uncovered",
                    format!(
                        "gate '{}' target '{target}' has no upstream validator; it can never clear",
                        gate.id
                    ),
                ));
            }
        }
    }
    errors
}

fn check_unique_ids(plan: &Plan) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let mut seen_requirements = BTreeSet::new();
    for requirement in &plan.requirements {
        if !seen_requirements.insert(&requirement.id) {
            errors.push(err(
                "duplicate_requirement_id",
                format!("requirement '{}' declared more than once", requirement.id),
            ));
        }
    }
    let mut seen_assertions = BTreeSet::new();
    for assertion in &plan.assertions {
        if !seen_assertions.insert(&assertion.id) {
            errors.push(err(
                "duplicate_assertion_id",
                format!("assertion '{}' declared more than once", assertion.id),
            ));
        }
    }
    let mut seen_tasks = BTreeSet::new();
    for task in &plan.tasks {
        if !seen_tasks.insert(&task.id) {
            errors.push(err(
                "duplicate_task_id",
                format!("task '{}' declared more than once", task.id),
            ));
        }
        // The terminal reviewer's runner tag shares the attempt-dir namespace
        // with plan tasks; a task by this name could leave crashed-attempt
        // dirs the closing reviewer would silently reuse.
        if task.id.as_str() == super::ids::TERMINAL_REVIEW_TASK_TAG {
            errors.push(err(
                "reserved_task_id",
                format!(
                    "task id '{}' is reserved for the terminal reviewer",
                    task.id
                ),
            ));
        }
    }
    errors
}

fn check_shape(plan: &Plan, inventory: &MissionTypeInventory) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    for assertion in &plan.assertions {
        if let Some(oracle) = &assertion.oracle {
            if !inventory.oracles.contains(oracle) {
                errors.push(err(
                    "unknown_oracle",
                    format!(
                        "assertion '{}' binds oracle '{oracle}' which the mission type does not provide",
                        assertion.id
                    ),
                ));
            }
        }
    }
    let known: BTreeSet<_> = plan.assertions.iter().map(|a| &a.id).collect();
    for task in &plan.tasks {
        match task.kind {
            TaskKind::Gate => {
                if task.role.is_some() {
                    errors.push(err(
                        "gate_with_role",
                        format!("gate '{}' must not name a role", task.id),
                    ));
                }
                if !task.body.is_empty() {
                    errors.push(err(
                        "gate_with_body",
                        format!("gate '{}' must have an empty body", task.id),
                    ));
                }
                if task.targets.is_empty() {
                    errors.push(err(
                        "empty_targets",
                        format!("gate '{}' must target at least one assertion", task.id),
                    ));
                }
            }
            TaskKind::Work | TaskKind::Validate => {
                if task.body.is_empty() {
                    errors.push(err(
                        "missing_body",
                        format!("task '{}' has no body", task.id),
                    ));
                }
                if task.kind == TaskKind::Validate && task.targets.is_empty() {
                    errors.push(err(
                        "empty_targets",
                        format!(
                            "validate task '{}' must target at least one assertion",
                            task.id
                        ),
                    ));
                }
                let Some(role) = &task.role else {
                    errors.push(err(
                        "missing_role",
                        format!("task '{}' names no role", task.id),
                    ));
                    continue;
                };
                let Some(output) = inventory.roles.get(role) else {
                    errors.push(err(
                        "unknown_role",
                        format!(
                            "task '{}' names role '{role}' which the mission type does not provide",
                            task.id
                        ),
                    ));
                    continue;
                };
                // Routing is bound to output semantics, never to names. The
                // planning-only outputs are incompatible with *every* execution
                // kind — this single chokepoint keeps a report/proposal role out
                // of an executed plan (closing planning recursion and the
                // manual-proposal hole).
                let compatible = output.execution_task_kind() == Some(task.kind);
                if !compatible {
                    errors.push(err(
                        "role_output_mismatch",
                        format!(
                            "task '{}' ({:?}) is incompatible with role '{role}' output semantics {:?}",
                            task.id, task.kind, output
                        ),
                    ));
                }
            }
        }
        for target in &task.targets {
            if !known.contains(target) {
                errors.push(err(
                    "task_targets_unknown_assertion",
                    format!("task '{}' targets unknown assertion '{target}'", task.id),
                ));
            }
        }
    }
    errors
}

fn check_deps_resolve(plan: &Plan) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let ids: BTreeSet<_> = plan.tasks.iter().map(|t| &t.id).collect();
    for task in &plan.tasks {
        for dep in &task.depends_on {
            if dep == &task.id {
                errors.push(err(
                    "self_loop",
                    format!("task '{}' depends on itself", task.id),
                ));
            } else if !ids.contains(dep) {
                errors.push(err(
                    "dep_unknown_task",
                    format!("task '{}' depends on unknown task '{dep}'", task.id),
                ));
            }
        }
    }
    errors
}

fn check_acyclic(plan: &Plan) -> Vec<PlanValidationError> {
    let mut indegree: BTreeMap<&TaskId, usize> = BTreeMap::new();
    let mut successors: BTreeMap<&TaskId, Vec<&TaskId>> = BTreeMap::new();
    for task in &plan.tasks {
        indegree.entry(&task.id).or_insert(0);
        for dep in &task.depends_on {
            *indegree.entry(&task.id).or_insert(0) += 1;
            successors.entry(dep).or_default().push(&task.id);
        }
    }
    let mut queue: Vec<&TaskId> = indegree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut visited = 0usize;
    while let Some(id) = queue.pop() {
        visited += 1;
        for succ in successors.get(id).into_iter().flatten() {
            let deg = indegree
                .get_mut(succ)
                .expect("successor is a declared task");
            *deg -= 1;
            if *deg == 0 {
                queue.push(succ);
            }
        }
    }
    if visited == plan.tasks.len() {
        return Vec::new();
    }
    let remaining: Vec<String> = indegree
        .iter()
        .filter(|(_, deg)| **deg > 0)
        .map(|(id, _)| id.to_string())
        .collect();
    vec![err(
        "cycle_detected",
        format!("dependency cycle among tasks: {}", remaining.join(", ")),
    )]
}

/// Zenith invariant: each assertion has exactly one active work coverer.
fn check_coverage(plan: &Plan) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    // Distinct coverer task ids per assertion: a single work task that lists the
    // same assertion twice in `targets` covers it once, not twice.
    let mut coverers: BTreeMap<_, BTreeSet<&TaskId>> = BTreeMap::new();
    for task in &plan.tasks {
        if task.kind != TaskKind::Work {
            continue;
        }
        for target in &task.targets {
            coverers.entry(target).or_default().insert(&task.id);
        }
    }
    for assertion in &plan.assertions {
        match coverers.get(&assertion.id).map(BTreeSet::len) {
            None | Some(0) => errors.push(err(
                "uncovered_assertion",
                format!("assertion '{}' has no work task covering it", assertion.id),
            )),
            Some(1) => {}
            Some(n) => errors.push(err(
                "over_covered_assertion",
                format!(
                    "assertion '{}' is covered by {n} work tasks; exactly one is required",
                    assertion.id,
                ),
            )),
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AssertionId, RequirementId};
    use crate::plan::{Assertion, PlanningTask, Requirement, RequirementKind, Task};

    fn aid(raw: &str) -> AssertionId {
        AssertionId::new(raw).expect("valid assertion id")
    }

    fn tid(raw: &str) -> TaskId {
        TaskId::new(raw).expect("valid task id")
    }

    fn assertion(id: &str) -> Assertion {
        Assertion {
            id: aid(id),
            prose: format!("claim {id}"),
            oracle: None,
        }
    }

    fn assertion_with_oracle(id: &str, oracle: &str) -> Assertion {
        Assertion {
            id: aid(id),
            prose: format!("claim {id}"),
            oracle: Some(OracleName::new(oracle).expect("valid oracle name")),
        }
    }

    fn task(
        id: &str,
        kind: TaskKind,
        role: Option<&str>,
        body: &str,
        targets: &[&str],
        deps: &[&str],
    ) -> Task {
        Task {
            id: tid(id),
            kind,
            body: body.to_string(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: role.map(|r| RoleName::new(r).expect("valid role name")),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    fn work(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        task(
            id,
            TaskKind::Work,
            Some("implementer"),
            "produce it",
            targets,
            deps,
        )
    }

    fn validate(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        task(
            id,
            TaskKind::Validate,
            Some("checker"),
            "check it",
            targets,
            deps,
        )
    }

    fn gate(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        task(id, TaskKind::Gate, None, "", targets, deps)
    }

    fn inventory() -> MissionTypeInventory {
        let mut roles = BTreeMap::new();
        roles.insert(
            RoleName::new("implementer").expect("valid role name"),
            OutputSemantics::ProducesArtifact,
        );
        roles.insert(
            RoleName::new("checker").expect("valid role name"),
            OutputSemantics::EmitsVerdict,
        );
        // Planning-only roles, for the planning-DAG and planning-role-on-an-
        // execution-task tests.
        roles.insert(
            RoleName::new("reporter").expect("valid role name"),
            OutputSemantics::ProducesReport,
        );
        roles.insert(
            RoleName::new("author").expect("valid role name"),
            OutputSemantics::ProposesPlan,
        );
        MissionTypeInventory {
            roles,
            oracles: BTreeSet::from([OracleName::new("cargo-test").expect("valid oracle name")]),
            // `Reviewed` so these structural tests aren't also subject to the
            // stop-bar-reachability check (exercised separately below).
            stop: StopBar::Reviewed,
        }
    }

    fn plan(assertions: Vec<Assertion>, tasks: Vec<Task>) -> Plan {
        let requirements = assertions
            .iter()
            .enumerate()
            .map(|(index, assertion)| Requirement {
                id: RequirementId::new(format!("REQ-{}", index + 1)).unwrap(),
                kind: RequirementKind::Capability,
                prose: format!("requirement for {}", assertion.id),
                disposition: RequirementDisposition::Covered {
                    assertion_ids: vec![assertion.id.clone()],
                },
            })
            .collect();
        Plan {
            requirements,
            assertions,
            tasks,
        }
    }

    fn codes(plan: &Plan) -> Vec<&'static str> {
        validate_plan(plan, &inventory())
            .into_iter()
            .map(|e| e.code)
            .collect()
    }

    const CLEAN: Vec<&str> = Vec::new();

    fn ptask(id: &str, role: &str, deps: &[&str]) -> PlanningTask {
        PlanningTask {
            id: tid(id),
            role: RoleName::new(role).expect("valid role name"),
            body: format!("do {id}"),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    fn planning_codes(tasks: Vec<PlanningTask>) -> Vec<&'static str> {
        validate_planning_dag(&PlanningDag { tasks }, &inventory())
            .into_iter()
            .map(|e| e.code)
            .collect()
    }

    // Fault-injection coverage for the planning-DAG guards unique to the loader:
    // exactly-one-author, author-is-the-unique-sink, and cycle detection have no
    // other test, so a fail-open regression would otherwise ship silently.
    #[test]
    fn planning_dag_guards() {
        // Valid: research → author, the author the unique sink.
        assert_eq!(
            planning_codes(vec![
                ptask("research", "reporter", &[]),
                ptask("author", "author", &["research"]),
            ]),
            CLEAN
        );
        // Empty is valid (no in-engine planning).
        assert_eq!(planning_codes(vec![]), CLEAN);
        // Two proposers → planning_author.
        assert_eq!(
            planning_codes(vec![
                ptask("a1", "author", &[]),
                ptask("a2", "author", &["a1"]),
            ]),
            vec!["planning_author"]
        );
        // Zero proposers → planning_author.
        assert_eq!(
            planning_codes(vec![ptask("r", "reporter", &[])]),
            vec!["planning_author"]
        );
        // A report node that doesn't drain into the author → planning_sink.
        assert_eq!(
            planning_codes(vec![
                ptask("orphan", "reporter", &[]),
                ptask("author", "author", &[]),
            ]),
            vec!["planning_sink"]
        );
        // A dependency cycle → cycle_detected.
        assert_eq!(
            planning_codes(vec![
                ptask("a", "reporter", &["author"]),
                ptask("author", "author", &["a"]),
            ]),
            vec!["cycle_detected"]
        );
        // A duplicate id → duplicate_task_id.
        assert_eq!(
            planning_codes(vec![
                ptask("dup", "reporter", &[]),
                ptask("dup", "author", &["dup"]),
            ]),
            vec!["duplicate_task_id"]
        );
        // An execution role in the planning DAG → role_output_mismatch.
        assert_eq!(
            planning_codes(vec![ptask("author", "implementer", &[])]),
            vec!["role_output_mismatch"]
        );
        // A dependency on an unknown planning task → dep_unknown_task.
        assert_eq!(
            planning_codes(vec![
                ptask("research", "reporter", &["ghost"]),
                ptask("author", "author", &["research"]),
            ]),
            vec!["dep_unknown_task"]
        );
    }

    // The check_shape chokepoint: a read-only planning role (produces-report /
    // proposes-plan) must never masquerade as an execution worker in a proposed
    // plan. Neither arm was exercised before.
    #[test]
    fn a_planning_role_on_an_execution_task_is_rejected() {
        let work_on_reporter = plan(
            vec![assertion_with_oracle("AA", "cargo-test")],
            vec![task(
                "t",
                TaskKind::Work,
                Some("reporter"),
                "do",
                &["AA"],
                &[],
            )],
        );
        assert!(codes(&work_on_reporter).contains(&"role_output_mismatch"));
        let validate_on_author = plan(
            vec![assertion_with_oracle("AA", "cargo-test")],
            vec![
                work("w", &["AA"], &[]),
                task(
                    "t",
                    TaskKind::Validate,
                    Some("author"),
                    "check",
                    &["AA"],
                    &[],
                ),
            ],
        );
        assert!(codes(&validate_on_author).contains(&"role_output_mismatch"));
    }

    // A gate whose target has no upstream validator can never clear — reject it
    // at author time (group 6) rather than parking at run time. The only
    // fault-injection test for this guard.
    #[test]
    fn a_gate_over_an_assertion_with_no_validator_is_rejected() {
        let sub = plan(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &[]), gate("g1", &["A1"], &["w1"])],
        );
        assert_eq!(codes(&sub), vec!["gate_target_uncovered"]);
    }

    #[test]
    fn empty_contract_returned_alone() {
        // Even with an empty task list...
        let sub = plan(vec![], vec![]);
        assert_eq!(codes(&sub), vec!["empty_contract"]);
        // ...or a task list full of shape errors, only empty_contract returns.
        let bad_gate = task(
            "g1",
            TaskKind::Gate,
            Some("implementer"),
            "body",
            &[],
            &["g1"],
        );
        let sub = plan(vec![], vec![bad_gate]);
        assert_eq!(codes(&sub), vec!["empty_contract"]);
    }

    #[test]
    fn empty_task_list_rejected() {
        let sub = plan(vec![assertion("A1")], vec![]);
        assert_eq!(codes(&sub), vec!["empty_task_list"]);
    }

    #[test]
    fn duplicate_ids_accumulate_within_the_group() {
        let sub = plan(
            vec![assertion("A1"), assertion("A1")],
            vec![work("w1", &["A1"], &[]), work("w1", &["A1"], &[])],
        );
        assert_eq!(
            codes(&sub),
            vec!["duplicate_assertion_id", "duplicate_task_id"]
        );
    }

    #[test]
    fn shape_errors() {
        let cases: Vec<(&str, Plan, Vec<&str>)> = vec![
            (
                "gate_with_role",
                plan(
                    vec![assertion("A1")],
                    vec![task(
                        "g1",
                        TaskKind::Gate,
                        Some("implementer"),
                        "",
                        &["A1"],
                        &[],
                    )],
                ),
                vec!["gate_with_role"],
            ),
            (
                "gate_with_body",
                plan(
                    vec![assertion("A1")],
                    vec![task("g1", TaskKind::Gate, None, "not empty", &["A1"], &[])],
                ),
                vec!["gate_with_body"],
            ),
            (
                "gate_empty_targets",
                plan(vec![assertion("A1")], vec![gate("g1", &[], &[])]),
                vec!["empty_targets"],
            ),
            (
                "validate_empty_targets",
                plan(vec![assertion("A1")], vec![validate("v1", &[], &[])]),
                vec!["empty_targets"],
            ),
            (
                "missing_body",
                plan(
                    vec![assertion("A1")],
                    vec![task(
                        "w1",
                        TaskKind::Work,
                        Some("implementer"),
                        "",
                        &["A1"],
                        &[],
                    )],
                ),
                vec!["missing_body"],
            ),
            (
                "missing_role",
                plan(
                    vec![assertion("A1")],
                    vec![task("w1", TaskKind::Work, None, "body", &["A1"], &[])],
                ),
                vec!["missing_role"],
            ),
            (
                "unknown_role",
                plan(
                    vec![assertion("A1")],
                    vec![task(
                        "w1",
                        TaskKind::Work,
                        Some("stranger"),
                        "body",
                        &["A1"],
                        &[],
                    )],
                ),
                vec!["unknown_role"],
            ),
            (
                "verdict_role_on_work_task",
                plan(
                    vec![assertion("A1")],
                    vec![task(
                        "w1",
                        TaskKind::Work,
                        Some("checker"),
                        "body",
                        &["A1"],
                        &[],
                    )],
                ),
                vec!["role_output_mismatch"],
            ),
            (
                "artifact_role_on_validate_task",
                plan(
                    vec![assertion("A1")],
                    vec![task(
                        "v1",
                        TaskKind::Validate,
                        Some("implementer"),
                        "body",
                        &["A1"],
                        &[],
                    )],
                ),
                vec!["role_output_mismatch"],
            ),
            (
                "unknown_oracle",
                plan(
                    vec![assertion_with_oracle("A1", "psychic")],
                    vec![work("w1", &["A1"], &[])],
                ),
                vec!["unknown_oracle"],
            ),
            (
                "task_targets_unknown_assertion",
                plan(vec![assertion("A1")], vec![work("w1", &["A2"], &[])]),
                vec!["task_targets_unknown_assertion"],
            ),
        ];
        for (name, sub, expected) in cases {
            assert_eq!(codes(&sub), expected, "case '{name}'");
        }
    }

    #[test]
    fn work_tasks_may_have_empty_targets() {
        let sub = plan(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &[]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn dependency_errors() {
        let sub = plan(vec![assertion("A1")], vec![work("w1", &["A1"], &["w1"])]);
        assert_eq!(codes(&sub), vec!["self_loop"]);

        let sub = plan(vec![assertion("A1")], vec![work("w1", &["A1"], &["ghost"])]);
        assert_eq!(codes(&sub), vec!["dep_unknown_task"]);
    }

    #[test]
    fn cycles_detected() {
        // 2-cycle.
        let sub = plan(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &["w2"]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), vec!["cycle_detected"]);

        // 3-cycle alongside an acyclic task.
        let sub = plan(
            vec![assertion("A1")],
            vec![
                work("w0", &["A1"], &[]),
                work("w1", &[], &["w3"]),
                work("w2", &[], &["w1"]),
                work("w3", &[], &["w2"]),
            ],
        );
        assert_eq!(codes(&sub), vec!["cycle_detected"]);
    }

    #[test]
    fn coverage_errors() {
        // A validate task targeting an assertion does not count as coverage.
        let sub = plan(
            vec![assertion("A1"), assertion("A2")],
            vec![work("w1", &["A1"], &[]), validate("v1", &["A2"], &[])],
        );
        assert_eq!(codes(&sub), vec!["uncovered_assertion"]);

        // Two work coverers is one too many.
        let sub = plan(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &[]), work("w2", &["A1"], &[])],
        );
        assert_eq!(codes(&sub), vec!["over_covered_assertion"]);

        // Exactly one work coverer is the happy case.
        let sub = plan(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn one_work_task_may_own_multiple_assertions() {
        let sub = plan(
            vec![assertion("A1"), assertion("A2")],
            vec![work("w1", &["A1", "A2"], &[])],
        );

        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn groups_short_circuit_in_order() {
        // Id duplication suppresses shape errors.
        let bad_gate = task("g1", TaskKind::Gate, Some("implementer"), "body", &[], &[]);
        let sub = plan(vec![assertion("A1")], vec![bad_gate.clone(), bad_gate]);
        assert_eq!(codes(&sub), vec!["duplicate_task_id"]);

        // Shape errors suppress dep errors.
        let sub = plan(
            vec![assertion("A1")],
            vec![task(
                "w1",
                TaskKind::Work,
                None,
                "body",
                &["A1"],
                &["ghost"],
            )],
        );
        assert_eq!(codes(&sub), vec!["missing_role"]);

        // Dep errors suppress cycle detection.
        let sub = plan(
            vec![assertion("A1")],
            vec![
                work("w1", &["A1"], &["ghost"]),
                work("w2", &[], &["w3"]),
                work("w3", &[], &["w2"]),
            ],
        );
        assert_eq!(codes(&sub), vec!["dep_unknown_task"]);

        // Cycle detection suppresses coverage (A2 is uncovered).
        let sub = plan(
            vec![assertion("A1"), assertion("A2")],
            vec![work("w1", &["A1"], &["w2"]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), vec!["cycle_detected"]);
    }

    #[test]
    fn rich_valid_plan_passes() {
        let sub = plan(
            vec![assertion_with_oracle("A1", "cargo-test"), assertion("A2")],
            vec![
                work("w1", &["A1"], &[]),
                work("w2", &["A2"], &["w1"]),
                validate("v1", &["A1", "A2"], &["w1", "w2"]),
                gate("g1", &["A1", "A2"], &["v1"]),
            ],
        );
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn verified_bar_rejects_an_oracle_less_assertion() {
        let mut verified = inventory();
        verified.stop = StopBar::Verified;
        let against = |sub: &Plan| -> Vec<&'static str> {
            validate_plan(sub, &verified)
                .into_iter()
                .map(|e| e.code)
                .collect()
        };

        // Under `verified`, an assertion with no oracle can never become
        // authoritatively Verified, so it is rejected at author time.
        let sub = plan(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(against(&sub), vec!["assertion_unprovable"]);

        // Bind an oracle and the same plan is accepted.
        let sub = plan(
            vec![assertion_with_oracle("A1", "cargo-test")],
            vec![work("w1", &["A1"], &[])],
        );
        assert_eq!(against(&sub), CLEAN);

        // The default `reviewed` inventory accepts the oracle-less plan.
        let sub = plan(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn the_terminal_review_task_tag_is_reserved() {
        // Regression (QA round 2): the reviewer's attempt-dir tag shares the
        // plan-task namespace; a task by that name could leave crashed-attempt
        // dirs the closing reviewer would silently reuse.
        let sub = plan(
            vec![assertion("A1")],
            vec![work(
                super::super::ids::TERMINAL_REVIEW_TASK_TAG,
                &["A1"],
                &[],
            )],
        );
        assert_eq!(codes(&sub), vec!["reserved_task_id"]);
    }
}
