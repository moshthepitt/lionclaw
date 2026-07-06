//! Fail-closed structural validation of a plan submission, ported from
//! Zenith (Apache-2.0, Intelligent Internet) `task_validation.py`.
//!
//! Check groups run in zenith's order and short-circuit per group: an id
//! error suppresses shape errors, and so on. A submission that fails any
//! group produces no event — the mission never advances on an invalid plan.
//!
//! Divergences: contract + task list validate together (one submission);
//! roles replace skills, and a task's role must carry compatible output
//! semantics (verdict roles validate, non-verdict roles work); assertion
//! oracle bindings must exist in the plugin inventory. Id charset rules are
//! enforced by the id newtypes at every deserialization boundary, so only
//! duplicates are checked here.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::{OracleName, RoleName, TaskId};
use super::plan::{OutputSemantics, PlanSubmission, TaskKind};

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

/// The plugin-supplied inventory the plan is validated against.
pub struct PluginInventory {
    pub roles: BTreeMap<RoleName, OutputSemantics>,
    pub oracles: BTreeSet<OracleName>,
}

pub fn validate_plan_submission(
    submission: &PlanSubmission,
    inventory: &PluginInventory,
) -> Vec<PlanValidationError> {
    // Group 0: emptiness (zenith empty_contract / empty_task_list).
    if submission.assertions.is_empty() {
        return vec![err(
            "empty_contract",
            "mission has no contract assertions; a plan must state falsifiable claims",
        )];
    }
    if submission.tasks.is_empty() {
        return vec![err("empty_task_list", "plan has no tasks")];
    }

    // Group 1: id uniqueness (charset enforced by the newtypes).
    let errors = check_unique_ids(submission);
    if !errors.is_empty() {
        return errors;
    }
    // Group 2: per-kind shape + role/oracle inventory resolution.
    let errors = check_shape(submission, inventory);
    if !errors.is_empty() {
        return errors;
    }
    // Group 3: dependency resolution.
    let errors = check_deps_resolve(submission);
    if !errors.is_empty() {
        return errors;
    }
    // Group 4: acyclicity (Kahn).
    let errors = check_acyclic(submission);
    if !errors.is_empty() {
        return errors;
    }
    // Group 5: coverage.
    check_coverage(submission)
}

fn check_unique_ids(submission: &PlanSubmission) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let mut seen_assertions = BTreeSet::new();
    for assertion in &submission.assertions {
        if !seen_assertions.insert(&assertion.id) {
            errors.push(err(
                "duplicate_assertion_id",
                format!("assertion '{}' declared more than once", assertion.id),
            ));
        }
    }
    let mut seen_tasks = BTreeSet::new();
    for task in &submission.tasks {
        if !seen_tasks.insert(&task.id) {
            errors.push(err(
                "duplicate_task_id",
                format!("task '{}' declared more than once", task.id),
            ));
        }
    }
    errors
}

fn check_shape(
    submission: &PlanSubmission,
    inventory: &PluginInventory,
) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    for assertion in &submission.assertions {
        if let Some(oracle) = &assertion.oracle {
            if !inventory.oracles.contains(oracle) {
                errors.push(err(
                    "unknown_oracle",
                    format!(
                        "assertion '{}' binds oracle '{oracle}' which the plugin does not provide",
                        assertion.id
                    ),
                ));
            }
        }
    }
    for task in &submission.tasks {
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
                        format!("validate task '{}' must target at least one assertion", task.id),
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
                            "task '{}' names role '{role}' which the plugin does not provide",
                            task.id
                        ),
                    ));
                    continue;
                };
                // Routing is bound to output semantics, never to names.
                let compatible = match output {
                    OutputSemantics::EmitsVerdict => task.kind == TaskKind::Validate,
                    OutputSemantics::Plans
                    | OutputSemantics::ProducesArtifact
                    | OutputSemantics::Egresses => task.kind == TaskKind::Work,
                };
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
        let known: BTreeSet<_> = submission.assertions.iter().map(|a| &a.id).collect();
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

fn check_deps_resolve(submission: &PlanSubmission) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let ids: BTreeSet<_> = submission.tasks.iter().map(|t| &t.id).collect();
    for task in &submission.tasks {
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

fn check_acyclic(submission: &PlanSubmission) -> Vec<PlanValidationError> {
    let mut indegree: BTreeMap<&TaskId, usize> = BTreeMap::new();
    let mut successors: BTreeMap<&TaskId, Vec<&TaskId>> = BTreeMap::new();
    for task in &submission.tasks {
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
            let deg = indegree.get_mut(succ).expect("successor is a declared task");
            *deg -= 1;
            if *deg == 0 {
                queue.push(succ);
            }
        }
    }
    if visited == submission.tasks.len() {
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
fn check_coverage(submission: &PlanSubmission) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let mut coverers: BTreeMap<_, Vec<&TaskId>> = BTreeMap::new();
    for task in &submission.tasks {
        if task.kind != TaskKind::Work {
            continue;
        }
        for target in &task.targets {
            coverers.entry(target).or_default().push(&task.id);
        }
    }
    for assertion in &submission.assertions {
        match coverers.get(&assertion.id).map(Vec::as_slice) {
            None | Some([]) => errors.push(err(
                "uncovered_assertion",
                format!("assertion '{}' has no work task covering it", assertion.id),
            )),
            Some([_]) => {}
            Some(many) => errors.push(err(
                "over_covered_assertion",
                format!(
                    "assertion '{}' is covered by {} work tasks; exactly one is required",
                    assertion.id,
                    many.len()
                ),
            )),
        }
    }
    errors
}
