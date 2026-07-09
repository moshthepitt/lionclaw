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
//! oracle bindings must exist in the mission-type inventory. Id charset rules are
//! enforced by the id newtypes at every deserialization boundary, so only
//! duplicates are checked here.

use std::collections::{BTreeMap, BTreeSet};

use super::event::{AmendmentOps, StopBar};
use super::ids::{OracleName, RoleName, TaskId};
use super::plan::{OutputSemantics, PlanSubmission, PlanningDag, TaskKind};
use super::state::MissionState;

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

pub fn validate_plan_submission(
    submission: &PlanSubmission,
    inventory: &MissionTypeInventory,
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
    let errors = check_coverage(submission);
    if !errors.is_empty() {
        return errors;
    }
    // Group 6: every gate target has an upstream validator (else the gate can
    // never clear — reject at author time instead of parking at run time).
    let errors = check_gate_coverage(submission);
    if !errors.is_empty() {
        return errors;
    }
    // Group 7: the declared stop bar is reachable. Under `Verified` every
    // assertion must bind an oracle — an oracle-less assertion can never become
    // authoritatively verified, and (contract being strengthen-only) can never
    // be removed, so it would cap the mission below `Verified` forever. Reject
    // at author time; a domain with genuinely unprovable claims declares
    // `stop = reviewed`.
    check_stop_bar_reachable(submission, inventory.stop)
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
    // awaits a manually submitted plan).
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

fn check_stop_bar_reachable(
    submission: &PlanSubmission,
    stop: StopBar,
) -> Vec<PlanValidationError> {
    if stop != StopBar::Verified {
        return Vec::new();
    }
    submission
        .assertions
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

/// Why an amendment is refused. The structural prechecks below carry the
/// invariants the whole-plan validator can't see (which task is live, whether
/// a bind strengthens); everything else (coverage, deps, cycles, shape) rides
/// on `validate_plan_submission` over the resulting plan, so amend-validation
/// can never drift from submit-validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AmendmentError {
    #[error("amendment is immaterial (it leaves the plan unchanged)")]
    Immaterial,
    #[error("task '{task}' is not live (unknown or already retired)")]
    UnknownTask { task: String },
    #[error("added task '{task}' reuses an existing task id (retired ids are never revived)")]
    TaskIdReused { task: String },
    #[error("supersede of '{old}' names replacement '{new}' that is not in `add`")]
    ReplacementMissing { old: String, new: String },
    #[error(
        "bind_oracle on assertion '{assertion}' would replace/unbind an oracle \
         (the contract is strengthen-only; bind an unbound assertion)"
    )]
    OracleUnbound { assertion: String },
    #[error("the resulting plan is invalid:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Invalid(Vec<PlanValidationError>),
}

/// Validate an amendment fail-closed: structural op prechecks + the full
/// submit-time validation over the *whole resulting plan* (atomicity, ADR
/// 0009). Contract weakening is unrepresentable — no op removes an assertion
/// or shrinks a binding — so only `bind_oracle`'s strengthen-only rule needs a
/// check here. No sealing check: honesty is carried by the oracle re-judging
/// the final head, so re-planning verified work cannot launder a verdict.
pub fn validate_plan_amendment(
    state: &MissionState,
    ops: &AmendmentOps,
    inventory: &MissionTypeInventory,
) -> Result<(), AmendmentError> {
    let Some(plan) = state.plan.as_ref() else {
        return Err(AmendmentError::Invalid(vec![err(
            "no_plan",
            "mission has no plan to amend",
        )]));
    };
    let is_live = |id: &TaskId| plan.tasks.iter().any(|t| &t.id == id);
    let added: BTreeSet<&TaskId> = ops.add.iter().map(|t| &t.id).collect();

    // Added ids must be genuinely new: a retired task keeps a `Superseded`
    // tombstone in `state.tasks` (removed from the live plan), so re-adding its
    // id would birth the new task tombstoned and never run it. `state.tasks`
    // covers both live and retired ids.
    for task in &ops.add {
        if state.tasks.contains_key(&task.id) {
            return Err(AmendmentError::TaskIdReused {
                task: task.id.to_string(),
            });
        }
    }
    for s in &ops.supersede {
        if !is_live(&s.old) {
            return Err(AmendmentError::UnknownTask {
                task: s.old.to_string(),
            });
        }
        if !added.contains(&s.new) {
            return Err(AmendmentError::ReplacementMissing {
                old: s.old.to_string(),
                new: s.new.to_string(),
            });
        }
    }
    for old in &ops.cancel {
        if !is_live(old) {
            return Err(AmendmentError::UnknownTask {
                task: old.to_string(),
            });
        }
    }
    // Strengthen-only: bind an existing, currently-unbound assertion (or the
    // identical oracle, idempotent). Anything else — a different oracle, or an
    // unknown assertion — is refused. Shares `bind_strengthens` with the fold.
    for b in &ops.bind_oracle {
        if !super::fold::bind_strengthens(&state.contract, &b.assertion, &b.oracle) {
            return Err(AmendmentError::OracleUnbound {
                assertion: b.assertion.to_string(),
            });
        }
    }

    let resulting = super::fold::resulting_plan(plan, ops);
    // Materiality: an amendment that leaves the plan unchanged (empty ops, or a
    // bind_oracle that only re-binds already-bound oracles) must not bump the
    // revision and re-open ratification for nothing.
    if resulting == *plan {
        return Err(AmendmentError::Immaterial);
    }
    let errors = validate_plan_submission(&resulting, inventory);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(AmendmentError::Invalid(errors))
    }
}

fn check_gate_coverage(submission: &PlanSubmission) -> Vec<PlanValidationError> {
    let by_id: BTreeMap<&TaskId, &super::plan::Task> =
        submission.tasks.iter().map(|t| (&t.id, t)).collect();
    let mut errors = Vec::new();
    for gate in submission.tasks.iter().filter(|t| t.kind == TaskKind::Gate) {
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
    inventory: &MissionTypeInventory,
) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    for assertion in &submission.assertions {
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
    let known: BTreeSet<_> = submission.assertions.iter().map(|a| &a.id).collect();
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
                // manual-submit hole).
                let compatible = match output {
                    OutputSemantics::ProducesArtifact => task.kind == TaskKind::Work,
                    OutputSemantics::EmitsVerdict => task.kind == TaskKind::Validate,
                    OutputSemantics::ProducesReport | OutputSemantics::ProposesPlan => false,
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
            let deg = indegree
                .get_mut(succ)
                .expect("successor is a declared task");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ids::AssertionId;
    use crate::model::plan::{Assertion, PlanningTask, Task};

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

    fn submission(assertions: Vec<Assertion>, tasks: Vec<Task>) -> PlanSubmission {
        PlanSubmission { assertions, tasks }
    }

    fn codes(submission: &PlanSubmission) -> Vec<&'static str> {
        validate_plan_submission(submission, &inventory())
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
    }

    // The check_shape chokepoint: a read-only planning role (produces-report /
    // proposes-plan) must never masquerade as an execution worker in a submitted
    // plan. Neither arm was exercised before.
    #[test]
    fn a_planning_role_on_an_execution_task_is_rejected() {
        let work_on_reporter = submission(
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
        let validate_on_author = submission(
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

    #[test]
    fn empty_contract_returned_alone() {
        // Even with an empty task list...
        let sub = submission(vec![], vec![]);
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
        let sub = submission(vec![], vec![bad_gate]);
        assert_eq!(codes(&sub), vec!["empty_contract"]);
    }

    #[test]
    fn empty_task_list_rejected() {
        let sub = submission(vec![assertion("A1")], vec![]);
        assert_eq!(codes(&sub), vec!["empty_task_list"]);
    }

    #[test]
    fn duplicate_ids_accumulate_within_the_group() {
        let sub = submission(
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
        let cases: Vec<(&str, PlanSubmission, Vec<&str>)> = vec![
            (
                "gate_with_role",
                submission(
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
                submission(
                    vec![assertion("A1")],
                    vec![task("g1", TaskKind::Gate, None, "not empty", &["A1"], &[])],
                ),
                vec!["gate_with_body"],
            ),
            (
                "gate_empty_targets",
                submission(vec![assertion("A1")], vec![gate("g1", &[], &[])]),
                vec!["empty_targets"],
            ),
            (
                "validate_empty_targets",
                submission(vec![assertion("A1")], vec![validate("v1", &[], &[])]),
                vec!["empty_targets"],
            ),
            (
                "missing_body",
                submission(
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
                submission(
                    vec![assertion("A1")],
                    vec![task("w1", TaskKind::Work, None, "body", &["A1"], &[])],
                ),
                vec!["missing_role"],
            ),
            (
                "unknown_role",
                submission(
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
                submission(
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
                submission(
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
                submission(
                    vec![assertion_with_oracle("A1", "psychic")],
                    vec![work("w1", &["A1"], &[])],
                ),
                vec!["unknown_oracle"],
            ),
            (
                "task_targets_unknown_assertion",
                submission(vec![assertion("A1")], vec![work("w1", &["A2"], &[])]),
                vec!["task_targets_unknown_assertion"],
            ),
        ];
        for (name, sub, expected) in cases {
            assert_eq!(codes(&sub), expected, "case '{name}'");
        }
    }

    #[test]
    fn work_tasks_may_have_empty_targets() {
        let sub = submission(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &[]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn dependency_errors() {
        let sub = submission(vec![assertion("A1")], vec![work("w1", &["A1"], &["w1"])]);
        assert_eq!(codes(&sub), vec!["self_loop"]);

        let sub = submission(vec![assertion("A1")], vec![work("w1", &["A1"], &["ghost"])]);
        assert_eq!(codes(&sub), vec!["dep_unknown_task"]);
    }

    #[test]
    fn cycles_detected() {
        // 2-cycle.
        let sub = submission(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &["w2"]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), vec!["cycle_detected"]);

        // 3-cycle alongside an acyclic task.
        let sub = submission(
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
        let sub = submission(
            vec![assertion("A1"), assertion("A2")],
            vec![work("w1", &["A1"], &[]), validate("v1", &["A2"], &[])],
        );
        assert_eq!(codes(&sub), vec!["uncovered_assertion"]);

        // Two work coverers is one too many.
        let sub = submission(
            vec![assertion("A1")],
            vec![work("w1", &["A1"], &[]), work("w2", &["A1"], &[])],
        );
        assert_eq!(codes(&sub), vec!["over_covered_assertion"]);

        // Exactly one work coverer is the happy case.
        let sub = submission(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(codes(&sub), CLEAN);
    }

    #[test]
    fn groups_short_circuit_in_order() {
        // Id duplication suppresses shape errors.
        let bad_gate = task("g1", TaskKind::Gate, Some("implementer"), "body", &[], &[]);
        let sub = submission(vec![assertion("A1")], vec![bad_gate.clone(), bad_gate]);
        assert_eq!(codes(&sub), vec!["duplicate_task_id"]);

        // Shape errors suppress dep errors.
        let sub = submission(
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
        let sub = submission(
            vec![assertion("A1")],
            vec![
                work("w1", &["A1"], &["ghost"]),
                work("w2", &[], &["w3"]),
                work("w3", &[], &["w2"]),
            ],
        );
        assert_eq!(codes(&sub), vec!["dep_unknown_task"]);

        // Cycle detection suppresses coverage (A2 is uncovered).
        let sub = submission(
            vec![assertion("A1"), assertion("A2")],
            vec![work("w1", &["A1"], &["w2"]), work("w2", &[], &["w1"])],
        );
        assert_eq!(codes(&sub), vec!["cycle_detected"]);
    }

    #[test]
    fn rich_valid_submission_passes() {
        let sub = submission(
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
        let against = |sub: &PlanSubmission| -> Vec<&'static str> {
            validate_plan_submission(sub, &verified)
                .into_iter()
                .map(|e| e.code)
                .collect()
        };

        // Under `verified`, an assertion with no oracle can never become
        // authoritatively Verified, so it is rejected at author time.
        let sub = submission(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(against(&sub), vec!["assertion_unprovable"]);

        // Bind an oracle and the same plan is accepted.
        let sub = submission(
            vec![assertion_with_oracle("A1", "cargo-test")],
            vec![work("w1", &["A1"], &[])],
        );
        assert_eq!(against(&sub), CLEAN);

        // The default `reviewed` inventory accepts the oracle-less plan.
        let sub = submission(vec![assertion("A1")], vec![work("w1", &["A1"], &[])]);
        assert_eq!(codes(&sub), CLEAN);
    }
}
