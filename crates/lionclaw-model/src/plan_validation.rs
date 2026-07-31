//! Fail-closed structural validation of a plan, ported from
//! Zenith (Apache-2.0, Intelligent Internet) `task_validation.py`.
//!
//! Check groups run in zenith's order and short-circuit per group: an id
//! error suppresses shape errors, and so on. A plan that fails any
//! group produces no event — the mission never advances on an invalid plan.
//!
//! The plan contains only contract and work DAG data. Role contracts,
//! assignments, judgment panels, and closing review live in the active team
//! snapshot and validate jointly with the plan.

use super::ids::TaskId;
use super::plan::{OutputSemantics, Plan, PlanProposal, RequirementDisposition};
use super::state::MissionState;
use super::{
    ChildMissionAssignment, MissionConfig, MissionProposal, OracleName, OracleSpec, StopBar,
    TaskAssignment, TeamRevision,
};
use crate::prelude::*;

/// Maximum direct fan-in for one task. Role reports are independently bounded
/// at ingress, so this limit also gives prompt construction a fixed aggregate
/// upstream-context ceiling.
pub const MAX_TASK_DEPENDENCIES: usize = 16;

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

pub fn validate_plan(
    plan: &Plan,
    team: &TeamRevision,
    oracles: &BTreeMap<OracleName, OracleSpec>,
    config: &MissionConfig,
) -> Vec<PlanValidationError> {
    validate_plan_with_descendant_usage(plan, team, oracles, config, 0)
}

pub(crate) fn validate_plan_with_descendant_usage(
    plan: &Plan,
    team: &TeamRevision,
    oracles: &BTreeMap<OracleName, OracleSpec>,
    config: &MissionConfig,
    descendant_usage: u64,
) -> Vec<PlanValidationError> {
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
    // Group 3: work shape plus team/oracle resolution.
    let errors = check_shape(plan, team, oracles, config, descendant_usage);
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
    // Group 6: one deliverable sink and no role instance assigned to
    // concurrently runnable work.
    let errors = check_dispatch_topology(plan, team);
    if !errors.is_empty() {
        return errors;
    }
    // Group 7: coverage.
    let errors = check_coverage(plan);
    if !errors.is_empty() {
        return errors;
    }
    // Group 8: the declared stop bar is reachable. A `Verified` mission must
    // launch fully oracle-provable. A domain with genuinely non-oracle proof
    // declares `stop = attested`; host-only obligations are recorded outside
    // the confined proof bar.
    check_stop_bar_reachable(plan, config.stop)
}

fn check_dispatch_topology(plan: &Plan, team: &TeamRevision) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let depended_on: BTreeSet<_> = plan
        .tasks
        .iter()
        .flat_map(|task| task.depends_on.iter())
        .collect();
    let sinks: Vec<_> = plan
        .tasks
        .iter()
        .filter(|task| !depended_on.contains(&task.id))
        .map(|task| task.id.to_string())
        .collect();
    if sinks.len() != 1 {
        errors.push(err(
            "deliverable_sink_count",
            format!("plan must have exactly one deliverable sink; found {sinks:?}"),
        ));
    }

    let mut by_role: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for task in &plan.tasks {
        if let Some(TaskAssignment::Role {
            role_instance: role,
        }) = team.task_assignments.get(&task.id)
        {
            by_role.entry(role).or_default().push(&task.id);
        }
    }
    for (role, tasks) in by_role {
        for (index, left) in tasks.iter().enumerate() {
            for right in tasks.iter().skip(index + 1) {
                if !depends_transitively(plan, left, right)
                    && !depends_transitively(plan, right, left)
                {
                    errors.push(err(
                        "concurrent_role_assignment",
                        format!(
                            "role instance '{role}' owns concurrently runnable tasks '{left}' and '{right}'"
                        ),
                    ));
                }
            }
        }
    }
    errors
}

fn depends_transitively(plan: &Plan, task: &TaskId, possible_ancestor: &TaskId) -> bool {
    let tasks: BTreeMap<_, _> = plan.tasks.iter().map(|task| (&task.id, task)).collect();
    let mut pending = vec![task];
    let mut seen = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if !seen.insert(current) {
            continue;
        }
        let Some(task) = tasks.get(current) else {
            continue;
        };
        if task.depends_on.contains(possible_ancestor) {
            return true;
        }
        pending.extend(task.depends_on.iter());
    }
    false
}

fn check_stop_bar_reachable(plan: &Plan, stop: StopBar) -> Vec<PlanValidationError> {
    let by_id: BTreeMap<_, _> = plan.assertions.iter().map(|a| (&a.id, a)).collect();
    let mut errors = Vec::new();
    for requirement in &plan.requirements {
        match &requirement.disposition {
            RequirementDisposition::ConfinedProvable { assertion_ids } => {
                for assertion_id in assertion_ids {
                    if by_id
                        .get(assertion_id)
                        .is_some_and(|assertion| assertion.oracle.is_none())
                    {
                        errors.push(err(
                            "assertion_unprovable",
                            format!(
                                "requirement '{}' classifies assertion '{}' as confined-provable, \
                                 but it binds no oracle",
                                requirement.id, assertion_id
                            ),
                        ));
                    }
                }
            }
            RequirementDisposition::ReviewerCheckable { assertion_ids } => {
                if stop == StopBar::Verified {
                    for assertion_id in assertion_ids {
                        errors.push(err(
                            "reviewer_checkable_under_verified",
                            format!(
                                "requirement '{}' classifies assertion '{}' as reviewer-checkable, \
                                 so the mission type must use stop = attested or reclassify it",
                                requirement.id, assertion_id
                            ),
                        ));
                    }
                }
            }
            RequirementDisposition::HostAcceptance { .. }
            | RequirementDisposition::Limitation { .. } => {}
        }
    }
    errors
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
    #[error("oracle change requires a plan proposal")]
    OracleChangeRequiresPlan,
    #[error("task '{task}' changes an existing task; retained task ids are immutable")]
    TaskChanged { task: String },
    #[error("new task '{task}' reuses a retired task id")]
    TaskIdReused { task: String },
    #[error("retired task '{task}' cannot be retained in a replacement plan")]
    RetiredTaskRetained { task: String },
    #[error("covered requirement change declarations do not match; expected {expected:?}, got {actual:?}")]
    RequirementChangesMismatch {
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error(
        "assertion supersession declarations do not match; expected {expected:?}, got {actual:?}"
    )]
    AssertionSupersessionsMismatch {
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error("assertion '{assertion}' names unknown replacement '{replacement}'")]
    UnknownAssertionReplacement {
        assertion: String,
        replacement: String,
    },
    #[error("the resulting plan is invalid:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Invalid(Vec<PlanValidationError>),
}

/// Validate the complete candidate plan at the proposal boundary. The normal
/// whole-plan validator owns structural correctness; this function owns only
/// revision monotonicity and id history.
pub fn validate_plan_proposal(
    state: &MissionState,
    proposal: &PlanProposal,
) -> Result<(), ProposalError> {
    validate_plan_transition(state, proposal, false)?;
    let Some(team) = state.team.as_ref() else {
        return Err(ProposalError::Invalid(vec![err(
            "missing_team",
            "mission has no accepted team revision",
        )]));
    };
    let errors = validate_plan_with_descendant_usage(
        &proposal.plan,
        team,
        &state.oracles,
        &state.config,
        state.descendant_count(),
    );
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ProposalError::Invalid(errors))
    }
}

pub fn validate_mission_proposal(
    state: &MissionState,
    proposal: &MissionProposal,
) -> Result<(), ProposalError> {
    let oracle_change = proposal
        .oracles
        .as_ref()
        .is_some_and(|oracles| oracles != &state.oracles);
    if proposal.plan.is_none() && proposal.team.is_none() {
        return if oracle_change {
            Err(ProposalError::OracleChangeRequiresPlan)
        } else {
            Err(ProposalError::Immaterial)
        };
    }
    if oracle_change && proposal.plan.is_none() {
        return Err(ProposalError::OracleChangeRequiresPlan);
    }
    if let Some(team) = &proposal.team {
        let expected = state
            .team
            .as_ref()
            .map_or(0, |current| current.revision.saturating_add(1));
        if team.revision != expected
            || team.validate_shape().is_err()
            || team
                .roles
                .values()
                .any(|role| !role.grants.within(&state.config.ceilings))
            || team.roles.values().any(|role| {
                role.resources
                    .within(&state.config.resource_ceilings)
                    .is_err()
            })
        {
            return Err(ProposalError::Invalid(vec![err(
                "invalid_team_revision",
                format!("proposed team must be complete revision {expected} within ceilings"),
            )]));
        }
        if (state.lineage.is_some() || team_has_child_mission(team)) && team_has_secret_grants(team)
        {
            return Err(ProposalError::Invalid(vec![err(
                "child_secret_grant_forbidden",
                "missions that author or execute child work must use secret-free role grants",
            )]));
        }
        if team_has_child_mission(team)
            && state
                .team
                .as_ref()
                .and_then(|current| current.role(&current.planning_assignment))
                .is_some_and(|planner| planner.grants.secrets)
        {
            return Err(ProposalError::Invalid(vec![err(
                "child_author_secret_grant_forbidden",
                "a secret-bearing planning role cannot author child mission assignments",
            )]));
        }
    }
    if let Some(plan) = &proposal.plan {
        validate_plan_transition(state, plan, oracle_change)?;
    }
    let oracles = proposal.oracles.as_ref().unwrap_or(&state.oracles);
    let oracle_errors = oracles
        .iter()
        .filter_map(|(name, spec)| {
            spec.validate(
                &state.config.ceilings,
                &state.config.resource_ceilings,
                &state.config.execution,
            )
            .err()
            .map(|error| {
                err(
                    "invalid_oracle",
                    format!("oracle '{name}' is invalid: {error}"),
                )
            })
        })
        .collect::<Vec<_>>();
    if !oracle_errors.is_empty() {
        return Err(ProposalError::Invalid(oracle_errors));
    }
    let team = proposal
        .team
        .as_ref()
        .or(state.team.as_ref())
        .ok_or_else(|| {
            ProposalError::Invalid(vec![err(
                "missing_team",
                "proposal has no team revision to validate against",
            )])
        })?;
    let plan = proposal
        .plan
        .as_ref()
        .map(|candidate| &candidate.plan)
        .or(state.plan.as_ref());
    if let Some(plan) = plan {
        let errors = validate_plan_with_descendant_usage(
            plan,
            team,
            oracles,
            &state.config,
            state.descendant_count(),
        );
        if !errors.is_empty() {
            return Err(ProposalError::Invalid(errors));
        }
    }
    Ok(())
}

/// Validate revision monotonicity and immutable ids before the caller checks
/// the resulting complete plan against the persisted inventory.
fn validate_plan_transition(
    state: &MissionState,
    proposal: &PlanProposal,
    allow_unchanged_plan: bool,
) -> Result<(), ProposalError> {
    if proposal.base_revision != state.revision {
        return Err(ProposalError::Stale {
            base_revision: proposal.base_revision,
            current_revision: state.revision,
        });
    }
    let Some(current) = state.plan.as_ref() else {
        if !proposal.requirement_changes.is_empty() {
            return Err(ProposalError::RequirementChangesMismatch {
                expected: vec![],
                actual: proposal
                    .requirement_changes
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        if !proposal.assertion_supersessions.is_empty() {
            return Err(ProposalError::AssertionSupersessionsMismatch {
                expected: vec![],
                actual: proposal
                    .assertion_supersessions
                    .iter()
                    .map(|entry| entry.assertion_id.to_string())
                    .collect(),
            });
        }
        return Ok(());
    };
    if proposal.plan == *current && !allow_unchanged_plan {
        return Err(ProposalError::Immaterial);
    }

    let next_requirements: BTreeMap<_, _> = proposal
        .plan
        .requirements
        .iter()
        .map(|r| (&r.id, r))
        .collect();
    let mut required_requirement_changes = BTreeSet::new();
    for old in &current.requirements {
        let Some(new) = next_requirements.get(&old.id) else {
            if old.disposition.is_proof_bearing() || old.disposition.is_recorded_obligation() {
                required_requirement_changes.insert(old.id.clone());
            }
            continue;
        };
        let disposition_preserves_intent =
            disposition_preserves_intent(&old.disposition, &new.disposition);
        if (old.disposition.is_proof_bearing() || old.disposition.is_recorded_obligation())
            && (old.kind != new.kind || old.prose != new.prose || !disposition_preserves_intent)
        {
            required_requirement_changes.insert(old.id.clone());
        }
    }
    let declared_requirement_changes: BTreeSet<_> =
        proposal.requirement_changes.iter().cloned().collect();
    if declared_requirement_changes.len() != proposal.requirement_changes.len()
        || declared_requirement_changes != required_requirement_changes
    {
        return Err(ProposalError::RequirementChangesMismatch {
            expected: required_requirement_changes
                .iter()
                .map(ToString::to_string)
                .collect(),
            actual: proposal
                .requirement_changes
                .iter()
                .map(ToString::to_string)
                .collect(),
        });
    }

    let next_assertions: BTreeMap<_, _> = proposal
        .plan
        .assertions
        .iter()
        .map(|a| (&a.id, a))
        .collect();
    let mut required_supersessions = BTreeSet::new();
    for old in &current.assertions {
        let strengthens = next_assertions.get(&old.id).is_some_and(|new| {
            old.prose == new.prose
                && (old.oracle == new.oracle || (old.oracle.is_none() && new.oracle.is_some()))
        });
        if !strengthens {
            required_supersessions.insert(old.id.clone());
        }
    }
    let declared_supersessions: BTreeSet<_> = proposal
        .assertion_supersessions
        .iter()
        .map(|entry| entry.assertion_id.clone())
        .collect();
    if declared_supersessions.len() != proposal.assertion_supersessions.len()
        || declared_supersessions != required_supersessions
    {
        return Err(ProposalError::AssertionSupersessionsMismatch {
            expected: required_supersessions
                .iter()
                .map(ToString::to_string)
                .collect(),
            actual: proposal
                .assertion_supersessions
                .iter()
                .map(|entry| entry.assertion_id.to_string())
                .collect(),
        });
    }
    for supersession in &proposal.assertion_supersessions {
        for replacement in &supersession.replacement_ids {
            if !next_assertions.contains_key(replacement) {
                return Err(ProposalError::UnknownAssertionReplacement {
                    assertion: supersession.assertion_id.to_string(),
                    replacement: replacement.to_string(),
                });
            }
        }
    }

    let current_tasks: BTreeMap<_, _> = current.tasks.iter().map(|t| (&t.id, t)).collect();
    for task in &proposal.plan.tasks {
        match current_tasks.get(&task.id) {
            Some(_)
                if state.tasks.get(&task.id).is_some_and(|runtime| {
                    runtime.status == super::state::TaskStatus::Superseded
                }) =>
            {
                return Err(ProposalError::RetiredTaskRetained {
                    task: task.id.to_string(),
                });
            }
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
    let mut referenced = BTreeMap::new();
    let mut errors = Vec::new();
    for requirement in &plan.requirements {
        if requirement.prose.trim().is_empty() {
            errors.push(err(
                "empty_requirement",
                format!("requirement '{}' has empty prose", requirement.id),
            ));
        }
        match &requirement.disposition {
            RequirementDisposition::ConfinedProvable { assertion_ids: ids } => {
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
                    if let Some(previous) = referenced.insert(id.clone(), "confined-provable") {
                        if previous != "confined-provable" {
                            errors.push(err(
                                "assertion_multiple_proof_dispositions",
                                format!(
                                    "assertion '{id}' is covered by both {previous} and confined-provable requirements"
                                ),
                            ));
                        }
                    }
                }
            }
            RequirementDisposition::ReviewerCheckable { assertion_ids: ids } => {
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
                    if let Some(previous) = referenced.insert(id.clone(), "reviewer-checkable") {
                        if previous != "reviewer-checkable" {
                            errors.push(err(
                                "assertion_multiple_proof_dispositions",
                                format!(
                                    "assertion '{id}' is covered by both {previous} and reviewer-checkable requirements"
                                ),
                            ));
                        }
                    }
                }
            }
            RequirementDisposition::HostAcceptance { rationale } if rationale.trim().is_empty() => {
                errors.push(err(
                    "empty_host_acceptance",
                    format!(
                        "requirement '{}' has an empty host-acceptance rationale",
                        requirement.id
                    ),
                ));
            }
            RequirementDisposition::HostAcceptance { .. } => {}
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
        if !referenced.contains_key(&assertion.id) {
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

fn disposition_preserves_intent(
    old: &RequirementDisposition,
    new: &RequirementDisposition,
) -> bool {
    use RequirementDisposition::{ConfinedProvable, HostAcceptance, Limitation, ReviewerCheckable};
    match (old, new) {
        (
            ConfinedProvable {
                assertion_ids: old_ids,
            },
            ConfinedProvable {
                assertion_ids: new_ids,
            },
        )
        | (
            ReviewerCheckable {
                assertion_ids: old_ids,
            },
            ReviewerCheckable {
                assertion_ids: new_ids,
            },
        )
        | (
            ReviewerCheckable {
                assertion_ids: old_ids,
            },
            ConfinedProvable {
                assertion_ids: new_ids,
            },
        ) => old_ids.iter().all(|id| new_ids.contains(id)),
        (HostAcceptance { rationale: old }, HostAcceptance { rationale: new })
        | (Limitation { rationale: old }, Limitation { rationale: new }) => old == new,
        (Limitation { .. }, ConfinedProvable { .. } | ReviewerCheckable { .. })
        | (Limitation { .. }, HostAcceptance { .. })
        | (HostAcceptance { .. }, ConfinedProvable { .. } | ReviewerCheckable { .. }) => true,
        (
            ConfinedProvable { .. },
            ReviewerCheckable { .. } | HostAcceptance { .. } | Limitation { .. },
        )
        | (ReviewerCheckable { .. }, HostAcceptance { .. } | Limitation { .. })
        | (HostAcceptance { .. }, Limitation { .. }) => false,
    }
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
    }
    errors
}

fn check_shape(
    plan: &Plan,
    team: &TeamRevision,
    oracles: &BTreeMap<OracleName, OracleSpec>,
    config: &MissionConfig,
    descendant_usage: u64,
) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    if let Err(detail) = team.validate_shape() {
        errors.push(err("invalid_team", detail));
        return errors;
    }
    if team_has_child_mission(team) && team_has_secret_grants(team) {
        errors.push(err(
            "child_secret_grant_forbidden",
            "missions that author child work must use secret-free role grants",
        ));
    }
    for (id, role) in &team.roles {
        if !config.runtime_ceilings.is_empty() && !config.runtime_ceilings.contains(&role.runtime) {
            errors.push(err(
                "runtime_exceeds_ceiling",
                format!("role instance '{id}' requests runtime outside mission ceilings"),
            ));
        }
        if !role.grants.within(&config.ceilings) {
            errors.push(err(
                "authority_exceeds_ceiling",
                format!("role instance '{id}' requests grants outside mission ceilings"),
            ));
        }
        if let Err(detail) = role.resources.within(&config.resource_ceilings) {
            errors.push(err(
                "resources_exceed_ceiling",
                format!(
                    "role instance '{id}' requests resources outside mission ceilings: {detail}"
                ),
            ));
        }
        if matches!(
            role.output,
            OutputSemantics::EmitsVerdict | OutputSemantics::EmitsGapVerdict
        ) && (role.grants.secrets || role.grants.writes)
        {
            errors.push(err(
                "judgment_floor",
                format!("judgment role instance '{id}' requests secrets or writes"),
            ));
        }
    }
    for assertion in &plan.assertions {
        if let Some(oracle) = &assertion.oracle {
            if !oracles.contains_key(oracle) {
                errors.push(err(
                    "unknown_oracle",
                    format!(
                        "assertion '{}' binds oracle '{oracle}' which the mission does not define",
                        assertion.id
                    ),
                ));
            }
        }
        let Some(panel) = team.judgment_assignments.get(&assertion.id) else {
            errors.push(err(
                "missing_judgment_assignment",
                format!(
                    "assertion '{}' has no assigned judgment panel",
                    assertion.id
                ),
            ));
            continue;
        };
        if panel.is_empty() {
            errors.push(err(
                "empty_judgment_assignment",
                format!("assertion '{}' has an empty judgment panel", assertion.id),
            ));
        }
        let mut seen = BTreeSet::new();
        for role_id in panel {
            if !seen.insert(role_id) {
                errors.push(err(
                    "duplicate_judge",
                    format!(
                        "assertion '{}' repeats judgment role instance '{role_id}'",
                        assertion.id
                    ),
                ));
            }
            match team.roles.get(role_id) {
                Some(role) if role.output == OutputSemantics::EmitsVerdict => {}
                Some(_) => errors.push(err(
                    "judgment_output_mismatch",
                    format!(
                        "assertion '{}' assigns non-verdict role instance '{role_id}'",
                        assertion.id
                    ),
                )),
                None => errors.push(err(
                    "unknown_judgment_role",
                    format!(
                        "assertion '{}' assigns unknown role instance '{role_id}'",
                        assertion.id
                    ),
                )),
            }
        }
    }
    if config.requires_gap_review && team.gap_review_assignment.is_none() {
        errors.push(err(
            "missing_gap_reviewer",
            "mission constitution requires a gap review assignment",
        ));
    }
    let known: BTreeSet<_> = plan.assertions.iter().map(|a| &a.id).collect();
    for task in &plan.tasks {
        if task.body.trim().is_empty() {
            errors.push(err(
                "missing_body",
                format!("task '{}' has no body", task.id),
            ));
        }
        let Some(assignment) = team.task_assignments.get(&task.id) else {
            errors.push(err(
                "missing_task_assignment",
                format!("task '{}' has no producer assignment", task.id),
            ));
            continue;
        };
        match assignment {
            TaskAssignment::Role { role_instance } => match team.roles.get(role_instance) {
                Some(role) if role.output.produces_task_output() => {}
                Some(_) => errors.push(err(
                    "task_output_mismatch",
                    format!(
                        "task '{}' assigns role instance '{role_instance}' whose output cannot satisfy a task",
                        task.id
                    ),
                )),
                None => errors.push(err(
                    "unknown_task_role",
                    format!(
                        "task '{}' assigns unknown role instance '{role_instance}'",
                        task.id
                    ),
                )),
            },
            TaskAssignment::ChildMission { mission } => {
                errors.extend(validate_child_assignment(&task.id, mission, config));
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
    for task_id in team.task_assignments.keys() {
        if !plan.tasks.iter().any(|task| &task.id == task_id) {
            errors.push(err(
                "assignment_unknown_task",
                format!("team assigns unknown task '{task_id}'"),
            ));
        }
    }
    let legal_attempts = config.recovery.max_attempts.max(1);
    let descendant_reservation = team
        .task_assignments
        .values()
        .filter_map(TaskAssignment::child_mission)
        .map(|mission| {
            u64::from(mission.config.execution.max_descendants)
                .saturating_add(1)
                .saturating_mul(u64::from(legal_attempts))
        })
        .sum::<u64>();
    let required_descendants = descendant_usage.saturating_add(descendant_reservation);
    if required_descendants > u64::from(config.execution.max_descendants) {
        errors.push(err(
            "descendant_limit_exceeded",
            format!(
                "mission has {descendant_usage} descendants and the plan reserves {descendant_reservation} more across {legal_attempts} legal task attempts above mission limit {}",
                config.execution.max_descendants
            ),
        ));
    }
    for assertion_id in team.judgment_assignments.keys() {
        if !plan
            .assertions
            .iter()
            .any(|assertion| &assertion.id == assertion_id)
        {
            errors.push(err(
                "assignment_unknown_assertion",
                format!("team assigns judgment for unknown assertion '{assertion_id}'"),
            ));
        }
    }
    errors
}

fn validate_child_assignment(
    task_id: &TaskId,
    child: &ChildMissionAssignment,
    parent: &MissionConfig,
) -> Vec<PlanValidationError> {
    let mut details = Vec::new();
    if child.objective.trim().is_empty()
        || child.objective.len() > super::MAX_CHILD_MISSION_OBJECTIVE_BYTES
    {
        details.push("objective is empty or exceeds its byte limit".to_string());
    }
    if !child.output.produces_task_output() {
        details.push("output must be produces_report or produces_artifact".to_string());
    }
    if child.deadline_secs == 0 || child.deadline_secs > parent.execution.max_task_time_secs {
        details.push("deadline exceeds the parent task-time ceiling".to_string());
    }
    if let Err(detail) = child.config.execution.validate() {
        details.push(format!("execution policy is invalid: {detail}"));
    }
    if child.config.recovery.max_attempts == 0
        || child.config.recovery.max_attempts > parent.recovery.max_attempts
    {
        details.push("recovery attempts exceed the parent ceiling".to_string());
    }
    if !parent.ceilings.contains(&child.config.ceilings) {
        details.push("authority ceilings exceed the parent".to_string());
    }
    if let Err(detail) = child
        .config
        .resource_ceilings
        .within(&parent.resource_ceilings)
    {
        details.push(format!("resource ceilings exceed the parent: {detail}"));
    }
    if child.config.runtime_ceilings.is_empty()
        || !child
            .config
            .runtime_ceilings
            .is_subset(&parent.runtime_ceilings)
    {
        details.push("runtime ceilings are empty or exceed the parent".to_string());
    }
    if child
        .config
        .skills
        .iter()
        .any(|(name, skill)| parent.skills.get(name) != Some(skill))
    {
        details.push("skill authority exceeds or differs from the parent".to_string());
    }
    let child_execution = &child.config.execution;
    let parent_execution = &parent.execution;
    if child_execution.default_timeout_secs > parent_execution.default_timeout_secs
        || child_execution.max_task_time_secs > parent_execution.max_task_time_secs
        || child_execution.max_task_time_secs > child.deadline_secs
        || child_execution.extension_step_secs > parent_execution.extension_step_secs
        || child_execution.effect_capacity > parent_execution.effect_capacity
        || child_execution.max_child_depth.saturating_add(1) > parent_execution.max_child_depth
        || child_execution.max_descendants.saturating_add(1) > parent_execution.max_descendants
        || (child_execution.auto_continue_candidate && !parent_execution.auto_continue_candidate)
        || (child_execution.auto_continue_proof && !parent_execution.auto_continue_proof)
    {
        details.push("execution authority is not equal to or narrower than the parent".to_string());
    }
    if child.digest().is_none() {
        details.push("canonical request exceeds its serialized byte limit".to_string());
    }

    let (Some(plan), Some(team), Some(oracles)) = (
        child.proposal.plan.as_ref(),
        child.proposal.team.as_ref(),
        child.proposal.oracles.as_ref(),
    ) else {
        details.push("initial proposal must contain a complete plan, team, and oracle map".into());
        return vec![err(
            "invalid_child_mission",
            format!("task '{task_id}' child mission: {}", details.join("; ")),
        )];
    };
    if team_has_secret_grants(team) {
        details.push("child team role grants must be secret-free".to_string());
    }
    if plan.base_revision != 0
        || !plan.requirement_changes.is_empty()
        || !plan.assertion_supersessions.is_empty()
        || team.revision != 0
    {
        details.push("initial child plan and team must start at revision zero".to_string());
    }
    for (name, oracle) in oracles {
        if let Err(detail) = oracle.validate(
            &child.config.ceilings,
            &child.config.resource_ceilings,
            &child.config.execution,
        ) {
            details.push(format!("oracle '{name}' is invalid: {detail}"));
        }
    }
    if details.is_empty() {
        let child_plan_errors = validate_plan(&plan.plan, team, oracles, &child.config);
        details.extend(child_plan_errors.into_iter().map(|error| error.to_string()));
        let depended_on: BTreeSet<_> = plan
            .plan
            .tasks
            .iter()
            .flat_map(|task| task.depends_on.iter())
            .collect();
        let sink = plan
            .plan
            .tasks
            .iter()
            .find(|task| !depended_on.contains(&task.id));
        let sink_output = sink
            .and_then(|task| team.task_assignments.get(&task.id))
            .and_then(|assignment| match assignment {
                TaskAssignment::Role { role_instance } => {
                    team.roles.get(role_instance).map(|role| role.output)
                }
                TaskAssignment::ChildMission { mission } => Some(mission.output),
            });
        if sink_output != Some(child.output) {
            details.push("deliverable sink does not match the declared child output".to_string());
        }
    }
    if details.is_empty() {
        Vec::new()
    } else {
        vec![err(
            "invalid_child_mission",
            format!("task '{task_id}' child mission: {}", details.join("; ")),
        )]
    }
}

fn team_has_child_mission(team: &TeamRevision) -> bool {
    team.task_assignments
        .values()
        .any(|assignment| matches!(assignment, TaskAssignment::ChildMission { .. }))
}

fn team_has_secret_grants(team: &TeamRevision) -> bool {
    team.roles.values().any(|role| role.grants.secrets)
}

fn check_deps_resolve(plan: &Plan) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    let ids: BTreeSet<_> = plan.tasks.iter().map(|t| &t.id).collect();
    for task in &plan.tasks {
        errors.extend(check_dependency_list(
            "task",
            &task.id,
            &task.depends_on,
            &ids,
        ));
    }
    errors
}

fn check_dependency_list(
    scope: &str,
    task_id: &TaskId,
    dependencies: &[TaskId],
    known_tasks: &BTreeSet<&TaskId>,
) -> Vec<PlanValidationError> {
    let mut errors = Vec::new();
    if dependencies.len() > MAX_TASK_DEPENDENCIES {
        errors.push(err(
            "dependency_fan_in",
            format!(
                "{scope} '{task_id}' has {} dependencies; the limit is {MAX_TASK_DEPENDENCIES}",
                dependencies.len()
            ),
        ));
    }
    let mut seen = BTreeSet::new();
    for dependency in dependencies {
        if !seen.insert(dependency) {
            errors.push(err(
                "duplicate_dependency",
                format!("{scope} '{task_id}' repeats dependency '{dependency}'"),
            ));
        } else if dependency == task_id {
            errors.push(err(
                "self_loop",
                format!("{scope} '{task_id}' depends on itself"),
            ));
        } else if !known_tasks.contains(dependency) {
            errors.push(err(
                "dep_unknown_task",
                format!("{scope} '{task_id}' depends on unknown task '{dependency}'"),
            ));
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
mod topology_tests {
    use super::*;
    use crate::{
        Assertion, AssertionId, AuthorityGrants, Requirement, RequirementDisposition,
        RequirementId, RequirementKind, RoleInstance, RoleInstanceId, Task,
    };

    fn role(id: &str, output: OutputSemantics) -> RoleInstance {
        RoleInstance {
            id: RoleInstanceId::new(id).unwrap(),
            purpose: id.into(),
            output,
            runtime: "codex".into(),
            instructions: id.into(),
            skills: Vec::new(),
            environment: BTreeMap::new(),
            grants: AuthorityGrants::default(),
            resources: Default::default(),
            deadline_secs: None,
        }
    }

    fn contract(tasks: Vec<Task>) -> Plan {
        let assertion = AssertionId::new("A-1").unwrap();
        Plan {
            requirements: vec![Requirement {
                id: RequirementId::new("REQ-1").unwrap(),
                kind: RequirementKind::Capability,
                prose: "behavior".into(),
                disposition: RequirementDisposition::ConfinedProvable {
                    assertion_ids: vec![assertion.clone()],
                },
            }],
            assertions: vec![Assertion {
                id: assertion,
                prose: "behavior holds".into(),
                oracle: None,
            }],
            tasks,
        }
    }

    fn team(assignments: BTreeMap<TaskId, RoleInstanceId>) -> TeamRevision {
        let planner = role("planner", OutputSemantics::ProposesPlan);
        let worker = role("worker", OutputSemantics::ProducesArtifact);
        let integrator = role("integrator", OutputSemantics::ProducesArtifact);
        let judge = role("judge", OutputSemantics::EmitsVerdict);
        TeamRevision {
            revision: 1,
            roles: [planner.clone(), worker, integrator, judge.clone()]
                .into_iter()
                .map(|role| (role.id.clone(), role))
                .collect(),
            planning_assignment: planner.id,
            task_assignments: assignments
                .into_iter()
                .map(|(task, role)| (task, role.into()))
                .collect(),
            judgment_assignments: BTreeMap::from([(
                AssertionId::new("A-1").unwrap(),
                vec![judge.id],
            )]),
            gap_review_assignment: None,
            guidance: None,
        }
    }

    fn task(id: &str, depends_on: &[&str]) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            body: id.into(),
            targets: vec![AssertionId::new("A-1").unwrap()],
            depends_on: depends_on
                .iter()
                .map(|id| TaskId::new(*id).unwrap())
                .collect(),
        }
    }

    #[test]
    fn rejects_multiple_deliverable_sinks() {
        let plan = contract(vec![task("left", &[]), task("right", &[])]);
        let team = team(BTreeMap::from([
            (
                TaskId::new("left").unwrap(),
                RoleInstanceId::new("worker").unwrap(),
            ),
            (
                TaskId::new("right").unwrap(),
                RoleInstanceId::new("integrator").unwrap(),
            ),
        ]));
        assert_eq!(
            check_dispatch_topology(&plan, &team)[0].code,
            "deliverable_sink_count"
        );
    }

    #[test]
    fn rejects_one_role_on_parallel_tasks_but_allows_a_serial_assignment() {
        let parallel = contract(vec![
            task("left", &[]),
            task("right", &[]),
            task("merge", &["left", "right"]),
        ]);
        let worker = RoleInstanceId::new("worker").unwrap();
        let parallel_team = team(BTreeMap::from([
            (TaskId::new("left").unwrap(), worker.clone()),
            (TaskId::new("right").unwrap(), worker.clone()),
            (
                TaskId::new("merge").unwrap(),
                RoleInstanceId::new("integrator").unwrap(),
            ),
        ]));
        assert!(check_dispatch_topology(&parallel, &parallel_team)
            .iter()
            .any(|error| error.code == "concurrent_role_assignment"));

        let serial = contract(vec![task("left", &[]), task("right", &["left"])]);
        let team = team(BTreeMap::from([
            (TaskId::new("left").unwrap(), worker.clone()),
            (TaskId::new("right").unwrap(), worker),
        ]));
        assert!(check_dispatch_topology(&serial, &team).is_empty());
    }

    #[test]
    fn accepts_a_read_only_report_role_for_an_assigned_task() {
        let plan = contract(vec![task("review", &[])]);
        let reporter = role("reporter", OutputSemantics::ProducesReport);
        let mut team = team(BTreeMap::from([(
            TaskId::new("review").unwrap(),
            reporter.id.clone(),
        )]));
        team.roles.insert(reporter.id.clone(), reporter);

        assert!(
            !check_shape(&plan, &team, &BTreeMap::new(), &MissionConfig::default(), 0,)
                .iter()
                .any(|error| error.code == "task_output_mismatch")
        );
    }

    #[test]
    fn accepts_report_synthesis_shape_with_multiple_dependencies() {
        let plan = contract(vec![
            task("left", &[]),
            task("right", &[]),
            task("review", &["left", "right"]),
        ]);
        let reporter = role("reporter", OutputSemantics::ProducesReport);
        let mut team = team(BTreeMap::from([
            (
                TaskId::new("left").unwrap(),
                RoleInstanceId::new("worker").unwrap(),
            ),
            (
                TaskId::new("right").unwrap(),
                RoleInstanceId::new("integrator").unwrap(),
            ),
            (TaskId::new("review").unwrap(), reporter.id.clone()),
        ]));
        team.roles.insert(reporter.id.clone(), reporter);

        assert!(
            !check_shape(&plan, &team, &BTreeMap::new(), &MissionConfig::default(), 0,)
                .iter()
                .any(|error| error.code == "read_only_task_fan_in")
        );
    }
}
