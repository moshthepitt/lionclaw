//! Plan vocabulary: the contract of assertions and the task DAG.
//!
//! Shapes ported from Zenith (Apache-2.0, Intelligent Internet) `models.py`
//! (`Task`, `TaskType`, `TaskList`), adapted: contract + task list are
//! submitted together as one [`PlanSubmission`], and tasks reference mission-type
//! *roles* rather than skills.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, OracleName, RoleName, TaskId};

/// How the drive loop consumes a role's handoff. The single closed axis the
/// engine routes on — names never enter enforcement or routing.
///
/// The execution kinds are `ProducesArtifact` (a writer) and `EmitsVerdict` (a
/// read-only judge). The planning kinds are `ProducesReport` (research/draft/
/// adversary — read-only prose) and `ProposesPlan` (the author, whose handoff
/// carries a `PlanSubmission`); both are read-only and only ever run in the
/// contract-free planning phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputSemantics {
    ProducesReport,
    ProducesArtifact,
    EmitsVerdict,
    ProposesPlan,
}

/// One falsifiable claim in the mission contract. `oracle` binds it to a
/// worker-independent engine-run check; without one it can only ever be
/// covered by advisory verdicts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assertion {
    pub id: AssertionId,
    pub prose: String,
    #[serde(default)]
    pub oracle: Option<OracleName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Work,
    Validate,
    Gate,
}

/// A DAG node. Dependencies are inline adjacency (`depends_on`); list order
/// is a topological tie-break hint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    pub id: TaskId,
    pub kind: TaskKind,
    /// Mission-specific instruction (must be empty for gates).
    #[serde(default)]
    pub body: String,
    /// Contract assertion ids this task addresses.
    #[serde(default)]
    pub targets: Vec<AssertionId>,
    /// Mission-type role dispatched for this task (required for work/validate,
    /// forbidden for gates).
    #[serde(default)]
    pub role: Option<RoleName>,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
}

/// The orchestrator-authored plan: contract + task DAG, submitted as one
/// unit and validated fail-closed before anything runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSubmission {
    pub assertions: Vec<Assertion>,
    pub tasks: Vec<Task>,
}

impl PlanSubmission {
    /// Whether every assertion binds an oracle — i.e. the plan is
    /// verified-possible (each claim can be authoritatively judged). The one
    /// predicate behind the `verified-possible`/`reviewed-only` display and the
    /// `Verified` stop-bar reachability check.
    pub fn all_assertions_bound(&self) -> bool {
        self.assertions.iter().all(|a| a.oracle.is_some())
    }
}

/// A node in the contract-free planning DAG. Unlike a `Task` it has no `kind`
/// and no `targets`: planning produces a *proposal*, not contract coverage. Its
/// role is always a `ProducesReport` or the single `ProposesPlan` author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningTask {
    pub id: TaskId,
    pub role: RoleName,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
}

/// The planning DAG a mission type ships: how an objective becomes a proposed
/// contract (research → draft → adversary → author). Empty means "no in-engine
/// planning" — the mission idles awaiting a manually submitted plan.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningDag {
    #[serde(default)]
    pub tasks: Vec<PlanningTask>,
}
