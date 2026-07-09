//! Plan vocabulary: the contract of assertions and the task DAG.
//!
//! Shapes ported from Zenith (Apache-2.0, Intelligent Internet) `models.py`
//! (`Task`, `TaskType`, `TaskList`), adapted: contract + task list are
//! submitted together as one [`PlanSubmission`], and tasks reference plugin
//! *roles* rather than skills.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, OracleName, RoleName, TaskId};

/// How the drive loop consumes a role's handoff. The single closed axis the
/// engine routes on — names never enter enforcement or routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputSemantics {
    Plans,
    ProducesArtifact,
    EmitsVerdict,
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
    /// Plugin role dispatched for this task (required for work/validate,
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
