//! Plan vocabulary: the contract of assertions and the task DAG.
//!
//! Shapes ported from Zenith (Apache-2.0, Intelligent Internet) `models.py`
//! (`Task`, `TaskType`, `TaskList`), adapted: one [`Plan`] contains the contract
//! and task list, and tasks reference mission-type
//! *roles* rather than skills.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, OracleName, RequirementId, RoleName, TaskId};

/// What part of the objective a requirement captures. This is descriptive
/// contract structure for people and planning roles; enforcement remains in
/// assertions and authoritative oracles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementKind {
    Capability,
    Constraint,
    Preservation,
    Validation,
}

/// How a plan accounts for one objective requirement. Every requirement is
/// either covered by falsifiable assertions or called out as an explicit
/// limitation; silent omission is not representable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequirementDisposition {
    Covered { assertion_ids: Vec<AssertionId> },
    Limitation { rationale: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub id: RequirementId,
    pub kind: RequirementKind,
    pub prose: String,
    pub disposition: RequirementDisposition,
}

/// How the drive loop consumes a role's handoff. The single closed axis the
/// engine routes on — names never enter enforcement or routing.
///
/// The execution kinds are `ProducesArtifact` (a writer), `EmitsVerdict` (a
/// per-assertion judge), and `EmitsGapVerdict` (the engine-owned objective
/// reviewer). The planning kinds are `ProducesReport` (research/draft/
/// adversary — read-only prose) and `ProposesPlan` (the author, whose handoff
/// carries a complete `Plan`); both are read-only and only ever run in the
/// contract-free planning phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputSemantics {
    ProducesReport,
    ProducesArtifact,
    EmitsVerdict,
    EmitsGapVerdict,
    ProposesPlan,
}

impl OutputSemantics {
    /// The stable kebab-case name (matches the serde repr).
    pub const fn slug(self) -> &'static str {
        match self {
            Self::ProducesReport => "produces-report",
            Self::ProducesArtifact => "produces-artifact",
            Self::EmitsVerdict => "emits-verdict",
            Self::EmitsGapVerdict => "emits-gap-verdict",
            Self::ProposesPlan => "proposes-plan",
        }
    }

    /// The execution task kind this output may serve. Planning and terminal
    /// review outputs never appear in an execution plan.
    pub const fn execution_task_kind(self) -> Option<TaskKind> {
        match self {
            Self::ProducesArtifact => Some(TaskKind::Work),
            Self::EmitsVerdict => Some(TaskKind::Validate),
            Self::ProducesReport | Self::EmitsGapVerdict | Self::ProposesPlan => None,
        }
    }
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

impl TaskKind {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::Validate => "validate",
            Self::Gate => "gate",
        }
    }
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

/// The orchestrator-authored contract and task DAG, validated fail-closed as
/// one unit before anything runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub requirements: Vec<Requirement>,
    pub assertions: Vec<Assertion>,
    pub tasks: Vec<Task>,
}

impl Plan {
    /// Whether every assertion binds an oracle — i.e. the plan is
    /// verified-possible (each claim can be authoritatively judged). Backs the
    /// CLI `verified-possible`/`reviewed-only` ceiling display. (The `Verified`
    /// stop-bar reachability check computes the same condition independently, to
    /// report the offending assertion ids.)
    pub fn all_assertions_bound(&self) -> bool {
        self.assertions.iter().all(|a| a.oracle.is_some())
    }
}

/// A complete candidate plan authored against one accepted plan revision.
/// Initial plans use `base_revision = 0`; every later proposal contains the
/// whole next plan rather than a second language of patch operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanProposal {
    pub base_revision: u32,
    pub plan: Plan,
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
/// planning" — the mission idles awaiting a manually proposed plan.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningDag {
    #[serde(default)]
    pub tasks: Vec<PlanningTask>,
}
