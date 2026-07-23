//! Plan vocabulary: the contract of assertions and the task DAG.
//!
//! Shapes ported from Zenith (Apache-2.0, Intelligent Internet) `models.py`
//! (`Task`, `TaskType`, `TaskList`), adapted: one [`Plan`] contains only the
//! contract and work graph; role contracts and assignments live in the team.

use serde::{Deserialize, Serialize};

use super::ids::{AssertionId, OracleName, RequirementId, TaskId};
use crate::prelude::*;

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

/// Host-resource lifetime implied by the closed output contract. Roles that
/// can pause for dialogue retain their state with the conversation; mandatory
/// judgment turns are one-shot and remain inside their disposable effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleResourceLifetime {
    Conversation,
    Effect,
}

impl OutputSemantics {
    /// Whether lead messages may carry producer-controlled reference prose to
    /// this output boundary. Judgment outputs receive only engine-owned
    /// evidence assembled by their dedicated prompt paths.
    pub const fn permits_message_references(self) -> bool {
        match self {
            Self::ProducesReport | Self::ProducesArtifact | Self::ProposesPlan => true,
            Self::EmitsVerdict | Self::EmitsGapVerdict => false,
        }
    }

    /// Whether a completed turn must carry the typed handoff for this output.
    /// Dialogue-producing roles may pause to ask the lead a question; judges
    /// cannot turn an absent verdict into dialogue.
    pub const fn requires_handoff(self) -> bool {
        match self {
            Self::ProducesReport | Self::ProducesArtifact | Self::ProposesPlan => false,
            Self::EmitsVerdict | Self::EmitsGapVerdict => true,
        }
    }

    pub const fn resource_lifetime(self) -> RoleResourceLifetime {
        if self.requires_handoff() {
            RoleResourceLifetime::Effect
        } else {
            RoleResourceLifetime::Conversation
        }
    }

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
}

#[cfg(test)]
mod output_semantics_tests {
    use super::{OutputSemantics, RoleResourceLifetime};

    #[test]
    fn output_semantics_derive_resource_lifetime_exhaustively() {
        for output in [
            OutputSemantics::ProducesReport,
            OutputSemantics::ProducesArtifact,
            OutputSemantics::ProposesPlan,
        ] {
            assert_eq!(
                output.resource_lifetime(),
                RoleResourceLifetime::Conversation
            );
        }
        for output in [
            OutputSemantics::EmitsVerdict,
            OutputSemantics::EmitsGapVerdict,
        ] {
            assert_eq!(output.resource_lifetime(), RoleResourceLifetime::Effect);
        }
    }

    #[test]
    fn reference_policy_is_exhaustive_and_judgment_safe() {
        assert!(OutputSemantics::ProducesReport.permits_message_references());
        assert!(OutputSemantics::ProducesArtifact.permits_message_references());
        assert!(OutputSemantics::ProposesPlan.permits_message_references());
        assert!(!OutputSemantics::EmitsVerdict.permits_message_references());
        assert!(!OutputSemantics::EmitsGapVerdict.permits_message_references());
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

/// One artifact-producing work assignment. Judgment and gates derive from
/// assertions, receipts, and the active team rather than authored task nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    pub id: TaskId,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub targets: Vec<AssertionId>,
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
    /// Covered requirements intentionally removed or weakened by this
    /// revision. Approval records this exact set as an explicit decision.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requirement_changes: Vec<RequirementId>,
    /// Prior assertions corrected or retired by this revision.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertion_supersessions: Vec<AssertionSupersession>,
    pub plan: Plan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionSupersession {
    pub assertion_id: AssertionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replacement_ids: Vec<AssertionId>,
}
