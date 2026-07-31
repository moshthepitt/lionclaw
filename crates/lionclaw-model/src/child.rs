//! Typed parent-to-child mission requests and folded receipts.

use serde::{Deserialize, Serialize};

use super::digest::{canonical_serialize_digest, CanonicalDigest};
use super::{
    ArtifactOutcome, EffectId, MissionConfig, MissionId, MissionProposal, OutputSemantics,
    PayloadRef, TaskCandidateRef, TaskId, TerminalState, TypedFailure,
};
use crate::prelude::*;

pub const MAX_CHILD_MISSION_OBJECTIVE_BYTES: usize = 64 * 1024;
pub const MAX_CHILD_MISSION_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_CHILD_MISSION_DEPTH: u32 = 16;
pub const MAX_CHILD_MISSION_DESCENDANTS: u32 = 1024;

/// A complete child mission authored as one task assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMissionAssignment {
    pub objective: String,
    pub output: OutputSemantics,
    pub config: MissionConfig,
    pub proposal: Box<MissionProposal>,
    pub deadline_secs: u64,
}

impl ChildMissionAssignment {
    /// Canonical identity of the complete authored child request.
    ///
    /// Every map reachable from this type is ordered. The model's canonical
    /// serde sink binds the complete typed request without adding a JSON
    /// dependency to the trusted kernel.
    pub fn digest(&self) -> Option<String> {
        canonical_serialize_digest(
            "lionclaw.child-mission-assignment.v1",
            self,
            MAX_CHILD_MISSION_REQUEST_BYTES,
        )
    }
}

/// Pure, deterministic work request projected by [`crate::next`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMissionRequest {
    pub parent_mission_id: MissionId,
    pub parent_effect_id: EffectId,
    pub child_mission_id: MissionId,
    pub task_id: TaskId,
    pub attempt_no: u32,
    pub request_digest: String,
    pub input_artifact: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_refs: Vec<TaskCandidateRef>,
    pub assignment: Box<ChildMissionAssignment>,
}

impl ChildMissionRequest {
    pub fn derive(
        parent_mission_id: &MissionId,
        task_id: TaskId,
        attempt_no: u32,
        input_artifact: String,
        dependency_refs: Vec<TaskCandidateRef>,
        assignment: ChildMissionAssignment,
    ) -> Option<Self> {
        let assignment_digest = assignment.digest()?;
        let mut digest = CanonicalDigest::new("lionclaw.child-mission-request.v1");
        digest.str("assignment_digest", &assignment_digest);
        digest.str("input_artifact", &input_artifact);
        digest.u64("dependency_count", dependency_refs.len() as u64);
        for (index, dependency) in dependency_refs.iter().enumerate() {
            digest.str(
                &format!("dependency.{index}.task_id"),
                dependency.task_id.as_str(),
            );
            digest.str(&format!("dependency.{index}.sha"), &dependency.sha);
        }
        let request_digest = digest.finish();
        let parent_effect_id =
            EffectId::for_child_mission(parent_mission_id, &task_id, attempt_no, &request_digest);
        let child_mission_id = MissionId::for_child_mission(
            parent_mission_id,
            &parent_effect_id,
            attempt_no,
            &request_digest,
        );
        Some(Self {
            parent_mission_id: parent_mission_id.clone(),
            parent_effect_id,
            child_mission_id,
            task_id,
            attempt_no,
            request_digest,
            input_artifact,
            dependency_refs,
            assignment: Box::new(assignment),
        })
    }
}

/// Kernel-owned ancestry recorded only by child creation admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionLineage {
    pub parent_mission_id: MissionId,
    pub parent_effect_id: EffectId,
    pub root_mission_id: MissionId,
    pub depth: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildMissionOutput {
    Artifact {
        artifact: ArtifactOutcome,
    },
    Report {
        report: PayloadRef,
        report_sha256: String,
    },
}

impl ChildMissionOutput {
    pub const fn semantics(&self) -> OutputSemantics {
        match self {
            Self::Artifact { .. } => OutputSemantics::ProducesArtifact,
            Self::Report { .. } => OutputSemantics::ProducesReport,
        }
    }

    pub fn report(&self) -> Option<&PayloadRef> {
        match self {
            Self::Report { report, .. } => Some(report),
            Self::Artifact { .. } => None,
        }
    }
}

/// Digest-only child proof summary. It is audit evidence, never parent proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildProofSummary {
    pub finish: Option<super::FinishClass>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authoritative_receipt_digests: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advisory_receipt_digests: Vec<String>,
}

/// Parent-side receipt derived by the kernel from folded child truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMissionReceipt {
    pub parent_mission_id: MissionId,
    pub parent_effect_id: EffectId,
    pub child_mission_id: MissionId,
    pub request_digest: String,
    pub input_artifact: String,
    pub terminal: TerminalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<ChildMissionOutput>,
    pub proof: ChildProofSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<TypedFailure>,
}

impl ChildMissionReceipt {
    pub fn matches_request(&self, request: &ChildMissionRequest) -> bool {
        self.parent_mission_id == request.parent_mission_id
            && self.parent_effect_id == request.parent_effect_id
            && self.child_mission_id == request.child_mission_id
            && self.request_digest == request.request_digest
            && self.input_artifact == request.input_artifact
            && self
                .output
                .as_ref()
                .is_none_or(|output| output.semantics() == request.assignment.output)
    }

    pub fn succeeded(&self) -> bool {
        matches!(self.terminal, TerminalState::Done { .. })
            && self.failure.is_none()
            && self.output.is_some()
    }
}
