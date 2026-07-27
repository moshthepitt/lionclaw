//! Decision validation, ported from Zenith (Apache-2.0, Intelligent
//! Internet) `controller.py::_validate_decisions`: a decision must target an
//! open attention item, and the action must be legal for that item's kind
//! (`retry` for reruns, `revise` for planning, and `accept` for explicit
//! waivers), and every decision carries a nonempty reason. Invalid decisions
//! are rejected before any event is recorded, so the fold only ever applies
//! legal transitions.

use crate::prelude::*;

use super::event::DecisionAction;
use super::state::{AttentionKind, MissionState};

const PLAN_PROPOSAL: &[DecisionAction] = &[DecisionAction::Approve, DecisionAction::Revise];
const NODE_FAILED: &[DecisionAction] = &[
    DecisionAction::Retry,
    DecisionAction::Revise,
    DecisionAction::Accept,
];
const NODE_ATTENTION: &[DecisionAction] = &[DecisionAction::Accept];
const ORACLE_FAILED: &[DecisionAction] = &[DecisionAction::Retry, DecisionAction::Revise];
const GATE_FAILED: &[DecisionAction] = &[DecisionAction::Revise, DecisionAction::Accept];
const GATE_CHECKPOINT: &[DecisionAction] = &[DecisionAction::Approve];
const TERMINAL_REVIEW_GAPS: &[DecisionAction] = &[
    DecisionAction::Retry,
    DecisionAction::Revise,
    DecisionAction::Accept,
];
const TERMINAL_REVIEW_FAILED: &[DecisionAction] = &[
    DecisionAction::Retry,
    DecisionAction::Revise,
    DecisionAction::Accept,
];

pub fn legal_actions(state: &MissionState, item: &super::AttentionItem) -> Vec<DecisionAction> {
    let mut actions = match item.kind {
        AttentionKind::PlanProposal => PLAN_PROPOSAL.to_vec(),
        AttentionKind::NodeFailed => NODE_FAILED.to_vec(),
        AttentionKind::NodeAttention => NODE_ATTENTION.to_vec(),
        AttentionKind::OracleFailed => ORACLE_FAILED.to_vec(),
        AttentionKind::GateFailed => GATE_FAILED.to_vec(),
        AttentionKind::GateCheckpoint => GATE_CHECKPOINT.to_vec(),
        AttentionKind::GapReviewGaps => TERMINAL_REVIEW_GAPS.to_vec(),
        AttentionKind::GapReviewFailed => TERMINAL_REVIEW_FAILED.to_vec(),
        AttentionKind::ProofFailed => {
            let Some(failure) = super::verdict::proof_failure_for_attention(state, item) else {
                return Vec::new();
            };
            let mut actions = Vec::new();
            if failure.retry_available(state) {
                actions.push(DecisionAction::Retry);
            }
            if failure.effect_id().is_some() {
                actions.push(DecisionAction::Repair);
            }
            actions.push(DecisionAction::Revise);
            actions
        }
    };
    if item.kind == AttentionKind::NodeFailed && item.task_id.is_none() {
        actions.retain(|action| action != &DecisionAction::Accept);
    }
    actions
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecisionError {
    #[error("no open attention item '{0}'")]
    UnknownItem(String),
    #[error("action {action:?} is not valid for a {kind:?} item")]
    InvalidAction {
        action: DecisionAction,
        kind: AttentionKind,
    },
    #[error("a decision requires a non-empty justification")]
    JustificationRequired,
}

pub fn validate_decision(
    state: &MissionState,
    attention_id: &str,
    action: &DecisionAction,
    justification: &str,
) -> Result<(), DecisionError> {
    let Some(item) = state.open_attention.get(attention_id) else {
        return Err(DecisionError::UnknownItem(attention_id.to_string()));
    };
    if !legal_actions(state, item).contains(action) {
        return Err(DecisionError::InvalidAction {
            action: action.clone(),
            kind: item.kind,
        });
    }
    let justification_missing = if action == &DecisionAction::Revise {
        justification.is_empty()
    } else {
        justification.trim().is_empty()
    };
    if justification_missing {
        return Err(DecisionError::JustificationRequired);
    }
    Ok(())
}
