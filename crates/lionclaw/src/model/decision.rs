//! Decision validation, ported from Zenith (Apache-2.0, Intelligent
//! Internet) `controller.py::_validate_decisions`: a decision must target an
//! open attention item, and the action must be legal for that item's kind
//! (`retry`/`continue` only for node failures, `ratify` only for the
//! ratification gate, etc.). Invalid decisions are rejected before any event
//! is recorded, so the fold only ever applies legal transitions.

use super::event::DecisionAction;
use super::state::{AttentionKind, MissionState};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecisionError {
    #[error("no open attention item '{0}'")]
    UnknownItem(String),
    #[error("action {action:?} is not valid for a {kind:?} item")]
    InvalidAction {
        action: DecisionAction,
        kind: AttentionKind,
    },
}

pub fn validate_decision(
    state: &MissionState,
    attention_id: &str,
    action: &DecisionAction,
) -> Result<(), DecisionError> {
    let Some(item) = state.open_attention.get(attention_id) else {
        return Err(DecisionError::UnknownItem(attention_id.to_string()));
    };
    let legal = match (action, item.kind) {
        (DecisionAction::Ratify, AttentionKind::Ratify) => true,
        (DecisionAction::Retry, AttentionKind::NodeFailed | AttentionKind::OracleFailed) => true,
        (
            DecisionAction::Continue,
            AttentionKind::NodeFailed
            | AttentionKind::NodeAttention
            | AttentionKind::OracleFailed
            | AttentionKind::GateCheckpoint
            | AttentionKind::GateFailed
            | AttentionKind::TerminalReview,
        ) => true,
        // Abort is always available while an item is open.
        (DecisionAction::Abort, _) => true,
        _ => false,
    };
    if legal {
        Ok(())
    } else {
        Err(DecisionError::InvalidAction {
            action: action.clone(),
            kind: item.kind,
        })
    }
}
