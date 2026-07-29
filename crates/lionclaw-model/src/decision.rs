//! Decision validation against the single workflow projection.
//!
//! A decision is legal only when the current `Next` advertises the exact
//! target/action pair. The event remains domain-shaped (`DecisionRecorded`);
//! `Choice` is the transient authorization surface.

use super::event::DecisionAction;
use super::state::MissionState;
use super::workflow::{next, Choice};
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecisionError {
    #[error("no current decision target '{0}'")]
    UnknownTarget(String),
    #[error("action {action:?} is not valid for decision target '{id}'")]
    InvalidAction { id: String, action: DecisionAction },
    #[error("a decision requires a non-empty justification")]
    JustificationRequired,
}

pub fn validate_decision(
    state: &MissionState,
    decision_id: &str,
    action: &DecisionAction,
    justification: &str,
) -> Result<(), DecisionError> {
    let projection = next(state);
    let target_exists = projection
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Decide { id, .. } if id == decision_id));
    if !target_exists {
        return Err(DecisionError::UnknownTarget(decision_id.to_string()));
    }
    if !projection.choices.iter().any(|choice| {
        matches!(
            choice,
            Choice::Decide {
                id: choice_issue,
                action: choice_action,
            } if choice_issue == decision_id && choice_action == action
        )
    }) {
        return Err(DecisionError::InvalidAction {
            id: decision_id.to_string(),
            action: action.clone(),
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
