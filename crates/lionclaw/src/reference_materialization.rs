//! Bounded, transient expansion of durable message reference identities.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::model::{EventEnvelope, MessageReference, MissionEvent, MissionState, PayloadRef};
use crate::store::BlobStore;

pub const MAX_REFERENCE_BYTES: usize = 64 * 1024;
pub const MAX_EXPANDED_REFERENCE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedReference {
    pub label: String,
    pub identity: String,
    pub content: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct ReferenceMaterializationError {
    pub reference: MessageReference,
    pub cause: crate::model::UnavailableReferenceCause,
    detail: String,
}

pub async fn materialize_references(
    state: &MissionState,
    events: &[EventEnvelope],
    blobs: &BlobStore,
    repo: &Path,
    references: &[MessageReference],
) -> std::result::Result<Vec<MaterializedReference>, ReferenceMaterializationError> {
    let mut total = 0usize;
    let mut expanded = Vec::with_capacity(references.len());
    for reference in references {
        let materialized: Result<(String, String, Vec<u8>)> = async {
            Ok(match reference {
                MessageReference::AuthoritativeReceipt { effect_id } => {
                    if !state.authoritative_receipts.contains(effect_id) {
                        bail!("authoritative receipt {effect_id} is not valid in this mission");
                    }
                    let success = events
                        .iter()
                        .find_map(|envelope| match &envelope.event {
                            MissionEvent::OracleRunCompleted {
                                effect_id: id,
                                outcome: Ok(success),
                                ..
                            } if id == effect_id => Some(success),
                            _ => None,
                        })
                        .with_context(|| format!("authoritative receipt {effect_id} is missing"))?;
                    let mut text = format!("exit_code: {}\n", success.exit_code);
                    append_payload(&mut text, "stdout", blobs, &success.stdout)?;
                    append_payload(&mut text, "stderr", blobs, &success.stderr)?;
                    (
                        "authoritative receipt".to_string(),
                        effect_id.to_string(),
                        text.into_bytes(),
                    )
                }
                MessageReference::ParkEvidence { effect_id } => {
                    // Ingress authorizes park evidence while the effect is parked.
                    // A later `continue` deliberately removes that live-state entry,
                    // but must not invalidate the immutable queued message boundary.
                    // The same-mission completed failure is the durable material.
                    let failure = events
                        .iter()
                        .rev()
                        .find_map(|envelope| match &envelope.event {
                            MissionEvent::RoleRunCompleted {
                                effect_id: id,
                                outcome: Err(failure),
                                ..
                            }
                            | MissionEvent::OracleRunCompleted {
                                effect_id: id,
                                outcome: Err(failure),
                                ..
                            } if id == effect_id => Some(failure),
                            MissionEvent::TerminalReviewCompleted {
                                effect_id: id,
                                outcome: Err(failure),
                                ..
                            } if id == effect_id => Some(failure),
                            _ => None,
                        })
                        .with_context(|| {
                            format!("park evidence {effect_id} is missing or unmaterializable")
                        })?;
                    let bytes =
                        serde_json::to_vec_pretty(failure).context("serializing park evidence")?;
                    ("park evidence".to_string(), effect_id.to_string(), bytes)
                }
                MessageReference::ReachableCommit { sha } => {
                    if !state.reachable_commits.contains(sha) {
                        bail!("commit {sha} is not reachable in this mission");
                    }
                    let bytes = crate::workspace::show_commit(repo, sha)
                        .await
                        .with_context(|| format!("materializing reachable commit {sha}"))?;
                    ("reachable commit".to_string(), sha.clone(), bytes)
                }
            })
        }
        .await;
        let (label, identity, bytes) =
            materialized.map_err(|error| ReferenceMaterializationError {
                reference: reference.clone(),
                cause: classify_unavailability(&error),
                detail: format!("{error:#}"),
            })?;
        account_materialized_bytes(&label, &identity, bytes.len(), &mut total).map_err(
            |error| ReferenceMaterializationError {
                reference: reference.clone(),
                cause: crate::model::UnavailableReferenceCause::ExpansionLimitExceeded,
                detail: error.to_string(),
            },
        )?;
        let content = String::from_utf8(bytes).map_err(|_| ReferenceMaterializationError {
            reference: reference.clone(),
            cause: crate::model::UnavailableReferenceCause::InvalidContent,
            detail: format!("{label} {identity} is not UTF-8 material"),
        })?;
        expanded.push(MaterializedReference {
            label,
            identity,
            content,
        });
    }
    Ok(expanded)
}

fn classify_unavailability(error: &anyhow::Error) -> crate::model::UnavailableReferenceCause {
    for source in error.chain() {
        if let Some(io) = source.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::NotFound => {
                    crate::model::UnavailableReferenceCause::SourceMissing
                }
                std::io::ErrorKind::PermissionDenied => {
                    crate::model::UnavailableReferenceCause::SourceUnreadable
                }
                _ => crate::model::UnavailableReferenceCause::SourceUnreadable,
            };
        }
    }
    crate::model::UnavailableReferenceCause::SourceMissing
}

fn account_materialized_bytes(
    label: &str,
    identity: &str,
    bytes: usize,
    total: &mut usize,
) -> Result<()> {
    if bytes > MAX_REFERENCE_BYTES {
        bail!("{label} {identity} exceeds the per-reference expansion bound");
    }
    // `render_conversation_message` inserts exactly this labelled wrapper in
    // the typed request.  Budget the expansion that crosses that boundary,
    // not merely the referenced source payload.
    let rendered_bytes = label
        .len()
        .checked_add(1)
        .and_then(|size| size.checked_add(identity.len()))
        .and_then(|size| size.checked_add(2))
        .and_then(|size| size.checked_add(bytes))
        .context("reference expansion size overflow")?;
    let next = total
        .checked_add(rendered_bytes)
        .context("reference expansion size overflow")?;
    if next > MAX_EXPANDED_REFERENCE_BYTES {
        bail!("message references exceed the aggregate expansion bound");
    }
    *total = next;
    Ok(())
}

fn append_payload(
    text: &mut String,
    label: &str,
    blobs: &BlobStore,
    payload: &PayloadRef,
) -> Result<()> {
    let value = blobs
        .resolve_bounded(payload, MAX_REFERENCE_BYTES)
        .with_context(|| format!("receipt {label} exceeds its materialization bound"))?;
    text.push_str(label);
    text.push_str(":\n");
    text.push_str(&value);
    text.push('\n');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_and_aggregate_limits_fail_closed() {
        let mut total = 0;
        assert!(account_materialized_bytes(
            "reachable commit",
            "a",
            MAX_REFERENCE_BYTES + 1,
            &mut total
        )
        .is_err());
        assert_eq!(
            total, 0,
            "a rejected item must not consume aggregate budget"
        );

        let wrapper = "authoritative receipt".len() + 1 + "0".len() + 2;
        let content = MAX_REFERENCE_BYTES - wrapper;
        for _ in 0..(MAX_EXPANDED_REFERENCE_BYTES / MAX_REFERENCE_BYTES) {
            account_materialized_bytes("authoritative receipt", "0", content, &mut total).unwrap();
        }
        assert!(account_materialized_bytes("park evidence", "overflow", 1, &mut total).is_err());
    }

    #[test]
    fn expansion_limits_accept_the_exact_bound() {
        let wrapper = "reachable commit".len() + 1 + "a".len() + 2;
        let mut total = MAX_EXPANDED_REFERENCE_BYTES - MAX_REFERENCE_BYTES - wrapper;
        account_materialized_bytes("reachable commit", "a", MAX_REFERENCE_BYTES, &mut total)
            .unwrap();
        assert_eq!(total, MAX_EXPANDED_REFERENCE_BYTES);
    }

    #[test]
    fn labelled_wrapper_cannot_escape_the_aggregate_bound() {
        let wrapper = "park evidence".len() + 1 + "effect".len() + 2;
        let mut total = MAX_EXPANDED_REFERENCE_BYTES - MAX_REFERENCE_BYTES;
        let error = account_materialized_bytes(
            "park evidence",
            "effect",
            MAX_REFERENCE_BYTES - wrapper + 1,
            &mut total,
        )
        .expect_err("label and identity bytes are part of transient expansion");
        assert!(error.to_string().contains("aggregate expansion bound"));
        assert_eq!(total, MAX_EXPANDED_REFERENCE_BYTES - MAX_REFERENCE_BYTES);
    }
}
