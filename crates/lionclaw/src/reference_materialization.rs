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

pub async fn materialize_references(
    state: &MissionState,
    events: &[EventEnvelope],
    blobs: &BlobStore,
    repo: &Path,
    references: &[MessageReference],
) -> Result<Vec<MaterializedReference>> {
    let mut total = 0usize;
    let mut expanded = Vec::with_capacity(references.len());
    for reference in references {
        let (label, identity, bytes) = match reference {
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
                if !state.parked_effects.contains_key(effect_id) {
                    bail!("park evidence {effect_id} is not valid in this mission");
                }
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
        };
        account_materialized_bytes(&label, &identity, bytes.len(), &mut total)?;
        let content = String::from_utf8(bytes)
            .with_context(|| format!("{label} {identity} is not UTF-8 material"))?;
        expanded.push(MaterializedReference {
            label,
            identity,
            content,
        });
    }
    Ok(expanded)
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
    let next = total
        .checked_add(bytes)
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

        for index in 0..(MAX_EXPANDED_REFERENCE_BYTES / MAX_REFERENCE_BYTES) {
            account_materialized_bytes(
                "authoritative receipt",
                &index.to_string(),
                MAX_REFERENCE_BYTES,
                &mut total,
            )
            .unwrap();
        }
        assert!(account_materialized_bytes("park evidence", "overflow", 1, &mut total).is_err());
    }

    #[test]
    fn expansion_limits_accept_the_exact_bound() {
        let mut total = MAX_EXPANDED_REFERENCE_BYTES - MAX_REFERENCE_BYTES;
        account_materialized_bytes("reachable commit", "a", MAX_REFERENCE_BYTES, &mut total)
            .unwrap();
        assert_eq!(total, MAX_EXPANDED_REFERENCE_BYTES);
    }
}
