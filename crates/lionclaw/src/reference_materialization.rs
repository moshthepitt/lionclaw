//! Bounded, transient expansion of durable message reference identities.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::model::{
    EventEnvelope, MessageReference, MissionEvent, MissionState, OracleRunSuccess, ParkedEffect,
    PayloadRef, TypedFailure,
};
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

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct ReferenceExpansionLimit(String);

pub async fn materialize_references(
    state: &MissionState,
    events: &[EventEnvelope],
    blobs: &BlobStore,
    repo: &Path,
    references: &[MessageReference],
) -> std::result::Result<Vec<MaterializedReference>, ReferenceMaterializationError> {
    ReferenceMaterializer::new(state, events, blobs, repo, references.iter())?
        .materialize(references)
        .await
}

pub(crate) fn reference_was_authoritative(
    events: &[EventEnvelope],
    reference: &MessageReference,
) -> Result<bool> {
    let (receipts, parks, _) = requested_authority(std::iter::once(reference));
    let authority = replayed_reference_authority(events, &receipts, &parks)?;
    Ok(match reference {
        MessageReference::AuthoritativeReceipt { effect_id } => {
            authority.receipts.contains_key(effect_id)
        }
        MessageReference::ParkEvidence { effect_id } => authority.parks.contains_key(effect_id),
        MessageReference::ReachableCommit { .. } => false,
    })
}

pub(crate) struct ReferenceMaterializer<'a> {
    state: &'a MissionState,
    blobs: &'a BlobStore,
    repo: &'a Path,
    authority: ReplayedReferenceAuthority,
}

impl<'a> ReferenceMaterializer<'a> {
    pub(crate) fn new<'r>(
        state: &'a MissionState,
        events: &[EventEnvelope],
        blobs: &'a BlobStore,
        repo: &'a Path,
        references: impl IntoIterator<Item = &'r MessageReference>,
    ) -> std::result::Result<Self, ReferenceMaterializationError> {
        let (requested_receipts, requested_parks, authority_reference) =
            requested_authority(references);
        let authority = replayed_reference_authority(events, &requested_receipts, &requested_parks)
            .map_err(|error| ReferenceMaterializationError {
                reference: authority_reference
                    .expect("authority replay only runs for receipt or park references"),
                cause: classify_unavailability(&error),
                detail: format!("{error:#}"),
            })?;
        Ok(Self {
            state,
            blobs,
            repo,
            authority,
        })
    }

    pub(crate) async fn materialize(
        &self,
        references: &[MessageReference],
    ) -> std::result::Result<Vec<MaterializedReference>, ReferenceMaterializationError> {
        let mut total = 0usize;
        let mut expanded = Vec::with_capacity(references.len());
        for reference in references {
            let materialized: Result<(String, String, Vec<u8>)> = async {
                Ok(match reference {
                    MessageReference::AuthoritativeReceipt { effect_id } => {
                        if !self.state.authoritative_receipts.contains(effect_id) {
                            bail!("authoritative receipt {effect_id} is not valid in this mission");
                        }
                        let success =
                            self.authority.receipts.get(effect_id).with_context(|| {
                                format!("authoritative receipt {effect_id} is missing")
                            })?;
                        let mut text = format!("exit_code: {}\n", success.exit_code);
                        append_payload(&mut text, "stdout", self.blobs, &success.stdout)?;
                        append_payload(&mut text, "stderr", self.blobs, &success.stderr)?;
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
                        // The fold may reclassify a raw success because a durable
                        // stop, deadline, or output contract wins. Capture the
                        // authoritative failure at the point this effect parks;
                        // immutable replay keeps it available after `continue`.
                        let failure = self.authority.parks.get(effect_id).with_context(|| {
                            format!("park evidence {effect_id} is missing or unmaterializable")
                        })?;
                        let bytes = serde_json::to_vec_pretty(failure)
                            .context("serializing authoritative park failure")?;
                        ("park evidence".to_string(), effect_id.to_string(), bytes)
                    }
                    MessageReference::ReachableCommit { sha } => {
                        if !self.state.reachable_commits.contains(sha) {
                            bail!("commit {sha} is not reachable in this mission");
                        }
                        let bytes =
                            crate::workspace::show_commit(self.repo, sha, MAX_REFERENCE_BYTES)
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
}

fn classify_unavailability(error: &anyhow::Error) -> crate::model::UnavailableReferenceCause {
    for source in error.chain() {
        if source.downcast_ref::<ReferenceExpansionLimit>().is_some() {
            return crate::model::UnavailableReferenceCause::ExpansionLimitExceeded;
        }
        if let Some(error) = source.downcast_ref::<crate::workspace::CommitMaterializationError>() {
            return match error {
                crate::workspace::CommitMaterializationError::Missing => {
                    crate::model::UnavailableReferenceCause::SourceMissing
                }
                crate::workspace::CommitMaterializationError::InvalidContent(_) => {
                    crate::model::UnavailableReferenceCause::InvalidContent
                }
                crate::workspace::CommitMaterializationError::Unreadable(_) => {
                    crate::model::UnavailableReferenceCause::SourceUnreadable
                }
                crate::workspace::CommitMaterializationError::ExpansionLimit => {
                    crate::model::UnavailableReferenceCause::ExpansionLimitExceeded
                }
            };
        }
        if matches!(
            source.downcast_ref::<crate::store::BlobReadError>(),
            Some(crate::store::BlobReadError::InvalidContent(_))
        ) {
            return crate::model::UnavailableReferenceCause::InvalidContent;
        }
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
    ensure_payload_within_limit(payload, &format!("receipt {label}"))?;
    let value = blobs
        .resolve_bounded(payload, MAX_REFERENCE_BYTES)
        .with_context(|| format!("receipt {label} exceeds its materialization bound"))?;
    text.push_str(label);
    text.push_str(":\n");
    text.push_str(&value);
    text.push('\n');
    Ok(())
}

fn ensure_payload_within_limit(payload: &PayloadRef, label: &str) -> Result<()> {
    let declared = payload.declared_len();
    if declared > MAX_REFERENCE_BYTES as u64 {
        return Err(ReferenceExpansionLimit(format!(
            "{label} declares {declared} bytes, which exceeds the {MAX_REFERENCE_BYTES}-byte limit"
        ))
        .into());
    }
    Ok(())
}

#[derive(Default)]
struct ReplayedReferenceAuthority {
    receipts: BTreeMap<crate::model::EffectId, OracleRunSuccess>,
    parks: BTreeMap<crate::model::EffectId, TypedFailure>,
}

fn replayed_reference_authority(
    events: &[EventEnvelope],
    requested_receipts: &BTreeSet<crate::model::EffectId>,
    requested_parks: &BTreeSet<crate::model::EffectId>,
) -> Result<ReplayedReferenceAuthority> {
    if requested_receipts.is_empty() && requested_parks.is_empty() {
        return Ok(ReplayedReferenceAuthority::default());
    }
    let mut events = events.iter();
    let first = events
        .next()
        .context("park evidence has no event history")?;
    let mut replay =
        crate::model::fold([first.clone()]).context("park evidence has no mission authority")?;
    let mut authority = ReplayedReferenceAuthority::default();
    capture_reference_authority(
        &replay,
        first,
        requested_receipts,
        requested_parks,
        &mut authority,
    );
    for envelope in events {
        crate::model::apply(&mut replay, envelope);
        capture_reference_authority(
            &replay,
            envelope,
            requested_receipts,
            requested_parks,
            &mut authority,
        );
        if authority.receipts.len() == requested_receipts.len()
            && authority.parks.len() == requested_parks.len()
        {
            break;
        }
    }
    Ok(authority)
}

fn requested_authority<'r>(
    references: impl IntoIterator<Item = &'r MessageReference>,
) -> (
    BTreeSet<crate::model::EffectId>,
    BTreeSet<crate::model::EffectId>,
    Option<MessageReference>,
) {
    let mut receipts = BTreeSet::new();
    let mut parks = BTreeSet::new();
    let mut first = None;
    for reference in references {
        match reference {
            MessageReference::AuthoritativeReceipt { effect_id } => {
                receipts.insert(effect_id.clone());
                first.get_or_insert_with(|| reference.clone());
            }
            MessageReference::ParkEvidence { effect_id } => {
                parks.insert(effect_id.clone());
                first.get_or_insert_with(|| reference.clone());
            }
            MessageReference::ReachableCommit { .. } => {}
        }
    }
    (receipts, parks, first)
}

fn capture_reference_authority(
    state: &MissionState,
    envelope: &EventEnvelope,
    requested_receipts: &BTreeSet<crate::model::EffectId>,
    requested_parks: &BTreeSet<crate::model::EffectId>,
    authority: &mut ReplayedReferenceAuthority,
) {
    if let MissionEvent::OracleRunCompleted {
        effect_id,
        outcome: Ok(success),
        ..
    } = &envelope.event
    {
        if requested_receipts.contains(effect_id)
            && state.authoritative_receipts.contains(effect_id)
        {
            authority
                .receipts
                .entry(effect_id.clone())
                .or_insert_with(|| success.clone());
        }
    }
    let completed_effect = match &envelope.event {
        MissionEvent::RoleTurnCompleted { effect_id, .. }
        | MissionEvent::OracleRunCompleted { effect_id, .. } => Some(effect_id),
        _ => None,
    };
    if let Some(effect_id) = completed_effect.filter(|id| requested_parks.contains(*id)) {
        if let Some(failure) = parked_failure(state, effect_id) {
            authority.parks.insert(effect_id.clone(), failure.clone());
        }
    }
}

fn parked_failure<'a>(
    state: &'a MissionState,
    effect_id: &crate::model::EffectId,
) -> Option<&'a TypedFailure> {
    match state.parked_effects.get(effect_id)? {
        ParkedEffect::RoleTurn { .. } => state
            .role_attempt_receipts
            .get(effect_id)
            .and_then(crate::model::RoleAttemptReceipt::failure),
        ParkedEffect::OracleRun { oracle } => state.oracle_failures.get(oracle),
    }
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

    #[test]
    fn rejected_completion_never_becomes_historical_reference_authority() {
        let mission_id = crate::model::MissionId::parse("m000000000001").unwrap();
        let effect_id = crate::model::EffectId::parse("0".repeat(64)).unwrap();
        let envelope = |sequence_no, event| EventEnvelope {
            mission_id: mission_id.clone(),
            sequence_no,
            recorded_at_ms: 0,
            stamps: crate::model::VersionStamps::default(),
            event,
        };
        let events = vec![
            envelope(
                1,
                MissionEvent::MissionCreated {
                    objective: "reference authority".into(),
                    mission_type: crate::model::MissionTypeRef {
                        name: "test".into(),
                        digest: "digest".into(),
                    },
                    image_id: "image".into(),
                    workspace_dir: "/workspace".into(),
                    base_sha: "base".into(),
                    config: crate::model::MissionConfig::default(),
                    delegation: crate::model::DelegationSet::none(),
                },
            ),
            envelope(
                2,
                MissionEvent::OracleRunCompleted {
                    assertion_ids: vec![],
                    oracle: crate::model::OracleName::new("forged-oracle").unwrap(),
                    judged_sha: "unrequested".into(),
                    attempt_no: 1,
                    effect_id: effect_id.clone(),
                    outcome: Ok(OracleRunSuccess {
                        exit_code: 0,
                        exit_signal: None,
                        stdout: PayloadRef::inline("forged prose"),
                        stderr: PayloadRef::inline(""),
                        prepared_inputs: vec![],
                        duration_ms: 1,
                    }),
                },
            ),
        ];

        for reference in [
            MessageReference::AuthoritativeReceipt {
                effect_id: effect_id.clone(),
            },
            MessageReference::ParkEvidence { effect_id },
        ] {
            assert!(!reference_was_authoritative(&events, &reference).unwrap());
        }
    }
}
