//! One rendering path for authoritative failure evidence. Events retain full
//! payloads; prompts and CLI views receive deterministic labelled excerpts.

use anyhow::Result;

use crate::model::{
    DecisionEvidence, EffectId, FailureEvidence, FailureFeedback, MissionState, PayloadRef,
    RoleAttemptDisposition, RoleAttemptReceipt, RoleEffectSource, RoleHandoffObservation,
    RoleTurnObservation,
};
use crate::store::BlobStore;

const HALF_EXCERPT_BYTES: usize = 4096;

pub fn render_evidence(blobs: &BlobStore, evidence: &FailureEvidence) -> Result<String> {
    let stdout = render_payload(blobs, &evidence.stdout);
    let stderr = render_payload(blobs, &evidence.stderr);
    Ok(format!(
        "exit code: {}\nsignal: {}\nstdout:\n{}\nstderr:\n{}",
        evidence.exit_code,
        evidence
            .exit_signal
            .map_or_else(|| "none".to_string(), |signal| signal.to_string()),
        stdout,
        stderr,
    ))
}

pub fn evidence_json(blobs: &BlobStore, evidence: &FailureEvidence) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "exit_code": evidence.exit_code,
        "exit_signal": evidence.exit_signal,
        "stdout": render_payload(blobs, &evidence.stdout),
        "stderr": render_payload(blobs, &evidence.stderr),
    }))
}

pub fn render_feedback(
    blobs: &BlobStore,
    state: &MissionState,
    feedback: &FailureFeedback,
) -> Result<String> {
    let mut rendered = feedback.summary.clone();
    if !feedback.justification.trim().is_empty() {
        rendered.push_str("\nDecision guidance: ");
        rendered.push_str(&feedback.justification);
    }
    let evidence = render_decision_evidence(blobs, state, &feedback.evidence)?;
    if !evidence.is_empty() {
        rendered.push('\n');
        rendered.push_str(&evidence);
    }
    Ok(rendered)
}

pub fn decision_evidence_json(
    blobs: &BlobStore,
    state: &MissionState,
    evidence: &DecisionEvidence,
) -> Result<serde_json::Value> {
    Ok(match evidence {
        DecisionEvidence::None => serde_json::Value::Null,
        DecisionEvidence::RoleAttempts { effect_ids } => serde_json::json!({
            "kind": "role_attempts",
            "receipts": effect_ids
                .iter()
                .map(|effect_id| role_attempt_reference_json(blobs, state, effect_id))
                .collect::<Vec<_>>(),
        }),
        DecisionEvidence::OracleRuntimeFailure { failure } => serde_json::json!({
            "kind": "oracle_runtime_failure",
            "failure": failure,
        }),
        DecisionEvidence::OracleVerdict { evidence } => serde_json::json!({
            "kind": "oracle_verdict",
            "evidence": evidence_json(blobs, evidence)?,
        }),
    })
}

pub fn render_decision_evidence(
    blobs: &BlobStore,
    state: &MissionState,
    evidence: &DecisionEvidence,
) -> Result<String> {
    Ok(match evidence {
        DecisionEvidence::None => String::new(),
        DecisionEvidence::RoleAttempts { effect_ids } => {
            let mut rendered = String::new();
            for effect_id in effect_ids {
                if !rendered.is_empty() {
                    rendered.push('\n');
                }
                rendered.push_str("Role attempt:\n");
                for line in render_role_attempt_reference(blobs, state, effect_id).lines() {
                    rendered.push_str("  ");
                    rendered.push_str(line);
                    rendered.push('\n');
                }
                rendered.pop();
            }
            rendered
        }
        DecisionEvidence::OracleRuntimeFailure { failure } => render_typed_failure(failure),
        DecisionEvidence::OracleVerdict { evidence } => render_evidence(blobs, evidence)?,
    })
}

pub fn render_typed_failure(failure: &lionclaw_runtime_api::TypedFailure) -> String {
    render_typed_failure_with_configuration(failure, true)
}

fn render_typed_failure_with_configuration(
    failure: &lionclaw_runtime_api::TypedFailure,
    include_configuration: bool,
) -> String {
    let evidence = failure.evidence();
    let mut rendered = format!("{}: {}", failure.category(), evidence.detail);
    if let Some(code) = &evidence.code {
        rendered.push_str("\ncode: ");
        rendered.push_str(code);
    }
    if let Some(reason) = &evidence.stop_reason {
        rendered.push_str("\nstop reason: ");
        rendered.push_str(reason);
    }
    if let Some(code) = evidence.exit_code {
        rendered.push_str("\nexit code: ");
        rendered.push_str(&code.to_string());
    }
    if !evidence.stderr.is_empty() {
        rendered.push_str("\nstderr: ");
        rendered.push_str(&evidence.stderr);
    }
    if !evidence.final_response.is_empty() {
        rendered.push_str("\nfinal response: ");
        rendered.push_str(&evidence.final_response);
    }
    let configuration = &evidence.configuration;
    if include_configuration
        && (configuration.requested_model.is_some()
            || configuration.applied_model.is_some()
            || configuration.requested_mode.is_some()
            || configuration.applied_mode.is_some())
    {
        rendered.push_str(&format!(
            "\nruntime configuration: model {:?} -> {:?}, mode {:?} -> {:?}",
            configuration.requested_model,
            configuration.applied_model,
            configuration.requested_mode,
            configuration.applied_mode,
        ));
    }
    rendered
}

fn typed_failure_json(
    failure: &lionclaw_runtime_api::TypedFailure,
    include_configuration: bool,
) -> serde_json::Value {
    let mut value = serde_json::to_value(failure).expect("typed failure serialization cannot fail");
    if !include_configuration {
        value
            .get_mut("evidence")
            .and_then(serde_json::Value::as_object_mut)
            .expect("typed failure evidence is an object")
            .remove("configuration");
    }
    value
}

pub fn role_attempt_receipt_json(
    blobs: &BlobStore,
    state: &MissionState,
    receipt: &RoleAttemptReceipt,
) -> serde_json::Value {
    let include_nested_configuration = receipt.effective_runtime_configuration().is_none();
    let turn = match &receipt.turn {
        Some(RoleTurnObservation::Completed {
            final_response,
            runtime_configuration: _,
        }) => serde_json::json!({
            "outcome": "completed",
            "final_response": {
                "payload": final_response,
                "content": render_payload(blobs, final_response),
            },
        }),
        Some(RoleTurnObservation::Failed { failure }) => {
            let repeated_by_disposition = matches!(
                &receipt.disposition,
                RoleAttemptDisposition::Failed { failure: settled } if settled == failure
            );
            serde_json::json!({
                "outcome": "failed",
                "failure": (!repeated_by_disposition).then(|| {
                    typed_failure_json(failure, include_nested_configuration)
                }),
            })
        }
        None => serde_json::Value::Null,
    };
    let handoff = match &receipt.handoff {
        Some(RoleHandoffObservation::Accepted { report }) => serde_json::json!({
            "outcome": "accepted",
            "payload": report,
            "content": render_payload(blobs, report),
        }),
        Some(RoleHandoffObservation::Rejected { failure }) => {
            let repeated_by_disposition = matches!(
                &receipt.disposition,
                RoleAttemptDisposition::Failed { failure: settled } if settled == failure
            );
            serde_json::json!({
                "outcome": "rejected",
                "failure": (!repeated_by_disposition).then(|| {
                    typed_failure_json(failure, include_nested_configuration)
                }),
            })
        }
        None => serde_json::Value::Null,
    };
    let disposition = match &receipt.disposition {
        RoleAttemptDisposition::Failed { failure } => serde_json::json!({
            "outcome": "failed",
            "failure": typed_failure_json(failure, include_nested_configuration),
        }),
        disposition => serde_json::to_value(disposition)
            .expect("role attempt disposition serialization cannot fail"),
    };
    let authority = state.role_attempt_authority(receipt);
    serde_json::json!({
        "effect_id": receipt.effect_id.as_str(),
        "authority": authority.evidence_use.slug(),
        "generation": authority.generation.slug(),
        "source": receipt.source,
        "effective_runtime_configuration": receipt.effective_runtime_configuration(),
        "turn": turn,
        "handoff": handoff,
        "disposition": disposition,
    })
}

pub fn role_attempt_reference_json(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
) -> serde_json::Value {
    resolved_role_attempt_reference_json(
        blobs,
        state,
        effect_id,
        state.role_attempt_receipts.get(effect_id),
    )
}

pub fn resolved_role_attempt_reference_json(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
    receipt: Option<&RoleAttemptReceipt>,
) -> serde_json::Value {
    receipt.map_or_else(
        || {
            serde_json::json!({
                "effect_id": effect_id.as_str(),
                "authority": "unavailable",
                "generation": "unavailable",
                "source": null,
                "effective_runtime_configuration": null,
                "turn": null,
                "handoff": null,
                "disposition": null,
            })
        },
        |receipt| role_attempt_receipt_json(blobs, state, receipt),
    )
}

pub fn render_role_attempt_receipt(
    blobs: &BlobStore,
    state: &MissionState,
    receipt: &RoleAttemptReceipt,
) -> String {
    let source = match &receipt.source {
        RoleEffectSource::Task {
            request,
            plan_revision,
            authorized_targets,
        } => format!(
            "task conversation={} namespace={:?} task={} attempt={} generation={} role={} \
             output={:?} runtime={} prompt={} base={} plan_revision={plan_revision} targets={}",
            request.conversation_id,
            request.namespace,
            request.task_id,
            request.attempt_no,
            request.assignment_epoch,
            request.role,
            request.output,
            request.runtime,
            request.prompt_hash,
            request.base_sha,
            authorized_targets
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        RoleEffectSource::TerminalReview {
            attempt_no,
            role,
            judged_sha,
        } => format!("terminal_review attempt={attempt_no} role={role} judged_sha={judged_sha}"),
    };
    let authority = state.role_attempt_authority(receipt);
    let mut rendered = format!(
        "effect: {}\nauthority: {}\ngeneration: {}\nsource: {source}",
        receipt.effect_id,
        authority.evidence_use.slug(),
        authority.generation.slug(),
    );
    if let Some(turn) = &receipt.turn {
        match turn {
            RoleTurnObservation::Completed {
                final_response,
                runtime_configuration: _,
            } => {
                rendered.push_str("\nturn: completed");
                rendered.push_str("\nfinal response:\n");
                rendered.push_str(&render_payload(blobs, final_response));
            }
            RoleTurnObservation::Failed { failure } => {
                rendered.push_str("\nturn: failed");
                let repeated_by_disposition = matches!(
                    &receipt.disposition,
                    RoleAttemptDisposition::Failed { failure: settled } if settled == failure
                );
                if !repeated_by_disposition {
                    rendered.push('\n');
                    rendered.push_str(&render_typed_failure_with_configuration(
                        failure,
                        receipt.effective_runtime_configuration().is_none(),
                    ));
                }
            }
        }
    }
    if let Some(handoff) = &receipt.handoff {
        match handoff {
            RoleHandoffObservation::Accepted { report } => {
                rendered.push_str("\nhandoff: accepted\n");
                rendered.push_str(&render_payload(blobs, report));
            }
            RoleHandoffObservation::Rejected { failure } => {
                rendered.push_str("\nhandoff: rejected");
                let repeated_by_disposition = matches!(
                    &receipt.disposition,
                    RoleAttemptDisposition::Failed { failure: settled } if settled == failure
                );
                if !repeated_by_disposition {
                    rendered.push('\n');
                    rendered.push_str(&render_typed_failure_with_configuration(
                        failure,
                        receipt.effective_runtime_configuration().is_none(),
                    ));
                }
            }
        }
    }
    if let Some(configuration) = receipt.effective_runtime_configuration() {
        rendered.push('\n');
        rendered.push_str("effective ");
        rendered.push_str(&render_runtime_configuration(configuration));
    }
    match &receipt.disposition {
        RoleAttemptDisposition::Active => rendered.push_str("\ndisposition: active"),
        RoleAttemptDisposition::Retired => rendered.push_str("\ndisposition: retired"),
        RoleAttemptDisposition::Succeeded { handoff, artifact } => {
            rendered.push_str("\ndisposition: succeeded");
            if let Some(handoff) = handoff {
                rendered.push_str("\nsettled handoff: ");
                match handoff {
                    crate::model::SettledHandoff::Work { request_attention } => {
                        rendered.push_str(&format!("work request_attention={request_attention}"));
                    }
                    crate::model::SettledHandoff::Validate {
                        items,
                        passed,
                        request_attention,
                    } => {
                        rendered.push_str(&format!(
                            "validate passed={passed} request_attention={request_attention}"
                        ));
                        for item in items {
                            rendered.push_str(&format!(
                                "\nvalidation item {}: {}",
                                item.item_id,
                                if item.passed { "pass" } else { "fail" }
                            ));
                        }
                    }
                    crate::model::SettledHandoff::Review { passed, gaps } => {
                        rendered.push_str(&format!("review passed={passed} gaps={}", gaps.len()));
                        for gap in gaps {
                            rendered.push_str(&format!(
                                "\ngap severity={} id={} requirement={}",
                                gap.severity.slug(),
                                gap.id.as_deref().unwrap_or("none"),
                                gap.requirement,
                            ));
                        }
                    }
                    crate::model::SettledHandoff::Plan {
                        proposal,
                        request_attention,
                    } => {
                        rendered.push_str(&format!(
                            "plan proposal={} request_attention={request_attention}",
                            proposal.as_ref().map_or_else(
                                || "none".into(),
                                |proposal| format!("base_revision={}", proposal.base_revision)
                            )
                        ));
                    }
                }
            }
            if let Some(artifact) = artifact {
                rendered.push_str(&format!(
                    "\nartifact: base={} head={}",
                    artifact.base_sha, artifact.head_sha
                ));
            }
        }
        RoleAttemptDisposition::Failed { failure } => {
            rendered.push_str("\ndisposition: failed\n");
            rendered.push_str(&render_typed_failure_with_configuration(
                failure,
                receipt.effective_runtime_configuration().is_none(),
            ));
        }
    }
    rendered
}

pub fn render_role_attempt_reference(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
) -> String {
    render_resolved_role_attempt_reference(
        blobs,
        state,
        effect_id,
        state.role_attempt_receipts.get(effect_id),
    )
}

pub fn render_resolved_role_attempt_reference(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
    receipt: Option<&RoleAttemptReceipt>,
) -> String {
    receipt.map_or_else(
        || {
            format!(
                "effect: {effect_id}\nauthority: unavailable\ngeneration: unavailable\nsource: unavailable\neffective runtime configuration: unavailable\nturn: unavailable\nhandoff: unavailable\ndisposition: unavailable"
            )
        },
        |receipt| render_role_attempt_receipt(blobs, state, receipt),
    )
}

fn render_runtime_configuration(
    configuration: &crate::model::RuntimeConfigurationEvidence,
) -> String {
    format!(
        "runtime configuration: model {} -> {} ({:?}), mode {} -> {} ({:?})",
        configuration.requested_model.as_deref().unwrap_or("none"),
        configuration.applied_model.as_deref().unwrap_or("none"),
        configuration.model_confirmation,
        configuration.requested_mode.as_deref().unwrap_or("none"),
        configuration.applied_mode.as_deref().unwrap_or("none"),
        configuration.mode_confirmation,
    )
}

fn render_payload(blobs: &BlobStore, payload: &PayloadRef) -> String {
    match blobs.resolve(payload) {
        Ok(text) => excerpt(&text),
        Err(error) => {
            let cause = if error
                .chain()
                .filter_map(|source| source.downcast_ref::<std::io::Error>())
                .any(|error| error.kind() == std::io::ErrorKind::NotFound)
            {
                "source_missing"
            } else if error
                .chain()
                .filter_map(|source| source.downcast_ref::<std::io::Error>())
                .any(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
            {
                "source_unreadable"
            } else {
                "invalid_content"
            };
            match payload {
                PayloadRef::Blob(blob) => format!(
                    "[unavailable payload: algo={} digest={} cause={cause}]",
                    blob.algo, blob.hex
                ),
                PayloadRef::Inline { .. } => {
                    format!("[unavailable inline payload: cause={cause}]")
                }
            }
        }
    }
}

pub fn excerpt(text: &str) -> String {
    if text.len() <= HALF_EXCERPT_BYTES * 2 {
        return text.to_string();
    }
    let mut first_end = HALF_EXCERPT_BYTES;
    while !text.is_char_boundary(first_end) {
        first_end -= 1;
    }
    let mut last_start = text.len() - HALF_EXCERPT_BYTES;
    while !text.is_char_boundary(last_start) {
        last_start += 1;
    }
    format!(
        "{}\n... {} bytes omitted ...\n{}",
        &text[..first_end],
        last_start - first_end,
        &text[last_start..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excerpt_keeps_both_ends_and_utf8_boundaries() {
        let text = format!("start-{}-end", "é".repeat(5000));
        let rendered = excerpt(&text);
        assert!(rendered.starts_with("start-"));
        assert!(rendered.ends_with("-end"));
        assert!(rendered.contains("bytes omitted"));
    }

    #[test]
    fn unavailable_blob_evidence_renders_bounded_typed_identity() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::new(dir.path().to_path_buf());
        let blob = blobs.put(b"authoritative bytes").unwrap();
        let payload = PayloadRef::Blob(blob.clone());
        let path = dir
            .path()
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);

        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            render_payload(&blobs, &payload),
            format!(
                "[unavailable payload: algo=sha256 digest={} cause=source_missing]",
                blob.hex
            )
        );

        std::fs::write(&path, b"corrupted evidence!").unwrap();
        assert_eq!(
            render_payload(&blobs, &payload),
            format!(
                "[unavailable payload: algo=sha256 digest={} cause=invalid_content]",
                blob.hex
            )
        );
    }

    #[test]
    fn role_report_views_preserve_provenance_when_content_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::new(dir.path().to_path_buf());
        let blob = blobs.put(b"review evidence").unwrap();
        let path = dir
            .path()
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        std::fs::remove_file(path).unwrap();
        let receipt = RoleAttemptReceipt {
            effect_id: crate::model::EffectId::parse("1".repeat(64)).unwrap(),
            source: RoleEffectSource::TerminalReview {
                attempt_no: 2,
                role: crate::model::RoleName::new("reviewer").unwrap(),
                judged_sha: "candidate".into(),
            },
            runtime_configuration: None,
            turn: Some(RoleTurnObservation::Completed {
                final_response: PayloadRef::inline("review complete"),
                runtime_configuration: Default::default(),
            }),
            handoff: Some(RoleHandoffObservation::Accepted {
                report: PayloadRef::Blob(blob.clone()),
            }),
            disposition: RoleAttemptDisposition::Succeeded {
                handoff: Some(crate::model::SettledHandoff::Review {
                    passed: true,
                    gaps: Vec::new(),
                }),
                artifact: None,
            },
        };
        let mission_id = crate::model::MissionId::from_digest_prefix("evidence-render");
        let mut state = crate::model::fold([crate::model::EventEnvelope {
            mission_id: mission_id.clone(),
            sequence_no: 1,
            recorded_at_ms: 0,
            stamps: Default::default(),
            event: crate::model::MissionEvent::MissionCreated {
                objective: "render evidence".into(),
                mission_type: crate::model::MissionTypeRef {
                    name: "test".into(),
                    digest: "test".into(),
                },
                runtime: "mock".into(),
                image_id: "test".into(),
                workspace_dir: "/tmp/test".into(),
                base_sha: "candidate".into(),
                config: Default::default(),
            },
        }])
        .unwrap();
        state
            .role_attempt_receipts
            .insert(receipt.effect_id.clone(), receipt.clone());

        let json = role_attempt_receipt_json(&blobs, &state, &receipt);
        assert_eq!(json["effect_id"], "1".repeat(64));
        assert_eq!(json["authority"], "historical");
        assert_eq!(json["generation"], "current");
        assert_eq!(json["source"]["kind"], "terminal_review");
        assert_eq!(json["source"]["attempt_no"], 2);
        assert_eq!(json["source"]["judged_sha"], "candidate");
        assert_eq!(json["turn"]["final_response"]["content"], "review complete");
        assert_eq!(json["handoff"]["payload"]["kind"], "blob");
        assert_eq!(json["handoff"]["payload"]["hex"], blob.hex);
        assert_eq!(
            json["handoff"]["content"],
            format!(
                "[unavailable payload: algo=sha256 digest={} cause=source_missing]",
                blob.hex
            )
        );
        assert_eq!(json["disposition"]["outcome"], "succeeded");

        let human = render_role_attempt_receipt(&blobs, &state, &receipt);
        assert!(human.contains("authority: historical"));
        assert!(human.contains("generation: current"));
        assert!(human.contains("terminal_review attempt=2 role=reviewer"));
        assert!(human.contains("judged_sha=candidate"));
        assert!(human.contains(&blob.hex));
        assert!(human.contains("cause=source_missing"));
        assert!(human.contains("disposition: succeeded"));

        let feedback = FailureFeedback {
            summary: "review failed".into(),
            evidence: DecisionEvidence::RoleAttempts {
                effect_ids: vec![receipt.effect_id.clone()],
            },
            justification: "repair the reviewed tree".into(),
        };
        let rendered = render_feedback(&blobs, &state, &feedback).unwrap();
        assert!(rendered.contains("Role attempt:"));
        assert!(rendered.contains("review evidence") || rendered.contains(&blob.hex));
        assert!(rendered.contains("repair the reviewed tree"));
    }

    #[test]
    fn typed_failure_human_view_keeps_response_and_runtime_configuration() {
        let failure = lionclaw_runtime_api::TypedFailure::PermanentRuntime {
            evidence: Box::new(lionclaw_runtime_api::TypedFailureEvidence {
                code: Some("adapter.setup".into()),
                detail: "runtime refused setup".into(),
                stop_reason: Some("configuration".into()),
                exit_code: Some(78),
                stderr: "setup diagnostic".into(),
                final_response: "partial response".into(),
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: Some("requested".into()),
                    applied_model: Some("applied".into()),
                    requested_mode: Some("review".into()),
                    applied_mode: Some("review".into()),
                    ..Default::default()
                },
            }),
        };

        let rendered = render_typed_failure(&failure);
        assert!(rendered.contains("permanent_runtime: runtime refused setup"));
        assert!(rendered.contains("code: adapter.setup"));
        assert!(rendered.contains("stop reason: configuration"));
        assert!(rendered.contains("exit code: 78"));
        assert!(rendered.contains("stderr: setup diagnostic"));
        assert!(rendered.contains("final response: partial response"));
        assert!(rendered.contains("model Some(\"requested\") -> Some(\"applied\")"));
    }

    #[test]
    fn receipt_projects_completed_turn_configuration_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::new(dir.path().join("blobs"));
        let mission_id = crate::model::MissionId::from_digest_prefix("receipt-config");
        let mut state = crate::model::fold([crate::model::EventEnvelope {
            mission_id: mission_id.clone(),
            sequence_no: 1,
            recorded_at_ms: 0,
            stamps: Default::default(),
            event: crate::model::MissionEvent::MissionCreated {
                objective: "render one configuration".into(),
                mission_type: crate::model::MissionTypeRef {
                    name: "test".into(),
                    digest: "test".into(),
                },
                runtime: "mock".into(),
                image_id: "test".into(),
                workspace_dir: "/tmp/test".into(),
                base_sha: "candidate".into(),
                config: Default::default(),
            },
        }])
        .unwrap();
        let configuration = crate::model::RuntimeConfigurationEvidence {
            requested_model: Some("requested".into()),
            applied_model: Some("applied".into()),
            requested_mode: Some("review".into()),
            applied_mode: Some("review".into()),
            ..Default::default()
        };
        let mut failure =
            lionclaw_runtime_api::TypedFailure::permanent("handoff.capture", "capture failed");
        failure.evidence_mut().configuration = configuration.clone();
        let effect_id = crate::model::EffectId::parse("7".repeat(64)).unwrap();
        let receipt = RoleAttemptReceipt {
            effect_id: effect_id.clone(),
            source: RoleEffectSource::TerminalReview {
                attempt_no: 1,
                role: crate::model::RoleName::new("reviewer").unwrap(),
                judged_sha: "candidate".into(),
            },
            runtime_configuration: Some(configuration),
            turn: Some(RoleTurnObservation::Completed {
                final_response: PayloadRef::inline("review complete"),
                runtime_configuration: Default::default(),
            }),
            handoff: None,
            disposition: RoleAttemptDisposition::Failed { failure },
        };
        state
            .role_attempt_receipts
            .insert(effect_id, receipt.clone());

        let json = role_attempt_receipt_json(&blobs, &state, &receipt);
        assert_eq!(
            json["effective_runtime_configuration"]["applied_model"],
            "applied"
        );
        assert!(!json["disposition"]["failure"]["evidence"]
            .as_object()
            .unwrap()
            .contains_key("configuration"));
        let serialized = serde_json::to_string(&json).unwrap();
        assert_eq!(serialized.matches("\"applied_model\"").count(), 1);

        let human = render_role_attempt_receipt(&blobs, &state, &receipt);
        assert_eq!(human.matches("runtime configuration").count(), 1);
        assert!(human.contains("effective runtime configuration"));
        assert!(human.contains("disposition: failed"));
        assert!(human.contains("handoff.capture"));
    }
}
