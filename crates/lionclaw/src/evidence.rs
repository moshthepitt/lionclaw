//! One rendering path for authoritative failure evidence. Events retain full
//! payloads; prompts and CLI views receive deterministic labelled excerpts.

use anyhow::Result;

use crate::model::{
    AuthoritativeVerdict, ChildMissionOutput, ChildMissionReceipt, DecisionEvidence, EffectId,
    FailureFeedback, MissionState, PayloadRef, RoleAttemptDisposition, RoleAttemptReceipt,
    RoleEffectSource, RuntimeUsage,
};
use crate::store::BlobStore;

const HALF_EXCERPT_BYTES: usize = 4096;

pub fn render_authoritative_receipt(
    blobs: &BlobStore,
    effect_id: &EffectId,
    verdict: &AuthoritativeVerdict,
) -> String {
    let (stdout, stderr) = verdict.evidence();
    format!(
        "effect: {effect_id}\nsource: oracle {}\nspec: {}\nassertions: {}\nattempt: {}\njudged artifact: {}\nenvironment: {}\nexit code: {}\nsignal: {}\nstdout:\n{}\nstderr:\n{}",
        verdict.oracle(),
        verdict.spec_digest(),
        verdict
            .assertion_ids()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        verdict.attempt_no(),
        verdict.judged_sha(),
        verdict.environment_digest(),
        verdict.exit_code(),
        verdict
            .exit_signal()
            .map_or_else(|| "none".to_string(), |signal| signal.to_string()),
        render_payload(blobs, stdout),
        render_payload(blobs, stderr),
    )
}

pub fn authoritative_receipt_json(
    blobs: &BlobStore,
    effect_id: &EffectId,
    verdict: &AuthoritativeVerdict,
) -> serde_json::Value {
    let (stdout, stderr) = verdict.evidence();
    serde_json::json!({
        "effect_id": effect_id,
        "oracle": verdict.oracle(),
        "spec_digest": verdict.spec_digest(),
        "assertion_ids": verdict.assertion_ids(),
        "attempt_no": verdict.attempt_no(),
        "judged_sha": verdict.judged_sha(),
        "environment_digest": verdict.environment_digest(),
        "exit_code": verdict.exit_code(),
        "exit_signal": verdict.exit_signal(),
        "stdout": render_payload(blobs, stdout),
        "stderr": render_payload(blobs, stderr),
        "prepared_inputs": verdict.prepared_inputs(),
    })
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

pub fn render_feedbacks(
    blobs: &BlobStore,
    state: &MissionState,
    feedback: &[FailureFeedback],
) -> Result<String> {
    let mut rendered = String::new();
    for item in feedback {
        if !rendered.is_empty() {
            rendered.push_str("\n\n");
        }
        rendered.push_str(&render_feedback(blobs, state, item)?);
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
        DecisionEvidence::AuthoritativeReceipts { effect_ids } => serde_json::json!({
            "kind": "authoritative_receipts",
            "receipts": effect_ids
                .iter()
                .map(|effect_id| state.authoritative_receipts.get(effect_id).map_or_else(
                    || serde_json::json!({
                        "effect_id": effect_id,
                        "authority": "unavailable",
                    }),
                    |verdict| authoritative_receipt_json(blobs, effect_id, verdict),
                ))
                .collect::<Vec<_>>(),
        }),
        DecisionEvidence::OracleRuntimeFailure { failure } => serde_json::json!({
            "kind": "oracle_runtime_failure",
            "failure": failure,
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
        DecisionEvidence::AuthoritativeReceipts { effect_ids } => {
            let mut rendered = String::new();
            for effect_id in effect_ids {
                if !rendered.is_empty() {
                    rendered.push('\n');
                }
                rendered.push_str("Authoritative receipt:\n");
                rendered.push_str(&state.authoritative_receipts.get(effect_id).map_or_else(
                    || format!("effect: {effect_id}\nauthority: unavailable"),
                    |verdict| render_authoritative_receipt(blobs, effect_id, verdict),
                ));
            }
            rendered
        }
        DecisionEvidence::OracleRuntimeFailure { failure } => render_typed_failure(failure),
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
    if evidence.runtime_usage.is_reported() {
        rendered.push('\n');
        rendered.push_str(&render_runtime_usage(&evidence.runtime_usage));
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
    let turn = match &receipt.final_response {
        Some(final_response) => serde_json::json!({
            "outcome": "completed",
            "final_response": {
                "payload": final_response,
                "content": render_payload(blobs, final_response),
            },
        }),
        None => serde_json::Value::Null,
    };
    let handoff = match &receipt.handoff {
        Some(handoff) => serde_json::json!({
            "outcome": "accepted",
            "payload": handoff.report(),
            "content": render_payload(blobs, handoff.report()),
        }),
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
        "runtime_usage": receipt.runtime_usage,
        "prepared_inputs": &receipt.prepared_inputs,
        "turn": turn,
        "handoff": handoff,
        "disposition": disposition,
    })
}

pub fn child_mission_receipt_json(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
    receipt: &ChildMissionReceipt,
) -> serde_json::Value {
    let report_content = match &receipt.output {
        Some(ChildMissionOutput::Report { report, .. }) => {
            serde_json::Value::String(render_payload(blobs, report))
        }
        Some(ChildMissionOutput::Artifact { .. }) | None => serde_json::Value::Null,
    };
    serde_json::json!({
        "effect_id": effect_id.as_str(),
        "authority": "kernel_folded_child_truth",
        "generation": "child_mission",
        "parent_mission_id": receipt.parent_mission_id.as_str(),
        "parent_effect_id": receipt.parent_effect_id.as_str(),
        "child_mission_id": receipt.child_mission_id.as_str(),
        "request_digest": receipt.request_digest,
        "input_artifact": receipt.input_artifact,
        "terminal": receipt.terminal,
        "output": receipt.output,
        "report_content": report_content,
        "proof_summary": receipt.proof,
        "failure": receipt.failure,
        "cleaned": state.cleaned_child_missions.contains(effect_id),
    })
}

pub fn render_child_mission_receipt(
    blobs: &BlobStore,
    state: &MissionState,
    effect_id: &EffectId,
    receipt: &ChildMissionReceipt,
) -> String {
    let output = match &receipt.output {
        Some(ChildMissionOutput::Artifact { artifact }) => {
            format!("artifact {} -> {}", artifact.base_sha, artifact.head_sha)
        }
        Some(ChildMissionOutput::Report {
            report,
            report_sha256,
        }) => format!(
            "report sha256={}\nreport:\n{}",
            report_sha256,
            render_payload(blobs, report)
        ),
        None => "none".to_string(),
    };
    let failure = receipt
        .failure
        .as_ref()
        .map_or_else(|| "none".to_string(), render_typed_failure);
    format!(
        "effect: {effect_id}\nsource: child mission {}\nauthority: kernel-folded child truth\nrequest: {}\ninput artifact: {}\nterminal: {}\noutput: {}\nchild proof summary: authoritative={} advisory={}\nfailure: {}\ncleaned: {}",
        receipt.child_mission_id,
        receipt.request_digest,
        receipt.input_artifact,
        receipt.terminal.slug(),
        output,
        receipt.proof.authoritative_receipt_digests.len(),
        receipt.proof.advisory_receipt_digests.len(),
        failure,
        state.cleaned_child_missions.contains(effect_id),
    )
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
                "runtime_usage": null,
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
        RoleEffectSource::Turn {
            request,
            plan_revision,
        } => format!(
            "turn role_instance={} team_revision={} task={} attempt={} generation={} \
             prompt={} base={} environment={} plan_revision={plan_revision} targets={}",
            request.role_instance,
            request.team_revision,
            request
                .task_id
                .as_ref()
                .map_or("none", |task| task.as_str()),
            request.attempt_no,
            request.assignment_epoch,
            request.prompt_hash,
            request.base_sha,
            request.environment_digest,
            request
                .assertion_ids
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
    };
    let authority = state.role_attempt_authority(receipt);
    let mut rendered = format!(
        "effect: {}\nauthority: {}\ngeneration: {}\nsource: {source}",
        receipt.effect_id,
        authority.evidence_use.slug(),
        authority.generation.slug(),
    );
    if let Some(final_response) = &receipt.final_response {
        rendered.push_str("\nturn: completed");
        rendered.push_str("\nfinal response:\n");
        rendered.push_str(&render_payload(blobs, final_response));
    }
    if let Some(handoff) = &receipt.handoff {
        rendered.push_str("\nhandoff: accepted\n");
        rendered.push_str(&render_payload(blobs, handoff.report()));
    }
    if let Some(configuration) = receipt.effective_runtime_configuration() {
        rendered.push('\n');
        rendered.push_str("effective ");
        rendered.push_str(&render_runtime_configuration(configuration));
    }
    if !receipt.prepared_inputs.is_empty() {
        rendered.push_str("\nprepared inputs: ");
        rendered.push_str(
            &receipt
                .prepared_inputs
                .iter()
                .map(|input| format!("{}@{}", input.name, crate::model::short_hex(&input.digest)))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    rendered.push('\n');
    rendered.push_str(&render_runtime_usage(&receipt.runtime_usage));
    match &receipt.disposition {
        RoleAttemptDisposition::Active => rendered.push_str("\ndisposition: active"),
        RoleAttemptDisposition::Retired => rendered.push_str("\ndisposition: retired"),
        RoleAttemptDisposition::Succeeded { handoff, artifact } => {
            rendered.push_str("\ndisposition: succeeded");
            if let Some(handoff) = handoff {
                rendered.push_str("\nsettled handoff: ");
                match &**handoff {
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
                                |proposal| proposal.plan.as_ref().map_or_else(
                                    || "team-only".to_string(),
                                    |plan| format!("base_revision={}", plan.base_revision)
                                )
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

fn render_runtime_usage(runtime_usage: &RuntimeUsage) -> String {
    let RuntimeUsage::Reported { usage } = runtime_usage else {
        return "runtime usage: not reported".to_string();
    };
    let mut fields = Vec::new();
    if let Some(tokens) = usage.input_tokens {
        fields.push(format!("input_tokens={tokens}"));
    }
    if let Some(tokens) = usage.output_tokens {
        fields.push(format!("output_tokens={tokens}"));
    }
    if let Some(tokens) = usage.total_tokens {
        fields.push(format!("total_tokens={tokens}"));
    }
    if let Some(tokens) = usage.reasoning_tokens {
        fields.push(format!("reasoning_tokens={tokens}"));
    }
    if let Some(tokens) = usage.cached_input_tokens {
        fields.push(format!("cached_input_tokens={tokens}"));
    }
    if let Some(tokens) = usage.context_used_tokens {
        fields.push(format!("context_used_tokens={tokens}"));
    }
    if let Some(tokens) = usage.context_window_tokens {
        fields.push(format!("context_window_tokens={tokens}"));
    }
    if let Some(cost) = &usage.cost {
        fields.push(format!(
            "cost={} {} ({})",
            cost.amount,
            cost.currency,
            cost.scope.slug()
        ));
    }
    if fields.is_empty() {
        "runtime usage: not reported".to_string()
    } else {
        format!("runtime usage: {}", fields.join(", "))
    }
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
