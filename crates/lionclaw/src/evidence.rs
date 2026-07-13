//! One rendering path for authoritative failure evidence. Events retain full
//! payloads; prompts and CLI views receive deterministic labelled excerpts.

use anyhow::Result;

use crate::model::{FailureEvidence, FailureFeedback};
use crate::store::BlobStore;

const HALF_EXCERPT_BYTES: usize = 4096;

pub fn render_evidence(blobs: &BlobStore, evidence: &FailureEvidence) -> Result<String> {
    let stdout = excerpt(&blobs.resolve(&evidence.stdout)?);
    let stderr = excerpt(&blobs.resolve(&evidence.stderr)?);
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
        "stdout": excerpt(&blobs.resolve(&evidence.stdout)?),
        "stderr": excerpt(&blobs.resolve(&evidence.stderr)?),
    }))
}

pub fn render_feedback(blobs: &BlobStore, feedback: &FailureFeedback) -> Result<String> {
    let mut rendered = feedback.summary.clone();
    if !feedback.justification.trim().is_empty() {
        rendered.push_str("\nDecision guidance: ");
        rendered.push_str(&feedback.justification);
    }
    if let Some(evidence) = &feedback.evidence {
        rendered.push('\n');
        rendered.push_str(&render_evidence(blobs, evidence)?);
    }
    if let Some(details) = &feedback.details {
        rendered.push_str("\nDetailed report:\n");
        rendered.push_str(&excerpt(&blobs.resolve(details)?));
    }
    Ok(rendered)
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
}
