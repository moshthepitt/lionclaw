//! One rendering path for authoritative failure evidence. Events retain full
//! payloads; prompts and CLI views receive deterministic labelled excerpts.

use anyhow::Result;

use crate::model::{FailureEvidence, FailureFeedback, PayloadRef};
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

pub fn render_feedback(blobs: &BlobStore, feedback: &FailureFeedback) -> Result<String> {
    let mut rendered = feedback.summary.clone();
    if let Some(failure) = &feedback.failure {
        let evidence = failure.evidence();
        rendered.push_str("\nFailure kind: ");
        rendered.push_str(failure.category());
        if let Some(code) = &evidence.code {
            rendered.push_str("\nFailure code: ");
            rendered.push_str(code);
        }
        rendered.push_str("\nFailure detail: ");
        rendered.push_str(&evidence.detail);
    }
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
        rendered.push_str(&render_payload(blobs, details));
    }
    Ok(rendered)
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
}
