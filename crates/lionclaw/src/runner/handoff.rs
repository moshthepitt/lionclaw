//! Strict handoff parsing. The agent writes `/mission/handoff/handoff.json`;
//! the engine never infers success from prose — a missing or malformed
//! handoff is a failed attempt. Schemas mirror Zenith's `WorkHandoff` /
//! `ValidateHandoff` (Apache-2.0, Intelligent Internet).

use std::path::Path;

use crate::model::{Handoff, OutputSemantics, RunErrorKind};
use crate::ports::RoleRunFailure;

pub const WORK_HANDOFF_SCHEMA: &str = "lionclaw.mission.work-handoff.v1";
pub const VALIDATE_HANDOFF_SCHEMA: &str = "lionclaw.mission.validate-handoff.v1";
pub const PLAN_HANDOFF_SCHEMA: &str = "lionclaw.mission.plan-handoff.v1";

pub fn expected_schema(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::EmitsVerdict => VALIDATE_HANDOFF_SCHEMA,
        OutputSemantics::ProposesPlan => PLAN_HANDOFF_SCHEMA,
        // A report role hands back a plain work handoff (prose, no proposal).
        OutputSemantics::ProducesReport | OutputSemantics::ProducesArtifact => WORK_HANDOFF_SCHEMA,
    }
}

/// The handoff file is a small control document; cap the read so an agent
/// can't OOM the host by writing a huge file into the rw handoff mount.
const MAX_HANDOFF_BYTES: u64 = 4 * 1024 * 1024;

/// Typed gaps land inline in the event log (only `PayloadRef`s externalize
/// to blobs), so cap them well under the file cap — for EVERY validate
/// handoff, not just the terminal review's (this parse is the one choke
/// point they all cross).
const MAX_GAPS_BYTES: usize = 256 * 1024;

/// Read and validate the handoff file for a finished role run.
pub fn read_handoff(dir: &Path, output: OutputSemantics) -> Result<Handoff, RoleRunFailure> {
    use std::io::Read;
    let path = dir.join("handoff.json");
    // The agent controls this file (the handoff mount is read-write). Reject
    // anything that is not a regular file BEFORE opening it: a symlink would let
    // the host read outside the mount, and a FIFO would block this thread
    // forever on open (never reaping the attempt dir). The agent's turn has
    // already ended, so there is no live race between this stat and the open.
    let meta = std::fs::symlink_metadata(&path).map_err(|err| RoleRunFailure {
        kind: RunErrorKind::HandoffMissing,
        detail: format!("no handoff at '{}': {err}", path.display()),
    })?;
    if !meta.file_type().is_file() {
        return Err(RoleRunFailure {
            kind: RunErrorKind::HandoffInvalid,
            detail: format!("handoff at '{}' is not a regular file", path.display()),
        });
    }
    let file = std::fs::File::open(&path).map_err(|err| RoleRunFailure {
        kind: RunErrorKind::HandoffMissing,
        detail: format!("no handoff at '{}': {err}", path.display()),
    })?;
    // Read one byte past the cap so an over-limit file is detected.
    let mut raw = String::new();
    file.take(MAX_HANDOFF_BYTES + 1)
        .read_to_string(&mut raw)
        .map_err(|err| RoleRunFailure {
            kind: RunErrorKind::HandoffInvalid,
            detail: format!("handoff at '{}' is not valid UTF-8: {err}", path.display()),
        })?;
    if raw.len() as u64 > MAX_HANDOFF_BYTES {
        return Err(RoleRunFailure {
            kind: RunErrorKind::HandoffInvalid,
            detail: format!("handoff exceeds {MAX_HANDOFF_BYTES} bytes"),
        });
    }
    parse_handoff(&raw, output)
}

fn parse_handoff(raw: &str, output: OutputSemantics) -> Result<Handoff, RoleRunFailure> {
    let invalid = |detail: String| RoleRunFailure {
        kind: RunErrorKind::HandoffInvalid,
        detail,
    };
    let mut value: serde_json::Value =
        serde_json::from_str(raw).map_err(|err| invalid(format!("handoff is not JSON: {err}")))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("handoff must be a JSON object".to_string()))?;
    let schema = object
        .remove("schema")
        .and_then(|v| v.as_str().map(str::to_string))
        .ok_or_else(|| invalid("handoff is missing the 'schema' string".to_string()))?;
    let expected = expected_schema(output);
    if schema != expected {
        return Err(invalid(format!(
            "handoff schema '{schema}' does not match this role's contract '{expected}'"
        )));
    }
    let handoff: Handoff = serde_json::from_value(value)
        .map_err(|err| invalid(format!("handoff does not match '{expected}': {err}")))?;
    if let Handoff::Validate { gaps, .. } = &handoff {
        let gaps_bytes = serde_json::to_vec(gaps)
            .map_err(|err| invalid(format!("gaps are not serializable: {err}")))?
            .len();
        if gaps_bytes > MAX_GAPS_BYTES {
            return Err(invalid(format!(
                "typed gaps are {gaps_bytes} bytes (cap {MAX_GAPS_BYTES}): \
                 cite short excerpts as evidence, not full logs"
            )));
        }
    }
    // The schema string and the payload tag must agree with the role's output.
    let tag_ok = matches!(
        (&handoff, output),
        (Handoff::Validate { .. }, OutputSemantics::EmitsVerdict)
            | (Handoff::Plan { .. }, OutputSemantics::ProposesPlan)
            | (
                Handoff::Work { .. },
                OutputSemantics::ProducesReport | OutputSemantics::ProducesArtifact
            )
    );
    if !tag_ok {
        return Err(invalid(format!(
            "handoff type does not match this role's output semantics ({output:?})"
        )));
    }
    Ok(handoff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::PayloadRef;

    #[test]
    fn oversized_handoff_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let big = format!(
            "{{\"schema\":\"lionclaw.mission.work-handoff.v1\",\"type\":\"work\",\"done\":true,\"report\":{{\"kind\":\"inline\",\"text\":\"{}\"}},\"request_attention\":false}}",
            "x".repeat((MAX_HANDOFF_BYTES + 1024) as usize)
        );
        std::fs::write(dir.path().join("handoff.json"), big).expect("write");
        let err = read_handoff(dir.path(), OutputSemantics::ProducesArtifact).expect_err("reject");
        assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
    }

    // A malicious agent could leave a symlink (traverse to a host file) or a
    // FIFO (block the host's open forever) at handoff.json. Both are rejected as
    // non-regular files — the read must never follow or block.
    #[cfg(unix)]
    #[test]
    fn a_non_regular_handoff_is_rejected_not_followed_or_hung() {
        // Symlink to a host file.
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "host bytes").unwrap();
        std::os::unix::fs::symlink(&secret, dir.path().join("handoff.json")).unwrap();
        let err = read_handoff(dir.path(), OutputSemantics::ProducesArtifact).expect_err("reject");
        assert_eq!(err.kind, RunErrorKind::HandoffInvalid);

        // FIFO — a plain open would block here forever; the stat gate must catch
        // it first and return promptly.
        let fifo_dir = tempfile::tempdir().expect("tempdir");
        let fifo = fifo_dir.path().join("handoff.json");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if made {
            let err = read_handoff(fifo_dir.path(), OutputSemantics::ProducesArtifact)
                .expect_err("reject");
            assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
        }
    }

    #[test]
    fn parses_work_handoff() {
        let raw = r#"{"schema":"lionclaw.mission.work-handoff.v1","type":"work",
                      "done":true,"report":{"kind":"inline","text":"done"},
                      "request_attention":false}"#;
        let handoff = parse_handoff(raw, OutputSemantics::ProducesArtifact).expect("parse");
        assert_eq!(
            handoff,
            Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false
            }
        );
    }

    #[test]
    fn parses_validate_handoff_with_items() {
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v1","type":"validate",
                      "done":true,"report":{"kind":"inline","text":"findings"},
                      "items":[{"item_id":"TESTS-PASS","passed":false}],
                      "passed":false,"request_attention":false}"#;
        let handoff = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect("parse");
        let Handoff::Validate { items, passed, .. } = handoff else {
            panic!("expected validate handoff");
        };
        assert!(!passed);
        assert_eq!(items.len(), 1);
        assert!(!items[0].passed);
    }

    #[test]
    fn parses_validate_handoff_with_gaps_and_nonce() {
        use crate::model::GapSeverity;
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v1","type":"validate",
                      "done":true,"report":{"kind":"inline","text":"map"},
                      "items":[],"passed":false,"nonce":"n-1",
                      "gaps":[{"severity":"blocking",
                               "requirement":"starts up",
                               "expected":"prints usage",
                               "observed":"panics",
                               "evidence":"cargo run -> panic"}],
                      "request_attention":false}"#;
        let handoff = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect("parse");
        let Handoff::Validate { gaps, nonce, .. } = handoff else {
            panic!("expected validate handoff");
        };
        assert_eq!(nonce.as_deref(), Some("n-1"));
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].severity, GapSeverity::Blocking);
    }

    #[test]
    fn oversized_gaps_are_rejected_for_every_validate_handoff() {
        // Regression (QA round 1): typed gaps land inline in the event log,
        // so the cap must hold at this parse — the one choke point every
        // validate handoff crosses — not just on the terminal-review path.
        let evidence = "x".repeat(300 * 1024);
        let raw = format!(
            r#"{{"schema":"lionclaw.mission.validate-handoff.v1","type":"validate",
                "done":true,"report":{{"kind":"inline","text":""}},
                "items":[],"passed":false,
                "gaps":[{{"severity":"blocking","requirement":"r",
                         "expected":"e","observed":"o","evidence":"{evidence}"}}],
                "request_attention":false}}"#
        );
        let err = parse_handoff(&raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
        assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
        assert!(err.detail.contains("cite short excerpts"));
    }

    #[test]
    fn rejects_unknown_gap_severity_and_unknown_gap_fields() {
        // The severity axis and the Gap shape are closed: a typo'd severity or
        // a stray field fails the attempt rather than passing as prose.
        for gap_json in [
            r#"{"severity":"severe","requirement":"r","expected":"e","observed":"o"}"#,
            r#"{"severity":"blocking","requirement":"r","expected":"e","observed":"o","note":"x"}"#,
            r#"{"severity":"blocking","requirement":"r"}"#,
        ] {
            let raw = format!(
                r#"{{"schema":"lionclaw.mission.validate-handoff.v1","type":"validate",
                    "done":true,"report":{{"kind":"inline","text":""}},
                    "items":[],"passed":false,"gaps":[{gap_json}],
                    "request_attention":false}}"#
            );
            let err = parse_handoff(&raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
            assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
        }
    }

    #[test]
    fn rejects_wrong_schema_for_role() {
        // A judge trying to hand back a work handoff is refused.
        let raw = r#"{"schema":"lionclaw.mission.work-handoff.v1","type":"work",
                      "done":true,"report":{"kind":"inline","text":"looks great"},
                      "request_attention":false}"#;
        let err = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
        assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
    }

    #[test]
    fn rejects_missing_schema_and_bad_json() {
        for raw in [r#"{"type":"work","done":true}"#, "not json"] {
            let err = parse_handoff(raw, OutputSemantics::ProducesArtifact).expect_err("refuse");
            assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
        }
    }

    #[test]
    fn rejects_invalid_item_ids() {
        // Item ids run through the AssertionId charset validation.
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v1","type":"validate",
                      "done":true,"report":{"kind":"inline","text":""},
                      "items":[{"item_id":"lowercase-bad","passed":true}],
                      "passed":true,"request_attention":false}"#;
        let err = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
        assert_eq!(err.kind, RunErrorKind::HandoffInvalid);
    }
}
