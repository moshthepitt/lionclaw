//! Strict handoff parsing. The agent writes `/mission/handoff/handoff.json`;
//! the engine never infers success from prose — a missing or malformed
//! handoff is a failed attempt. Schemas mirror Zenith's `WorkHandoff` /
//! `ValidateHandoff` (Apache-2.0, Intelligent Internet).

use std::path::Path;

use crate::model::{Handoff, OutputSemantics, RunErrorKind};
use crate::ports::RoleRunFailure;

pub const WORK_HANDOFF_SCHEMA: &str = "lionclaw.mission.work-handoff.v1";
pub const VALIDATE_HANDOFF_SCHEMA: &str = "lionclaw.mission.validate-handoff.v1";

pub fn expected_schema(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::EmitsVerdict => VALIDATE_HANDOFF_SCHEMA,
        OutputSemantics::Plans | OutputSemantics::ProducesArtifact | OutputSemantics::Egresses => {
            WORK_HANDOFF_SCHEMA
        }
    }
}

/// The handoff file is a small control document; cap the read so an agent
/// can't OOM the host by writing a huge file into the rw handoff mount.
const MAX_HANDOFF_BYTES: u64 = 4 * 1024 * 1024;

/// Read and validate the handoff file for a finished role run.
pub fn read_handoff(dir: &Path, output: OutputSemantics) -> Result<Handoff, RoleRunFailure> {
    use std::io::Read;
    let path = dir.join("handoff.json");
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
    // The schema string and the payload tag must agree.
    let tag_ok = match (&handoff, output) {
        (Handoff::Validate { .. }, OutputSemantics::EmitsVerdict) => true,
        (Handoff::Work { .. }, OutputSemantics::EmitsVerdict) => false,
        (Handoff::Work { .. }, _) => true,
        (Handoff::Validate { .. }, _) => false,
    };
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
