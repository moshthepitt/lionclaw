//! Strict handoff parsing. The agent writes `/mission/handoff/handoff.json`;
//! the engine never infers required output from prose — a missing required or
//! malformed handoff is a failed attempt. Schemas mirror Zenith's `WorkHandoff` /
//! `ValidateHandoff` (Apache-2.0, Intelligent Internet).

use std::path::Path;

use crate::model::{
    Gap, Handoff, OutputSemantics, PayloadRef, PlanProposal, ValidationItem, MAX_ROLE_REPORT_BYTES,
};
use lionclaw_runtime_api::TypedFailure;
use serde::{Deserialize, Serialize};

pub const WORK_HANDOFF_SCHEMA: &str = "lionclaw.mission.work-handoff.v2";
pub const VALIDATE_HANDOFF_SCHEMA: &str = "lionclaw.mission.validate-handoff.v2";
pub const REVIEW_HANDOFF_SCHEMA: &str = "lionclaw.mission.review-handoff.v2";
pub const PLAN_HANDOFF_SCHEMA: &str = "lionclaw.mission.plan-handoff.v2";

/// Agent-controlled wire data. Reports are strings at this boundary; only the
/// engine can mint durable content-addressed payload references.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum AgentHandoff {
    Work {
        done: bool,
        report: String,
        request_attention: bool,
    },
    Validate {
        done: bool,
        report: String,
        items: Vec<ValidationItem>,
        passed: bool,
        request_attention: bool,
    },
    Review {
        done: bool,
        report: String,
        passed: bool,
        gaps: Vec<Gap>,
        nonce: String,
    },
    Plan {
        done: bool,
        report: String,
        #[serde(default)]
        proposal: Option<PlanProposal>,
        request_attention: bool,
    },
}

impl From<AgentHandoff> for Handoff {
    fn from(handoff: AgentHandoff) -> Self {
        match handoff {
            AgentHandoff::Work {
                done,
                report,
                request_attention,
            } => Self::Work {
                done,
                report: PayloadRef::inline(report),
                request_attention,
            },
            AgentHandoff::Validate {
                done,
                report,
                items,
                passed,
                request_attention,
            } => Self::Validate {
                done,
                report: PayloadRef::inline(report),
                items,
                passed,
                request_attention,
            },
            AgentHandoff::Review {
                done,
                report,
                passed,
                gaps,
                nonce,
            } => Self::Review {
                done,
                report: PayloadRef::inline(report),
                passed,
                gaps,
                nonce,
            },
            AgentHandoff::Plan {
                done,
                report,
                proposal,
                request_attention,
            } => Self::Plan {
                done,
                report: PayloadRef::inline(report),
                proposal,
                request_attention,
            },
        }
    }
}

pub fn expected_schema(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::EmitsVerdict => VALIDATE_HANDOFF_SCHEMA,
        OutputSemantics::EmitsGapVerdict => REVIEW_HANDOFF_SCHEMA,
        OutputSemantics::ProposesPlan => PLAN_HANDOFF_SCHEMA,
        // A report role hands back a plain work handoff (prose, no proposal).
        OutputSemantics::ProducesReport | OutputSemantics::ProducesArtifact => WORK_HANDOFF_SCHEMA,
    }
}

/// The handoff file is a small control document; cap the read so an agent
/// can't OOM the host by writing a huge file into the rw handoff mount.
const MAX_HANDOFF_BYTES: u64 = 4 * 1024 * 1024;

/// Typed gaps land inline in the event log (only `PayloadRef`s externalize
/// to blobs), so cap them well under the file cap.
const MAX_GAPS_BYTES: usize = 256 * 1024;

/// Read and validate the handoff file for a finished role run.
pub fn read_handoff(dir: &Path, output: OutputSemantics) -> Result<Handoff, TypedFailure> {
    use std::io::Read;
    let path = dir.join("handoff.json");
    // The agent controls this file (the handoff mount is read-write). Reject
    // anything that is not a regular file BEFORE opening it: a symlink would let
    // the host read outside the mount, and a FIFO would block this thread
    // forever on open (never reaping the attempt dir). The agent's turn has
    // already ended, so there is no live race between this stat and the open.
    let invalid = |code: &str, detail: String| TypedFailure::invalid(code, detail);
    let meta = std::fs::symlink_metadata(&path).map_err(|err| {
        invalid(
            "handoff.missing",
            format!("no handoff at '{}': {err}", path.display()),
        )
    })?;
    if !meta.file_type().is_file() {
        return Err(invalid(
            "handoff.file_type",
            format!("handoff at '{}' is not a regular file", path.display()),
        ));
    }
    let file = std::fs::File::open(&path).map_err(|err| {
        invalid(
            "handoff.missing",
            format!("no handoff at '{}': {err}", path.display()),
        )
    })?;
    // Read one byte past the cap so an over-limit file is detected.
    let mut raw = String::new();
    file.take(MAX_HANDOFF_BYTES + 1)
        .read_to_string(&mut raw)
        .map_err(|err| {
            invalid(
                "handoff.utf8",
                format!("handoff at '{}' is not valid UTF-8: {err}", path.display()),
            )
        })?;
    if raw.len() as u64 > MAX_HANDOFF_BYTES {
        return Err(invalid(
            "handoff.too_large",
            format!("handoff exceeds {MAX_HANDOFF_BYTES} bytes"),
        ));
    }
    parse_handoff(&raw, output)
}

/// Read a role handoff under its output-owned presence policy. Optional absence
/// is a successful dialogue checkpoint; every present filesystem object is validated fail-closed by
/// `read_handoff`, including dangling symlinks and non-regular files.
pub fn read_optional_handoff(
    dir: &Path,
    output: OutputSemantics,
) -> Result<Option<Handoff>, TypedFailure> {
    let path = dir.join("handoff.json");
    match std::fs::symlink_metadata(&path) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && !output.requires_handoff() =>
        {
            Ok(None)
        }
        _ => read_handoff(dir, output).map(Some),
    }
}

/// Inspect a handoff left by an interrupted driver. Absence means the role
/// never reached the handoff boundary; any object that does exist is validated
/// exactly like a live completion so malformed evidence is not erased.
pub(crate) fn read_retained_handoff(
    dir: &Path,
    output: OutputSemantics,
) -> Result<Option<Handoff>, TypedFailure> {
    let path = dir.join("handoff.json");
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        _ => read_handoff(dir, output).map(Some),
    }
}

fn parse_handoff(raw: &str, output: OutputSemantics) -> Result<Handoff, TypedFailure> {
    let invalid = |detail: String| TypedFailure::invalid("handoff.schema", detail);
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
    let handoff: Handoff = serde_json::from_value::<AgentHandoff>(value)
        .map(Handoff::from)
        .map_err(|err| invalid(format!("handoff does not match '{expected}': {err}")))?;
    validate_handoff(&handoff, output)?;
    Ok(handoff)
}

/// Validate the complete bounded role-output document regardless of whether
/// it came from the production wire parser or another `RoleRunner`.
pub(crate) fn validate_handoff(
    handoff: &Handoff,
    output: OutputSemantics,
) -> Result<(), TypedFailure> {
    let invalid = |detail: String| TypedFailure::invalid("handoff.schema", detail);
    if bounded_serialized_len(handoff, MAX_HANDOFF_BYTES as usize)
        .map_err(|err| invalid(format!("handoff is not serializable: {err}")))?
        .is_none()
    {
        return Err(TypedFailure::invalid(
            "handoff.too_large",
            format!("handoff exceeds {MAX_HANDOFF_BYTES} bytes"),
        ));
    }
    if !handoff.matches_output(output) {
        return Err(invalid(format!(
            "handoff type does not match this role's output semantics ({output:?})"
        )));
    }
    if let Handoff::Review { gaps, .. } = &handoff {
        if bounded_serialized_len(gaps, MAX_GAPS_BYTES)
            .map_err(|err| invalid(format!("gaps are not serializable: {err}")))?
            .is_none()
        {
            return Err(invalid(format!(
                "typed gaps exceed {MAX_GAPS_BYTES} bytes: \
                 cite short excerpts as evidence, not full logs"
            )));
        }
        // A gap is a falsifiable claim: every prose field must say something.
        // (The prompt promises evidence-less claims are rejected; hold it.)
        for gap in gaps {
            for (field, text) in [
                ("requirement", &gap.requirement),
                ("expected", &gap.expected),
                ("observed", &gap.observed),
                ("evidence", &gap.evidence),
            ] {
                if text.trim().is_empty() {
                    return Err(invalid(format!(
                        "gap '{}' has an empty '{field}': every gap must state \
                         its requirement, expected and observed behavior, and evidence",
                        gap.id.as_deref().unwrap_or("<unnamed>")
                    )));
                }
            }
        }
    }
    match handoff.report() {
        PayloadRef::Inline { text: report } if report.len() > MAX_ROLE_REPORT_BYTES => {
            return Err(TypedFailure::invalid(
                "handoff.report_too_large",
                format!(
                    "handoff report is {} bytes; the limit is {MAX_ROLE_REPORT_BYTES}",
                    report.len()
                ),
            ));
        }
        PayloadRef::Blob(_) => {
            return Err(TypedFailure::invalid(
                "handoff.payload_ref",
                "role output must provide inline text; only the engine may mint blob references",
            ));
        }
        PayloadRef::Inline { .. } => {}
    }
    Ok(())
}

struct LimitedWriter {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}

impl std::io::Write for LimitedWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.bytes.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other("serialized size overflow"));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other("serialized value exceeds byte limit"));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Return the exact JSON size when it fits, or `None` immediately after the
/// cap is crossed. This validates alternate-runner values without allocating a
/// second copy of provider-controlled data.
fn bounded_serialized_len<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<Option<usize>, serde_json::Error> {
    let mut writer = LimitedWriter {
        bytes: 0,
        limit,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(Some(writer.bytes)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use serde::ser::SerializeSeq;

    use super::*;
    use crate::model::{BlobRef, PayloadRef};

    #[test]
    fn absent_handoff_is_an_ordinary_checkpoint_but_present_invalid_data_is_not() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            read_optional_handoff(dir.path(), OutputSemantics::ProducesArtifact).unwrap(),
            None
        );

        std::fs::write(dir.path().join("handoff.json"), "not json").expect("write invalid handoff");
        let failure = read_optional_handoff(dir.path(), OutputSemantics::ProducesArtifact)
            .expect_err("a present malformed handoff must enter schema rework");
        assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
    }

    #[test]
    fn handoff_presence_is_exhaustively_owned_by_output_semantics() {
        let dir = tempfile::tempdir().expect("tempdir");
        for output in [
            OutputSemantics::ProducesReport,
            OutputSemantics::ProducesArtifact,
            OutputSemantics::ProposesPlan,
        ] {
            assert_eq!(read_optional_handoff(dir.path(), output).unwrap(), None);
        }
        for output in [
            OutputSemantics::EmitsVerdict,
            OutputSemantics::EmitsGapVerdict,
        ] {
            let failure = read_optional_handoff(dir.path(), output)
                .expect_err("verdict semantics require their typed handoff");
            assert!(failure.is_invalid_output());
            assert_eq!(failure.evidence().code.as_deref(), Some("handoff.missing"));
        }
    }

    #[test]
    fn interrupted_recovery_distinguishes_absence_from_malformed_evidence() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            read_retained_handoff(dir.path(), OutputSemantics::EmitsVerdict).unwrap(),
            None
        );

        std::fs::write(dir.path().join("handoff.json"), "not json").expect("write invalid handoff");
        let failure = read_retained_handoff(dir.path(), OutputSemantics::EmitsVerdict)
            .expect_err("present malformed evidence must survive recovery");
        assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
    }

    #[test]
    fn oversized_handoff_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let big = format!(
            "{{\"schema\":\"lionclaw.mission.work-handoff.v2\",\"type\":\"work\",\"done\":true,\"report\":\"{}\",\"request_attention\":false}}",
            "x".repeat((MAX_HANDOFF_BYTES + 1024) as usize)
        );
        std::fs::write(dir.path().join("handoff.json"), big).expect("write");
        let err = read_handoff(dir.path(), OutputSemantics::ProducesArtifact).expect_err("reject");
        assert!(err.is_invalid_output());
    }

    #[test]
    fn oversized_report_is_rejected_as_invalid_output() {
        let raw = format!(
            r#"{{"schema":"{WORK_HANDOFF_SCHEMA}","type":"work","done":true,"report":"{}","request_attention":false}}"#,
            "x".repeat(MAX_ROLE_REPORT_BYTES + 1)
        );
        let error = parse_handoff(&raw, OutputSemantics::ProducesArtifact)
            .expect_err("a report cannot exceed the aggregate prompt budget");
        assert!(error.is_invalid_output());
        assert_eq!(
            error.evidence().code.as_deref(),
            Some("handoff.report_too_large")
        );
    }

    #[test]
    fn alternate_runners_use_the_same_closed_output_contract_as_the_wire_parser() {
        let wrong_type = Handoff::Validate {
            done: true,
            report: PayloadRef::inline("not a work handoff"),
            items: Vec::new(),
            passed: true,
            request_attention: false,
        };
        let failure = validate_handoff(&wrong_type, OutputSemantics::ProducesArtifact)
            .expect_err("a runner cannot bypass the output contract");
        assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));

        let runner_minted_blob = Handoff::Work {
            done: true,
            report: PayloadRef::Blob(BlobRef {
                algo: "sha256".into(),
                hex: "a".repeat(64),
                len: 1,
            }),
            request_attention: false,
        };
        let failure = validate_handoff(&runner_minted_blob, OutputSemantics::ProducesArtifact)
            .expect_err("only the engine may mint report blob references");
        assert_eq!(
            failure.evidence().code.as_deref(),
            Some("handoff.payload_ref")
        );
    }

    #[test]
    fn bounded_serialization_stops_without_traversing_a_virtual_huge_value() {
        struct CountedSequence<'a> {
            visited: &'a Cell<usize>,
        }

        impl Serialize for CountedSequence<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut sequence = serializer.serialize_seq(Some(1_000_000))?;
                for _ in 0..1_000_000 {
                    self.visited.set(self.visited.get() + 1);
                    sequence.serialize_element("bounded-record")?;
                }
                sequence.end()
            }
        }

        let visited = Cell::new(0);
        assert_eq!(
            bounded_serialized_len(&CountedSequence { visited: &visited }, 1024).unwrap(),
            None
        );
        assert!(
            visited.get() < 100,
            "serializer traversed {} items",
            visited.get()
        );
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
        assert!(err.is_invalid_output());

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
            assert!(err.is_invalid_output());
        }
    }

    #[test]
    fn parses_work_handoff() {
        let raw = r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work",
                      "done":true,"report":"done",
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
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v2","type":"validate",
                      "done":true,"report":"findings",
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
    fn ordinary_validate_handoff_rejects_terminal_review_fields() {
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v2","type":"validate",
                      "done":true,"report":"checked",
                      "items":[],"passed":true,"request_attention":false,
                      "nonce":"terminal-only","gaps":[]}"#;
        let err = parse_handoff(raw, OutputSemantics::EmitsVerdict)
            .expect_err("terminal-review fields must not enter a validator handoff");
        assert!(err.is_invalid_output());
    }

    #[test]
    fn parses_review_handoff_with_gaps_and_nonce() {
        use crate::model::GapSeverity;
        let raw = r#"{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                      "done":true,"report":"map",
                      "passed":false,"nonce":"n-1",
                      "gaps":[{"severity":"blocking",
                               "requirement":"starts up",
                               "expected":"prints usage",
                               "observed":"panics",
                               "evidence":"cargo run -> panic"}]}"#;
        let handoff = parse_handoff(raw, OutputSemantics::EmitsGapVerdict).expect("parse");
        let Handoff::Review { gaps, nonce, .. } = handoff else {
            panic!("expected review handoff");
        };
        assert_eq!(nonce, "n-1");
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].severity, GapSeverity::Blocking);
    }

    #[test]
    fn review_handoff_rejects_validator_fields_and_requires_a_nonce() {
        for raw in [
            r#"{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                "done":true,"report":"map",
                "items":[],"passed":true,"nonce":"n-1","gaps":[]}"#,
            r#"{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                "done":true,"report":"map",
                "passed":true,"request_attention":false,"nonce":"n-1","gaps":[]}"#,
            r#"{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                "done":true,"report":"map",
                "passed":true,"gaps":[]}"#,
        ] {
            let err = parse_handoff(raw, OutputSemantics::EmitsGapVerdict)
                .expect_err("review and validator contracts must stay disjoint");
            assert!(err.is_invalid_output());
        }
    }

    #[test]
    fn oversized_review_gaps_are_rejected() {
        // Typed gaps land inline in the event log, so the terminal-review
        // parser caps them before they can reach an event.
        let evidence = "x".repeat(300 * 1024);
        let raw = format!(
            r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                "done":true,"report":"",
                "passed":false,"nonce":"n-1",
                "gaps":[{{"severity":"blocking","requirement":"r",
                         "expected":"e","observed":"o","evidence":"{evidence}"}}]
                }}"#
        );
        let err = parse_handoff(&raw, OutputSemantics::EmitsGapVerdict).expect_err("must refuse");
        assert!(err.is_invalid_output());
        assert!(err.detail().contains("cite short excerpts"));
    }

    #[test]
    fn rejects_unknown_gap_severity_and_unknown_gap_fields() {
        // The severity axis and the Gap shape are closed: a typo'd severity,
        // a stray field, a missing field, or an empty prose field (a gap is a
        // falsifiable claim — the prompt promises evidence-less claims are
        // rejected) fails the attempt rather than passing as prose.
        for gap_json in [
            r#"{"severity":"severe","requirement":"r","expected":"e","observed":"o"}"#,
            r#"{"severity":"blocking","requirement":"r","expected":"e","observed":"o","note":"x"}"#,
            r#"{"severity":"blocking","requirement":"r"}"#,
            r#"{"severity":"blocking","requirement":"r","expected":"e","observed":"o"}"#,
            r#"{"severity":"blocking","requirement":"r","expected":"e","observed":"o","evidence":"  "}"#,
            r#"{"severity":"blocking","requirement":"","expected":"e","observed":"o","evidence":"v"}"#,
        ] {
            let raw = format!(
                r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review",
                    "done":true,"report":"",
                    "passed":false,"nonce":"n-1","gaps":[{gap_json}]}}"#
            );
            let err =
                parse_handoff(&raw, OutputSemantics::EmitsGapVerdict).expect_err("must refuse");
            assert!(err.is_invalid_output());
        }
    }

    #[test]
    fn rejects_wrong_schema_for_role() {
        // A judge trying to hand back a work handoff is refused.
        let raw = r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work",
                      "done":true,"report":"looks great",
                      "request_attention":false}"#;
        let err = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
        assert!(err.is_invalid_output());
    }

    #[test]
    fn rejects_missing_schema_and_bad_json() {
        for raw in [r#"{"type":"work","done":true}"#, "not json"] {
            let err = parse_handoff(raw, OutputSemantics::ProducesArtifact).expect_err("refuse");
            assert!(err.is_invalid_output());
        }
    }

    #[test]
    fn rejects_invalid_item_ids() {
        // Item ids run through the AssertionId charset validation.
        let raw = r#"{"schema":"lionclaw.mission.validate-handoff.v2","type":"validate",
                      "done":true,"report":"",
                      "items":[{"item_id":"lowercase-bad","passed":true}],
                      "passed":true,"request_attention":false}"#;
        let err = parse_handoff(raw, OutputSemantics::EmitsVerdict).expect_err("must refuse");
        assert!(err.is_invalid_output());
    }

    #[test]
    fn agent_handoffs_cannot_supply_durable_blob_references() {
        let raw = r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work",
                      "done":true,
                      "report":{"kind":"blob","blob":{"algo":"sha256",
                                "hex":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "len":1}},
                      "request_attention":false}"#;
        let err = parse_handoff(raw, OutputSemantics::ProducesArtifact)
            .expect_err("only the engine may mint payload refs");
        assert!(err.is_invalid_output());
    }
}
