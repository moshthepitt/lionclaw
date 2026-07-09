//! Verdict provenance and finish classification.
//!
//! Structural honesty: [`AuthoritativeVerdict`] has private fields and a
//! `pub(crate)` constructor — only the fold can mint one, and it only does so
//! from `OracleRunCompleted` events (an engine-run command's real exit code).
//! Agent handoffs have no field that could ever become authoritative.

use serde::{Deserialize, Serialize};

use super::event::{PayloadRef, StopBar};
use super::ids::OracleName;
use super::state::{AdvisoryStatus, MissionState};

/// A worker-independent, reproducible verdict from an engine-run oracle,
/// with its evidence. The evidence floor is the constructor signature: no
/// exit code and output, no verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeVerdict {
    passed: bool,
    oracle: OracleName,
    judged_sha: String,
    exit_code: i32,
    exit_signal: Option<i32>,
    stdout: PayloadRef,
    stderr: PayloadRef,
    at_seq: u64,
}

impl AuthoritativeVerdict {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_oracle_outcome(
        oracle: OracleName,
        judged_sha: String,
        exit_code: i32,
        exit_signal: Option<i32>,
        stdout: PayloadRef,
        stderr: PayloadRef,
        at_seq: u64,
    ) -> Self {
        Self {
            passed: exit_code == 0 && exit_signal.is_none(),
            oracle,
            judged_sha,
            exit_code,
            exit_signal,
            stdout,
            stderr,
            at_seq,
        }
    }

    pub fn passed(&self) -> bool {
        self.passed
    }

    pub fn oracle(&self) -> &OracleName {
        &self.oracle
    }

    pub fn judged_sha(&self) -> &str {
        &self.judged_sha
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }

    pub fn evidence(&self) -> (&PayloadRef, &PayloadRef) {
        (&self.stdout, &self.stderr)
    }
}

/// How honest a finish is. The engine says "verified" only with fresh
/// authoritative coverage of every assertion; advisory-only green is
/// "internally consistent", never verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishClass {
    Verified,
    InternallyConsistent,
    Unverified,
}

impl StopBar {
    /// Does a finish class clear this honesty bar? `Verified` demands an
    /// authoritative oracle pass; `Reviewed` also accepts agent-only
    /// `InternallyConsistent`. `Unverified` clears neither.
    pub const fn satisfied_by(self, finish: FinishClass) -> bool {
        matches!(
            (self, finish),
            (StopBar::Verified, FinishClass::Verified)
                | (
                    StopBar::Reviewed,
                    FinishClass::Verified | FinishClass::InternallyConsistent
                )
        )
    }
}

/// Classify a finished mission. Freshness: an authoritative verdict counts
/// only if it judged the mission's current artifact commit.
///
/// A fresh authoritative **fail** is ground truth and dominates: it forces
/// Unverified no matter what an advisory verdict claims. "Internally
/// consistent" means advisory-only green with *no* fresh authoritative
/// contradiction — never a green advisory papering over a real oracle
/// failure.
pub fn classify_finish(state: &MissionState) -> FinishClass {
    if state.contract.is_empty() {
        return FinishClass::Unverified;
    }
    let mut all_authoritative_pass = true;
    let mut all_green = true;
    for assertion in state.contract.values() {
        let fresh = assertion
            .last_authoritative
            .as_ref()
            .filter(|v| v.judged_sha() == state.current_sha);
        match fresh {
            Some(v) if v.passed() => {}
            Some(_) => {
                // Fresh authoritative fail: ground truth, dominates advisory.
                all_authoritative_pass = false;
                all_green = false;
            }
            None => {
                all_authoritative_pass = false;
                if assertion.advisory != AdvisoryStatus::Passed {
                    all_green = false;
                }
            }
        }
    }
    if all_authoritative_pass {
        FinishClass::Verified
    } else if all_green {
        FinishClass::InternallyConsistent
    } else {
        FinishClass::Unverified
    }
}
