//! Verdict provenance and finish classification.
//!
//! Structural honesty: [`AuthoritativeVerdict`] has private fields and a
//! `pub(crate)` constructor — only the fold can mint one, and it only does so
//! from `OracleRunCompleted` events (an engine-run command's real exit code).
//! Agent handoffs have no field that could ever become authoritative.

use serde::{Deserialize, Serialize};

use super::event::PayloadRef;
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

/// Classify a finished mission. Freshness: an authoritative verdict counts
/// only if it judged the mission's current artifact commit.
pub fn classify_finish(state: &MissionState) -> FinishClass {
    let mut all_authoritative = true;
    let mut all_green = true;
    for assertion in state.contract.values() {
        let fresh_pass = assertion
            .last_authoritative
            .as_ref()
            .is_some_and(|v| v.judged_sha() == state.current_sha && v.passed());
        if !fresh_pass {
            all_authoritative = false;
            let advisory_green = assertion.advisory == AdvisoryStatus::Passed;
            if !advisory_green {
                all_green = false;
            }
        }
    }
    if state.contract.is_empty() {
        return FinishClass::Unverified;
    }
    if all_authoritative {
        FinishClass::Verified
    } else if all_green {
        FinishClass::InternallyConsistent
    } else {
        FinishClass::Unverified
    }
}
