//! Verdict provenance and finish classification.
//!
//! Structural honesty: [`AuthoritativeVerdict`] has private fields and a
//! `pub(crate)` constructor — only the fold can mint one, and it only does so
//! from `OracleRunCompleted` events (an engine-run command's real exit code).
//! Agent handoffs have no field that could ever become authoritative.

use serde::{Deserialize, Serialize};

use super::event::{PayloadRef, PreparedInputRef, StopBar};
use super::ids::OracleName;
use super::state::{AdvisoryStatus, MissionState};
use crate::prelude::*;

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    prepared_inputs: Vec<PreparedInputRef>,
}

impl AuthoritativeVerdict {
    pub(crate) fn from_oracle_outcome(
        oracle: OracleName,
        judged_sha: String,
        exit_code: i32,
        exit_signal: Option<i32>,
        stdout: PayloadRef,
        stderr: PayloadRef,
        prepared_inputs: Vec<PreparedInputRef>,
    ) -> Self {
        Self {
            passed: exit_code == 0 && exit_signal.is_none(),
            oracle,
            judged_sha,
            exit_code,
            exit_signal,
            stdout,
            stderr,
            prepared_inputs,
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

    /// Whether this verdict judged the mission's current artifact commit — the
    /// freshness the honesty moat turns on. One definition, so the fold, the
    /// scheduler, and the report can never disagree about what "fresh" means.
    pub fn is_fresh_at(&self, current_sha: &str) -> bool {
        self.judged_sha == current_sha
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }

    pub fn exit_signal(&self) -> Option<i32> {
        self.exit_signal
    }

    pub fn evidence(&self) -> (&PayloadRef, &PayloadRef) {
        (&self.stdout, &self.stderr)
    }

    pub fn prepared_inputs(&self) -> &[PreparedInputRef] {
        &self.prepared_inputs
    }
}

/// How honest a finish is. The engine says "verified" only with fresh
/// authoritative coverage of every assertion; judged-only green is
/// "attested", never verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishClass {
    Verified,
    Attested,
    Unverified,
}

impl FinishClass {
    /// The stable snake_case name (matches the serde repr). One source for
    /// both the fold and the CLI, so no `format!("{:?}").to_lowercase()` drifts
    /// into `internallyconsistent`.
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Attested => "attested",
            Self::Unverified => "unverified",
        }
    }
}

impl StopBar {
    /// Does a finish class clear this honesty bar? `Verified` demands an
    /// authoritative oracle pass; `Attested` also accepts fresh judged proof.
    /// `Unverified` clears neither.
    pub const fn satisfied_by(self, finish: FinishClass) -> bool {
        matches!(
            (self, finish),
            (StopBar::Verified, FinishClass::Verified)
                | (
                    StopBar::Attested,
                    FinishClass::Verified | FinishClass::Attested
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
    let Some(plan) = &state.plan else {
        return FinishClass::Unverified;
    };
    if state.contract.is_empty() {
        return FinishClass::Unverified;
    }
    let mut all_authoritative_pass = true;
    let mut all_green = true;
    for (assertion_id, assertion) in &state.contract {
        let fresh = assertion
            .last_authoritative
            .as_ref()
            .filter(|v| v.is_fresh_at(state.deliverable_head()));
        match fresh {
            Some(v) if v.passed() => {}
            Some(_) => {
                // Fresh authoritative fail: ground truth, dominates advisory.
                all_authoritative_pass = false;
                all_green = false;
            }
            None => {
                all_authoritative_pass = false;
            }
        }
        if plan.assertion_requires_confined_proof(assertion_id) {
            if !matches!(fresh, Some(v) if v.passed()) {
                all_green = false;
            }
        } else if plan.assertion_requires_judged_proof(assertion_id) {
            all_authoritative_pass = false;
            if state.advisory_status(assertion_id) != AdvisoryStatus::Passed {
                all_green = false;
            }
        } else {
            all_authoritative_pass = false;
            all_green = false;
        }
    }
    if all_authoritative_pass {
        FinishClass::Verified
    } else if all_green {
        FinishClass::Attested
    } else {
        FinishClass::Unverified
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // P7's "honest exit code at finish": Verified is cleared only by a Verified
    // finish; Attested also accepts judged-only proof;
    // Unverified clears neither. `cmd_advance` exits nonzero when this is false.
    #[test]
    fn stop_bar_satisfied_by_truth_table() {
        use FinishClass::{Attested, Unverified, Verified};
        assert!(StopBar::Verified.satisfied_by(Verified));
        assert!(!StopBar::Verified.satisfied_by(Attested));
        assert!(!StopBar::Verified.satisfied_by(Unverified));
        assert!(StopBar::Attested.satisfied_by(Verified));
        assert!(StopBar::Attested.satisfied_by(Attested));
        assert!(!StopBar::Attested.satisfied_by(Unverified));
    }
}
