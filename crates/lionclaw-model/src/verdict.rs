//! Verdict provenance and finish classification.
//!
//! Structural honesty: [`AuthoritativeVerdict`] has private fields and a
//! `pub(crate)` constructor — only the fold can mint one, and it only does so
//! from `OracleRunCompleted` events (an engine-run command's real exit code).
//! Agent handoffs have no field that could ever become authoritative.

use serde::{Deserialize, Serialize};

use super::event::{OracleRunSuccess, PayloadRef, PreparedInputRef, StopBar};
use super::ids::{AssertionId, EffectId, OracleName, RoleInstanceId};
use super::state::{MissionState, RoleAttemptReceipt, RoleEffectSource};
use crate::prelude::*;

/// A worker-independent, reproducible verdict from an engine-run oracle,
/// with its evidence. The evidence floor is the constructor signature: no
/// exit code and output, no verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeVerdict {
    passed: bool,
    assertion_ids: Vec<AssertionId>,
    oracle: OracleName,
    spec_digest: String,
    judged_sha: String,
    environment_digest: String,
    attempt_no: u32,
    exit_code: i32,
    exit_signal: Option<i32>,
    stdout: PayloadRef,
    stderr: PayloadRef,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    prepared_inputs: Vec<PreparedInputRef>,
}

impl AuthoritativeVerdict {
    pub(crate) fn from_oracle_success(
        assertion_ids: Vec<AssertionId>,
        oracle: OracleName,
        spec_digest: String,
        judged_sha: String,
        environment_digest: String,
        attempt_no: u32,
        success: &OracleRunSuccess,
    ) -> Self {
        Self {
            passed: success.exit_code == 0 && success.exit_signal.is_none(),
            assertion_ids,
            oracle,
            spec_digest,
            judged_sha,
            environment_digest,
            attempt_no,
            exit_code: success.exit_code,
            exit_signal: success.exit_signal,
            stdout: success.stdout.clone(),
            stderr: success.stderr.clone(),
            prepared_inputs: success.prepared_inputs.clone(),
        }
    }

    pub fn passed(&self) -> bool {
        self.passed
    }

    pub fn assertion_ids(&self) -> &[AssertionId] {
        &self.assertion_ids
    }

    pub fn oracle(&self) -> &OracleName {
        &self.oracle
    }

    pub fn spec_digest(&self) -> &str {
        &self.spec_digest
    }

    pub fn judged_sha(&self) -> &str {
        &self.judged_sha
    }

    pub fn environment_digest(&self) -> &str {
        &self.environment_digest
    }

    pub fn attempt_no(&self) -> u32 {
        self.attempt_no
    }

    /// Whether this verdict judged the mission's current artifact commit under
    /// its current immutable runtime environment. One definition, so the fold,
    /// scheduler, and reports cannot drift on what "fresh" means.
    pub fn is_fresh_at(&self, state: &MissionState) -> bool {
        self.judged_sha == state.deliverable_head()
            && self.environment_digest == state.environment_digest()
            && state
                .oracles
                .get(&self.oracle)
                .is_some_and(|spec| spec.digest() == self.spec_digest)
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

    fn same_identity(&self, other: &Self) -> bool {
        self.assertion_ids == other.assertion_ids
            && self.oracle == other.oracle
            && self.spec_digest == other.spec_digest
            && self.judged_sha == other.judged_sha
            && self.environment_digest == other.environment_digest
            && self.prepared_inputs == other.prepared_inputs
    }

    fn same_outcome(&self, other: &Self) -> bool {
        self.exit_code == other.exit_code
            && self.exit_signal == other.exit_signal
            && self.stdout == other.stdout
            && self.stderr == other.stderr
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProofSource {
    Command {
        oracle: OracleName,
        assertion_ids: Vec<AssertionId>,
    },
    Judgment {
        role_instance: RoleInstanceId,
        assertion_ids: Vec<AssertionId>,
    },
    Review {
        role_instance: RoleInstanceId,
    },
}

impl ProofSource {
    pub(crate) fn assertion_ids(&self) -> &[AssertionId] {
        match self {
            Self::Command { assertion_ids, .. } | Self::Judgment { assertion_ids, .. } => {
                assertion_ids
            }
            Self::Review { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProofFailure {
    Receipt {
        source: ProofSource,
        effect_id: EffectId,
    },
    StopBar {
        finish: FinishClass,
        assertion_ids: Vec<AssertionId>,
    },
}

impl ProofFailure {
    pub(crate) fn effect_id(&self) -> Option<&EffectId> {
        match self {
            Self::Receipt { effect_id, .. } => Some(effect_id),
            Self::StopBar { .. } => None,
        }
    }

    pub(crate) fn assertion_ids(&self) -> &[AssertionId] {
        match self {
            Self::Receipt { source, .. } => source.assertion_ids(),
            Self::StopBar { assertion_ids, .. } => assertion_ids,
        }
    }

    pub(crate) fn retry_available(&self, state: &MissionState) -> bool {
        match self {
            Self::Receipt {
                source: ProofSource::Command { .. },
                effect_id,
            } => command_retry_available(state, effect_id),
            Self::Receipt {
                source: ProofSource::Judgment { .. } | ProofSource::Review { .. },
                effect_id,
            } => role_proof_retry_available(state, effect_id),
            Self::StopBar { .. } => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProofReadiness {
    Pending(Vec<ProofSource>),
    Failed(Vec<ProofFailure>),
    Satisfied(FinishClass),
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

/// Derive every required proof obligation once. Failed proof dominates pending
/// work, and only a fresh all-green proof set can satisfy the declared stop bar.
pub(crate) fn proof_readiness(state: &MissionState) -> ProofReadiness {
    let Some(plan) = &state.plan else {
        return ProofReadiness::Pending(Vec::new());
    };
    if state.contract.is_empty() {
        return ProofReadiness::Pending(Vec::new());
    }

    let mut pending_commands: BTreeMap<OracleName, Vec<AssertionId>> = BTreeMap::new();
    let mut pending_judgments = Vec::new();
    let mut failed_commands: BTreeMap<(OracleName, EffectId), Vec<AssertionId>> = BTreeMap::new();
    let mut failed_judgments: BTreeMap<(RoleInstanceId, EffectId), Vec<AssertionId>> =
        BTreeMap::new();
    let mut all_authoritative_pass = true;
    let mut all_green = true;

    for (assertion_id, assertion) in &state.contract {
        let fresh_authoritative = assertion
            .last_authoritative_receipt
            .as_ref()
            .and_then(|effect_id| {
                state
                    .authoritative_receipts
                    .get(effect_id)
                    .map(|verdict| (effect_id, verdict))
            })
            .filter(|(_, verdict)| verdict.is_fresh_at(state));

        if plan.assertion_requires_confined_proof(assertion_id) {
            match fresh_authoritative {
                Some((_, verdict)) if verdict.passed() => {}
                Some((effect_id, verdict)) => {
                    failed_commands
                        .entry((verdict.oracle().clone(), effect_id.clone()))
                        .or_default()
                        .push(assertion_id.clone());
                    all_authoritative_pass = false;
                    all_green = false;
                }
                None => {
                    if let Some(oracle) = &assertion.oracle {
                        pending_commands
                            .entry(oracle.clone())
                            .or_default()
                            .push(assertion_id.clone());
                    }
                    all_authoritative_pass = false;
                    all_green = false;
                }
            }
        } else if plan.assertion_requires_judged_proof(assertion_id) {
            all_authoritative_pass = false;
            let Some(panel) = state
                .team
                .as_ref()
                .and_then(|team| team.judgment_assignments.get(assertion_id))
            else {
                all_green = false;
                continue;
            };
            for role_instance in panel {
                let receipt = assertion
                    .last_advisory
                    .get(role_instance)
                    .and_then(|effect_id| {
                        state
                            .advisory_receipt(assertion_id, role_instance, effect_id)
                            .map(|(receipt, passed)| (effect_id, receipt, passed))
                    });
                match receipt {
                    Some((_, _, true)) => {}
                    Some((effect_id, _, false)) => {
                        failed_judgments
                            .entry((role_instance.clone(), effect_id.clone()))
                            .or_default()
                            .push(assertion_id.clone());
                        all_green = false;
                    }
                    None => {
                        pending_judgments.push(ProofSource::Judgment {
                            role_instance: role_instance.clone(),
                            assertion_ids: vec![assertion_id.clone()],
                        });
                        all_green = false;
                    }
                }
            }
        } else {
            if !matches!(fresh_authoritative, Some((_, verdict)) if verdict.passed()) {
                all_authoritative_pass = false;
            }
            all_green = false;
        }
    }

    let mut failures = Vec::new();
    failures.extend(
        failed_commands
            .into_iter()
            .map(
                |((oracle, effect_id), assertion_ids)| ProofFailure::Receipt {
                    source: ProofSource::Command {
                        oracle,
                        assertion_ids,
                    },
                    effect_id,
                },
            ),
    );
    failures.extend(failed_judgments.into_iter().map(
        |((role_instance, effect_id), assertion_ids)| ProofFailure::Receipt {
            source: ProofSource::Judgment {
                role_instance,
                assertion_ids,
            },
            effect_id,
        },
    ));
    if !failures.is_empty() {
        return ProofReadiness::Failed(failures);
    }

    let mut pending = pending_judgments;
    pending.extend(pending_commands.into_iter().map(|(oracle, assertion_ids)| {
        ProofSource::Command {
            oracle,
            assertion_ids,
        }
    }));
    if !pending.is_empty() {
        return ProofReadiness::Pending(pending);
    }

    let finish = if all_authoritative_pass {
        FinishClass::Verified
    } else if all_green {
        FinishClass::Attested
    } else {
        FinishClass::Unverified
    };
    if !state.config.stop.satisfied_by(finish) {
        return ProofReadiness::Failed(vec![ProofFailure::StopBar {
            finish,
            assertion_ids: state.contract.keys().cloned().collect(),
        }]);
    }
    review_readiness(state, finish)
}

fn review_readiness(state: &MissionState, finish: FinishClass) -> ProofReadiness {
    if !state.config.requires_gap_review {
        return ProofReadiness::Satisfied(finish);
    }
    let Some(role_instance) = state
        .team
        .as_ref()
        .and_then(|team| team.gap_review_assignment.clone())
    else {
        return ProofReadiness::Pending(Vec::new());
    };
    let source = ProofSource::Review {
        role_instance: role_instance.clone(),
    };
    let Some(receipt) = state.latest_taskless_assignment_receipt(&role_instance, &[]) else {
        return ProofReadiness::Pending(vec![source]);
    };
    if !state.role_attempt_is_fresh(receipt) {
        return ProofReadiness::Pending(vec![source]);
    }
    match &receipt.disposition {
        super::RoleAttemptDisposition::Succeeded {
            handoff: Some(handoff),
            ..
        } => {
            let super::SettledHandoff::Review { passed, gaps } = handoff.as_ref() else {
                return ProofReadiness::Pending(vec![source]);
            };
            let blocking = !passed
                || gaps
                    .iter()
                    .any(|gap| gap.severity == super::GapSeverity::Blocking);
            if !blocking {
                ProofReadiness::Satisfied(finish)
            } else if state.parked_effects.contains_key(&receipt.effect_id) {
                ProofReadiness::Failed(vec![ProofFailure::Receipt {
                    source,
                    effect_id: receipt.effect_id.clone(),
                }])
            } else {
                ProofReadiness::Pending(vec![source])
            }
        }
        super::RoleAttemptDisposition::Failed { failure } => {
            let consecutive = state
                .taskless_assignment_failure(&role_instance, &[])
                .map(|(_, _, consecutive)| consecutive)
                .unwrap_or_default();
            if failure.automatically_retryable() && consecutive < state.config.recovery.max_attempts
            {
                ProofReadiness::Pending(vec![source])
            } else {
                ProofReadiness::Failed(vec![ProofFailure::Receipt {
                    source,
                    effect_id: receipt.effect_id.clone(),
                }])
            }
        }
        super::RoleAttemptDisposition::Active | super::RoleAttemptDisposition::Retired => {
            ProofReadiness::Pending(vec![source])
        }
        super::RoleAttemptDisposition::Succeeded { handoff: None, .. } => {
            ProofReadiness::Pending(vec![source])
        }
    }
}

fn command_retry_available(state: &MissionState, effect_id: &EffectId) -> bool {
    let Some(current) = state.authoritative_receipts.get(effect_id) else {
        return false;
    };
    state
        .authoritative_receipts
        .values()
        .filter(|receipt| {
            receipt.attempt_no() < current.attempt_no() && receipt.same_identity(current)
        })
        .max_by_key(|receipt| receipt.attempt_no())
        .is_none_or(|previous| !previous.same_outcome(current))
}

fn role_proof_retry_available(state: &MissionState, effect_id: &EffectId) -> bool {
    let Some(current) = state.role_attempt_receipts.get(effect_id) else {
        return false;
    };
    let RoleEffectSource::Turn {
        request: current_request,
        plan_revision: current_revision,
    } = &current.source;
    state
        .role_attempt_receipts
        .values()
        .filter_map(|receipt| {
            let RoleEffectSource::Turn {
                request,
                plan_revision,
            } = &receipt.source;
            (*plan_revision == *current_revision
                && request.attempt_no < current_request.attempt_no
                && same_role_proof_identity(request, current_request))
            .then_some((request.attempt_no, receipt))
        })
        .max_by_key(|(attempt_no, _)| *attempt_no)
        .is_none_or(|(_, previous)| !same_role_proof_outcome(previous, current))
}

fn same_role_proof_identity(
    left: &super::RoleTurnProvenance,
    right: &super::RoleTurnProvenance,
) -> bool {
    left.role_instance == right.role_instance
        && left.team_revision == right.team_revision
        && left.task_id == right.task_id
        && left.assertion_ids == right.assertion_ids
        && left.assignment_epoch == right.assignment_epoch
        && left.prompt_template == right.prompt_template
        && (left.prompt_template == super::RolePromptTemplate::GapReview
            || left.prompt_hash == right.prompt_hash)
        && left.base_sha == right.base_sha
        && left.environment_digest == right.environment_digest
        && left.instrument_identity == right.instrument_identity
        && left.dependency_refs == right.dependency_refs
        && left.workspace_preparation == right.workspace_preparation
}

fn same_role_proof_outcome(left: &RoleAttemptReceipt, right: &RoleAttemptReceipt) -> bool {
    left.disposition == right.disposition && left.accepted_report() == right.accepted_report()
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
