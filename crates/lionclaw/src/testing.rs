//! Mock port implementations for the deterministic core's tests
//! (`feature = "testing"`). Scripted responders with call logs and per-key
//! invocation counters — the resume tests assert an idempotency key is never
//! executed twice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::model::{ArtifactOutcome, Gap, Handoff, PayloadRef, TaskId};
use crate::ports::{
    Clock, OracleFailure, OracleOutcome, OracleRunRequest, OracleRunner, RoleRunFailure,
    RoleRunOutcome, RoleRunRequest, RoleRunner,
};

/// A terminal-review verdict outcome that echoes the request prompt's nonce
/// (judges have no clone, so `artifact` is always `None`).
pub fn review_verdict(request: &RoleRunRequest, passed: bool, gaps: Vec<Gap>) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Handoff::Review {
            done: true,
            report: PayloadRef::inline("requirement map + observations"),
            passed,
            gaps,
            nonce: crate::prompt::handoff_nonce(&request.prompt)
                .expect("terminal-review prompt has a nonce")
                .to_string(),
        },
        artifact: None,
        model_id: Some("mock-model".to_string()),
    }
}

/// Deterministic monotonic clock — proves nothing depends on real time.
#[derive(Default)]
pub struct MockClock {
    now: AtomicI64,
}

impl Clock for MockClock {
    fn now_ms(&self) -> i64 {
        self.now.fetch_add(1, Ordering::SeqCst) + 1_000_000
    }
}

type RoleScript =
    Box<dyn Fn(&RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> + Send + Sync>;

pub struct MockRoleRunner {
    script: RoleScript,
    pub calls: Mutex<Vec<(TaskId, u32, String)>>,
    pub invocations_by_key: Mutex<BTreeMap<String, u32>>,
}

impl MockRoleRunner {
    pub fn new(script: RoleScript) -> Self {
        Self {
            script,
            calls: Mutex::new(Vec::new()),
            invocations_by_key: Mutex::new(BTreeMap::new()),
        }
    }

    /// A worker that reports done and "commits" a deterministic new sha.
    pub fn happy(head_sha: &str) -> Self {
        let head_sha = head_sha.to_string();
        Self::new(Box::new(move |request| {
            Ok(RoleRunOutcome {
                handoff: Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("did the work"),
                    request_attention: false,
                },
                artifact: Some(ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: head_sha.clone(),
                }),
                model_id: Some("mock-model".to_string()),
            })
        }))
    }

    pub fn max_invocations_per_key(&self) -> u32 {
        self.invocations_by_key
            .lock()
            .expect("lock")
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }
}

#[async_trait]
impl RoleRunner for MockRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        self.calls.lock().expect("lock").push((
            request.task_id.clone(),
            request.attempt_no,
            request.idempotency_key.clone(),
        ));
        *self
            .invocations_by_key
            .lock()
            .expect("lock")
            .entry(request.idempotency_key.clone())
            .or_insert(0) += 1;
        (self.script)(&request)
    }
}

type OracleScript =
    Box<dyn Fn(&OracleRunRequest) -> Result<OracleOutcome, OracleFailure> + Send + Sync>;

pub struct MockOracleRunner {
    script: OracleScript,
    pub calls: Mutex<Vec<(String, String)>>,
}

impl MockOracleRunner {
    pub fn new(script: OracleScript) -> Self {
        Self {
            script,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// An oracle with a fixed exit code.
    pub fn exiting(exit_code: i32) -> Self {
        Self::new(Box::new(move |_| {
            Ok(OracleOutcome {
                exit_code,
                exit_signal: None,
                stdout: format!("oracle exit {exit_code}").into_bytes(),
                stderr: Vec::new(),
                duration_ms: 42,
            })
        }))
    }
}

#[async_trait]
impl OracleRunner for MockOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, OracleFailure> {
        self.calls
            .lock()
            .expect("lock")
            .push((request.oracle.to_string(), request.judged_sha.clone()));
        (self.script)(&request)
    }
}
