//! `lionclaw mission self-test`: drive the real stack (real store + fold +
//! loop + real podman confinement + real engine-run oracle) and assert the
//! four Slice-1 invariants, complete plan revision, terminal-review closure,
//! native read-only skill mounting, and prepared inputs (eight checks). Hermetic and
//! model-auth-free — the *oracle*
//! decides every outcome, so no agent turn (and no model credentials) is
//! required. The agentic multi-run eval stays in `scripts/mission-eval.sh`.
//!
//! Exit codes: 0 all green · 1 a check failed · 2 podman/image unavailable
//! (the runtime checks were skipped — never green-washed).

use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::authority::{
    compile_authority, compile_role_plan, oracle_authority, AuthorityCeiling, CompiledAuthority,
    MissionMounts, RolePlanRequest,
};
use crate::config::RuntimeProfiles;
use crate::engine::{Engine, EngineServices, MissionDisposition, ProposeError};
use crate::mission_type::{load_mission_type, MissionTypeError};
use crate::model::{
    ArtifactOutcome, Assertion, AssertionId, DecisionAction, EffectId, FinishClass, Gap,
    GapSeverity, Handoff, MissionConfig, MissionEvent, MissionId, MissionPhase, OracleName,
    PayloadRef, Plan, PlanProposal, ProposalError, Requirement, RequirementDisposition,
    RequirementId, RequirementKind, ReviewAcceptanceKind, RoleName, RunErrorKind, Task, TaskId,
    TaskKind, TaskStatus,
};
use crate::oracle::OciOracleRunner;
use crate::ports::{
    OracleFailure, OracleOutcome, OracleRunRequest, OracleRunner, RoleRunFailure, RoleRunOutcome,
    RoleRunRequest, RoleRunner, SystemClock,
};
use crate::runner::MissionProgramExecutor;
use crate::store::MissionStore;
use crate::workspace;

use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{ExecutionOutput, RuntimeAuthRegistry, RuntimeProgramExecutor};

const RUNTIME_IMAGE: &str = "localhost/lionclaw-runtime-dev:v1";

/// A no-op producer: clears its work task without changing the tree, so the
/// real oracle judges the base fixture as-is. Used by the oracle-honesty
/// check, where the tree must stay broken for the oracle to decide.
struct NoopRoleRunner;

#[async_trait]
impl RoleRunner for NoopRoleRunner {
    async fn run(&self, _request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("self-test noop worker"),
                request_attention: false,
            },
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: "self-test noop worker".to_string(),
        })
    }
}

/// A worker that "commits" a fixed head, plus a terminal reviewer that
/// returns one blocking gap (echoing the prompt's nonce, as a real agent
/// must). Drives check (6) without a model or a container.
struct ReviewParkRoleRunner;

#[async_trait]
impl RoleRunner for ReviewParkRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        if request.task_id.as_str() == crate::engine::TERMINAL_REVIEW_TASK_TAG {
            Ok(RoleRunOutcome {
                handoff: Handoff::Review {
                    done: true,
                    report: PayloadRef::inline("self-test scripted review"),
                    passed: false,
                    gaps: vec![Gap {
                        id: Some("GAP-1".to_string()),
                        severity: GapSeverity::Blocking,
                        requirement: "the objective's behavior".to_string(),
                        expected: "it works".to_string(),
                        observed: "it does not".to_string(),
                        evidence: "self-test scripted verdict".to_string(),
                    }],
                    nonce: crate::prompt::handoff_nonce(&request.prompt)
                        .expect("terminal-review prompt has a nonce")
                        .to_string(),
                },
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "self-test scripted review".to_string(),
            })
        } else {
            Ok(RoleRunOutcome {
                handoff: Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("self-test worker"),
                    request_attention: false,
                },
                artifact: Some(ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: request.base_sha,
                }),
                runtime_configuration: Default::default(),
                final_response: "self-test worker".to_string(),
            })
        }
    }
}

/// An engine-side oracle with a fixed exit code — check (6) is about the
/// closure gate, not the oracle, so no container is needed.
struct FixedOracleRunner(i32);

#[async_trait]
impl OracleRunner for FixedOracleRunner {
    async fn run(&self, _request: OracleRunRequest) -> Result<OracleOutcome, OracleFailure> {
        Ok(OracleOutcome {
            exit_code: self.0,
            exit_signal: None,
            stdout: format!("self-test oracle exit {}", self.0).into_bytes(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }
}

/// Wraps the real confined oracle and counts how many times it actually ran,
/// so check (1) can assert resume does NOT re-execute the oracle.
struct CountingOracleRunner {
    inner: OciOracleRunner,
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl OracleRunner for CountingOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, OracleFailure> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.run(request).await
    }
}

#[derive(Debug)]
enum CheckStatus {
    Pass,
    Fail(String),
    Skip(String),
}

struct Check {
    name: &'static str,
    status: CheckStatus,
}

pub async fn run(json: bool) -> Result<ExitCode> {
    let podman = podman_readiness().await;
    let mut checks = Vec::new();

    // (3) Moat, (5) re-planning, and (6) terminal review are pure — they
    // always run, even without podman.
    checks.push(Check {
        name: "moat-refuses-over-privileged-judge",
        status: to_status(check_moat().await),
    });
    checks.push(Check {
        name: "replanning-revises-atomically-and-strengthen-only",
        status: to_status(check_replanning().await),
    });
    checks.push(Check {
        name: "terminal-review-gates-closure",
        status: to_status(check_terminal_review().await),
    });

    // (1),(2),(4),(7) need real confinement.
    for (name, runtime_check) in runtime_checks() {
        let status = match &podman {
            Ok(()) => to_status(runtime_check().await),
            Err(reason) => CheckStatus::Skip(reason.clone()),
        };
        checks.push(Check { name, status });
    }

    report(&checks, json);
    Ok(exit_code(&checks))
}

type RuntimeCheck = fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

fn runtime_checks() -> Vec<(&'static str, RuntimeCheck)> {
    vec![
        ("writable-worker-writes-land-and-resume-no-dup", || {
            Box::pin(check_happy_writer_and_resume())
        }),
        ("oracle-honesty-on-real-broken-code", || {
            Box::pin(check_oracle_honesty())
        }),
        ("confinement-read-only-workspace-erofs", || {
            Box::pin(check_confinement_erofs())
        }),
        ("runtime-native-skill-mount", || {
            Box::pin(check_runtime_skill_mount())
        }),
        ("prepared-input-feeds-network-off-oracle", || {
            Box::pin(check_prepared_input())
        }),
    ]
}

fn to_status(result: Result<()>) -> CheckStatus {
    match result {
        Ok(()) => CheckStatus::Pass,
        Err(e) => CheckStatus::Fail(format!("{e:#}")),
    }
}

fn report(checks: &[Check], json: bool) {
    if json {
        let items: Vec<_> = checks
            .iter()
            .map(|c| {
                let (status, detail) = match &c.status {
                    CheckStatus::Pass => ("pass", String::new()),
                    CheckStatus::Fail(d) => ("fail", d.clone()),
                    CheckStatus::Skip(d) => ("skip", d.clone()),
                };
                serde_json::json!({ "check": c.name, "status": status, "detail": detail })
            })
            .collect();
        println!("{}", serde_json::json!({ "checks": items }));
        return;
    }
    for c in checks {
        match &c.status {
            CheckStatus::Pass => println!("PASS  {}", c.name),
            CheckStatus::Fail(d) => println!("FAIL  {}: {d}", c.name),
            CheckStatus::Skip(d) => println!("SKIP  {}: {d}", c.name),
        }
    }
}

fn exit_code(checks: &[Check]) -> ExitCode {
    if checks
        .iter()
        .any(|c| matches!(c.status, CheckStatus::Fail(_)))
    {
        ExitCode::from(1)
    } else if checks
        .iter()
        .any(|c| matches!(c.status, CheckStatus::Skip(_)))
    {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

/// podman on PATH and the runtime image present (the confinement backend
/// refuses to auto-pull). Either missing → an honest skip, not a failure.
async fn podman_readiness() -> Result<(), String> {
    let has_podman = tokio::process::Command::new("podman")
        .arg("--version")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !has_podman {
        return Err("podman is not available on PATH".to_string());
    }
    let has_image = tokio::process::Command::new("podman")
        .args(["image", "exists", RUNTIME_IMAGE])
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);
    if !has_image {
        return Err(format!("runtime image '{RUNTIME_IMAGE}' is not built"));
    }
    Ok(())
}

// ---- Embedded fixtures & mission types (self-contained; no repo files needed) ----

const ADD_CARGO: &str = "[package]\nname = \"selftest-add\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n";
/// `add` subtracts — the test fails until a worker fixes it. Used by check (1)
/// to prove a real writable worker's fix lands and is judged.
const BROKEN_ADD_LIB: &str = "\
pub fn add(a: i64, b: i64) -> i64 { a - b }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn adds() { assert_eq!(add(2, 3), 5); }
}
";
/// The known-good fix the scripted worker writes.
const FIXED_ADD_LIB: &str = "\
pub fn add(a: i64, b: i64) -> i64 { a + b }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn adds() { assert_eq!(add(2, 3), 5); }
}
";

// The oracle-honesty fixture reuses interval-bug (a real off-by-one, single
// source of truth).
const BROKEN_CARGO: &str = include_str!("../tests/fixtures/eval/interval-bug/Cargo.toml");
const BROKEN_LIB: &str = include_str!("../tests/fixtures/eval/interval-bug/src/lib.rs");

/// A `mission.toml` for a self-test mission type: `stop = verified`, running
/// in the runtime image the readiness probe already gated on.
fn manifest_toml(name: &str) -> String {
    format!("[mission-type]\nname = \"{name}\"\nstop = \"verified\"\nimage = \"{RUNTIME_IMAGE}\"\n")
}
const IMPLEMENTER_ROLE: &str = "\
---
output: produces-artifact
runtime: codex
---
Self-test worker.
";
const CARGO_TEST_ORACLE: &str = "#!/bin/sh\nset -e\ncd /workspace\nexec cargo test --locked\n";
const PREPARED_CARGO_TEST_ORACLE: &str =
    "#!/bin/sh\nset -e\ntest \"$(cat /inputs/fixture/sentinel)\" = prepared\ncd /workspace\nexec cargo test --locked\n";
const PREPARE_FIXTURE_INPUT: &str =
    "#!/bin/sh\nset -e\nprintf prepared > \"$LIONCLAW_OUTPUT/sentinel\"\n";

// A verdict role that illegally requests secrets — the loader must refuse it.
// (A judge can't be declared *writable* in a mission type — workspace access is
// derived from output — so an over-privileged judge is a secrets-requesting
// one.)
const SECRETS_JUDGE_REVIEWER: &str = "\
---
output: emits-verdict
secrets: true
---
A verdict role illegally requesting secrets — the loader must refuse it.
";

/// Write a mission-type dir: mission.toml + roles/implementer.md + oracles/cargo-test.
fn materialize_sw_mission_type(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("roles"))?;
    std::fs::create_dir_all(root.join("oracles"))?;
    std::fs::write(root.join("mission.toml"), manifest_toml("selftest"))?;
    std::fs::write(root.join("playbook.md"), "# Self-test\n")?;
    std::fs::write(root.join("roles/implementer.md"), IMPLEMENTER_ROLE)?;
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, CARGO_TEST_ORACLE)?;
    workspace::make_executable(&oracle)?;
    Ok(())
}

fn materialize_input_mission_type(root: &Path) -> Result<()> {
    materialize_sw_mission_type(root)?;
    std::fs::write(
        root.join("mission.toml"),
        format!(
            "{}\n[[inputs]]\nname = \"fixture\"\nnetwork = true\nkey-files = [\"Cargo.lock\"]\n",
            manifest_toml("input-selftest")
        ),
    )?;
    std::fs::create_dir_all(root.join("inputs"))?;
    let input = root.join("inputs/fixture");
    std::fs::write(&input, PREPARE_FIXTURE_INPUT)?;
    workspace::make_executable(&input)?;
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, PREPARED_CARGO_TEST_ORACLE)?;
    workspace::make_executable(&oracle)?;
    Ok(())
}

fn materialize_secrets_judge_mission_type(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("roles"))?;
    std::fs::write(root.join("mission.toml"), manifest_toml("secrets-judge"))?;
    std::fs::write(root.join("playbook.md"), "# Secrets judge\n")?;
    std::fs::write(root.join("roles/reviewer.md"), SECRETS_JUDGE_REVIEWER)?;
    Ok(())
}

/// Materialize a Cargo crate into a fresh git repo (lockfile generated so the
/// oracle's `cargo test --locked` works offline). Returns the HEAD sha.
async fn materialize_repo(root: &Path, cargo_toml: &str, lib_rs: &str) -> Result<String> {
    std::fs::create_dir_all(root.join("src"))?;
    std::fs::write(root.join("Cargo.toml"), cargo_toml)?;
    std::fs::write(root.join("src/lib.rs"), lib_rs)?;
    // A trivial lock (no deps); ignore failure — an empty crate still locks.
    let _ = tokio::process::Command::new("cargo")
        .arg("generate-lockfile")
        .current_dir(root)
        .output()
        .await;
    git(root, &["init", "-q"]).await?;
    git(root, &["add", "-A"]).await?;
    git(
        root,
        &[
            "-c",
            "user.name=selftest",
            "-c",
            "user.email=selftest@local",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "self-test fixture",
        ],
    )
    .await?;
    let head = git(root, &["rev-parse", "HEAD"]).await?;
    Ok(head.trim().to_string())
}

async fn git(root: &Path, args: &[&str]) -> Result<String> {
    let out = tokio::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .await
        .with_context(|| format!("git {args:?}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The single oracle-bound assertion + its one covering work task.
fn oracle_plan() -> Plan {
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("TESTS-GREEN").expect("requirement id"),
            kind: RequirementKind::Validation,
            prose: "the project test suite passes".to_string(),
            disposition: RequirementDisposition::Covered {
                assertion_ids: vec![AssertionId::new("TESTS-PASS").expect("assertion id")],
            },
        }],
        assertions: vec![Assertion {
            id: AssertionId::new("TESTS-PASS").expect("assertion id"),
            prose: "cargo test passes at the judged commit".to_string(),
            oracle: Some(OracleName::new("cargo-test").expect("oracle name")),
        }],
        tasks: vec![Task {
            id: TaskId::new("build").expect("task id"),
            kind: TaskKind::Work,
            body: "produce the change".to_string(),
            targets: vec![AssertionId::new("TESTS-PASS").expect("assertion id")],
            role: Some(RoleName::new("implementer").expect("role name")),
            depends_on: Vec::new(),
        }],
    }
}

fn proposal(base_revision: u32, plan: Plan) -> PlanProposal {
    PlanProposal {
        base_revision,
        plan,
    }
}

async fn approve_plan(engine: &Engine, mission_id: &MissionId) -> Result<()> {
    engine
        .decide(
            mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "self-test approves the plan",
        )
        .await?;
    Ok(())
}

/// A real writable worker without a model: it checks out the repo, writes a
/// known-good fix and commits it **inside a real read-write container**, and
/// returns the captured commit — proving the allow-side of confinement (writes
/// land) and that the engine records the commit. Reuses the same checkout/capture
/// helpers as the production `OciRoleRunner`.
struct ScriptedRoleRunner {
    fixed_lib: &'static str,
}

#[async_trait]
impl RoleRunner for ScriptedRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        self.run_inner(request).await.map_err(|e| RoleRunFailure {
            kind: RunErrorKind::Launch,
            detail: format!("{e:#}"),
            final_response: String::new(),
        })
    }
}

impl ScriptedRoleRunner {
    async fn run_inner(&self, request: RoleRunRequest) -> Result<RoleRunOutcome> {
        let attempt_tag = request.effect_id.as_str();
        let dest = request.state_dir.join("selftest-work").join(attempt_tag);
        workspace::create_checkout(&request.workspace_dir, &dest, &request.base_sha).await?;
        // The produces-artifact role compiles to a writable workspace.
        let authority = compile_authority(&request.role, &AuthorityCeiling::default())
            .map_err(|e| anyhow::anyhow!("authority refused to compile: {e}"))?;
        // Write the fix and commit, in a real read-write container. Commit
        // identity + gpgsign=off come from the checkout's git config.
        let script = format!(
            "set -e; cd /workspace; cat > src/lib.rs <<'LIONCLAW_SELFTEST_EOF'\n{}LIONCLAW_SELFTEST_EOF\ngit add -A; git commit -q -m 'self-test scripted fix'",
            self.fixed_lib
        );
        let output = run_confined_sh(&authority, &dest, &[], &script).await?;
        if output.exit_code != Some(0) {
            anyhow::bail!(
                "scripted writer failed (exit {:?}): {}",
                output.exit_code,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let head = workspace::capture_worker_result(
            &request.workspace_dir,
            &dest,
            request.mission_id.as_str(),
            &request.effect_id,
        )
        .await?;
        workspace::remove_dir(&dest).await?;
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("self-test scripted fix"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha.clone(),
                head_sha: head,
            }),
            runtime_configuration: Default::default(),
            final_response: "self-test scripted fix".to_string(),
        })
    }
}

/// Run a shell command in a real container under a compiled role plan. Shared
/// by the writable scripted worker and the read-only EROFS probe.
async fn run_confined_sh(
    authority: &CompiledAuthority,
    workspace_source: &Path,
    judged_roots: &[std::path::PathBuf],
    script: &str,
) -> Result<ExecutionOutput> {
    // No mission type in this probe, so set the image directly.
    let mut profile = RuntimeProfiles::built_in()?.get("codex")?;
    profile.confinement.oci_mut().image = Some(RUNTIME_IMAGE.to_string());
    let compiled = compile_role_plan(RolePlanRequest {
        authority,
        runtime_id: "codex".to_string(),
        confinement: profile.confinement.clone(),
        mounts: MissionMounts {
            workspace: workspace_source.to_path_buf(),
            extras: Vec::new(),
        },
        judged_roots,
        environment: vec![("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string())],
        hard_timeout: Duration::from_secs(120),
    })
    .map_err(|e| anyhow::anyhow!("plan refused to compile: {e}"))?;
    let mut executor = MissionProgramExecutor::new(
        compiled.plan().clone(),
        RuntimeAuthRegistry::empty(),
        &EffectId::for_parts(&["selftest", "confined-command"]),
    );
    executor
        .execute_captured(RuntimeProgramSpec {
            executable: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            environment: Vec::new(),
            stdin: String::new(),
            auth: None,
        })
        .await
        .context("running the confined command")
}

/// Build an engine over `repo` with the given worker + the counting real oracle.
async fn build_engine(
    repo: &Path,
    type_dir: &Path,
    role_runner: Arc<dyn RoleRunner>,
    oracle_count: Arc<AtomicUsize>,
) -> Result<Engine> {
    let mission_type = load_mission_type(type_dir, &AuthorityCeiling::default())
        .map_err(|e| anyhow::anyhow!("mission type load failed: {e}"))?;
    let store = MissionStore::open(repo).await?;
    workspace::ensure_excluded(repo).await?;
    let mut profile = RuntimeProfiles::built_in()?.get("codex")?;
    let image = mission_type.image.clone();
    profile.confinement.oci_mut().image = Some(image.clone());
    let effect_cleaner = Arc::new(crate::effect_cleanup::LocalEffectCleaner::new(
        profile.confinement.oci().engine.clone(),
    ));
    Ok(Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        image,
        EngineServices::new(
            role_runner,
            Arc::new(CountingOracleRunner {
                inner: OciOracleRunner::new(profile),
                count: oracle_count,
            }),
            effect_cleaner,
            Arc::new(SystemClock),
        ),
    ))
}

// ---- The six checks ----

/// (1) A real writable worker fixes a broken tree in a container; its commit
/// lands and the engine records it (`current_sha` advances); the real oracle
/// then judges the fix and the mission reaches VERIFIED. A fresh engine on the
/// same DB resumes and does NOT re-run the oracle (counter stays 1).
async fn check_happy_writer_and_resume() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_sw_mission_type(type_dir.path())?;
    let base = materialize_repo(repo.path(), ADD_CARGO, BROKEN_ADD_LIB).await?;
    let count = Arc::new(AtomicUsize::new(0));
    let worker = Arc::new(ScriptedRoleRunner {
        fixed_lib: FIXED_ADD_LIB,
    });

    let mission_id = {
        let engine =
            build_engine(repo.path(), type_dir.path(), worker.clone(), count.clone()).await?;
        let id = engine
            .create_mission(
                &repo.path().to_string_lossy(),
                "self-test writable worker",
                &base,
                MissionConfig {
                    ..Default::default()
                },
            )
            .await?;
        engine
            .propose_plan(&id, proposal(0, oracle_plan()))
            .await
            .map_err(|e| anyhow::anyhow!("plan proposal rejected: {e}"))?;
        approve_plan(&engine, &id).await?;
        assert_verified(&engine, &id).await?;
        // The worker's writes landed and the engine recorded the commit.
        let state = engine.load_state(&id).await?;
        if state.current_sha == base {
            anyhow::bail!("worker ran but no commit was recorded (current_sha unchanged)");
        }
        id
    };

    // Resume from disk with a fresh engine sharing the same oracle counter.
    let engine2 = build_engine(repo.path(), type_dir.path(), worker, count.clone()).await?;
    assert_verified(&engine2, &mission_id).await?;

    let ran = count.load(Ordering::SeqCst);
    if ran != 1 {
        anyhow::bail!("oracle ran {ran} times across two invocations; expected exactly 1");
    }
    let completions = engine2
        .store()
        .load(&mission_id)
        .await?
        .iter()
        .filter(|e| matches!(e.event, MissionEvent::OracleRunCompleted { .. }))
        .count();
    if completions != 1 {
        anyhow::bail!("log has {completions} oracle completions; expected 1");
    }
    Ok(())
}

async fn check_prepared_input() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_input_mission_type(type_dir.path())?;
    let base = materialize_repo(repo.path(), ADD_CARGO, FIXED_ADD_LIB).await?;
    let count = Arc::new(AtomicUsize::new(0));
    let engine = build_engine(
        repo.path(),
        type_dir.path(),
        Arc::new(NoopRoleRunner),
        count,
    )
    .await?;
    let id = engine
        .create_mission(
            &repo.path().to_string_lossy(),
            "self-test prepared input",
            &base,
            MissionConfig {
                ..Default::default()
            },
        )
        .await?;
    engine
        .propose_plan(&id, proposal(0, oracle_plan()))
        .await
        .map_err(|error| anyhow::anyhow!("plan proposal rejected: {error}"))?;
    approve_plan(&engine, &id).await?;
    assert_verified(&engine, &id).await?;

    let state = engine.load_state(&id).await?;
    let verdict = state
        .contract
        .values()
        .next()
        .and_then(|assertion| assertion.last_authoritative.as_ref())
        .context("prepared-input oracle verdict missing")?;
    let input = verdict
        .prepared_inputs()
        .first()
        .context("oracle receipt omitted its prepared input")?;
    if input.name.as_str() != "fixture" {
        anyhow::bail!("unexpected prepared input '{}'", input.name);
    }
    let cache = repo
        .path()
        .join(".lionclaw/inputs/sha256")
        .join(&input.digest[..2])
        .join(&input.digest[2..4])
        .join(&input.digest);
    if !cache.join("sentinel").is_file() {
        anyhow::bail!(
            "prepared input was not atomically published at '{}'",
            cache.display()
        );
    }
    Ok(())
}

async fn assert_verified(engine: &Engine, id: &MissionId) -> Result<()> {
    let outcome = engine.advance(id).await?;
    let state = engine.load_state(id).await?;
    match state.phase {
        MissionPhase::Done {
            finish: FinishClass::Verified,
        } => Ok(()),
        other => anyhow::bail!("expected verified finish, got {other:?} (outcome {outcome:?})"),
    }
}

/// (2) The real cargo-test oracle on a genuinely-broken tree records a valid
/// authoritative failure and parks on the repair path. The noop worker leaves
/// the tree broken so the oracle is what decides.
async fn check_oracle_honesty() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_sw_mission_type(type_dir.path())?;
    let base = materialize_repo(repo.path(), BROKEN_CARGO, BROKEN_LIB).await?;
    let count = Arc::new(AtomicUsize::new(0));
    let engine = build_engine(
        repo.path(),
        type_dir.path(),
        Arc::new(NoopRoleRunner),
        count,
    )
    .await?;
    let id = engine
        .create_mission(
            &repo.path().to_string_lossy(),
            "self-test oracle honesty",
            &base,
            MissionConfig {
                ..Default::default()
            },
        )
        .await?;
    engine
        .propose_plan(&id, proposal(0, oracle_plan()))
        .await
        .map_err(|e| anyhow::anyhow!("plan proposal rejected: {e}"))?;
    approve_plan(&engine, &id).await?;
    let outcome = engine.advance(&id).await?;
    let state = engine.load_state(&id).await?;
    let verdict = state
        .contract
        .values()
        .next()
        .and_then(|assertion| assertion.last_authoritative.as_ref())
        .context("failing oracle produced no authoritative verdict")?;
    if verdict.passed() {
        anyhow::bail!("the genuinely broken tree received an authoritative pass");
    }
    if outcome.disposition != MissionDisposition::Parked
        || !state
            .open_attention
            .contains_key("oracle_verdict_failed:cargo-test")
    {
        anyhow::bail!("failing oracle did not park on its repair path: {outcome:?}");
    }
    Ok(())
}

/// (3) A mission type declaring an over-privileged judge refuses to load with a
/// typed moat violation — so the mission never starts (no event log).
async fn check_moat() -> Result<()> {
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_secrets_judge_mission_type(type_dir.path())?;
    match load_mission_type(type_dir.path(), &AuthorityCeiling::default()) {
        Ok(_) => anyhow::bail!("over-privileged judge mission type loaded (moat breached)"),
        Err(MissionTypeError::Moat { .. }) => Ok(()),
        Err(other) => anyhow::bail!("mission type refused, but not by the moat: {other}"),
    }
}

/// (5) Re-planning through the shipped binary: a revision supersedes a live
/// task and strengthens the contract atomically (revision bumps, the old task
/// becomes a `Superseded` tombstone), while a contract-weakening revision is
/// refused. Pure — no agent turn, no oracle run — so it always runs.
async fn check_replanning() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_sw_mission_type(type_dir.path())?;
    let base = materialize_repo(repo.path(), ADD_CARGO, FIXED_ADD_LIB).await?;
    let engine = build_engine(
        repo.path(),
        type_dir.path(),
        Arc::new(NoopRoleRunner),
        Arc::new(AtomicUsize::new(0)),
    )
    .await?;
    let mission_id = engine
        .create_mission(
            repo.path().to_str().context("utf8 repo path")?,
            "re-planning self-test",
            &base,
            MissionConfig {
                ..Default::default()
            },
        )
        .await?;
    engine
        .propose_plan(&mission_id, proposal(0, oracle_plan()))
        .await
        .map_err(|e| anyhow::anyhow!("plan proposal rejected: {e}"))?;
    approve_plan(&engine, &mission_id).await?;

    // Replace the sole coverer with a new-id task in one complete revision.
    let mut next = oracle_plan();
    next.tasks = vec![Task {
        id: TaskId::new("build2").expect("task id"),
        kind: TaskKind::Work,
        body: "produce the change again".to_string(),
        targets: vec![AssertionId::new("TESTS-PASS").expect("assertion id")],
        role: Some(RoleName::new("implementer").expect("role name")),
        depends_on: Vec::new(),
    }];
    engine
        .propose_plan(&mission_id, proposal(1, next))
        .await
        .map_err(|e| anyhow::anyhow!("revision rejected: {e}"))?;
    approve_plan(&engine, &mission_id).await?;

    let state = engine.load_state(&mission_id).await?;
    if state.revision != 2 {
        anyhow::bail!("expected revision 2 after revision, got {}", state.revision);
    }
    if state.tasks[&TaskId::new("build").unwrap()].status != TaskStatus::Superseded {
        anyhow::bail!("superseded task is not a tombstone");
    }
    if state
        .plan
        .as_ref()
        .is_some_and(|p| p.tasks.iter().any(|t| t.id.as_str() == "build"))
    {
        anyhow::bail!("superseded task still in the live plan");
    }

    // A complete revision still cannot weaken the contract by rebinding an
    // existing assertion to a different oracle.
    let mut weaken = state.plan.clone().expect("accepted plan");
    weaken.assertions[0].oracle = Some(OracleName::new("cargo-clippy").expect("oracle name"));
    match engine.propose_plan(&mission_id, proposal(2, weaken)).await {
        Err(ProposeError::Rejected(ProposalError::AssertionWeakened { .. })) => Ok(()),
        Ok(()) => anyhow::bail!("contract-weakening revision was accepted"),
        Err(other) => anyhow::bail!("weakening refused for the wrong reason: {other}"),
    }
}

/// (6) Terminal review gates closure: a mission type declaring a closing
/// review does not close on a blocking verdict — it parks for a human, and
/// only an explicit `accept` (acknowledge) lets it finish, with the
/// acknowledgment on record. Also: the loader refuses `stop = "reviewed"`
/// without the declaration (that bar is *defined* by the review). Pure — no
/// agent turn, no oracle run — so it always runs.
async fn check_terminal_review() -> Result<()> {
    // Loader gate: an agent-graded bar without an independent closing review
    // must refuse to load.
    let bar_dir = tempfile::tempdir().context("tempdir")?;
    materialize_sw_mission_type(bar_dir.path())?;
    std::fs::write(
        bar_dir.path().join("mission.toml"),
        format!("[mission-type]\nname = \"reviewed-bare\"\nstop = \"reviewed\"\nimage = \"{RUNTIME_IMAGE}\"\n"),
    )?;
    match load_mission_type(bar_dir.path(), &AuthorityCeiling::default()) {
        Ok(_) => anyhow::bail!("stop=reviewed loaded without [terminal-review]"),
        Err(MissionTypeError::Manifest(detail))
            if detail.contains("requires [terminal-review]") => {}
        Err(other) => anyhow::bail!("refused, but not for the missing review: {other}"),
    }

    // Closure gate: blocking verdict → park → acknowledge → done.
    let type_dir = tempfile::tempdir().context("tempdir")?;
    materialize_sw_mission_type(type_dir.path())?;
    std::fs::write(
        type_dir.path().join("mission.toml"),
        format!(
            "[mission-type]\nname = \"selftest\"\nstop = \"verified\"\nimage = \"{RUNTIME_IMAGE}\"\n\
             \n[terminal-review]\nrole = \"gap-reviewer\"\n"
        ),
    )?;
    std::fs::write(
        type_dir.path().join("roles/gap-reviewer.md"),
        "---\noutput: emits-gap-verdict\nruntime: codex\n---\nSelf-test gap reviewer.\n",
    )?;
    let mission_type = load_mission_type(type_dir.path(), &AuthorityCeiling::default())
        .map_err(|e| anyhow::anyhow!("review mission type load failed: {e}"))?;

    let repo = tempfile::tempdir().context("tempdir")?;
    let base = materialize_repo(repo.path(), ADD_CARGO, FIXED_ADD_LIB).await?;
    let config = MissionConfig {
        terminal_review: mission_type.terminal_review.clone(),
        ..Default::default()
    };
    let engine = Engine::new(
        MissionStore::open(repo.path()).await?,
        mission_type,
        "codex".to_string(),
        RUNTIME_IMAGE.to_string(),
        EngineServices::new(
            Arc::new(ReviewParkRoleRunner),
            Arc::new(FixedOracleRunner(0)),
            Arc::new(crate::effect_cleanup::LocalEffectCleaner::new(
                "podman".to_string(),
            )),
            Arc::new(SystemClock),
        ),
    );
    let mission_id = engine
        .create_mission(
            repo.path().to_str().context("utf8 repo path")?,
            "terminal-review self-test",
            &base,
            config,
        )
        .await?;
    engine
        .propose_plan(&mission_id, proposal(0, oracle_plan()))
        .await
        .map_err(|e| anyhow::anyhow!("plan proposal rejected: {e}"))?;
    approve_plan(&engine, &mission_id).await?;

    engine.advance(&mission_id).await?;
    let parked = engine.load_state(&mission_id).await?;
    if !matches!(parked.phase, MissionPhase::AttentionNeeded) {
        anyhow::bail!(
            "a blocking review verdict did not park the mission (phase {:?})",
            parked.phase
        );
    }
    if !parked
        .open_attention
        .contains_key("terminal_review_gaps:mission")
    {
        anyhow::bail!("park is not the terminal-review gaps item");
    }

    engine
        .decide(
            &mission_id,
            "terminal_review_gaps:mission",
            DecisionAction::Accept,
            "self-test acknowledges the gap",
        )
        .await?;
    engine.advance(&mission_id).await?;
    let done = engine.load_state(&mission_id).await?;
    if !matches!(
        done.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    ) {
        anyhow::bail!(
            "acknowledged mission did not close verified: {:?}",
            done.phase
        );
    }
    match &done.terminal_review.accepted {
        Some(a) if a.kind == ReviewAcceptanceKind::AcknowledgedGaps && a.judged_sha == base => {
            Ok(())
        }
        other => anyhow::bail!("acknowledgment not on record at the judged sha: {other:?}"),
    }
}

/// (4) A read-only role gets complete Git inspection while writes to both the
/// working tree and repository metadata are denied by the container.
async fn check_confinement_erofs() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    materialize_repo(repo.path(), ADD_CARGO, FIXED_ADD_LIB).await?;
    let checkout = tempfile::tempdir().context("tempdir")?;
    let judged = checkout.path().join("tree");
    workspace::create_checkout(repo.path(), &judged, "HEAD").await?;

    let authority = oracle_authority("erofs-probe");
    let output = run_confined_sh(
        &authority,
        &judged,
        std::slice::from_ref(&judged),
        "set -e; \
         git rev-parse --is-inside-work-tree >/dev/null; \
         git status --porcelain; \
         git log -1 --format=%H >/dev/null; \
         git blame -L 1,1 Cargo.toml >/dev/null; \
         echo GIT_OK; \
         if echo poison > /workspace/.git/HEAD; then echo GIT_WROTE; else echo GIT_DENIED; fi; \
         if echo probe > /workspace/PROBE; then echo WROTE; else echo DENIED; fi",
    )
    .await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_lowercase();
    let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
    if !stdout.contains("git_ok") {
        anyhow::bail!("Git inspection did not complete in the read-only checkout: {stdout:?}");
    }
    if stdout.contains("git_wrote") || !stdout.contains("git_denied") {
        anyhow::bail!("read-only Git metadata was writable: {stdout:?}");
    }
    if stdout.lines().any(|line| line == "wrote") || judged.join("PROBE").exists() {
        anyhow::bail!("the read-only workspace was writable");
    }
    // Require the script's own DENIED marker: it proves /bin/sh actually ran
    // inside the container and hit the `||` branch after the write failed —
    // not that podman merely failed to launch (which would also lack "wrote").
    if !stdout.contains("denied") {
        anyhow::bail!(
            "probe did not reach the DENIED branch (container may not have run): stdout={stdout:?} stderr={stderr:?}"
        );
    }
    // And corroborate the reason is the read-only mount.
    if !(stderr.contains("read-only file system") || stderr.contains("permission denied")) {
        anyhow::bail!("write was denied but not by the read-only mount: {stderr:?}");
    }
    Ok(())
}

/// (7) Mission skills are mounted read-only at the runtime's native skill path.
async fn check_runtime_skill_mount() -> Result<()> {
    let workspace = tempfile::tempdir().context("tempdir")?;
    let runtime_home = tempfile::tempdir().context("tempdir")?;
    let skill = tempfile::tempdir().context("tempdir")?;
    std::fs::write(skill.path().join("SKILL.md"), "mission-skill-probe\n")
        .context("writing skill probe")?;

    let mut profile = RuntimeProfiles::built_in()?.get("codex")?;
    profile.confinement.oci_mut().image = Some(RUNTIME_IMAGE.to_string());
    let mut extras = vec![MountSpec {
        source: runtime_home.path().to_path_buf(),
        target: lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET.to_string(),
        access: MountAccess::ReadWrite,
    }];
    extras.extend(crate::runner::prepare_skill_mounts(
        runtime_home.path(),
        &[crate::mission_type::SkillPackage {
            name: "mission-probe".to_string(),
            root: skill.path().to_path_buf(),
            description: "mission skill probe".to_string(),
        }],
        profile.skills_dir.as_ref(),
    )?);
    let authority = oracle_authority("skill-mount-probe");
    let judged_roots = [workspace.path().to_path_buf()];
    let compiled = compile_role_plan(RolePlanRequest {
        authority: &authority,
        runtime_id: "skill-mount-probe".to_string(),
        confinement: profile.confinement,
        mounts: MissionMounts {
            workspace: workspace.path().to_path_buf(),
            extras,
        },
        judged_roots: &judged_roots,
        environment: vec![(
            "HOME".to_string(),
            lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET.to_string(),
        )],
        hard_timeout: Duration::from_secs(120),
    })
    .map_err(|err| anyhow::anyhow!("plan refused to compile: {err}"))?;
    let mut executor = MissionProgramExecutor::new(
        compiled.plan().clone(),
        RuntimeAuthRegistry::empty(),
        &EffectId::for_parts(&["selftest", "readonly-inspection"]),
    );
    let output = executor
        .execute_captured(RuntimeProgramSpec {
            executable: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "test ! -L \"$HOME/.agents/skills/mission-probe\" && test \"$(cat \"$HOME/.agents/skills/mission-probe/SKILL.md\")\" = mission-skill-probe && ! printf changed > \"$HOME/.agents/skills/mission-probe/SKILL.md\""
                    .to_string(),
            ],
            environment: Vec::new(),
            stdin: String::new(),
            auth: None,
        })
        .await
        .context("running skill mount probe")?;
    if output.exit_code != Some(0) {
        anyhow::bail!(
            "skill mount probe failed (exit {:?}): {}",
            output.exit_code,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    std::fs::remove_dir_all(runtime_home.path().join(".agents"))
        .context("native skill mountpoints were not removable after the container exited")?;
    Ok(())
}
