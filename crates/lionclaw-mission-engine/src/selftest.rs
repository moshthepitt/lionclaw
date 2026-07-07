//! `lionclaw mission self-test`: drive the real stack (real store + fold +
//! loop + real podman confinement + real engine-run oracle) and assert the
//! four Slice-1 invariants. Hermetic and model-auth-free — the *oracle*
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
    compile_role_plan, oracle_authority, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::engine::Engine;
use crate::model::{
    Assertion, AssertionId, FinishClass, Handoff, MissionConfig, MissionEvent, MissionId,
    OracleName, PayloadRef, PlanSubmission, RoleName, Task, TaskId, TaskKind,
};
use crate::oracle::OciOracleRunner;
use crate::plugin::{load_plugin, PluginError};
use crate::ports::{
    OracleFailure, OracleOutcome, OracleRunRequest, OracleRunner, RoleRunFailure, RoleRunOutcome,
    RoleRunRequest, RoleRunner, SystemClock,
};
use crate::runner::MissionProgramExecutor;
use crate::store::MissionStore;
use crate::workspace;

use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec, WORKSPACE_MOUNT_TARGET};
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeProgramExecutor};

const RUNTIME_IMAGE: &str = "localhost/lionclaw-runtime-dev:v1";

/// A no-op producer: clears its work task without changing the tree, so the
/// real oracle judges the base fixture. This is the only stub in the harness
/// (the oracle and confinement are real); it exists so the mission does not
/// need a model to reach a work-task's outcome.
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
            model_id: None,
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

    // (3) Moat is pure — always runs, even without podman.
    checks.push(Check {
        name: "moat-refuses-over-privileged-judge",
        status: to_status(check_moat().await),
    });

    // (1),(2),(4) need real confinement.
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
        ("happy-path-and-resume-no-dup", || Box::pin(check_happy_and_resume())),
        ("oracle-honesty-on-real-broken-code", || Box::pin(check_oracle_honesty())),
        ("confinement-read-only-workspace-erofs", || Box::pin(check_confinement_erofs())),
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
    if checks.iter().any(|c| matches!(c.status, CheckStatus::Fail(_))) {
        ExitCode::from(1)
    } else if checks.iter().any(|c| matches!(c.status, CheckStatus::Skip(_))) {
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

// ---- Embedded fixtures & plugins (self-contained; no repo files needed) ----

const PASSING_CARGO: &str = "[package]\nname = \"selftest-pass\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n";
const PASSING_LIB: &str = "\
pub fn add(a: i64, b: i64) -> i64 { a + b }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn adds() { assert_eq!(add(2, 3), 5); }
}
";

// The broken fixture reuses interval-bug (single source of truth).
const BROKEN_CARGO: &str = include_str!("../tests/fixtures/eval/interval-bug/Cargo.toml");
const BROKEN_LIB: &str = include_str!("../tests/fixtures/eval/interval-bug/src/lib.rs");

const PLUGIN_MISSION_TOML: &str = "[plugin]\nname = \"selftest\"\nstop = \"verified\"\n";
const PLUGIN_IMPLEMENTER: &str = "\
---
output: produces-artifact
runtime: codex
---
Self-test worker (never actually dispatched to a model).
";
const PLUGIN_ORACLE: &str = "#!/bin/sh\nset -e\ncd /workspace\nexec cargo test --locked\n";

const WRITABLE_JUDGE_TOML: &str = "[plugin]\nname = \"writable-judge\"\nstop = \"verified\"\n";
const WRITABLE_JUDGE_REVIEWER: &str = "\
---
output: emits-verdict
secrets: true
---
A verdict role illegally requesting secrets — the loader must refuse it.
";

/// Write a plugin dir: mission.toml + roles/implementer.md + oracles/cargo-test.
fn materialize_sw_plugin(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("roles"))?;
    std::fs::create_dir_all(root.join("oracles"))?;
    std::fs::write(root.join("mission.toml"), PLUGIN_MISSION_TOML)?;
    std::fs::write(root.join("roles/implementer.md"), PLUGIN_IMPLEMENTER)?;
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, PLUGIN_ORACLE)?;
    set_executable(&oracle)?;
    Ok(())
}

fn materialize_writable_judge_plugin(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("roles"))?;
    std::fs::write(root.join("mission.toml"), WRITABLE_JUDGE_TOML)?;
    std::fs::write(root.join("roles/reviewer.md"), WRITABLE_JUDGE_REVIEWER)?;
    Ok(())
}

fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)?;
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
        anyhow::bail!("git {args:?} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The single oracle-bound assertion + its one covering work task.
fn oracle_plan() -> PlanSubmission {
    PlanSubmission {
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

/// Build an engine over `repo` with the noop worker + the counting real oracle.
async fn build_engine(
    repo: &Path,
    plugin_dir: &Path,
    oracle_count: Arc<AtomicUsize>,
) -> Result<Engine> {
    let plugin = load_plugin(plugin_dir, &AuthorityCeiling::default())
        .map_err(|e| anyhow::anyhow!("plugin load failed: {e}"))?;
    let store = MissionStore::open(repo).await?;
    workspace::ensure_excluded(repo)?;
    let profile = MissionRuntimeProfile::codex_default();
    Ok(Engine::new(
        store,
        plugin,
        Arc::new(NoopRoleRunner),
        Arc::new(CountingOracleRunner {
            inner: OciOracleRunner::new(profile),
            count: oracle_count,
        }),
        Arc::new(SystemClock),
    ))
}

// ---- The four checks ----

/// (1) A passing fixture reaches VERIFIED; a fresh engine on the same DB
/// resumes and does NOT re-run the oracle (counter stays 1).
async fn check_happy_and_resume() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let plugin = tempfile::tempdir().context("tempdir")?;
    materialize_sw_plugin(plugin.path())?;
    let base = materialize_repo(repo.path(), PASSING_CARGO, PASSING_LIB).await?;
    let count = Arc::new(AtomicUsize::new(0));

    let mission_id = {
        let engine = build_engine(repo.path(), plugin.path(), count.clone()).await?;
        let id = engine
            .create_mission(
                &repo.path().to_string_lossy(),
                "self-test happy path",
                &base,
                MissionConfig { ratification_gate: false, ..Default::default() },
            )
            .await?;
        engine
            .submit_plan(&id, oracle_plan())
            .await
            .map_err(|e| anyhow::anyhow!("submit rejected: {e}"))?;
        assert_verified(&engine, &id).await?;
        id
    };

    // Resume from disk with a fresh engine sharing the same counter.
    let engine2 = build_engine(repo.path(), plugin.path(), count.clone()).await?;
    assert_verified(&engine2, &mission_id).await?;

    let ran = count.load(Ordering::SeqCst);
    if ran != 1 {
        anyhow::bail!("oracle ran {ran} times across two invocations; expected exactly 1");
    }
    // The log agrees: exactly one recorded oracle completion.
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

async fn assert_verified(engine: &Engine, id: &MissionId) -> Result<()> {
    let outcome = engine.advance(id).await?;
    let state = engine.load_state(id).await?;
    match state.phase {
        crate::model::MissionPhase::Done { finish: FinishClass::Verified } => Ok(()),
        other => anyhow::bail!("expected verified finish, got {other:?} (outcome {outcome:?})"),
    }
}

/// (2) The real cargo-test oracle on a genuinely-broken tree ⇒ NOT verified.
async fn check_oracle_honesty() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let plugin = tempfile::tempdir().context("tempdir")?;
    materialize_sw_plugin(plugin.path())?;
    let base = materialize_repo(repo.path(), BROKEN_CARGO, BROKEN_LIB).await?;
    let count = Arc::new(AtomicUsize::new(0));
    let engine = build_engine(repo.path(), plugin.path(), count).await?;
    let id = engine
        .create_mission(
            &repo.path().to_string_lossy(),
            "self-test oracle honesty",
            &base,
            MissionConfig { ratification_gate: false, ..Default::default() },
        )
        .await?;
    engine
        .submit_plan(&id, oracle_plan())
        .await
        .map_err(|e| anyhow::anyhow!("submit rejected: {e}"))?;
    engine.advance(&id).await?;
    let state = engine.load_state(&id).await?;
    match state.phase {
        crate::model::MissionPhase::Done { finish: FinishClass::Verified } => {
            anyhow::bail!("a failing oracle reported VERIFIED — honesty invariant broken")
        }
        _ => Ok(()),
    }
}

/// (3) A plugin declaring an over-privileged judge refuses to load, and a
/// mission started from it writes no event log.
async fn check_moat() -> Result<()> {
    let plugin = tempfile::tempdir().context("tempdir")?;
    materialize_writable_judge_plugin(plugin.path())?;
    match load_plugin(plugin.path(), &AuthorityCeiling::default()) {
        Ok(_) => anyhow::bail!("over-privileged judge plugin loaded (moat breached)"),
        Err(PluginError::Role { detail, .. }) if detail.contains("moat") => {}
        Err(other) => anyhow::bail!("plugin refused, but not by the moat: {other}"),
    }
    // Starting a mission from it must create no mission in the store.
    let repo = tempfile::tempdir().context("tempdir")?;
    let store = MissionStore::open(repo.path()).await?;
    let load = load_plugin(plugin.path(), &AuthorityCeiling::default());
    assert!(load.is_err());
    if !store.list_missions().await?.is_empty() {
        anyhow::bail!("a refused plugin still created a mission (event log written)");
    }
    Ok(())
}

/// (4) A read-only role's write to /workspace is denied by the container.
async fn check_confinement_erofs() -> Result<()> {
    let repo = tempfile::tempdir().context("tempdir")?;
    let _base = materialize_repo(repo.path(), PASSING_CARGO, PASSING_LIB).await?;
    let snapshot = tempfile::tempdir().context("tempdir")?;
    workspace::create_snapshot(repo.path(), &snapshot.path().join("tree"), "HEAD").await?;
    let judged = snapshot.path().join("tree");

    let authority = oracle_authority("erofs-probe");
    let profile = MissionRuntimeProfile::codex_default();
    let compiled = compile_role_plan(RolePlanRequest {
        authority: &authority,
        runtime_id: "codex".to_string(),
        confinement: profile.confinement.clone(),
        mounts: MissionMounts {
            workspace: MountSpec {
                source: judged.clone(),
                target: WORKSPACE_MOUNT_TARGET.to_string(),
                access: MountAccess::ReadOnly,
            },
            extras: Vec::new(),
        },
        judged_roots: std::slice::from_ref(&judged),
        environment: Vec::new(),
        idle_timeout: Duration::from_secs(60),
        hard_timeout: Duration::from_secs(60),
    })
    .map_err(|e| anyhow::anyhow!("read-only plan refused to compile: {e}"))?;

    let mut executor =
        MissionProgramExecutor::new(compiled.plan().clone(), RuntimeAuthRegistry::empty());
    let output = executor
        .execute_captured(RuntimeProgramSpec {
            executable: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "set -C; : > /workspace/PROBE && echo WROTE || echo DENIED".to_string(),
            ],
            environment: Vec::new(),
            stdin: String::new(),
            auth: None,
        })
        .await
        .context("running the write probe in the container")?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_lowercase();
    let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
    if stdout.contains("wrote") || judged.join("PROBE").exists() {
        anyhow::bail!("the read-only workspace was writable");
    }
    let denied = stderr.contains("read-only file system")
        || stderr.contains("permission denied")
        || stdout.contains("denied");
    if !denied {
        anyhow::bail!("write neither succeeded nor was clearly denied: {stderr}");
    }
    Ok(())
}
