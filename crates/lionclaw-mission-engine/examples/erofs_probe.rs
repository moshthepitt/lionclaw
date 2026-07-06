//! Confinement proof: compile a read-only (verdict-role) plan and attempt a
//! write to /workspace inside the container. The container must deny it
//! (EROFS / permission denied). Usage: cargo run --example erofs_probe -- <repo> <sha>

use std::path::PathBuf;
use std::time::Duration;

use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec, WORKSPACE_MOUNT_TARGET};
use lionclaw_mission_engine::authority::{
    compile_role_plan, oracle_authority, MissionMounts, RolePlanRequest,
};
use lionclaw_mission_engine::config::MissionRuntimeProfile;
use lionclaw_mission_engine::runner::MissionProgramExecutor;
use lionclaw_mission_engine::workspace;
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeProgramExecutor};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let repo = PathBuf::from(&args[1]).canonicalize()?;
    let sha = args[2].clone();

    let snapshot = std::env::temp_dir().join("erofs-probe-snapshot");
    workspace::create_snapshot(&repo, &snapshot, &sha).await?;

    // A verdict-role authority: read-only workspace, on an enforcing rung.
    let authority = oracle_authority("erofs-probe");
    let profile = MissionRuntimeProfile::codex_default();
    let compiled = compile_role_plan(RolePlanRequest {
        authority: &authority,
        runtime_id: "codex".to_string(),
        confinement: profile.confinement.clone(),
        mounts: MissionMounts {
            workspace: MountSpec {
                source: snapshot.clone(),
                target: WORKSPACE_MOUNT_TARGET.to_string(),
                access: MountAccess::ReadOnly,
            },
            extras: Vec::new(),
        },
        judged_roots: &[snapshot.clone()],
        environment: Vec::new(),
        idle_timeout: Duration::from_secs(60),
        hard_timeout: Duration::from_secs(60),
    })?;

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
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let denied = stderr.to_lowercase().contains("read-only file system")
        || stderr.to_lowercase().contains("permission denied")
        || stdout.contains("DENIED");
    let wrote = stdout.contains("WROTE") || snapshot.join("PROBE").exists();

    println!("stdout: {}", stdout.trim());
    println!("stderr: {}", stderr.trim());
    let _ = tokio::fs::remove_dir_all(&snapshot).await;

    if wrote {
        eprintln!("FAIL: workspace was writable");
        std::process::exit(1);
    }
    if !denied {
        eprintln!("FAIL: write neither succeeded nor was clearly denied");
        std::process::exit(2);
    }
    println!("PASS: read-only workspace denied the write (EROFS/permission denied)");
    Ok(())
}
