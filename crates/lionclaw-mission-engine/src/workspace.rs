//! Git workspace isolation for confined runs.
//!
//! Linked worktrees cannot cross the container boundary (their `.git` file
//! points at the host repo by absolute path), so:
//! - **Writers** get a full `git clone --no-hardlinks` at the base commit —
//!   a self-contained repo; the user's `.git` never enters the container,
//!   and no hardlinked object inode is shared with it. The engine fetches
//!   the resulting commit back into the target repo under `refs/mission/…`
//!   (content-addressed: `git rev-parse --verify` reconciles on resume).
//! - **Judges and oracles** get a `git archive` snapshot — no `.git` at all,
//!   nothing to tamper with, byte-stable for a given commit.
//!
//! Worker output is a recorded commit, never auto-applied (roborev's
//! captured-patch discipline, adapted to content-addressed outcomes).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tokio::process::Command;

pub async fn head_sha(repo: &Path) -> Result<String> {
    let out = git(repo, &["rev-parse", "HEAD"]).await?;
    Ok(out.trim().to_string())
}

pub async fn commit_exists(repo: &Path, sha: &str) -> bool {
    git(repo, &["rev-parse", "--verify", "--quiet", &format!("{sha}^{{commit}}")])
        .await
        .is_ok()
}

/// Keep mission state out of the user's `git status` without touching
/// tracked files.
pub fn ensure_excluded(repo: &Path) -> Result<()> {
    let exclude = repo.join(".git/info/exclude");
    if let Some(parent) = exclude.parent() {
        if !parent.exists() {
            // Not a repo layout we manage (e.g. bare); skip silently.
            return Ok(());
        }
    }
    let current = std::fs::read_to_string(&exclude).unwrap_or_default();
    if !current.lines().any(|line| line.trim() == ".lionclaw/") {
        let mut updated = current;
        if !updated.is_empty() && !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str(".lionclaw/\n");
        std::fs::write(&exclude, updated).context("failed to update .git/info/exclude")?;
    }
    Ok(())
}

/// A writer's isolated clone, checked out on a mission branch at `base_sha`.
pub struct WorkerClone {
    pub dir: PathBuf,
    pub branch: String,
}

pub async fn create_worker_clone(
    repo: &Path,
    dest: &Path,
    mission_id: &str,
    attempt_tag: &str,
    base_sha: &str,
) -> Result<WorkerClone> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .context("failed to clear stale worker clone")?;
    }
    tokio::fs::create_dir_all(dest.parent().context("clone dest has no parent")?).await?;
    let repo_str = repo.to_string_lossy();
    let dest_str = dest.to_string_lossy();
    run(
        Command::new("git")
            .args(["clone", "--quiet", "--no-hardlinks", &repo_str, &dest_str]),
        "git clone",
    )
    .await?;
    let branch = format!("mission/{mission_id}/{attempt_tag}");
    git(dest, &["checkout", "--quiet", "-b", &branch, base_sha]).await?;
    // Container-visible commit identity lives in the runtime home; the
    // clone-local config is belt and braces for host-side git operations.
    git(dest, &["config", "user.name", "LionClaw Mission"]).await?;
    git(dest, &["config", "user.email", "mission@lionclaw.local"]).await?;
    // The agent commits inside the container; never require a signing key.
    git(dest, &["config", "commit.gpgsign", "false"]).await?;
    Ok(WorkerClone {
        dir: dest.to_path_buf(),
        branch,
    })
}

/// Post-run artifact capture: the tree must be committed clean; the head
/// commit is fetched back into the target repo under `refs/mission/…` so it
/// survives clone teardown.
pub async fn capture_worker_result(
    repo: &Path,
    clone: &WorkerClone,
) -> Result<Option<String>> {
    let status = git(&clone.dir, &["status", "--porcelain"]).await?;
    if !status.trim().is_empty() {
        bail!(
            "worker left uncommitted changes ({} paths)",
            status.lines().count()
        );
    }
    let head = head_sha(&clone.dir).await?;
    let refspec = format!("+refs/heads/{}:refs/{}", clone.branch, clone.branch);
    let clone_str = clone.dir.to_string_lossy();
    run(
        Command::new("git")
            .current_dir(repo)
            .args(["fetch", "--quiet", &clone_str, &refspec]),
        "git fetch from worker clone",
    )
    .await?;
    Ok(Some(head))
}

/// Materialize a read-only snapshot of `sha` (no `.git`) for judges and
/// oracles.
pub async fn create_snapshot(repo: &Path, dest: &Path, sha: &str) -> Result<()> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .context("failed to clear stale snapshot")?;
    }
    tokio::fs::create_dir_all(dest).await?;
    let tar_path = dest.with_extension("snapshot.tar");
    let tar_str = tar_path.to_string_lossy().into_owned();
    git(repo, &["archive", "--format=tar", "-o", &tar_str, sha]).await?;
    let unpack = run(
        Command::new("tar")
            .arg("-xf")
            .arg(&tar_path)
            .arg("-C")
            .arg(dest),
        "tar extract snapshot",
    )
    .await;
    let _ = tokio::fs::remove_file(&tar_path).await;
    unpack
}

pub async fn remove_dir(dir: &Path) {
    let _ = tokio::fs::remove_dir_all(dir).await;
}

async fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let out = git_bytes(repo, args).await?;
    Ok(String::from_utf8_lossy(&out).into_owned())
}

async fn git_bytes(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to spawn git {args:?}"))?;
    if !output.status.success() {
        bail!(
            "git {:?} failed in '{}': {}",
            args,
            repo.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

async fn run(command: &mut Command, label: &str) -> Result<()> {
    let output = command
        .output()
        .await
        .with_context(|| format!("failed to spawn {label}"))?;
    if !output.status.success() {
        bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}
