//! Git workspace isolation for confined runs.
//!
//! Linked worktrees cannot cross the container boundary (their `.git` file
//! points at the host repo by absolute path), so:
//! - **Writers** get a full `git clone --no-hardlinks` at the base commit —
//!   a self-contained repo; the user's `.git` never enters the container,
//!   and no hardlinked object inode is shared with it. The engine fetches
//!   the resulting commit back into the target repo under `refs/mission/…`
//!   (content-addressed: `git rev-parse --verify` reconciles on resume).
//! - **Judges and oracles** get a `checkout-index` snapshot of the exact
//!   committed tree via a throwaway index — no `.git` at all, nothing to
//!   tamper with. Deliberately NOT `git archive`: archive honors a committed
//!   `.gitattributes export-ignore`, which would let a worker hide a file
//!   (e.g. a failing test) from the very oracle judging its commit.
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
    git(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{sha}^{{commit}}"),
        ],
    )
    .await
    .is_ok()
}

/// Keep mission state out of the user's `git status` without touching tracked
/// files. Resolves the exclude file via git rather than assuming
/// `.git/info/exclude`: in a linked worktree `.git` is a file and the exclude
/// lives in the common dir, so the hard-coded path would silently miss it.
pub async fn ensure_excluded(repo: &Path) -> Result<()> {
    // git prints the path relative to `repo` (normal repo) or absolute (linked
    // worktree common dir); `join` handles both. A failure means "not a repo we
    // manage" (bare, or git absent) — skip silently.
    let Ok(rel) = git(repo, &["rev-parse", "--git-path", "info/exclude"]).await else {
        return Ok(());
    };
    let exclude = repo.join(rel.trim());
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let current = match std::fs::read_to_string(&exclude) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        // Can't read it safely (permissions, or non-UTF-8 patterns): leave it
        // untouched rather than clobber the user's excludes — the worst case is
        // `.lionclaw/` showing in `git status`.
        Err(_) => return Ok(()),
    };
    if !current.lines().any(|line| line.trim() == ".lionclaw/") {
        let mut updated = current;
        if !updated.is_empty() && !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str(".lionclaw/\n");
        std::fs::write(&exclude, updated).context("failed to update the git exclude file")?;
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
        Command::new("git").args(["clone", "--quiet", "--no-hardlinks", &repo_str, &dest_str]),
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

/// Why post-run artifact capture failed. Distinguishes the one agent-behavior
/// case (an uncommitted tree) from everything else (git infra / a moved HEAD),
/// so the runner labels the persisted failure correctly.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("worker left uncommitted changes ({0} paths)")]
    DirtyWorktree(usize),
    #[error(transparent)]
    Infra(#[from] anyhow::Error),
}

/// Post-run artifact capture: the tree must be committed clean; the head
/// commit is fetched back into the target repo under `refs/mission/…` so it
/// survives clone teardown.
pub async fn capture_worker_result(
    repo: &Path,
    clone: &WorkerClone,
) -> Result<String, CaptureError> {
    let status = git(&clone.dir, &["status", "--porcelain"]).await?;
    if !status.trim().is_empty() {
        return Err(CaptureError::DirtyWorktree(status.lines().count()));
    }
    let head = head_sha(&clone.dir).await?;
    // Fetch the clone's *HEAD commit* (not the branch tip) so the object we
    // record is the object we store — a worker that moves HEAD off its branch
    // (detached commit, `checkout -b other`, `reset`) cannot make the engine
    // record a `head_sha` it never transferred.
    let refspec = format!("+HEAD:refs/{}", clone.branch);
    let clone_str = clone.dir.to_string_lossy();
    run(
        Command::new("git")
            .current_dir(repo)
            .args(["fetch", "--quiet", &clone_str, &refspec]),
        "git fetch from worker clone",
    )
    .await?;
    // The engine observes the commit from git, never trusts the agent: verify
    // the recorded head actually landed in the target repo.
    if !commit_exists(repo, &head).await {
        return Err(CaptureError::Infra(anyhow::anyhow!(
            "worker HEAD {head} was not transferred into the repo (moved off its branch?)"
        )));
    }
    Ok(head)
}

/// Materialize a read-only snapshot of `sha`'s exact committed tree (no `.git`)
/// for judges and oracles. Uses a throwaway index + `checkout-index` rather than
/// `git archive`: archive would honor a committed `.gitattributes export-ignore`,
/// letting a worker hide files from the oracle judging its own commit.
pub async fn create_snapshot(repo: &Path, dest: &Path, sha: &str) -> Result<()> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .context("failed to clear stale snapshot")?;
    }
    tokio::fs::create_dir_all(dest).await?;
    let index = dest.with_extension("snapshot.index");
    let index_str = index.to_string_lossy().into_owned();
    // `checkout-index --prefix` requires a trailing separator and creates the
    // leading directories itself.
    let prefix = format!("{}/", dest.to_string_lossy());
    let materialize = async {
        run(
            Command::new("git")
                .current_dir(repo)
                .env("GIT_INDEX_FILE", &index_str)
                .args(["read-tree", sha]),
            "git read-tree",
        )
        .await?;
        run(
            Command::new("git")
                .current_dir(repo)
                .env("GIT_INDEX_FILE", &index_str)
                .args(["checkout-index", "--all", &format!("--prefix={prefix}")]),
            "git checkout-index",
        )
        .await
    };
    let result = materialize.await;
    let _ = tokio::fs::remove_file(&index).await;
    result
}

pub async fn remove_dir(dir: &Path) {
    let _ = tokio::fs::remove_dir_all(dir).await;
}

/// The unified diff between two commits (`from..to`). Empty when the tree did
/// not change (a writer that committed nothing yields `to == from`).
pub async fn diff(repo: &Path, from: &str, to: &str) -> Result<String> {
    git(repo, &["diff", &format!("{from}..{to}")]).await
}

/// Create a branch `name` at `sha` without touching HEAD or the worktree. With
/// `force`, move an existing branch; otherwise fail if it already exists.
pub async fn create_branch(repo: &Path, name: &str, sha: &str, force: bool) -> Result<()> {
    let mut args = vec!["branch"];
    if force {
        args.push("--force");
    }
    args.push(name);
    args.push(sha);
    git(repo, &args).await.map(|_| ())
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn init_repo(dir: &Path) -> String {
        std::fs::write(dir.join("f.txt"), "base\n").unwrap();
        git(dir, &["init", "-q"]).await.unwrap();
        for (k, v) in [
            ("user.name", "t"),
            ("user.email", "t@l"),
            ("commit.gpgsign", "false"),
        ] {
            git(dir, &["config", k, v]).await.unwrap();
        }
        git(dir, &["add", "-A"]).await.unwrap();
        git(dir, &["commit", "-q", "-m", "base"]).await.unwrap();
        head_sha(dir).await.unwrap()
    }

    // Regression (QA): a worker that moves HEAD off its mission branch must
    // not make the engine record a commit that was never transferred.
    #[tokio::test]
    async fn capture_records_the_actual_head_even_when_moved_off_branch() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let clone = create_worker_clone(
            repo.path(),
            &work.path().join("clone"),
            "mabc123def456",
            "fix-a1",
            &base,
        )
        .await
        .unwrap();

        // The worker detaches HEAD and commits there, leaving the mission
        // branch pointing at base.
        git(&clone.dir, &["checkout", "--quiet", "--detach"])
            .await
            .unwrap();
        std::fs::write(clone.dir.join("f.txt"), "worker change\n").unwrap();
        git(&clone.dir, &["add", "-A"]).await.unwrap();
        git(&clone.dir, &["commit", "-q", "-m", "off-branch"])
            .await
            .unwrap();
        let detached = head_sha(&clone.dir).await.unwrap();
        assert_ne!(detached, base);

        let recorded = capture_worker_result(repo.path(), &clone).await.unwrap();
        // The recorded head is the worker's actual HEAD, and it really landed
        // in the target repo (so a later snapshot succeeds).
        assert_eq!(recorded, detached);
        assert!(commit_exists(repo.path(), &recorded).await);
    }

    #[tokio::test]
    async fn capture_rejects_a_dirty_worktree() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let clone = create_worker_clone(
            repo.path(),
            &work.path().join("clone"),
            "mabc123def456",
            "a1",
            &base,
        )
        .await
        .unwrap();
        std::fs::write(clone.dir.join("f.txt"), "uncommitted\n").unwrap();
        assert!(capture_worker_result(repo.path(), &clone).await.is_err());
    }

    async fn commit_change(repo: &Path, contents: &str) -> String {
        std::fs::write(repo.join("f.txt"), contents).unwrap();
        git(repo, &["add", "-A"]).await.unwrap();
        git(repo, &["commit", "-q", "-m", "change"]).await.unwrap();
        head_sha(repo).await.unwrap()
    }

    // `report --patch` / `apply` read the diff between the recorded base and
    // head; an unchanged tree (a writer that committed nothing) must yield an
    // empty diff, never an error.
    #[tokio::test]
    async fn diff_shows_changes_and_is_empty_for_an_unchanged_tree() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let head = commit_change(repo.path(), "changed\n").await;

        let changed = diff(repo.path(), &base, &head).await.unwrap();
        assert!(
            changed.contains("+changed"),
            "diff shows the change: {changed}"
        );
        assert!(
            diff(repo.path(), &base, &base)
                .await
                .unwrap()
                .trim()
                .is_empty(),
            "an unchanged tree diffs empty, not errors"
        );
    }

    // `apply` creates `lionclaw/<id>` at the produced commit without moving HEAD;
    // it refuses to clobber an existing branch unless forced.
    #[tokio::test]
    async fn create_branch_pins_a_sha_and_guards_against_clobber() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let head = commit_change(repo.path(), "changed\n").await;
        let head_before = head_sha(repo.path()).await.unwrap();

        create_branch(repo.path(), "lionclaw/m1", &base, false)
            .await
            .unwrap();
        assert_eq!(
            git(repo.path(), &["rev-parse", "lionclaw/m1"])
                .await
                .unwrap()
                .trim(),
            base,
            "the branch points at the requested sha"
        );
        assert_eq!(
            head_sha(repo.path()).await.unwrap(),
            head_before,
            "HEAD is untouched"
        );

        assert!(
            create_branch(repo.path(), "lionclaw/m1", &head, false)
                .await
                .is_err(),
            "an existing branch is not clobbered without --force"
        );
        create_branch(repo.path(), "lionclaw/m1", &head, true)
            .await
            .unwrap();
        assert_eq!(
            git(repo.path(), &["rev-parse", "lionclaw/m1"])
                .await
                .unwrap()
                .trim(),
            head,
            "--force moves the branch"
        );
    }

    // The oracle judges the EXACT committed tree: a worker must not be able to
    // hide a file (e.g. a failing test) from its judge with a committed
    // `.gitattributes export-ignore`, which `git archive` honors. Regression for
    // the false-Verified vector.
    #[tokio::test]
    async fn snapshot_materializes_the_full_tree_ignoring_export_ignore() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        std::fs::write(repo.path().join("hidden.txt"), "the failing test\n").unwrap();
        std::fs::write(
            repo.path().join(".gitattributes"),
            "hidden.txt export-ignore\n",
        )
        .unwrap();
        git(repo.path(), &["add", "-A"]).await.unwrap();
        git(repo.path(), &["commit", "-q", "-m", "hide a file"])
            .await
            .unwrap();
        let head = head_sha(repo.path()).await.unwrap();

        let dest = repo.path().join("snap");
        create_snapshot(repo.path(), &dest, &head).await.unwrap();
        assert!(
            dest.join("hidden.txt").exists(),
            "an export-ignored file must still appear in the oracle's snapshot"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("hidden.txt")).unwrap(),
            "the failing test\n",
            "the snapshot is the exact committed bytes"
        );
        assert!(!dest.join(".git").exists(), "the snapshot has no .git");
    }
}
