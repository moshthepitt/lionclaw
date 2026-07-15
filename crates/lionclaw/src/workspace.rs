//! Git workspace isolation for confined runs.
//!
//! Every role and oracle gets the same self-contained checkout at its exact
//! base commit. The authority compiler decides whether that checkout is mounted
//! read-write or read-only; workspace materialization has no role policy of its
//! own. `--no-hardlinks --dissociate` keeps confined Git objects independent
//! from the user repository and any object store it borrows from, including
//! under OCI relabeling. Artifact-producing output is a recorded commit fetched
//! back under `refs/mission/…`, never auto-applied.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::model::EffectId;
use tokio::process::Command;

pub async fn head_sha(repo: &Path) -> Result<String> {
    let out = git(repo, &["rev-parse", "HEAD"]).await?;
    Ok(out.trim().to_string())
}

pub async fn commit_exists(repo: &Path, sha: &str) -> bool {
    resolve_commit(repo, sha).await.is_ok()
}

pub async fn is_dirty(repo: &Path) -> Result<bool> {
    Ok(!git(repo, &["status", "--porcelain"])
        .await?
        .trim()
        .is_empty())
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

/// Create a clean, complete Git checkout at `sha`. Callers decide mount access;
/// this function deliberately has no writer/judge/oracle mode.
pub async fn create_checkout(repo: &Path, dest: &Path, sha: &str) -> Result<()> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .context("failed to clear stale checkout")?;
    }
    tokio::fs::create_dir_all(dest.parent().context("checkout dest has no parent")?).await?;
    let expected = resolve_commit(repo, sha)
        .await
        .with_context(|| format!("resolving checkout commit '{sha}'"))?;
    let expected = expected.trim();
    let mut clone = Command::new("git");
    clone.args([
        "clone",
        "--quiet",
        "--no-hardlinks",
        "--dissociate",
        "--no-checkout",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "user.name=LionClaw Mission",
        "-c",
        "user.email=mission@lionclaw.local",
        "-c",
        "commit.gpgsign=false",
        "--",
    ]);
    clone.arg(repo).arg(dest);
    run(&mut clone, "git clone").await?;
    git(dest, &["checkout", "--quiet", "--detach", expected]).await?;
    let actual = head_sha(dest).await?;
    if actual != expected {
        bail!("checkout HEAD {actual} does not match requested commit {expected}");
    }
    let status = git(dest, &["status", "--porcelain"]).await?;
    if !status.is_empty() {
        bail!("new checkout is unexpectedly dirty");
    }
    Ok(())
}

/// Why post-run artifact capture failed. Distinguishes the one agent-behavior
/// case (an uncommitted tree) from everything else (Git infrastructure),
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
/// survives checkout teardown.
pub async fn capture_worker_result(
    repo: &Path,
    checkout: &Path,
    mission_id: &str,
    effect_id: &EffectId,
) -> Result<String, CaptureError> {
    let status = git(checkout, &["status", "--porcelain"]).await?;
    if !status.trim().is_empty() {
        return Err(CaptureError::DirtyWorktree(status.lines().count()));
    }
    let head = head_sha(checkout).await?;
    // Fetch the checkout's HEAD commit so the object we report is the object we
    // store, regardless of which refs the agent created or moved locally.
    let mission_ref = format!("refs/mission/{mission_id}/{effect_id}");
    let refspec = format!("+HEAD:{mission_ref}");
    let mut fetch = Command::new("git");
    fetch
        .current_dir(repo)
        .args([
            "fetch",
            "--quiet",
            "--no-write-fetch-head",
            "--no-auto-maintenance",
        ])
        .arg(checkout)
        .arg(&refspec);
    run(&mut fetch, "git fetch from worker checkout").await?;
    // The engine observes the commit from Git, never trusts the agent: verify
    // that the exact durable ref landed at the reported head.
    let stored = resolve_commit(repo, &mission_ref)
        .await
        .map_err(CaptureError::Infra)?;
    if stored.trim() != head {
        return Err(CaptureError::Infra(anyhow::anyhow!(
            "captured mission ref points at {}, expected worker HEAD {head}",
            stored.trim()
        )));
    }
    Ok(head)
}

/// Delete the captured ref for an effect whose outcome was never committed.
/// A missing ref is already clean.
pub async fn discard_worker_result(
    repo: &Path,
    mission_id: &str,
    effect_id: &EffectId,
) -> Result<()> {
    let mission_ref = format!("refs/mission/{mission_id}/{effect_id}");
    git(repo, &["update-ref", "-d", &mission_ref]).await?;
    Ok(())
}

pub async fn remove_dir(dir: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove '{}'", dir.display())),
    }
}

/// `chmod 0o755` — used to keep staged oracle executables executable.
#[cfg(unix)]
pub fn make_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)
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

async fn resolve_commit(repo: &Path, revision: &str) -> Result<String> {
    let peeled = format!("{revision}^{{commit}}");
    git(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &peeled,
        ],
    )
    .await
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

    #[tokio::test]
    async fn capture_records_the_actual_detached_head() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let fetch_head = repo.path().join(".git/FETCH_HEAD");
        std::fs::write(&fetch_head, "source sentinel\n").unwrap();
        let source_status = git(repo.path(), &["status", "--porcelain"]).await.unwrap();
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();

        std::fs::write(checkout.join("f.txt"), "worker change\n").unwrap();
        git(&checkout, &["add", "-A"]).await.unwrap();
        git(&checkout, &["commit", "-q", "-m", "off-branch"])
            .await
            .unwrap();
        let detached = head_sha(&checkout).await.unwrap();
        assert_ne!(detached, base);

        let effect_id = EffectId::for_parts(&["test", "fix-a1"]);
        let recorded = capture_worker_result(repo.path(), &checkout, "mabc123def456", &effect_id)
            .await
            .unwrap();
        // The recorded head is the worker's actual HEAD, and it really landed
        // in the target repo (so a later checkout succeeds).
        assert_eq!(recorded, detached);
        assert!(commit_exists(repo.path(), &recorded).await);
        assert_eq!(
            std::fs::read_to_string(fetch_head).unwrap(),
            "source sentinel\n",
            "capture must not rewrite the user's FETCH_HEAD"
        );
        assert_eq!(head_sha(repo.path()).await.unwrap(), base);
        assert_eq!(
            git(repo.path(), &["status", "--porcelain"]).await.unwrap(),
            source_status,
            "capture must not change the source worktree or index"
        );

        let replay = work.path().join("replay");
        create_checkout(repo.path(), &replay, &recorded)
            .await
            .unwrap();
        assert_eq!(head_sha(&replay).await.unwrap(), recorded);
    }

    #[tokio::test]
    async fn capture_rejects_a_dirty_worktree() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();
        std::fs::write(checkout.join("f.txt"), "uncommitted\n").unwrap();
        // An uncommitted tree is DirtyWorktree specifically — not the Infra
        // bucket, which would mislabel the persisted failure.
        let effect_id = EffectId::for_parts(&["test", "a1"]);
        assert!(matches!(
            capture_worker_result(repo.path(), &checkout, "mabc123def456", &effect_id).await,
            Err(CaptureError::DirtyWorktree(_))
        ));
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

    #[tokio::test]
    async fn checkout_is_a_clean_complete_git_repo_at_the_exact_commit() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
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
        let judged = head_sha(repo.path()).await.unwrap();

        let attempt = tempfile::tempdir().unwrap();
        let checkout = attempt.path().join("checkout");
        create_checkout(repo.path(), &checkout, &judged)
            .await
            .unwrap();

        assert!(checkout.join(".git").is_dir());
        assert_eq!(head_sha(&checkout).await.unwrap(), judged);
        assert!(git(&checkout, &["status", "--porcelain"])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            std::fs::read_to_string(checkout.join("hidden.txt")).unwrap(),
            "the failing test\n"
        );
        assert!(git(&checkout, &["log", "--format=%H", "--all"])
            .await
            .unwrap()
            .lines()
            .any(|line| line == base));
        assert_eq!(
            git(
                &checkout,
                &["show", "--format=", "--no-renames", "HEAD:hidden.txt"]
            )
            .await
            .unwrap(),
            "the failing test\n"
        );
        assert!(git(&checkout, &["diff", "--stat", &base, &judged])
            .await
            .unwrap()
            .contains("hidden.txt"));
        assert!(git(&checkout, &["blame", "-L", "1,1", "f.txt"])
            .await
            .is_ok());
        assert_eq!(
            git(&checkout, &["merge-base", &base, &judged])
                .await
                .unwrap()
                .trim(),
            base
        );
    }

    #[tokio::test]
    async fn checkout_rejects_a_non_commit_revision() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        std::fs::write(repo.path().join("blob.txt"), "not a commit\n").unwrap();
        let blob = git(repo.path(), &["hash-object", "-w", "blob.txt"])
            .await
            .unwrap();
        let attempt = tempfile::tempdir().unwrap();
        let checkout = attempt.path().join("checkout");

        let error = create_checkout(repo.path(), &checkout, blob.trim())
            .await
            .expect_err("a blob cannot be judged as a commit");

        assert!(error.to_string().contains("resolving checkout commit"));
        assert!(!checkout.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkout_does_not_hardlink_git_objects_to_the_source() {
        use std::os::unix::fs::MetadataExt;

        let repo = tempfile::tempdir().unwrap();
        let head = init_repo(repo.path()).await;
        let attempt = tempfile::tempdir().unwrap();
        let checkout = attempt.path().join("checkout");

        create_checkout(repo.path(), &checkout, &head)
            .await
            .unwrap();

        let object = Path::new("objects").join(&head[..2]).join(&head[2..]);
        let source = std::fs::metadata(repo.path().join(".git").join(&object)).unwrap();
        let isolated = std::fs::metadata(checkout.join(".git").join(&object)).unwrap();
        assert_ne!(
            (source.dev(), source.ino()),
            (isolated.dev(), isolated.ino()),
            "confined checkout objects must not share source-repository inodes"
        );
    }

    #[tokio::test]
    async fn checkout_dissociates_from_a_source_object_alternate() {
        let temp = tempfile::tempdir().unwrap();
        let origin = temp.path().join("origin");
        std::fs::create_dir(&origin).unwrap();
        let head = init_repo(&origin).await;
        let source = temp.path().join("source");
        let mut shared_clone = Command::new("git");
        shared_clone
            .args(["clone", "--quiet", "--shared", "--"])
            .arg(&origin)
            .arg(&source);
        run(&mut shared_clone, "shared test clone").await.unwrap();
        assert!(source.join(".git/objects/info/alternates").is_file());

        let checkout = temp.path().join("checkout");
        create_checkout(&source, &checkout, &head).await.unwrap();
        assert!(!checkout.join(".git/objects/info/alternates").exists());

        std::fs::rename(&origin, temp.path().join("origin-away")).unwrap();
        assert_eq!(head_sha(&checkout).await.unwrap(), head);
        assert!(git(&checkout, &["fsck", "--full", "--no-dangling"])
            .await
            .is_ok());
    }
}
