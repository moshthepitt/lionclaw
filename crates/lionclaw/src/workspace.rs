//! Git workspace isolation for confined runs.
//!
//! Every role and oracle gets the same self-contained checkout at its exact
//! base commit. The authority compiler decides whether that checkout is mounted
//! read-write or read-only; workspace materialization has no role policy of its
//! own. `--no-local` uses Git's normal transport instead of copying or linking
//! the source object store, keeping confined objects independent and packed.
//! Artifact-producing output is a recorded commit fetched back under
//! `refs/mission/…`, never auto-applied.

use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rustix::fs::{open, Mode, OFlags};
use rustix::io::Errno;

use crate::model::EffectId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitMaterializationError {
    #[error("commit object is missing")]
    Missing,
    #[error("commit object has invalid content: {0}")]
    InvalidContent(String),
    #[error("commit object is unreadable: {0}")]
    Unreadable(String),
    #[error("commit expansion exceeds its materialization limit")]
    ExpansionLimit,
}

/// Proof that LionClaw observed and fetched one clean worker HEAD for one exact
/// effect. The durable event payload and binding are intentionally hidden.
///
/// ```compile_fail
/// use lionclaw::model::ArtifactOutcome;
/// use lionclaw::ports::CapturedArtifact;
/// let _ = CapturedArtifact(ArtifactOutcome {
///     base_sha: "base".into(),
///     head_sha: "claimed".into(),
/// });
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedArtifact(CapturedArtifactInner);

#[derive(Debug, Clone, PartialEq, Eq)]
enum CapturedArtifactInner {
    Verified {
        mission_id: crate::model::MissionId,
        effect_id: EffectId,
        outcome: crate::model::ArtifactOutcome,
    },
    #[cfg(any(test, feature = "testing"))]
    TestRequest(crate::model::ArtifactOutcome),
}

impl CapturedArtifact {
    fn verified(
        mission_id: crate::model::MissionId,
        effect_id: EffectId,
        base_sha: String,
        head_sha: String,
    ) -> Self {
        Self(CapturedArtifactInner::Verified {
            mission_id,
            effect_id,
            outcome: crate::model::ArtifactOutcome { base_sha, head_sha },
        })
    }

    pub fn base_sha(&self) -> &str {
        &self.outcome().base_sha
    }

    pub fn head_sha(&self) -> &str {
        &self.outcome().head_sha
    }

    fn outcome(&self) -> &crate::model::ArtifactOutcome {
        match &self.0 {
            CapturedArtifactInner::Verified { outcome, .. } => outcome,
            #[cfg(any(test, feature = "testing"))]
            CapturedArtifactInner::TestRequest(outcome) => outcome,
        }
    }

    pub(crate) fn validate_binding(
        &self,
        mission_id: &crate::model::MissionId,
        effect_id: &EffectId,
        base_sha: &str,
    ) -> Result<(), String> {
        match &self.0 {
            CapturedArtifactInner::Verified {
                mission_id: captured_mission,
                effect_id: captured_effect,
                outcome,
            } if captured_mission == mission_id
                && captured_effect == effect_id
                && outcome.base_sha == base_sha =>
            {
                Ok(())
            }
            CapturedArtifactInner::Verified { .. } => {
                Err("captured artifact belongs to a different effect request".to_string())
            }
            #[cfg(any(test, feature = "testing"))]
            CapturedArtifactInner::TestRequest(_) => {
                Err("unverified testing artifact reached engine settlement".to_string())
            }
        }
    }

    pub(crate) fn as_outcome(&self) -> &crate::model::ArtifactOutcome {
        self.outcome()
    }

    pub(crate) fn into_outcome(self) -> crate::model::ArtifactOutcome {
        match self.0 {
            CapturedArtifactInner::Verified { outcome, .. } => outcome,
            #[cfg(any(test, feature = "testing"))]
            CapturedArtifactInner::TestRequest(_) => {
                unreachable!("testing requests are rejected before conversion")
            }
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn for_testing(base_sha: impl Into<String>, head_sha: impl Into<String>) -> Self {
        Self(CapturedArtifactInner::TestRequest(
            crate::model::ArtifactOutcome {
                base_sha: base_sha.into(),
                head_sha: head_sha.into(),
            },
        ))
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn test_request(&self) -> Option<&crate::model::ArtifactOutcome> {
        match &self.0 {
            CapturedArtifactInner::TestRequest(outcome) => Some(outcome),
            CapturedArtifactInner::Verified { .. } => None,
        }
    }
}

/// Engine-issued authority to capture one exact task checkout into one exact
/// mission effect. A runner may change the checkout, but cannot redirect the
/// target repository, base, mission, or effect identity.
#[derive(Debug, Clone)]
pub struct ArtifactCapture {
    repo: PathBuf,
    checkout: PathBuf,
    required_base: String,
    mission_id: crate::model::MissionId,
    effect_id: EffectId,
}

impl ArtifactCapture {
    pub(crate) fn new(
        repo: PathBuf,
        checkout: PathBuf,
        required_base: String,
        mission_id: crate::model::MissionId,
        effect_id: EffectId,
    ) -> Self {
        Self {
            repo,
            checkout,
            required_base,
            mission_id,
            effect_id,
        }
    }

    pub fn checkout_dir(&self) -> &Path {
        &self.checkout
    }

    pub async fn capture(&self) -> Result<CapturedArtifact, CaptureError> {
        capture_worker_result(
            &self.repo,
            &self.checkout,
            &self.required_base,
            &self.mission_id,
            &self.effect_id,
        )
        .await
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn prepare_for_testing(&self, recreate: bool) -> Result<()> {
        if recreate || !self.checkout.is_dir() {
            replace_checkout(&self.repo, &self.checkout, &self.required_base).await?;
        } else {
            let head = head_sha(&self.checkout).await?;
            if !is_ancestor(&self.checkout, &self.required_base, &head).await? {
                bail!(
                    "retained test checkout HEAD {head} diverges from required base {}",
                    self.required_base
                );
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn capture_test_commit(
        &self,
        _label: &str,
    ) -> Result<CapturedArtifact, CaptureError> {
        let current = head_sha(&self.checkout)
            .await
            .map_err(CaptureError::Infra)?;
        if current != self.required_base {
            return self.capture().await;
        }
        git(&self.checkout, &["add", "-A"])
            .await
            .map_err(CaptureError::Infra)?;
        let mut commit = managed_git_command();
        commit
            .current_dir(&self.checkout)
            .env("GIT_AUTHOR_DATE", "2000-01-02T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2000-01-02T00:00:00Z")
            .args(["commit", "--quiet", "--allow-empty", "-m"])
            .arg("LionClaw test artifact");
        run(&mut commit, "creating test artifact commit")
            .await
            .map_err(CaptureError::Infra)?;
        self.capture().await
    }
}

pub async fn head_sha(repo: &Path) -> Result<String> {
    let out = resolve_managed_commit(repo, "HEAD").await?;
    Ok(out.trim().to_string())
}

pub async fn commit_exists(repo: &Path, sha: &str) -> bool {
    resolve_managed_commit(repo, sha).await.is_ok()
}

pub async fn task_is_dirty(repo: &Path) -> Result<bool> {
    Ok(!observed_task_git(repo, &["status", "--porcelain"])
        .await?
        .trim()
        .is_empty())
}

pub async fn is_ancestor(repo: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    let status = managed_git_command()
        .current_dir(repo)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .await
        .context("failed to spawn git merge-base --is-ancestor")?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => bail!(
            "git merge-base --is-ancestor failed in '{}'",
            repo.display()
        ),
    }
}

pub async fn task_head_sha(repo: &Path) -> Result<String> {
    let out = observed_task_git(repo, &["rev-parse", "HEAD"]).await?;
    Ok(out.trim().to_string())
}

pub async fn task_commit_exists(repo: &Path, sha: &str) -> bool {
    let peeled = format!("{sha}^{{commit}}");
    observed_task_git(
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
    .is_ok()
}

pub async fn task_is_ancestor(repo: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    let observer = TaskGitObserver::open(repo).await?;
    observer.is_ancestor(ancestor, descendant).await
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
    let expected = resolve_managed_commit(repo, sha)
        .await
        .with_context(|| format!("resolving checkout commit '{sha}'"))?;
    let expected = expected.trim();
    let mut clone = managed_git_command();
    clone.args([
        "clone",
        "--quiet",
        "--no-local",
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
    // Normal clone refspecs do not retain LionClaw's private refs/mission/*
    // namespace. Fetch the already-resolved object explicitly so a captured
    // candidate remains materializable without copying the whole source object
    // store.
    git(
        dest,
        &[
            "fetch",
            "--quiet",
            "--no-write-fetch-head",
            "origin",
            expected,
        ],
    )
    .await?;
    git(dest, &["checkout", "--quiet", "--detach", expected]).await?;
    let actual = task_head_sha(dest).await?;
    if actual != expected {
        bail!("checkout HEAD {actual} does not match requested commit {expected}");
    }
    let status = git(dest, &["status", "--porcelain"]).await?;
    if !status.is_empty() {
        bail!("new checkout is unexpectedly dirty");
    }
    Ok(())
}

/// Materialize a checkout completely before replacing a task's durable clone.
/// The two fixed siblings also make an interrupted swap recoverable on the
/// next invocation without storing another authority-bearing state machine.
pub async fn replace_checkout(repo: &Path, dest: &Path, sha: &str) -> Result<()> {
    let parent = dest.parent().context("checkout dest has no parent")?;
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .context("checkout dest has no UTF-8 file name")?;
    let staging = parent.join(format!(".{name}.prepare"));
    let previous = parent.join(format!(".{name}.previous"));

    if !dest.exists() && previous.exists() {
        std::fs::rename(&previous, dest).context("restoring interrupted task checkout swap")?;
    } else if dest.exists() {
        remove_dir(&previous).await?;
    }
    remove_dir(&staging).await?;
    create_checkout(repo, &staging, sha).await?;

    if dest.exists() {
        std::fs::rename(dest, &previous).context("preserving previous task checkout")?;
    }
    if let Err(error) = std::fs::rename(&staging, dest) {
        if previous.exists() && !dest.exists() {
            let _ = std::fs::rename(&previous, dest);
        }
        return Err(error).context("publishing replacement task checkout");
    }
    remove_dir(&previous).await?;
    Ok(())
}

/// Materialize the immutable index used by live workspace observation. The
/// index lives beside the task checkout, outside the runtime mount, and is
/// built from the trusted target repository rather than worker Git metadata.
pub async fn prepare_task_observer_index(
    repo: &Path,
    index: &Path,
    base_sha: &str,
    reset: bool,
) -> Result<()> {
    match std::fs::symlink_metadata(index) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            if !reset {
                return Ok(());
            }
        }
        Ok(_) => bail!(
            "task observer index '{}' is not a regular file",
            index.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading task observer index '{}'", index.display()))
        }
    }
    std::fs::create_dir_all(index.parent().context("observer index has no parent")?)?;
    let sequence = OBSERVER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = index.with_extension(format!("prepare-{}-{sequence}", std::process::id()));
    let mut command = managed_git_command();
    command
        .current_dir(repo)
        .env("GIT_INDEX_FILE", &temporary)
        .args(["read-tree", "--reset", base_sha]);
    let result = async {
        run(&mut command, "git read-tree for task observer").await?;
        let metadata = std::fs::symlink_metadata(&temporary)
            .context("Git did not create the task observer index")?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            bail!("Git created an invalid task observer index");
        }
        std::fs::rename(&temporary, index).context("publishing task observer index")?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Observe a live task worktree relative to its assignment base without
/// opening any worker-controlled `.git` path.
pub async fn observe_task_workspace(
    repo: &Path,
    worktree: &Path,
    index: &Path,
    base_sha: &str,
) -> Result<String> {
    let observer = TaskGitObserver::from_engine_baseline(repo, worktree, index, base_sha).await?;
    let diff = observer
        .raw_output(&["diff", "--no-ext-diff", "--no-textconv", "--stat"])
        .await?;
    ensure_observer_success(&diff, "read task workspace diffstat")?;
    let status = observer.raw_output(&["status", "--short"]).await?;
    ensure_observer_success(&status, "read task workspace status")?;
    let mut summary = String::from_utf8_lossy(&diff.stdout).into_owned();
    summary.push_str(&String::from_utf8_lossy(&status.stdout));
    Ok(summary)
}

fn ensure_observer_success(
    output: &lionclaw_runtime_api::ExecutionOutput,
    operation: &str,
) -> Result<()> {
    if !output.success() {
        bail!(
            "{operation} failed ({}): {}",
            output.status_description(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Why post-run artifact capture failed. Correctable dirty output, permanent
/// history divergence, and Git infrastructure failures have different retry
/// semantics at the runner boundary.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("worker left uncommitted changes ({0} paths)")]
    DirtyWorktree(usize),
    #[error("worker HEAD {head} does not descend from required base {required_base}")]
    HistoryDiverged { required_base: String, head: String },
    #[error(transparent)]
    Infra(#[from] anyhow::Error),
}

/// Post-run artifact capture: the tree must be committed clean; the head
/// commit is fetched back into the target repo under `refs/mission/…` so it
/// survives checkout teardown.
async fn capture_worker_result(
    repo: &Path,
    checkout: &Path,
    required_base: &str,
    mission_id: &crate::model::MissionId,
    effect_id: &EffectId,
) -> Result<crate::ports::CapturedArtifact, CaptureError> {
    let observer = TaskGitObserver::open(checkout).await?;
    let status = observer.output(&["status", "--porcelain"]).await?;
    if !status.trim().is_empty() {
        return Err(CaptureError::DirtyWorktree(status.lines().count()));
    }
    let head = observer.output(&["rev-parse", "HEAD"]).await?;
    let head = head.trim().to_string();
    if !observer.is_ancestor(required_base, &head).await? {
        return Err(CaptureError::HistoryDiverged {
            required_base: required_base.to_string(),
            head,
        });
    }
    let fetch_source = observer.local_fetch_source()?;
    let fetch_url = format!("file://{}", fetch_source.display());
    // Fetch the checkout's HEAD commit so the object we report is the object we
    // store, regardless of which refs the agent created or moved locally.
    let mission_ref = format!("refs/mission/{mission_id}/{effect_id}");
    let refspec = format!("+HEAD:{mission_ref}");
    let mut fetch = managed_git_command();
    fetch
        .current_dir(repo)
        .args([
            "fetch",
            "--quiet",
            "--no-write-fetch-head",
            "--no-auto-maintenance",
        ])
        .arg(&fetch_url)
        .arg(&refspec);
    run(&mut fetch, "git fetch from worker checkout").await?;
    // The engine observes the commit from Git, never trusts the agent: verify
    // that the exact durable ref landed at the reported head.
    let stored = resolve_managed_commit(repo, &mission_ref)
        .await
        .map_err(CaptureError::Infra)?;
    if stored.trim() != head {
        return Err(CaptureError::Infra(anyhow::anyhow!(
            "captured mission ref points at {}, expected worker HEAD {head}",
            stored.trim()
        )));
    }
    Ok(CapturedArtifact::verified(
        mission_id.clone(),
        effect_id.clone(),
        required_base.to_string(),
        head,
    ))
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
    git(
        repo,
        &[
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            &format!("{from}..{to}"),
        ],
    )
    .await
}

/// Render one trusted, already-authorized commit for transient message context.
pub async fn show_commit(repo: &Path, sha: &str, max_bytes: usize) -> Result<Vec<u8>> {
    if !valid_object_id(sha) {
        return Err(
            CommitMaterializationError::InvalidContent("invalid object identity".into()).into(),
        );
    }
    let sha = sha.to_ascii_lowercase();
    let observer = CommitGitObserver::open(repo, &sha).await?;
    verify_commit_object(&observer, &sha).await?;
    let mut command = observer.command();
    configure_commit_show(&mut command, &sha);
    let output = run_git_bounded(&mut command, None, max_bytes, MAX_GIT_DIAGNOSTIC_BYTES)
        .await
        .map_err(|error| match error {
            BoundedGitError::Limit(GitStream::Stdout) => CommitMaterializationError::ExpansionLimit,
            error => CommitMaterializationError::Unreadable(error.to_string()),
        })?;
    if !output.status.success() {
        if let Err(error) = verify_commit_object(&observer, &sha).await {
            return Err(error.into());
        }
        return Err(
            CommitMaterializationError::InvalidContent(git_diagnostic(&output.stderr)).into(),
        );
    }
    Ok(output.stdout)
}

fn configure_commit_show(command: &mut Command, sha: &str) {
    command.args([
        "show",
        "--format=fuller",
        "--no-use-mailmap",
        "--no-notes",
        "--no-show-signature",
        "--no-decorate",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--no-color",
        "--full-index",
        "--binary",
        sha,
        "--",
    ]);
}

struct CommitGitObserver {
    metadata: PathBuf,
    objects: PathBuf,
}

impl CommitGitObserver {
    async fn open(repo: &Path, sha: &str) -> std::result::Result<Self, CommitMaterializationError> {
        let mut command = managed_git_command();
        command.current_dir(repo).args([
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "objects",
        ]);
        let output = run_git_bounded(
            &mut command,
            None,
            MAX_GIT_PATH_BYTES,
            MAX_GIT_DIAGNOSTIC_BYTES,
        )
        .await
        .map_err(|error| CommitMaterializationError::Unreadable(error.to_string()))?;
        if !output.status.success() {
            return Err(CommitMaterializationError::Unreadable(git_diagnostic(
                &output.stderr,
            )));
        }
        let objects = parse_git_path(&output.stdout)?;
        let objects = std::fs::canonicalize(objects).map_err(|error| {
            CommitMaterializationError::Unreadable(format!(
                "resolving Git object database: {error}"
            ))
        })?;
        let metadata = std::fs::metadata(&objects).map_err(|error| {
            CommitMaterializationError::Unreadable(format!(
                "reading Git object database '{}': {error}",
                objects.display()
            ))
        })?;
        if !metadata.is_dir() {
            return Err(CommitMaterializationError::InvalidContent(format!(
                "Git object database '{}' is not a directory",
                objects.display()
            )));
        }
        let metadata = create_isolated_git_metadata(sha)
            .map_err(|error| CommitMaterializationError::Unreadable(error.to_string()))?;
        Ok(Self { metadata, objects })
    }

    fn command(&self) -> Command {
        isolated_git_command(&self.metadata, &self.objects)
    }

    fn loose_object_path(&self, sha: &str) -> PathBuf {
        self.objects.join(&sha[..2]).join(&sha[2..])
    }
}

impl Drop for CommitGitObserver {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.metadata);
    }
}

fn parse_git_path(bytes: &[u8]) -> std::result::Result<PathBuf, CommitMaterializationError> {
    let path = std::str::from_utf8(bytes).map_err(|_| {
        CommitMaterializationError::InvalidContent("Git object path is not UTF-8".into())
    })?;
    let path = path.strip_suffix('\n').unwrap_or(path);
    if path.is_empty() || path.contains(['\n', '\r', '\0']) {
        return Err(CommitMaterializationError::InvalidContent(
            "Git object path is malformed".into(),
        ));
    }
    Ok(PathBuf::from(path))
}

async fn verify_commit_object(
    observer: &CommitGitObserver,
    sha: &str,
) -> std::result::Result<(), CommitMaterializationError> {
    let loose_object = inspect_loose_object(observer, sha)?;
    let mut command = observer.command();
    command.args([
        "cat-file",
        "--batch-check=%(objectname) %(objecttype) %(objectsize)",
    ]);
    let input = format!("{sha}\n");
    let output = run_git_bounded(
        &mut command,
        Some(input.as_bytes()),
        MAX_GIT_PROTOCOL_BYTES,
        MAX_GIT_DIAGNOSTIC_BYTES,
    )
    .await
    .map_err(|error| CommitMaterializationError::Unreadable(error.to_string()))?;
    if !output.status.success() {
        return Err(CommitMaterializationError::Unreadable(git_diagnostic(
            &output.stderr,
        )));
    }
    let fields = output
        .stdout
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    if fields.len() == 3
        && fields[0] == sha.as_bytes()
        && fields[1] == b"commit"
        && fields[2].iter().all(u8::is_ascii_digit)
    {
        return Ok(());
    }
    if fields.len() == 2 && fields[0] == sha.as_bytes() && fields[1] == b"missing" {
        return match (loose_object, output.stderr.is_empty()) {
            (LooseObjectState::Absent, true) => Err(CommitMaterializationError::Missing),
            (LooseObjectState::Absent, false) => Err(CommitMaterializationError::Unreadable(
                git_diagnostic(&output.stderr),
            )),
            (LooseObjectState::Readable, _) => Err(CommitMaterializationError::InvalidContent(
                "Git rejected the readable loose object".into(),
            )),
        };
    }
    Err(CommitMaterializationError::InvalidContent(git_diagnostic(
        &output.stderr,
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LooseObjectState {
    Absent,
    Readable,
}

fn inspect_loose_object(
    observer: &CommitGitObserver,
    sha: &str,
) -> std::result::Result<LooseObjectState, CommitMaterializationError> {
    let path = observer.loose_object_path(sha);
    let descriptor = match open(
        &path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(Errno::NOENT) => return Ok(LooseObjectState::Absent),
        Err(Errno::ACCESS | Errno::PERM) => {
            return Err(CommitMaterializationError::Unreadable(format!(
                "permission denied opening '{}'",
                path.display()
            )))
        }
        Err(Errno::LOOP) => {
            return Err(CommitMaterializationError::InvalidContent(format!(
                "loose object '{}' is a symlink",
                path.display()
            )))
        }
        Err(error) => {
            return Err(CommitMaterializationError::Unreadable(format!(
                "opening loose object '{}': {error}",
                path.display()
            )))
        }
    };
    let file = File::from(descriptor);
    let metadata = file.metadata().map_err(|error| {
        CommitMaterializationError::Unreadable(format!(
            "reading loose object metadata '{}': {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(CommitMaterializationError::InvalidContent(format!(
            "loose object '{}' is not a regular file",
            path.display()
        )));
    }
    Ok(LooseObjectState::Readable)
}

const MAX_GIT_PROTOCOL_BYTES: usize = 256;
const MAX_GIT_PATH_BYTES: usize = 4 * 1024;
const MAX_GIT_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const GIT_MATERIALIZATION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct BoundedGitOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
enum GitStream {
    Stdout,
    Stderr,
}

impl std::fmt::Display for GitStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum BoundedGitError {
    #[error("failed to spawn Git: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("Git {0} exceeds its capture limit")]
    Limit(GitStream),
    #[error("failed to read Git {stream}: {source}")]
    Read {
        stream: GitStream,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write Git stdin: {0}")]
    Write(#[source] std::io::Error),
    #[error("failed to wait for Git: {0}")]
    Wait(#[source] std::io::Error),
    #[error("Git materialization exceeded its execution deadline")]
    TimedOut,
}

async fn run_git_bounded(
    command: &mut Command,
    input: Option<&[u8]>,
    stdout_limit: usize,
    stderr_limit: usize,
) -> std::result::Result<BoundedGitOutput, BoundedGitError> {
    run_git_bounded_with_timeout(
        command,
        input,
        stdout_limit,
        stderr_limit,
        GIT_MATERIALIZATION_TIMEOUT,
    )
    .await
}

async fn run_git_bounded_with_timeout(
    command: &mut Command,
    input: Option<&[u8]>,
    stdout_limit: usize,
    stderr_limit: usize,
    timeout: Duration,
) -> std::result::Result<BoundedGitOutput, BoundedGitError> {
    command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(BoundedGitError::Spawn)?;
    let mut stdout = child.stdout.take().expect("Git stdout is configured");
    let mut stderr = child.stderr.take().expect("Git stderr is configured");
    let operation = async {
        if let Some(input) = input {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                BoundedGitError::Write(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Git stdin was not captured",
                ))
            })?;
            stdin
                .write_all(input)
                .await
                .map_err(BoundedGitError::Write)?;
            stdin.shutdown().await.map_err(BoundedGitError::Write)?;
        }
        tokio::try_join!(
            read_git_stream(&mut stdout, stdout_limit, GitStream::Stdout),
            read_git_stream(&mut stderr, stderr_limit, GitStream::Stderr),
            async { child.wait().await.map_err(BoundedGitError::Wait) },
        )
    };
    match tokio::time::timeout(timeout, operation).await {
        Ok(Ok((stdout, stderr, status))) => Ok(BoundedGitOutput {
            status,
            stdout,
            stderr,
        }),
        Ok(Err(error)) => {
            terminate_and_reap(&mut child).await;
            Err(error)
        }
        Err(_) => {
            terminate_and_reap(&mut child).await;
            Err(BoundedGitError::TimedOut)
        }
    }
}

async fn read_git_stream<R: AsyncRead + Unpin>(
    reader: &mut R,
    limit: usize,
    stream: GitStream,
) -> std::result::Result<Vec<u8>, BoundedGitError> {
    let mut bytes = Vec::with_capacity(limit.min(16 * 1024));
    reader
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| BoundedGitError::Read { stream, source })?;
    if bytes.len() > limit {
        return Err(BoundedGitError::Limit(stream));
    }
    Ok(bytes)
}

async fn terminate_and_reap(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn git_diagnostic(stderr: &[u8]) -> String {
    let diagnostic = String::from_utf8_lossy(stderr).trim().to_string();
    if diagnostic.is_empty() {
        "Git rejected the commit object".into()
    } else {
        diagnostic
    }
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

/// Resolve a ref that LionClaw itself just created in the source repository.
/// Worker-controlled repositories use `TaskGitObserver`, whose synthetic
/// metadata prevents Git configuration from becoming executable authority.
async fn resolve_managed_commit(repo: &Path, revision: &str) -> Result<String> {
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

async fn observed_task_git(repo: &Path, args: &[&str]) -> Result<String> {
    let observer = TaskGitObserver::open(repo).await?;
    observer.output(args).await
}

pub async fn observed_git_output(
    repo: &Path,
    args: &[&str],
) -> Result<lionclaw_runtime_api::ExecutionOutput> {
    observed_git_output_with_timeout(repo, args, Path::new("git"), Duration::from_millis(500)).await
}

async fn observed_git_output_with_timeout(
    repo: &Path,
    args: &[&str],
    executable: &Path,
    timeout: Duration,
) -> Result<lionclaw_runtime_api::ExecutionOutput> {
    tokio::time::timeout(timeout, async {
        let observer = TaskGitObserver::open(repo).await?;
        observer.raw_output_with(executable, args).await
    })
    .await
    .with_context(|| format!("isolated git {args:?} exceeded observation deadline"))?
}

async fn git_bytes(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = managed_git_command()
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
    command.kill_on_drop(true);
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

/// Git mutations against the operator repository or a newly-created clone.
/// Worker-owned repositories are read only through `TaskGitObserver` below.
fn managed_git_command() -> Command {
    let mut command = Command::new("git");
    clear_git_authority_environment(&mut command);
    command
        .kill_on_drop(true)
        .arg("--no-optional-locks")
        .arg("--no-replace-objects")
        .args(["-c", "core.fsmonitor=false"])
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(["-c", "core.pager=cat"])
        .args(["-c", "diff.external="])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}

fn isolated_git_command(metadata: &Path, objects: &Path) -> Command {
    let mut command = Command::new("git");
    configure_isolated_git_command(&mut command, metadata, objects);
    command
}

fn configure_isolated_git_command(command: &mut Command, metadata: &Path, objects: &Path) {
    clear_git_authority_environment(command);
    command
        .kill_on_drop(true)
        .arg("--no-optional-locks")
        .arg("--no-replace-objects")
        .args(["-c", "core.fsmonitor=false"])
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(["-c", "core.pager=cat"])
        .args(["-c", "core.attributesFile=/dev/null"])
        .args(["-c", "diff.external="])
        .env("GIT_DIR", metadata)
        .env("GIT_OBJECT_DIRECTORY", objects)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG", "/dev/null")
        .env("GIT_ATTR_NOSYSTEM", "1");
}

fn clear_git_authority_environment(command: &mut Command) {
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
        "GIT_REPLACE_REF_BASE",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_DIFF_OPTS",
        "GIT_EXTERNAL_DIFF",
        "GIT_ATTR_NOSYSTEM",
        "GIT_ATTR_SOURCE",
        "GIT_NOTES_REF",
        "GIT_NOTES_DISPLAY_REF",
    ] {
        command.env_remove(key);
    }
}

static OBSERVER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_GIT_METADATA_BYTES: u64 = 4 * 1024;
const MAX_PACKED_REFS_BYTES: u64 = 8 * 1024 * 1024;

struct TaskGitObserver {
    metadata: PathBuf,
    worktree: PathBuf,
    index: PathBuf,
    objects: PathBuf,
}

impl TaskGitObserver {
    async fn open(worktree: &Path) -> Result<Self> {
        let worktree = worktree.to_path_buf();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel_on_drop = CancelBlockingTraversal(cancelled.clone());
        tokio::task::spawn_blocking(move || Self::open_blocking(&worktree, &cancelled))
            .await
            .context("joining task Git metadata observer")?
    }

    fn open_blocking(worktree: &Path, cancelled: &AtomicBool) -> Result<Self> {
        directory(worktree, "task worktree")?;
        let dot_git = worktree.join(".git");
        let git_dir = directory(&dot_git, "standalone task Git directory").with_context(|| {
            format!(
                "task worktree '{}' must use LionClaw's standalone .git directory",
                worktree.display()
            )
        })?;
        reject_git_indirection(&git_dir.join("commondir"), "Git common-directory pointer")?;
        reject_git_indirection(
            &git_dir.join("objects/info/alternates"),
            "Git object alternate",
        )?;
        reject_git_indirection(
            &git_dir.join("objects/info/http-alternates"),
            "Git HTTP object alternate",
        )?;
        let head = resolve_task_head(&git_dir)?;
        let index = regular_file(&git_dir.join("index"), "Git index")?;
        let objects = directory(&git_dir.join("objects"), "Git object database")?;
        validate_worker_object_database(&objects, cancelled)?;
        Self::create(worktree, index, objects, &head)
    }

    async fn from_engine_baseline(
        repo: &Path,
        worktree: &Path,
        index: &Path,
        head: &str,
    ) -> Result<Self> {
        if !valid_object_id(head) {
            bail!("recorded task base is not a full Git object ID");
        }
        let objects = git(
            repo,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "objects",
            ],
        )
        .await?;
        let objects = std::fs::canonicalize(objects.trim())
            .context("resolving trusted target Git object database")?;
        let worktree = worktree.to_path_buf();
        let index = index.to_path_buf();
        let head = head.to_string();
        tokio::task::spawn_blocking(move || Self::create(&worktree, index, objects, &head))
            .await
            .context("joining trusted task workspace observer")?
    }

    fn create(worktree: &Path, index: PathBuf, objects: PathBuf, head: &str) -> Result<Self> {
        directory(worktree, "task worktree")?;
        let index = regular_file(&index, "task observer index")?;
        directory(&objects, "Git object database")?;
        let metadata = create_isolated_git_metadata(head)?;
        let observer = Self {
            metadata,
            worktree: worktree.to_path_buf(),
            index,
            objects,
        };
        Ok(observer)
    }

    fn tokio_command_with(&self, executable: &Path) -> Command {
        let mut command = Command::new(executable);
        self.configure_tokio(&mut command);
        command
    }

    async fn output(&self, args: &[&str]) -> Result<String> {
        let output = self.raw_output(args).await?;
        if !output.success() {
            bail!(
                "isolated git {:?} failed in '{}': {}",
                args,
                self.worktree.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    async fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool> {
        let output = self
            .raw_output(&["merge-base", "--is-ancestor", ancestor, descendant])
            .await?;
        match output.exit_code {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => bail!(
                "isolated git merge-base --is-ancestor failed in '{}'",
                self.worktree.display()
            ),
        }
    }

    async fn raw_output(&self, args: &[&str]) -> Result<lionclaw_runtime_api::ExecutionOutput> {
        self.raw_output_with(Path::new("git"), args).await
    }

    async fn raw_output_with(
        &self,
        executable: &Path,
        args: &[&str],
    ) -> Result<lionclaw_runtime_api::ExecutionOutput> {
        let mut command = self.tokio_command_with(executable);
        command.args(args);
        lionclaw_confinement::process::run_command_bounded(&mut command)
            .await
            .with_context(|| format!("failed to run isolated git {args:?}"))
    }

    fn local_fetch_source(&self) -> Result<&Path> {
        let objects = self.metadata.join("objects");
        let info = objects.join("info");
        std::fs::create_dir(&info)?;
        let object_path = self.objects.to_string_lossy();
        if object_path.contains(['\n', '\r']) {
            bail!("Git object path cannot be represented safely for capture");
        }
        std::fs::write(info.join("alternates"), format!("{object_path}\n"))?;
        Ok(&self.metadata)
    }

    fn configure_tokio(&self, command: &mut Command) {
        configure_isolated_git_command(command, &self.metadata, &self.objects);
        command
            .env("GIT_WORK_TREE", &self.worktree)
            .env("GIT_INDEX_FILE", &self.index)
            .env("GIT_ATTR_NOSYSTEM", "1");
    }
}

fn create_isolated_git_metadata(head: &str) -> Result<PathBuf> {
    let metadata = loop {
        let sequence = OBSERVER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = std::env::temp_dir().join(format!(
            "lionclaw-git-observer-{}-{sequence}",
            std::process::id()
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => break candidate,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("creating isolated Git metadata"),
        }
    };
    let result: Result<()> = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700))?;
        }
        std::fs::create_dir(metadata.join("objects"))?;
        std::fs::create_dir(metadata.join("refs"))?;
        std::fs::write(metadata.join("HEAD"), format!("{head}\n"))?;
        std::fs::write(metadata.join("config"), synthetic_repository_config(head))?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_dir_all(&metadata);
        return Err(error).context("materializing isolated Git metadata");
    }
    Ok(metadata)
}

/// Fresh task clones are transferred as packs (`--no-local`), so this budget
/// covers the bounded loose objects a worker can legitimately create while
/// preventing capture/observation from becoming an unbounded host traversal.
const MAX_WORKER_OBJECT_ENTRIES: usize = 16 * 1024;
const MAX_WORKER_OBJECT_DEPTH: usize = 16;

struct CancelBlockingTraversal(Arc<AtomicBool>);

impl Drop for CancelBlockingTraversal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Capture runs only after the runtime is gone. Before passing the worker's
/// object database to host Git, reject every indirection and special file so
/// Git cannot escape into another host path.
fn validate_worker_object_database(root: &Path, cancelled: &AtomicBool) -> Result<()> {
    let mut pending = vec![(root.to_path_buf(), 0_usize)];
    let mut entries = 0_usize;
    while let Some((directory, depth)) = pending.pop() {
        if cancelled.load(Ordering::Acquire) {
            bail!("worker Git object validation was cancelled");
        }
        if depth > MAX_WORKER_OBJECT_DEPTH {
            bail!("worker Git object database exceeds the depth limit");
        }
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("reading Git object directory '{}'", directory.display()))?
        {
            if cancelled.load(Ordering::Acquire) {
                bail!("worker Git object validation was cancelled");
            }
            let entry = entry?;
            entries = entries.saturating_add(1);
            if entries > MAX_WORKER_OBJECT_ENTRIES {
                bail!("worker Git object database exceeds the entry limit");
            }
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                bail!("Git object path '{}' is a symlink", path.display());
            }
            if kind.is_dir() {
                pending.push((path, depth + 1));
            } else if !kind.is_file() {
                bail!("Git object path '{}' is not a regular file", path.display());
            }
        }
    }
    Ok(())
}

impl Drop for TaskGitObserver {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.metadata);
    }
}

fn reject_git_indirection(path: &Path, label: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => bail!(
            "{label} '{}' is not allowed in a task clone",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("reading {label} '{}'", path.display())),
    }
}

fn regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            Ok(path.to_path_buf())
        }
        Ok(_) => bail!("{label} '{}' is not a regular file", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("{label} '{}' does not exist", path.display())
        }
        Err(error) => Err(error).with_context(|| format!("reading {label} '{}'", path.display())),
    }
}

fn try_read_bounded_regular_bytes(path: &Path, label: &str, limit: u64) -> Result<Option<Vec<u8>>> {
    let descriptor = match open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(Errno::NOENT) => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("opening {label} '{}'", path.display()))
        }
    };
    let mut file = File::from(descriptor);
    let metadata = file
        .metadata()
        .with_context(|| format!("reading {label} metadata '{}'", path.display()))?;
    if !metadata.is_file() {
        bail!("{label} '{}' is not a regular file", path.display());
    }
    if metadata.len() > limit {
        bail!(
            "{label} '{}' exceeds the {} byte metadata limit",
            path.display(),
            limit
        );
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading {label} '{}'", path.display()))?;
    if bytes.len() as u64 > limit {
        bail!(
            "{label} '{}' exceeds the {} byte metadata limit",
            path.display(),
            limit
        );
    }
    Ok(Some(bytes))
}

fn read_bounded_regular_string(path: &Path, label: &str) -> Result<String> {
    let bytes = try_read_bounded_regular_bytes(path, label, MAX_GIT_METADATA_BYTES)?
        .with_context(|| format!("opening {label} '{}': file not found", path.display()))?;
    String::from_utf8(bytes).with_context(|| format!("{label} '{}' is not UTF-8", path.display()))
}

fn directory(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("reading {label} '{}'", path.display()))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        bail!("{label} '{}' is not a directory", path.display());
    }
    Ok(path.to_path_buf())
}

fn resolve_task_head(git_dir: &Path) -> Result<String> {
    let head = read_bounded_regular_string(&git_dir.join("HEAD"), "Git HEAD")?;
    let head = head.trim();
    if valid_object_id(head) {
        return Ok(head.to_string());
    }
    let reference = head
        .strip_prefix("ref:")
        .map(str::trim)
        .filter(|reference| {
            reference.starts_with("refs/")
                && !reference.contains("..")
                && reference.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'.')
                })
        })
        .context("invalid symbolic Git HEAD")?;
    let reference_path = safe_reference_path(git_dir, reference)?;
    if let Some(bytes) =
        try_read_bounded_regular_bytes(&reference_path, "Git reference", MAX_GIT_METADATA_BYTES)?
    {
        let value = String::from_utf8(bytes)
            .with_context(|| format!("Git reference '{reference}' is not UTF-8"))?;
        let value = value.trim();
        if !valid_object_id(value) {
            bail!("Git reference '{reference}' does not contain a full object ID");
        }
        return Ok(value.to_string());
    }
    read_packed_reference(git_dir, reference)?.with_context(|| {
        format!("symbolic Git HEAD reference '{reference}' does not resolve in the task clone")
    })
}

fn safe_reference_path(git_dir: &Path, reference: &str) -> Result<PathBuf> {
    let relative = Path::new(reference);
    let components = relative.components().collect::<Vec<_>>();
    if components.len() < 2
        || components.first() != Some(&Component::Normal("refs".as_ref()))
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("invalid symbolic Git HEAD reference '{reference}'");
    }
    let mut path = git_dir.to_path_buf();
    let mut parent_missing = false;
    for component in &components[..components.len() - 1] {
        let Component::Normal(component) = component else {
            unreachable!("reference components were validated")
        };
        path.push(component);
        if !parent_missing {
            match std::fs::symlink_metadata(&path) {
                Ok(metadata)
                    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => bail!(
                    "Git reference directory '{}' is not a directory",
                    path.display()
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    parent_missing = true;
                }
                Err(error) => return Err(error).context("reading Git reference directory"),
            }
        }
    }
    let Component::Normal(file_name) = components.last().expect("reference is nonempty") else {
        unreachable!("reference components were validated")
    };
    path.push(file_name);
    Ok(path)
}

fn read_packed_reference(git_dir: &Path, reference: &str) -> Result<Option<String>> {
    let path = git_dir.join("packed-refs");
    let Some(bytes) =
        try_read_bounded_regular_bytes(&path, "packed Git references", MAX_PACKED_REFS_BYTES)?
    else {
        return Ok(None);
    };
    let packed = std::str::from_utf8(&bytes)
        .with_context(|| format!("packed Git references '{}' are not UTF-8", path.display()))?;
    for line in packed.lines() {
        if line.starts_with(['#', '^']) {
            continue;
        }
        let Some((object, name)) = line.split_once(' ') else {
            continue;
        };
        if name == reference {
            if !valid_object_id(object) {
                bail!("packed Git reference '{reference}' has an invalid object ID");
            }
            return Ok(Some(object.to_string()));
        }
    }
    Ok(None)
}

fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn synthetic_repository_config(head: &str) -> &'static [u8] {
    if head.len() == 64 && valid_object_id(head) {
        b"[core]\n\trepositoryFormatVersion = 1\n[extensions]\n\tobjectFormat = sha256\n"
    } else {
        b""
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_mission_id() -> crate::model::MissionId {
        crate::model::MissionId::parse("mabc123def456").unwrap()
    }

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

    #[cfg(unix)]
    #[tokio::test]
    async fn disposable_git_observation_has_a_hard_deadline() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let fake_git = repo.path().join("stalling-git");
        std::fs::write(&fake_git, "#!/bin/sh\nexec sleep 60\n").unwrap();
        make_executable(&fake_git).unwrap();

        let started = tokio::time::Instant::now();
        let error = observed_git_output_with_timeout(
            repo.path(),
            &["status", "--short"],
            &fake_git,
            Duration::from_millis(20),
        )
        .await
        .expect_err("disposable observation must time out");

        assert!(error.to_string().contains("observation deadline"));
        assert!(started.elapsed() < Duration::from_secs(1));
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
        let recorded = capture_worker_result(
            repo.path(),
            &checkout,
            &base,
            &test_mission_id(),
            &effect_id,
        )
        .await
        .unwrap();
        // The recorded head is the worker's actual HEAD, and it really landed
        // in the target repo (so a later checkout succeeds).
        assert_eq!(recorded.head_sha(), detached);
        assert!(commit_exists(repo.path(), recorded.head_sha()).await);
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
        create_checkout(repo.path(), &replay, recorded.head_sha())
            .await
            .unwrap();
        assert_eq!(head_sha(&replay).await.unwrap(), recorded.head_sha());
    }

    #[tokio::test]
    async fn captured_artifacts_are_bound_to_the_issuing_effect() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();
        let mission_id = test_mission_id();
        let effect_id = EffectId::for_parts(&["test", "bound-capture"]);
        let capture = ArtifactCapture::new(
            repo.path().to_path_buf(),
            checkout,
            base.clone(),
            mission_id.clone(),
            effect_id.clone(),
        );
        let captured = capture.capture().await.unwrap();

        assert_eq!(
            captured.validate_binding(&mission_id, &effect_id, &base),
            Ok(())
        );
        assert!(captured
            .validate_binding(
                &crate::model::MissionId::parse("mdef456abc123").unwrap(),
                &effect_id,
                &base,
            )
            .is_err());
        assert!(captured
            .validate_binding(
                &mission_id,
                &EffectId::for_parts(&["test", "different-effect"]),
                &base,
            )
            .is_err());
        assert!(captured
            .validate_binding(&mission_id, &effect_id, "different-base")
            .is_err());

        let unverified = CapturedArtifact::for_testing(base.clone(), base);
        assert!(unverified
            .validate_binding(&mission_id, &effect_id, unverified.base_sha())
            .is_err());
    }

    #[tokio::test]
    async fn capture_rejects_a_clean_head_that_rolls_back_its_assignment_base() {
        let repo = tempfile::tempdir().unwrap();
        let previous = init_repo(repo.path()).await;
        let required_base = commit_change(repo.path(), "serial base\n").await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &required_base)
            .await
            .unwrap();
        git(&checkout, &["reset", "--hard", &previous])
            .await
            .unwrap();

        let effect_id = EffectId::for_parts(&["test", "history-rollback"]);
        let error = capture_worker_result(
            repo.path(),
            &checkout,
            &required_base,
            &test_mission_id(),
            &effect_id,
        )
        .await
        .expect_err("capture must reject a clean HEAD that precedes the required base");
        assert!(matches!(error, CaptureError::HistoryDiverged { .. }));
        let mission_ref = format!("refs/mission/mabc123def456/{effect_id}");
        assert!(resolve_managed_commit(repo.path(), &mission_ref)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn capture_supports_a_symbolic_head_stored_in_packed_refs() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();
        git(&checkout, &["checkout", "-q", "-b", "worker"])
            .await
            .unwrap();
        let head = commit_change(&checkout, "packed worker change\n").await;
        git(&checkout, &["pack-refs", "--all", "--prune"])
            .await
            .unwrap();
        assert!(!checkout.join(".git/refs/heads/worker").exists());

        let effect_id = EffectId::for_parts(&["test", "packed-head"]);
        let captured = capture_worker_result(
            repo.path(),
            &checkout,
            &base,
            &test_mission_id(),
            &effect_id,
        )
        .await
        .expect("a valid packed symbolic HEAD remains capturable");
        assert_eq!(captured.head_sha(), head);
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
            capture_worker_result(
                repo.path(),
                &checkout,
                &base,
                &test_mission_id(),
                &effect_id
            )
            .await,
            Err(CaptureError::DirtyWorktree(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_rejects_symlinked_object_database_descendants() {
        use std::os::unix::fs::symlink;

        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();
        let pack = checkout.join(".git/objects/pack");
        std::fs::remove_dir_all(&pack).unwrap();
        symlink(repo.path().join(".git/objects/pack"), &pack).unwrap();

        let effect_id = EffectId::for_parts(&["test", "object-pack-symlink"]);
        let error = capture_worker_result(
            repo.path(),
            &checkout,
            &base,
            &test_mission_id(),
            &effect_id,
        )
        .await
        .expect_err("capture must not pass descendant object symlinks to host Git");
        assert!(error.to_string().contains("symlink"));
    }

    #[tokio::test]
    async fn capture_never_executes_worker_git_configuration() {
        let repo = tempfile::tempdir().unwrap();
        let base = init_repo(repo.path()).await;
        let work = tempfile::tempdir().unwrap();
        let checkout = work.path().join("checkout");
        create_checkout(repo.path(), &checkout, &base)
            .await
            .unwrap();
        let marker = work.path().join("worker-config-executed");
        let hostile = work.path().join("hostile");
        std::fs::write(
            &hostile,
            format!("#!/bin/sh\ntouch '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        make_executable(&hostile).unwrap();
        git(
            &checkout,
            &["config", "core.fsmonitor", hostile.to_str().unwrap()],
        )
        .await
        .unwrap();
        git(
            &checkout,
            &[
                "config",
                "uploadpack.packObjectsHook",
                hostile.to_str().unwrap(),
            ],
        )
        .await
        .unwrap();

        let effect_id = EffectId::for_parts(&["test", "hostile-config"]);
        capture_worker_result(
            repo.path(),
            &checkout,
            &base,
            &test_mission_id(),
            &effect_id,
        )
        .await
        .unwrap();
        assert!(
            !marker.exists(),
            "capture must not execute worker status or upload-pack configuration"
        );
    }

    #[tokio::test]
    async fn task_observation_rejects_a_git_directory_pointer() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let metadata = repo.path().join("moved-git");
        std::fs::rename(repo.path().join(".git"), &metadata).unwrap();
        std::fs::write(
            repo.path().join(".git"),
            format!("gitdir: {}\n", metadata.display()),
        )
        .unwrap();

        let error = observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("task clones must use LionClaw's standalone Git topology");
        assert!(error.to_string().contains("standalone"));
    }

    #[tokio::test]
    async fn task_observation_rejects_a_common_directory_pointer() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        std::fs::write(repo.path().join(".git/commondir"), "../external\n").unwrap();

        let error = observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("task clones cannot redirect their common Git directory");
        assert!(error.to_string().contains("common-directory"));
    }

    #[tokio::test]
    async fn task_observation_rejects_an_external_object_alternate() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let external = tempfile::tempdir().unwrap();
        init_repo(external.path()).await;
        let info = repo.path().join(".git/objects/info");
        std::fs::create_dir_all(&info).unwrap();
        std::fs::write(
            info.join("alternates"),
            format!("{}\n", external.path().join(".git/objects").display()),
        )
        .unwrap();

        let error = observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("task observation must not follow an external object database");
        assert!(error.to_string().contains("alternate"));
    }

    #[tokio::test]
    async fn task_observer_construction_has_a_small_object_tree_budget() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let objects = repo.path().join(".git/objects/aa");
        std::fs::create_dir_all(&objects).unwrap();
        for index in 0..=MAX_WORKER_OBJECT_ENTRIES {
            std::fs::write(objects.join(format!("{index:08x}")), b"x").unwrap();
        }

        let started = tokio::time::Instant::now();
        let error = match TaskGitObserver::open(repo.path()).await {
            Ok(_) => panic!("oversized worker object trees must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("entry limit"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn task_observation_rejects_oversized_head_metadata() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        std::fs::write(repo.path().join(".git/HEAD"), "a".repeat(8 * 1024)).unwrap();

        let error = observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("task metadata reads must be bounded");
        assert!(error.to_string().contains("exceeds"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn task_observation_rejects_a_fifo_without_blocking() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let head = repo.path().join(".git/HEAD");
        std::fs::remove_file(&head).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &head,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();

        let started = tokio::time::Instant::now();
        observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("special Git metadata files are never opened as streams");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn task_observation_rejects_oversized_packed_refs() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        git(repo.path(), &["pack-refs", "--all", "--prune"])
            .await
            .unwrap();
        let packed = repo.path().join(".git/packed-refs");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&packed)
            .unwrap()
            .set_len(MAX_PACKED_REFS_BYTES + 1)
            .unwrap();

        let error = observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("packed reference parsing has a fixed total bound");
        assert!(error.to_string().contains("exceeds"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn task_observation_rejects_symlinked_packed_refs() {
        use std::os::unix::fs::symlink;

        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        git(repo.path(), &["pack-refs", "--all", "--prune"])
            .await
            .unwrap();
        let packed = repo.path().join(".git/packed-refs");
        let external = repo.path().join("external-packed-refs");
        std::fs::rename(&packed, &external).unwrap();
        symlink(&external, &packed).unwrap();

        observed_git_output(repo.path(), &["status", "--short"])
            .await
            .expect_err("packed references must be a no-follow regular file");
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
    async fn managed_source_operations_support_a_linked_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        let base = init_repo(&repo).await;
        let linked = temp.path().join("linked");
        git(
            &repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                linked.to_str().unwrap(),
                &base,
            ],
        )
        .await
        .unwrap();

        assert_eq!(head_sha(&linked).await.unwrap(), base);
        assert!(commit_exists(&linked, &base).await);
        assert!(is_ancestor(&linked, &base, &base).await.unwrap());
        assert!(diff(&linked, &base, &base).await.unwrap().is_empty());
        assert!(!show_commit(&linked, &base, 64 * 1024)
            .await
            .unwrap()
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commit_materialization_distinguishes_missing_and_corrupt_objects() {
        use std::os::unix::fs::PermissionsExt;

        for corrupt in [false, true] {
            let repo = tempfile::tempdir().unwrap();
            let head = init_repo(repo.path()).await;
            assert!(!show_commit(repo.path(), &head, 64 * 1024)
                .await
                .unwrap()
                .is_empty());
            let object = repo
                .path()
                .join(".git/objects")
                .join(&head[..2])
                .join(&head[2..]);
            if corrupt {
                let mut permissions = std::fs::metadata(&object).unwrap().permissions();
                permissions.set_mode(0o600);
                std::fs::set_permissions(&object, permissions).unwrap();
                std::fs::write(&object, b"invalid zlib object").unwrap();
            } else {
                std::fs::remove_file(&object).unwrap();
            }

            let error = show_commit(repo.path(), &head, 64 * 1024)
                .await
                .unwrap_err();
            let typed = error
                .downcast_ref::<CommitMaterializationError>()
                .expect("typed commit materialization error");
            assert!(matches!(
                (corrupt, typed),
                (false, CommitMaterializationError::Missing)
                    | (true, CommitMaterializationError::InvalidContent(_))
            ));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commit_materialization_classifies_an_unreadable_loose_object() {
        use std::os::unix::fs::PermissionsExt;

        let repo = tempfile::tempdir().unwrap();
        let head = init_repo(repo.path()).await;
        let object = repo
            .path()
            .join(".git/objects")
            .join(&head[..2])
            .join(&head[2..]);
        let original = std::fs::metadata(&object).unwrap().permissions();
        std::fs::set_permissions(&object, std::fs::Permissions::from_mode(0o000)).unwrap();

        let error = show_commit(repo.path(), &head, 64 * 1024)
            .await
            .expect_err("an unreadable loose object must fail closed");
        std::fs::set_permissions(&object, original).unwrap();

        assert!(matches!(
            error.downcast_ref::<CommitMaterializationError>(),
            Some(CommitMaterializationError::Unreadable(_))
        ));
    }

    #[tokio::test]
    async fn commit_materialization_ignores_replacement_refs() {
        let repo = tempfile::tempdir().unwrap();
        let original = init_repo(repo.path()).await;
        let replacement = commit_change(repo.path(), "replacement sentinel\n").await;
        let replacement_tree = git(
            repo.path(),
            &["rev-parse", &format!("{replacement}^{{tree}}")],
        )
        .await
        .unwrap();
        let replacement_root = git(
            repo.path(),
            &[
                "commit-tree",
                replacement_tree.trim(),
                "-m",
                "replacement root",
            ],
        )
        .await
        .unwrap();
        git(
            repo.path(),
            &["replace", &original, replacement_root.trim()],
        )
        .await
        .unwrap();

        let replaced = Command::new("git")
            .current_dir(repo.path())
            .args(["show", "--format=fuller", &original])
            .output()
            .await
            .unwrap();
        assert!(replaced.status.success());
        let replaced = String::from_utf8(replaced.stdout).unwrap();
        assert!(replaced.contains("replacement sentinel"));

        let materialized = show_commit(repo.path(), &original, 64 * 1024)
            .await
            .unwrap();
        let materialized = String::from_utf8(materialized).unwrap();
        assert!(materialized.contains("base"));
        assert!(!materialized.contains("replacement sentinel"));
    }

    #[tokio::test]
    async fn commit_materialization_is_isolated_from_repository_presentation_config() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let head = commit_change(repo.path(), "canonical presentation\n").await;
        let baseline = show_commit(repo.path(), &head, 64 * 1024).await.unwrap();

        for (key, value) in [
            ("diff.noprefix", "true"),
            ("diff.context", "0"),
            ("diff.algorithm", "histogram"),
            ("core.abbrev", "5"),
            ("log.decorate", "full"),
        ] {
            git(repo.path(), &["config", key, value]).await.unwrap();
        }

        assert_eq!(
            show_commit(repo.path(), &head, 64 * 1024).await.unwrap(),
            baseline,
            "repository-local presentation config must not alter prompt bytes"
        );
    }

    #[tokio::test]
    async fn commit_materialization_is_isolated_from_ambient_attributes() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let target = commit_change(repo.path(), "ambient attribute sentinel\n").await;
        let baseline = show_commit(repo.path(), &target, 64 * 1024).await.unwrap();

        std::fs::write(repo.path().join(".gitattributes"), "*.txt -diff\n").unwrap();
        git(repo.path(), &["add", ".gitattributes"]).await.unwrap();
        git(repo.path(), &["commit", "-q", "-m", "hostile attributes"])
            .await
            .unwrap();
        let attribute_source = head_sha(repo.path()).await.unwrap();
        let xdg = tempfile::tempdir().unwrap();
        let user_attributes = xdg.path().join("git/attributes");
        std::fs::create_dir_all(user_attributes.parent().unwrap()).unwrap();
        std::fs::write(&user_attributes, "*.txt -diff\n").unwrap();

        let observer = CommitGitObserver::open(repo.path(), &target).await.unwrap();
        let mut hostile = observer.command();
        hostile
            .env("GIT_ATTR_SOURCE", &attribute_source)
            .env("XDG_CONFIG_HOME", xdg.path());
        configure_commit_show(&mut hostile, &target);
        let hostile = run_git_bounded(&mut hostile, None, 64 * 1024, MAX_GIT_DIAGNOSTIC_BYTES)
            .await
            .unwrap();
        assert!(hostile.status.success());
        assert_ne!(
            hostile.stdout, baseline,
            "the hostile attribute fixtures must affect an unprotected command"
        );

        let mut protected = Command::new("git");
        protected
            .env("GIT_ATTR_SOURCE", &attribute_source)
            .env("XDG_CONFIG_HOME", xdg.path());
        configure_isolated_git_command(&mut protected, &observer.metadata, &observer.objects);
        configure_commit_show(&mut protected, &target);
        let protected = run_git_bounded(&mut protected, None, 64 * 1024, MAX_GIT_DIAGNOSTIC_BYTES)
            .await
            .unwrap();
        assert!(protected.status.success());
        assert_eq!(protected.stdout, baseline);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commit_materialization_never_runs_textconv() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        let marker = repo.path().join("textconv-ran");
        let textconv = repo.path().join("textconv");
        std::fs::write(
            &textconv,
            format!(
                "#!/bin/sh\nprintf converted\nprintf ran > '{}'\n",
                marker.display()
            ),
        )
        .unwrap();
        make_executable(&textconv).unwrap();
        std::fs::write(repo.path().join("opaque.bin"), b"raw sentinel\n").unwrap();
        std::fs::write(repo.path().join(".gitattributes"), "*.bin diff=custom\n").unwrap();
        git(
            repo.path(),
            &["config", "diff.custom.textconv", textconv.to_str().unwrap()],
        )
        .await
        .unwrap();
        git(repo.path(), &["add", "opaque.bin", ".gitattributes"])
            .await
            .unwrap();
        git(repo.path(), &["commit", "-q", "-m", "textconv fixture"])
            .await
            .unwrap();
        let head = head_sha(repo.path()).await.unwrap();

        let converted = git(repo.path(), &["show", "--textconv", &head])
            .await
            .unwrap();
        assert!(converted.contains("converted"));
        assert!(
            marker.exists(),
            "the configured converter is a real fixture"
        );
        std::fs::remove_file(&marker).unwrap();

        let materialized = show_commit(repo.path(), &head, 64 * 1024).await.unwrap();
        assert!(String::from_utf8(materialized)
            .unwrap()
            .contains("raw sentinel"));
        assert!(
            !marker.exists(),
            "exact materialization must disable textconv"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bounded_git_overflow_kills_and_reaps_the_process() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pid");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            &format!("printf %s $$ > '{}'; exec yes", pid_file.display()),
        ]);

        let error = run_git_bounded(&mut command, None, 1024, 1024)
            .await
            .expect_err("unbounded output must be rejected");
        assert!(matches!(error, BoundedGitError::Limit(GitStream::Stdout)));
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        assert!(
            !Path::new("/proc").join(pid).exists(),
            "overflowing process must be reaped before return"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bounded_git_deadline_kills_and_reaps_the_process() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pid");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            &format!("printf %s $$ > '{}'; exec sleep 60", pid_file.display()),
        ]);

        let error =
            run_git_bounded_with_timeout(&mut command, None, 1024, 1024, Duration::from_millis(20))
                .await
                .expect_err("a stalled Git process must time out");
        assert!(matches!(error, BoundedGitError::TimedOut));
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        assert!(
            !Path::new("/proc").join(pid).exists(),
            "timed-out process must be reaped before return"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commit_materialization_rejects_a_nonregular_loose_object() {
        let repo = tempfile::tempdir().unwrap();
        let head = init_repo(repo.path()).await;
        let object = repo
            .path()
            .join(".git/objects")
            .join(&head[..2])
            .join(&head[2..]);
        std::fs::remove_file(&object).unwrap();
        let status = std::process::Command::new("mkfifo")
            .arg(&object)
            .status()
            .unwrap();
        assert!(status.success());

        let error = show_commit(repo.path(), &head, 64 * 1024)
            .await
            .expect_err("a special-file object must fail before Git opens it");
        assert!(matches!(
            error.downcast_ref::<CommitMaterializationError>(),
            Some(CommitMaterializationError::InvalidContent(_))
        ));
    }

    #[tokio::test]
    async fn commit_materialization_rejects_oversized_output_at_the_process_boundary() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path()).await;
        std::fs::write(repo.path().join("large.txt"), "x".repeat(16 * 1024)).unwrap();
        git(repo.path(), &["add", "large.txt"]).await.unwrap();
        git(repo.path(), &["commit", "-q", "-m", "large commit"])
            .await
            .unwrap();
        let head = head_sha(repo.path()).await.unwrap();

        let error = show_commit(repo.path(), &head, 1024)
            .await
            .expect_err("Git output must be bounded while it is read");
        assert!(matches!(
            error.downcast_ref::<CommitMaterializationError>(),
            Some(CommitMaterializationError::ExpansionLimit)
        ));
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
    async fn checkout_transfers_source_objects_into_an_independent_pack() {
        let repo = tempfile::tempdir().unwrap();
        let head = init_repo(repo.path()).await;
        let attempt = tempfile::tempdir().unwrap();
        let checkout = attempt.path().join("checkout");

        create_checkout(repo.path(), &checkout, &head)
            .await
            .unwrap();

        let source_loose_object = Path::new("objects").join(&head[..2]).join(&head[2..]);
        assert!(repo
            .path()
            .join(".git")
            .join(&source_loose_object)
            .is_file());
        assert!(
            !checkout.join(".git").join(&source_loose_object).exists(),
            "normal transport must not copy the source's loose-object topology"
        );
        assert!(
            std::fs::read_dir(checkout.join(".git/objects/pack"))
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("pack")),
            "normal transport must materialize the reachable source history as a pack"
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

    #[tokio::test]
    async fn replacement_recovers_an_interrupted_swap_before_publishing_the_new_base() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        let base = init_repo(&repo).await;
        let checkout = temp.path().join("work");
        replace_checkout(&repo, &checkout, &base).await.unwrap();

        let next = commit_change(&repo, "next\n").await;
        let previous = temp.path().join(".work.previous");
        let staging = temp.path().join(".work.prepare");
        std::fs::rename(&checkout, &previous).unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("partial"), "interrupted clone").unwrap();

        replace_checkout(&repo, &checkout, &next).await.unwrap();
        assert_eq!(head_sha(&checkout).await.unwrap(), next);
        assert!(!previous.exists());
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn isolated_observation_and_capture_support_sha256_repositories() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        let mut init = managed_git_command();
        init.args(["init", "--quiet", "--object-format=sha256"])
            .arg(&repo);
        run(&mut init, "initialize SHA-256 repository")
            .await
            .unwrap();
        git(&repo, &["config", "user.name", "test"]).await.unwrap();
        git(&repo, &["config", "user.email", "test@local"])
            .await
            .unwrap();
        git(&repo, &["config", "commit.gpgsign", "false"])
            .await
            .unwrap();
        std::fs::write(repo.join("f.txt"), "base\n").unwrap();
        git(&repo, &["add", "f.txt"]).await.unwrap();
        git(&repo, &["commit", "--quiet", "-m", "base"])
            .await
            .unwrap();
        let base = git(&repo, &["rev-parse", "HEAD"]).await.unwrap();
        assert_eq!(base.trim().len(), 64);
        assert_eq!(head_sha(&repo).await.unwrap(), base.trim());
        assert!(!task_is_dirty(&repo).await.unwrap());
        assert!(diff(&repo, base.trim(), base.trim())
            .await
            .unwrap()
            .is_empty());

        let checkout = temp.path().join("checkout");
        create_checkout(&repo, &checkout, base.trim())
            .await
            .unwrap();
        let effect_id = EffectId::for_parts(&["test", "sha256-capture"]);
        let captured = capture_worker_result(
            &repo,
            &checkout,
            base.trim(),
            &test_mission_id(),
            &effect_id,
        )
        .await
        .unwrap();
        assert_eq!(captured.head_sha(), base.trim());
    }
}
