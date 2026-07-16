//! Git workspace isolation for confined runs.
//!
//! Every role and oracle gets the same self-contained checkout at its exact
//! base commit. The authority compiler decides whether that checkout is mounted
//! read-write or read-only; workspace materialization has no role policy of its
//! own. `--no-hardlinks --dissociate` keeps confined Git objects independent
//! from the user repository and any object store it borrows from, including
//! under OCI relabeling. Artifact-producing output is a recorded commit fetched
//! back under `refs/mission/…`, never auto-applied.

use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rustix::fs::{open, Mode, OFlags};
use rustix::io::Errno;

use crate::model::EffectId;
use tokio::process::Command;

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
pub async fn capture_worker_result(
    repo: &Path,
    checkout: &Path,
    required_base: &str,
    mission_id: &str,
    effect_id: &EffectId,
) -> Result<String, CaptureError> {
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
        .arg(fetch_source)
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
        .args(["-c", "core.fsmonitor=false"])
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(["-c", "core.pager=cat"])
        .args(["-c", "diff.external="])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
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
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_EXTERNAL_DIFF",
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
        tokio::task::spawn_blocking(move || Self::open_blocking(&worktree))
            .await
            .context("joining task Git metadata observer")?
    }

    fn open_blocking(worktree: &Path) -> Result<Self> {
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
        let index = optional_regular_file(&git_dir.join("index"), "Git index")?;
        let objects = directory(&git_dir.join("objects"), "Git object database")?;
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
        let observer = Self {
            metadata,
            worktree: worktree.to_path_buf(),
            index,
            objects,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&observer.metadata, std::fs::Permissions::from_mode(0o700))?;
        }
        std::fs::create_dir(observer.metadata.join("objects"))?;
        std::fs::create_dir(observer.metadata.join("refs"))?;
        std::fs::write(observer.metadata.join("HEAD"), format!("{head}\n"))?;
        std::fs::write(
            observer.metadata.join("config"),
            synthetic_repository_config(&head),
        )?;
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
        clear_git_authority_environment(command);
        command
            .kill_on_drop(true)
            .arg("--no-optional-locks")
            .env("GIT_DIR", &self.metadata)
            .env("GIT_WORK_TREE", &self.worktree)
            .env("GIT_INDEX_FILE", &self.index)
            .env("GIT_OBJECT_DIRECTORY", &self.objects)
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG", "/dev/null");
    }
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

fn optional_regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            Ok(path.to_path_buf())
        }
        Ok(_) => bail!("{label} '{}' is not a regular file", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path.to_path_buf()),
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
        let recorded =
            capture_worker_result(repo.path(), &checkout, &base, "mabc123def456", &effect_id)
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
            "mabc123def456",
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
        let captured =
            capture_worker_result(repo.path(), &checkout, &base, "mabc123def456", &effect_id)
                .await
                .expect("a valid packed symbolic HEAD remains capturable");
        assert_eq!(captured, head);
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
            capture_worker_result(repo.path(), &checkout, &base, "mabc123def456", &effect_id).await,
            Err(CaptureError::DirtyWorktree(_))
        ));
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
        capture_worker_result(repo.path(), &checkout, &base, "mabc123def456", &effect_id)
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
        let captured =
            capture_worker_result(&repo, &checkout, base.trim(), "mabc123def456", &effect_id)
                .await
                .unwrap();
        assert_eq!(captured, base.trim());
    }
}
