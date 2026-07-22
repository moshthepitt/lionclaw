use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeProgramExecutor};
use tokio::io::AsyncReadExt;

use crate::authority::{
    compile_role_plan, prepared_input_authority, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::mission_type::{ContentDigest, PreparedInput, MAX_PREPARED_INPUT_CONTENT_BYTES};
use crate::model::PreparedInputRef;

use super::{MissionProgramExecutor, SCRATCH_MOUNT_TARGET};

const INPUT_PROGRAM_TARGET: &str = "/mission/input/prepare";
const INPUT_OUTPUT_TARGET: &str = "/output";
pub(crate) const INPUTS_MOUNT_TARGET: &str = "/inputs";
const MAX_KEY_ENTRIES: usize = 8 * 1024;
const MAX_KEY_DEPTH: usize = 64;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
// Prepared outputs are immutable, so format changes supersede known-bad prior
// evidence with a new content identity instead of mutating it in place.
const PREPARED_INPUT_FORMAT: &[u8] = b"lionclaw-prepared-input-v2";

#[derive(Default)]
struct InputHashBudget {
    bytes: u64,
    entries: usize,
}

impl InputHashBudget {
    fn account_bytes(&mut self, kind: &str, path: &Path, len: u64) -> Result<()> {
        self.bytes = self.bytes.checked_add(len).with_context(|| {
            format!("prepared-input {kind} byte count overflowed while applying the hashing limit")
        })?;
        if self.bytes > MAX_PREPARED_INPUT_CONTENT_BYTES {
            bail!(
                "prepared-input {kind} '{}' exceeds the {} byte limit",
                path.display(),
                MAX_PREPARED_INPUT_CONTENT_BYTES
            );
        }
        Ok(())
    }

    fn account(&mut self, declared: &Path, relative: &Path, depth: usize, len: u64) -> Result<()> {
        if depth > MAX_KEY_DEPTH {
            bail!(
                "prepared-input key '{}' exceeds the depth limit at '{}'",
                declared.display(),
                relative.display()
            );
        }
        self.entries = self.entries.saturating_add(1);
        if self.entries > MAX_KEY_ENTRIES {
            bail!(
                "prepared-input key '{}' exceeds the {} entry limit",
                declared.display(),
                MAX_KEY_ENTRIES
            );
        }
        self.account_bytes("key", declared, len)
    }
}

pub(crate) struct PreparedInputs {
    pub mounts: Vec<MountSpec>,
    pub environment: Vec<(String, String)>,
    pub refs: Vec<PreparedInputRef>,
}

pub(crate) async fn prepare_inputs(
    profile: &MissionRuntimeProfile,
    state_dir: &Path,
    attempt_dir: &Path,
    checkout: &Path,
    inputs: &[PreparedInput],
    effect_id: &crate::model::EffectId,
) -> Result<PreparedInputs> {
    let mut prepared = PreparedInputs {
        mounts: Vec::new(),
        environment: Vec::new(),
        refs: Vec::new(),
    };
    for input in inputs {
        let digest = input_cache_key(profile, checkout, input).await?;
        let directory = prepare_one(
            profile,
            state_dir,
            attempt_dir,
            checkout,
            input,
            &digest,
            effect_id,
        )
        .await?;
        prepared.mounts.push(MountSpec {
            source: directory,
            target: format!("{INPUTS_MOUNT_TARGET}/{}", input.name),
            access: MountAccess::ReadOnly,
        });
        prepared.environment.extend(input.environment.clone());
        prepared.refs.push(PreparedInputRef {
            name: input.name.clone(),
            digest,
        });
    }
    Ok(prepared)
}

async fn input_cache_key(
    profile: &MissionRuntimeProfile,
    checkout: &Path,
    input: &PreparedInput,
) -> Result<String> {
    input_cache_key_for_format(profile, checkout, input, PREPARED_INPUT_FORMAT).await
}

async fn input_cache_key_for_format(
    profile: &MissionRuntimeProfile,
    checkout: &Path,
    input: &PreparedInput,
    format: &[u8],
) -> Result<String> {
    let mut digest = ContentDigest::new();
    digest.feed("format", format, false);
    digest.feed("name", input.name.as_str().as_bytes(), false);
    digest.feed(
        "image",
        profile
            .confinement
            .oci()
            .image
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        false,
    );
    digest.feed("network", &[u8::from(input.network)], false);
    let mut budget = InputHashBudget::default();
    let program_metadata = tokio::fs::symlink_metadata(&input.program)
        .await
        .with_context(|| {
            format!(
                "reading prepared-input program '{}'",
                input.program.display()
            )
        })?;
    if program_metadata.file_type().is_symlink() || !program_metadata.is_file() {
        bail!(
            "prepared-input program '{}' is not a regular file",
            input.program.display()
        );
    }
    budget.account_bytes("program", &input.program, program_metadata.len())?;
    feed_regular_file(
        &mut digest,
        b"program",
        &input.program,
        &program_metadata,
        true,
        "prepared-input program",
    )
    .await?;
    for (name, value) in &input.environment {
        digest.feed(&format!("environment/{name}"), value.as_bytes(), false);
    }
    for key in &input.key_files {
        feed_key_path(&mut digest, checkout, key, key, 0, &mut budget).await?;
    }
    Ok(digest.finish())
}

fn feed_key_path<'a>(
    digest: &'a mut ContentDigest,
    checkout: &'a Path,
    declared: &'a Path,
    relative: &'a Path,
    depth: usize,
    budget: &'a mut InputHashBudget,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let path = checkout.join(relative);
        let metadata = tokio::fs::symlink_metadata(&path)
            .await
            .with_context(|| format!("reading prepared-input key '{}'", declared.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "prepared-input key '{}' contains a symlink at '{}'",
                declared.display(),
                relative.display()
            );
        }
        let mut logical = b"key/".to_vec();
        logical.extend_from_slice(relative.as_os_str().as_bytes());
        if metadata.is_file() {
            use std::os::unix::fs::PermissionsExt;
            budget.account(declared, relative, depth, metadata.len())?;
            feed_regular_file(
                digest,
                &logical,
                &path,
                &metadata,
                metadata.permissions().mode() & 0o111 != 0,
                "prepared-input key",
            )
            .await?;
            return Ok(());
        }
        if !metadata.is_dir() {
            bail!(
                "prepared-input key '{}' contains a non-file entry at '{}'",
                declared.display(),
                relative.display()
            );
        }
        budget.account(declared, relative, depth, 0)?;
        logical.push(b'/');
        digest.feed_bytes(&logical, b"directory", false);
        let mut directory = tokio::fs::read_dir(&path).await?;
        let mut entries = Vec::new();
        while let Some(entry) = directory.next_entry().await? {
            if entries.len() >= MAX_KEY_ENTRIES {
                bail!(
                    "prepared-input key '{}' exceeds the {} entry limit",
                    declared.display(),
                    MAX_KEY_ENTRIES
                );
            }
            entries.push(entry.file_name());
        }
        entries.sort();
        for entry in entries {
            let child = relative.join(entry);
            feed_key_path(digest, checkout, declared, &child, depth + 1, budget).await?;
        }
        Ok(())
    })
}

async fn feed_regular_file(
    digest: &mut ContentDigest,
    logical: &[u8],
    path: &Path,
    expected: &std::fs::Metadata,
    executable: bool,
    kind: &str,
) -> Result<()> {
    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK,
        rustix::fs::Mode::empty(),
    )
    .with_context(|| format!("opening {kind} '{}'", path.display()))?;
    let file = std::fs::File::from(descriptor);
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != expected.len() {
        bail!(
            "{kind} '{}' changed while it was being hashed",
            path.display()
        );
    }
    digest.feed_bytes_header(logical, opened.len(), executable);
    let mut file = tokio::fs::File::from_std(file);
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut read = 0_u64;
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        read = read.saturating_add(count as u64);
        if read > opened.len() {
            bail!("{kind} '{}' grew while it was being hashed", path.display());
        }
        digest.feed_chunk(&buffer[..count]);
        tokio::task::yield_now().await;
    }
    if read != opened.len() {
        bail!(
            "{kind} '{}' shrank while it was being hashed",
            path.display()
        );
    }
    Ok(())
}

async fn prepare_one(
    profile: &MissionRuntimeProfile,
    state_dir: &Path,
    attempt_dir: &Path,
    checkout: &Path,
    input: &PreparedInput,
    digest: &str,
    effect_id: &crate::model::EffectId,
) -> Result<PathBuf> {
    prepare_cached(
        state_dir,
        attempt_dir,
        input,
        digest,
        |staging| async move {
            run_preparation_program(profile, checkout, input, effect_id, &staging).await
        },
    )
    .await
}

struct PreparationStaging {
    output: PathBuf,
    scratch: PathBuf,
    program_dir: PathBuf,
}

async fn prepare_cached<F, Fut>(
    state_dir: &Path,
    attempt_dir: &Path,
    input: &PreparedInput,
    digest: &str,
    prepare: F,
) -> Result<PathBuf>
where
    F: FnOnce(PreparationStaging) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let parent = state_dir
        .join("inputs")
        .join("sha256")
        .join(&digest[..2])
        .join(&digest[2..4]);
    let destination = parent.join(digest);
    if destination.is_dir() {
        return Ok(destination);
    }
    if destination.exists() {
        bail!(
            "prepared-input cache entry '{}' is not a directory",
            destination.display()
        );
    }
    tokio::fs::create_dir_all(&parent).await?;
    let staging = attempt_dir.join(format!("input-{}", input.name));
    crate::workspace::remove_dir(&staging).await?;
    let staging = PreparationStaging {
        output: staging.join("output"),
        scratch: staging.join("scratch"),
        program_dir: staging.join("program"),
    };
    for directory in [&staging.output, &staging.scratch, &staging.program_dir] {
        tokio::fs::create_dir_all(directory).await?;
    }
    let output = staging.output.clone();
    prepare(staging).await?;

    match std::fs::rename(&output, &destination) {
        Ok(()) => {}
        Err(_) if destination.is_dir() => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "publishing prepared input '{}' at '{}'",
                    input.name,
                    destination.display()
                )
            });
        }
    }
    Ok(destination)
}

async fn run_preparation_program(
    profile: &MissionRuntimeProfile,
    checkout: &Path,
    input: &PreparedInput,
    effect_id: &crate::model::EffectId,
    staging: &PreparationStaging,
) -> Result<()> {
    let program = staging.program_dir.join("prepare");
    tokio::fs::copy(&input.program, &program).await?;
    crate::workspace::make_executable(&program)?;

    let authority = prepared_input_authority(input.name.as_str(), input.network);
    let judged_roots = [crate::authority::canonical_or_lexical(checkout)];
    let compiled = compile_role_plan(RolePlanRequest {
        authority: &authority,
        runtime_id: profile.name.clone(),
        confinement: profile.confinement.clone(),
        mounts: MissionMounts {
            workspace: checkout.to_path_buf(),
            extras: vec![
                MountSpec {
                    source: staging.program_dir.clone(),
                    target: "/mission/input".to_string(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: staging.output.clone(),
                    target: INPUT_OUTPUT_TARGET.to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: staging.scratch.clone(),
                    target: SCRATCH_MOUNT_TARGET.to_string(),
                    access: MountAccess::ReadWrite,
                },
            ],
        },
        judged_roots: &judged_roots,
        environment: preparation_environment(),
    })
    .map_err(|error| anyhow::anyhow!("prepared-input plan refused to compile: {error}"))?;
    let program = RuntimeProgramSpec {
        executable: INPUT_PROGRAM_TARGET.to_string(),
        args: Vec::new(),
        environment: Vec::new(),
        stdin: String::new(),
        auth: None,
    };
    let mut executor = MissionProgramExecutor::new(
        compiled.plan().clone(),
        RuntimeAuthRegistry::empty(),
        effect_id,
    );
    let run = executor.execute_captured(program).await?;
    if run.exit_code != Some(0) || run.exit_signal.is_some() {
        let stdout = crate::evidence::excerpt(&String::from_utf8_lossy(&run.stdout));
        let stderr = crate::evidence::excerpt(&String::from_utf8_lossy(&run.stderr));
        bail!(
            "prepared input '{}' failed (exit {:?}, signal {:?})\nstdout:\n{}\nstderr:\n{}",
            input.name,
            run.exit_code,
            run.exit_signal,
            stdout,
            stderr
        );
    }

    Ok(())
}

fn preparation_environment() -> Vec<(String, String)> {
    vec![
        ("HOME".to_string(), SCRATCH_MOUNT_TARGET.to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
        (
            "LIONCLAW_OUTPUT".to_string(),
            INPUT_OUTPUT_TARGET.to_string(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::ffi::OsStringExt;

    use super::*;
    use crate::config::RuntimeProfiles;
    use crate::model::InputName;

    fn profile() -> MissionRuntimeProfile {
        RuntimeProfiles::from_toml(
            "[runtimes.test]\ndriver = \"acp\"\ncommand = \"test\"\n",
            Path::new("/home/test"),
        )
        .unwrap()
        .get("test")
        .unwrap()
    }

    fn input(program: PathBuf, key: &str) -> PreparedInput {
        PreparedInput {
            name: InputName::new("deps").unwrap(),
            program,
            network: true,
            key_files: vec![PathBuf::from(key)],
            environment: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn cache_key_changes_with_declared_content_and_rejects_symlinks() {
        let checkout = tempfile::tempdir().unwrap();
        let program_dir = tempfile::tempdir().unwrap();
        let program = program_dir.path().join("prepare");
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        std::fs::write(checkout.path().join("lock"), "one").unwrap();
        let profile = profile();
        let input = input(program, "lock");

        let first = input_cache_key(&profile, checkout.path(), &input)
            .await
            .unwrap();
        std::fs::write(checkout.path().join("lock"), "two").unwrap();
        let second = input_cache_key(&profile, checkout.path(), &input)
            .await
            .unwrap();
        assert_ne!(first, second);

        std::fs::remove_file(checkout.path().join("lock")).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", checkout.path().join("lock")).unwrap();
        assert!(input_cache_key(&profile, checkout.path(), &input)
            .await
            .unwrap_err()
            .to_string()
            .contains("symlink"));
    }

    #[tokio::test]
    async fn cache_key_distinguishes_raw_non_utf8_descendant_names() {
        let checkout = tempfile::tempdir().unwrap();
        let program_dir = tempfile::tempdir().unwrap();
        let program = program_dir.path().join("prepare");
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        let tree = checkout.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        let raw = tree.join(std::ffi::OsString::from_vec(vec![0x80]));
        std::fs::write(&raw, "same").unwrap();
        let profile = profile();
        let input = input(program, "tree");
        let raw_digest = input_cache_key(&profile, checkout.path(), &input)
            .await
            .unwrap();

        std::fs::remove_file(raw).unwrap();
        std::fs::write(tree.join("\u{fffd}"), "same").unwrap();
        let utf8_digest = input_cache_key(&profile, checkout.path(), &input)
            .await
            .unwrap();

        assert_ne!(raw_digest, utf8_digest, "path identity must be byte-exact");
    }

    #[tokio::test]
    async fn cache_key_rejects_oversized_declared_content_without_reading_it_whole() {
        let checkout = tempfile::tempdir().unwrap();
        let program_dir = tempfile::tempdir().unwrap();
        let program = program_dir.path().join("prepare");
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        let oversized = checkout.path().join("oversized");
        std::fs::File::create(&oversized)
            .unwrap()
            .set_len(MAX_PREPARED_INPUT_CONTENT_BYTES + 1)
            .unwrap();

        let error = input_cache_key(&profile(), checkout.path(), &input(program, "oversized"))
            .await
            .expect_err("prepared-input content has one aggregate byte ceiling");
        assert!(error.to_string().contains("byte limit"));
    }

    #[tokio::test]
    async fn cache_key_rejects_an_oversized_program_before_reading_it() {
        let checkout = tempfile::tempdir().unwrap();
        let program_dir = tempfile::tempdir().unwrap();
        let program = program_dir.path().join("prepare");
        std::fs::File::create(&program)
            .unwrap()
            .set_len(MAX_PREPARED_INPUT_CONTENT_BYTES + 1)
            .unwrap();

        let error = input_cache_key(&profile(), checkout.path(), &input(program, "missing"))
            .await
            .expect_err("program and key content share one aggregate byte ceiling");
        assert!(
            error.to_string().contains("program") && error.to_string().contains("byte limit"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn input_hash_budget_enforces_aggregate_entry_and_depth_limits() {
        let declared = Path::new("tree");
        let mut bytes = InputHashBudget::default();
        bytes
            .account_bytes(
                "program",
                Path::new("prepare"),
                MAX_PREPARED_INPUT_CONTENT_BYTES,
            )
            .unwrap();
        assert!(bytes
            .account_bytes("key", declared, 1)
            .unwrap_err()
            .to_string()
            .contains("byte limit"));

        let mut entries = InputHashBudget::default();
        for index in 0..MAX_KEY_ENTRIES {
            entries
                .account(declared, Path::new("entry"), 0, u64::from(index == 0))
                .unwrap();
        }
        assert!(entries
            .account(declared, Path::new("extra"), 0, 0)
            .unwrap_err()
            .to_string()
            .contains("entry limit"));

        assert!(InputHashBudget::default()
            .account(declared, Path::new("deep"), MAX_KEY_DEPTH + 1, 0)
            .unwrap_err()
            .to_string()
            .contains("depth limit"));
    }

    #[tokio::test]
    async fn prior_format_cache_is_regenerated_and_v2_cache_is_immutable() {
        let root = tempfile::tempdir().unwrap();
        let checkout = root.path().join("checkout");
        let state = root.path().join("state");
        let attempt = root.path().join("attempt");
        std::fs::create_dir_all(&checkout).unwrap();
        std::fs::create_dir_all(&attempt).unwrap();
        let program = root.path().join("prepare");
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        std::fs::write(checkout.join("Cargo.lock"), "key-v1").unwrap();
        let input = input(program, "Cargo.lock");
        let profile = profile();

        let old_digest =
            input_cache_key_for_format(&profile, &checkout, &input, b"lionclaw-prepared-input-v1")
                .await
                .unwrap();
        let old = state
            .join("inputs/sha256")
            .join(&old_digest[..2])
            .join(&old_digest[2..4])
            .join(&old_digest);
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("complete"), "stale contents").unwrap();

        let digest = input_cache_key(&profile, &checkout, &input).await.unwrap();
        assert_ne!(digest, old_digest);
        let runs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = runs.clone();
        let published = prepare_cached(&state, &attempt, &input, &digest, move |staging| {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                let package = staging.output.join("vendor/representative/src");
                tokio::fs::create_dir_all(&package).await?;
                tokio::fs::write(
                    staging.output.join("vendor/representative/Cargo.toml"),
                    "[package]\nname='representative'\nversion='1.0.0'\n",
                )
                .await?;
                tokio::fs::write(package.join("lib.rs"), "pub fn retained() {}\n").await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(published.file_name().unwrap(), digest.as_str());
        assert!(published.join("vendor/representative/Cargo.toml").is_file());
        assert!(published.join("vendor/representative/src/lib.rs").is_file());
        assert_eq!(
            std::fs::read_to_string(old.join("complete")).unwrap(),
            "stale contents"
        );

        let cached = prepare_cached(&state, &attempt, &input, &digest, |_| async {
            panic!("an unchanged complete v2 input must be a stable cache hit")
        })
        .await
        .unwrap();
        assert_eq!(cached, published);

        std::fs::write(checkout.join("Cargo.lock"), "key-v2").unwrap();
        let changed = input_cache_key(&profile, &checkout, &input).await.unwrap();
        assert_ne!(changed, digest);
        let observed = runs.clone();
        let changed_path = prepare_cached(&state, &attempt, &input, &changed, move |staging| {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                tokio::fs::write(staging.output.join("regenerated"), "new key").await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_ne!(changed_path, published);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
