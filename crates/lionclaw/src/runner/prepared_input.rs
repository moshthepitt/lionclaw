use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeProgramExecutor};

use crate::authority::{
    compile_role_plan, prepared_input_authority, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::mission_type::{ContentDigest, PreparedInput};
use crate::model::PreparedInputRef;

use super::{MissionProgramExecutor, SCRATCH_MOUNT_TARGET};

const INPUT_PROGRAM_TARGET: &str = "/mission/input/prepare";
const INPUT_OUTPUT_TARGET: &str = "/output";
pub(crate) const INPUTS_MOUNT_TARGET: &str = "/inputs";

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
    let mut digest = ContentDigest::new();
    digest.feed("format", b"lionclaw-prepared-input-v1", false);
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
    digest.feed("program", &tokio::fs::read(&input.program).await?, true);
    for (name, value) in &input.environment {
        digest.feed(&format!("environment/{name}"), value.as_bytes(), false);
    }
    for key in &input.key_files {
        feed_key_path(&mut digest, checkout, key, key).await?;
    }
    Ok(digest.finish())
}

fn feed_key_path<'a>(
    digest: &'a mut ContentDigest,
    checkout: &'a Path,
    declared: &'a Path,
    relative: &'a Path,
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
        let logical = format!("key/{}", relative.to_string_lossy());
        if metadata.is_file() {
            use std::os::unix::fs::PermissionsExt;
            digest.feed(
                &logical,
                &tokio::fs::read(&path).await?,
                metadata.permissions().mode() & 0o111 != 0,
            );
            return Ok(());
        }
        if !metadata.is_dir() {
            bail!(
                "prepared-input key '{}' contains a non-file entry at '{}'",
                declared.display(),
                relative.display()
            );
        }
        digest.feed(&format!("{logical}/"), b"directory", false);
        let mut directory = tokio::fs::read_dir(&path).await?;
        let mut entries = Vec::new();
        while let Some(entry) = directory.next_entry().await? {
            entries.push(entry.file_name());
        }
        entries.sort();
        for entry in entries {
            let child = relative.join(entry);
            feed_key_path(digest, checkout, declared, &child).await?;
        }
        Ok(())
    })
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
    let output = staging.join("output");
    let scratch = staging.join("scratch");
    let program_dir = staging.join("program");
    for directory in [&output, &scratch, &program_dir] {
        tokio::fs::create_dir_all(directory).await?;
    }
    let program = program_dir.join("prepare");
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
                    source: program_dir,
                    target: "/mission/input".to_string(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: output.clone(),
                    target: INPUT_OUTPUT_TARGET.to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: scratch,
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

    use super::*;
    use crate::config::RuntimeProfiles;
    use crate::model::InputName;

    #[tokio::test]
    async fn cache_key_changes_with_declared_content_and_rejects_symlinks() {
        let checkout = tempfile::tempdir().unwrap();
        let program_dir = tempfile::tempdir().unwrap();
        let program = program_dir.path().join("prepare");
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        std::fs::write(checkout.path().join("lock"), "one").unwrap();
        let profile = RuntimeProfiles::from_toml(
            "[runtimes.test]\ndriver = \"acp\"\ncommand = \"test\"\n",
            Path::new("/home/test"),
        )
        .unwrap()
        .get("test")
        .unwrap();
        let input = PreparedInput {
            name: InputName::new("deps").unwrap(),
            program,
            network: true,
            key_files: vec![PathBuf::from("lock")],
            environment: BTreeMap::new(),
        };

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
}
