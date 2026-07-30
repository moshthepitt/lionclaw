use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use lionclaw_durable_fs::RootedDirectory;
use lionclaw_runtime_api::RuntimeResumeMode;
use sha2::{Digest, Sha256};

use super::OciRoleRunner;
use crate::authority::AuthorityCeiling;
use crate::config::RuntimeProfiles;
use crate::model::{
    AuthorityGrants, EffectId, MissionId, OutputSemantics, RoleInstance, RoleInstanceId,
};
use crate::ports::{
    EffectCleaner, EffectCleanupRequest, ExecutionControl, RoleRunner, RoleTurnOutcome,
    RoleTurnRequest,
};
use crate::resources::MissionDirs;

const REAL_TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const REAL_CANCELLATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const REAL_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_PROOF_BINARY_BYTES: u64 = 1024 * 1024 * 1024;

fn sha256_regular_file(path: &Path, label: &str) -> String {
    let metadata = std::fs::symlink_metadata(path)
        .unwrap_or_else(|error| panic!("inspecting {label} '{}': {error}", path.display()));
    assert!(
        metadata.file_type().is_file(),
        "{label} '{}' is not a regular file",
        path.display()
    );
    assert!(
        metadata.len() <= MAX_PROOF_BINARY_BYTES,
        "{label} '{}' is {} bytes; proof limit is {MAX_PROOF_BINARY_BYTES}",
        path.display(),
        metadata.len()
    );
    let mut file = std::fs::File::open(path)
        .unwrap_or_else(|error| panic!("opening {label} '{}': {error}", path.display()));
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut observed = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .unwrap_or_else(|error| panic!("reading {label} '{}': {error}", path.display()));
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(read as u64)
            .expect("proof binary byte count overflowed");
        assert!(
            observed <= MAX_PROOF_BINARY_BYTES,
            "{label} '{}' grew beyond the proof limit while hashing",
            path.display()
        );
        digest.update(&buffer[..read]);
    }
    assert_eq!(
        observed,
        metadata.len(),
        "{label} '{}' changed while it was hashed",
        path.display()
    );
    hex::encode(digest.finalize())
}

async fn await_bounded_cleanup(
    runtime: &str,
    effect: &str,
    proof_root: &Path,
    cleanup: impl std::future::Future<Output = ()>,
) {
    assert!(
        tokio::time::timeout(REAL_CLEANUP_TIMEOUT, cleanup)
            .await
            .is_ok(),
        "{runtime} {effect} cleanup exceeded its exact deadline in preserved root '{}'",
        proof_root.display()
    );
}

async fn await_bounded_real_turn(
    runtime: &str,
    effect: &str,
    proof_root: &Path,
    control: tokio::sync::watch::Sender<ExecutionControl>,
    mut run: tokio::task::JoinHandle<Result<RoleTurnOutcome, lionclaw_runtime_api::TypedFailure>>,
    cleanup: impl std::future::Future<Output = ()>,
) -> Result<RoleTurnOutcome, lionclaw_runtime_api::TypedFailure> {
    let joined = match tokio::time::timeout(REAL_TURN_TIMEOUT, &mut run).await {
        Ok(joined) => joined,
        Err(_) => {
            control.send_replace(ExecutionControl::DeadlineExhausted);
            match tokio::time::timeout(REAL_CANCELLATION_TIMEOUT, &mut run).await {
                Ok(joined) => joined,
                Err(_) => {
                    run.abort();
                    let _ = tokio::time::timeout(REAL_CANCELLATION_TIMEOUT, &mut run).await;
                    await_bounded_cleanup(runtime, effect, proof_root, cleanup).await;
                    panic!(
                        "{runtime} {effect} turn did not settle after its exact deadline in preserved root '{}'",
                        proof_root.display()
                    );
                }
            }
        }
    };
    await_bounded_cleanup(runtime, effect, proof_root, cleanup).await;
    joined.unwrap_or_else(|panic| {
        panic!(
            "{runtime} {effect} runner panicked in preserved root '{}': {panic}",
            proof_root.display()
        )
    })
}

#[tokio::test]
#[ignore = "requires explicit preserved root, real Codex auth, network, and OCI image"]
async fn real_codex_runtime_resumes_across_exact_effect_cleanup() {
    prove_real_runtime_continuity("codex", Path::new(".codex/auth.json")).await;
}

#[tokio::test]
#[ignore = "requires explicit preserved root, real OpenCode auth, network, and OCI image"]
async fn real_opencode_runtime_resumes_across_exact_effect_cleanup() {
    prove_real_runtime_continuity("opencode", Path::new(".local/share/opencode/auth.json")).await;
}

#[tokio::test]
#[ignore = "requires explicit preserved root, real Hermes auth, network, and OCI image"]
async fn real_hermes_runtime_resumes_across_exact_effect_cleanup() {
    prove_real_runtime_continuity("hermes", Path::new(".hermes/config.yaml")).await;
}

async fn prove_real_runtime_continuity(runtime: &str, credential_target: &Path) {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize LionClaw source root");
    let tracked_source_is_clean = Command::new("git")
        .current_dir(&source_root)
        .args(["diff", "--quiet", "HEAD", "--"])
        .status()
        .expect("inspect tracked LionClaw source");
    assert!(
        tracked_source_is_clean.success(),
        "real runtime proof requires a committed tracked LionClaw source tree"
    );
    let source_head = Command::new("git")
        .current_dir(&source_root)
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read LionClaw source head");
    assert!(
        source_head.status.success(),
        "could not resolve LionClaw source head"
    );
    let source_head =
        String::from_utf8(source_head.stdout).expect("LionClaw source head must be UTF-8");
    let source_head = source_head.trim();
    let harness_sha256 = hex::encode(Sha256::digest(include_bytes!(
        "real_runtime_continuity_tests.rs"
    )));
    let proof_binary = std::env::current_exe().expect("resolve executing continuity test binary");
    let proof_binary_sha256 = sha256_regular_file(&proof_binary, "continuity test binary");

    let root = std::env::var_os("LIONCLAW_CONTINUITY_ROOT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .expect("LIONCLAW_CONTINUITY_ROOT must name a fresh, non-existing proof root");
    let image_ref = std::env::var("LIONCLAW_REAL_RUNTIME_IMAGE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .expect("LIONCLAW_REAL_RUNTIME_IMAGE must explicitly name the tested OCI image");
    match std::fs::symlink_metadata(&root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!(
            "cannot verify that continuity root '{}' is absent: {error}",
            root.display()
        ),
        Ok(_) => panic!(
            "refusing existing LIONCLAW_CONTINUITY_ROOT '{}'; proof evidence is never overwritten",
            root.display()
        ),
    }
    std::fs::create_dir(&root).unwrap_or_else(|error| {
        panic!(
            "creating fresh continuity root '{}': {error}; its parent must already exist",
            root.display()
        )
    });
    let root = root
        .canonicalize()
        .unwrap_or_else(|error| panic!("canonicalizing fresh continuity root: {error}"));

    let repo = root.join("repo");
    std::fs::create_dir(&repo)
        .unwrap_or_else(|error| panic!("creating proof repository '{}': {error}", repo.display()));
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .current_dir(&repo)
            .args(args)
            .output()
            .unwrap_or_else(|error| panic!("running git {args:?}: {error}"));
        assert!(
            output.status.success(),
            "git {args:?} failed in preserved proof root '{}':\nstdout:\n{}\nstderr:\n{}",
            root.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .unwrap_or_else(|error| panic!("git {args:?} emitted non-UTF-8 stdout: {error}"))
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "LionClaw Runtime Continuity"]);
    git(&["config", "user.email", "runtime-continuity@lionclaw.local"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(
        repo.join("continuity-source.txt"),
        "standalone runtime continuity source\n",
    )
    .unwrap();
    git(&["add", "continuity-source.txt"]);
    git(&["commit", "-q", "-m", "continuity source"]);
    let base_sha = git(&["rev-parse", "HEAD"]).trim().to_string();
    assert_eq!(
        git(&["rev-list", "--count", "HEAD"]).trim(),
        "1",
        "proof repository must begin with exactly one committed source revision"
    );
    assert!(
        git(&["status", "--porcelain"]).is_empty(),
        "fresh proof repository must be clean"
    );
    let repo = repo
        .canonicalize()
        .unwrap_or_else(|error| panic!("canonicalizing proof repository: {error}"));
    let state_dir = root.join("state");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&state_dir)
        .unwrap_or_else(|error| {
            panic!(
                "creating private proof state root '{}': {error}",
                state_dir.display()
            )
        });

    let image_identity =
        lionclaw_confinement::resolve_oci_image_compatibility_identity("podman", &image_ref).await;
    let image_id = image_identity.unwrap_or_else(|error| {
        panic!(
            "resolving explicit runtime image '{image_ref}' in preserved root '{}': {error:#}",
            root.display()
        )
    });
    let mut profile = RuntimeProfiles::built_in()
        .expect("load built-in runtime profiles")
        .get(runtime)
        .unwrap_or_else(|error| panic!("load built-in {runtime} profile: {error:#}"));
    profile.confinement.oci_mut().image = Some(image_id.clone());
    let profiles = RuntimeProfiles::single(profile);
    let first_runner = OciRoleRunner::new(profiles.clone(), AuthorityCeiling::default());

    let mission_id = MissionId::for_creation(
        repo.to_str()
            .expect("continuity repository path must be valid UTF-8"),
        &format!("{runtime}-real-runtime-continuity"),
        1,
    );
    let role_instance = RoleInstanceId::new("investigator").unwrap();
    let assignment_epoch = 1;
    let mission_dirs = MissionDirs::new(&state_dir, &mission_id);
    let cleaner = crate::effect_cleanup::LocalEffectCleaner::new("podman".to_string());
    let role = RoleInstance {
        id: role_instance.clone(),
        purpose: "real runtime continuity investigator".into(),
        output: OutputSemantics::ProducesReport,
        runtime: runtime.to_string(),
        instructions: "Exercise the real runtime native resume path.".into(),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            network: true,
            ..Default::default()
        },
        resources: Default::default(),
        deadline_secs: Some(300),
    };

    let request = |effect_id: EffectId, attempt_no: u32, prompt: String| {
        let deadline_ms =
            crate::activity::now_ms().saturating_add(REAL_TURN_TIMEOUT.as_millis() as i64);
        let (control_tx, control) =
            tokio::sync::watch::channel(ExecutionControl::RunUntil(deadline_ms));
        let (activity, _activity_rx) = tokio::sync::watch::channel(None);
        (
            RoleTurnRequest {
                mission_id: mission_id.clone(),
                task_id: None,
                assertion_ids: Vec::new(),
                attempt_no,
                effect_id,
                role: role.clone(),
                environment: BTreeMap::new(),
                skills: Vec::new(),
                prepared_inputs: Vec::new(),
                resource_ceilings: Default::default(),
                prompt,
                base_sha: base_sha.clone(),
                environment_digest: "sha256:continuity-test".to_string(),
                dependency_refs: Vec::new(),
                assignment_epoch,
                workspace_preparation: crate::model::WorkspacePreparation::Preserve,
                deadline_ms,
                control,
                activity,
                workspace_dir: repo.clone(),
                state_dir: state_dir.clone(),
                artifact_capture: None,
            },
            control_tx,
        )
    };
    let cleanup_effect = |effect_id: EffectId| {
        let cleaner = &cleaner;
        let proof_root = &root;
        let effect_root = mission_dirs.effect(&effect_id).role().root().to_path_buf();
        let cleanup = EffectCleanupRequest {
            mission_id: mission_id.clone(),
            effect_id,
            workspace_dir: repo.clone(),
            state_dir: state_dir.clone(),
            discard_artifact: false,
        };
        async move {
            cleaner.quiesce(&cleanup).await.unwrap_or_else(|error| {
                panic!(
                    "exact effect quiesce failed in preserved root '{}': {error:?}",
                    proof_root.display()
                )
            });
            cleaner.cleanup(cleanup).await.unwrap_or_else(|error| {
                panic!(
                    "exact effect cleanup failed in preserved root '{}': {error:?}",
                    proof_root.display()
                )
            });
            assert!(
                !effect_root.try_exists().unwrap_or_else(|error| {
                    panic!(
                        "could not verify removal of exact effect root '{}': {error}",
                        effect_root.display()
                    )
                }),
                "exact effect root '{}' survived cleanup",
                effect_root.display()
            );
        }
    };
    assert_eq!(
        crate::model::role_prompt_template(OutputSemantics::ProducesReport),
        crate::model::RolePromptTemplate::Planning,
        "live proof must use a production-valid role contract"
    );

    // The proof token must not be derivable from the checked-in test or its
    // stable mission coordinates.
    #[expect(clippy::disallowed_methods)]
    let token = format!(
        "LIONCLAW-CONTINUITY-{}-{}",
        runtime.to_ascii_uppercase(),
        uuid::Uuid::new_v4().simple()
    );
    let first_effect = EffectId::for_parts(&["real-continuity", runtime, "first"]);
    let (first_request, first_control) = request(
        first_effect.clone(),
        1,
        format!(
            "Remember this exact token for the next turn: {token}\n\
             Reply briefly that it is stored. Do not repeat or transform the token."
        ),
    );
    assert_eq!(first_request.assignment_epoch, assignment_epoch);
    assert_eq!(
        first_request.role.id, role_instance,
        "first effect must address the expected conversation generation"
    );
    let first_run = tokio::spawn(async move { first_runner.run(first_request).await });
    let first_outcome = await_bounded_real_turn(
        runtime,
        "first",
        &root,
        first_control,
        first_run,
        cleanup_effect(first_effect.clone()),
    )
    .await
    .unwrap_or_else(|failure| {
        panic!(
            "{runtime} first turn failed in preserved root '{}': {failure:?}",
            root.display()
        )
    });
    assert!(
        !first_outcome.final_response.trim().is_empty(),
        "{runtime} first turn returned no response"
    );
    let role_state = mission_dirs.role(&role_instance).role_state().clone();
    let profiles_root = role_state.session_control_root().join("profiles");
    let mut profile_entries = std::fs::read_dir(&profiles_root)
        .unwrap_or_else(|error| {
            panic!(
                "reading {runtime} retained profile root '{}': {error}",
                profiles_root.display()
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("read exact retained profile entries");
    assert_eq!(
        profile_entries.len(),
        1,
        "{runtime} first production turn must create exactly one retained profile"
    );
    let retained_usage = role_state
        .assess_runtime_retention()
        .expect("real runtime retained state remains safely account-able");
    assert_eq!(retained_usage.profiles, 1);
    assert!(retained_usage.bytes <= 512 * 1024 * 1024);
    assert!(retained_usage.entries <= 100_000);
    assert!(retained_usage.max_depth <= 128);
    let native_state_key = profile_entries
        .pop()
        .expect("one retained profile")
        .file_name()
        .into_string()
        .expect("retained profile key must be UTF-8");
    let runtime_profile = role_state
        .runtime_profile(&native_state_key)
        .expect("derive the production-selected retained runtime profile");
    let assert_credential_not_retained = || {
        let credential = runtime_profile.native_home().join(credential_target);
        match std::fs::symlink_metadata(&credential) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!(
                "{runtime} retained credential path '{}' could not be inspected: {error}",
                credential.display()
            ),
            Ok(metadata) => {
                assert!(
                    metadata.file_type().is_file(),
                    "{runtime} retained credential path '{}' is not a regular file",
                    credential.display()
                );
                let contents = std::fs::read(&credential).unwrap_or_else(|error| {
                    panic!(
                        "{runtime} retained credential file '{}' could not be read: {error}",
                        credential.display()
                    )
                });
                assert!(
                    contents.is_empty(),
                    "{runtime} credential bytes remained in retained native home at '{}'",
                    credential.display()
                );
            }
        }
    };
    let first_mode =
        lionclaw_runtime_api::recorded_runtime_resume_mode(runtime_profile.runtime_state())
            .expect("read first-turn native observation");
    assert_eq!(
        first_mode,
        Some(RuntimeResumeMode::Reconstructed),
        "{runtime} first turn was not an observed production reconstruction"
    );
    assert!(runtime_profile.native_home().is_dir());
    assert!(runtime_profile.runtime_state().path().is_dir());
    assert!(runtime_profile.runtime_state().control_path().is_dir());
    assert_eq!(
        lionclaw_runtime_api::recorded_runtime_resume_mode(runtime_profile.runtime_state())
            .expect("read retained first-turn native observation"),
        Some(RuntimeResumeMode::Reconstructed)
    );
    assert_credential_not_retained();

    let second_effect = EffectId::for_parts(&["real-continuity", runtime, "second"]);
    let (second_request, second_control) = request(
        second_effect.clone(),
        2,
        "What exact token did I ask you to remember in the preceding turn? Reply with only that token."
            .to_string(),
    );
    assert_eq!(second_request.assignment_epoch, assignment_epoch);
    assert_eq!(
        second_request.role.id, role_instance,
        "both effects must address the same conversation generation"
    );
    let second_runner = OciRoleRunner::new(profiles, AuthorityCeiling::default());
    let second_run = tokio::spawn(async move { second_runner.run(second_request).await });
    let second_outcome = await_bounded_real_turn(
        runtime,
        "second",
        &root,
        second_control,
        second_run,
        cleanup_effect(second_effect.clone()),
    )
    .await
    .unwrap_or_else(|failure| {
        panic!(
            "{runtime} second turn failed in preserved root '{}': {failure:?}",
            root.display()
        )
    });
    assert_eq!(
        second_outcome.final_response.trim(),
        token,
        "{runtime} did not recall the random first-turn token through its native session"
    );
    let second_mode =
        lionclaw_runtime_api::recorded_runtime_resume_mode(runtime_profile.runtime_state())
            .expect("read second-turn native observation");
    assert_eq!(
        second_mode,
        Some(RuntimeResumeMode::Resumed),
        "{runtime} second turn was not an observed production resume"
    );
    assert!(!mission_dirs
        .effect(&first_effect)
        .role()
        .root()
        .try_exists()
        .expect("verify first effect remains absent"));

    assert!(runtime_profile.native_home().is_dir());
    assert!(runtime_profile.runtime_state().path().is_dir());
    assert!(runtime_profile.runtime_state().control_path().is_dir());
    assert_eq!(
        lionclaw_runtime_api::recorded_runtime_resume_mode(runtime_profile.runtime_state())
            .expect("read retained second-turn native observation"),
        Some(RuntimeResumeMode::Resumed)
    );
    assert_credential_not_retained();
    assert_eq!(
        std::fs::read_dir(&profiles_root)
            .expect("reread retained profile root")
            .count(),
        1,
        "{runtime} fresh runner must reuse the exact retained profile"
    );

    let hash = |value: &str| hex::encode(Sha256::digest(value.as_bytes()));
    let receipt = serde_json::to_vec_pretty(&serde_json::json!({
        "schema": "lionclaw.runtime-continuity-proof.v2",
        "lionclaw_source_head": source_head,
        "proof_harness_sha256": harness_sha256,
        "executing_test_binary_sha256": proof_binary_sha256,
        "runtime": runtime,
        "image_reference": image_ref,
        "image_identity": image_id,
        "mission_id": mission_id.as_str(),
        "role_instance_id": role_instance.as_str(),
        "native_state_key": native_state_key,
        "base_sha": base_sha,
        "first_effect_id": first_effect.as_str(),
        "second_effect_id": second_effect.as_str(),
        "first_observed_mode": format!("{:?}", first_mode.expect("first observed mode")),
        "second_observed_mode": format!("{:?}", second_mode.expect("second observed mode")),
        "token_sha256": hash(&token),
        "first_response_sha256": hash(&first_outcome.final_response),
        "second_response_sha256": hash(&second_outcome.final_response),
        "effects_cleaned": true,
        "declared_credential_projection_removed_after_cleanup": true,
        "retained_native_home_present": runtime_profile.native_home().is_dir(),
        "retained_runtime_state_present": runtime_profile.runtime_state().path().is_dir(),
    }))
    .expect("encode continuity proof receipt");
    let receipt_name = format!("runtime-continuity-{runtime}.json");
    RootedDirectory::new(root.clone(), root.clone())
        .expect("open proof receipt authority")
        .write_private_atomic(
            OsStr::new(&receipt_name),
            &receipt,
            64 * 1024,
            "runtime continuity proof receipt",
        )
        .expect("write continuity proof receipt");
}
