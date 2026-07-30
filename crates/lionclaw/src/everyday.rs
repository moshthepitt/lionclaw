//! The everyday LionClaw entrypoint: one confined native orchestrator UI over
//! the repository and its event-sourced mission truth.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use lionclaw_confinement::{
    ExecutionOutput, ExecutionRequest, MountAccess, MountSpec, NetworkMode,
    RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET,
};
use lionclaw_runtime_api::{
    RuntimeDriverRegistry, RuntimeNativeSessionObservation, RuntimeNativeStateAvailability,
    RuntimeTerminalProgramInput,
};
use serde::Serialize;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeProfiles};
use crate::driver_lock::DriverGuard;
use crate::model::{
    fold, next, AuthorityGrants, ConfinementResources, MissionId, Next, OutputSemantics,
    RecoveryConfig, RoleInstance, RoleInstanceId, TerminalState,
};
use crate::operator_bridge::{OperatorBridge, SOCKET_MOUNT_TARGET};
use crate::resources::EverydayDirs;
use crate::runner::OciRoleRunner;
use crate::store::MissionStore;

const STANDARD_SKILL: &str = include_str!("../../../skills/lionclaw/SKILL.md");
const STANDARD_SKILL_NAME: &str = "lionclaw";
const RUNTIME_CONTEXT_FILE: &str = "AGENTS.generated.md";
const CURRENT_MISSION_FILE: &str = "current-mission";
const CURRENT_MISSION_LIMIT: usize = 64;
const SCRATCH_MOUNT_TARGET: &str = "/scratch";
pub const NONTERMINAL_EXIT_CODE: u8 = 2;

#[async_trait]
pub trait AttachedRuntimeExecutor: Send + Sync {
    async fn resolve_image_compatibility_identity(
        &self,
        engine: &str,
        image: &str,
    ) -> Result<String>;

    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput>;
}

#[derive(Debug, Default)]
pub struct ProductionAttachedRuntimeExecutor;

#[async_trait]
impl AttachedRuntimeExecutor for ProductionAttachedRuntimeExecutor {
    async fn resolve_image_compatibility_identity(
        &self,
        engine: &str,
        image: &str,
    ) -> Result<String> {
        lionclaw_confinement::resolve_oci_image_compatibility_identity(engine, image).await
    }

    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        lionclaw_confinement::execute_attached(request).await
    }
}

pub struct EverydayRunRequest {
    pub repo: PathBuf,
    pub runtime: String,
    pub store: MissionStore,
    pub profiles: RuntimeProfiles,
    pub drivers: Option<RuntimeDriverRegistry>,
    pub auth: Option<lionclaw_runtime_api::RuntimeAuthRegistry>,
    pub executor: Arc<dyn AttachedRuntimeExecutor>,
    pub runtime_root: PathBuf,
    pub new_selection: bool,
    pub operator_transports: Option<crate::cli::MissionTransports>,
}

#[derive(Debug)]
pub struct EverydayRunOutcome {
    pub runtime: String,
    pub runtime_status: String,
    pub mission: MissionOutcome,
    pub runtime_termination: RuntimeTermination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTermination {
    Exited,
    Failed,
    Signaled,
    LaunchFailed,
    Unknown,
}

#[derive(Debug)]
pub enum MissionOutcome {
    None,
    Ambiguous { live: usize, total: usize },
    Nonterminal { id: MissionId },
    Done { id: MissionId },
    Aborted { id: MissionId },
}

impl EverydayRunOutcome {
    pub fn exit_code(&self) -> std::process::ExitCode {
        if self.runtime_termination != RuntimeTermination::Exited {
            return std::process::ExitCode::FAILURE;
        }
        match self.mission {
            MissionOutcome::Done { .. } => std::process::ExitCode::SUCCESS,
            MissionOutcome::Aborted { .. } => std::process::ExitCode::FAILURE,
            MissionOutcome::None
            | MissionOutcome::Ambiguous { .. }
            | MissionOutcome::Nonterminal { .. } => {
                std::process::ExitCode::from(NONTERMINAL_EXIT_CODE)
            }
        }
    }

    pub fn print(&self) {
        let verb = match self.runtime_termination {
            RuntimeTermination::Exited | RuntimeTermination::Failed => "exited",
            RuntimeTermination::Signaled => "was terminated with",
            RuntimeTermination::LaunchFailed => "failed to launch:",
            RuntimeTermination::Unknown => "ended with",
        };
        println!("runtime {} {verb} {}", self.runtime, self.runtime_status);
        match &self.mission {
            MissionOutcome::None => println!("mission state: no mission"),
            MissionOutcome::Ambiguous { live, total } => {
                println!("mission state: ambiguous ({live} live, {total} total)")
            }
            MissionOutcome::Nonterminal { id } => {
                println!("mission {id}: nonterminal")
            }
            MissionOutcome::Done { id } => println!("mission {id}: done"),
            MissionOutcome::Aborted { id } => println!("mission {id}: aborted"),
        }
    }
}

pub async fn run(request: EverydayRunRequest) -> Result<EverydayRunOutcome> {
    let EverydayRunRequest {
        repo,
        runtime,
        store,
        profiles,
        drivers,
        auth,
        executor,
        runtime_root,
        new_selection,
        operator_transports,
    } = request;
    let judged_root = crate::authority::canonical_or_lexical(&repo);
    let runtime_root = crate::authority::canonical_or_lexical(&runtime_root);
    if crate::authority::paths_overlap(&runtime_root, &judged_root) {
        bail!(
            "writable everyday runtime state '{}' overlaps judged root '{}'",
            runtime_root.display(),
            judged_root.display()
        );
    }
    let mut profile = profiles.get(&runtime)?;
    let runner = match (drivers, auth) {
        (Some(drivers), Some(auth)) => {
            OciRoleRunner::with_registries(profiles, AuthorityCeiling::default(), drivers, auth)
        }
        (None, None) => OciRoleRunner::new(profiles, AuthorityCeiling::default()),
        _ => bail!("runtime driver and auth registries must be supplied together"),
    };
    let driver = runner.driver(&profile)?;
    let config = OciRoleRunner::driver_config(&profile)?;
    runner.auth_registry(&profile)?;
    driver
        .validate_config(&config)
        .with_context(|| format!("runtime '{}' driver configuration is invalid", profile.name))?;
    let oci = profile.confinement.oci();
    let image_ref = oci
        .image
        .as_deref()
        .context("everyday runtime profile has no confinement image")?;
    let image_id = executor
        .resolve_image_compatibility_identity(&oci.engine, image_ref)
        .await
        .with_context(|| format!("resolving image '{image_ref}'"))?;
    profile.confinement.oci_mut().image = Some(image_id);

    let dirs = EverydayDirs::new(store.lionclaw_dir(), runtime_root);
    dirs.prepare().context("preparing everyday runtime state")?;
    let _guard = DriverGuard::try_acquire(dirs.driver_lock())?
        .context("another `lionclaw run` owns this repository")?;
    let operator_bridge = OperatorBridge::start(&repo, operator_transports)?;
    prepare_standard_skill(
        dirs.operator_skill(),
        operator_bridge.client_script()?.as_bytes(),
    )?;

    let auth = runner
        .materialize_runtime_auth(&profile, NetworkMode::On, dirs.auth_staging().to_path_buf())
        .await
        .context("runtime auth materialization is invalid")?;
    let profile_key = profile.native_state_key(auth.identity());
    dirs.role_state()
        .admit_runtime_profile_async(profile_key.clone())
        .await
        .context("retained everyday runtime state admission refused")?;
    let runtime_profile = dirs.role_state().runtime_profile(&profile_key)?;
    runtime_profile
        .prepare()
        .context("preparing everyday runtime profile")?;
    let skills_dir = profile
        .skills_dir
        .as_ref()
        .context("runtime profile has no skills-dir for the standard LionClaw skill")?;
    runtime_profile
        .prepare_native_home_dir(&skills_dir.relative_skill_path(STANDARD_SKILL_NAME)?)?;

    let plan = everyday_plan(
        &repo,
        &profile,
        &dirs,
        &runtime_profile,
        operator_bridge.socket_path(),
    )?;
    let adapter = driver.create_adapter(config);
    let mut mission_selector = EverydayMissionSelector::begin(&store, &dirs, new_selection).await?;
    let maximum_attempts = RecoveryConfig::default().max_attempts;
    let mut runtime_status = "without an exit status".to_string();
    let mut runtime_termination = RuntimeTermination::Unknown;

    for attempt_no in 1..=maximum_attempts {
        let facts = mission_selector.load_facts(&store, &profile.name).await?;
        write_runtime_context(dirs.role_state().runtime(), &facts)?;
        let session_attempt = profile
            .native_resume
            .then(|| {
                lionclaw_runtime_api::begin_runtime_session_attempt(runtime_profile.runtime_state())
            })
            .transpose()
            .context("native terminal session state is invalid")?;
        let resume = session_attempt
            .as_ref()
            .is_some_and(|attempt| attempt.previous_ready().is_ready());
        let program = adapter.build_terminal_program(RuntimeTerminalProgramInput {
            session_id: uuid_from_key(&profile_key),
            runtime_state: runtime_profile.runtime_state().clone(),
            resume,
            bootstrap_message: bootstrap_message(),
        })?;
        let execution = executor
            .execute(ExecutionRequest {
                plan: plan.clone(),
                program,
                resource_name: None,
                runtime_secrets_mount: None,
                auth_staging_root: Some(auth.staging_root().to_path_buf()),
                runtime_auth: auth.materialization(),
            })
            .await;

        match execution {
            Ok(output) if output.success() => {
                runtime_status = output.status_description();
                runtime_termination = RuntimeTermination::Exited;
                if let Some(attempt) = session_attempt {
                    attempt.commit(if resume {
                        RuntimeNativeSessionObservation::Resumed
                    } else {
                        RuntimeNativeSessionObservation::Reconstructed {
                            state: RuntimeNativeStateAvailability::Reopenable,
                        }
                    })?;
                }
                break;
            }
            Ok(output) => {
                runtime_status = output.status_description();
                if let Some(attempt) = session_attempt {
                    attempt.restore_previous()?;
                }
                if output.exit_signal.is_none() && output.exit_code.is_some() {
                    runtime_termination = RuntimeTermination::Failed;
                    break;
                }
                runtime_termination = if output.exit_signal.is_some() {
                    RuntimeTermination::Signaled
                } else {
                    RuntimeTermination::Unknown
                };
            }
            Err(error) => {
                runtime_status = format!("{error:#}");
                if let Some(attempt) = session_attempt {
                    attempt.restore_previous()?;
                }
                runtime_termination = RuntimeTermination::LaunchFailed;
            }
        }
        if attempt_no < maximum_attempts {
            eprintln!(
                "runtime {} {} {}; restarting ({}/{})",
                profile.name,
                match runtime_termination {
                    RuntimeTermination::Signaled => "was terminated with",
                    RuntimeTermination::LaunchFailed => "failed to launch:",
                    RuntimeTermination::Unknown => "ended with",
                    RuntimeTermination::Exited | RuntimeTermination::Failed => {
                        unreachable!("ordinary process exits are not retried")
                    }
                },
                runtime_status,
                attempt_no + 1,
                maximum_attempts
            );
        }
    }

    dirs.role_state()
        .assess_runtime_retention_async()
        .await
        .context("retained everyday runtime state post-run check refused")?;
    let facts = mission_selector.load_facts(&store, &profile.name).await?;
    Ok(EverydayRunOutcome {
        runtime: profile.name,
        runtime_status,
        mission: facts.outcome(),
        runtime_termination,
    })
}

pub(crate) fn runtime_root(repo: &Path) -> Result<PathBuf> {
    let repo = repo
        .canonicalize()
        .with_context(|| format!("resolving everyday repository '{}'", repo.display()))?;
    let digest = <sha2::Sha256 as sha2::Digest>::digest(repo.as_os_str().as_encoded_bytes());
    Ok(crate::mission_type::Home::from_env()?
        .root()
        .join("everyday")
        .join(hex::encode(digest)))
}

fn everyday_plan(
    repo: &Path,
    profile: &MissionRuntimeProfile,
    dirs: &EverydayDirs,
    runtime_profile: &crate::resources::RuntimeProfileDirs,
    operator_socket: &Path,
) -> Result<lionclaw_confinement::EffectiveExecutionPlan> {
    let role = RoleInstance {
        id: RoleInstanceId::new("orchestrator")?,
        purpose: "everyday orchestrator".to_string(),
        output: OutputSemantics::ProducesReport,
        runtime: profile.name.clone(),
        instructions: String::new(),
        skills: vec![STANDARD_SKILL_NAME.to_string()],
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            network: true,
            install: true,
            writes: false,
            ..Default::default()
        },
        resources: ConfinementResources::default(),
        deadline_secs: None,
    };
    let authority = compile_authority(&role, &AuthorityCeiling::default())?;
    let skill_target = profile
        .skills_dir
        .as_ref()
        .context("runtime profile has no skills-dir")?
        .mount_target(STANDARD_SKILL_NAME)?;
    let extras = vec![
        MountSpec {
            source: dirs.role_state().scratch().to_path_buf(),
            target: SCRATCH_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        },
        MountSpec {
            source: dirs.role_state().runtime().to_path_buf(),
            target: RUNTIME_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        },
        MountSpec {
            source: runtime_profile.native_home().to_path_buf(),
            target: RUNTIME_HOME_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        },
        MountSpec {
            source: dirs.operator_skill().to_path_buf(),
            target: skill_target,
            access: MountAccess::ReadOnly,
        },
        MountSpec {
            source: operator_socket.to_path_buf(),
            target: SOCKET_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        },
    ];
    let judged_roots = [crate::authority::canonical_or_lexical(repo)];
    Ok(compile_role_plan(RolePlanRequest {
        authority: &authority,
        runtime_id: profile.name.clone(),
        confinement: profile.confinement.clone(),
        mounts: MissionMounts {
            workspace: repo.to_path_buf(),
            extras,
        },
        working_dir: repo.to_path_buf(),
        judged_roots: &judged_roots,
        environment: crate::runner::runtime_home_environment(),
        resources: ConfinementResources::default(),
        resource_ceilings: &ConfinementResources::default(),
    })?
    .plan()
    .clone())
}

#[derive(Debug, Serialize)]
struct EverydayFacts {
    repository: &'static str,
    runtime: String,
    selection: MissionSelection,
    mission: Option<SelectedMission>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MissionSelection {
    None,
    Unambiguous,
    Ambiguous { live: usize, total: usize },
}

#[derive(Debug, Serialize)]
struct SelectedMission {
    id: MissionId,
    terminal: Option<TerminalState>,
    next: Next,
}

impl EverydayFacts {
    fn outcome(&self) -> MissionOutcome {
        let Some(mission) = &self.mission else {
            return match self.selection {
                MissionSelection::None => MissionOutcome::None,
                MissionSelection::Ambiguous { live, total } => {
                    MissionOutcome::Ambiguous { live, total }
                }
                MissionSelection::Unambiguous => unreachable!("selected mission is required"),
            };
        };
        match mission.terminal {
            None => MissionOutcome::Nonterminal {
                id: mission.id.clone(),
            },
            Some(TerminalState::Done { .. }) => MissionOutcome::Done {
                id: mission.id.clone(),
            },
            Some(TerminalState::Aborted { .. }) => MissionOutcome::Aborted {
                id: mission.id.clone(),
            },
        }
    }
}

struct EverydayMissionSelector {
    files: lionclaw_durable_fs::RootedDirectory,
    ignored_settled: BTreeSet<MissionId>,
}

impl EverydayMissionSelector {
    async fn begin(store: &MissionStore, dirs: &EverydayDirs, new_selection: bool) -> Result<Self> {
        let files = dirs.files()?;
        let all = load_mission_states(store).await?;
        let ignored_settled = all
            .iter()
            .filter(|(_, state)| state.is_terminal())
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>();
        if let Some(bound) = read_current_mission(&files)? {
            let (_, state) = all
                .iter()
                .find(|(id, _)| id == &bound)
                .with_context(|| format!("current everyday mission '{bound}' does not exist"))?;
            if new_selection {
                if !state.is_terminal() {
                    bail!(
                        "current everyday mission '{bound}' is still live; finish or abort it before using `lionclaw run --new`"
                    );
                }
                files.remove_file(OsStr::new(CURRENT_MISSION_FILE), "current everyday mission")?;
            } else if state.is_terminal() && !has_apply_choice(state) {
                files.remove_file(OsStr::new(CURRENT_MISSION_FILE), "current everyday mission")?;
            }
        } else if new_selection {
            bail!("`lionclaw run --new` requires a current completed mission");
        }
        Ok(Self {
            files,
            ignored_settled,
        })
    }

    async fn load_facts(&mut self, store: &MissionStore, runtime: &str) -> Result<EverydayFacts> {
        let all = load_mission_states(store).await?;
        let live = all
            .iter()
            .filter(|(_, state)| !state.is_terminal())
            .cloned()
            .collect::<Vec<_>>();
        let bound = read_current_mission(&self.files)?;
        let (selected, candidate_count) = if let Some(bound) = &bound {
            (
                Some(
                    all.iter()
                        .find(|(id, _)| id == bound)
                        .cloned()
                        .with_context(|| {
                            format!("current everyday mission '{bound}' does not exist")
                        })?,
                ),
                1,
            )
        } else {
            let candidates = all
                .iter()
                .filter(|(id, state)| !state.is_terminal() || !self.ignored_settled.contains(id))
                .cloned()
                .collect::<Vec<_>>();
            (
                match candidates.as_slice() {
                    [only] => Some(only.clone()),
                    _ => None,
                },
                candidates.len(),
            )
        };
        if bound.is_none() {
            if let Some((id, _)) = &selected {
                self.files.write_private_atomic(
                    OsStr::new(CURRENT_MISSION_FILE),
                    id.as_str().as_bytes(),
                    CURRENT_MISSION_LIMIT,
                    "current everyday mission",
                )?;
            }
        }
        let (selection, mission) = match selected {
            Some((id, state)) => (
                MissionSelection::Unambiguous,
                Some(SelectedMission {
                    id,
                    terminal: state.terminal.clone(),
                    next: next(&state),
                }),
            ),
            None if candidate_count == 0 => (MissionSelection::None, None),
            None => (
                MissionSelection::Ambiguous {
                    live: live.len(),
                    total: all.len(),
                },
                None,
            ),
        };
        Ok(EverydayFacts {
            repository: "/workspace",
            runtime: runtime.to_string(),
            selection,
            mission,
        })
    }
}

async fn load_mission_states(
    store: &MissionStore,
) -> Result<Vec<(MissionId, crate::model::MissionState)>> {
    let mut all = Vec::new();
    for mission_id in store.list_missions().await? {
        let Some(state) = fold(store.load(&mission_id).await?) else {
            continue;
        };
        all.push((mission_id, state));
    }
    Ok(all)
}

fn has_apply_choice(state: &crate::model::MissionState) -> bool {
    next(state)
        .choices
        .iter()
        .any(|choice| matches!(choice, crate::model::Choice::Apply { .. }))
}

fn read_current_mission(files: &lionclaw_durable_fs::RootedDirectory) -> Result<Option<MissionId>> {
    let Some(raw) = files.read_private_bounded_with_metadata(
        OsStr::new(CURRENT_MISSION_FILE),
        CURRENT_MISSION_LIMIT,
        "current everyday mission",
    )?
    else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&raw.0).context("current everyday mission is not UTF-8")?;
    Ok(Some(
        MissionId::parse(text).context("current everyday mission identity is invalid")?,
    ))
}

pub(crate) fn current_mission(store: &MissionStore) -> Result<Option<MissionId>> {
    let files = lionclaw_durable_fs::RootedDirectory::new(
        store.lionclaw_dir().to_path_buf(),
        store.lionclaw_dir().join("everyday"),
    )?;
    read_current_mission(&files)
}

fn bootstrap_message() -> String {
    "Use the installed LionClaw skill. Read /runtime/AGENTS.generated.md for current repository and mission facts before acting. The repository is read-only; use /scratch for transient files and bridge stdin for proposal JSON.".to_string()
}

fn write_runtime_context(runtime_dir: &Path, facts: &EverydayFacts) -> Result<()> {
    let facts = serde_json::to_string_pretty(facts)?;
    let context = format!(
        "# LionClaw Current Facts\n\nThese are neutral folded facts, not an instruction to choose an action.\nUse only an exact choice currently present in `mission.next.choices`.\n\n```json\n{facts}\n```\n"
    );
    write_atomic(runtime_dir, RUNTIME_CONTEXT_FILE, context.as_bytes(), 0o600)
}

fn prepare_standard_skill(root: &Path, client: &[u8]) -> Result<()> {
    write_atomic(root, "SKILL.md", STANDARD_SKILL.as_bytes(), 0o600)?;
    write_atomic(root, "lionclaw", client, 0o700)
}

fn write_atomic(root: &Path, name: &str, contents: &[u8], mode: u32) -> Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(root)
        .with_context(|| format!("creating temporary file beneath '{}'", root.display()))?;
    temporary
        .write_all(contents)
        .with_context(|| format!("writing temporary '{name}'"))?;
    temporary
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(mode))?;
    temporary
        .persist(root.join(name))
        .map(|_| ())
        .with_context(|| format!("publishing '{}'", root.join(name).display()))
}

fn uuid_from_key(key: &str) -> uuid::Uuid {
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_message_contains_no_workflow_action() {
        let message = bootstrap_message();
        for action in [
            "approve", "retry", "repair", "revise", "accept", "finish", "abort", "apply",
        ] {
            assert!(!message.contains(action));
        }
    }

    #[test]
    fn outcome_never_equates_nonterminal_runtime_success_with_mission_success() {
        let outcome = EverydayRunOutcome {
            runtime: "test".to_string(),
            runtime_status: "code 0".to_string(),
            mission: MissionOutcome::Nonterminal {
                id: MissionId::parse("m000000000000").unwrap(),
            },
            runtime_termination: RuntimeTermination::Exited,
        };
        assert_eq!(
            outcome.exit_code(),
            std::process::ExitCode::from(NONTERMINAL_EXIT_CODE)
        );
    }
}
