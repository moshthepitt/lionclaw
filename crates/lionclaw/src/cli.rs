//! The mission CLI surface. Each command is a per-mission stdio run: open
//! the store, fold, act, park or exit. Host-as-orchestrator: the human's
//! agent session invokes these as tools and reads `--json` output.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Args, CommandFactory, Parser, Subcommand};

use crate::authority::AuthorityCeiling;
use crate::config::{MissionRuntimeProfile, RuntimeProfiles};
use crate::engine::{
    load_mission_view, record_control, Engine, EngineServices, MissionDisposition, MissionView,
};
use crate::mission_type::{
    add_skill, install_mission_type, load_materialized_mission_type, load_mission_type,
    materialize_mission_type, remove_skill, BundledMissionTypes, Home, MissionType,
    MissionTypeLocator, SkillSource,
};
use crate::model::{
    fold, short_hex, ControlAction, ConversationLifecycle, ConversationRecipient, DecisionAction,
    EffectId, FinishClass, MissionEvent, MissionId, MissionPhase, MAX_MESSAGE_BYTES,
};
use crate::oracle::OciOracleRunner;
use crate::ports::{Clock, SystemClock};
use crate::runner::OciRoleRunner;
use crate::store::{BlobStore, MissionStore, NewEvent};
use crate::workspace;

#[derive(Parser)]
#[command(name = "lionclaw", about = "LionClaw mission engine")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Install the bundled mission types into `~/.lionclaw` (run once).
    Install(InstallArgs),
    /// Check the install: podman, git, and each installed mission type + image.
    Doctor,
    /// Add or remove skills in a mission type bundle.
    #[command(subcommand)]
    Skill(SkillCommand),
    /// Mission engine commands.
    #[command(subcommand)]
    Mission(MissionCommand),
    /// Render the lionclaw(1) manual page to stdout.
    Man,
}

#[derive(Args)]
pub struct InstallArgs {
    /// Mission type directories to install (defaults to bundled mission types).
    pub mission_types: Vec<PathBuf>,
    /// Overwrite mission types already installed.
    #[arg(long)]
    pub force: bool,
}

#[derive(Subcommand)]
pub enum SkillCommand {
    /// Copy a local or Git skill package into a mission type.
    Add(SkillAddArgs),
    /// Remove an unassigned skill package from a mission type.
    Remove(SkillRemoveArgs),
}

#[derive(Args)]
#[command(group(ArgGroup::new("source").required(true).args(["path", "git"])))]
pub struct SkillAddArgs {
    /// Installed mission type name or explicit mission type directory.
    #[arg(long = "mission-type")]
    pub mission_type: String,
    /// Local skill package directory.
    #[arg(long, conflicts_with = "git")]
    pub path: Option<PathBuf>,
    /// Git repository containing the skill package.
    #[arg(long, conflicts_with = "path", requires = "rev")]
    pub git: Option<String>,
    /// Git revision to fetch. Required with --git.
    #[arg(long, requires = "git")]
    pub rev: Option<String>,
    /// Skill package directory within the Git checkout.
    #[arg(long, requires = "git")]
    pub subdir: Option<PathBuf>,
    /// Replace a different package already installed under the same name.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct SkillRemoveArgs {
    /// Installed mission type name or explicit mission type directory.
    #[arg(long = "mission-type")]
    pub mission_type: String,
    /// Skill package name.
    pub name: String,
}

#[derive(Subcommand)]
pub enum MissionCommand {
    /// Create a mission over a target repo.
    Start(StartArgs),
    /// Drive a mission until it parks, finishes, or awaits input.
    Advance(AdvanceArgs),
    #[command(hide = true)]
    Driver(DriverArgs),
    #[command(hide = true)]
    DriverStderr(DriverStderrArgs),
    /// Show a mission's state (contract, phase, finish grade).
    Status(StatusArgs),
    /// The verifiable receipt: what was proven, by what, and what was NOT.
    Report(ReportArgs),
    /// Create a branch (`lionclaw/<id>`) at the mission's produced commit.
    Apply(ApplyArgs),
    /// Print a mission's event log.
    Log(LogArgs),
    /// List missions awaiting input or blocked on cleanup.
    Inbox(InboxArgs),
    /// Send durable lead feedback to current role conversations.
    Send(SendArgs),
    /// Inspect or propose complete plan revisions.
    #[command(subcommand)]
    Plan(PlanCommand),
    /// Resolve an open attention item.
    Decide(DecideArgs),
    /// Request cancellation of one exact active effect.
    Stop(ControlArgs),
    /// Extend one exact active effect's deadline.
    Extend(ExtendArgs),
    /// Resume one exact parked effect in its preserved workspace.
    Continue(ControlArgs),
    /// Inspect mission types.
    #[command(subcommand)]
    Type(TypeCommand),
    /// Drive the real stack end-to-end and assert the core invariants — moat,
    /// oracle honesty, writable-worker resume, confinement, re-planning (needs
    /// podman; model-auth-free).
    SelfTest(SelfTestArgs),
}

#[derive(Subcommand)]
pub enum PlanCommand {
    /// Show the current or pending plan.
    Show(PlanShowArgs),
    /// Propose a complete plan revision from JSON.
    Propose(PlanProposeArgs),
}

#[derive(Subcommand)]
pub enum TypeCommand {
    /// List installed mission types.
    List(TypeListArgs),
    /// Show one mission type (roles, oracles, stop bar, playbook).
    Show(TypeShowArgs),
    /// Validate a mission type directory (loader + moat) without installing it.
    Check(TypeCheckArgs),
}

#[derive(Args)]
pub struct TypeListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TypeShowArgs {
    /// Installed mission type name or explicit mission type directory.
    pub mission_type: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TypeCheckArgs {
    /// Installed mission type name or explicit mission type directory.
    pub mission_type: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct StartArgs {
    /// Installed mission type name or explicit mission type directory.
    #[arg(long = "type")]
    pub mission_type: String,
    /// Target repository (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// The mission objective.
    #[arg(long)]
    pub objective: String,
    #[arg(long, default_value = "codex")]
    pub runtime: String,
    /// Override the mission type's confinement image for this mission.
    #[arg(long)]
    pub image: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct InboxArgs {
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
#[command(group(ArgGroup::new("recipients").required(true).args(["to", "all"])))]
pub struct SendArgs {
    /// Mission id (defaults to the only active mission in the repository).
    #[arg(long)]
    pub mission_id: Option<String>,
    /// Current conversation id or an unambiguous current task name. Repeatable.
    #[arg(long = "to", action = clap::ArgAction::Append, conflicts_with = "all")]
    pub to: Vec<String>,
    /// Address every current role-instance conversation.
    #[arg(long)]
    pub all: bool,
    /// Exact UTF-8 message body.
    pub message: String,
    /// Reference an authoritative receipt by effect id. Repeatable.
    #[arg(long = "receipt", action = clap::ArgAction::Append)]
    pub receipts: Vec<String>,
    /// Reference parked-effect evidence by effect id. Repeatable.
    #[arg(long = "park", action = clap::ArgAction::Append)]
    pub parks: Vec<String>,
    /// Reference a commit reachable in this mission. Repeatable.
    #[arg(long = "commit", action = clap::ArgAction::Append)]
    pub commits: Vec<String>,
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
pub struct PlanShowArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct DecideArgs {
    pub mission_id: String,
    /// The attention item id (see `mission status`/`inbox`).
    pub item: String,
    /// One of: approve | retry | repair | revise | accept | abort.
    pub action: String,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Why a non-revise decision is appropriate. Recorded in the log.
    #[arg(long, conflicts_with_all = ["feedback_file", "feedback_stdin"])]
    pub justification: Option<String>,
    /// Read exact revise feedback from this UTF-8 file.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["justification", "feedback_stdin"])]
    pub feedback_file: Option<PathBuf>,
    /// Read exact revise feedback from stdin.
    #[arg(long, conflicts_with_all = ["justification", "feedback_file"])]
    pub feedback_stdin: bool,
}

#[derive(Args)]
pub struct PlanProposeArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Proposal JSON ({ "base_revision": N, "plan": {...} }); `-` reads stdin.
    #[arg(long = "file")]
    pub file: PathBuf,
}

#[derive(Args)]
pub struct AdvanceArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    /// Wait for the detached driver to reach its next checkpoint.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Args)]
pub struct DriverArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long)]
    pub handshake: PathBuf,
}

#[derive(Args)]
pub struct DriverStderrArgs {
    #[arg(long)]
    pub mission_dir: PathBuf,
}

#[derive(Args)]
pub struct ControlArgs {
    pub mission_id: String,
    pub effect_id: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub reason: String,
}

#[derive(Args)]
pub struct ExtendArgs {
    pub mission_id: String,
    pub effect_id: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Additional seconds from the effect's current effective deadline.
    #[arg(long)]
    pub seconds: u64,
    #[arg(long)]
    pub reason: String,
}

#[derive(Args)]
pub struct StatusArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    /// Follow the bounded activity projection until the driver exits.
    #[arg(long)]
    pub watch: bool,
}

#[derive(Args)]
pub struct ReportArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Include the base..head diff (text output only).
    #[arg(long)]
    pub patch: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct ApplyArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Overwrite the branch if it already exists.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct LogArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
pub struct SelfTestArgs {
    #[arg(long)]
    pub json: bool,
}

/// Run a command, returning the process exit code (so callers can gate on
/// e.g. `type check` without the command itself calling `process::exit`).
pub async fn run(cli: Cli) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cli.command {
        Command::Install(args) => cmd_install(args).await.map(|()| ExitCode::SUCCESS),
        Command::Doctor => cmd_doctor().await,
        Command::Skill(cmd) => cmd_skill(cmd).await.map(|()| ExitCode::SUCCESS),
        Command::Mission(cmd) => run_mission(cmd).await,
        Command::Man => {
            clap_mangen::Man::new(Cli::command()).render(&mut std::io::stdout())?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

async fn run_mission(cmd: MissionCommand) -> Result<std::process::ExitCode> {
    // One error policy for every command: a `--json` command reports failure as
    // a structured `{"ok":false,"error":…}` envelope on stdout and exits 1; a
    // human command lets the error bubble to stderr. Success output is each
    // command's own concern.
    let json = cmd.is_json();
    match dispatch_mission(cmd).await {
        Ok(code) => Ok(code),
        Err(err) if json => {
            // `{err:#}` renders the full anyhow context chain (outer: cause: …),
            // not just the outermost message, so a JSON caller gets the real
            // reason.
            println!(
                "{}",
                serde_json::json!({ "ok": false, "error": format!("{err:#}") })
            );
            Ok(std::process::ExitCode::FAILURE)
        }
        Err(err) => Err(err),
    }
}

impl MissionCommand {
    /// Whether this invocation asked for machine output (`--json`).
    fn is_json(&self) -> bool {
        match self {
            Self::Start(a) => a.json,
            Self::Status(a) => a.json,
            Self::Report(a) => a.json,
            Self::Plan(a) => a.is_json(),
            Self::Inbox(a) => a.json,
            Self::Advance(a) => a.json,
            Self::Driver(_) | Self::DriverStderr(_) => false,
            Self::SelfTest(a) => a.json,
            Self::Type(t) => t.is_json(),
            Self::Apply(_)
            | Self::Log(_)
            | Self::Send(_)
            | Self::Decide(_)
            | Self::Stop(_)
            | Self::Extend(_)
            | Self::Continue(_) => false,
        }
    }
}

impl PlanCommand {
    fn is_json(&self) -> bool {
        match self {
            Self::Show(args) => args.json,
            Self::Propose(_) => false,
        }
    }
}

impl TypeCommand {
    fn is_json(&self) -> bool {
        match self {
            Self::List(a) => a.json,
            Self::Show(a) => a.json,
            Self::Check(a) => a.json,
        }
    }
}

async fn dispatch_mission(cmd: MissionCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        MissionCommand::Start(args) => cmd_start(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Advance(args) => cmd_advance(args).await,
        MissionCommand::Driver(args) => cmd_driver(args).await,
        MissionCommand::DriverStderr(args) => cmd_driver_stderr(args).await,
        MissionCommand::Status(args) => cmd_status(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Report(args) => cmd_report(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Apply(args) => cmd_apply(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Log(args) => cmd_log(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Inbox(args) => cmd_inbox(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Send(args) => cmd_send(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Plan(cmd) => cmd_plan(cmd).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Decide(args) => cmd_decide(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Stop(args) => cmd_control(args, false).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Extend(args) => cmd_extend(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Continue(args) => cmd_control(args, true).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Type(cmd) => cmd_type(cmd).await,
        MissionCommand::SelfTest(args) => crate::selftest::run(args.json).await,
    }
}

fn runtime_profiles() -> Result<RuntimeProfiles> {
    RuntimeProfiles::load(&Home::from_env()?)
}

fn validated_role_profile(
    role: &crate::mission_type::RoleDefinition,
    runtime: &str,
    profiles: &RuntimeProfiles,
) -> Result<MissionRuntimeProfile> {
    let profile = profiles.get(runtime).with_context(|| {
        format!(
            "role '{}' resolves to unavailable runtime '{runtime}'",
            role.name
        )
    })?;
    OciRoleRunner::validate_profile(&profile).with_context(|| {
        format!(
            "role '{}' resolves to invalid runtime '{runtime}'",
            role.name
        )
    })?;
    Ok(profile)
}

fn validate_explicit_role_runtimes(
    mission_type: &MissionType,
    profiles: &RuntimeProfiles,
) -> Result<()> {
    for role in mission_type.roles.values() {
        if let Some(runtime) = &role.runtime {
            validated_role_profile(role, runtime, profiles)?;
        }
    }
    Ok(())
}

fn validate_mission_runtimes(
    mission_type: &MissionType,
    default_runtime: &str,
    profiles: &RuntimeProfiles,
) -> Result<MissionRuntimeProfile> {
    let default_profile = profiles.get(default_runtime)?;
    OciRoleRunner::validate_profile(&default_profile)
        .with_context(|| format!("default runtime '{default_runtime}' is invalid"))?;
    let default_engine = &default_profile.confinement.oci().engine;
    for role in mission_type.roles.values() {
        let runtime = role.runtime.as_deref().unwrap_or(default_runtime);
        let profile = validated_role_profile(role, runtime, profiles)?;
        if profile.confinement.oci().engine != *default_engine {
            bail!(
                "role '{}' resolves to runtime '{}' using OCI engine '{}', but mission default runtime '{}' uses '{}'; one mission requires one OCI engine",
                role.name,
                runtime,
                profile.confinement.oci().engine,
                default_runtime,
                default_engine
            );
        }
    }
    Ok(default_profile)
}

/// Build an engine over an open store, a loaded mission type, and a runtime
/// profile (whose image the caller has already pinned).
#[allow(clippy::too_many_arguments)]
async fn assemble_engine(
    store: MissionStore,
    repo: &Path,
    mission_type: crate::mission_type::MissionType,
    runtime: String,
    image_id: String,
    profiles: RuntimeProfiles,
    mut default_profile: MissionRuntimeProfile,
    ceiling: AuthorityCeiling,
) -> Result<Engine> {
    workspace::ensure_excluded(repo).await?;
    default_profile.confinement.oci_mut().image = Some(image_id.clone());
    let role_runner = Arc::new(OciRoleRunner::new(profiles, image_id.clone(), ceiling));
    let effect_cleaner = Arc::new(crate::effect_cleanup::LocalEffectCleaner::new(
        default_profile.confinement.oci().engine.clone(),
    ));
    let oracle_runner = Arc::new(OciOracleRunner::new(default_profile));
    Ok(Engine::new(
        store,
        mission_type,
        runtime,
        image_id,
        EngineServices::new(
            role_runner,
            oracle_runner,
            effect_cleaner,
            Arc::new(SystemClock),
        ),
    ))
}

/// Build an engine to create a mission from its validated snapshot.
/// The confinement image is resolved to a content id here — once, at start — so
/// a later rebuild of the tag cannot silently change the instrument. The engine
/// carries the runtime + image id it records on `MissionCreated`.
async fn build_engine_for_start(
    store: MissionStore,
    repo: &Path,
    mission_type: MissionType,
    runtime: &str,
    image_override: Option<&str>,
) -> Result<Engine> {
    let ceiling = AuthorityCeiling::default();
    let profiles = runtime_profiles()?;
    let default_profile = validate_mission_runtimes(&mission_type, runtime, &profiles)?;
    let engine = default_profile.confinement.oci().engine.clone();
    let image_ref = start_image_ref(&mission_type.image, image_override);
    let image_id =
        lionclaw_confinement::resolve_oci_image_compatibility_identity(&engine, image_ref)
            .await
            .with_context(|| format!("resolving image '{image_ref}'"))?;
    assemble_engine(
        store,
        repo,
        mission_type,
        runtime.to_string(),
        image_id,
        profiles,
        default_profile,
        ceiling,
    )
    .await
}

fn start_image_ref<'a>(mission_type_image: &'a str, image_override: Option<&'a str>) -> &'a str {
    image_override.unwrap_or(mission_type_image)
}

/// Build an engine for an existing mission from its immutable bundle snapshot.
async fn build_engine_for_mission(
    store: MissionStore,
    repo: &Path,
    mission_id: &MissionId,
) -> Result<Engine> {
    let state = store.require_state(mission_id).await?;
    let ceiling = AuthorityCeiling::default();
    let mission_type = load_mission_type_snapshot(&store, mission_id, &ceiling)?;
    let profiles = runtime_profiles()?;
    let default_profile = validate_mission_runtimes(&mission_type, &state.runtime, &profiles)?;
    let engine = assemble_engine(
        store,
        repo,
        mission_type,
        state.runtime.clone(),
        state.image_id.clone(),
        profiles,
        default_profile,
        ceiling,
    )
    .await?;
    // Verify the pinned mission-type digest before anything runs.
    engine.load_state(mission_id).await?;
    Ok(engine)
}

/// Resolve `--repo` (an explicit path is canonicalized; omitted ⇒ the enclosing
/// git worktree root) and open its store. The root is discovered *before* the
/// store is opened, so a default never creates a stray `.lionclaw/` in a
/// subdirectory (`MissionStore::open` creates unconditionally).
async fn open_store(explicit_repo: Option<PathBuf>) -> Result<(PathBuf, MissionStore)> {
    let repo = match explicit_repo {
        Some(path) => path.canonicalize().context("repo path")?,
        None => git_worktree_root().await?,
    };
    let store = MissionStore::open(&repo).await?;
    Ok((repo, store))
}

/// The top level of the git worktree containing the current directory.
async fn git_worktree_root() -> Result<PathBuf> {
    let out = tokio::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .context("failed to run git")?;
    if !out.status.success() {
        bail!("--repo not given and the current directory is not inside a git repository");
    }
    let root = String::from_utf8_lossy(&out.stdout).trim().to_string();
    PathBuf::from(root).canonicalize().context("repo path")
}

/// Resolve a mission id: an explicit id is parsed; omitted ⇒ the sole live
/// mission, or — once every mission has finished — the sole mission overall
/// (so post-completion commands still default). Errors (never guesses) only
/// when the choice is ambiguous: no mission, or more than one live.
async fn resolve_mission_id(store: &MissionStore, explicit: Option<&str>) -> Result<MissionId> {
    if let Some(id) = explicit {
        return MissionId::parse(id).map_err(Into::into);
    }
    let mut all = Vec::new();
    let mut live = Vec::new();
    for mission_id in store.list_missions().await? {
        let Some(state) = fold(store.load(&mission_id).await?) else {
            continue;
        };
        if !state.phase.is_terminal() {
            live.push(mission_id.clone());
        }
        all.push(mission_id);
    }
    // Prefer the sole live mission; once every mission has finished, still default
    // to the sole mission — report/apply are post-completion commands. Refuse to
    // guess only when the choice is genuinely ambiguous.
    match (live.as_slice(), all.as_slice()) {
        ([only], _) => Ok(only.clone()),
        ([], [only]) => Ok(only.clone()),
        ([], []) => bail!("no mission in this repo; pass a mission id"),
        ([], _) => bail!("{} missions, none live; pass a mission id", all.len()),
        _ => bail!("{} live missions; pass a mission id to choose", live.len()),
    }
}

/// Read a JSON argument from a file, or from stdin when the path is `-`.
fn read_json_arg<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = if path == Path::new("-") {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read JSON from stdin")?;
        buf
    } else {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read '{}'", path.display()))?
    };
    serde_json::from_str(&text).context("invalid JSON")
}

async fn cmd_start(args: StartArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let base_sha = workspace::head_sha(&repo).await?;
    let workspace_dir = repo.to_string_lossy();
    let clock = SystemClock;
    let now_ms = clock.now_ms();
    let mission_id = MissionId::for_creation(&workspace_dir, &args.objective, now_ms);
    let mission_dir = store.mission_dir(&mission_id);
    let source = resolve_mission_type(&args.mission_type)?;
    create_mission_dir(&store, &mission_id)?;
    let result = async {
        let mission_type =
            snapshot_mission_type(&store, &mission_id, &source, &AuthorityCeiling::default())?;
        let engine = build_engine_for_start(
            store,
            &repo,
            mission_type,
            &args.runtime,
            args.image.as_deref(),
        )
        .await?;
        engine
            .create_mission_with_id(
                mission_id.clone(),
                now_ms,
                &repo.to_string_lossy(),
                &args.objective,
                &base_sha,
            )
            .await?;
        Ok::<_, anyhow::Error>((mission_id.clone(), engine))
    }
    .await;
    let (mission_id, engine) = match result {
        Ok(created) => created,
        Err(err) => {
            let _ = std::fs::remove_dir_all(&mission_dir);
            return Err(err);
        }
    };
    if args.json {
        println!(
            "{}",
            serde_json::json!({ "mission_id": mission_id.as_str(), "base_sha": base_sha })
        );
    } else {
        println!("started mission {mission_id} at {base_sha}");
        println!(
            "{}",
            start_next_step(
                engine.mission_type().planning.tasks.len(),
                &mission_id,
                &repo
            )
        );
    }
    Ok(())
}

fn snapshot_mission_type(
    store: &MissionStore,
    mission_id: &MissionId,
    source: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType> {
    materialize_mission_type(source, &store.mission_type_dir(mission_id), ceiling)
        .with_context(|| format!("mission type '{}' is invalid", source.display()))
}

fn create_mission_dir(store: &MissionStore, mission_id: &MissionId) -> Result<()> {
    let mission_dir = store.mission_dir(mission_id);
    let missions_dir = mission_dir
        .parent()
        .context("mission directory has no parent")?;
    std::fs::create_dir_all(missions_dir)
        .with_context(|| format!("creating '{}'", missions_dir.display()))?;
    std::fs::create_dir(&mission_dir)
        .with_context(|| format!("creating new mission directory '{}'", mission_dir.display()))
}

fn load_mission_type_snapshot(
    store: &MissionStore,
    mission_id: &MissionId,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType> {
    load_materialized_mission_type(&store.mission_type_dir(mission_id), ceiling)
        .with_context(|| format!("mission type snapshot for '{mission_id}'"))
}

fn start_next_step(planning_tasks: usize, mission_id: &MissionId, repo: &Path) -> String {
    let advance = format!(
        "lionclaw mission advance {mission_id} --repo {}",
        repo.display()
    );
    if planning_tasks == 0 {
        format!("next: propose a plan with `lionclaw mission plan propose`, then run: {advance}")
    } else {
        format!("next: {advance}")
    }
}

async fn cmd_inbox(args: InboxArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mut pending = Vec::new();
    for mission_id in store.list_missions().await? {
        let view = load_mission_view(&store, &mission_id).await?;
        if matches!(
            view.disposition,
            MissionDisposition::AwaitingPlan
                | MissionDisposition::AwaitingLead
                | MissionDisposition::Parked
                | MissionDisposition::CleanupBlocked
        ) {
            pending.push(view);
        }
    }
    if args.json {
        let mut missions = Vec::with_capacity(pending.len());
        for view in &pending {
            missions.push(mission_view_json(view, &store).await?);
        }
        println!("{}", serde_json::json!({ "missions": missions }));
    } else if pending.is_empty() {
        println!("inbox empty: no missions awaiting input or cleanup");
    } else {
        for view in &pending {
            print_mission_view(view, &store, false).await?;
            println!("  objective: {}", view.state.objective);
            println!("  next: {}", view.next_actions().join(" | "));
        }
    }
    Ok(())
}

async fn cmd_plan(command: PlanCommand) -> Result<()> {
    match command {
        PlanCommand::Show(args) => cmd_plan_show(args).await,
        PlanCommand::Propose(args) => cmd_plan_propose(args).await,
    }
}

async fn cmd_plan_show(args: PlanShowArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    let (plan, pending, base_revision) = if let Some(proposal) = &state.proposal {
        (&proposal.plan, true, proposal.base_revision)
    } else if let Some(plan) = &state.plan {
        (plan, false, state.revision)
    } else {
        bail!("mission {mission_id} has no current or pending plan");
    };
    // The verified/reviewed ceiling: a plan is verified-possible iff every
    // assertion binds an oracle.
    let ceiling = if plan.all_assertions_bound() {
        "verified-possible"
    } else {
        "reviewed-only"
    };
    if args.json {
        let bindings: Vec<_> = plan
            .assertions
            .iter()
            .map(|a| {
                serde_json::json!({
                    "id": a.id.as_str(),
                    "prose": a.prose,
                    "oracle": a.oracle.as_ref().map(|o| o.as_str()),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "pending": pending,
                "base_revision": base_revision,
                "ceiling": ceiling,
                "requirements": plan.requirements,
                "assertions": bindings,
                "tasks": plan.tasks,
            })
        );
    } else {
        let label = if pending { "pending" } else { "current" };
        println!("{label} plan for mission {mission_id} ({ceiling}):");
        for requirement in &plan.requirements {
            println!(
                "  {} [{:?}] {}",
                requirement.id, requirement.kind, requirement.prose
            );
        }
        for a in &plan.assertions {
            let oracle = a
                .oracle
                .as_ref()
                .map_or("- no oracle (advisory)", |o| o.as_str());
            println!("  {} -> {}\n    {}", a.id, oracle, a.prose);
        }
        println!("  ({} tasks)", plan.tasks.len());
    }
    Ok(())
}

async fn cmd_plan_propose(args: PlanProposeArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id).await?;
    let proposal = read_json_arg(&args.file)?;
    engine
        .propose_plan(&mission_id, proposal)
        .await
        .context("plan proposal rejected")?;
    println!("plan proposed for mission {mission_id}");
    Ok(())
}

/// The branch name and target commit for `apply`, or an error when the mission
/// produced no commit (`current == base`) — never create an empty branch.
fn apply_target(mission_id: &MissionId, base_sha: &str, current_sha: &str) -> Result<String> {
    if current_sha == base_sha {
        bail!("mission {mission_id} produced no commit to apply");
    }
    Ok(format!("lionclaw/{mission_id}"))
}

async fn cmd_apply(args: ApplyArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    let branch = apply_target(&mission_id, &state.base_sha, state.deliverable_head())?;
    workspace::create_branch(&repo, &branch, state.deliverable_head(), args.force)
        .await
        .with_context(|| {
            format!("could not create branch '{branch}' (already exists? use --force)")
        })?;
    println!(
        "applied mission {mission_id} → branch {branch} ({})",
        short_hex(state.deliverable_head())
    );
    Ok(())
}

async fn cmd_report(args: ReportArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let view = load_mission_view(&store, &mission_id).await?;
    let state = &view.state;
    let workspace_observations =
        crate::activity::task_workspace_observations(store.lionclaw_dir(), state).await;

    let finish = state.phase.finish();
    // Per-assertion evidence: the oracle that judged it, its exit code, the
    // commit it judged, whether that verdict is fresh at the final head, and a
    // short excerpt of its output.
    struct ReportRow {
        id: String,
        oracle: Option<String>,
        /// The oracle never ran and never will (a human waived it) — the row
        /// says so in both formats, never "owed".
        waived: bool,
        verdict: Option<serde_json::Value>,
    }
    let mut rows = Vec::new();
    for (aid, a) in &state.contract {
        let verdict = if let Some(v) = a.last_authoritative.as_ref() {
            let (stdout, stderr) = v.evidence();
            let evidence = crate::model::FailureEvidence {
                exit_code: v.exit_code(),
                exit_signal: v.exit_signal(),
                stdout: stdout.clone(),
                stderr: stderr.clone(),
            };
            Some(serde_json::json!({
                "oracle": v.oracle().as_str(),
                "passed": v.passed(),
                "exit_code": v.exit_code(),
                "exit_signal": v.exit_signal(),
                "judged_sha": v.judged_sha(),
                "fresh": v.is_fresh_at(state.deliverable_head()),
                "prepared_inputs": v.prepared_inputs(),
                "evidence": crate::evidence::evidence_json(store.blobs(), &evidence)?,
            }))
        } else {
            None
        };
        rows.push(ReportRow {
            id: aid.as_str().to_string(),
            oracle: a.oracle.as_ref().map(|o| o.as_str().to_string()),
            waived: a
                .oracle
                .as_ref()
                .is_some_and(|o| state.waived_oracles.contains(o)),
            verdict,
        });
    }
    let uncovered: Vec<&str> = state
        .contract
        .iter()
        .filter(|(_, a)| a.oracle.is_none())
        .map(|(id, _)| id.as_str())
        .collect();

    if args.json {
        use crate::model::ReviewOutcome;
        // Gated together with the summary: a config-less mission has no
        // review, whatever a hostile log writer recorded — the three review
        // fields must never contradict each other.
        let review = review_summary(state);
        let (review_gaps, review_report, review_acceptance) = if review.is_null() {
            (
                serde_json::Value::Null,
                serde_json::Value::Null,
                serde_json::Value::Null,
            )
        } else {
            // Gaps and the reviewer's report belong to the verdict; a stale
            // verdict describes a superseded tree, so only a FRESH one is
            // serialized (the summary's verdict/fresh fields say why).
            let (gaps, report) = match &state.terminal_review.outcome {
                Some(ReviewOutcome::Verdict(v)) if v.is_fresh_at(state.deliverable_head()) => (
                    serde_json::to_value(&v.gaps)?,
                    serde_json::Value::String(store.blobs().resolve(&v.report)?),
                ),
                _ => (serde_json::Value::Null, serde_json::Value::Null),
            };
            let accepted = state
                .terminal_review
                .accepted
                .as_ref()
                .map(|a| {
                    serde_json::json!({
                        "kind": a.kind.slug(),
                        "judged_sha": a.judged_sha,
                        "fresh": a.is_fresh_at(state.deliverable_head()),
                        "justification": a.justification,
                    })
                })
                .unwrap_or(serde_json::Value::Null);
            (gaps, report, accepted)
        };
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "objective": state.objective,
                "mission_type": { "name": state.mission_type.name, "digest": state.mission_type.digest },
                "runtime": state.runtime,
                "image_id": state.image_id,
                "stop_bar": state.config.stop.slug(),
                "base_sha": state.base_sha,
                "current_sha": state.current_sha,
                "finish": finish.map(|f| f.slug()),
                "disposition": view.disposition.slug(),
                "next_actions": view.next_actions(),
                "tasks": state.tasks.iter().map(|(id, task)| {
                    task_runtime_json(
                        &store,
                        id,
                        task,
                        workspace_observations.get(id),
                    )
                }).collect::<Result<Vec<_>>>()?,
                "planning_tasks": state.planning.tasks.iter().map(|(id, task)| {
                    task_runtime_json(&store, id, task, None)
                }).collect::<Result<Vec<_>>>()?,
                "conversations": conversation_views(state, &store)?,
                "assertions": rows.iter().map(|row| serde_json::json!({
                    "id": row.id,
                    "oracle": row.oracle,
                    "waived": row.waived,
                    "verdict": row.verdict,
                })).collect::<Vec<_>>(),
                "not_covered_by_an_oracle": uncovered,
                "waived_oracles": state.waived_oracles.iter().map(|o| o.as_str()).collect::<Vec<_>>(),
                "acknowledged_gates": state.acknowledged_gates.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
                "oracle_failures": state.oracle_failures,
                "terminal_review": review,
                "terminal_review_gaps": review_gaps,
                "terminal_review_report": review_report,
                "terminal_review_acceptance": review_acceptance,
                "attention": state.open_attention.values().map(|item| {
                    attention_json(store.blobs(), item)
                }).collect::<Result<Vec<_>>>()?,
                "cleanup_failure": cleanup_failure_json(state),
            })
        );
        return Ok(());
    }

    println!("Mission {mission_id} — {}", state.objective);
    println!(
        "  type:    {} @ {}",
        state.mission_type.name,
        short_hex(&state.mission_type.digest)
    );
    println!("  runtime: {}   image: {}", state.runtime, state.image_id);
    println!(
        "  commit:  {} → {}",
        short_hex(&state.base_sha),
        short_hex(state.deliverable_head())
    );
    match finish {
        Some(FinishClass::Verified) => {
            println!(
                "  finish:  VERIFIED — a fresh oracle pass at the final commit for every assertion"
            )
        }
        Some(FinishClass::InternallyConsistent) => {
            println!("  finish:  INTERNALLY-CONSISTENT — no machine checked this; an agent said so")
        }
        Some(FinishClass::Unverified) => {
            println!("  finish:  UNVERIFIED — not proven at the final commit")
        }
        None => println!(
            "  finish:  (not finished; phase {})",
            phase_slug(&state.phase)
        ),
    }
    println!("  bar:     {:?}", state.config.stop);
    println!(
        "  state:   {} ({})",
        phase_slug(&state.phase),
        view.disposition.slug()
    );
    for (task_id, task) in &state.tasks {
        if let Some(failure) = &task.last_failure {
            print_typed_failure(failure, &format!("  task {task_id} failure: "));
        }
        if let Some(configuration) = &task.last_runtime_configuration {
            println!(
                "  task {task_id}: model {:?} -> {:?}, mode {:?} -> {:?}",
                configuration.requested_model,
                configuration.applied_model,
                configuration.requested_mode,
                configuration.applied_mode,
            );
        }
        if let Some(response) = &task.final_response {
            println!("  task {task_id} final response:");
            for line in store.blobs().resolve(response)?.lines() {
                println!("    {line}");
            }
        }
    }
    print_task_workspace_observations(state, "  ", &workspace_observations);
    for (task_id, task) in &state.planning.tasks {
        if let Some(failure) = &task.last_failure {
            print_typed_failure(failure, &format!("  planning task {task_id} failure: "));
        }
        if let Some(configuration) = &task.last_runtime_configuration {
            println!(
                "  planning task {task_id}: model {:?} -> {:?}, mode {:?} -> {:?}",
                configuration.requested_model,
                configuration.applied_model,
                configuration.requested_mode,
                configuration.applied_mode,
            );
        }
        if let Some(response) = &task.final_response {
            println!("  planning task {task_id} final response:");
            for line in store.blobs().resolve(response)?.lines() {
                println!("    {line}");
            }
        }
    }
    print_conversations(state, &store, "  ")?;
    print_non_task_failures(state);
    if let Some(failure) = &state.cleanup_failure {
        println!(
            "  cleanup: blocked for effect {} ({:?}): {}",
            failure.effect_id,
            failure.resource,
            failure.failure.detail()
        );
    }
    if let Some(line) = review_line(state) {
        println!("  {line}");
        if let Some(a) = &state.terminal_review.accepted {
            println!(
                "           {} at {}: \"{}\"{}",
                a.kind.slug(),
                short_hex(&a.judged_sha),
                a.justification,
                if a.is_fresh_at(state.deliverable_head()) {
                    ""
                } else {
                    " [STALE — superseded by later work]"
                },
            );
        }
        if let Some(crate::model::ReviewOutcome::Verdict(v)) = &state.terminal_review.outcome {
            if v.is_fresh_at(state.deliverable_head()) {
                for gap in &v.gaps {
                    println!(
                        "    [{}] {}{}",
                        gap.severity.slug(),
                        gap.id
                            .as_deref()
                            .map(|id| format!("{id}: "))
                            .unwrap_or_default(),
                        gap.requirement,
                    );
                    println!("        expected: {}", gap.expected);
                    println!("        observed: {}", gap.observed);
                    if !gap.evidence.is_empty() {
                        println!("        evidence: {}", gap.evidence);
                    }
                }
                // The reviewer's own account — the requirement map and what
                // it observed — is the receipt's primary review evidence.
                println!("    reviewer's report:");
                for line in store.blobs().resolve(&v.report)?.lines() {
                    println!("      {line}");
                }
            } else {
                // Stale findings describe a superseded tree; never render
                // them like fresh ones.
                println!(
                    "    (a superseded verdict at {} recorded {} gap(s); see 'mission log')",
                    short_hex(&v.judged_sha),
                    v.gaps.len()
                );
            }
        }
    }
    println!("\n  assertions:");
    for row in &rows {
        let id = &row.id;
        match &row.verdict {
            Some(v) => {
                let passed = v["passed"].as_bool().unwrap_or(false);
                let fresh = v["fresh"].as_bool().unwrap_or(false);
                println!(
                    "    {id}: {} by {} (exit {}){}",
                    if passed { "PASS" } else { "FAIL" },
                    v["oracle"].as_str().unwrap_or("?"),
                    v["exit_code"],
                    if fresh {
                        ""
                    } else {
                        " [STALE — not at the final commit]"
                    },
                );
                let prepared = v["prepared_inputs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|input| {
                        Some(format!(
                            "{}@{}",
                            input["name"].as_str()?,
                            short_hex(input["digest"].as_str()?)
                        ))
                    })
                    .collect::<Vec<_>>();
                if !prepared.is_empty() {
                    println!("      prepared inputs: {}", prepared.join(", "));
                }
                if !passed {
                    let assertion = state.contract.get(
                        &crate::model::AssertionId::new(id).expect("stored assertion id")
                    ).expect("stored assertion");
                    let verdict = assertion.last_authoritative.as_ref().expect("report verdict");
                    let (stdout, stderr) = verdict.evidence();
                    let evidence = crate::model::FailureEvidence {
                        exit_code: verdict.exit_code(),
                        exit_signal: verdict.exit_signal(),
                        stdout: stdout.clone(),
                        stderr: stderr.clone(),
                    };
                    for line in crate::evidence::render_evidence(store.blobs(), &evidence)?.lines() {
                        println!("      {line}");
                    }
                }
            }
            None if row.waived => println!(
                "    {id}: WAIVED — its oracle failed to run and a human accepted closing without it"
            ),
            None if row.oracle.is_some() => println!("    {id}: (oracle owed, not yet run)"),
            None => println!("    {id}: advisory only — no oracle can prove this"),
        }
    }
    if !uncovered.is_empty() {
        println!(
            "\n  NOT covered by an oracle (agent judgement only): {}",
            uncovered.join(", ")
        );
    }
    if !state.acknowledged_gates.is_empty() {
        println!(
            "\n  gates a human confirmed or accepted: {}",
            state
                .acknowledged_gates
                .iter()
                .map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !state.open_attention.is_empty() {
        println!("\n  attention:");
        for item in state.open_attention.values() {
            print_attention(store.blobs(), item, "    ")?;
        }
    }
    if !matches!(view.disposition, MissionDisposition::Terminal) {
        println!("\n  next: {}", view.next_actions().join(" | "));
    }
    if args.patch && state.current_sha != state.base_sha {
        let diff = workspace::diff(&repo, &state.base_sha, state.deliverable_head()).await?;
        println!(
            "\n--- diff {}..{} ---\n{diff}",
            state.base_sha, state.current_sha
        );
    }
    Ok(())
}

async fn cmd_decide(args: DecideArgs) -> Result<()> {
    let action = parse_decision_action(&args.action)?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let justification = decision_text(&args, &action)?;
    let (_repo, store) = open_store(args.repo).await?;
    crate::engine::record_decision(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &args.item,
        action,
        &justification,
    )
    .await?;
    println!(
        "recorded decision on '{}' for mission {mission_id}",
        args.item
    );
    Ok(())
}

async fn cmd_send(args: SendArgs) -> Result<()> {
    if args.message.len() > MAX_MESSAGE_BYTES {
        bail!("message exceeds {MAX_MESSAGE_BYTES} bytes");
    }
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let events = store.load(&mission_id).await?;
    let state = fold(events.clone()).context("mission has no creation event")?;
    let current: Vec<_> = state
        .conversations
        .iter()
        .filter(|(_, conversation)| {
            conversation.lifecycle != ConversationLifecycle::Completed
                && state
                    .tasks_in(conversation.namespace)
                    .get(&conversation.task_id)
                    .is_some_and(|task| task.assignment_epoch == conversation.assignment_epoch)
        })
        .collect();
    let selected: Vec<_> = if args.all {
        current
    } else {
        let mut selected = Vec::new();
        for selector in &args.to {
            let matches: Vec<_> = current
                .iter()
                .copied()
                .filter(|(id, conversation)| {
                    id.as_str() == selector || conversation.task_id.as_str() == selector
                })
                .collect();
            match matches.as_slice() {
                [] => bail!("recipient '{selector}' is not a current conversation or task"),
                [one] => selected.push(*one),
                _ => bail!("task name '{selector}' is ambiguous; use a conversation id"),
            }
        }
        selected
    };
    if selected.is_empty() {
        bail!("recipient set is empty");
    }
    if selected.len() > crate::model::MAX_MESSAGE_RECIPIENTS {
        bail!("message has too many recipients");
    }
    let mut ids = std::collections::BTreeSet::new();
    if selected.iter().any(|(id, _)| !ids.insert((*id).clone())) {
        bail!("recipient set contains a duplicate conversation");
    }
    let recipients: Vec<_> = selected
        .into_iter()
        .map(|(conversation_id, conversation)| ConversationRecipient {
            conversation_id: conversation_id.clone(),
            role: conversation.role.clone(),
            namespace: conversation.namespace,
            task_id: conversation.task_id.clone(),
            assignment_epoch: conversation.assignment_epoch,
        })
        .collect();
    let mut references =
        Vec::with_capacity(args.receipts.len() + args.parks.len() + args.commits.len());
    if args.receipts.len() + args.parks.len() + args.commits.len()
        > crate::model::MAX_MESSAGE_REFERENCES
    {
        bail!("message has too many references");
    }
    for effect_id in &args.receipts {
        references.push(crate::model::MessageReference::AuthoritativeReceipt {
            effect_id: EffectId::parse(effect_id)?,
        });
    }
    for effect_id in &args.parks {
        references.push(crate::model::MessageReference::ParkEvidence {
            effect_id: EffectId::parse(effect_id)?,
        });
    }
    for sha in args.commits {
        references.push(crate::model::MessageReference::ReachableCommit { sha });
    }
    for reference in &references {
        let valid = match reference {
            crate::model::MessageReference::AuthoritativeReceipt { effect_id } => {
                state.authoritative_receipts.contains(effect_id)
            }
            crate::model::MessageReference::ParkEvidence { effect_id } => {
                state.parked_effects.contains_key(effect_id)
            }
            crate::model::MessageReference::ReachableCommit { sha } => {
                state.reachable_commits.contains(sha)
            }
        };
        if !valid {
            bail!("reference is not valid authority in this mission");
        }
    }
    crate::reference_materialization::materialize_references(
        &state,
        &events,
        store.blobs(),
        &repo,
        &references,
    )
    .await
    .context("message reference validation failed")?;
    store
        .append(
            &mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::MessageSent {
                recipients,
                body: args.message,
                references,
            })],
            SystemClock.now_ms(),
        )
        .await?;
    println!("message recorded for mission {mission_id}");
    Ok(())
}

async fn cmd_control(args: ControlArgs, resume: bool) -> Result<()> {
    let mission_id = MissionId::parse(&args.mission_id)?;
    let effect_id = EffectId::parse(&args.effect_id)?;
    let (_repo, store) = open_store(args.repo).await?;
    let action = if resume {
        ControlAction::Continue { automatic: false }
    } else {
        ControlAction::Stop
    };
    record_control(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &effect_id,
        action,
        &args.reason,
    )
    .await?;
    println!(
        "recorded {} for effect {effect_id}",
        if resume { "continue" } else { "stop" }
    );
    Ok(())
}

async fn cmd_extend(args: ExtendArgs) -> Result<()> {
    if args.seconds == 0 {
        bail!("--seconds must be greater than zero");
    }
    let mission_id = MissionId::parse(&args.mission_id)?;
    let effect_id = EffectId::parse(&args.effect_id)?;
    let (_repo, store) = open_store(args.repo).await?;
    let state = store.require_state(&mission_id).await?;
    let effect = state
        .inflight
        .get(&effect_id)
        .with_context(|| format!("effect '{effect_id}' is not active; control is stale"))?;
    let old_deadline_ms = effect.deadline_ms();
    let extension_ms = i64::try_from(
        args.seconds
            .checked_mul(1_000)
            .context("deadline extension overflows milliseconds")?,
    )
    .context("deadline extension is too large")?;
    let new_deadline_ms = old_deadline_ms
        .checked_add(extension_ms)
        .context("extended deadline overflows epoch milliseconds")?;
    record_control(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &effect_id,
        ControlAction::ExtendDeadline {
            old_deadline_ms,
            new_deadline_ms,
            automatic: false,
        },
        &args.reason,
    )
    .await?;
    println!("extended effect {effect_id} deadline to {new_deadline_ms}");
    Ok(())
}

fn parse_decision_action(action: &str) -> Result<DecisionAction> {
    Ok(match action {
        "approve" => DecisionAction::Approve,
        "retry" => DecisionAction::Retry,
        "repair" => DecisionAction::Repair,
        "revise" => DecisionAction::Revise,
        "accept" => DecisionAction::Accept,
        "abort" => DecisionAction::Abort,
        other => bail!("unknown action '{other}' (approve|retry|repair|revise|accept|abort)"),
    })
}

fn decision_text(args: &DecideArgs, action: &DecisionAction) -> Result<String> {
    if action == &DecisionAction::Revise {
        if args.justification.is_some() {
            bail!(
                "revise does not accept --justification; use --feedback-file or --feedback-stdin"
            );
        }
        return match (&args.feedback_file, args.feedback_stdin) {
            (Some(path), false) => read_feedback_file(path),
            (None, true) => {
                use std::io::Read;
                let mut bytes = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut bytes)
                    .context("failed to read revise feedback from stdin")?;
                decode_feedback(bytes, "stdin")
            }
            _ => bail!("revise requires exactly one of --feedback-file PATH or --feedback-stdin"),
        };
    }

    if args.feedback_file.is_some() || args.feedback_stdin {
        bail!("--feedback-file and --feedback-stdin are only valid with revise");
    }
    let justification = args
        .justification
        .as_deref()
        .context("non-revise decisions require --justification")?;
    if justification.trim().is_empty() {
        bail!("non-revise decisions require a non-empty --justification");
    }
    Ok(justification.to_string())
}

fn read_feedback_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read revise feedback from '{}'", path.display()))?;
    decode_feedback(bytes, &format!("'{}'", path.display()))
}

fn decode_feedback(bytes: Vec<u8>, source: &str) -> Result<String> {
    if bytes.is_empty() {
        bail!("revise feedback from {source} is empty");
    }
    String::from_utf8(bytes).with_context(|| format!("revise feedback from {source} is not UTF-8"))
}

#[cfg(unix)]
fn isolate_driver_process_group(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(not(unix))]
fn isolate_driver_process_group(_command: &mut std::process::Command) {}

#[cfg(unix)]
fn terminate_driver_process_group(process: &mut std::process::Child) -> Result<()> {
    let mut group_error = None;
    if let Some(pid) = rustix::process::Pid::from_raw(process.id() as i32) {
        match rustix::process::kill_process_group(pid, rustix::process::Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => {}
            Err(error) => group_error = Some(error),
        }
    }
    let _ = process.kill();
    let _ = process.wait();
    match group_error {
        Some(error) => Err(error).context("killing detached driver process group"),
        None => Ok(()),
    }
}

#[cfg(not(unix))]
fn terminate_driver_process_group(process: &mut std::process::Child) -> Result<()> {
    let _ = process.kill();
    let _ = process.wait();
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverStartup {
    Acquired,
    LostRace,
}

struct DetachedDriver {
    process: std::process::Child,
    stderr_spool: Option<std::process::Child>,
    cleanup_on_drop: bool,
}

impl DetachedDriver {
    async fn terminate_and_reap(&mut self) -> Result<()> {
        let group_result = terminate_driver_process_group(&mut self.process);
        self.settle_stderr().await;
        if group_result.is_ok() {
            self.cleanup_on_drop = false;
        }
        group_result
    }

    async fn settle_stderr(&mut self) {
        let Some(stderr_spool) = &mut self.stderr_spool else {
            return;
        };
        let settled = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if stderr_spool.try_wait()?.is_some() {
                    return std::io::Result::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if !matches!(settled, Ok(Ok(()))) {
            let _ = stderr_spool.kill();
            let _ = stderr_spool.wait();
        }
    }

    async fn wait(&mut self) -> Result<std::process::ExitStatus> {
        let status = loop {
            if let Some(status) = self.process.try_wait()? {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        self.settle_stderr().await;
        Ok(status)
    }

    fn detach(&mut self) {
        self.cleanup_on_drop = false;
    }
}

impl Drop for DetachedDriver {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        let _ = terminate_driver_process_group(&mut self.process);
        if let Some(stderr_spool) = &mut self.stderr_spool {
            let _ = stderr_spool.kill();
            let _ = stderr_spool.wait();
        }
    }
}

fn spawn_detached_driver(
    command: &mut std::process::Command,
    mission_dir: &Path,
) -> Result<DetachedDriver> {
    spawn_detached_driver_with(command, mission_dir, std::process::Command::spawn)
}

fn spawn_detached_driver_with(
    command: &mut std::process::Command,
    mission_dir: &Path,
    spawn_stderr_spool: impl FnOnce(&mut std::process::Command) -> std::io::Result<std::process::Child>,
) -> Result<DetachedDriver> {
    let executable = std::env::current_exe()?;
    command.stderr(std::process::Stdio::piped());
    let process = command
        .spawn()
        .context("spawning detached mission driver")?;
    let mut child = DetachedDriver {
        process,
        stderr_spool: None,
        cleanup_on_drop: true,
    };
    let stderr = child
        .process
        .stderr
        .take()
        .context("detached mission driver did not expose stderr")?;
    let mut spool = std::process::Command::new(executable);
    spool
        .arg("mission")
        .arg("driver-stderr")
        .arg("--mission-dir")
        .arg(mission_dir)
        .stdin(std::process::Stdio::from(stderr))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    isolate_driver_process_group(&mut spool);
    child.stderr_spool =
        Some(spawn_stderr_spool(&mut spool).context("spawning bounded driver stderr spool")?);
    Ok(child)
}

async fn await_driver_startup(
    child: &mut DetachedDriver,
    handshake: &Path,
    mission_dir: &Path,
) -> Result<DriverStartup> {
    await_driver_startup_with_timeout(child, handshake, mission_dir, Duration::from_secs(5)).await
}

async fn await_driver_startup_with_timeout(
    child: &mut DetachedDriver,
    handshake: &Path,
    mission_dir: &Path,
    timeout: Duration,
) -> Result<DriverStartup> {
    let startup = tokio::time::timeout(timeout, async {
        loop {
            if handshake.is_file() {
                return Ok(DriverStartup::Acquired);
            }
            if let Some(status) = child.process.try_wait()? {
                child.settle_stderr().await;
                if status.success() {
                    return Ok(DriverStartup::LostRace);
                }
                let detail = crate::activity::driver_error(mission_dir)
                    .unwrap_or_else(|| "no driver error evidence was recorded".into());
                bail!("mission driver exited before startup ({status}): {detail}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("mission driver startup handshake timed out")
    .and_then(|result| result);
    match startup {
        Ok(DriverStartup::Acquired) => {
            child.detach();
            Ok(DriverStartup::Acquired)
        }
        Ok(DriverStartup::LostRace) => {
            child.terminate_and_reap().await?;
            Ok(DriverStartup::LostRace)
        }
        Err(startup_error) => match child.terminate_and_reap().await {
            Ok(()) => Err(startup_error),
            Err(cleanup_error) => Err(startup_error.context(format!(
                "failed to clean up detached driver startup: {cleanup_error:#}"
            ))),
        },
    }
}

async fn wait_for_existing_driver(store: &MissionStore, mission_id: &MissionId) -> Result<()> {
    let lock_path = store.driver_lock_path(mission_id);
    tokio::task::spawn_blocking(move || crate::driver_lock::DriverGuard::acquire(&lock_path))
        .await
        .context("joining driver-lock waiter")??;
    Ok(())
}

async fn cmd_advance(args: AdvanceArgs) -> Result<std::process::ExitCode> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let initial = load_mission_view(&store, &mission_id).await?;
    let mut child = None;
    let mut startup = None;
    if matches!(
        initial.disposition,
        MissionDisposition::Ready | MissionDisposition::CleanupBlocked
    ) {
        let handshake = store.mission_dir(&mission_id).join(format!(
            "driver-{}-{}.ready",
            std::process::id(),
            SystemClock.now_ms()
        ));
        crate::activity::clear_driver_run_evidence(&store.mission_dir(&mission_id))?;
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .arg("mission")
            .arg("driver")
            .arg(mission_id.as_str())
            .arg("--repo")
            .arg(&repo)
            .arg("--handshake")
            .arg(&handshake)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null());
        isolate_driver_process_group(&mut command);
        let spawned = spawn_detached_driver(&mut command, &store.mission_dir(&mission_id))?;
        child = Some(spawned);
        let startup_result = await_driver_startup(
            child.as_mut().expect("driver was just spawned"),
            &handshake,
            &store.mission_dir(&mission_id),
        )
        .await;
        let _ = std::fs::remove_file(&handshake);
        startup = Some(startup_result?);
    }
    if args.wait {
        if let Some(mut child) = child {
            let status = child.wait().await?;
            if !status.success() {
                let detail = crate::activity::driver_error(&store.mission_dir(&mission_id))
                    .unwrap_or_else(|| "no driver error evidence was recorded".into());
                bail!("mission driver exited unsuccessfully ({status}): {detail}");
            }
            if startup == Some(DriverStartup::LostRace) {
                wait_for_existing_driver(&store, &mission_id).await?;
            }
        } else if matches!(initial.disposition, MissionDisposition::Running) {
            wait_for_existing_driver(&store, &mission_id).await?;
        }
    }
    let engine = build_engine_for_mission(store, &repo, &mission_id).await?;
    let view = load_mission_view(engine.store(), &mission_id).await?;
    let state = &view.state;
    print_mission_view(&view, engine.store(), args.json).await?;
    // Closing over acknowledged review gaps (or a waived review) was an
    // explicit, justified human decision — exit SUCCESS, but say so. The
    // summary already applies the freshness law, so a stale verdict from a
    // superseded head never triggers a false note here.
    if matches!(state.phase, MissionPhase::Done { .. }) {
        let summary = review_summary(state);
        let blocking = summary["gaps"]["blocking"].as_u64().unwrap_or(0);
        let total = blocking
            + summary["gaps"]["major"].as_u64().unwrap_or(0)
            + summary["gaps"]["minor"].as_u64().unwrap_or(0);
        if summary["verdict"] == serde_json::json!("gaps")
            && summary["acknowledged"].as_bool() == Some(true)
        {
            // Zero blocking gaps under a "gaps" verdict = the reviewer's
            // fail bit; the note must never read as "0 gaps waved through".
            if blocking == 0 {
                eprintln!(
                    "note: closed over a review that FAILED the product \
                     ({total} gap(s) recorded), acknowledged by a human; see 'mission report'"
                );
            } else {
                eprintln!(
                    "note: closed with {blocking} blocking gap(s) acknowledged by a human \
                     ({total} recorded in total); see 'mission report'"
                );
            }
        } else if summary["verdict"] == serde_json::json!("waived") {
            eprintln!("note: closed with the terminal review waived; see 'mission report'");
        }
    }
    // The exit code reflects the honesty bar: a mission that finished below the
    // stop bar its mission type declares exits nonzero, so a caller or CI can
    // gate on "actually verified" without parsing output.
    Ok(match &state.phase {
        MissionPhase::Done { finish } if !state.config.stop.satisfied_by(*finish) => {
            eprintln!(
                "finished {finish:?}, below the mission type's stop bar {:?}",
                state.config.stop
            );
            std::process::ExitCode::FAILURE
        }
        _ => std::process::ExitCode::SUCCESS,
    })
}

async fn cmd_driver(args: DriverArgs) -> Result<std::process::ExitCode> {
    let mission_id = MissionId::parse(&args.mission_id)?;
    let store = MissionStore::open(&args.repo).await?;
    let mission_dir = store.mission_dir(&mission_id);
    crate::activity::clear_driver_error(&mission_dir)?;
    let result = async {
        let engine = build_engine_for_mission(store, &args.repo, &mission_id).await?;
        engine
            .advance_with_handshake(&mission_id, Some(&args.handshake))
            .await?;
        Ok(std::process::ExitCode::SUCCESS)
    }
    .await;
    if let Err(error) = &result {
        let _ = crate::activity::record_driver_error(&mission_dir, error);
    }
    result
}

async fn cmd_driver_stderr(args: DriverStderrArgs) -> Result<std::process::ExitCode> {
    tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        crate::activity::spool_driver_stderr(stdin.lock(), &args.mission_dir)
    })
    .await
    .context("joining driver stderr spool")??;
    Ok(std::process::ExitCode::SUCCESS)
}

async fn cmd_status(args: StatusArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    if args.watch {
        return watch_status(&store, &mission_id, args.json).await;
    }
    let view = load_mission_view(&store, &mission_id).await?;
    let state = &view.state;
    if args.json {
        let mut value = mission_view_json(&view, &store).await?;
        let activity = running_activity_bytes(&store, &mission_id, view.disposition)
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .unwrap_or(serde_json::Value::Null);
        value["activity"] = activity;
        value["driver_error"] = crate::activity::driver_error(&store.mission_dir(&mission_id))
            .map_or(serde_json::Value::Null, serde_json::Value::String);
        println!("{value}");
    } else {
        let workspace_observations =
            crate::activity::task_workspace_observations(store.lionclaw_dir(), state).await;
        println!(
            "mission {mission_id}: {} (revision {}, {})",
            phase_slug(&state.phase),
            state.revision,
            view.disposition.slug(),
        );
        println!("objective: {}", state.objective);
        if let Some(failure) = &state.cleanup_failure {
            println!(
                "cleanup blocked for effect {} ({:?}): {}",
                failure.effect_id,
                failure.resource,
                failure.failure.detail()
            );
        }
        if let Some(line) = review_line(state) {
            println!("{line}");
        }
        for (id, assertion) in &state.contract {
            let auth = assertion
                .last_authoritative
                .as_ref()
                .map(|v| if v.passed() { "pass" } else { "fail" })
                .unwrap_or("—");
            println!("  {id}: authoritative={auth}");
        }
        for (id, task) in &state.planning.tasks {
            if task.status != crate::model::TaskStatus::Pending {
                println!(
                    "  planning {id}: status={:?} runtime={:?}",
                    task.status, task.last_runtime_configuration
                );
                if let Some(response) = &task.final_response {
                    println!("    final response: {}", store.blobs().resolve(response)?);
                }
            }
        }
        print_conversations(state, &store, "  ")?;
        print_task_workspace_observations(state, "  ", &workspace_observations);
        if view.disposition == MissionDisposition::Running {
            print_activity(&store, &mission_id)?;
        }
        if let Some(error) = crate::activity::driver_error(&store.mission_dir(&mission_id)) {
            println!("driver error: {error}");
        }
        for (effect_id, parked) in &state.parked_effects {
            println!(
                "parked effect {}: {:?}; legal control=continue",
                effect_id, parked
            );
        }
        print_planning_input(store.blobs(), state, "")?;
        for item in state.open_attention.values() {
            print_attention(store.blobs(), item, "  ")?;
        }
        println!("next: {}", view.next_actions().join(" | "));
    }
    Ok(())
}

async fn watch_status(store: &MissionStore, mission_id: &MissionId, json: bool) -> Result<()> {
    let mut previous = Vec::new();
    loop {
        let disposition = load_mission_view(store, mission_id).await?.disposition;
        let Some(bytes) = running_activity_bytes(store, mission_id, disposition) else {
            return Ok(());
        };
        if !bytes.is_empty() && bytes != previous {
            if json {
                println!("{}", String::from_utf8_lossy(&bytes));
            } else if let Ok(activity) =
                serde_json::from_slice::<crate::activity::ActivityProjection>(&bytes)
            {
                for effect in activity.effects {
                    println!(
                        "{} {} elapsed={}ms deadline={} workspace={}",
                        effect.effect_id,
                        effect.last_activity,
                        effect.elapsed_ms,
                        effect.deadline_ms,
                        workspace_observation_summary(&effect.workspace)
                    );
                }
            }
            previous = bytes;
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

fn running_activity_bytes(
    store: &MissionStore,
    mission_id: &MissionId,
    disposition: MissionDisposition,
) -> Option<Vec<u8>> {
    (disposition == MissionDisposition::Running).then(|| {
        std::fs::read(crate::activity::path(&store.mission_dir(mission_id))).unwrap_or_default()
    })
}

fn print_activity(store: &MissionStore, mission_id: &MissionId) -> Result<()> {
    let path = crate::activity::path(&store.mission_dir(mission_id));
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(());
    };
    let activity: crate::activity::ActivityProjection = serde_json::from_slice(&bytes)?;
    for effect in activity.effects {
        println!(
            "activity {}: {} elapsed={}ms deadline={} controls={}",
            effect.effect_id,
            effect.last_activity,
            effect.elapsed_ms,
            effect.deadline_ms,
            effect.legal_controls.join("|")
        );
    }
    Ok(())
}

async fn cmd_log(args: LogArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    for envelope in store.load(&mission_id).await? {
        println!(
            "{:>4} {}",
            envelope.sequence_no,
            envelope.event.event_type()
        );
    }
    Ok(())
}

async fn cmd_install(args: InstallArgs) -> Result<()> {
    let home = Home::from_env()?;
    let bundled = if args.mission_types.is_empty() {
        Some(BundledMissionTypes::materialize()?)
    } else {
        None
    };
    let sources = match &bundled {
        Some(bundle) => mission_type_directories(bundle.root())?,
        None => args.mission_types,
    };
    let dest_dir = home.mission_types_dir();
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("creating '{}'", dest_dir.display()))?;

    let mut installed_count = 0usize;
    for src_dir in sources {
        let outcome = install_mission_type(
            &src_dir,
            &dest_dir,
            args.force,
            &AuthorityCeiling::default(),
        )
        .with_context(|| format!("mission type at '{}' is invalid", src_dir.display()))?;
        if outcome.installed {
            println!("installed {}", outcome.name);
            installed_count += 1;
        } else {
            println!(
                "skip {} (already installed; --force to overwrite)",
                outcome.name
            );
        }
    }
    if installed_count == 0 {
        println!("nothing to install (all mission types already present)");
    }
    println!("home: {}", home.root().display());
    Ok(())
}

fn mission_type_directories(parent: &Path) -> Result<Vec<PathBuf>> {
    let mut directories = std::fs::read_dir(parent)
        .with_context(|| format!("reading '{}'", parent.display()))?
        .filter_map(|entry| match entry {
            Ok(entry) => match entry.file_type() {
                Ok(file_type) if file_type.is_dir() => Some(Ok(entry.path())),
                Ok(_) => None,
                Err(err) => Some(Err(err.into())),
            },
            Err(err) => Some(Err(err.into())),
        })
        .collect::<Result<Vec<_>>>()?;
    directories.sort();
    Ok(directories)
}

fn resolve_mission_type(raw: &str) -> Result<PathBuf> {
    MissionTypeLocator::parse(raw)?.resolve()
}

async fn cmd_skill(cmd: SkillCommand) -> Result<()> {
    match cmd {
        SkillCommand::Add(args) => {
            let mission_root = resolve_mission_type(&args.mission_type)?;
            let source = match (args.path, args.git) {
                (Some(path), None) => SkillSource::Path(path),
                (None, Some(git)) => SkillSource::Git {
                    git,
                    rev: args
                        .rev
                        .context("--rev is required when adding a Git skill")?,
                    subdir: args.subdir.unwrap_or_default(),
                },
                _ => unreachable!("clap enforces exactly one skill source"),
            };
            let change = add_skill(
                &mission_root,
                source,
                args.force,
                &AuthorityCeiling::default(),
            )
            .await?;
            if change.changed {
                println!("added {} {}", change.name, short_hex(&change.digest));
            } else {
                println!("unchanged {} {}", change.name, short_hex(&change.digest));
            }
        }
        SkillCommand::Remove(args) => {
            let mission_root = resolve_mission_type(&args.mission_type)?;
            let change = remove_skill(&mission_root, &args.name, &AuthorityCeiling::default())?;
            println!("removed {} {}", change.name, short_hex(&change.digest));
        }
    }
    Ok(())
}

async fn cmd_doctor() -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    let mut ok = true;
    let mut check = |label: &str, pass: bool, detail: &str| {
        println!(
            "{} {label}{}",
            if pass { "PASS" } else { "FAIL" },
            if detail.is_empty() {
                String::new()
            } else {
                format!(" — {detail}")
            }
        );
        ok &= pass;
    };

    check("podman", command_ok("podman", &["--version"]).await, "");
    check("git", command_ok("git", &["--version"]).await, "");

    let home = Home::from_env()?;
    let profiles = match RuntimeProfiles::load(&home) {
        Ok(profiles) => {
            check(
                "runtime profiles",
                true,
                &profiles.names().collect::<Vec<_>>().join(", "),
            );
            Some(profiles)
        }
        Err(err) => {
            check("runtime profiles", false, &format!("{err:#}"));
            None
        }
    };
    let types = home.installed_mission_types()?;
    if types.is_empty() {
        check(
            "mission types installed",
            false,
            "none — run `lionclaw install`",
        );
    }
    for name in &types {
        match load_mission_type(&home.mission_type_dir(name), &AuthorityCeiling::default()) {
            Ok(mt) => {
                if let Some(profiles) = &profiles {
                    if let Err(err) = validate_explicit_role_runtimes(&mt, profiles) {
                        check(
                            &format!("mission type '{name}'"),
                            false,
                            &format!("{err:#}"),
                        );
                        continue;
                    }
                }
                let img = command_ok("podman", &["image", "exists", &mt.image]).await;
                check(
                    &format!("mission type '{name}'"),
                    img,
                    &if img {
                        short_hex(mt.digest())
                    } else {
                        format!("image '{}' not present", mt.image)
                    },
                );
            }
            Err(e) => check(&format!("mission type '{name}'"), false, &e.to_string()),
        }
    }
    Ok(if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

async fn command_ok(program: &str, args: &[&str]) -> bool {
    tokio::process::Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn cmd_type(cmd: TypeCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        TypeCommand::List(args) => {
            let home = Home::from_env()?;
            let types = home.installed_mission_types()?;
            if args.json {
                println!("{}", serde_json::json!({ "mission_types": types }));
            } else if types.is_empty() {
                println!("no mission types installed (run `lionclaw install`)");
            } else {
                for name in types {
                    println!("{name}");
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        TypeCommand::Show(args) => {
            let dir = resolve_mission_type(&args.mission_type)?;
            show_mission_type(&dir, args.json)
        }
        TypeCommand::Check(args) => {
            let source = resolve_mission_type(&args.mission_type)?;
            let prepared = tempfile::tempdir().context("preparing mission type check")?;
            let mission_type = materialize_mission_type(
                &source,
                &prepared.path().join("mission-type"),
                &AuthorityCeiling::default(),
            )
            .context("mission type invalid")?;
            validate_explicit_role_runtimes(&mission_type, &runtime_profiles()?)
                .context("mission type runtime unavailable")?;
            show_loaded_mission_type(&mission_type, args.json);
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// Load a mission type from `dir` and print it; nonzero exit if it fails to
/// load (so a caller can gate on validity).
fn show_mission_type(dir: &Path, json: bool) -> Result<std::process::ExitCode> {
    match load_mission_type(dir, &AuthorityCeiling::default()) {
        Ok(mt) => {
            validate_explicit_role_runtimes(&mt, &runtime_profiles()?)
                .context("mission type runtime unavailable")?;
            show_loaded_mission_type(&mt, json);
            Ok(std::process::ExitCode::SUCCESS)
        }
        // Bubble up — `run_mission` renders the `{"ok":false,…}` envelope for
        // `--json` and lets the human path print to stderr; either way exit 1.
        Err(err) => Err(anyhow::Error::from(err).context("mission type invalid")),
    }
}

fn show_loaded_mission_type(mt: &MissionType, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "name": mt.name,
                "digest": mt.digest(),
                "stop": mt.stop.slug(),
                "image": mt.image,
                "roles": mt.roles.keys().map(|role| role.as_str()).collect::<Vec<_>>(),
                "role_skills": mt.roles.values().map(|role| {
                    (role.name.as_str(), &role.skills)
                }).collect::<std::collections::BTreeMap<_, _>>(),
                "skills": mt.skills.keys().collect::<Vec<_>>(),
                "inputs": mt.inputs.values().map(|input| {
                    serde_json::json!({
                        "name": input.name.as_str(),
                        "network": input.network,
                        "key_files": input.key_files,
                        "environment": input.environment,
                    })
                }).collect::<Vec<_>>(),
                "oracles": mt.oracles.keys().map(|o| o.as_str()).collect::<Vec<_>>(),
                "terminal_review": mt.terminal_review.as_ref().map(|tr| {
                    serde_json::json!({ "role": tr.role.as_str() })
                }),
                "playbook": mt.playbook,
            })
        );
        return;
    }
    println!("mission type '{}' is valid", mt.name);
    println!("  digest: {}", short_hex(mt.digest()));
    println!("  stop:  {:?}", mt.stop);
    println!("  image: {}", mt.image);
    if let Some(tr) = &mt.terminal_review {
        println!("  terminal review: {}", tr.role);
    }
    println!(
        "  roles: {}",
        mt.roles
            .keys()
            .map(|r| r.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  skills: {}",
        mt.skills.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    println!(
        "  inputs: {}",
        mt.inputs
            .values()
            .map(|input| format!(
                "{} (network={}, keys={})",
                input.name,
                input.network,
                input
                    .key_files
                    .iter()
                    .map(|path| path.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("+")
            ))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  oracles: {}",
        mt.oracles
            .keys()
            .map(|o| o.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(playbook) = &mt.playbook {
        println!("\n--- playbook ---\n{playbook}");
    }
}

async fn print_mission_view(view: &MissionView, store: &MissionStore, json: bool) -> Result<()> {
    let state = &view.state;
    let blobs = store.blobs();
    let mission_id = state.mission_id.as_str();
    if json {
        println!("{}", mission_view_json(view, store).await?);
    } else {
        match view.disposition {
            MissionDisposition::AwaitingPlan => {
                println!("mission {mission_id}: awaiting a plan");
                print_planning_input(blobs, state, "  ")?;
            }
            MissionDisposition::Parked => {
                let workspace_observations =
                    crate::activity::task_workspace_observations(store.lionclaw_dir(), state).await;
                println!(
                    "mission {mission_id}: parked ({} attention item(s))",
                    state.open_attention.len()
                );
                for item in state.open_attention.values() {
                    print_attention(blobs, item, "  ")?;
                }
                for (task_id, task) in &state.tasks {
                    if let Some(failure) = &task.last_failure {
                        print_typed_failure(failure, &format!("  task {task_id} failure: "));
                    }
                    if let Some(response) = &task.final_response {
                        println!("  task {task_id} final response:");
                        for line in blobs.resolve(response)?.lines() {
                            println!("    {line}");
                        }
                    }
                }
                print_task_workspace_observations(state, "  ", &workspace_observations);
                print_non_task_failures(state);
            }
            MissionDisposition::Running => {
                println!("mission {mission_id}: running under another driver")
            }
            MissionDisposition::AwaitingLead => {
                println!("mission {mission_id}: awaiting lead feedback");
                print_conversations(state, store, "  ")?;
            }
            MissionDisposition::CleanupBlocked => {
                let failure = state
                    .cleanup_failure
                    .as_ref()
                    .expect("cleanup-blocked view has failure detail");
                println!(
                    "mission {mission_id}: cleanup blocked for effect {} ({:?})",
                    failure.effect_id, failure.resource
                );
                println!("  {}", failure.failure.detail());
            }
            MissionDisposition::Terminal => {
                println!("mission {mission_id}: {}", phase_slug(&state.phase));
                if let Some(line) = review_line(state) {
                    println!("  {line}");
                }
            }
            MissionDisposition::Ready => println!("mission {mission_id}: ready to advance"),
        }
    }
    Ok(())
}

fn print_task_workspace_observations(
    state: &crate::model::MissionState,
    indent: &str,
    workspace_observations: &std::collections::BTreeMap<
        crate::model::TaskId,
        crate::activity::WorkspaceObservation,
    >,
) {
    for task_id in state.tasks.keys() {
        match workspace_observations.get(task_id) {
            Some(crate::activity::WorkspaceObservation::Changed { diffstat }) => {
                println!("{indent}task {task_id} retained work:");
                for line in diffstat.lines() {
                    println!("{indent}  {line}");
                }
            }
            Some(crate::activity::WorkspaceObservation::Unavailable { reason }) => {
                println!("{indent}task {task_id} workspace observation unavailable: {reason}");
            }
            Some(crate::activity::WorkspaceObservation::NotApplicable)
            | Some(crate::activity::WorkspaceObservation::NotCreated)
            | Some(crate::activity::WorkspaceObservation::Clean)
            | None => {}
        }
    }
}

fn workspace_observation_summary(observation: &crate::activity::WorkspaceObservation) -> String {
    match observation {
        crate::activity::WorkspaceObservation::NotApplicable => "n/a".into(),
        crate::activity::WorkspaceObservation::NotCreated => "not-created".into(),
        crate::activity::WorkspaceObservation::Clean => "clean".into(),
        crate::activity::WorkspaceObservation::Changed { diffstat } => diffstat.clone(),
        crate::activity::WorkspaceObservation::Unavailable { reason } => {
            format!("unavailable ({reason})")
        }
    }
}

async fn mission_view_json(view: &MissionView, store: &MissionStore) -> Result<serde_json::Value> {
    let state = &view.state;
    let blobs = store.blobs();
    let workspace_observations =
        crate::activity::task_workspace_observations(store.lionclaw_dir(), state).await;
    Ok(serde_json::json!({
        "mission_id": state.mission_id.as_str(),
        "phase": phase_slug(&state.phase),
        "finish": state.phase.finish().map(|finish| finish.slug()),
        "disposition": view.disposition.slug(),
        "next_actions": view.next_actions(),
        "revision": state.revision,
        "current_sha": state.current_sha,
        "objective": state.objective,
        "conversations": conversation_views(state, store)?,
        "tasks": state.tasks.iter().map(|(id, task)| {
            task_runtime_json(
                store,
                id,
                task,
                workspace_observations.get(id),
            )
        }).collect::<Result<Vec<_>>>()?,
        "planning_tasks": state.planning.tasks.iter().map(|(id, task)| {
            task_runtime_json(
                store,
                id,
                task,
                Some(&crate::activity::WorkspaceObservation::NotApplicable),
            )
        }).collect::<Result<Vec<_>>>()?,
        "planning_input": planning_input_json(state, blobs)?,
        "contract": state.contract.iter().map(|(id, assertion)| {
            serde_json::json!({
                "id": id.as_str(),
                "advisory": assertion.advisory.slug(),
                "authoritative_pass": assertion
                    .last_authoritative
                    .as_ref()
                    .map(|verdict| verdict.passed()),
            })
        }).collect::<Vec<_>>(),
        "attention": state.open_attention.values().map(|item| {
            attention_json(blobs, item)
        }).collect::<Result<Vec<_>>>()?,
        "cleanup_failure": cleanup_failure_json(state),
        "oracle_failures": state.oracle_failures,
        "parked_effects": state.parked_effects.iter().map(|(effect_id, parked)| {
            serde_json::json!({
                "effect_id": effect_id.as_str(),
                "kind": parked,
                "legal_controls": ["continue"],
            })
        }).collect::<Vec<_>>(),
        "terminal_review": review_summary(state),
    }))
}

fn task_runtime_json(
    store: &MissionStore,
    id: &crate::model::TaskId,
    task: &crate::model::TaskRuntimeState,
    workspace_observation: Option<&crate::activity::WorkspaceObservation>,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "id": id.as_str(),
        "status": format!("{:?}", task.status).to_ascii_lowercase(),
        "workspace_base_sha": task.workspace_base_sha,
        "assignment_epoch": task.assignment_epoch,
        "workspace_observation": workspace_observation,
        "runtime_configuration": task.last_runtime_configuration,
        "failure": task.last_failure,
        "final_response": task.final_response.as_ref()
            .map(|response| store.blobs().resolve(response))
            .transpose()?,
    }))
}

fn conversation_views(
    state: &crate::model::MissionState,
    store: &MissionStore,
) -> Result<Vec<serde_json::Value>> {
    state
        .conversations
        .iter()
        .map(|(id, conversation)| {
            let runtime_root = store
                .mission_dir(&state.mission_id)
                .join("conversations")
                .join(id.as_str())
                .join("runtime");
            let resume_mode = match lionclaw_runtime_api::recorded_runtime_resume_mode(
                &runtime_root,
            )? {
                Some(lionclaw_runtime_api::RuntimeResumeMode::Resumed) => "native_session",
                Some(lionclaw_runtime_api::RuntimeResumeMode::Reconstructed) | None => {
                    "canonical_reconstruction"
                }
            };
            Ok(serde_json::json!({
                "id": id.as_str(),
                "role": conversation.role.as_str(),
                "namespace": conversation.namespace,
                "task_id": conversation.task_id.as_str(),
                "assignment_epoch": conversation.assignment_epoch,
                "workspace_base_sha": conversation.workspace_base_sha,
                "lifecycle": conversation.lifecycle,
                "final_response": state.tasks_in(conversation.namespace)
                    .get(&conversation.task_id)
                    .and_then(|task| task.final_response.as_ref())
                    .map(|response| store.blobs().resolve(response))
                    .transpose()?,
                "queued_messages": conversation.queued,
                "consumed_through": conversation.consumed_through,
                "active_message_boundary": conversation.active_delivery.as_ref().map(|delivery| delivery.message_boundary),
                "presented_messages": conversation.active_delivery.as_ref().map(|delivery| &delivery.presented_messages),
                "invalid_handoff_reworks": conversation.invalid_handoff_reworks,
                "runtime_resume_mode": resume_mode,
                "legal_actions": match conversation.lifecycle {
                    crate::model::ConversationLifecycle::AwaitingLead => vec!["mission send"],
                    crate::model::ConversationLifecycle::Running => vec!["mission status", "mission send"],
                    crate::model::ConversationLifecycle::Ready
                    | crate::model::ConversationLifecycle::ReworkingInvalidHandoff => vec!["mission advance", "mission send"],
                    crate::model::ConversationLifecycle::Completed => Vec::new(),
                },
            }))
        })
        .collect()
}

fn print_conversations(
    state: &crate::model::MissionState,
    store: &MissionStore,
    indent: &str,
) -> Result<()> {
    for conversation in conversation_views(state, store)? {
        println!(
            "{indent}conversation {}: lifecycle={} queued={} delivery_through={} resume={}",
            conversation["id"].as_str().unwrap_or("?"),
            conversation["lifecycle"].as_str().unwrap_or("?"),
            conversation["queued_messages"]
                .as_array()
                .map_or(0, Vec::len),
            conversation["consumed_through"],
            conversation["runtime_resume_mode"].as_str().unwrap_or("?")
        );
        if let Some(response) = conversation["final_response"].as_str() {
            println!("{indent}  final response: {response}");
        }
    }
    Ok(())
}

fn planning_input_json(
    state: &crate::model::MissionState,
    blobs: &BlobStore,
) -> Result<serde_json::Value> {
    if state.planning_input.latest_rejected_proposal.is_none()
        && state.planning_input.refinement.is_none()
    {
        return Ok(serde_json::Value::Null);
    }
    let refinement = match state.planning_input.refinement.as_ref() {
        Some(crate::model::PlanningRefinement::Guidance(guidance)) => serde_json::json!({
            "kind": "guidance",
            "text": guidance,
        }),
        Some(crate::model::PlanningRefinement::FailureEvidence(feedback)) => serde_json::json!({
            "kind": "failure_evidence",
            "summary": feedback.summary,
            "justification": feedback.justification,
            "evidence": feedback.evidence.as_ref()
                .map(|evidence| crate::evidence::evidence_json(blobs, evidence))
                .transpose()?,
            "details": feedback.details.as_ref()
                .map(|details| blobs.resolve(details).map(|text| crate::evidence::excerpt(&text)))
                .transpose()?,
        }),
        None => serde_json::Value::Null,
    };
    Ok(serde_json::json!({
        "base_revision": state.planning_base_revision.unwrap_or(state.revision),
        "latest_rejected_proposal": state.planning_input.latest_rejected_proposal,
        "refinement": refinement,
    }))
}

fn print_planning_input(
    blobs: &BlobStore,
    state: &crate::model::MissionState,
    indent: &str,
) -> Result<()> {
    let input = &state.planning_input;
    if input.latest_rejected_proposal.is_none() && input.refinement.is_none() {
        return Ok(());
    }
    println!("{indent}active planning input:");
    if let Some(proposal) = &input.latest_rejected_proposal {
        println!(
            "{indent}  latest rejected complete proposal targeted revision {}",
            proposal.base_revision
        );
    }
    match input.refinement.as_ref() {
        Some(crate::model::PlanningRefinement::Guidance(guidance)) => {
            println!("{indent}  human guidance:");
            for line in guidance.lines() {
                println!("{indent}    {line}");
            }
        }
        Some(crate::model::PlanningRefinement::FailureEvidence(feedback)) => {
            println!("{indent}  failure evidence:");
            for line in crate::evidence::render_feedback(blobs, feedback)?.lines() {
                println!("{indent}    {line}");
            }
        }
        None => {}
    }
    Ok(())
}

fn cleanup_failure_json(state: &crate::model::MissionState) -> serde_json::Value {
    state
        .cleanup_failure
        .as_ref()
        .map(|failure| {
            serde_json::json!({
                "effect_id": failure.effect_id.as_str(),
                "resource": failure.resource,
                "kind": failure.failure.category(),
                "detail": failure.failure.detail(),
            })
        })
        .unwrap_or(serde_json::Value::Null)
}

fn print_typed_failure(failure: &lionclaw_runtime_api::TypedFailure, prefix: &str) {
    let evidence = failure.evidence();
    println!("{prefix}{}: {}", failure.category(), evidence.detail);
    if let Some(code) = &evidence.code {
        println!("    code: {code}");
    }
    if let Some(reason) = &evidence.stop_reason {
        println!("    stop reason: {reason}");
    }
    if let Some(code) = evidence.exit_code {
        println!("    exit code: {code}");
    }
    if !evidence.stderr.is_empty() {
        println!("    stderr: {}", evidence.stderr);
    }
    if !evidence.final_response.is_empty() {
        println!("    final response: {}", evidence.final_response);
    }
    let configuration = &evidence.configuration;
    if configuration.requested_model.is_some()
        || configuration.applied_model.is_some()
        || configuration.requested_mode.is_some()
        || configuration.applied_mode.is_some()
    {
        println!(
            "    runtime configuration: model {:?} -> {:?}, mode {:?} -> {:?}",
            configuration.requested_model,
            configuration.applied_model,
            configuration.requested_mode,
            configuration.applied_mode,
        );
    }
}

fn print_non_task_failures(state: &crate::model::MissionState) {
    for (oracle, failure) in &state.oracle_failures {
        print_typed_failure(failure, &format!("  oracle {oracle} failure: "));
    }
    if let Some(crate::model::ReviewOutcome::Failed { failure }) = &state.terminal_review.outcome {
        print_typed_failure(failure, "  terminal review failure: ");
    }
}

fn attention_json(
    blobs: &BlobStore,
    item: &crate::model::AttentionItem,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "id": item.id,
        "kind": item.kind.slug(),
        "report": item.report,
        "assertion_ids": item.assertion_ids.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
        "actions": crate::model::decision::allowed_actions(item.kind)
            .iter()
            .map(crate::model::DecisionAction::slug)
            .collect::<Vec<_>>(),
        "evidence": item.evidence.as_ref()
            .map(|evidence| crate::evidence::evidence_json(blobs, evidence))
            .transpose()?,
        "details": item.details.as_ref()
            .map(|details| blobs.resolve(details).map(|text| crate::evidence::excerpt(&text)))
            .transpose()?,
    }))
}

fn print_attention(
    blobs: &BlobStore,
    item: &crate::model::AttentionItem,
    indent: &str,
) -> Result<()> {
    println!("{indent}[{}] {}", item.id, item.report);
    let actions = crate::model::decision::allowed_actions(item.kind)
        .iter()
        .map(crate::model::DecisionAction::slug)
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{indent}  actions: {actions}");
    if let Some(evidence) = &item.evidence {
        for line in crate::evidence::render_evidence(blobs, evidence)?.lines() {
            println!("{indent}  {line}");
        }
    }
    if let Some(details) = &item.details {
        println!("{indent}  detailed report:");
        for line in crate::evidence::excerpt(&blobs.resolve(details)?).lines() {
            println!("{indent}    {line}");
        }
    }
    Ok(())
}

/// The mission phase as a slug, carrying the finish grade for `Done`
/// (`done:verified`). The variant slugs live on the enums (one source).
fn phase_slug(phase: &MissionPhase) -> String {
    match phase {
        MissionPhase::Done { finish } => format!("done:{}", finish.slug()),
        other => other.slug().to_string(),
    }
}

/// The terminal-review summary — ONE source of truth behind the advance
/// banner, `status`, `report`, and every `--json` output. `Null` when the
/// mission declares no review.
fn review_summary(state: &crate::model::MissionState) -> serde_json::Value {
    use crate::model::{AttentionKind, GapSeverity, ReviewOutcome};
    let Some(config) = &state.config.terminal_review else {
        return serde_json::Value::Null;
    };
    let tr = &state.terminal_review;
    // A terminal mission owes nothing: whatever is not settled by a fresh
    // verdict or a fresh waiver was deliberately skipped (a below-bar finish
    // never burns a review; an abort ends everything) — never report it as
    // still "owed".
    let done = state.phase.is_terminal();
    let proof_failed = state.open_attention.values().any(|item| {
        matches!(
            item.kind,
            AttentionKind::OracleFailed | AttentionKind::OracleVerdictFailed
        )
    });
    let waived = tr.waived_at(state.deliverable_head());
    let (verdict, judged_sha, fresh, counts, acknowledged) = match &tr.outcome {
        Some(ReviewOutcome::Verdict(v)) => {
            let count = |s: GapSeverity| v.gaps.iter().filter(|g| g.severity == s).count();
            let is_fresh = v.is_fresh_at(state.deliverable_head());
            let kind = if !is_fresh && done {
                "skipped"
            } else if v.blocking() {
                "gaps"
            } else {
                "clean"
            };
            (
                kind,
                Some(v.judged_sha.clone()),
                Some(is_fresh),
                Some(serde_json::json!({
                    "blocking": count(GapSeverity::Blocking),
                    "major": count(GapSeverity::Major),
                    "minor": count(GapSeverity::Minor),
                })),
                tr.acknowledges(v),
            )
        }
        Some(ReviewOutcome::Failed { .. }) => ("failed", None, None, None, false),
        None if waived => ("waived", None, None, None, false),
        None if done || proof_failed => ("skipped", None, None, None, false),
        None => ("owed", None, None, None, false),
    };
    serde_json::json!({
        "role": config.role.as_str(),
        "verdict": verdict,
        "judged_sha": judged_sha,
        "fresh": fresh,
        "gaps": counts,
        "acknowledged": acknowledged,
        "waived": waived,
        "attempts": tr.attempts,
        "failure": match &tr.outcome {
            Some(ReviewOutcome::Failed { failure }) => serde_json::to_value(failure).ok(),
            _ => None,
        },
    })
}

/// The one-line human rendering of `review_summary`; `None` when the mission
/// declares no review.
fn review_line(state: &crate::model::MissionState) -> Option<String> {
    let summary = review_summary(state);
    if summary.is_null() {
        return None;
    }
    let sha = summary["judged_sha"]
        .as_str()
        .map(short_hex)
        .unwrap_or_default();
    let stale = if summary["fresh"] == serde_json::json!(false) {
        " [STALE — not at the final commit]"
    } else {
        ""
    };
    let blocking = summary["gaps"]["blocking"].as_u64().unwrap_or(0);
    let major = summary["gaps"]["major"].as_u64().unwrap_or(0);
    let minor = summary["gaps"]["minor"].as_u64().unwrap_or(0);
    let acknowledged = if summary["acknowledged"].as_bool().unwrap_or(false) {
        " — acknowledged by a human"
    } else {
        ""
    };
    Some(match summary["verdict"].as_str().unwrap_or("owed") {
        "clean" => format!("review: clean (judged {sha}){stale}"),
        // A "gaps" verdict without a single blocking gap means the park came
        // from the reviewer's fail bit — say so, never "0 blocking gap(s)"
        // on a parked mission.
        "gaps" if blocking == 0 => format!(
            "review: FAILED the product ({} gap(s) recorded) — see its report \
             (judged {sha}){stale}{acknowledged}",
            major + minor,
        ),
        "gaps" => format!(
            "review: {blocking} blocking, {major} major, {minor} minor gap(s) \
             (judged {sha}){stale}{acknowledged}",
        ),
        "failed" if state.phase.is_terminal() => {
            "review: FAILED to run before the mission ended".to_string()
        }
        "failed" => "review: FAILED to run — retry, accept, or abort".to_string(),
        "waived" => "review: WAIVED after a failure — no verdict was recorded".to_string(),
        "skipped" if matches!(state.phase, MissionPhase::Aborted { .. }) => {
            "review: none — the mission was aborted before a review settled".to_string()
        }
        "skipped" => {
            "review: skipped — the finish is below the stop bar; no review is owed".to_string()
        }
        _ => "review: owed — not yet judged at the final commit".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        OracleRunSuccess, PayloadRef, RuntimeConfigurationEvidence, TerminalReviewSuccess,
    };
    use lionclaw_runtime_api::{TypedFailure, TypedFailureEvidence};
    use std::collections::{BTreeMap, BTreeSet};

    #[cfg(unix)]
    #[test]
    fn detached_driver_uses_a_process_group_isolated_from_the_invoker() {
        fn process_group(pid: u32) -> String {
            String::from_utf8(
                std::process::Command::new("ps")
                    .args(["-o", "pgid=", "-p", &pid.to_string()])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap()
            .trim()
            .to_string()
        }

        let parent_group = process_group(std::process::id());
        let mut child = std::process::Command::new("sh");
        child.args(["-c", "ps -o pgid= -p $$"]);
        isolate_driver_process_group(&mut child);
        let child_group = String::from_utf8(child.output().unwrap().stdout)
            .unwrap()
            .trim()
            .to_string();
        assert_ne!(child_group, parent_group);
    }

    #[test]
    fn cli_has_no_plan_approval_bypass_and_exposes_only_explicit_decision_inputs() {
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            "software-dev",
            "--objective",
            "fix it",
            "--yes",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            "mabc123def456",
            "plan_proposal:mission",
            "approve",
            "--justification",
            "reviewed the proposed contract",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            "mabc123def456",
            "plan_proposal:mission",
            "revise",
            "--feedback-file",
            "feedback.md",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            "mabc123def456",
            "plan_proposal:mission",
            "revise",
            "--feedback-stdin",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            "mabc123def456",
            "plan_proposal:mission",
            "revise",
            "--feedback-file",
            "feedback.md",
            "--feedback-stdin",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            "mabc123def456",
            "plan_proposal:mission",
            "approve",
            "--justification",
            "reviewed the proposed contract",
            "--actor",
            "caller-supplied",
        ])
        .is_err());
        for removed in ["--actor", "--justification"] {
            assert!(Cli::try_parse_from([
                "lionclaw",
                "mission",
                "plan",
                "propose",
                "mabc123def456",
                "--file",
                "proposal.json",
                removed,
                "caller-supplied",
            ])
            .is_err());
        }
    }

    fn decision_args(action: &str) -> DecideArgs {
        DecideArgs {
            mission_id: "mabc123def456".to_string(),
            item: "plan_proposal:mission".to_string(),
            action: action.to_string(),
            repo: None,
            justification: None,
            feedback_file: None,
            feedback_stdin: false,
        }
    }

    #[test]
    fn decision_inputs_are_action_specific() {
        let mut approve = decision_args("approve");
        assert!(decision_text(&approve, &DecisionAction::Approve)
            .unwrap_err()
            .to_string()
            .contains("require --justification"));
        approve.justification = Some(" \n\t".to_string());
        assert!(decision_text(&approve, &DecisionAction::Approve)
            .unwrap_err()
            .to_string()
            .contains("non-empty"));
        approve.justification = Some("contract checked".to_string());
        assert_eq!(
            decision_text(&approve, &DecisionAction::Approve).unwrap(),
            "contract checked"
        );

        let revise = decision_args("revise");
        assert!(decision_text(&revise, &DecisionAction::Revise)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
        let mut revise_with_justification = decision_args("revise");
        revise_with_justification.justification = Some("inline".to_string());
        assert!(
            decision_text(&revise_with_justification, &DecisionAction::Revise)
                .unwrap_err()
                .to_string()
                .contains("does not accept --justification")
        );
        let mut retry = decision_args("retry");
        retry.feedback_file = Some(PathBuf::from("feedback.md"));
        assert!(decision_text(&retry, &DecisionAction::Retry)
            .unwrap_err()
            .to_string()
            .contains("only valid with revise"));
    }

    #[test]
    fn revise_feedback_file_preserves_large_utf8_input_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feedback.md");
        let feedback = format!("  first\0line\n{}\t", "x".repeat(140 * 1024));
        std::fs::write(&path, feedback.as_bytes()).unwrap();
        let mut args = decision_args("revise");
        args.feedback_file = Some(path);

        assert_eq!(
            decision_text(&args, &DecisionAction::Revise).unwrap(),
            feedback
        );
    }

    #[test]
    fn revise_feedback_rejects_empty_or_invalid_utf8_input() {
        assert!(decode_feedback(Vec::new(), "stdin")
            .unwrap_err()
            .to_string()
            .contains("is empty"));
        assert!(decode_feedback(vec![0xff], "stdin")
            .unwrap_err()
            .to_string()
            .contains("is not UTF-8"));
        assert_eq!(
            decode_feedback(b" \n\t".to_vec(), "stdin").unwrap(),
            " \n\t"
        );
    }

    fn mission_type_with_runtime(runtime: Option<&str>) -> MissionType {
        use crate::mission_type::{MissionTypeDefinition, RoleDefinition};
        use crate::model::{OutputSemantics, RoleName, StopBar};

        let name = RoleName::new("worker").expect("role name");
        MissionType::for_testing(MissionTypeDefinition {
            name: "runtime-test".to_string(),
            stop: StopBar::Verified,
            image: "image".to_string(),
            planning: Default::default(),
            recovery: Default::default(),
            execution: Default::default(),
            terminal_review: None,
            playbook: None,
            roles: BTreeMap::from([(
                name.clone(),
                RoleDefinition {
                    name,
                    output: OutputSemantics::ProducesArtifact,
                    runtime: runtime.map(str::to_string),
                    timeout_secs: None,
                    network: true,
                    secrets: false,
                    skills: Vec::new(),
                    prompt_body: "work".to_string(),
                },
            )]),
            skills: BTreeMap::new(),
            inputs: BTreeMap::new(),
            oracles: BTreeMap::new(),
        })
    }

    fn mid() -> MissionId {
        MissionId::parse("mabc123def456").unwrap()
    }

    #[test]
    fn mission_send_cli_has_an_unambiguous_message_and_recipient_shape() {
        let parsed = Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            "mabc123def456",
            "--to",
            "task-a",
            "--to",
            "task-b",
            "lead feedback",
        ])
        .expect("valid send command");
        let Command::Mission(MissionCommand::Send(args)) = parsed.command else {
            panic!("expected send command");
        };
        assert_eq!(args.mission_id.as_deref(), Some("mabc123def456"));
        assert_eq!(args.to, ["task-a", "task-b"]);
        assert_eq!(args.message, "lead feedback");
        assert!(Cli::try_parse_from([
            "lionclaw", "mission", "send", "--all", "--to", "task-a", "feedback",
        ])
        .is_err());
    }

    #[test]
    fn apply_refuses_a_mission_that_produced_no_commit() {
        // current == base ⇒ nothing to apply (never an empty branch).
        assert!(apply_target(&mid(), "base", "base").is_err());
        // a produced commit ⇒ the `lionclaw/<id>` branch target.
        assert_eq!(
            apply_target(&mid(), "base", "head").unwrap(),
            "lionclaw/mabc123def456"
        );
    }

    #[test]
    fn start_image_override_replaces_the_mission_type_default() {
        assert_eq!(start_image_ref("type-image", None), "type-image");
        assert_eq!(
            start_image_ref("type-image", Some("override-image")),
            "override-image"
        );
    }

    #[test]
    fn mission_runtime_validation_rejects_unknown_role_profiles() {
        let profiles = RuntimeProfiles::from_toml(
            "[runtimes.default]\ndriver = \"acp\"\ncommand = \"agent\"\n",
            Path::new("/home/alice"),
        )
        .expect("profiles");
        let err = validate_mission_runtimes(
            &mission_type_with_runtime(Some("missing")),
            "default",
            &profiles,
        )
        .expect_err("unknown role runtime");
        assert!(
            err.to_string()
                .contains("role 'worker' resolves to unavailable runtime 'missing'"),
            "got {err:#}"
        );
    }

    #[test]
    fn mission_runtime_validation_requires_one_oci_engine() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.default]
            driver = "acp"
            command = "agent"

            [runtimes.other]
            driver = "acp"
            command = "other-agent"
            confinement = { backend = "podman", engine = "other-podman" }
            "#,
            Path::new("/home/alice"),
        )
        .expect("profiles");
        let err = validate_mission_runtimes(
            &mission_type_with_runtime(Some("other")),
            "default",
            &profiles,
        )
        .expect_err("mixed OCI engines");
        assert!(err
            .to_string()
            .contains("one mission requires one OCI engine"));
    }

    #[test]
    fn mission_runtime_validation_checks_driver_and_auth_compatibility() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.bad]
            driver = "codex"
            command = "codex"
            auth = { kind = "native-home", source = "~/.agent", target = ".agent", required-files = ["auth.json"] }
            "#,
            Path::new("/home/alice"),
        )
        .expect("profiles");
        let err = validate_mission_runtimes(&mission_type_with_runtime(None), "bad", &profiles)
            .expect_err("incompatible auth");
        assert!(
            err.to_string().contains("default runtime 'bad' is invalid"),
            "got {err:#}"
        );
    }

    /// Fold a hand-built review mission to a state, for summary rendering
    /// tests (sequence numbers assigned by position).
    const REVIEW_PROMPT_HASH: &str =
        "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

    fn review_mission_id() -> crate::model::MissionId {
        crate::model::MissionId::parse("mabc123def456").unwrap()
    }

    fn review_role_effect() -> crate::model::EffectId {
        crate::model::EffectId::for_role_request(
            crate::model::TaskNamespace::Execution,
            &review_mission_id(),
            &crate::model::TaskId::new("fix").unwrap(),
            1,
            1,
            REVIEW_PROMPT_HASH,
        )
    }

    fn review_oracle_effect() -> crate::model::EffectId {
        crate::model::EffectId::for_oracle_request(
            &review_mission_id(),
            &crate::model::OracleName::new("cargo-test").unwrap(),
            "h1",
            1,
        )
    }

    fn terminal_review_effect() -> crate::model::EffectId {
        crate::model::EffectId::for_terminal_review_request(&review_mission_id(), "h1", 1)
    }

    fn review_state(tail: Vec<crate::model::MissionEvent>) -> crate::model::MissionState {
        use crate::model::*;
        let mut events = vec![
            MissionEvent::MissionCreated {
                objective: "obj".into(),
                mission_type: MissionTypeRef {
                    name: "t".into(),
                    digest: "d".into(),
                },
                runtime: "codex".into(),
                image_id: "img".into(),
                workspace_dir: "/w".into(),
                base_sha: "base".into(),
                config: MissionConfig {
                    plan_inventory: PlanInventory {
                        roles: BTreeMap::from([
                            (
                                RoleName::new("implementer").unwrap(),
                                OutputSemantics::ProducesArtifact,
                            ),
                            (
                                RoleName::new("gap-reviewer").unwrap(),
                                OutputSemantics::EmitsGapVerdict,
                            ),
                        ]),
                        oracles: BTreeSet::from([OracleName::new("cargo-test").unwrap()]),
                    },
                    recovery: RecoveryConfig { max_attempts: 1 },
                    execution: Default::default(),
                    terminal_review: Some(TerminalReviewConfig {
                        role: RoleName::new("gap-reviewer").unwrap(),
                    }),
                    ..Default::default()
                },
            },
            MissionEvent::PlanProposed {
                proposal: PlanProposal {
                    base_revision: 0,
                    plan: Plan {
                        requirements: vec![Requirement {
                            id: RequirementId::new("REQ-1").unwrap(),
                            kind: RequirementKind::Capability,
                            prose: "tests pass".into(),
                            disposition: RequirementDisposition::Covered {
                                assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
                            },
                        }],
                        assertions: vec![Assertion {
                            id: AssertionId::new("TESTS-PASS").unwrap(),
                            prose: "tests pass".into(),
                            oracle: Some(OracleName::new("cargo-test").unwrap()),
                        }],
                        tasks: vec![Task {
                            id: TaskId::new("fix").unwrap(),
                            kind: TaskKind::Work,
                            body: "fix".into(),
                            targets: vec![AssertionId::new("TESTS-PASS").unwrap()],
                            role: Some(RoleName::new("implementer").unwrap()),
                            depends_on: vec![],
                        }],
                    },
                },
                plan_hash: "h".into(),
            },
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "test fixture approves the plan".into(),
            },
            MissionEvent::RoleRunRequested {
                conversation_id: crate::model::ConversationId::for_role_instance(
                    &review_mission_id(),
                    TaskNamespace::Execution,
                    &TaskId::new("fix").unwrap(),
                    &RoleName::new("implementer").unwrap(),
                    1,
                ),
                namespace: TaskNamespace::Execution,
                task_id: TaskId::new("fix").unwrap(),
                attempt_no: 1,
                effect_id: review_role_effect(),
                role: RoleName::new("implementer").unwrap(),
                output: OutputSemantics::ProducesArtifact,
                runtime: "codex".into(),
                prompt: PayloadRef::inline("prompt"),
                base_sha: "base".into(),
                assignment_epoch: 1,
                message_boundary: 3,
                presented_messages: vec![],
                recreate_workspace: true,
                requested_at_ms: 0,
                not_before_ms: 0,
                deadline_ms: 100_000,
                budget_deadline_ms: 100_000,
            },
            MissionEvent::RoleRunCompleted {
                effect_id: review_role_effect(),
                request: Box::new(crate::model::RoleRunRequestIdentity {
                    conversation_id: crate::model::ConversationId::for_role_instance(
                        &review_mission_id(),
                        TaskNamespace::Execution,
                        &TaskId::new("fix").unwrap(),
                        &RoleName::new("implementer").unwrap(),
                        1,
                    ),
                    namespace: TaskNamespace::Execution,
                    task_id: TaskId::new("fix").unwrap(),
                    attempt_no: 1,
                    assignment_epoch: 1,
                    role: RoleName::new("implementer").unwrap(),
                    output: OutputSemantics::ProducesArtifact,
                    runtime: "codex".into(),
                    prompt_hash: REVIEW_PROMPT_HASH.into(),
                    prompt: PayloadRef::inline("prompt"),
                    base_sha: "base".into(),
                    recreate_workspace: true,
                    message_boundary: 3,
                    presented_messages: vec![],
                }),
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report: PayloadRef::inline("done"),
                        request_attention: false,
                    }),
                    artifact: Some(ArtifactOutcome {
                        base_sha: "base".into(),
                        head_sha: "h1".into(),
                    }),
                    final_response: PayloadRef::inline("done"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ];
        for event in tail {
            match &event {
                MissionEvent::OracleRunCompleted {
                    assertion_ids,
                    oracle,
                    judged_sha,
                    attempt_no,
                    effect_id,
                    ..
                } => events.push(MissionEvent::OracleRunRequested {
                    assertion_ids: assertion_ids.clone(),
                    oracle: oracle.clone(),
                    judged_sha: judged_sha.clone(),
                    attempt_no: *attempt_no,
                    effect_id: effect_id.clone(),
                    requested_at_ms: 0,
                    not_before_ms: 0,
                    deadline_ms: 100_000,
                }),
                MissionEvent::TerminalReviewCompleted {
                    attempt_no,
                    effect_id,
                    judged_sha,
                    ..
                } => events.push(MissionEvent::TerminalReviewRequested {
                    attempt_no: *attempt_no,
                    effect_id: effect_id.clone(),
                    role: RoleName::new("gap-reviewer").unwrap(),
                    runtime: "codex".into(),
                    prompt: PayloadRef::inline("review prompt"),
                    judged_sha: judged_sha.clone(),
                    nonce: "test-nonce".into(),
                    requested_at_ms: 0,
                    not_before_ms: 0,
                    deadline_ms: 100_000,
                    budget_deadline_ms: 100_000,
                }),
                _ => {}
            }
            events.push(event);
        }
        fold(events.into_iter().enumerate().map(|(i, event)| {
            let mut stamps = VersionStamps::default();
            if matches!(&event, MissionEvent::RoleRunRequested { .. }) {
                stamps.prompt_hash = Some(REVIEW_PROMPT_HASH.into());
            }
            EventEnvelope {
                mission_id: review_mission_id(),
                sequence_no: i as u64 + 1,
                recorded_at_ms: 0,
                stamps,
                event,
            }
        }))
        .expect("state")
    }

    fn oracle_completed(exit_code: i32) -> crate::model::MissionEvent {
        use crate::model::*;
        MissionEvent::OracleRunCompleted {
            assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
            oracle: OracleName::new("cargo-test").unwrap(),
            judged_sha: "h1".into(),
            attempt_no: 1,
            effect_id: review_oracle_effect(),
            outcome: Ok(OracleRunSuccess {
                exit_code,
                exit_signal: None,
                stdout: PayloadRef::inline(""),
                stderr: PayloadRef::inline(""),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }),
        }
    }

    #[test]
    fn a_below_bar_close_reports_the_review_as_skipped_not_owed() {
        // A fresh oracle FAIL parks below the bar for repair, while the closing
        // and the engine deliberately never dispatches the reviewer — the
        // review remains deliberately skipped rather than "owed".
        let state = review_state(vec![oracle_completed(1)]);
        assert!(matches!(state.phase, MissionPhase::AttentionNeeded));
        let summary = review_summary(&state);
        assert_eq!(summary["verdict"], serde_json::json!("skipped"));
        let line = review_line(&state).expect("line");
        assert!(line.contains("skipped"), "got: {line}");
        assert!(!line.contains("not yet judged"), "got: {line}");
    }

    #[test]
    fn attention_json_keeps_labelled_stderr_failure_evidence() {
        use crate::model::{AssertionId, MissionEvent, OracleName, PayloadRef};

        let state = review_state(vec![MissionEvent::OracleRunCompleted {
            assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
            oracle: OracleName::new("cargo-test").unwrap(),
            judged_sha: "h1".into(),
            attempt_no: 1,
            effect_id: review_oracle_effect(),
            outcome: Ok(OracleRunSuccess {
                exit_code: 1,
                exit_signal: None,
                stdout: PayloadRef::inline("ordinary output"),
                stderr: PayloadRef::inline("the actual diagnostic"),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }),
        }]);
        let item = state
            .open_attention
            .get("oracle_verdict_failed:cargo-test")
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let blobs = BlobStore::new(temp.path().join("blobs"));
        let json = attention_json(&blobs, item).unwrap();

        assert_eq!(json["evidence"]["stdout"], "ordinary output");
        assert_eq!(json["evidence"]["stderr"], "the actual diagnostic");
        assert_eq!(json["actions"][1], "repair");
    }

    #[tokio::test]
    async fn mission_view_json_carries_one_disposition_and_action_projection() {
        use crate::model::{
            PayloadRef, RuntimeConfigurationEvidence, TaskId, TaskRuntimeState, TaskStatus,
        };

        let mut state = review_state(vec![oracle_completed(1)]);
        state.planning.tasks.insert(
            TaskId::new("planner").unwrap(),
            TaskRuntimeState {
                status: TaskStatus::Failed,
                attempts: 1,
                consecutive_failures: 1,
                last_report: None,
                last_failure: None,
                feedback: Vec::new(),
                last_runtime_configuration: Some(RuntimeConfigurationEvidence {
                    requested_model: Some("requested".into()),
                    applied_model: Some("applied".into()),
                    model_confirmation: Some(
                        lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
                    ),
                    requested_mode: Some("plan".into()),
                    applied_mode: Some("plan".into()),
                    mode_confirmation: Some(
                        lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
                    ),
                }),
                workspace_base_sha: Some("base".into()),
                assignment_epoch: 1,
                final_response: Some(PayloadRef::inline("planning stopped here")),
            },
        );
        state.tasks.insert(
            TaskId::new("retained").unwrap(),
            TaskRuntimeState {
                status: TaskStatus::Failed,
                attempts: 1,
                consecutive_failures: 1,
                last_report: None,
                last_failure: None,
                feedback: Vec::new(),
                last_runtime_configuration: None,
                workspace_base_sha: Some("base".into()),
                assignment_epoch: 1,
                final_response: None,
            },
        );
        state.tasks.insert(
            TaskId::new("unobservable").unwrap(),
            TaskRuntimeState {
                status: TaskStatus::Failed,
                attempts: 1,
                consecutive_failures: 1,
                last_report: None,
                last_failure: None,
                feedback: Vec::new(),
                last_runtime_configuration: None,
                workspace_base_sha: Some("base".into()),
                assignment_epoch: 1,
                final_response: None,
            },
        );
        let conversation_id = crate::model::ConversationId::for_role_instance(
            &state.mission_id,
            crate::model::TaskNamespace::Execution,
            &TaskId::new("retained").unwrap(),
            &crate::model::RoleName::new("implementer").unwrap(),
            1,
        );
        state.conversations.insert(
            conversation_id.clone(),
            crate::model::ConversationState {
                role: crate::model::RoleName::new("implementer").unwrap(),
                namespace: crate::model::TaskNamespace::Execution,
                task_id: TaskId::new("retained").unwrap(),
                assignment_epoch: 1,
                workspace_base_sha: "base".into(),
                lifecycle: crate::model::ConversationLifecycle::AwaitingLead,
                queued: vec![crate::model::QueuedMessage {
                    sequence_no: 7,
                    body: "lead context".into(),
                    references: vec![crate::model::MessageReference::ReachableCommit {
                        sha: "base".into(),
                    }],
                    marker: crate::model::DeliveryMarker::PossiblyDelivered,
                }],
                consumed_through: 3,
                active_delivery: None,
                invalid_handoff_reworks: 1,
            },
        );
        let mission_id = state.mission_id.clone();
        let mut view = MissionView {
            state,
            disposition: MissionDisposition::Parked,
        };
        let temp = tempfile::tempdir().unwrap();
        let store = MissionStore::open(temp.path()).await.unwrap();
        for args in [
            &["init", "-q"][..],
            &["config", "user.name", "test"][..],
            &["config", "user.email", "test@local"][..],
            &["config", "commit.gpgsign", "false"][..],
        ] {
            assert!(std::process::Command::new("git")
                .args(args)
                .current_dir(temp.path())
                .status()
                .unwrap()
                .success());
        }
        std::fs::write(temp.path().join("base.txt"), "base\n").unwrap();
        assert!(std::process::Command::new("git")
            .args(["add", "base.txt"])
            .current_dir(temp.path())
            .status()
            .unwrap()
            .success());
        assert!(std::process::Command::new("git")
            .args(["commit", "-q", "-m", "base"])
            .current_dir(temp.path())
            .status()
            .unwrap()
            .success());
        let base = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(temp.path())
            .output()
            .unwrap();
        let base = String::from_utf8(base.stdout).unwrap().trim().to_string();
        let retained = store
            .lionclaw_dir()
            .join("missions")
            .join(mission_id.as_str())
            .join("tasks/retained/work");
        crate::workspace::create_checkout(temp.path(), &retained, &base)
            .await
            .unwrap();
        let observer_index = retained.parent().unwrap().join("observer.index");
        crate::workspace::prepare_task_observer_index(temp.path(), &observer_index, &base, true)
            .await
            .unwrap();
        view.state
            .tasks
            .get_mut(&TaskId::new("retained").unwrap())
            .unwrap()
            .workspace_base_sha = Some(base.clone());
        view.state
            .tasks
            .get_mut(&TaskId::new("unobservable").unwrap())
            .unwrap()
            .workspace_base_sha = Some(base);
        std::fs::write(retained.join("partial.txt"), "preserved\n").unwrap();
        let unobservable = store
            .lionclaw_dir()
            .join("missions")
            .join(mission_id.as_str())
            .join("tasks/unobservable/work");
        std::fs::create_dir_all(&unobservable).unwrap();
        std::fs::write(unobservable.join("partial.txt"), "unknown\n").unwrap();
        std::fs::write(
            crate::activity::path(&store.mission_dir(&mission_id)),
            b"stale",
        )
        .unwrap();
        assert!(running_activity_bytes(&store, &mission_id, MissionDisposition::Parked).is_none());
        assert_eq!(
            running_activity_bytes(&store, &mission_id, MissionDisposition::Running).as_deref(),
            Some(b"stale".as_slice())
        );
        std::fs::remove_file(crate::activity::path(&store.mission_dir(&mission_id))).unwrap();
        assert_eq!(
            running_activity_bytes(&store, &mission_id, MissionDisposition::Running),
            Some(Vec::new()),
            "a running watch waits through the pre-projection startup window"
        );
        let json = mission_view_json(&view, &store).await.unwrap();

        assert_eq!(json["phase"], "attention_needed");
        assert_eq!(json["disposition"], "parked");
        assert_eq!(json["next_actions"], serde_json::json!(["mission decide"]));
        assert_eq!(json["conversations"][0]["id"], conversation_id.as_str());
        assert_eq!(json["conversations"][0]["lifecycle"], "awaiting_lead");
        assert_eq!(
            json["conversations"][0]["queued_messages"][0]["marker"],
            "possibly_delivered"
        );
        assert_eq!(
            json["conversations"][0]["runtime_resume_mode"],
            "canonical_reconstruction"
        );
        assert_eq!(json["planning_input"], serde_json::Value::Null);
        assert_eq!(json["cleanup_failure"], serde_json::Value::Null);
        assert_eq!(json["attention"][0]["kind"], "oracle_verdict_failed");
        assert_eq!(json["planning_tasks"][0]["id"], "planner");
        assert_eq!(
            json["planning_tasks"][0]["workspace_observation"],
            serde_json::json!({"status": "not_applicable"})
        );
        let retained = json["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["id"] == "retained")
            .unwrap();
        assert!(retained["workspace_observation"]["diffstat"]
            .as_str()
            .unwrap()
            .contains("partial.txt"));
        assert_eq!(retained["workspace_observation"]["status"], "changed");
        let unobservable = json["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["id"] == "unobservable")
            .unwrap();
        assert_eq!(
            unobservable["workspace_observation"]["status"],
            "unavailable"
        );
        assert!(unobservable["workspace_observation"]["reason"]
            .as_str()
            .unwrap()
            .contains("observer index"));
        assert_eq!(
            json["planning_tasks"][0]["runtime_configuration"]["applied_model"],
            "applied"
        );
        assert_eq!(
            json["planning_tasks"][0]["final_response"],
            "planning stopped here"
        );
    }

    #[tokio::test]
    async fn successful_driver_exit_before_handshake_is_a_benign_ownership_race() {
        let temp = tempfile::tempdir().unwrap();
        let handshake = temp.path().join("never-published.ready");
        let process = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let stderr_spool = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let mut child = DetachedDriver {
            process,
            stderr_spool: Some(stderr_spool),
            cleanup_on_drop: true,
        };

        assert_eq!(
            await_driver_startup(&mut child, &handshake, temp.path())
                .await
                .unwrap(),
            DriverStartup::LostRace
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn startup_timeout_terminates_and_reaps_driver_and_spool() {
        let temp = tempfile::tempdir().unwrap();
        let handshake = temp.path().join("never-published.ready");
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 60"]);
        isolate_driver_process_group(&mut command);
        let process = command.spawn().unwrap();
        let stderr_spool = std::process::Command::new("sh")
            .args(["-c", "sleep 60"])
            .spawn()
            .unwrap();
        let mut child = DetachedDriver {
            process,
            stderr_spool: Some(stderr_spool),
            cleanup_on_drop: true,
        };

        let error = await_driver_startup_with_timeout(
            &mut child,
            &handshake,
            temp.path(),
            Duration::from_millis(20),
        )
        .await
        .expect_err("a missing startup handshake has one bounded failure path");

        assert!(error.to_string().contains("handshake timed out"));
        assert!(child.process.try_wait().unwrap().is_some());
        assert!(child
            .stderr_spool
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spool_spawn_failure_terminates_driver_process_group() {
        let temp = tempfile::tempdir().unwrap();
        let driver_pid_path = temp.path().join("driver.pid");
        let descendant_pid_path = temp.path().join("descendant.pid");
        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg(
                "echo $$ > \"$DRIVER_PID_PATH\"; \
                 sleep 60 & echo $! > \"$DESCENDANT_PID_PATH\"; wait",
            )
            .env("DRIVER_PID_PATH", &driver_pid_path)
            .env("DESCENDANT_PID_PATH", &descendant_pid_path);
        isolate_driver_process_group(&mut command);

        let mut published_pids = None;
        let error = spawn_detached_driver_with(&mut command, temp.path(), |_| {
            for _ in 0..200 {
                let driver_pid = std::fs::read_to_string(&driver_pid_path)
                    .ok()
                    .and_then(|pid| pid.trim().parse::<i32>().ok());
                let descendant_pid = std::fs::read_to_string(&descendant_pid_path)
                    .ok()
                    .and_then(|pid| pid.trim().parse::<i32>().ok());
                if let (Some(driver_pid), Some(descendant_pid)) = (driver_pid, descendant_pid) {
                    published_pids = Some((driver_pid, descendant_pid));
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            if published_pids.is_none() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "driver did not publish process ids",
                ));
            }
            Err(std::io::Error::other("injected spool spawn failure"))
        })
        .err()
        .expect("the injected spool failure must fail construction");
        assert!(error
            .to_string()
            .contains("spawning bounded driver stderr spool"));
        let (driver_pid, descendant_pid) =
            published_pids.expect("driver did not publish process ids");

        assert!(!Path::new(&format!("/proc/{driver_pid}")).exists());
        let descendant_path = PathBuf::from(format!("/proc/{descendant_pid}"));
        for _ in 0..50 {
            if !descendant_path.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let survived = descendant_path.exists();
        if survived {
            if let Some(pid) = rustix::process::Pid::from_raw(descendant_pid) {
                let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
            }
        }
        assert!(!survived, "driver descendant survived constructor cleanup");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dropping_an_unresolved_startup_reaps_both_children() {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "sleep 60"]);
        isolate_driver_process_group(&mut command);
        let process = command.spawn().unwrap();
        let stderr_spool = std::process::Command::new("sh")
            .args(["-c", "sleep 60"])
            .spawn()
            .unwrap();
        let driver_pid = process.id();
        let spool_pid = stderr_spool.id();

        drop(DetachedDriver {
            process,
            stderr_spool: Some(stderr_spool),
            cleanup_on_drop: true,
        });

        assert!(!Path::new(&format!("/proc/{driver_pid}")).exists());
        assert!(!Path::new(&format!("/proc/{spool_pid}")).exists());
    }

    #[tokio::test]
    async fn lost_startup_race_waits_for_the_winning_driver_lock() {
        let temp = tempfile::tempdir().unwrap();
        let store = MissionStore::open(temp.path()).await.unwrap();
        let mission_id = MissionId::from_digest_prefix("1234567890abcdef");
        let winner =
            crate::driver_lock::DriverGuard::acquire(&store.driver_lock_path(&mission_id)).unwrap();
        let waiter = tokio::spawn({
            let store = store.clone();
            let mission_id = mission_id.clone();
            async move { wait_for_existing_driver(&store, &mission_id).await }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "--wait must remain with the winning driver"
        );
        drop(winner);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter observes driver release")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn mission_view_json_projects_complete_manual_replanning_input() {
        use crate::model::{FailureEvidence, FailureFeedback, PlanningRefinement};

        let mut state = review_state(vec![]);
        state.planning_base_revision = Some(1);
        state.planning_input.latest_rejected_proposal = Some(crate::model::PlanProposal {
            base_revision: 1,
            plan: state.plan.clone().expect("accepted plan"),
        });
        state.planning_input.refinement = Some(PlanningRefinement::Guidance(
            "  preserve this exactly\n\t".to_string(),
        ));
        let view = MissionView {
            state,
            disposition: MissionDisposition::AwaitingPlan,
        };
        let temp = tempfile::tempdir().unwrap();
        let store = MissionStore::open(temp.path()).await.unwrap();
        let json = mission_view_json(&view, &store).await.unwrap();

        assert_eq!(json["planning_input"]["base_revision"], 1);
        assert_eq!(
            json["planning_input"]["latest_rejected_proposal"]["base_revision"],
            1
        );
        assert_eq!(json["planning_input"]["refinement"]["kind"], "guidance");
        assert_eq!(
            json["planning_input"]["refinement"]["text"],
            "  preserve this exactly\n\t"
        );

        let mut state = view.state;
        state.planning_input.refinement = Some(PlanningRefinement::FailureEvidence(Box::new(
            FailureFeedback {
                summary: "oracle failed".to_string(),
                evidence: Some(FailureEvidence {
                    exit_code: 1,
                    exit_signal: None,
                    stdout: crate::model::PayloadRef::inline("ordinary output"),
                    stderr: crate::model::PayloadRef::inline("actual diagnostic"),
                }),
                details: Some(crate::model::PayloadRef::inline("review detail")),
                justification: "repair this".to_string(),
            },
        )));
        let json = planning_input_json(&state, store.blobs()).unwrap();
        assert_eq!(json["refinement"]["kind"], "failure_evidence");
        assert_eq!(json["refinement"]["evidence"]["stdout"], "ordinary output");
        assert_eq!(
            json["refinement"]["evidence"]["stderr"],
            "actual diagnostic"
        );
        assert_eq!(json["refinement"]["details"], "review detail");
        assert_eq!(json["refinement"]["justification"], "repair this");
    }

    #[test]
    fn a_fail_bit_with_only_minor_gaps_renders_as_failed_not_zero_blocking() {
        // Regression (QA round 3): passed=false with only minor gaps parks
        // via blocking() dominance; the line must lead with the fail, never
        // "0 blocking, 0 major, 2 minor gap(s)" as if nothing blocked.
        use crate::model::{Gap, GapSeverity, MissionEvent};
        let minor = |req: &str| Gap {
            id: None,
            severity: GapSeverity::Minor,
            requirement: req.into(),
            expected: "e".into(),
            observed: "o".into(),
            evidence: "v".into(),
        };
        let state = review_state(vec![
            oracle_completed(0),
            MissionEvent::TerminalReviewCompleted {
                attempt_no: 1,
                effect_id: terminal_review_effect(),
                judged_sha: "h1".into(),
                outcome: Ok(TerminalReviewSuccess {
                    passed: false,
                    gaps: vec![minor("a"), minor("b")],
                    report: crate::model::PayloadRef::inline("failed overall"),
                    final_response: PayloadRef::inline("reviewed"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ]);
        assert!(matches!(state.phase, MissionPhase::AttentionNeeded));
        let line = review_line(&state).expect("line");
        assert!(line.contains("FAILED the product"), "got: {line}");
        assert!(!line.contains("0 blocking"), "got: {line}");
    }

    #[test]
    fn an_aborted_mission_never_advises_impossible_decisions() {
        // Regression (QA round 3): a dead mission must not print "owed" or a
        // retry/waive/abort menu no decision can act on.
        use crate::model::MissionEvent;
        // Aborted while parked on a review failure.
        let state = review_state(vec![
            oracle_completed(0),
            MissionEvent::TerminalReviewCompleted {
                attempt_no: 1,
                effect_id: terminal_review_effect(),
                judged_sha: "h1".into(),
                outcome: Err(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(TypedFailureEvidence::new(None, "boom")),
                }),
            },
            MissionEvent::DecisionRecorded {
                attention_id: "terminal_review_failed:mission".into(),
                action: crate::model::DecisionAction::Abort,
                justification: "give up".into(),
            },
        ]);
        assert!(matches!(state.phase, MissionPhase::Aborted { .. }));
        let line = review_line(&state).expect("line");
        assert!(line.contains("before the mission ended"), "got: {line}");
        assert!(!line.contains("retry"), "got: {line}");

        // Aborted before any review dispatch: "none", not "owed" forever.
        let state = review_state(vec![MissionEvent::MissionAborted {
            reason: "operator stop".into(),
        }]);
        let line = review_line(&state).expect("line");
        assert!(line.contains("the mission was aborted"), "got: {line}");
        assert!(!line.contains("not yet judged"), "got: {line}");
    }

    #[test]
    fn an_unstructured_fail_renders_as_failed_not_zero_gaps() {
        // Regression (QA round 2): passed=false with zero typed gaps parks
        // the mission; the line must say so, never "0 blocking, 0 major,
        // 0 minor gap(s)".
        use crate::model::MissionEvent;
        let state = review_state(vec![
            oracle_completed(0),
            MissionEvent::TerminalReviewCompleted {
                attempt_no: 1,
                effect_id: terminal_review_effect(),
                judged_sha: "h1".into(),
                outcome: Ok(TerminalReviewSuccess {
                    passed: false,
                    gaps: vec![],
                    report: crate::model::PayloadRef::inline("it does not work"),
                    final_response: PayloadRef::inline("reviewed"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ]);
        assert!(matches!(state.phase, MissionPhase::AttentionNeeded));
        let line = review_line(&state).expect("line");
        assert!(line.contains("FAILED the product"), "got: {line}");
        assert!(!line.contains("0 blocking"), "got: {line}");
    }

    #[test]
    fn start_text_points_to_planning_when_the_type_has_a_planning_dag() {
        let repo = Path::new("/tmp/repo");
        assert_eq!(
            start_next_step(3, &mid(), repo),
            "next: lionclaw mission advance mabc123def456 --repo /tmp/repo"
        );
        assert_eq!(
            start_next_step(0, &mid(), repo),
            "next: propose a plan with `lionclaw mission plan propose`, then run: lionclaw mission advance mabc123def456 --repo /tmp/repo"
        );
    }

    #[tokio::test]
    async fn mission_snapshot_is_the_only_resume_source_and_collisions_are_preserved() {
        let workspace = tempfile::tempdir().unwrap();
        let store = MissionStore::open(workspace.path()).await.unwrap();
        let source = workspace.path().join("source-type");
        std::fs::create_dir_all(source.join("roles")).unwrap();
        std::fs::write(
            source.join("mission.toml"),
            "[mission-type]\nname = \"snapshot-test\"\nstop = \"verified\"\nimage = \"img\"\n",
        )
        .unwrap();
        std::fs::write(
            source.join("roles/worker.md"),
            "---\noutput: produces-artifact\n---\nWork.\n",
        )
        .unwrap();
        std::fs::write(source.join("playbook.md"), "# Snapshot test\n").unwrap();
        let id = mid();
        create_mission_dir(&store, &id).unwrap();
        let snapshotted =
            snapshot_mission_type(&store, &id, &source, &AuthorityCeiling::default()).unwrap();

        std::fs::remove_dir_all(&source).unwrap();
        let loaded = load_mission_type_snapshot(&store, &id, &AuthorityCeiling::default()).unwrap();
        assert_eq!(loaded.digest(), snapshotted.digest());
        assert!(create_mission_dir(&store, &id).is_err());
        assert!(store.mission_type_dir(&id).is_dir());

        std::fs::remove_dir_all(store.mission_type_dir(&id)).unwrap();
        assert!(load_mission_type_snapshot(&store, &id, &AuthorityCeiling::default()).is_err());
    }
}
