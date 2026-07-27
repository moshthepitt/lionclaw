//! The mission CLI surface. Each command is a per-mission stdio run: open
//! the store, fold, act, park or exit. Host-as-orchestrator: the human's
//! agent session invokes these as tools and reads `--json` output.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Args, CommandFactory, Parser, Subcommand};

use crate::authority::AuthorityCeiling;
use crate::config::{MissionRuntimeProfile, RuntimeProfiles};
use crate::engine::{
    load_mission_view, reconcile_disposable_conversation_resources_if_idle, record_control, Engine,
    EngineServices, MissionDisposition, MissionView,
};
use crate::mission_type::{
    add_mission_skill, add_skill, install_mission_type, load_materialized_mission_type,
    load_mission_type, materialize_mission_type, remove_skill, BundledMissionTypes, Home,
    MissionType, MissionTypeLocator, SkillSource,
};
use crate::model::{
    fold, short_hex, AuthorityGrants, ControlAction, DecisionAction, EffectId,
    EnvironmentPreflight, FinishClass, InputName, MissionGuidance, MissionId, MissionPhase,
    MissionSkill, OutputSemantics, RequirementDisposition, RoleInstance, RoleInstanceId, TaskId,
};
use crate::oracle::OciOracleRunner;
use crate::ports::{Clock, OracleRunner, SystemClock};
use crate::runner::OciRoleRunner;
use crate::store::NewEvent;
use crate::store::{BlobStore, MissionStore};
use crate::workspace;
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeDriverRegistry};

/// External transports used by the canonical mission-command dispatcher.
///
/// The ordinary executable uses [`MissionTransports::production`]. Integration
/// tests may replace only the native runtime/auth registries and oracle
/// transport; engine, store, workspace, capture, cleanup, and fold behavior
/// remain the production implementations selected below.
#[derive(Clone)]
pub struct MissionTransports {
    profiles: Option<RuntimeProfiles>,
    runtime: Option<(RuntimeDriverRegistry, RuntimeAuthRegistry)>,
    oracle: Option<Arc<dyn OracleRunner>>,
}

impl MissionTransports {
    pub fn production() -> Self {
        Self {
            profiles: None,
            runtime: None,
            oracle: None,
        }
    }

    pub fn external(
        profiles: RuntimeProfiles,
        drivers: RuntimeDriverRegistry,
        auth: RuntimeAuthRegistry,
        oracle: Arc<dyn OracleRunner>,
    ) -> Self {
        Self {
            profiles: Some(profiles),
            runtime: Some((drivers, auth)),
            oracle: Some(oracle),
        }
    }

    fn profiles(&self) -> Result<RuntimeProfiles> {
        self.profiles.clone().map_or_else(runtime_profiles, Ok)
    }
}

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
    /// Print restart-safe lead guidance derived from folded state.
    Guide(GuideArgs),
    /// Inspect or assign the mission runtime environment.
    #[command(subcommand)]
    Environment(EnvironmentCommand),
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
    /// Inspect or revise the mission-owned team.
    #[command(subcommand)]
    Team(TeamCommand),
    /// Add mission-local skills without mutating the mission type.
    #[command(subcommand)]
    Skill(MissionSkillCommand),
    /// Resolve an open attention item.
    Decide(DecideArgs),
    /// Finish a mission whose proof bar is satisfied.
    Finish(FinishArgs),
    /// Abort any nonterminal mission while preserving its evidence.
    Abort(AbortArgs),
    /// Request cancellation of one exact active effect.
    Stop(ControlArgs),
    /// Extend one exact active effect's deadline.
    Extend(ExtendArgs),
    /// Resume one exact parked effect, optionally archiving and rebuilding its workspace.
    Continue(ContinueArgs),
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
pub enum EnvironmentCommand {
    /// Show the mission's active digest-pinned runtime environment.
    Show(EnvironmentShowArgs),
    /// Assign a preflighted digest-pinned image to subsequent effects.
    Use(EnvironmentUseArgs),
}

#[derive(Subcommand)]
pub enum TeamCommand {
    Show(TeamShowArgs),
    Add(TeamAddArgs),
    Reassign(TeamReassignArgs),
    Retire(TeamRoleArgs),
    SetRuntime(TeamSetRuntimeArgs),
    GuideSet(TeamGuideSetArgs),
    AssignSkill(TeamAssignSkillArgs),
}

#[derive(Subcommand)]
pub enum MissionSkillCommand {
    Add(MissionSkillAddArgs),
}

#[derive(Args)]
pub struct TeamShowArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TeamAddArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    pub role_instance: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub purpose: String,
    #[arg(long)]
    pub output: String,
    #[arg(long)]
    pub runtime: String,
    #[arg(long)]
    pub instructions_file: PathBuf,
    #[arg(long = "skill")]
    pub skills: Vec<String>,
    #[arg(long)]
    pub deadline_secs: Option<u64>,
    #[arg(long)]
    pub secrets: bool,
    #[arg(long)]
    pub network: bool,
    #[arg(long)]
    pub install: bool,
    #[arg(long)]
    pub writes: bool,
    #[arg(long = "device")]
    pub devices: Vec<String>,
    #[arg(long = "input")]
    pub inputs: Vec<String>,
    #[arg(long = "tmpfs")]
    pub tmpfs: Vec<String>,
}

#[derive(Args)]
pub struct TeamReassignArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    pub task: String,
    pub role_instance: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
pub struct TeamRoleArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    pub role_instance: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
pub struct TeamSetRuntimeArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    pub role_instance: String,
    pub runtime: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
pub struct TeamGuideSetArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub file: PathBuf,
}

#[derive(Args)]
pub struct TeamAssignSkillArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    pub role_instance: String,
    pub skill: String,
    #[arg(long)]
    pub repo: Option<PathBuf>,
}

#[derive(Args)]
#[command(group(ArgGroup::new("source").required(true).args(["path", "git"])))]
pub struct MissionSkillAddArgs {
    #[arg(long)]
    pub mission_id: Option<String>,
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long, conflicts_with = "git")]
    pub path: Option<PathBuf>,
    #[arg(long, conflicts_with = "path", requires = "rev")]
    pub git: Option<String>,
    #[arg(long, requires = "git")]
    pub rev: Option<String>,
    #[arg(long, requires = "git")]
    pub subdir: Option<PathBuf>,
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
    /// Current role-instance id or an unambiguous current task name. Repeatable.
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
    /// One of: approve | retry | repair | revise | accept.
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
pub struct AbortArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Why the mission is ending. Recorded verbatim in the event log.
    #[arg(long)]
    pub reason: String,
}

#[derive(Args)]
pub struct FinishArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Why the mission is complete. Recorded verbatim in the event log.
    #[arg(long)]
    pub reason: String,
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
    pub state_dir: PathBuf,
    #[arg(long)]
    pub mission_id: String,
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
pub struct ContinueArgs {
    #[command(flatten)]
    pub control: ControlArgs,
    /// Archive the exact retained writer checkout before rebuilding it.
    #[arg(long)]
    pub recreate: bool,
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
pub struct GuideArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct EnvironmentShowArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct EnvironmentUseArgs {
    /// Digest-pinned image ref: `sha256:<hex>` or `<name>@sha256:<hex>`.
    pub image: String,
    /// Mission id (default: the sole live mission in this repo).
    #[arg(long)]
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Why this mission should switch to this image.
    #[arg(long)]
    pub reason: String,
    #[arg(long)]
    pub json: bool,
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
    run_with_transports(cli, MissionTransports::production()).await
}

/// Run the same parsed-command dispatcher as the executable while replacing
/// only transports that cross the process/runtime boundary.
pub async fn run_with_transports(
    cli: Cli,
    transports: MissionTransports,
) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cli.command {
        Command::Install(args) => cmd_install(args).await.map(|()| ExitCode::SUCCESS),
        Command::Doctor => cmd_doctor().await,
        Command::Skill(cmd) => cmd_skill(cmd).await.map(|()| ExitCode::SUCCESS),
        Command::Mission(cmd) => run_mission(cmd, &transports).await,
        Command::Man => {
            clap_mangen::Man::new(Cli::command()).render(&mut std::io::stdout())?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

async fn run_mission(
    cmd: MissionCommand,
    transports: &MissionTransports,
) -> Result<std::process::ExitCode> {
    // One error policy for every command: a `--json` command reports failure as
    // a structured `{"ok":false,"error":…}` envelope on stdout and exits 1; a
    // human command lets the error bubble to stderr. Success output is each
    // command's own concern.
    let json = cmd.is_json();
    match dispatch_mission(cmd, transports).await {
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
            Self::Guide(a) => a.json,
            Self::Report(a) => a.json,
            Self::Plan(a) => a.is_json(),
            Self::Environment(a) => a.is_json(),
            Self::Team(TeamCommand::Show(a)) => a.json,
            Self::Inbox(a) => a.json,
            Self::Advance(a) => a.json,
            Self::Driver(_) | Self::DriverStderr(_) => false,
            Self::SelfTest(a) => a.json,
            Self::Type(t) => t.is_json(),
            Self::Apply(_)
            | Self::Log(_)
            | Self::Send(_)
            | Self::Team(_)
            | Self::Skill(_)
            | Self::Decide(_)
            | Self::Finish(_)
            | Self::Abort(_)
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

impl EnvironmentCommand {
    fn is_json(&self) -> bool {
        match self {
            Self::Show(args) => args.json,
            Self::Use(args) => args.json,
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

async fn dispatch_mission(
    cmd: MissionCommand,
    transports: &MissionTransports,
) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        MissionCommand::Start(args) => cmd_start(args, transports)
            .await
            .map(|()| ExitCode::SUCCESS),
        MissionCommand::Advance(args) => cmd_advance(args, transports).await,
        MissionCommand::Driver(args) => cmd_driver(args, transports).await,
        MissionCommand::DriverStderr(args) => cmd_driver_stderr(args).await,
        MissionCommand::Status(args) => cmd_status(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Guide(args) => cmd_guide(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Environment(cmd) => cmd_environment(cmd, transports)
            .await
            .map(|()| ExitCode::SUCCESS),
        MissionCommand::Report(args) => cmd_report(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Apply(args) => cmd_apply(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Log(args) => cmd_log(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Inbox(args) => cmd_inbox(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Send(args) => cmd_send(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Plan(cmd) => cmd_plan(cmd, transports).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Team(cmd) => cmd_team(cmd, transports).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Skill(cmd) => cmd_mission_skill(cmd, transports)
            .await
            .map(|()| ExitCode::SUCCESS),
        MissionCommand::Decide(args) => cmd_decide(args, transports)
            .await
            .map(|()| ExitCode::SUCCESS),
        MissionCommand::Finish(args) => cmd_finish(args, transports)
            .await
            .map(|()| ExitCode::SUCCESS),
        MissionCommand::Abort(args) => cmd_abort(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Stop(args) => cmd_stop(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Extend(args) => cmd_extend(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Continue(args) => cmd_continue(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Type(cmd) => cmd_type(cmd).await,
        MissionCommand::SelfTest(args) => crate::selftest::run(args.json).await,
    }
}

fn runtime_profiles() -> Result<RuntimeProfiles> {
    RuntimeProfiles::load(&Home::from_env()?)
}

fn validated_role_profile(
    role: &crate::model::RoleInstance,
    runtime: &str,
    profiles: &RuntimeProfiles,
    transports: &MissionTransports,
) -> Result<MissionRuntimeProfile> {
    let profile = profiles.get(runtime).with_context(|| {
        format!(
            "role '{}' resolves to unavailable runtime '{runtime}'",
            role.id
        )
    })?;
    validate_runtime_profile(&profile, transports)
        .with_context(|| format!("role '{}' resolves to invalid runtime '{runtime}'", role.id))?;
    Ok(profile)
}

fn validate_explicit_role_runtimes(
    mission_type: &MissionType,
    profiles: &RuntimeProfiles,
) -> Result<()> {
    for role in mission_type.default_team.roles.values() {
        validated_role_profile(
            role,
            &role.runtime,
            profiles,
            &MissionTransports::production(),
        )?;
    }
    Ok(())
}

fn validate_team_runtimes(
    team: &crate::model::TeamRevision,
    default_runtime: &str,
    profiles: &RuntimeProfiles,
    transports: &MissionTransports,
) -> Result<MissionRuntimeProfile> {
    let default_profile = profiles.get(default_runtime)?;
    validate_runtime_profile(&default_profile, transports)
        .with_context(|| format!("default runtime '{default_runtime}' is invalid"))?;
    let default_engine = &default_profile.confinement.oci().engine;
    for role in team.roles.values() {
        let runtime = role.runtime.as_str();
        let profile = validated_role_profile(role, runtime, profiles, transports)?;
        if profile.confinement.oci().engine != *default_engine {
            bail!(
                "role '{}' resolves to runtime '{}' using OCI engine '{}', but mission default runtime '{}' uses '{}'; one mission requires one OCI engine",
                role.id,
                runtime,
                profile.confinement.oci().engine,
                default_runtime,
                default_engine
            );
        }
    }
    Ok(default_profile)
}

fn validate_runtime_profile(
    profile: &MissionRuntimeProfile,
    transports: &MissionTransports,
) -> Result<()> {
    match &transports.runtime {
        Some((drivers, auth)) => {
            OciRoleRunner::validate_profile_with_registries(profile, drivers.clone(), auth.clone())
        }
        None => OciRoleRunner::validate_profile(profile),
    }
}

/// Build an engine over an open store, a loaded mission type, and a runtime
/// profile (whose image the caller has already pinned).
#[allow(clippy::too_many_arguments)]
async fn assemble_engine(
    store: MissionStore,
    repo: &Path,
    mission_type: crate::mission_type::MissionType,
    image_id: String,
    profiles: RuntimeProfiles,
    mut default_profile: MissionRuntimeProfile,
    ceiling: AuthorityCeiling,
    transports: &MissionTransports,
) -> Result<Engine> {
    workspace::ensure_excluded(repo).await?;
    default_profile.confinement.oci_mut().image = Some(image_id.clone());
    let runtime_identities = profiles.instrument_identities();
    let role_runner = Arc::new(match &transports.runtime {
        Some((drivers, auth)) => {
            OciRoleRunner::with_registries(profiles, ceiling, drivers.clone(), auth.clone())
        }
        None => OciRoleRunner::new(profiles, ceiling),
    });
    let effect_cleaner = Arc::new(crate::effect_cleanup::LocalEffectCleaner::new(
        default_profile.confinement.oci().engine.clone(),
    ));
    let oracle_runner: Arc<dyn OracleRunner> = transports
        .oracle
        .clone()
        .unwrap_or_else(|| Arc::new(OciOracleRunner::new(default_profile)));
    Ok(Engine::new(
        store,
        mission_type,
        image_id,
        EngineServices::new(
            role_runner,
            oracle_runner,
            effect_cleaner,
            Arc::new(SystemClock),
        )
        .with_runtime_identities(runtime_identities),
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
    transports: &MissionTransports,
) -> Result<Engine> {
    let ceiling = AuthorityCeiling::default();
    let profiles = transports.profiles()?;
    let default_profile =
        validate_team_runtimes(&mission_type.default_team, runtime, &profiles, transports)?;
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
        image_id,
        profiles,
        default_profile,
        ceiling,
        transports,
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
    transports: &MissionTransports,
) -> Result<Engine> {
    let state = store.require_state(mission_id).await?;
    let ceiling = AuthorityCeiling::default();
    let mission_type = load_mission_type_snapshot(&store, mission_id, &ceiling)?;
    let profiles = transports.profiles()?;
    let runtime = state
        .team
        .as_ref()
        .and_then(|team| team.role(&team.planning_assignment))
        .map(|role| role.runtime.clone())
        .context("mission has no active planning role runtime")?;
    let team = state.team.as_ref().context("mission has no active team")?;
    let default_profile = validate_team_runtimes(team, &runtime, &profiles, transports)?;
    let engine = assemble_engine(
        store,
        repo,
        mission_type,
        state.image_id.clone(),
        profiles,
        default_profile,
        ceiling,
        transports,
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

async fn cmd_start(args: StartArgs, transports: &MissionTransports) -> Result<()> {
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
            transports,
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
    let (mission_id, _engine) = match result {
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
        println!("{}", start_next_step(1, &mission_id, &repo));
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

async fn cmd_plan(command: PlanCommand, transports: &MissionTransports) -> Result<()> {
    match command {
        PlanCommand::Show(args) => cmd_plan_show(args).await,
        PlanCommand::Propose(args) => cmd_plan_propose(args, transports).await,
    }
}

async fn cmd_plan_show(args: PlanShowArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    let (plan, pending, base_revision) = if let Some(proposal) = &state.proposal {
        let proposal = proposal
            .plan
            .as_ref()
            .context("pending proposal changes only the team")?;
        (&proposal.plan, true, proposal.base_revision)
    } else if let Some(plan) = &state.plan {
        (plan, false, state.revision)
    } else {
        bail!("mission {mission_id} has no current or pending plan");
    };
    // The verified/attested ceiling follows proof disposition, not only oracle
    // presence. Reviewer-checkable, host-acceptance, and limitation
    // requirements can only close honestly at the attested bar.
    let ceiling = if plan.verified_possible() {
        "verified-possible"
    } else {
        "attested-only"
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
                "planning_input": planning_input_json(&state, store.blobs())?,
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
        print_planning_input(store.blobs(), &state, "")?;
    }
    Ok(())
}

async fn cmd_plan_propose(args: PlanProposeArgs, transports: &MissionTransports) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id, transports).await?;
    let proposal = read_json_arg(&args.file)?;
    engine
        .propose_plan(&mission_id, proposal)
        .await
        .context("plan proposal rejected")?;
    println!("plan proposed for mission {mission_id}");
    Ok(())
}

async fn mission_engine(
    repo: Option<PathBuf>,
    mission_id: Option<&str>,
    transports: &MissionTransports,
) -> Result<(MissionId, Engine)> {
    let (repo, store) = open_store(repo).await?;
    let mission_id = resolve_mission_id(&store, mission_id).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id, transports).await?;
    Ok((mission_id, engine))
}

async fn cmd_team(command: TeamCommand, transports: &MissionTransports) -> Result<()> {
    match command {
        TeamCommand::Show(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let team = state.team.context("mission has no configured team")?;
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "mission_id": mission_id,
                        "team": team,
                        "skills": state.skills,
                    })
                );
            } else {
                println!("team revision {} for mission {mission_id}", team.revision);
                for role in team.roles.values() {
                    println!(
                        "  {}  output={} runtime={} skills={} tmpfs={}",
                        role.id,
                        output_name(role.output),
                        role.runtime,
                        if role.skills.is_empty() {
                            "-".to_string()
                        } else {
                            role.skills.join(",")
                        },
                        if role.resources.tmpfs.is_empty() {
                            "-".to_string()
                        } else {
                            role.resources.tmpfs.join(",")
                        }
                    );
                }
                println!("  planner: {}", team.planning_assignment);
                for (task, role) in &team.task_assignments {
                    println!("  task {task}: {role}");
                }
                for (assertion, panel) in &team.judgment_assignments {
                    println!(
                        "  judgment {assertion}: {}",
                        panel
                            .iter()
                            .map(RoleInstanceId::as_str)
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                }
                if let Some(role) = &team.gap_review_assignment {
                    println!("  gap review: {role}");
                }
                if let Some(guidance) = &team.guidance {
                    println!("  guidance: {}", short_hex(&guidance.digest));
                }
            }
        }
        TeamCommand::Add(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let mut team = next_team(&state)?;
            let id = RoleInstanceId::new(args.role_instance)?;
            if team.roles.contains_key(&id) {
                bail!("role instance '{id}' already exists");
            }
            let inputs = args
                .inputs
                .into_iter()
                .map(InputName::new)
                .collect::<Result<BTreeSet<_>, _>>()?;
            let role = RoleInstance {
                id: id.clone(),
                purpose: args.purpose,
                output: parse_output(&args.output)?,
                runtime: args.runtime,
                instructions: std::fs::read_to_string(&args.instructions_file)
                    .with_context(|| format!("reading '{}'", args.instructions_file.display()))?,
                skills: args.skills,
                environment: Default::default(),
                grants: AuthorityGrants {
                    secrets: args.secrets,
                    network: args.network,
                    install: args.install,
                    writes: args.writes,
                    devices: args.devices.into_iter().collect(),
                    inputs,
                },
                resources: crate::model::ConfinementResources { tmpfs: args.tmpfs },
                deadline_secs: args.deadline_secs,
            };
            let profiles = transports.profiles()?;
            validated_role_profile(&role, &role.runtime, &profiles, transports)?;
            team.roles.insert(id, role);
            engine.configure_team(&mission_id, team).await?;
            println!("added role instance to mission {mission_id}");
        }
        TeamCommand::Reassign(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let task = TaskId::new(args.task)?;
            if !state
                .plan
                .as_ref()
                .is_some_and(|plan| plan.tasks.iter().any(|candidate| candidate.id == task))
            {
                bail!("active plan has no task '{task}'");
            }
            let role = RoleInstanceId::new(args.role_instance)?;
            let mut team = next_team(&state)?;
            team.task_assignments.insert(task, role);
            engine.configure_team(&mission_id, team).await?;
            println!("reassigned task in mission {mission_id}");
        }
        TeamCommand::Retire(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let role = RoleInstanceId::new(args.role_instance)?;
            let mut team = next_team(&state)?;
            if team.planning_assignment == role
                || team
                    .task_assignments
                    .values()
                    .any(|assigned| assigned == &role)
                || team
                    .judgment_assignments
                    .values()
                    .any(|panel| panel.contains(&role))
                || team.gap_review_assignment.as_ref() == Some(&role)
            {
                bail!("role instance '{role}' still owns an assignment");
            }
            if team.roles.remove(&role).is_none() {
                bail!("team has no role instance '{role}'");
            }
            engine.configure_team(&mission_id, team).await?;
            println!("retired role instance '{role}' from mission {mission_id}");
        }
        TeamCommand::SetRuntime(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let role_id = RoleInstanceId::new(args.role_instance)?;
            let mut team = next_team(&state)?;
            let role = team
                .roles
                .get_mut(&role_id)
                .with_context(|| format!("team has no role instance '{role_id}'"))?;
            role.runtime = args.runtime;
            let profiles = transports.profiles()?;
            validated_role_profile(role, &role.runtime, &profiles, transports)?;
            engine.configure_team(&mission_id, team).await?;
            println!("updated runtime for '{role_id}' in mission {mission_id}");
        }
        TeamCommand::GuideSet(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let mut team = next_team(&state)?;
            let text = std::fs::read_to_string(&args.file)
                .with_context(|| format!("reading '{}'", args.file.display()))?;
            team.guidance = Some(MissionGuidance::new(text));
            engine.configure_team(&mission_id, team).await?;
            println!("updated guidance for mission {mission_id}");
        }
        TeamCommand::AssignSkill(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let state = engine.load_state(&mission_id).await?;
            let role_id = RoleInstanceId::new(args.role_instance)?;
            let mut team = next_team(&state)?;
            let role = team
                .roles
                .get_mut(&role_id)
                .with_context(|| format!("team has no role instance '{role_id}'"))?;
            if !role.skills.contains(&args.skill) {
                role.skills.push(args.skill.clone());
            }
            engine.configure_team(&mission_id, team).await?;
            println!(
                "assigned skill '{}' to '{}' in mission {mission_id}",
                args.skill, role_id
            );
        }
    }
    Ok(())
}

async fn cmd_mission_skill(
    command: MissionSkillCommand,
    transports: &MissionTransports,
) -> Result<()> {
    match command {
        MissionSkillCommand::Add(args) => {
            let (mission_id, engine) =
                mission_engine(args.repo, args.mission_id.as_deref(), transports).await?;
            let source = match (args.path, args.git) {
                (Some(path), None) => SkillSource::Path(path),
                (None, Some(git)) => SkillSource::Git {
                    git,
                    rev: args.rev.context("--rev is required with --git")?,
                    subdir: args.subdir.unwrap_or_default(),
                },
                _ => unreachable!("clap enforces exactly one skill source"),
            };
            let (change, package) =
                add_mission_skill(&engine.store().mission_skills_dir(&mission_id), source).await?;
            engine
                .add_mission_skill(
                    &mission_id,
                    MissionSkill {
                        name: package.name,
                        digest: change.digest.clone(),
                        description: package.description,
                    },
                )
                .await?;
            println!(
                "{} mission skill {} {}",
                if change.changed { "added" } else { "recorded" },
                change.name,
                short_hex(&change.digest)
            );
        }
    }
    Ok(())
}

fn next_team(state: &crate::model::MissionState) -> Result<crate::model::TeamRevision> {
    let mut team = state
        .team
        .clone()
        .context("mission has no configured team")?;
    team.revision = team.revision.saturating_add(1);
    Ok(team)
}

fn parse_output(raw: &str) -> Result<OutputSemantics> {
    match raw {
        "produces-report" => Ok(OutputSemantics::ProducesReport),
        "produces-artifact" => Ok(OutputSemantics::ProducesArtifact),
        "emits-verdict" => Ok(OutputSemantics::EmitsVerdict),
        "emits-gap-verdict" => Ok(OutputSemantics::EmitsGapVerdict),
        "proposes-plan" => Ok(OutputSemantics::ProposesPlan),
        _ => bail!(
            "unknown output '{raw}'; expected produces-report, produces-artifact, emits-verdict, emits-gap-verdict, or proposes-plan"
        ),
    }
}

fn output_name(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::ProducesReport => "produces-report",
        OutputSemantics::ProducesArtifact => "produces-artifact",
        OutputSemantics::EmitsVerdict => "emits-verdict",
        OutputSemantics::EmitsGapVerdict => "emits-gap-verdict",
        OutputSemantics::ProposesPlan => "proposes-plan",
    }
}

fn parse_digest_pinned_image_ref(raw: &str) -> Result<String> {
    let image = raw.trim();
    if let Some(hex) = image.strip_prefix("sha256:") {
        return Ok(format!("sha256:{}", normalized_sha256_hex(hex)?));
    }
    if let Some((name, hex)) = image.rsplit_once("@sha256:") {
        if name.trim().is_empty() {
            bail!("environment image digest ref requires an image name before '@sha256:'");
        }
        return Ok(format!("{name}@sha256:{}", normalized_sha256_hex(hex)?));
    }
    bail!(
        "environment use requires a digest-pinned image ref (sha256:<hex> or <name>@sha256:<hex>)"
    )
}

fn normalize_oci_image_id(raw: &str) -> Result<String> {
    let image_id = raw.trim();
    if let Some(hex) = image_id.strip_prefix("sha256:") {
        return Ok(format!("sha256:{}", normalized_sha256_hex(hex)?));
    }
    Ok(format!("sha256:{}", normalized_sha256_hex(image_id)?))
}

fn normalized_sha256_hex(hex: &str) -> Result<String> {
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("expected a sha256 digest with exactly 64 hex characters");
    }
    Ok(hex.to_ascii_lowercase())
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
    if !matches!(state.phase, MissionPhase::Done { .. }) {
        bail!("mission {mission_id} did not complete; retained work is not accepted for apply");
    }
    let branch = apply_target(&mission_id, &state.base_sha, state.deliverable_head())?;
    workspace::create_branch(&repo, &branch, state.deliverable_head(), args.force)
        .await
        .with_context(|| {
            format!("could not create branch '{branch}' (already exists? use --force)")
        })?;
    store
        .append(
            &mission_id,
            state.head,
            &[NewEvent::new(crate::model::MissionEvent::ResultApplied {
                branch: branch.clone(),
                sha: state.deliverable_head().to_string(),
                reason: "mission apply created the result branch".into(),
            })],
            SystemClock.now_ms(),
        )
        .await
        .with_context(|| {
            format!(
                "branch '{branch}' was created, but ResultApplied could not be recorded; inspect mission log before applying again"
            )
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
        verdict: Option<serde_json::Value>,
        advisory_results: Vec<serde_json::Value>,
    }
    let mut rows = Vec::new();
    for (aid, a) in &state.contract {
        let verdict = if let Some((effect_id, v)) =
            a.last_authoritative_receipt.as_ref().and_then(|effect_id| {
                state
                    .authoritative_receipts
                    .get(effect_id)
                    .map(|verdict| (effect_id, verdict))
            }) {
            Some(serde_json::json!({
                "effect_id": effect_id,
                "oracle": v.oracle().as_str(),
                "passed": v.passed(),
                "exit_code": v.exit_code(),
                "exit_signal": v.exit_signal(),
                "judged_sha": v.judged_sha(),
                "environment_digest": v.environment_digest(),
                "fresh": v.is_fresh_at(state),
                "prepared_inputs": v.prepared_inputs(),
                "evidence": crate::evidence::authoritative_receipt_json(
                    store.blobs(),
                    effect_id,
                    v,
                ),
            }))
        } else {
            None
        };
        rows.push(ReportRow {
            id: aid.as_str().to_string(),
            oracle: a.oracle.as_ref().map(|o| o.as_str().to_string()),
            verdict,
            advisory_results: assertion_advisory_json(state, aid, a, store.blobs(), true),
        });
    }
    let uncovered: Vec<&str> = state
        .plan
        .as_ref()
        .map(|plan| {
            let mut assertion_ids = BTreeSet::new();
            for requirement in &plan.requirements {
                match &requirement.disposition {
                    RequirementDisposition::ConfinedProvable { assertion_ids: ids } => {
                        for assertion_id in ids {
                            if !plan.assertion_has_oracle(assertion_id) {
                                assertion_ids.insert(assertion_id.as_str());
                            }
                        }
                    }
                    RequirementDisposition::ReviewerCheckable { assertion_ids: ids } => {
                        assertion_ids.extend(ids.iter().map(|id| id.as_str()));
                    }
                    RequirementDisposition::HostAcceptance { .. }
                    | RequirementDisposition::Limitation { .. } => {}
                }
            }
            assertion_ids.into_iter().collect()
        })
        .unwrap_or_default();
    let host_acceptance_obligations = state
        .plan
        .as_ref()
        .map(|plan| {
            plan.requirements
                .iter()
                .filter_map(|requirement| match &requirement.disposition {
                    RequirementDisposition::HostAcceptance { rationale } => {
                        Some((requirement, rationale))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if args.json {
        let review = review_summary(state, store.blobs());
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "objective": state.objective,
                "mission_type": { "name": state.mission_type.name, "digest": state.mission_type.digest },
                "image_id": state.image_id,
                "environment": environment_json(state),
                "team": state.team,
                "stop_bar": state.config.stop.slug(),
                "base_sha": state.base_sha,
                "current_sha": state.deliverable_head(),
                "deliverable_head": state.deliverable_head(),
                "finish": finish.map(|f| f.slug()),
                "disposition": view.disposition.slug(),
                "next_actions": view.next_actions(),
                "tasks": state.tasks.iter().map(|(id, task)| {
                    task_runtime_json(
                        state,
                        id,
                        task,
                        workspace_observations.get(id),
                        store.blobs(),
                    )
                }).collect::<Result<Vec<_>>>()?,
                "parked_effects": parked_effect_views(state),
                "conversations": conversation_views(state, &store)?,
                "role_attempt_receipts": role_attempt_receipts_json(state, store.blobs()),
                "assertions": rows.iter().map(|row| serde_json::json!({
                    "id": row.id,
                    "oracle": row.oracle,
                    "verdict": row.verdict,
                    "advisory_results": row.advisory_results,
                })).collect::<Vec<_>>(),
                "host_acceptance_obligations": host_acceptance_obligations.iter().map(|(requirement, rationale)| serde_json::json!({
                    "id": requirement.id.as_str(),
                    "kind": requirement.kind,
                    "requirement": requirement.prose,
                    "rationale": rationale,
                })).collect::<Vec<_>>(),
                "not_verified_by_oracle": uncovered,
                "superseded_assertions": superseded_assertions_json(state, store.blobs()),
                "acknowledged_gates": state.acknowledged_gates.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
                "oracle_failures": state.oracle_failures,
                "gap_review": review,
                "attention": state.open_attention.values().map(|item| {
                    attention_json(store.blobs(), state, item)
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
    println!("  image:   {}", state.image_id);
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
        Some(FinishClass::Attested) => {
            println!("  finish:  ATTESTED — fresh assigned judges passed without a fresh oracle contradiction")
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
        print_task_outcome(
            store.blobs(),
            state,
            task_id,
            task,
            &format!("  task {task_id}"),
        );
    }
    print_task_workspace_observations(state, "  ", &workspace_observations);
    print_workspace_control_state(state, "  ");
    print_parked_controls(state, "  ");
    print_conversations(state, &store, "  ")?;
    print_non_task_failures(store.blobs(), state);
    print_role_attempt_receipts(store.blobs(), state, "  ");
    if let Some(failure) = &state.cleanup_failure {
        println!(
            "  cleanup: blocked for effect {} ({:?}): {}",
            failure.effect_id,
            failure.resource,
            failure.failure.detail()
        );
    }
    if let Some(line) = review_line(state, store.blobs()) {
        println!("  {line}");
        if let Some((receipt, judged_sha, is_fresh, _, gaps)) = gap_review_verdict(state) {
            if is_fresh {
                for gap in gaps {
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
                for line in
                    crate::evidence::render_role_attempt_receipt(store.blobs(), state, receipt)
                        .lines()
                {
                    println!("      {line}");
                }
            } else {
                // Stale findings describe a superseded tree; never render
                // them like fresh ones.
                println!(
                    "    (a superseded verdict at {} recorded {} gap(s); see 'mission log')",
                    short_hex(judged_sha),
                    gaps.len()
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
                        " [STALE - not at the current commit/environment]"
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
                    let assertion = state
                        .contract
                        .get(&crate::model::AssertionId::new(id).expect("stored assertion id"))
                        .expect("stored assertion");
                    let effect_id = assertion
                        .last_authoritative_receipt
                        .as_ref()
                        .expect("report verdict receipt");
                    let verdict = state
                        .authoritative_receipts
                        .get(effect_id)
                        .expect("report verdict");
                    for line in crate::evidence::render_authoritative_receipt(
                        store.blobs(),
                        effect_id,
                        verdict,
                    )
                    .lines()
                    {
                        println!("      {line}");
                    }
                }
            }
            None if row.oracle.is_some() => println!("    {id}: (oracle owed, not yet run)"),
            None => println!("    {id}: advisory only — no oracle can prove this"),
        }
        for advisory in &row.advisory_results {
            println!(
                "      validator {}: {}",
                advisory["role_instance"].as_str().unwrap_or("?"),
                match advisory["passed"].as_bool() {
                    Some(true) => "PASS",
                    Some(false) => "FAIL",
                    None => "UNAVAILABLE",
                }
            );
            println!(
                "        exact handoff effect: {}",
                advisory["effect_id"].as_str().unwrap_or("?")
            );
        }
    }
    if !uncovered.is_empty() {
        println!(
            "\n  NOT verified by an oracle (judged proof only or missing oracle): {}",
            uncovered.join(", ")
        );
    }
    if !host_acceptance_obligations.is_empty() {
        println!("\n  host acceptance obligations:");
        for (requirement, rationale) in host_acceptance_obligations {
            println!("    {}: {}", requirement.id, requirement.prose);
            println!("      rationale: {rationale}");
        }
    }
    print_superseded_assertions(state, store.blobs(), "  ");
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
            print_attention(store.blobs(), state, item, "    ")?;
        }
    }
    if !matches!(view.disposition, MissionDisposition::Terminal) {
        println!("\n  next: {}", view.next_actions().join(" | "));
    }
    if args.patch && state.deliverable_head() != state.base_sha {
        let diff = workspace::diff(&repo, &state.base_sha, state.deliverable_head()).await?;
        println!(
            "\n--- diff {}..{} ---\n{diff}",
            state.base_sha,
            state.deliverable_head()
        );
    }
    Ok(())
}

async fn cmd_guide(args: GuideArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let view = load_mission_view(&store, &mission_id).await?;
    if args.json {
        println!("{}", guide_json(&view, &store).await?);
    } else {
        print_guide(&view, &store).await?;
    }
    Ok(())
}

async fn cmd_environment(
    command: EnvironmentCommand,
    transports: &MissionTransports,
) -> Result<()> {
    match command {
        EnvironmentCommand::Show(args) => cmd_environment_show(args).await,
        EnvironmentCommand::Use(args) => cmd_environment_use(args, transports).await,
    }
}

async fn cmd_environment_show(args: EnvironmentShowArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "environment": environment_json(&state),
            })
        );
        return Ok(());
    }
    println!("mission {mission_id} environment");
    print_environment(&state, "  ");
    Ok(())
}

async fn cmd_environment_use(
    args: EnvironmentUseArgs,
    transports: &MissionTransports,
) -> Result<()> {
    let image_ref = parse_digest_pinned_image_ref(&args.image)?;
    if args.reason.trim().is_empty() {
        bail!("environment use requires a non-empty --reason");
    }
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let Some(_driver_guard) =
        crate::driver_lock::DriverGuard::try_acquire(&store.driver_lock_path(&mission_id))?
    else {
        bail!("mission {mission_id} has a running driver; retry after it parks or finishes");
    };
    let state = store.require_state(&mission_id).await?;
    if state.phase.is_terminal() {
        bail!("mission {mission_id} is terminal; environment assignment is closed");
    }
    if !state.inflight.is_empty() {
        bail!(
            "mission {mission_id} has {} active effect(s); retry after they settle",
            state.inflight.len()
        );
    }
    let team = state.team.as_ref().context("mission has no active team")?;
    let runtime = team
        .role(&team.planning_assignment)
        .map(|role| role.runtime.clone())
        .context("mission has no active planning role runtime")?;
    let profiles = transports.profiles()?;
    let profile = validate_team_runtimes(team, &runtime, &profiles, transports)?;
    let engine = profile.confinement.oci().engine.clone();
    let image_id =
        lionclaw_confinement::resolve_oci_image_compatibility_identity(&engine, &image_ref)
            .await
            .with_context(|| format!("preflighting digest-pinned image '{image_ref}'"))?;
    let image_id = normalize_oci_image_id(&image_id)?;
    let preflight = EnvironmentPreflight {
        engine,
        image_ref: image_ref.clone(),
        image_id: image_id.clone(),
    };
    let event = NewEvent::new(crate::model::MissionEvent::EnvironmentAssigned {
        image_ref: image_ref.clone(),
        image_id: image_id.clone(),
        preflight,
        team_revision: Some(team.revision),
        reason: args.reason,
    });
    store
        .append(&mission_id, state.head, &[event], SystemClock.now_ms())
        .await
        .with_context(|| {
            format!(
                "mission {mission_id} changed while assigning environment; retry from current status"
            )
        })?;
    if args.json {
        let updated = store.require_state(&mission_id).await?;
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "mission_id": mission_id.as_str(),
                "environment": environment_json(&updated),
            })
        );
    } else {
        println!(
            "assigned environment for mission {mission_id}: {}",
            short_hex(&image_id)
        );
    }
    Ok(())
}

async fn cmd_decide(args: DecideArgs, transports: &MissionTransports) -> Result<()> {
    let action = parse_decision_action(&args.action)?;
    let justification = decision_text(&args, &action)?;
    let (mission_id, engine) =
        mission_engine(args.repo, Some(args.mission_id.as_str()), transports).await?;
    engine
        .decide(&mission_id, &args.item, action, &justification)
        .await?;
    println!(
        "recorded decision on '{}' for mission {mission_id}",
        args.item
    );
    Ok(())
}

async fn cmd_abort(args: AbortArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    crate::engine::record_abort(&store, SystemClock.now_ms(), &mission_id, &args.reason).await?;
    println!("aborted mission {mission_id}");
    Ok(())
}

async fn cmd_finish(args: FinishArgs, transports: &MissionTransports) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id, transports).await?;
    engine.finish(&mission_id, &args.reason).await
}

async fn cmd_send(args: SendArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let mut references =
        Vec::with_capacity(args.receipts.len() + args.parks.len() + args.commits.len());
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
    crate::engine::record_message(
        &store,
        &repo,
        &mission_id,
        crate::engine::MessageCommand {
            selectors: args.to,
            all: args.all,
            body: args.message,
            references,
        },
        SystemClock.now_ms(),
    )
    .await?;
    println!("message recorded for mission {mission_id}");
    Ok(())
}

async fn cmd_stop(args: ControlArgs) -> Result<()> {
    let mission_id = MissionId::parse(&args.mission_id)?;
    let effect_id = EffectId::parse(&args.effect_id)?;
    let (_repo, store) = open_store(args.repo).await?;
    let action = ControlAction::Stop;
    record_control(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &effect_id,
        action,
        &args.reason,
    )
    .await?;
    println!("recorded stop for effect {effect_id}");
    Ok(())
}

async fn cmd_continue(args: ContinueArgs) -> Result<()> {
    let ControlArgs {
        mission_id,
        effect_id,
        repo,
        reason,
    } = args.control;
    let mission_id = MissionId::parse(&mission_id)?;
    let effect_id = EffectId::parse(&effect_id)?;
    let (_repo, store) = open_store(repo).await?;
    let mode = if args.recreate {
        crate::model::ContinueMode::RecreateWorkspace
    } else {
        crate::model::ContinueMode::Preserve
    };
    record_control(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &effect_id,
        ControlAction::Continue {
            automatic: false,
            mode,
        },
        &reason,
    )
    .await?;
    println!(
        "recorded {} for effect {effect_id}",
        if args.recreate {
            "continue --recreate"
        } else {
            "continue"
        }
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
        other => bail!("unknown action '{other}' (approve|retry|repair|revise|accept)"),
    })
}

#[cfg(test)]
mod team_cli_tests {
    use super::*;

    #[test]
    fn parses_direct_team_runtime_and_guidance_commands() {
        let runtime = Cli::try_parse_from([
            "lionclaw",
            "mission",
            "team",
            "set-runtime",
            "engineer",
            "hermes",
        ])
        .unwrap();
        assert!(matches!(
            runtime.command,
            Command::Mission(MissionCommand::Team(TeamCommand::SetRuntime(_)))
        ));

        let guidance = Cli::try_parse_from([
            "lionclaw",
            "mission",
            "team",
            "guide-set",
            "--file",
            "guidance.md",
        ])
        .unwrap();
        assert!(matches!(
            guidance.command,
            Command::Mission(MissionCommand::Team(TeamCommand::GuideSet(_)))
        ));
    }

    #[test]
    fn parses_mission_guide_and_environment_commands() {
        let guide = Cli::try_parse_from(["lionclaw", "mission", "guide", "--json"]).unwrap();
        assert!(matches!(
            guide.command,
            Command::Mission(MissionCommand::Guide(_))
        ));

        let show =
            Cli::try_parse_from(["lionclaw", "mission", "environment", "show", "--json"]).unwrap();
        assert!(matches!(
            show.command,
            Command::Mission(MissionCommand::Environment(EnvironmentCommand::Show(_)))
        ));

        let digest = format!("sha256:{}", "a".repeat(64));
        let assign = Cli::try_parse_from([
            "lionclaw",
            "mission",
            "environment",
            "use",
            digest.as_str(),
            "--reason",
            "benchmark image",
        ])
        .unwrap();
        assert!(matches!(
            assign.command,
            Command::Mission(MissionCommand::Environment(EnvironmentCommand::Use(_)))
        ));
    }

    #[test]
    fn environment_image_refs_must_be_digest_pinned() {
        let digest = "A".repeat(64);
        assert_eq!(
            parse_digest_pinned_image_ref(&format!("localhost/test@sha256:{digest}")).unwrap(),
            format!("localhost/test@sha256:{}", "a".repeat(64))
        );
        assert!(parse_digest_pinned_image_ref("localhost/test:latest").is_err());
        assert!(parse_digest_pinned_image_ref("sha256:1234").is_err());
    }

    #[test]
    fn parses_mission_local_skill_add() {
        let cli = Cli::try_parse_from([
            "lionclaw",
            "mission",
            "skill",
            "add",
            "--path",
            "skills/specialist",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Mission(MissionCommand::Skill(MissionSkillCommand::Add(_)))
        ));
    }
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
    mission_dirs: &crate::resources::MissionDirs,
) -> Result<DetachedDriver> {
    spawn_detached_driver_with(command, mission_dirs, std::process::Command::spawn)
}

fn spawn_detached_driver_with(
    command: &mut std::process::Command,
    mission_dirs: &crate::resources::MissionDirs,
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
        .arg("--state-dir")
        .arg(mission_dirs.state_dir())
        .arg("--mission-id")
        .arg(mission_dirs.mission_id().as_str())
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
    mission_dirs: &crate::resources::MissionDirs,
) -> Result<DriverStartup> {
    await_driver_startup_with_timeout(child, handshake, mission_dirs, Duration::from_secs(5)).await
}

async fn await_driver_startup_with_timeout(
    child: &mut DetachedDriver,
    handshake: &Path,
    mission_dirs: &crate::resources::MissionDirs,
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
                let detail = crate::activity::driver_error(mission_dirs)
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

async fn cmd_advance(
    args: AdvanceArgs,
    transports: &MissionTransports,
) -> Result<std::process::ExitCode> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let initial = load_mission_view(&store, &mission_id).await?;
    let mut child = None;
    let mut startup = None;
    if matches!(
        initial.disposition,
        MissionDisposition::Ready | MissionDisposition::CleanupBlocked
    ) {
        let mission_dirs = store.mission_dirs(&mission_id);
        let handshake = mission_dirs.root().join(format!(
            "driver-{}-{}.ready",
            std::process::id(),
            SystemClock.now_ms()
        ));
        crate::activity::clear_driver_run_evidence(&mission_dirs)?;
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
        let spawned = spawn_detached_driver(&mut command, &mission_dirs)?;
        child = Some(spawned);
        let startup_result = await_driver_startup(
            child.as_mut().expect("driver was just spawned"),
            &handshake,
            &mission_dirs,
        )
        .await;
        let _ = std::fs::remove_file(&handshake);
        startup = Some(startup_result?);
    }
    if args.wait {
        if let Some(mut child) = child {
            let status = child.wait().await?;
            if !status.success() {
                let detail = crate::activity::driver_error(&store.mission_dirs(&mission_id))
                    .unwrap_or_else(|| "no driver error evidence was recorded".into());
                bail!("mission driver exited unsuccessfully ({status}): {detail}");
            }
            if startup == Some(DriverStartup::LostRace) {
                wait_for_existing_driver(&store, &mission_id).await?;
            }
        } else if matches!(
            initial.disposition,
            MissionDisposition::Running | MissionDisposition::Terminal
        ) {
            wait_for_existing_driver(&store, &mission_id).await?;
        }
    }
    // Parked and fully terminal missions still use advance as the explicit
    // retry after a descriptor-safe scratch removal failed. A live or newly
    // launched driver owns its own final reconciliation.
    reconcile_disposable_conversation_resources_if_idle(&store, &mission_id).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id, transports).await?;
    let view = load_mission_view(engine.store(), &mission_id).await?;
    let state = &view.state;
    print_mission_view(&view, engine.store(), args.json).await?;
    // Closing over acknowledged review gaps (or a waived review) was an
    // explicit, justified human decision — exit SUCCESS, but say so. The
    // summary already applies the freshness law, so a stale verdict from a
    // superseded head never triggers a false note here.
    if matches!(state.phase, MissionPhase::Done { .. }) {
        let summary = review_summary(state, engine.store().blobs());
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
            eprintln!("note: closed with the gap review waived; see 'mission report'");
        }
    }
    Ok(std::process::ExitCode::SUCCESS)
}

async fn cmd_driver(
    args: DriverArgs,
    transports: &MissionTransports,
) -> Result<std::process::ExitCode> {
    let mission_id = MissionId::parse(&args.mission_id)?;
    let store = MissionStore::open(&args.repo).await?;
    let mission_dirs = store.mission_dirs(&mission_id);
    crate::activity::clear_driver_error(&mission_dirs)?;
    let result = async {
        let engine = build_engine_for_mission(store, &args.repo, &mission_id, transports).await?;
        engine
            .advance_with_handshake(&mission_id, Some(&args.handshake))
            .await?;
        Ok(std::process::ExitCode::SUCCESS)
    }
    .await;
    if let Err(error) = &result {
        let _ = crate::activity::record_driver_error(&mission_dirs, error);
    }
    result
}

async fn cmd_driver_stderr(args: DriverStderrArgs) -> Result<std::process::ExitCode> {
    let mission_id = MissionId::parse(&args.mission_id)?;
    let mission_dirs = crate::resources::MissionDirs::new(&args.state_dir, &mission_id);
    tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        crate::activity::spool_driver_stderr(stdin.lock(), &mission_dirs)
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
        println!("{}", status_json(&view, &store).await?);
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
        print_environment(state, "");
        if let Some(failure) = &state.cleanup_failure {
            println!(
                "cleanup blocked for effect {} ({:?}): {}",
                failure.effect_id,
                failure.resource,
                failure.failure.detail()
            );
        }
        if let Some(line) = review_line(state, store.blobs()) {
            println!("{line}");
        }
        for (id, assertion) in &state.contract {
            let auth = state
                .authoritative_verdict(assertion)
                .map(|v| if v.passed() { "pass" } else { "fail" })
                .unwrap_or("—");
            println!("  {id}: authoritative={auth}");
        }
        print_conversations(state, &store, "  ")?;
        for (task_id, task) in &state.tasks {
            print_task_outcome(
                store.blobs(),
                state,
                task_id,
                task,
                &format!("  task {task_id}"),
            );
        }
        print_non_task_failures(store.blobs(), state);
        print_role_attempt_receipts(store.blobs(), state, "  ");
        print_superseded_assertions(state, store.blobs(), "  ");
        print_task_workspace_observations(state, "  ", &workspace_observations);
        print_workspace_control_state(state, "  ");
        if view.disposition == MissionDisposition::Running {
            print_activity(&store, state)?;
        }
        if let Some(error) = crate::activity::driver_error(&store.mission_dirs(&mission_id)) {
            println!("driver error: {error}");
        }
        for (effect_id, parked) in &state.parked_effects {
            let controls = parked_legal_controls(state, effect_id);
            println!(
                "parked effect {}: {:?}; legal controls={}",
                effect_id,
                parked,
                controls_display(&controls)
            );
        }
        print_planning_input(store.blobs(), state, "")?;
        for item in state.open_attention.values() {
            print_attention(store.blobs(), state, item, "  ")?;
        }
        println!("next: {}", view.next_actions().join(" | "));
    }
    Ok(())
}

async fn watch_status(store: &MissionStore, mission_id: &MissionId, json: bool) -> Result<()> {
    let mut previous = Vec::new();
    loop {
        let view = load_mission_view(store, mission_id).await?;
        let Some(activity) = running_activity(store, &view.state, view.disposition) else {
            if view.disposition != MissionDisposition::Running {
                return Ok(());
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
            continue;
        };
        let bytes = if json {
            serde_json::to_vec(&status_json(&view, store).await?)?
        } else {
            serde_json::to_vec(&activity)?
        };
        if bytes != previous {
            if json {
                println!("{}", String::from_utf8_lossy(&bytes));
            } else {
                for effect in activity.effects {
                    let Some(inflight) = view.state.inflight.iter().find_map(|(id, inflight)| {
                        (id.as_str() == effect.effect_id).then_some(inflight)
                    }) else {
                        continue;
                    };
                    println!(
                        "{} {} elapsed={}ms deadline={} workspace={}",
                        effect.effect_id,
                        effect.last_activity,
                        effect.elapsed_ms,
                        inflight.deadline_ms(),
                        workspace_observation_summary(&effect.workspace)
                    );
                }
                print_conversations(&view.state, store, "  ")?;
            }
            std::io::stdout().flush()?;
            previous = bytes;
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn status_json(view: &MissionView, store: &MissionStore) -> Result<serde_json::Value> {
    let mut value = mission_view_json(view, store).await?;
    value["activity"] = running_activity(store, &view.state, view.disposition)
        .and_then(|activity| serde_json::to_value(activity).ok())
        .unwrap_or(serde_json::Value::Null);
    value["driver_error"] =
        crate::activity::driver_error(&store.mission_dirs(&view.state.mission_id))
            .map_or(serde_json::Value::Null, serde_json::Value::String);
    Ok(value)
}

fn running_activity(
    store: &MissionStore,
    state: &crate::model::MissionState,
    disposition: MissionDisposition,
) -> Option<crate::activity::ActivityProjection> {
    (disposition == MissionDisposition::Running)
        .then(|| crate::activity::load_validated(&store.mission_dirs(&state.mission_id), state))
        .flatten()
}

fn print_activity(store: &MissionStore, state: &crate::model::MissionState) -> Result<()> {
    let Some(activity) =
        crate::activity::load_validated(&store.mission_dirs(&state.mission_id), state)
    else {
        return Ok(());
    };
    for effect in activity.effects {
        let Some(inflight) = state
            .inflight
            .iter()
            .find_map(|(id, inflight)| (id.as_str() == effect.effect_id).then_some(inflight))
        else {
            return Ok(());
        };
        println!(
            "activity {}: {} elapsed={}ms deadline={} controls={}",
            effect.effect_id,
            effect.last_activity,
            effect.elapsed_ms,
            inflight.deadline_ms(),
            if state.reached_deadlines.contains_key(
                state
                    .inflight
                    .keys()
                    .find(|id| id.as_str() == effect.effect_id)
                    .unwrap()
            ) {
                ""
            } else {
                "stop|extend_deadline"
            }
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
                "roles": mt.default_team.roles.keys().map(|role| role.as_str()).collect::<Vec<_>>(),
                "role_skills": mt.default_team.roles.values().map(|role| {
                    (role.id.as_str(), &role.skills)
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
                "team": mt.default_team,
                "ceilings": mt.ceilings,
                "resource_ceilings": mt.resource_ceilings,
                "oracle_resources": mt.oracle_resources,
                "oracle_devices": mt.oracle_devices,
                "playbook": mt.playbook,
            })
        );
        return;
    }
    println!("mission type '{}' is valid", mt.name);
    println!("  digest: {}", short_hex(mt.digest()));
    println!("  stop:  {:?}", mt.stop);
    println!("  image: {}", mt.image);
    println!("  planning role: {}", mt.default_team.planning_assignment);
    if let Some(role) = &mt.default_team.gap_review_assignment {
        println!("  gap review role: {role}");
    }
    println!(
        "  roles: {}",
        mt.default_team
            .roles
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
    if !mt.resource_ceilings.tmpfs.is_empty() {
        println!(
            "  tmpfs ceilings: {}",
            mt.resource_ceilings.tmpfs.join(", ")
        );
    }
    if !mt.oracle_resources.is_empty() {
        println!("  oracle resources:");
        for (oracle, resources) in &mt.oracle_resources {
            println!("    {oracle}: tmpfs={}", resources.tmpfs.join(","));
        }
    }
    if !mt.oracle_devices.is_empty() {
        println!("  oracle devices:");
        for (oracle, devices) in &mt.oracle_devices {
            println!(
                "    {oracle}: {}",
                devices.iter().cloned().collect::<Vec<_>>().join(",")
            );
        }
    }
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
                    print_attention(blobs, state, item, "  ")?;
                }
                print_conversations(state, store, "  ")?;
                print_task_workspace_observations(state, "  ", &workspace_observations);
            }
            MissionDisposition::Running => {
                println!("mission {mission_id}: running under another driver")
            }
            MissionDisposition::AwaitingLead => {
                println!("mission {mission_id}: awaiting lead feedback");
                print_conversations(state, store, "  ")?;
            }
            MissionDisposition::CleanupBlocked => {
                if let Some(failure) = &state.cleanup_failure {
                    println!(
                        "mission {mission_id}: cleanup blocked for effect {} ({:?})",
                        failure.effect_id, failure.resource
                    );
                    println!("  {}", failure.failure.detail());
                } else {
                    println!(
                        "mission {mission_id}: {} with {} inherited effect(s) awaiting cleanup",
                        phase_slug(&state.phase),
                        state.inflight.len()
                    );
                    for effect_id in state.inflight.keys() {
                        println!("  effect {effect_id}: no live driver; recovery required");
                    }
                }
            }
            MissionDisposition::Terminal => {
                println!("mission {mission_id}: {}", phase_slug(&state.phase));
                if let Some(line) = review_line(state, blobs) {
                    println!("  {line}");
                }
            }
            MissionDisposition::Ready => println!("mission {mission_id}: ready to advance"),
        }
        for (task_id, task) in &state.tasks {
            print_task_outcome(blobs, state, task_id, task, &format!("  task {task_id}"));
        }
        print_non_task_failures(blobs, state);
        print_role_attempt_receipts(blobs, state, "  ");
        if matches!(view.disposition, MissionDisposition::Terminal) {
            print_gap_review_receipt(blobs, state, "  ");
        }
        print_workspace_control_state(state, "  ");
        print_parked_controls(state, "  ");
        print_superseded_assertions(state, blobs, "  ");
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

async fn guide_json(view: &MissionView, store: &MissionStore) -> Result<serde_json::Value> {
    let state = &view.state;
    Ok(serde_json::json!({
        "mission_id": state.mission_id.as_str(),
        "objective": state.objective,
        "phase": phase_slug(&state.phase),
        "finish": state.phase.finish().map(|finish| finish.slug()),
        "disposition": view.disposition.slug(),
        "stop_bar": state.config.stop.slug(),
        "revision": state.revision,
        "team_revision": state.team.as_ref().map(|team| team.revision),
        "current_sha": state.deliverable_head(),
        "deliverable_head": state.deliverable_head(),
        "environment": environment_json(state),
        "next_actions": view.next_actions(),
        "operator_loop": [
            "mission guide",
            "mission status --json",
        ],
        "active_effects": active_effects_json(state),
        "parked_effects": parked_effect_views(state),
        "attention": state.open_attention.values().map(|item| {
            attention_json(store.blobs(), state, item)
        }).collect::<Result<Vec<_>>>()?,
        "conversations": conversation_views(state, store)?,
    }))
}

async fn print_guide(view: &MissionView, store: &MissionStore) -> Result<()> {
    let state = &view.state;
    println!("mission {} guide", state.mission_id);
    println!("objective: {}", state.objective);
    println!(
        "state: {} ({})",
        phase_slug(&state.phase),
        view.disposition.slug()
    );
    println!("commit: {}", short_hex(state.deliverable_head()));
    print_environment(state, "");
    if !state.inflight.is_empty() {
        println!("active effects:");
        for effect in active_effects_json(state) {
            println!(
                "  {} {} deadline={}",
                effect["effect_id"].as_str().unwrap_or("?"),
                effect["kind"].as_str().unwrap_or("?"),
                effect["deadline_ms"]
            );
        }
    }
    if !state.parked_effects.is_empty() {
        println!("parked effects:");
        for (effect_id, parked) in &state.parked_effects {
            println!("  {effect_id}: {:?}", parked);
        }
    }
    if !state.open_attention.is_empty() {
        println!("attention:");
        for item in state.open_attention.values() {
            println!("  {} [{}] {}", item.id, item.kind.slug(), item.report);
        }
    }
    print_conversations(state, store, "  ")?;
    println!("next: {}", view.next_actions().join(" | "));
    Ok(())
}

fn environment_json(state: &crate::model::MissionState) -> serde_json::Value {
    serde_json::json!({
        "image_id": &state.image_id,
        "active_assignment": state.environment_history.last(),
        "history": &state.environment_history,
    })
}

fn print_environment(state: &crate::model::MissionState, indent: &str) {
    println!("{indent}environment: {}", state.image_id);
    if let Some(active) = state.environment_history.last() {
        println!(
            "{indent}  assignment {}: {} via {}",
            active.revision, active.image_ref, active.preflight.engine
        );
    }
}

fn active_effects_json(state: &crate::model::MissionState) -> Vec<serde_json::Value> {
    state
        .inflight
        .iter()
        .map(|(effect_id, effect)| match effect {
            crate::model::InflightEffect::RoleTurn {
                role_instance,
                task_id,
                assertion_ids,
                deadline_ms,
                ..
            } => serde_json::json!({
                "effect_id": effect_id.as_str(),
                "kind": "role_turn",
                "role_instance": role_instance.as_str(),
                "task_id": task_id.as_ref().map(|task| task.as_str()),
                "assertion_ids": assertion_ids.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "deadline_ms": deadline_ms,
            }),
            crate::model::InflightEffect::OracleRun {
                oracle,
                assertion_ids,
                deadline_ms,
                ..
            } => serde_json::json!({
                "effect_id": effect_id.as_str(),
                "kind": "oracle_run",
                "oracle": oracle.as_str(),
                "assertion_ids": assertion_ids.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
                "deadline_ms": deadline_ms,
            }),
        })
        .collect()
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
        "team_revision": state.team.as_ref().map(|team| team.revision),
        "environment": environment_json(state),
        "current_sha": state.deliverable_head(),
        "deliverable_head": state.deliverable_head(),
        "objective": state.objective,
        "conversations": conversation_views(state, store)?,
        "role_attempt_receipts": role_attempt_receipts_json(state, blobs),
        "tasks": state.tasks.iter().map(|(id, task)| {
            task_runtime_json(
                state,
                id,
                task,
                workspace_observations.get(id),
                blobs,
            )
        }).collect::<Result<Vec<_>>>()?,
        "planning_input": planning_input_json(state, blobs)?,
        "contract": state.contract.iter().map(|(id, assertion)| {
            serde_json::json!({
                "id": id.as_str(),
                "advisory": state.advisory_status(id).slug(),
                "advisory_results": assertion_advisory_json(
                    state,
                    id,
                    assertion,
                    blobs,
                    true,
                ),
                "authoritative_pass": state
                    .authoritative_verdict(assertion)
                    .map(crate::model::AuthoritativeVerdict::passed),
            })
        }).collect::<Vec<_>>(),
        "superseded_assertions": superseded_assertions_json(state, blobs),
        "attention": state.open_attention.values().map(|item| {
            attention_json(blobs, state, item)
        }).collect::<Result<Vec<_>>>()?,
        "cleanup_failure": cleanup_failure_json(state),
        "oracle_failures": state.oracle_failures,
        "parked_effects": parked_effect_views(state),
        "gap_review": review_summary(state, blobs),
        "gap_review_receipt": gap_review_receipt_json(state, blobs),
    }))
}

fn assertion_advisory_json(
    state: &crate::model::MissionState,
    assertion_id: &crate::model::AssertionId,
    assertion: &crate::model::AssertionState,
    blobs: &BlobStore,
    require_current: bool,
) -> Vec<serde_json::Value> {
    assertion
        .last_advisory
        .iter()
        .map(|(role_instance, effect_id)| {
            let resolved = if require_current {
                state.advisory_receipt(assertion_id, role_instance, effect_id)
            } else {
                let receipt = state.role_attempt_receipts.get(effect_id);
                receipt.and_then(|receipt| {
                    let crate::model::RoleEffectSource::Turn { request, .. } = &receipt.source;
                    let output = state
                        .team_history
                        .get(&request.team_revision)?
                        .role(&request.role_instance)?
                        .output;
                    if &request.role_instance != role_instance
                        || output != crate::model::OutputSemantics::EmitsVerdict
                        || !request.assertion_ids.contains(assertion_id)
                    {
                        return None;
                    }
                    let crate::model::SettledHandoff::Validate { items, .. } =
                        receipt.settled_handoff()?
                    else {
                        return None;
                    };
                    items
                        .iter()
                        .find(|item| &item.item_id == assertion_id)
                        .map(|item| (receipt, item.passed))
                })
            };
            serde_json::json!({
                "role_instance": role_instance.as_str(),
                "effect_id": effect_id.as_str(),
                "passed": resolved.map(|(_, passed)| passed),
                "receipt": crate::evidence::resolved_role_attempt_reference_json(
                    blobs,
                    state,
                    effect_id,
                    resolved.map(|(receipt, _)| receipt),
                ),
            })
        })
        .collect()
}

fn superseded_assertions_json(
    state: &crate::model::MissionState,
    blobs: &BlobStore,
) -> Vec<serde_json::Value> {
    state
        .superseded_assertions
        .iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.assertion.id.as_str(),
                "prose": entry.assertion.prose,
                "replacement_ids": entry.replacement_ids.iter()
                    .map(|id| id.as_str()).collect::<Vec<_>>(),
                "superseded_at_revision": entry.superseded_at_revision,
                "advisory_results": assertion_advisory_json(
                    state,
                    &entry.assertion.id,
                    &entry.state,
                    blobs,
                    false,
                ),
                "authoritative_pass": state.authoritative_verdict(&entry.state)
                    .map(crate::model::AuthoritativeVerdict::passed),
            })
        })
        .collect()
}

fn print_superseded_assertions(
    state: &crate::model::MissionState,
    blobs: &BlobStore,
    indent: &str,
) {
    if state.superseded_assertions.is_empty() {
        return;
    }
    println!("{indent}superseded assertions:");
    for entry in &state.superseded_assertions {
        let replacements = entry
            .replacement_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{indent}  {} at revision {} -> [{}]",
            entry.assertion.id, entry.superseded_at_revision, replacements
        );
        for advisory in
            assertion_advisory_json(state, &entry.assertion.id, &entry.state, blobs, false)
        {
            println!(
                "{indent}    validator {}: {} effect={} [SUPERSEDED]",
                advisory["task_id"].as_str().unwrap_or("?"),
                match advisory["passed"].as_bool() {
                    Some(true) => "PASS",
                    Some(false) => "FAIL",
                    None => "UNAVAILABLE",
                },
                advisory["effect_id"].as_str().unwrap_or("?"),
            );
        }
    }
}

fn task_runtime_json(
    state: &crate::model::MissionState,
    id: &crate::model::TaskId,
    task: &crate::model::TaskRuntimeState,
    workspace_observation: Option<&crate::activity::WorkspaceObservation>,
    blobs: &BlobStore,
) -> Result<serde_json::Value> {
    let assignment = task.role_assignment.as_ref();
    let workspace = task.workspace_provenance.as_ref();
    Ok(serde_json::json!({
        "id": id.as_str(),
        "status": format!("{:?}", task.status).to_ascii_lowercase(),
        "assignment_base_sha": assignment.map(|assignment| assignment.base_sha.as_str()),
        "candidate_sha": task.candidate_sha.as_deref(),
        "pending_base_sha": task.pending_base_sha.as_deref(),
        "assignment_epoch": assignment.map(|assignment| assignment.assignment_epoch).unwrap_or(0),
        "workspace_provenance": workspace.map(|workspace| serde_json::json!({
            "effect_id": workspace.effect_id.as_str(),
            "base_sha": workspace.base_sha,
            "assignment_epoch": workspace.assignment_epoch,
            "archived_effect_id": workspace.archived_effect_id.as_ref()
                .map(|effect_id| effect_id.as_str()),
        })),
        "pending_workspace_recreation": task.pending_workspace_recreation.as_ref()
            .map(|effect_id| effect_id.as_str()),
        "workspace_observation": workspace_observation,
        "outcome": task.last_outcome.as_ref().map(|outcome| {
            task_outcome_json(state, id, blobs, outcome)
        }),
    }))
}

fn task_outcome_json(
    state: &crate::model::MissionState,
    task_id: &crate::model::TaskId,
    blobs: &BlobStore,
    outcome: &crate::model::TaskAttemptOutcome,
) -> serde_json::Value {
    let effect_id = outcome.effect_id();
    let kind = match outcome {
        crate::model::TaskAttemptOutcome::Accepted { .. } => "accepted",
        crate::model::TaskAttemptOutcome::Failed { .. } => "failed",
    };
    let receipt = state
        .task_last_role_attempt(task_id)
        .filter(|receipt| &receipt.effect_id == effect_id);
    serde_json::json!({
        "kind": kind,
        "effect_id": effect_id.as_str(),
        "receipt": crate::evidence::resolved_role_attempt_reference_json(
            blobs,
            state,
            effect_id,
            receipt,
        ),
    })
}

fn print_task_outcome(
    blobs: &BlobStore,
    state: &crate::model::MissionState,
    task_id: &crate::model::TaskId,
    task: &crate::model::TaskRuntimeState,
    label: &str,
) {
    let Some(outcome) = &task.last_outcome else {
        return;
    };
    let kind = match outcome {
        crate::model::TaskAttemptOutcome::Accepted { .. } => "accepted",
        crate::model::TaskAttemptOutcome::Failed { .. } => "failed",
    };
    println!("{label} {kind} role attempt {}:", outcome.effect_id());
    let receipt = state
        .task_last_role_attempt(task_id)
        .filter(|receipt| receipt.effect_id == *outcome.effect_id());
    for line in crate::evidence::render_resolved_role_attempt_reference(
        blobs,
        state,
        outcome.effect_id(),
        receipt,
    )
    .lines()
    {
        println!("    {line}");
    }
}

fn parked_legal_controls(
    state: &crate::model::MissionState,
    effect_id: &crate::model::EffectId,
) -> Vec<&'static str> {
    if !state.parked_effect_is_continuable(effect_id) {
        return Vec::new();
    }
    let mut controls = Vec::new();
    if state.parked_continue_is_legal(effect_id, crate::model::ContinueMode::Preserve) {
        controls.push("continue");
    }
    if state.parked_continue_is_legal(effect_id, crate::model::ContinueMode::RecreateWorkspace) {
        controls.push("continue --recreate");
    }
    controls
}

fn parked_effect_views(state: &crate::model::MissionState) -> Vec<serde_json::Value> {
    state
        .parked_effects
        .iter()
        .filter(|(effect_id, _)| state.parked_effect_is_continuable(effect_id))
        .map(|(effect_id, parked)| {
            serde_json::json!({
                "effect_id": effect_id.as_str(),
                "kind": parked,
                "legal_controls": parked_legal_controls(state, effect_id),
            })
        })
        .collect()
}

fn controls_display(controls: &[&str]) -> String {
    if controls.is_empty() {
        "none".into()
    } else {
        controls.join(", ")
    }
}

fn print_parked_controls(state: &crate::model::MissionState, indent: &str) {
    for (effect_id, parked) in &state.parked_effects {
        let controls = parked_legal_controls(state, effect_id);
        println!(
            "{indent}parked effect {effect_id}: {parked:?}; legal controls={}",
            controls_display(&controls)
        );
    }
}

fn print_workspace_control_state(state: &crate::model::MissionState, indent: &str) {
    for line in workspace_control_lines(state) {
        println!("{indent}{line}");
    }
}

fn workspace_control_lines(state: &crate::model::MissionState) -> Vec<String> {
    state
        .tasks
        .iter()
        .filter_map(|(task_id, task)| {
            task.pending_workspace_recreation.as_ref().map(|effect_id| {
                format!(
                "task {task_id} has pending workspace archive/recreation intent from parked effect {effect_id}"
                )
            })
        })
        .collect()
}

fn conversation_views(
    state: &crate::model::MissionState,
    store: &MissionStore,
) -> Result<Vec<serde_json::Value>> {
    state
        .conversations
        .iter()
        .map(|(id, conversation)| {
            let session_control_root = crate::resources::MissionDirs::new(
                store.lionclaw_dir(),
                &state.mission_id,
            )
            .role(id)
            .role_state()
            .session_control_root()
            .to_path_buf();
            let resume_mode = match lionclaw_runtime_api::recorded_runtime_resume_mode_at(
                store.lionclaw_dir(),
                &session_control_root,
            )? {
                Some(lionclaw_runtime_api::RuntimeResumeMode::Resumed) => "native_session",
                Some(lionclaw_runtime_api::RuntimeResumeMode::Reconstructed) | None => {
                    "canonical_reconstruction"
                }
            };
            Ok(serde_json::json!({
                "id": id.as_str(),
                "role_instance": conversation.role_instance.as_str(),
                "lifecycle": conversation.lifecycle,
                "final_response": conversation.final_response.as_ref()
                    .map(|response| store.blobs().resolve(response))
                    .transpose()?,
                "role_attempts": state.role_attempt_receipts.values()
                    .filter(|receipt| matches!(
                        &receipt.source,
                        crate::model::RoleEffectSource::Turn {
                            request,
                            ..
                        } if &request.role_instance == id
                    ))
                    .map(|receipt| crate::evidence::role_attempt_receipt_json(
                        store.blobs(),
                        state,
                        receipt,
                    ))
                    .collect::<Vec<_>>(),
                "queued_messages": conversation.queued,
                "consumed_through": conversation.consumed_through,
                "active_message_boundary": conversation.active_delivery.as_ref().map(|delivery| delivery.message_boundary),
                "presented_messages": conversation.active_delivery.as_ref().map(|delivery| &delivery.presented_messages),
                "invalid_handoff_reworks": conversation.invalid_handoff_reworks,
                "runtime_resume_mode": resume_mode,
                "legal_actions": state.conversation_legal_actions(id),
                "retained_workspace_archives": state.retained_workspace_archives
                    .iter()
                    .filter(|(task_id, _)| state.team.as_ref()
                        .and_then(|team| team.task_assignments.get(*task_id)) == Some(id))
                    .flat_map(|(_, archives)| archives)
                    .map(|effect_id| effect_id.as_str())
                    .collect::<Vec<_>>(),
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
            "{indent}conversation {}: lifecycle={} queued={} delivery_through={} resume={} legal_actions={}",
            conversation["id"].as_str().unwrap_or("?"),
            conversation["lifecycle"].as_str().unwrap_or("?"),
            conversation["queued_messages"]
                .as_array()
                .map_or(0, Vec::len),
            conversation["consumed_through"],
            conversation["runtime_resume_mode"].as_str().unwrap_or("?"),
            conversation["legal_actions"]
                .as_array()
                .map(|actions| actions
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<Vec<_>>()
                    .join("|"))
                .unwrap_or_default()
        );
        if let Some(response) = conversation["final_response"].as_str() {
            println!("{indent}  final response: {response}");
        }
        if let Some(archives) = conversation["retained_workspace_archives"].as_array() {
            for effect_id in archives {
                println!(
                    "{indent}  retained workspace archive: {}",
                    effect_id.as_str().unwrap_or("?")
                );
            }
        }
        if let Some(messages) = conversation["queued_messages"].as_array() {
            for message in messages {
                println!(
                    "{indent}  queued message {}: marker={} body={} references={}",
                    message["sequence_no"],
                    message["marker"].as_str().unwrap_or("?"),
                    message["body"].as_str().unwrap_or("?"),
                    message["references"]
                );
            }
        }
    }
    Ok(())
}

fn role_attempt_receipts_json(
    state: &crate::model::MissionState,
    blobs: &BlobStore,
) -> Vec<serde_json::Value> {
    state
        .role_attempt_receipts
        .values()
        .map(|receipt| crate::evidence::role_attempt_receipt_json(blobs, state, receipt))
        .collect()
}

fn print_role_attempt_receipts(
    blobs: &BlobStore,
    state: &crate::model::MissionState,
    indent: &str,
) {
    for receipt in state.role_attempt_receipts.values() {
        println!("{indent}role attempt receipt:");
        for line in crate::evidence::render_role_attempt_receipt(blobs, state, receipt).lines() {
            println!("{indent}  {line}");
        }
    }
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
            "evidence": crate::evidence::decision_evidence_json(
                blobs,
                state,
                &feedback.evidence,
            )?,
        }),
        None => serde_json::Value::Null,
    };
    Ok(serde_json::json!({
        "base_revision": state.revision,
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
            proposal
                .plan
                .as_ref()
                .map_or(state.revision, |plan| plan.base_revision)
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
            for line in crate::evidence::render_feedback(blobs, state, feedback)?.lines() {
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
    let rendered = crate::evidence::render_typed_failure(failure);
    for (index, line) in rendered.lines().enumerate() {
        if index == 0 {
            println!("{prefix}{line}");
        } else {
            println!("    {line}");
        }
    }
}

fn print_non_task_failures(blobs: &BlobStore, state: &crate::model::MissionState) {
    for (oracle, failure) in &state.oracle_failures {
        print_typed_failure(failure, &format!("  oracle {oracle} failure: "));
    }
    if let Some(crate::model::ReviewOutcome::Failed { effect_id }) = &state.gap_review.outcome {
        if let Some(receipt) = state.role_attempt_receipts.get(effect_id) {
            if let Some(failure) = receipt.failure() {
                print_typed_failure(failure, "  gap review failure: ");
            }
            println!("  gap review receipt:");
            for line in crate::evidence::render_role_attempt_receipt(blobs, state, receipt).lines()
            {
                println!("    {line}");
            }
        } else {
            println!("  gap review failure receipt unavailable: {effect_id}");
        }
    }
}

fn gap_review_receipt_json(
    state: &crate::model::MissionState,
    blobs: &BlobStore,
) -> serde_json::Value {
    state
        .gap_review
        .outcome
        .as_ref()
        .map_or(serde_json::Value::Null, |outcome| {
            crate::evidence::role_attempt_reference_json(blobs, state, outcome.effect_id())
        })
}

fn print_gap_review_receipt(blobs: &BlobStore, state: &crate::model::MissionState, indent: &str) {
    if let Some(outcome) = state.gap_review.outcome.as_ref() {
        println!("{indent}gap review receipt:");
        for line in
            crate::evidence::render_role_attempt_reference(blobs, state, outcome.effect_id())
                .lines()
        {
            println!("{indent}  {line}");
        }
    }
}

fn attention_json(
    blobs: &BlobStore,
    state: &crate::model::MissionState,
    item: &crate::model::AttentionItem,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "id": item.id,
        "kind": item.kind.slug(),
        "report": item.report,
        "assertion_ids": item.assertion_ids.iter().map(|id| id.as_str()).collect::<Vec<_>>(),
        "actions": crate::model::decision::legal_actions(state, item)
            .iter()
            .map(crate::model::DecisionAction::slug)
            .collect::<Vec<_>>(),
        "evidence": crate::evidence::decision_evidence_json(blobs, state, &item.evidence)?,
    }))
}

fn print_attention(
    blobs: &BlobStore,
    state: &crate::model::MissionState,
    item: &crate::model::AttentionItem,
    indent: &str,
) -> Result<()> {
    println!("{indent}[{}] {}", item.id, item.report);
    let actions = crate::model::decision::legal_actions(state, item)
        .iter()
        .map(crate::model::DecisionAction::slug)
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{indent}  actions: {actions}");
    let evidence = crate::evidence::render_decision_evidence(blobs, state, &item.evidence)?;
    if !evidence.is_empty() {
        for line in evidence.lines() {
            println!("{indent}  {line}");
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

/// The gap-review summary — ONE source of truth behind the advance
/// banner, `status`, `report`, and every `--json` output. `Null` when the
/// mission declares no review.
fn gap_review_verdict(
    state: &crate::model::MissionState,
) -> Option<(
    &crate::model::RoleAttemptReceipt,
    &str,
    bool,
    bool,
    &[crate::model::Gap],
)> {
    let crate::model::ReviewOutcome::Verdict { effect_id } = state.gap_review.outcome.as_ref()?
    else {
        return None;
    };
    let receipt = state.role_attempt_receipts.get(effect_id)?;
    let crate::model::RoleEffectSource::Turn { request, .. } = &receipt.source;
    let role = state
        .team_history
        .get(&request.team_revision)?
        .role(&request.role_instance)?;
    if role.output != crate::model::OutputSemantics::EmitsGapVerdict {
        return None;
    }
    let crate::model::SettledHandoff::Review { passed, gaps } = receipt.settled_handoff()? else {
        return None;
    };
    Some((
        receipt,
        request.base_sha.as_str(),
        request.is_fresh_at(state),
        *passed,
        gaps,
    ))
}

fn review_summary(state: &crate::model::MissionState, blobs: &BlobStore) -> serde_json::Value {
    use crate::model::{AttentionKind, GapSeverity, ReviewOutcome};
    if !state.config.requires_gap_review {
        return serde_json::Value::Null;
    }
    let tr = &state.gap_review;
    // A terminal mission owes nothing: a successful finish requires a settled
    // review, while an abort ends every remaining obligation.
    let done = state.phase.is_terminal();
    let proof_failed = state.open_attention.values().any(|item| {
        matches!(
            item.kind,
            AttentionKind::OracleFailed | AttentionKind::ProofFailed
        )
    });
    let waived = tr.waived_at(state);
    let (verdict, judged_sha, fresh, counts, acknowledged) =
        if let Some((_, judged_sha, is_fresh, passed, gaps)) = gap_review_verdict(state) {
            let count =
                |severity: GapSeverity| gaps.iter().filter(|gap| gap.severity == severity).count();
            let blocking = !passed || gaps.iter().any(|gap| gap.severity == GapSeverity::Blocking);
            let kind = if !is_fresh && done {
                "skipped"
            } else if blocking {
                "gaps"
            } else {
                "clean"
            };
            (
                kind,
                Some(judged_sha.to_string()),
                Some(is_fresh),
                Some(serde_json::json!({
                    "blocking": count(GapSeverity::Blocking),
                    "major": count(GapSeverity::Major),
                    "minor": count(GapSeverity::Minor),
                })),
                tr.acknowledges_sha(state, judged_sha),
            )
        } else {
            match &tr.outcome {
                Some(ReviewOutcome::Verdict { .. }) | Some(ReviewOutcome::Failed { .. }) => {
                    ("failed", None, None, None, false)
                }
                None if waived => ("waived", None, None, None, false),
                None if done || proof_failed => ("skipped", None, None, None, false),
                None => ("owed", None, None, None, false),
            }
        };
    serde_json::json!({
        "role": state.team.as_ref()
            .and_then(|team| team.gap_review_assignment.as_ref())
            .map(|role| role.as_str()),
        "verdict": verdict,
        "judged_sha": judged_sha,
        "fresh": fresh,
        "gaps": counts,
        "acknowledged": acknowledged,
        "waived": waived,
        "attempts": tr.attempts,
        "failure_receipt": match &tr.outcome {
            Some(ReviewOutcome::Failed { effect_id }) => {
                crate::evidence::role_attempt_reference_json(blobs, state, effect_id)
            }
            _ => serde_json::Value::Null,
        },
    })
}

/// The one-line human rendering of `review_summary`; `None` when the mission
/// declares no review.
fn review_line(state: &crate::model::MissionState, blobs: &BlobStore) -> Option<String> {
    let summary = review_summary(state, blobs);
    if summary.is_null() {
        return None;
    }
    let sha = summary["judged_sha"]
        .as_str()
        .map(short_hex)
        .unwrap_or_default();
    let stale = if summary["fresh"] == serde_json::json!(false) {
        " [STALE - not at the current commit/environment]"
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
        "skipped" => "review: deferred — required proof is not settled".to_string(),
        _ => "review: owed — not yet judged at the final commit".to_string(),
    })
}
