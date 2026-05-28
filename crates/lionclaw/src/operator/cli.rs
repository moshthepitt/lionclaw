use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, IsTerminal, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use clap::{ArgGroup, Args, Parser, Subcommand};
use cron::Schedule;
use uuid::Uuid;

use crate::{
    contracts::{
        ChannelGrantView, ChannelHealthCheck, ChannelPairingInviteRequest,
        ChannelPairingListResponse, ChannelPairingStatus, ChannelPairingView,
        ChannelRoutingProfile, ContinuityDraftActionRequest, ContinuityDraftListRequest,
        ContinuityPathRequest, ContinuitySearchRequest, JobCreateRequest, JobRefRequest,
        JobRunsRequest, JobScheduleDto, TrustTier,
    },
    home::LionClawHome,
    kernel::{
        jobs::normalize_cron_expression,
        runtime::{ConfinementConfig, ExecutionLimits, MountAccess, OciConfinementConfig},
    },
    operator::{
        attach::attach_channel,
        command_display::{lionclaw_home_command_prefix, shell_quote_arg},
        config::{
            derive_skill_alias, normalize_podman_executable, normalize_runtime_command,
            ChannelLaunchMode, OperatorConfig, RuntimeProfileConfig,
        },
        connect::{connect_channel, ConnectChannelRequest, ConnectEnvInputs, ConnectOutcome},
        doctor::run_doctor,
        managed_units::{SystemdUserUnitManager, UnitManager},
        operations::{
            down_instance, no_managed_units_message, operate_project_instances,
            project_log_components, selected_log_components, up_instance, write_journal_logs,
            LogFilter, LogOptions, StackOperation,
        },
        reconcile::{
            add_channel_with_contact, add_skill, open_kernel, open_runtime_kernel_for_work_root,
            pairing_approve, pairing_block, pairing_invite, pairing_list, pairing_revoke,
            remove_channel, remove_skill, resolve_installed_skill_worker_entrypoint,
            ChannelContactSetup,
        },
        run::{run_local, RunLocalInvocation},
        run_runtime_tui::{run_runtime_tui, RunRuntimeTuiInvocation},
        run_tui::{
            run_console, run_launch_blocker, run_project_launch_blocker, LaunchBlocker,
            RunConsoleInvocation,
        },
        runtime::{resolve_runtime_id, validate_runtime_availability},
        runtime_integration::{
            configure_runtime_profile_with_engine_resolver, configure_runtime_required_message,
            ConfigureRuntimeOutcome,
        },
        runtime_mounts::{
            add_runtime_mount, configured_runtime_mounts, remove_runtime_mount,
            validate_configured_runtime_mounts, RuntimeMountContext,
        },
        skills::{list_installed_skills, InstalledSkill},
        snapshot::SKILL_INSTALL_METADATA_FILE,
        status::{
            missing_target_for_status, render_project_status_all_with_manager,
            render_target_status_with_manager, validate_status_all_target,
        },
        target::{
            adopt_project_instance, create_project_instance, discover_project_root, init_project,
            list_project_instance_statuses, list_project_instances, resolve_existing_project_root,
            resolve_project_setup_root, resolve_target, TargetContext, TargetSelection,
            WorkRootRequirement,
        },
        team_local::ensure_project_team_local,
    },
    project_inventory::ProjectInstanceRuntimeContext,
    runtime_timeouts::{parse_duration, RuntimeTurnTimeouts},
};

const PROJECT_VALIDATE_MISMATCH_EXIT: u8 = 20;

#[derive(Debug, Parser)]
#[command(name = "lionclaw")]
#[command(about = "LionClaw operator CLI")]
pub struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Target an exact LionClaw instance home"
    )]
    home: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Target a LionClaw project root"
    )]
    project: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        value_name = "NAME",
        help = "Target a project instance"
    )]
    instance: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    Instance {
        #[command(subcommand)]
        command: InstanceCommand,
    },
    Configure(ConfigureArgs),
    Connect(ConnectArgs),
    Run(RunArgs),
    Up(AllArgs),
    Down(AllArgs),
    Logs(ProductLogsArgs),
    Status(StatusArgs),
    Doctor(DoctorArgs),
    #[command(hide = true)]
    ProjectValidate(ProjectValidateArgs),
    Runtime {
        #[command(subcommand)]
        command: Box<RuntimeCommand>,
    },
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    Continuity {
        #[command(subcommand)]
        command: ContinuityCommand,
    },
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
}

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("run_mode").args(["plain", "runtime_tui"])))]
struct RunArgs {
    #[arg(long, help = "Use the plain line-oriented interactive path")]
    plain: bool,
    #[arg(
        long,
        help = "Attach directly to the selected runtime's own terminal UI"
    )]
    runtime_tui: bool,
    #[arg(long)]
    continue_last_session: bool,
    #[arg(
        long,
        conflicts_with = "runtime_tui",
        value_parser = parse_runtime_timeout,
        help = "Runtime turn limit, such as 30m, 2h, or 7200s"
    )]
    timeout: Option<Duration>,
    runtime: Option<String>,
}

#[derive(Debug, Args)]
struct ConfigureArgs {
    #[arg(long)]
    runtime: Option<String>,
}

#[derive(Debug, Args)]
struct ConnectArgs {
    channel_or_path: String,
    #[arg(long = "env-file", value_name = "PATH")]
    env_file: Option<PathBuf>,
    #[arg(long = "from-env", value_name = "NAME")]
    from_env: Vec<String>,
    #[arg(long)]
    contact: bool,
    #[arg(long = "conversation-ref", value_name = "REF")]
    conversation_ref: Option<String>,
    #[arg(long = "thread-ref", value_name = "REF")]
    thread_ref: Option<String>,
}

#[derive(Debug, Args)]
struct StatusArgs {
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct AllArgs {
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct ProductLogsArgs {
    #[arg(short = 'f', long)]
    follow: bool,
    #[arg(long, default_value_t = 200)]
    tail: usize,
    #[arg(long)]
    since: Option<String>,
    #[arg(long)]
    daemon: bool,
    #[arg(long)]
    workers: bool,
    #[arg(long, value_name = "CHANNEL")]
    worker: Option<String>,
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    Init,
}

#[derive(Debug, Subcommand)]
enum InstanceCommand {
    Create(InstanceCreateArgs),
    List,
    Adopt(InstanceAdoptArgs),
}

#[derive(Debug, Args)]
struct InstanceCreateArgs {
    name: String,
    #[arg(
        long = "work-root",
        value_name = "PATH",
        help = "Default host directory this instance operates on, resolved from the project root"
    )]
    work_root: Option<PathBuf>,
    #[arg(long = "create-work-root", help = "Create the work root during setup")]
    create_work_root: bool,
}

#[derive(Debug, Args)]
struct InstanceAdoptArgs {
    name: String,
    #[arg(value_name = "HOME")]
    source_home: PathBuf,
    #[arg(
        long = "work-root",
        value_name = "PATH",
        help = "Default host directory this instance operates on, resolved from the project root"
    )]
    work_root: Option<PathBuf>,
    #[arg(long = "create-work-root", help = "Create the work root during setup")]
    create_work_root: bool,
}

#[derive(Debug, Args)]
struct ProjectValidateArgs {
    #[arg(long = "runtime-id")]
    runtime_id: String,
    #[arg(long = "runtime-kind")]
    runtime_kind: String,
    #[arg(long = "runtime-bin")]
    runtime_bin: String,
    #[arg(long = "podman-bin")]
    podman_bin: Option<String>,
    #[arg(long = "runtime-image")]
    runtime_image: String,
    #[arg(long = "channel-alias")]
    channel_alias: String,
    #[arg(long = "channel-source")]
    channel_source: String,
    #[arg(long = "channel-id")]
    channel_id: String,
    #[arg(long = "launch-mode")]
    launch_mode: String,
    #[arg(long = "project-skill")]
    project_skills: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum RuntimeCommand {
    Add(Box<RuntimeAddArgs>),
    Rm(RuntimeRmArgs),
    Ls,
    Mount {
        #[command(subcommand)]
        command: RuntimeMountCommand,
    },
    SetDefault(RuntimeSetDefaultArgs),
}

#[derive(Debug, Subcommand)]
enum RuntimeMountCommand {
    List(RuntimeMountListArgs),
    Add(RuntimeMountAddArgs),
    Remove(RuntimeMountRemoveArgs),
}

#[derive(Debug, Args)]
struct RuntimeAddArgs {
    id: String,
    #[arg(long)]
    kind: String,
    #[arg(
        long = "bin",
        help = "Runtime command to execute inside the confinement image"
    )]
    executable: String,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    agent: Option<String>,
    #[arg(long, help = "Host Podman executable LionClaw should use")]
    engine: Option<String>,
    #[arg(long, help = "Runtime image LionClaw should run with Podman")]
    image: Option<String>,
    #[arg(long, help = "Use a read-only container root filesystem")]
    read_only_rootfs: bool,
    #[arg(long = "tmpfs", help = "Add a tmpfs mount inside the confined runtime")]
    tmpfs: Vec<String>,
    #[arg(long = "memory-limit")]
    memory_limit: Option<String>,
    #[arg(long = "cpu-limit")]
    cpu_limit: Option<String>,
    #[arg(long = "pids-limit")]
    pids_limit: Option<u32>,
}

#[derive(Debug, Args)]
struct RuntimeRmArgs {
    id: String,
}

#[derive(Debug, Args)]
struct RuntimeSetDefaultArgs {
    id: String,
}

#[derive(Debug, Args)]
struct RuntimeMountListArgs {
    runtime_id: String,
}

#[derive(Debug, Args)]
struct RuntimeMountAddArgs {
    runtime_id: String,
    target: String,
    #[arg(long)]
    source: PathBuf,
    #[arg(long = "read-only", conflicts_with = "read_write")]
    read_only: bool,
    #[arg(long = "read-write", conflicts_with = "read_only")]
    read_write: bool,
}

#[derive(Debug, Args)]
struct RuntimeMountRemoveArgs {
    runtime_id: String,
    target: String,
}

#[derive(Debug, Subcommand)]
enum SkillCommand {
    Install(SkillAddArgs),
    Add(SkillAddArgs),
    List,
    Remove(SkillRmArgs),
    #[command(hide = true)]
    Rm(SkillRmArgs),
    #[command(hide = true)]
    WorkerPath(SkillWorkerPathArgs),
}

#[derive(Debug, Args)]
struct SkillAddArgs {
    source: String,
    #[arg(long)]
    alias: Option<String>,
    #[arg(long, default_value = "local")]
    reference: String,
}

#[derive(Debug, Args)]
struct SkillRmArgs {
    alias: String,
}

#[derive(Debug, Args)]
struct SkillWorkerPathArgs {
    alias: String,
}

#[derive(Debug, Subcommand)]
enum ChannelCommand {
    Add(ChannelAddArgs),
    List(ChannelListArgs),
    Remove(ChannelRmArgs),
    Rm(ChannelRmArgs),
    Attach(ChannelAttachArgs),
    Pairing {
        #[command(subcommand)]
        command: ChannelPairingCommand,
    },
}

#[derive(Debug, Args)]
struct ChannelAddArgs {
    id: String,
    #[arg(long)]
    skill: Option<String>,
    #[arg(long, default_value = "background")]
    launch: String,
    #[arg(long = "required-env")]
    required_env: Vec<String>,
    #[arg(long)]
    contact: bool,
    #[arg(long = "conversation-ref", value_name = "REF")]
    conversation_ref: Option<String>,
    #[arg(long = "thread-ref", value_name = "REF")]
    thread_ref: Option<String>,
}

#[derive(Debug, Args)]
struct ChannelRmArgs {
    id: String,
}

#[derive(Debug, Args)]
struct ChannelListArgs {
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct ChannelAttachArgs {
    id: String,
    #[arg(long)]
    peer: Option<String>,
    #[arg(long)]
    runtime: Option<String>,
}

#[derive(Debug, Subcommand)]
enum ChannelPairingCommand {
    List(PairingListArgs),
    Invite(PairingInviteArgs),
    Approve(PairingApproveArgs),
    Block(PairingBlockArgs),
    Revoke(PairingRevokeArgs),
}

#[derive(Debug, Subcommand)]
enum JobCommand {
    Add(Box<JobAddArgs>),
    Ls,
    Show(JobRefArgs),
    Pause(JobRefArgs),
    Resume(JobRefArgs),
    Run(JobRefArgs),
    Rm(JobRefArgs),
    Runs(JobRunsArgs),
    Tick,
}

#[derive(Debug, Subcommand)]
enum ContinuityCommand {
    Status,
    Search(ContinuitySearchArgs),
    Get(ContinuityPathArgs),
    Drafts {
        #[command(subcommand)]
        command: ContinuityDraftCommand,
    },
    Loops {
        #[command(subcommand)]
        command: ContinuityLoopCommand,
    },
    Proposals {
        #[command(subcommand)]
        command: ContinuityProposalCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ContinuityLoopCommand {
    Ls,
    Resolve(ContinuityPathArgs),
}

#[derive(Debug, Subcommand)]
enum ContinuityDraftCommand {
    Ls(ContinuityDraftListArgs),
    Promote(ContinuityDraftPathArgs),
    Discard(ContinuityDraftPathArgs),
}

#[derive(Debug, Subcommand)]
enum ContinuityProposalCommand {
    Ls,
    Merge(ContinuityPathArgs),
    Reject(ContinuityPathArgs),
}

#[derive(Debug, Args)]
struct PairingListArgs {
    #[arg(long)]
    channel_id: Option<String>,
    #[arg(long)]
    status: Option<String>,
}

#[derive(Debug, Args)]
struct PairingApproveArgs {
    channel_id: String,
    pairing: String,
    #[arg(long)]
    label: Option<String>,
    #[arg(long = "routing-profile")]
    routing_profile: Option<String>,
    #[arg(long, default_value = "main")]
    trust_tier: String,
}

#[derive(Debug, Args)]
struct PairingInviteArgs {
    channel_id: String,
    #[arg(long = "scope")]
    scope: String,
    #[arg(long)]
    label: Option<String>,
    #[arg(long = "conversation-ref")]
    conversation_ref: Option<String>,
    #[arg(long = "thread-ref")]
    thread_ref: Option<String>,
    #[arg(long = "expires-in", value_parser = parse_runtime_timeout)]
    expires_in: Option<Duration>,
    #[arg(long = "max-claims")]
    max_claims: Option<u32>,
}

#[derive(Debug, Args)]
struct ContinuityDraftListArgs {
    #[arg(long)]
    runtime: Option<String>,
}

#[derive(Debug, Args)]
struct ContinuityDraftPathArgs {
    relative_path: String,
    #[arg(long)]
    runtime: Option<String>,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("block_target")
        .required(true)
        .args(["sender_ref", "pairing_id"])
))]
struct PairingBlockArgs {
    channel_id: String,
    #[arg(value_name = "SENDER_REF")]
    sender_ref: Option<String>,
    #[arg(long = "pairing-id")]
    pairing_id: Option<Uuid>,
    #[arg(long = "conversation-ref")]
    conversation_ref: Option<String>,
    #[arg(long = "thread-ref")]
    thread_ref: Option<String>,
    #[arg(long)]
    reason: Option<String>,
}

#[derive(Debug, Args)]
struct PairingRevokeArgs {
    channel_id: String,
    grant_id: Uuid,
    #[arg(long)]
    reason: Option<String>,
}

#[derive(Debug, Args)]
struct JobAddArgs {
    name: String,
    #[arg(long)]
    schedule: String,
    #[arg(long)]
    tz: Option<String>,
    #[arg(long)]
    prompt: Option<String>,
    #[arg(long = "prompt-file")]
    prompt_file: Option<String>,
    #[arg(long)]
    runtime: Option<String>,
    #[arg(long = "allow")]
    allow_capabilities: Vec<String>,
    #[arg(long = "deliver-channel")]
    deliver_channel: Option<String>,
    #[arg(long = "deliver-conversation")]
    deliver_conversation: Option<String>,
    #[arg(long = "deliver-thread")]
    deliver_thread: Option<String>,
    #[arg(long = "deliver-reply-to")]
    deliver_reply_to: Option<String>,
    #[arg(long = "retry-attempts")]
    retry_attempts: Option<u32>,
}

#[derive(Debug, Args)]
struct JobRefArgs {
    job_id: String,
}

#[derive(Debug, Args)]
struct JobRunsArgs {
    job_id: String,
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(Debug, Args)]
struct ContinuitySearchArgs {
    query: String,
    #[arg(long, default_value_t = 10)]
    limit: usize,
}

#[derive(Debug, Args)]
struct ContinuityPathArgs {
    relative_path: String,
}

pub async fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let target_selection = TargetSelection {
        home: cli.home,
        project: cli.project,
        instance: cli.instance,
    };
    let command = cli.command;
    let env_home = LionClawHome::from_env();
    let run_uses_tui = matches!(
        &command,
        Command::Run(args) if should_use_run_tui(
            args,
            std::io::stdin().is_terminal(),
            std::io::stdout().is_terminal()
        )
    );
    let resolved_target = match if run_uses_tui {
        resolve_target(&target_selection, WorkRootRequirement::Optional).map(Some)
    } else {
        resolve_command_target(&target_selection, &command)
    } {
        Ok(target) => target,
        Err(err) if run_uses_tui => {
            if let Some(project_root) = empty_project_for_tui_launch_blocker(&target_selection) {
                run_project_launch_blocker(
                    project_root.clone(),
                    LaunchBlocker::no_project_instances(&project_root),
                )
                .await?;
            } else {
                run_launch_blocker(LaunchBlocker::standalone(err.to_string())).await?;
            }
            return Ok(ExitCode::from(1));
        }
        Err(err) => return Err(err),
    };
    let home = resolved_target
        .as_ref()
        .map(|target| target.instance_home.clone())
        .unwrap_or(env_home);

    match command {
        Command::Project { command } => match command {
            ProjectCommand::Init => {
                let project_root = resolve_project_setup_root(&target_selection)?;
                let result = init_project(&project_root)?;
                ensure_project_team_local(&result.project_root).await?;
                println!(
                    "initialized LionClaw project {}",
                    result.project_root.display()
                );
                println!(
                    "* {} home={} work-root={}",
                    result.instance.name,
                    result.instance.home.display(),
                    result.instance.work_root.display()
                );
            }
        },
        Command::Instance { command } => match command {
            InstanceCommand::Create(args) => {
                let project_root = resolve_existing_project_root(&target_selection)?;
                let instance = create_project_instance(
                    &project_root,
                    &args.name,
                    args.work_root.as_deref(),
                    args.create_work_root,
                )?;
                ensure_project_team_local(&project_root).await?;
                println!(
                    "created instance {} home={} work-root={}",
                    instance.name,
                    instance.home.display(),
                    instance.work_root.display()
                );
            }
            InstanceCommand::List => {
                let project_root = resolve_existing_project_root(&target_selection)?;
                let entries = list_project_instances(&project_root)?;
                if entries.is_empty() {
                    println!("no instances found");
                } else {
                    for entry in entries {
                        let marker = if entry.is_default { "*" } else { " " };
                        let work_root = entry
                            .work_root
                            .as_ref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "-".to_string());
                        let shared = if entry.shared_work_root_count > 1 {
                            entry.shared_work_root_count.to_string()
                        } else {
                            "-".to_string()
                        };
                        println!(
                            "{} {} home={} work-root={} shared={}",
                            marker,
                            entry.name,
                            entry.home.display(),
                            work_root,
                            shared
                        );
                    }
                }
            }
            InstanceCommand::Adopt(args) => {
                let project_root = resolve_existing_project_root(&target_selection)?;
                let instance = adopt_project_instance(
                    &project_root,
                    &args.name,
                    &args.source_home,
                    args.work_root.as_deref(),
                    args.create_work_root,
                )?;
                ensure_project_team_local(&project_root).await?;
                println!(
                    "adopted instance {} home={} work-root={}",
                    instance.name,
                    instance.home.display(),
                    instance.work_root.display()
                );
            }
        },
        Command::Configure(args) => {
            let target = resolved_target
                .as_ref()
                .ok_or_else(|| anyhow!("configure requires a resolved LionClaw target"))?;
            let stdin = std::io::stdin();
            let interactive = stdin.is_terminal();
            let mut input = BufReader::new(stdin);
            let stdout = std::io::stdout();
            let mut output = stdout;
            let outcome = configure_runtime_with_io(
                &target.instance_home,
                args.runtime,
                interactive,
                &mut input,
                &mut output,
            )
            .await?;
            print_configure_outcome(&target.instance_home, &outcome);
        }
        Command::Connect(args) => {
            let target = resolved_target
                .as_ref()
                .ok_or_else(|| anyhow!("connect requires a resolved LionClaw target"))?;
            let connect_started_at = Utc::now();
            let stdin = std::io::stdin();
            let interactive = stdin.is_terminal();
            let mut input = BufReader::new(stdin);
            let stdout = std::io::stdout();
            let mut output = stdout;
            let manager = SystemdUserUnitManager;
            let outcome = connect_channel(
                ConnectChannelRequest {
                    home: &target.instance_home,
                    manager: &manager,
                    project_root: target.project_root.as_deref(),
                    work_root: target.require_work_root()?,
                    channel_or_path: &args.channel_or_path,
                    env_inputs: ConnectEnvInputs {
                        env_file: args.env_file,
                        from_env: args.from_env,
                    },
                    contact: ChannelContactSetup {
                        enabled: args.contact,
                        conversation_ref: args.conversation_ref,
                        thread_ref: args.thread_ref,
                        instance_name: target.instance_name.clone(),
                    },
                    interactive,
                    hide_prompt_input: interactive,
                },
                &mut input,
                &mut output,
            )
            .await?;
            print_connect_outcome(&target.instance_home, &outcome, connect_started_at).await?;
        }
        Command::Run(args) => {
            let target = resolved_target
                .as_ref()
                .ok_or_else(|| anyhow!("run requires a resolved LionClaw target"))?;
            let timeout_override = args.timeout.map(RuntimeTurnTimeouts::with_turn_timeout);
            if args.runtime_tui {
                if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
                    bail!("run --runtime-tui requires terminal stdin and stdout");
                }
                run_runtime_tui(RunRuntimeTuiInvocation {
                    home: &target.instance_home,
                    work_root: target.require_work_root()?,
                    instance_name: target.instance_name.as_deref(),
                    project_instance_runtime: target.project_instance_runtime_context()?,
                    requested_runtime: args.runtime,
                    continue_last_session: args.continue_last_session,
                })
                .await?;
            } else if should_use_run_tui(
                &args,
                std::io::stdin().is_terminal(),
                std::io::stdout().is_terminal(),
            ) {
                let outcome = run_console(RunConsoleInvocation {
                    target,
                    requested_runtime: args.runtime,
                    continue_last_session: args.continue_last_session,
                    timeout_override,
                })
                .await?;
                if outcome.launch_blocked {
                    return Ok(ExitCode::from(1));
                }
            } else {
                run_local(RunLocalInvocation {
                    home: &target.instance_home,
                    project_root: target.project_root.as_deref(),
                    work_root: target.require_work_root()?,
                    instance_name: target.instance_name.as_deref(),
                    project_instance_runtime: target.project_instance_runtime_context()?,
                    requested_runtime: args.runtime,
                    continue_last_session: args.continue_last_session,
                    timeout_override,
                })
                .await?;
            }
        }
        Command::Up(args) => {
            let manager = SystemdUserUnitManager;
            if args.all {
                validate_project_wide_target("up --all", &target_selection)?;
                let project_root = discover_project_root(&target_selection)?;
                let report =
                    operate_project_instances(&project_root, &manager, StackOperation::Up).await;
                print!("{}", report.render());
                if report.has_failures() {
                    return Ok(ExitCode::from(1));
                }
            } else {
                let target = resolved_target
                    .as_ref()
                    .ok_or_else(|| anyhow!("up requires a resolved LionClaw target"))?;
                let message = up_instance(
                    &target.instance_home,
                    &manager,
                    target.project_root.as_deref(),
                    target.require_work_root()?,
                )
                .await?;
                println!("{message}");
            }
        }
        Command::Down(args) => {
            let manager = SystemdUserUnitManager;
            if args.all {
                validate_project_wide_target("down --all", &target_selection)?;
                let project_root = discover_project_root(&target_selection)?;
                let report =
                    operate_project_instances(&project_root, &manager, StackOperation::Down).await;
                print!("{}", report.render());
                if report.has_failures() {
                    return Ok(ExitCode::from(1));
                }
            } else {
                let target = resolved_target
                    .as_ref()
                    .ok_or_else(|| anyhow!("down requires a resolved LionClaw target"))?;
                let message = down_instance(&target.instance_home, &manager).await?;
                println!("{message}");
            }
        }
        Command::Logs(args) => {
            let filter = parse_log_filter(&args)?;
            let options = LogOptions {
                follow: args.follow,
                tail: args.tail,
                since: args.since,
            };
            if args.all {
                validate_project_wide_target("logs --all", &target_selection)?;
                let project_root = discover_project_root(&target_selection)?;
                let components = project_log_components(&project_root, &filter).await?;
                if components.is_empty() {
                    println!("{}", no_managed_units_message(true));
                } else {
                    let mut stdout = std::io::stdout();
                    write_journal_logs(&components, &options, true, &mut stdout).await?;
                }
            } else {
                let target = resolved_target
                    .as_ref()
                    .ok_or_else(|| anyhow!("logs requires a resolved LionClaw target"))?;
                let components = selected_log_components(
                    &target.instance_home,
                    target.instance_name.as_deref(),
                    &filter,
                )
                .await?;
                if components.is_empty() {
                    println!("{}", no_managed_units_message(false));
                } else {
                    let mut stdout = std::io::stdout();
                    write_journal_logs(&components, &options, false, &mut stdout).await?;
                }
            }
        }
        Command::Status(args) => {
            if args.all {
                validate_status_all_target(&target_selection)?;
                let project_root = discover_project_root(&target_selection)?;
                let manager = SystemdUserUnitManager;
                let status =
                    render_project_status_all_with_manager(&project_root, &manager).await?;
                print!("{status}");
            } else {
                let target = resolved_target
                    .as_ref()
                    .ok_or_else(missing_target_for_status)?;
                let manager = SystemdUserUnitManager;
                let status = render_target_status_with_manager(target, &manager).await?;
                print!("{status}");
            }
        }
        Command::Doctor(args) => {
            if args.all {
                validate_project_wide_target("doctor --all", &target_selection)?;
            }
            return run_doctor_command(&target_selection, args.all).await;
        }
        Command::ProjectValidate(args) => {
            let issues = validate_project_managed_config(&home, &args).await?;
            if issues.is_empty() {
                println!("managed project state matches expected values");
            } else {
                for issue in issues {
                    println!("{issue}");
                }
                return Ok(ExitCode::from(PROJECT_VALIDATE_MISMATCH_EXIT));
            }
        }
        Command::Runtime { command } => match *command {
            RuntimeCommand::Add(args) => {
                let executable = normalize_runtime_command(&args.executable)?;
                let profile = build_runtime_profile(&args, executable)?;
                let mut config = OperatorConfig::load(&home).await?;
                config.upsert_runtime(args.id.clone(), profile);
                validate_runtime_availability(&config, &args.id)?;
                config.save(&home).await?;
                println!("configured runtime {}", args.id);
            }
            RuntimeCommand::Rm(args) => {
                let mut config = OperatorConfig::load(&home).await?;
                let removed = config.remove_runtime(&args.id);
                config.save(&home).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.id
                );
            }
            RuntimeCommand::Ls => {
                let config = OperatorConfig::load(&home).await?;
                if config.runtimes.is_empty() {
                    println!("no runtimes configured");
                } else {
                    for (id, profile) in &config.runtimes {
                        let marker = if config.defaults.runtime.as_deref() == Some(id.as_str()) {
                            "*"
                        } else {
                            " "
                        };
                        println!(
                            "{} {} kind={} command={} confinement={}",
                            marker,
                            id,
                            profile.kind(),
                            profile.executable(),
                            profile.confinement().backend().as_str()
                        );
                    }
                }
            }
            RuntimeCommand::Mount { command } => {
                let target = resolved_target.as_ref();
                let project_root = target.and_then(|target| target.project_root.as_deref());
                let work_root = target.and_then(|target| target.work_root.as_deref());
                let mount_context = RuntimeMountContext {
                    home: &home,
                    project_root,
                    work_root,
                };
                match command {
                    RuntimeMountCommand::List(args) => {
                        let config = OperatorConfig::load(&home).await?;
                        let mounts = configured_runtime_mounts(&config, &args.runtime_id)?;
                        if mounts.is_empty() {
                            println!("no mounts configured for runtime {}", args.runtime_id);
                        } else {
                            for mount in mounts {
                                println!(
                                    "target={} source={} access={}",
                                    mount.target,
                                    mount.source.display(),
                                    mount.access.as_str()
                                );
                            }
                        }
                        validate_configured_runtime_mounts(
                            mount_context.home,
                            mount_context.project_root,
                            mount_context.work_root,
                            &config,
                            &args.runtime_id,
                        )?;
                    }
                    RuntimeMountCommand::Add(args) => {
                        let mut config = OperatorConfig::load(&home).await?;
                        let access = match (args.read_only, args.read_write) {
                            (_, true) => MountAccess::ReadWrite,
                            _ => MountAccess::ReadOnly,
                        };
                        let mount = add_runtime_mount(
                            &mut config,
                            mount_context,
                            &args.runtime_id,
                            &args.target,
                            &args.source,
                            access,
                        )?;
                        config.save(&home).await?;
                        println!(
                            "mounted {} at {} ({}) for runtime {}",
                            mount.source.display(),
                            mount.target,
                            mount.access.as_str(),
                            args.runtime_id
                        );
                        print_runtime_state_change_note();
                    }
                    RuntimeMountCommand::Remove(args) => {
                        let mut config = OperatorConfig::load(&home).await?;
                        let mount =
                            remove_runtime_mount(&mut config, &args.runtime_id, &args.target)?;
                        config.save(&home).await?;
                        println!(
                            "removed mount {} ({}) from runtime {}",
                            mount.target,
                            mount.source.display(),
                            args.runtime_id
                        );
                        print_runtime_state_change_note();
                    }
                }
            }
            RuntimeCommand::SetDefault(args) => {
                let mut config = OperatorConfig::load(&home).await?;
                config.set_default_runtime(&args.id)?;
                config.save(&home).await?;
                println!("default runtime set to {}", args.id);
            }
        },
        Command::Skill { command } => match command {
            SkillCommand::Add(args) | SkillCommand::Install(args) => {
                let alias = args
                    .alias
                    .unwrap_or_else(|| derive_skill_alias(&args.source));
                add_skill(&home, alias.clone(), args.source, args.reference).await?;
                println!("installed skill {alias}");
                print_runtime_state_change_note();
            }
            SkillCommand::List => {
                let skills = list_installed_skills(&home).await?;
                print_installed_skills(&skills);
            }
            SkillCommand::Remove(args) | SkillCommand::Rm(args) => {
                let removed = remove_skill(&home, &args.alias).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.alias
                );
                if removed {
                    print_runtime_state_change_note();
                }
            }
            SkillCommand::WorkerPath(args) => {
                let worker =
                    resolve_installed_skill_worker_entrypoint(&home, &args.alias, None).await?;
                println!("{}", worker.display());
            }
        },
        Command::Channel { command } => match command {
            ChannelCommand::Add(args) => {
                let skill = args.skill.unwrap_or_else(|| args.id.clone());
                let launch_mode =
                    ChannelLaunchMode::from_str(&args.launch).map_err(anyhow::Error::msg)?;
                add_channel_with_contact(
                    &home,
                    args.id.clone(),
                    skill.clone(),
                    launch_mode,
                    args.required_env,
                    ChannelContactSetup {
                        enabled: args.contact,
                        conversation_ref: args.conversation_ref,
                        thread_ref: args.thread_ref,
                        instance_name: resolved_target
                            .as_ref()
                            .and_then(|target| target.instance_name.clone()),
                    },
                )
                .await?;
                println!(
                    "configured channel {} -> {} ({})",
                    args.id,
                    skill,
                    launch_mode.as_str()
                );
                print_runtime_state_change_note();
            }
            ChannelCommand::List(args) => {
                if args.all {
                    validate_channel_list_all_target(&target_selection)?;
                    let project_root =
                        resolve_existing_project_root(&target_selection).map_err(|_| {
                            anyhow!(
                                "channel list --all requires a LionClaw project; run from the project root or pass --project PATH"
                            )
                        })?;
                    let manager = SystemdUserUnitManager;
                    print!(
                        "{}",
                        render_project_channel_list(&project_root, &manager).await?
                    );
                } else {
                    let target = resolved_target.as_ref().ok_or_else(|| {
                        anyhow!("channel list requires a resolved LionClaw target")
                    })?;
                    let manager = SystemdUserUnitManager;
                    print!(
                        "{}",
                        render_instance_channel_list(
                            target.instance_name.as_deref().unwrap_or("direct-home"),
                            &target.instance_home,
                            &manager,
                        )
                        .await?
                    );
                }
            }
            ChannelCommand::Remove(args) | ChannelCommand::Rm(args) => {
                let manager = SystemdUserUnitManager;
                stop_owned_channel_unit(&home, &manager, &args.id).await?;
                let removed = remove_channel(&home, &args.id).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.id
                );
                if removed {
                    print_runtime_state_change_note();
                }
            }
            ChannelCommand::Attach(args) => {
                let manager = SystemdUserUnitManager;
                let work_root = required_command_work_root(&resolved_target, "channel attach")?;
                attach_channel(
                    &home,
                    &manager,
                    resolved_target
                        .as_ref()
                        .and_then(|target| target.project_root.as_deref()),
                    work_root,
                    args.id,
                    args.peer,
                    args.runtime,
                )
                .await?;
            }
            ChannelCommand::Pairing { command } => match command {
                ChannelPairingCommand::List(args) => {
                    let status = args
                        .status
                        .as_deref()
                        .map(ChannelPairingStatus::from_str)
                        .transpose()
                        .map_err(anyhow::Error::msg)?;
                    let response = pairing_list(&home, args.channel_id, status).await?;
                    print!("{}", render_channel_pairing_list(&response));
                }
                ChannelPairingCommand::Invite(args) => {
                    let requested_profile =
                        ChannelRoutingProfile::from_str(&args.scope).map_err(anyhow::Error::msg)?;
                    let expires_in_ms = args
                        .expires_in
                        .map(|duration| {
                            u64::try_from(duration.as_millis())
                                .context("expires-in is too large to represent")
                        })
                        .transpose()?;
                    let invite = pairing_invite(
                        &home,
                        ChannelPairingInviteRequest {
                            channel_id: args.channel_id,
                            requested_profile,
                            label: args.label,
                            conversation_ref: args.conversation_ref,
                            thread_ref: args.thread_ref,
                            expires_in_ms,
                            max_claims: args.max_claims,
                            operator_actor: None,
                        },
                    )
                    .await?;
                    println!("Pairing token: {}", invite.token);
                    println!("Expires: {}", invite.expires_at.to_rfc3339());
                    println!("Claims: {}", invite.max_claims);
                }
                ChannelPairingCommand::Approve(args) => {
                    let trust_tier =
                        TrustTier::from_str(&args.trust_tier).map_err(anyhow::Error::msg)?;
                    let routing_profile = args
                        .routing_profile
                        .as_deref()
                        .map(ChannelRoutingProfile::from_str)
                        .transpose()
                        .map_err(anyhow::Error::msg)?;
                    let grant = pairing_approve(
                        &home,
                        args.channel_id,
                        args.pairing,
                        routing_profile,
                        trust_tier,
                        args.label,
                    )
                    .await?;
                    println!(
                        "approved channel={} grant={} profile={} trust={}",
                        grant.grant.channel_id,
                        grant.grant.grant_id,
                        grant.grant.routing_profile.as_str(),
                        grant.grant.trust_tier.as_str()
                    );
                }
                ChannelPairingCommand::Block(args) => {
                    let channel_id = args.channel_id;
                    let blocked = pairing_block(
                        &home,
                        channel_id.clone(),
                        args.sender_ref,
                        args.pairing_id,
                        args.conversation_ref,
                        args.thread_ref,
                        args.reason,
                    )
                    .await?;
                    if let Some(grant) = blocked.grant {
                        println!(
                            "blocked channel={} grant={} profile={} pairings={}",
                            grant.channel_id,
                            grant.grant_id,
                            grant.routing_profile.as_str(),
                            blocked.blocked_pairing_ids.len()
                        );
                    } else {
                        println!(
                            "blocked channel={} pairings={}",
                            channel_id,
                            blocked.blocked_pairing_ids.len()
                        );
                    }
                }
                ChannelPairingCommand::Revoke(args) => {
                    let revoked =
                        pairing_revoke(&home, args.channel_id, args.grant_id, args.reason).await?;
                    println!(
                        "revoked grant={} revoked={}",
                        revoked.grant_id, revoked.revoked
                    );
                }
            },
        },
        Command::Continuity { command } => {
            let config = OperatorConfig::load(&home).await?;
            match command {
                ContinuityCommand::Drafts { command } => {
                    let work_root =
                        required_command_work_root(&resolved_target, "continuity drafts")?;
                    let project_instance_runtime = required_project_instance_runtime_context(
                        &resolved_target,
                        "continuity drafts",
                    )?;
                    let kernel = open_runtime_kernel_for_work_root(
                        &home,
                        &config,
                        None,
                        work_root,
                        project_instance_runtime,
                        None,
                    )
                    .await?;
                    match command {
                        ContinuityDraftCommand::Ls(args) => {
                            let response = kernel
                                .list_continuity_drafts(ContinuityDraftListRequest {
                                    runtime_id: args.runtime,
                                })
                                .await?;
                            println!("runtime={}", response.runtime_id);
                            for draft in response.drafts {
                                println!(
                                    "path={} size={} media_type={}",
                                    draft.relative_path, draft.size_bytes, draft.media_type
                                );
                            }
                        }
                        ContinuityDraftCommand::Promote(args) => {
                            let response = kernel
                                .promote_continuity_draft_with_actor(
                                    ContinuityDraftActionRequest {
                                        runtime_id: args.runtime,
                                        relative_path: args.relative_path,
                                    },
                                    "operator",
                                )
                                .await?;
                            println!(
                                "promoted runtime={} draft={} artifact={}",
                                response.runtime_id, response.draft_path, response.artifact_path
                            );
                        }
                        ContinuityDraftCommand::Discard(args) => {
                            let response = kernel
                                .discard_continuity_draft_with_actor(
                                    ContinuityDraftActionRequest {
                                        runtime_id: args.runtime,
                                        relative_path: args.relative_path,
                                    },
                                    "operator",
                                )
                                .await?;
                            println!(
                                "discarded runtime={} draft={}",
                                response.runtime_id, response.draft_path
                            );
                        }
                    }
                }
                other => {
                    let kernel = open_kernel(&home, &config, None).await?;
                    match other {
                        ContinuityCommand::Status => {
                            let status = kernel.continuity_status().await?;
                            println!("memory={}", status.memory_path);
                            println!("active={}", status.active_path);
                            println!(
                                "latest_daily={}",
                                status.latest_daily_path.unwrap_or_else(|| "-".to_string())
                            );
                            println!("open_loops={}", status.open_loops.len());
                            println!("memory_proposals={}", status.memory_proposals.len());
                            println!("recent_artifacts={}", status.recent_artifacts.len());
                        }
                        ContinuityCommand::Search(args) => {
                            let response = kernel
                                .continuity_search(ContinuitySearchRequest {
                                    query: args.query,
                                    limit: Some(args.limit),
                                })
                                .await?;
                            for item in response.matches {
                                println!(
                                    "path={} title={} snippet={}",
                                    item.relative_path, item.title, item.snippet
                                );
                            }
                        }
                        ContinuityCommand::Get(args) => {
                            let response = kernel
                                .continuity_get(ContinuityPathRequest {
                                    relative_path: args.relative_path,
                                })
                                .await?;
                            print!("{}", response.content);
                        }
                        ContinuityCommand::Loops { command } => match command {
                            ContinuityLoopCommand::Ls => {
                                for open_loop in kernel.list_continuity_open_loops().await?.loops {
                                    println!(
                                        "path={} title={} next_step={}",
                                        open_loop.relative_path,
                                        open_loop.title,
                                        open_loop.next_step.unwrap_or_else(|| "-".to_string())
                                    );
                                }
                            }
                            ContinuityLoopCommand::Resolve(args) => {
                                let response = kernel
                                    .resolve_continuity_open_loop_with_actor(
                                        ContinuityPathRequest {
                                            relative_path: args.relative_path,
                                        },
                                        "operator",
                                    )
                                    .await?;
                                println!("resolved {}", response.archived_path);
                            }
                        },
                        ContinuityCommand::Proposals { command } => match command {
                            ContinuityProposalCommand::Ls => {
                                for proposal in
                                    kernel.list_continuity_memory_proposals().await?.proposals
                                {
                                    println!(
                                        "path={} title={} entries={}",
                                        proposal.relative_path,
                                        proposal.title,
                                        proposal.entries.len()
                                    );
                                }
                            }
                            ContinuityProposalCommand::Merge(args) => {
                                let response = kernel
                                    .merge_continuity_memory_proposal_with_actor(
                                        ContinuityPathRequest {
                                            relative_path: args.relative_path,
                                        },
                                        "operator",
                                    )
                                    .await?;
                                println!(
                                    "merged {} into {}",
                                    response.archived_path,
                                    response
                                        .memory_path
                                        .unwrap_or_else(|| "MEMORY.md".to_string())
                                );
                            }
                            ContinuityProposalCommand::Reject(args) => {
                                let response = kernel
                                    .reject_continuity_memory_proposal_with_actor(
                                        ContinuityPathRequest {
                                            relative_path: args.relative_path,
                                        },
                                        "operator",
                                    )
                                    .await?;
                                println!("rejected {}", response.archived_path);
                            }
                        },
                        ContinuityCommand::Drafts { .. } => unreachable!(),
                    }
                }
            }
        }
        Command::Job { command } => {
            let config = OperatorConfig::load(&home).await?;
            match command {
                JobCommand::Run(args) => {
                    let work_root = required_command_work_root(&resolved_target, "job run")?;
                    let project_instance_runtime =
                        required_project_instance_runtime_context(&resolved_target, "job run")?;
                    let kernel = open_runtime_kernel_for_work_root(
                        &home,
                        &config,
                        None,
                        work_root,
                        project_instance_runtime,
                        None,
                    )
                    .await?;
                    let job_id = parse_job_id(&args.job_id)?;
                    let response = kernel.run_job_now(JobRefRequest { job_id }).await?;
                    println!(
                        "ran {} run={} status={:?}",
                        response.job.job_id, response.run.run_id, response.run.status
                    );
                }
                JobCommand::Tick => {
                    let work_root = required_command_work_root(&resolved_target, "job tick")?;
                    let project_instance_runtime =
                        required_project_instance_runtime_context(&resolved_target, "job tick")?;
                    let kernel = open_runtime_kernel_for_work_root(
                        &home,
                        &config,
                        None,
                        work_root,
                        project_instance_runtime,
                        None,
                    )
                    .await?;
                    let response = kernel.scheduler_tick().await?;
                    println!("claimed {} scheduled runs", response.claimed_runs);
                }
                other => match other {
                    JobCommand::Add(args) => {
                        let args = *args;
                        let runtime_id = resolve_runtime_id(&config, args.runtime.as_deref())?;
                        let kernel = open_kernel(&home, &config, None).await?;
                        let prompt_text = load_job_prompt(args.prompt, args.prompt_file).await?;
                        let schedule = parse_job_schedule_spec(&args.schedule, args.tz.as_deref())?;
                        let has_delivery_detail =
                            args.deliver_thread.is_some() || args.deliver_reply_to.is_some();
                        let delivery = match (args.deliver_channel, args.deliver_conversation) {
                            (None, None) if !has_delivery_detail => None,
                            (Some(channel_id), Some(conversation_ref)) => {
                                Some(crate::contracts::JobDeliveryTargetDto {
                                    channel_id,
                                    conversation_ref,
                                    thread_ref: args.deliver_thread,
                                    reply_to_ref: args.deliver_reply_to,
                                })
                            }
                            _ => {
                                return Err(anyhow!(
                                        "--deliver-channel and --deliver-conversation are required for delivery"
                                    ));
                            }
                        };
                        let response = kernel
                            .create_job(JobCreateRequest {
                                name: args.name,
                                runtime_id,
                                schedule,
                                prompt_text,
                                allow_capabilities: args.allow_capabilities,
                                delivery,
                                retry_attempts: args.retry_attempts,
                            })
                            .await?;
                        println!(
                            "created job {} next_run_at={}",
                            response.job.job_id,
                            response
                                .job
                                .next_run_at
                                .map(|value| value.to_rfc3339())
                                .unwrap_or_else(|| "-".to_string())
                        );
                    }
                    JobCommand::Ls => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        for job in kernel.list_jobs().await?.jobs {
                            println!(
                                "job={} enabled={} runtime={} next_run_at={} status={}",
                                job.job_id,
                                job.enabled,
                                job.runtime_id,
                                job.next_run_at
                                    .map(|value| value.to_rfc3339())
                                    .unwrap_or_else(|| "-".to_string()),
                                job.last_status
                                    .map(|value| value.as_str().to_string())
                                    .unwrap_or_else(|| "-".to_string()),
                            );
                        }
                    }
                    JobCommand::Show(args) => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        let job_id = parse_job_id(&args.job_id)?;
                        let job = kernel.get_job(job_id).await?.job;
                        println!("job={}", job.job_id);
                        println!("name={}", job.name);
                        println!("enabled={}", job.enabled);
                        println!("runtime={}", job.runtime_id);
                        println!(
                            "next_run_at={}",
                            job.next_run_at
                                .map(|value| value.to_rfc3339())
                                .unwrap_or_else(|| "-".to_string())
                        );
                        println!(
                            "delivery={}",
                            job.delivery
                                .map(format_job_delivery)
                                .unwrap_or_else(|| "-".to_string())
                        );
                    }
                    JobCommand::Pause(args) => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        let job_id = parse_job_id(&args.job_id)?;
                        let response = kernel.pause_job(job_id).await?;
                        println!("paused {}", response.job.job_id);
                    }
                    JobCommand::Resume(args) => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        let job_id = parse_job_id(&args.job_id)?;
                        let response = kernel.resume_job(job_id).await?;
                        println!(
                            "resumed {} next_run_at={}",
                            response.job.job_id,
                            response
                                .job
                                .next_run_at
                                .map(|value| value.to_rfc3339())
                                .unwrap_or_else(|| "-".to_string())
                        );
                    }
                    JobCommand::Rm(args) => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        let job_id = parse_job_id(&args.job_id)?;
                        let response = kernel.remove_job(job_id).await?;
                        println!(
                            "{} {}",
                            if response.removed {
                                "removed"
                            } else {
                                "left unchanged"
                            },
                            response.job_id
                        );
                    }
                    JobCommand::Runs(args) => {
                        let kernel = open_kernel(&home, &config, None).await?;
                        let job_id = parse_job_id(&args.job_id)?;
                        for run in kernel
                            .list_job_runs(JobRunsRequest {
                                job_id,
                                limit: Some(args.limit),
                            })
                            .await?
                            .runs
                        {
                            println!(
                                    "run={} attempt={} trigger={:?} status={:?} started_at={} session={} turn={} error={}",
                                    run.run_id,
                                    run.attempt_no,
                                    run.trigger_kind,
                                    run.status,
                                    run.started_at.to_rfc3339(),
                                    run.session_id
                                        .map(|value| value.to_string())
                                        .unwrap_or_else(|| "-".to_string()),
                                    run.turn_id
                                        .map(|value| value.to_string())
                                        .unwrap_or_else(|| "-".to_string()),
                                    run.error_text.unwrap_or_else(|| "-".to_string()),
                                );
                        }
                    }
                    JobCommand::Run(_) | JobCommand::Tick => unreachable!(),
                },
            }
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn should_use_run_tui(args: &RunArgs, stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
    !args.plain && !args.runtime_tui && stdin_is_terminal && stdout_is_terminal
}

fn empty_project_for_tui_launch_blocker(selection: &TargetSelection) -> Option<PathBuf> {
    if selection.home.is_some() {
        return None;
    }
    let project_root = discover_project_root(selection).ok()?;
    let instances = list_project_instance_statuses(&project_root).ok()?;
    instances.is_empty().then_some(project_root)
}

fn resolve_command_target(
    selection: &TargetSelection,
    command: &Command,
) -> Result<Option<crate::operator::target::TargetContext>> {
    let requirement = match command {
        Command::Configure(_) => Some(WorkRootRequirement::Optional),
        Command::Connect(_) => Some(WorkRootRequirement::Required),
        Command::Run(_) => Some(WorkRootRequirement::Required),
        Command::Up(args) => {
            if args.all {
                None
            } else {
                Some(WorkRootRequirement::Required)
            }
        }
        Command::Down(args) => {
            if args.all {
                None
            } else {
                Some(WorkRootRequirement::Optional)
            }
        }
        Command::Logs(args) => {
            if args.all {
                None
            } else {
                Some(WorkRootRequirement::Optional)
            }
        }
        Command::Status(args) => {
            if args.all {
                None
            } else {
                Some(WorkRootRequirement::Optional)
            }
        }
        Command::Doctor(_) => None,
        Command::Channel { command } => match command {
            ChannelCommand::Attach(_) => Some(WorkRootRequirement::Required),
            ChannelCommand::List(args) if args.all => None,
            ChannelCommand::Add(_)
            | ChannelCommand::List(_)
            | ChannelCommand::Remove(_)
            | ChannelCommand::Rm(_)
            | ChannelCommand::Pairing { .. } => Some(WorkRootRequirement::Optional),
        },
        Command::Continuity { command } => match command {
            ContinuityCommand::Drafts { .. } => Some(WorkRootRequirement::Required),
            ContinuityCommand::Status
            | ContinuityCommand::Search(_)
            | ContinuityCommand::Get(_)
            | ContinuityCommand::Loops { .. }
            | ContinuityCommand::Proposals { .. } => Some(WorkRootRequirement::Optional),
        },
        Command::Job { command } => match command {
            JobCommand::Run(_) | JobCommand::Tick => Some(WorkRootRequirement::Required),
            JobCommand::Add(_)
            | JobCommand::Ls
            | JobCommand::Show(_)
            | JobCommand::Pause(_)
            | JobCommand::Resume(_)
            | JobCommand::Rm(_)
            | JobCommand::Runs(_) => Some(WorkRootRequirement::Optional),
        },
        Command::Runtime { command } => match command.as_ref() {
            RuntimeCommand::Mount {
                command: RuntimeMountCommand::Add(_) | RuntimeMountCommand::List(_),
            } => Some(WorkRootRequirement::Required),
            RuntimeCommand::Add(_)
            | RuntimeCommand::Rm(_)
            | RuntimeCommand::Ls
            | RuntimeCommand::Mount {
                command: RuntimeMountCommand::Remove(_),
            }
            | RuntimeCommand::SetDefault(_) => Some(WorkRootRequirement::Optional),
        },
        Command::Skill { .. } => Some(WorkRootRequirement::Optional),
        Command::Project { .. } | Command::Instance { .. } | Command::ProjectValidate(_) => None,
    };

    requirement
        .map(|requirement| resolve_target(selection, requirement))
        .transpose()
}

fn required_command_work_root<'a>(
    resolved_target: &'a Option<TargetContext>,
    command: &str,
) -> Result<&'a Path> {
    let target = resolved_target
        .as_ref()
        .ok_or_else(|| anyhow!("{command} requires a resolved LionClaw target"))?;
    target.require_work_root()
}

fn required_project_instance_runtime_context(
    resolved_target: &Option<TargetContext>,
    command: &str,
) -> Result<Option<ProjectInstanceRuntimeContext>> {
    let target = resolved_target
        .as_ref()
        .ok_or_else(|| anyhow!("{command} requires a resolved LionClaw target"))?;
    target.project_instance_runtime_context()
}

fn validate_project_wide_target(command: &str, selection: &TargetSelection) -> Result<()> {
    if selection.home.is_some() {
        bail!("{command} requires a project context and cannot be combined with --home");
    }
    if selection.instance.is_some() {
        bail!("{command} is project-wide and cannot be combined with --instance");
    }
    Ok(())
}

fn parse_log_filter(args: &ProductLogsArgs) -> Result<LogFilter> {
    let selected =
        usize::from(args.daemon) + usize::from(args.workers) + usize::from(args.worker.is_some());
    if selected > 1 {
        bail!("logs accepts only one of --daemon, --workers, or --worker");
    }
    if args.daemon {
        Ok(LogFilter::Daemon)
    } else if args.workers {
        Ok(LogFilter::Workers)
    } else if let Some(worker) = args.worker.as_deref() {
        Ok(LogFilter::Worker(worker.to_string()))
    } else {
        Ok(LogFilter::Default)
    }
}

async fn run_doctor_command(selection: &TargetSelection, all: bool) -> Result<ExitCode> {
    let manager = SystemdUserUnitManager;
    match run_doctor(selection, all, &manager).await {
        Ok(report) => {
            print!("{}", report.render());
            if report.has_errors() {
                Ok(ExitCode::from(1))
            } else {
                Ok(ExitCode::SUCCESS)
            }
        }
        Err(err) => {
            eprintln!("doctor: {err:#}");
            Ok(ExitCode::from(2))
        }
    }
}

fn print_installed_skills(skills: &[InstalledSkill]) {
    if skills.is_empty() {
        println!("no skills installed");
        return;
    }
    println!("skill       source                         reference  status");
    for skill in skills {
        println!(
            "{:<11} {:<30} {:<10} {}",
            skill.alias,
            skill.source.as_deref().unwrap_or("-"),
            skill.reference.as_deref().unwrap_or("-"),
            skill.status.as_str()
        );
    }
}

async fn configure_runtime_with_io<R: BufRead, W: Write>(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    interactive: bool,
    input: &mut R,
    output: &mut W,
) -> Result<ConfigureRuntimeOutcome> {
    configure_runtime_with_io_and_engine_resolver(
        home,
        requested_runtime,
        interactive,
        input,
        output,
        normalize_podman_executable,
    )
    .await
}

async fn configure_runtime_with_io_and_engine_resolver<R: BufRead, W: Write, F>(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    interactive: bool,
    input: &mut R,
    output: &mut W,
    resolve_engine: F,
) -> Result<ConfigureRuntimeOutcome>
where
    F: FnMut(&str) -> Result<String>,
{
    let runtime_id = match requested_runtime
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(runtime_id) => runtime_id.to_string(),
        None if interactive => prompt_configure_runtime(input, output)?,
        None => return Err(anyhow!(configure_runtime_required_message())),
    };

    let mut config = OperatorConfig::load(home).await?;
    let outcome =
        configure_runtime_profile_with_engine_resolver(&mut config, &runtime_id, resolve_engine)?;
    config.save(home).await?;
    Ok(outcome)
}

fn prompt_configure_runtime<R: BufRead, W: Write>(input: &mut R, output: &mut W) -> Result<String> {
    writeln!(output, "Supported runtimes: codex")?;
    write!(output, "Runtime to configure: ")?;
    output.flush()?;

    let mut line = String::new();
    input.read_line(&mut line)?;
    let runtime_id = line.trim();
    if runtime_id.is_empty() {
        return Err(anyhow!(configure_runtime_required_message()));
    }
    Ok(runtime_id.to_string())
}

fn print_configure_outcome(home: &LionClawHome, outcome: &ConfigureRuntimeOutcome) {
    let profile_state = if outcome.created_profile {
        "created"
    } else {
        "preserved"
    };
    println!(
        "configured runtime {} for {} ({profile_state} profile)",
        outcome.runtime_id,
        home.root().display()
    );
    println!("default runtime set to {}", outcome.runtime_id);
}

const CONNECT_LINK_HEALTH_ATTEMPTS: usize = 80;
const CONNECT_LINK_HEALTH_DELAY: Duration = Duration::from_millis(250);
const CONNECT_LINK_EXPIRES_IN_MS: u64 = 15 * 60 * 1000;
const PAIRING_URL_TOKEN_PLACEHOLDER: &str = "{token}";

async fn print_connect_outcome(
    home: &LionClawHome,
    outcome: &ConnectOutcome,
    health_not_before: DateTime<Utc>,
) -> Result<()> {
    println!(
        "connected channel {} using skill {} ({})",
        outcome.channel_id,
        outcome.skill_alias,
        outcome.launch.as_str()
    );
    match outcome.launch {
        ChannelLaunchMode::Interactive => {
            println!("interactive channel worker exited");
        }
        ChannelLaunchMode::Background => {
            println!("background channel worker is running");
            if let Some(link) =
                create_direct_pairing_link(home, &outcome.channel_id, health_not_before).await
            {
                println!("start and connect with this one-use link:");
                println!("  {link}");
                println!("treat it like an access link; it expires in 15 minutes");
            }
            println!("pair or approve peers with:");
            println!(
                "  {}",
                channel_pairing_list_command(home, &outcome.channel_id)
            );
        }
    }
    Ok(())
}

async fn create_direct_pairing_link(
    home: &LionClawHome,
    channel_id: &str,
    health_not_before: DateTime<Utc>,
) -> Option<String> {
    let template = wait_for_pairing_url_template(home, channel_id, health_not_before).await?;
    let invite = pairing_invite(
        home,
        ChannelPairingInviteRequest {
            channel_id: channel_id.to_string(),
            requested_profile: ChannelRoutingProfile::Direct,
            label: Some("Direct channel link".to_string()),
            conversation_ref: None,
            thread_ref: None,
            expires_in_ms: Some(CONNECT_LINK_EXPIRES_IN_MS),
            max_claims: Some(1),
            operator_actor: None,
        },
    )
    .await
    .ok()?;
    render_pairing_url(&template, &invite.token)
}

async fn wait_for_pairing_url_template(
    home: &LionClawHome,
    channel_id: &str,
    health_not_before: DateTime<Utc>,
) -> Option<String> {
    let config = OperatorConfig::load(home).await.ok()?;
    let kernel = open_kernel(home, &config, None).await.ok()?;
    for attempt in 0..CONNECT_LINK_HEALTH_ATTEMPTS {
        if let Ok(health) = kernel.get_channel_health(channel_id).await {
            if let Some(report) = health.latest_report {
                if report.observed_at >= health_not_before {
                    if let Some(template) = pairing_url_template(&report.checks) {
                        return Some(template);
                    }
                }
            }
        }
        if attempt + 1 < CONNECT_LINK_HEALTH_ATTEMPTS {
            tokio::time::sleep(CONNECT_LINK_HEALTH_DELAY).await;
        }
    }
    None
}

fn pairing_url_template(checks: &[ChannelHealthCheck]) -> Option<String> {
    checks
        .iter()
        .find_map(|check| safe_pairing_url_template(&check.details))
}

fn safe_pairing_url_template(details: &serde_json::Value) -> Option<String> {
    let template = details.get("pairing_url_template")?.as_str()?.trim();
    if !safe_public_https_url(template) {
        return None;
    }
    if !template.contains(PAIRING_URL_TOKEN_PLACEHOLDER) {
        return None;
    }
    Some(template.to_string())
}

fn render_pairing_url(template: &str, token: &str) -> Option<String> {
    if token.is_empty() || token.chars().any(char::is_control) {
        return None;
    }
    let rendered = template.replace(PAIRING_URL_TOKEN_PLACEHOLDER, token);
    safe_public_https_url(&rendered).then_some(rendered)
}

fn safe_public_https_url(url: &str) -> bool {
    !url.is_empty()
        && url.len() <= 2048
        && url.starts_with("https://")
        && !url.chars().any(char::is_control)
}

fn channel_pairing_list_command(home: &LionClawHome, channel_id: &str) -> String {
    format!(
        "{} channel pairing list --channel-id {}",
        lionclaw_home_command_prefix(home),
        shell_quote_arg(channel_id)
    )
}

fn render_channel_pairing_list(response: &ChannelPairingListResponse) -> String {
    let mut lines = response
        .pairings
        .iter()
        .map(render_channel_pairing)
        .collect::<Vec<_>>();
    lines.extend(response.grants.iter().map(render_channel_grant));

    if lines.is_empty() {
        String::new()
    } else {
        let mut output = lines.join("\n");
        output.push('\n');
        output
    }
}

fn render_channel_pairing(pairing: &ChannelPairingView) -> String {
    format!(
        "channel={} pairing={} status={} profile={} sender={} conversation={} thread={} label={}",
        pairing.channel_id,
        pairing.pairing_id,
        pairing.status.as_str(),
        pairing.requested_profile.as_str(),
        optional_value(pairing.sender_ref.as_deref()),
        optional_value(pairing.conversation_ref.as_deref()),
        optional_value(pairing.thread_ref.as_deref()),
        optional_value(pairing.label.as_deref()),
    )
}

fn render_channel_grant(grant: &ChannelGrantView) -> String {
    format!(
        "channel={} grant={} status={} profile={} trust={} sender={} conversation={} thread={} label={}",
        grant.channel_id,
        grant.grant_id,
        grant.status,
        grant.routing_profile.as_str(),
        grant.trust_tier.as_str(),
        optional_value(grant.sender_ref.as_deref()),
        optional_value(grant.conversation_ref.as_deref()),
        optional_value(grant.thread_ref.as_deref()),
        optional_value(grant.label.as_deref()),
    )
}

fn optional_value(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

fn validate_channel_list_all_target(selection: &TargetSelection) -> Result<()> {
    if selection.home.is_some() {
        return Err(anyhow!(
            "channel list --all requires a project context; run from the project root or pass --project PATH"
        ));
    }
    if selection.instance.is_some() {
        return Err(anyhow!(
            "channel list --all is project-wide and cannot be combined with --instance"
        ));
    }
    Ok(())
}

async fn render_project_channel_list<M: UnitManager>(
    project_root: &Path,
    manager: &M,
) -> Result<String> {
    let mut output = String::new();
    output.push_str(&format!("project: {}\n", project_root.display()));
    for entry in list_project_instance_statuses(project_root)? {
        output.push('\n');
        output.push_str(
            &render_instance_channel_list(&entry.name, &LionClawHome::new(entry.home), manager)
                .await?,
        );
    }
    Ok(output)
}

async fn render_instance_channel_list<M: UnitManager>(
    instance_name: &str,
    home: &LionClawHome,
    manager: &M,
) -> Result<String> {
    let config = OperatorConfig::load(home).await?;
    let mut output = String::new();
    output.push_str(&format!("instance: {instance_name}\n"));
    output.push_str("channel   launch       skill      unit\n");
    if config.channels.is_empty() {
        output.push_str("(none)\n");
        return Ok(output);
    }
    let owned_units = manager.owned_units(home)?;
    for channel in config.channels {
        let unit = match channel.launch_mode {
            ChannelLaunchMode::Interactive => "n/a".to_string(),
            ChannelLaunchMode::Background => match owned_units.channel(&channel.id) {
                Some(unit) => manager.unit_status(unit).await?,
                None => "not-installed".to_string(),
            },
        };
        output.push_str(&format!(
            "{:<9} {:<12} {:<10} {}\n",
            channel.id,
            channel.launch_mode.as_str(),
            channel.skill,
            unit
        ));
    }
    Ok(output)
}

async fn stop_owned_channel_unit<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: &str,
) -> Result<()> {
    let Some(unit) = manager
        .owned_units(home)?
        .channel(channel_id)
        .map(str::to_string)
    else {
        return Ok(());
    };
    manager.down_units(&[unit]).await
}

fn print_runtime_state_change_note() {
    println!(
        "direct runs pick this up on the next launch; rerun 'lionclaw connect <channel>' or 'lionclaw up' if a managed daemon is already running"
    );
}

#[derive(Debug, serde::Deserialize)]
struct ProjectSkillInstallMetadata {
    #[serde(default)]
    source: String,
}

async fn validate_project_managed_config(
    home: &LionClawHome,
    args: &ProjectValidateArgs,
) -> Result<Vec<String>> {
    let config = OperatorConfig::load(home).await?;
    let expected_launch_mode =
        ChannelLaunchMode::from_str(&args.launch_mode).map_err(anyhow::Error::msg)?;
    let mut issues = Vec::new();

    if let Some(runtime) = config.runtime(&args.runtime_id) {
        if runtime.kind() != args.runtime_kind {
            issues.push(format!(
                "runtime {:?} has kind={:?}; expected {:?}",
                args.runtime_id,
                runtime.kind(),
                args.runtime_kind
            ));
        }
        if runtime.executable() != args.runtime_bin {
            issues.push(format!(
                "runtime {:?} has executable={:?}; expected {:?}",
                args.runtime_id,
                runtime.executable(),
                args.runtime_bin
            ));
        }

        let ConfinementConfig::Oci(confinement) = runtime.confinement();
        if let Some(expected_engine) = args
            .podman_bin
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if confinement.engine != expected_engine {
                issues.push(format!(
                    "runtime {:?} confinement.engine={:?}; expected {:?}",
                    args.runtime_id, confinement.engine, expected_engine
                ));
            }
        }
        if confinement.image.as_deref() != Some(args.runtime_image.as_str()) {
            issues.push(format!(
                "runtime {:?} confinement.image={:?}; expected {:?}",
                args.runtime_id, confinement.image, args.runtime_image
            ));
        }
    }

    if let Some(actual_default) = config.defaults.runtime.as_deref() {
        if actual_default != args.runtime_id {
            issues.push(format!(
                "default runtime is {:?}; expected {:?}",
                actual_default, args.runtime_id
            ));
        }
    }

    if let Some(channel) = config
        .channels
        .iter()
        .find(|channel| channel.id == args.channel_id)
    {
        if channel.skill != args.channel_alias {
            issues.push(format!(
                "channel {:?} has skill={:?}; expected {:?}",
                args.channel_id, channel.skill, args.channel_alias
            ));
        }
        if channel.launch_mode != expected_launch_mode {
            issues.push(format!(
                "channel {:?} has launch_mode={:?}; expected {:?}",
                args.channel_id,
                channel.launch_mode.as_str(),
                expected_launch_mode.as_str()
            ));
        }
    }

    let mut expected_skills = BTreeMap::new();
    expected_skills.insert(
        args.channel_alias.clone(),
        format!("local:{}", args.channel_source),
    );
    for spec in &args.project_skills {
        let (alias, source) = parse_project_skill_spec(spec)?;
        expected_skills.insert(alias, source);
    }

    for (alias, expected_source) in expected_skills {
        if let Some(actual_source) = installed_skill_source(home, &alias).await? {
            if actual_source != expected_source {
                issues.push(format!(
                    "skill {alias:?} has source={actual_source:?}; expected {expected_source:?}"
                ));
            }
        }
    }

    Ok(issues)
}

fn parse_project_skill_spec(raw: &str) -> Result<(String, String)> {
    let (alias, source) = raw
        .split_once('=')
        .ok_or_else(|| anyhow!("invalid --project-skill '{raw}'; expected alias=path"))?;
    let alias = alias.trim();
    let source = source.trim();
    if alias.is_empty() || source.is_empty() {
        return Err(anyhow!(
            "invalid --project-skill '{raw}'; expected alias=path"
        ));
    }
    Ok((alias.to_string(), format!("local:{source}")))
}

async fn installed_skill_source(home: &LionClawHome, alias: &str) -> Result<Option<String>> {
    let metadata_path = home
        .skills_dir()
        .join(alias)
        .join(SKILL_INSTALL_METADATA_FILE);
    if !tokio::fs::try_exists(&metadata_path)
        .await
        .with_context(|| format!("failed to stat {}", metadata_path.display()))?
    {
        return Ok(None);
    }

    let content = tokio::fs::read_to_string(&metadata_path)
        .await
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let metadata: ProjectSkillInstallMetadata = toml::from_str(&content)
        .with_context(|| format!("failed to parse {}", metadata_path.display()))?;
    Ok(Some(metadata.source))
}

fn parse_runtime_timeout(raw: &str) -> std::result::Result<Duration, String> {
    parse_duration(raw)
}

async fn load_job_prompt(prompt: Option<String>, prompt_file: Option<String>) -> Result<String> {
    match (prompt, prompt_file) {
        (Some(text), None) => {
            let trimmed = text.trim().to_string();
            if trimmed.is_empty() {
                return Err(anyhow!("--prompt cannot be empty"));
            }
            Ok(trimmed)
        }
        (None, Some(path)) => {
            let content = tokio::fs::read_to_string(&path).await?;
            let trimmed = content.trim().to_string();
            if trimmed.is_empty() {
                return Err(anyhow!("--prompt-file cannot be empty"));
            }
            Ok(trimmed)
        }
        (Some(_), Some(_)) => Err(anyhow!("use either --prompt or --prompt-file, not both")),
        (None, None) => Err(anyhow!("either --prompt or --prompt-file is required")),
    }
}

fn parse_job_id(raw: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(raw.trim()).map_err(|_| anyhow!("invalid job id '{raw}'"))
}

fn format_job_delivery(delivery: crate::contracts::JobDeliveryTargetDto) -> String {
    let mut parts = vec![delivery.channel_id, delivery.conversation_ref];
    if let Some(thread_ref) = delivery.thread_ref {
        parts.push(thread_ref);
    }
    if let Some(reply_to_ref) = delivery.reply_to_ref {
        parts.push(format!("reply_to={reply_to_ref}"));
    }
    parts.join(":")
}

fn parse_job_schedule_spec(raw: &str, timezone: Option<&str>) -> Result<JobScheduleDto> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(anyhow!("--schedule requires a value"));
    }

    if let Some(rest) = value.strip_prefix("every ") {
        let every_ms = parse_job_duration_ms(rest)?;
        return Ok(JobScheduleDto::Interval {
            every_ms,
            anchor_ms: Utc::now().timestamp_millis(),
        });
    }

    if let Ok(delay_ms) = parse_job_duration_ms(value) {
        let run_at =
            Utc::now() + ChronoDuration::milliseconds(i64::try_from(delay_ms).unwrap_or(i64::MAX));
        return Ok(JobScheduleDto::Once { run_at });
    }

    if value.contains('T') || value.chars().take(4).all(|ch| ch.is_ascii_digit()) {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
            return Ok(JobScheduleDto::Once {
                run_at: parsed.with_timezone(&Utc),
            });
        }
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
            return Ok(JobScheduleDto::Once {
                run_at: DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc),
            });
        }
    }

    Schedule::from_str(&normalize_cron_expression(value)?)?;
    Ok(JobScheduleDto::Cron {
        expr: value.to_string(),
        timezone: timezone.unwrap_or("UTC").to_string(),
    })
}

fn parse_job_duration_ms(raw: &str) -> Result<u64> {
    let trimmed = raw.trim().to_ascii_lowercase();
    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| anyhow!("invalid duration '{raw}'"))?;
    let (number, unit) = trimmed.split_at(split_at);
    let value = number
        .parse::<u64>()
        .map_err(|_| anyhow!("invalid duration '{raw}'"))?;
    let multiplier = match unit {
        "ms" => 1_u64,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        _ => return Err(anyhow!("invalid duration unit '{raw}'")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("duration '{raw}' is too large"))
}

fn build_runtime_profile(
    args: &RuntimeAddArgs,
    executable: String,
) -> Result<RuntimeProfileConfig> {
    match args.kind.trim() {
        "codex" => {
            validate_codex_profile_args(args)?;
            let confinement = build_confinement_config(args)?;
            Ok(RuntimeProfileConfig::Codex {
                executable,
                model: args.model.clone(),
                confinement,
            })
        }
        "opencode" => {
            let confinement = build_confinement_config(args)?;
            Ok(RuntimeProfileConfig::OpenCode {
                executable,
                model: args.model.clone(),
                agent: args.agent.clone(),
                confinement,
            })
        }
        other => Err(anyhow!(
            "unsupported runtime kind '{other}'; expected 'codex' or 'opencode'"
        )),
    }
}

fn build_confinement_config(args: &RuntimeAddArgs) -> Result<ConfinementConfig> {
    Ok(ConfinementConfig::Oci(OciConfinementConfig {
        engine: match args
            .engine
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(engine) => normalize_podman_executable(engine)?,
            None => normalize_podman_executable("podman")?,
        },
        image: Some(
            args.image
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("runtime image is required"))?
                .to_string(),
        ),
        read_only_rootfs: args.read_only_rootfs,
        tmpfs: args
            .tmpfs
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect(),
        additional_mounts: Vec::new(),
        limits: ExecutionLimits {
            memory_limit: args
                .memory_limit
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            cpu_limit: args
                .cpu_limit
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            pids_limit: args.pids_limit,
        },
    }))
}

fn validate_codex_profile_args(args: &RuntimeAddArgs) -> Result<()> {
    if args.agent.is_some() {
        return Err(anyhow!(
            "opencode-specific flags are not valid for kind 'codex'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::runtime::ConfinementConfig;
    use crate::operator::managed_units::{
        channel_unit_name, ensure_unit_identity, render_channel_unit, ChannelUnitSpec,
    };
    use chrono::Duration as ChronoDuration;
    use std::io::Cursor;
    use tempfile::{NamedTempFile, TempDir};

    #[cfg(unix)]
    fn runtime_add_args(kind: &str) -> (TempDir, RuntimeAddArgs) {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let podman = temp_dir.path().join("podman");
        std::fs::write(&podman, "#!/usr/bin/env bash\n").expect("write podman");
        std::fs::set_permissions(&podman, std::fs::Permissions::from_mode(0o755))
            .expect("chmod podman");

        (
            temp_dir,
            RuntimeAddArgs {
                id: "runtime".to_string(),
                kind: kind.to_string(),
                executable: "runtime-bin".to_string(),
                model: None,
                agent: None,
                engine: Some(podman.to_string_lossy().to_string()),
                image: None,
                read_only_rootfs: false,
                tmpfs: Vec::new(),
                memory_limit: None,
                cpu_limit: None,
                pids_limit: None,
            },
        )
    }

    fn default_logs_args() -> ProductLogsArgs {
        ProductLogsArgs {
            follow: false,
            tail: 200,
            since: None,
            daemon: false,
            workers: false,
            worker: None,
            all: false,
        }
    }

    #[test]
    fn parses_product_log_filter_exclusively() {
        let mut args = default_logs_args();
        args.worker = Some("telegram".to_string());
        assert_eq!(
            parse_log_filter(&args).expect("worker filter"),
            LogFilter::Worker("telegram".to_string())
        );

        args.daemon = true;
        let err = parse_log_filter(&args).expect_err("conflicting filters should fail");
        assert!(err.to_string().contains("only one"));
    }

    #[test]
    fn instance_adopt_positional_home_does_not_set_global_home() {
        let cli = Cli::try_parse_from([
            "lionclaw",
            "instance",
            "adopt",
            "shared",
            "/tmp/lionclaw-home",
            "--work-root",
            ".",
        ])
        .expect("parse instance adopt");

        assert!(cli.home.is_none());
        match cli.command {
            Command::Instance {
                command: InstanceCommand::Adopt(args),
            } => {
                assert_eq!(args.name, "shared");
                assert_eq!(args.source_home, PathBuf::from("/tmp/lionclaw-home"));
                assert_eq!(args.work_root, Some(PathBuf::from(".")));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn project_namespace_exposes_only_project_setup() {
        assert!(Cli::try_parse_from(["lionclaw", "project", "init"]).is_ok());
        assert!(Cli::try_parse_from(["lionclaw", "project", "status"]).is_err());
        assert!(Cli::try_parse_from(["lionclaw", "project", "doctor"]).is_err());
    }

    #[test]
    fn runtime_mount_commands_parse_under_runtime_namespace() {
        let cli = Cli::try_parse_from([
            "lionclaw",
            "runtime",
            "mount",
            "add",
            "codex",
            "docs",
            "--source",
            "/srv/docs",
            "--read-write",
        ])
        .expect("parse runtime mount add");

        match cli.command {
            Command::Runtime { command } => match *command {
                RuntimeCommand::Mount {
                    command: RuntimeMountCommand::Add(args),
                } => {
                    assert_eq!(args.runtime_id, "codex");
                    assert_eq!(args.target, "docs");
                    assert_eq!(args.source, PathBuf::from("/srv/docs"));
                    assert!(args.read_write);
                }
                other => panic!("unexpected runtime command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }

        assert!(Cli::try_parse_from([
            "lionclaw",
            "runtime",
            "mount",
            "remove",
            "codex",
            "/reference/docs",
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["lionclaw", "runtime", "mount", "list", "codex"]).is_ok());
    }

    #[test]
    fn plain_doctor_does_not_pre_resolve_a_target() {
        let target = resolve_command_target(
            &TargetSelection::default(),
            &Command::Doctor(DoctorArgs { all: false }),
        )
        .expect("doctor target resolution");

        assert!(target.is_none());
    }

    #[test]
    fn project_wide_commands_reject_home_and_instance_selectors() {
        let with_home = TargetSelection {
            home: Some(PathBuf::from("/tmp/lionclaw-home")),
            project: None,
            instance: None,
        };
        let err =
            validate_project_wide_target("logs --all", &with_home).expect_err("--home should fail");
        assert!(err.to_string().contains("cannot be combined with --home"));

        let with_instance = TargetSelection {
            home: None,
            project: None,
            instance: Some("reviewer".to_string()),
        };
        let err = validate_project_wide_target("logs --all", &with_instance)
            .expect_err("--instance should fail");
        assert!(err
            .to_string()
            .contains("cannot be combined with --instance"));
    }

    #[test]
    fn parses_duration_schedule_as_once() {
        let before = Utc::now();
        let schedule = parse_job_schedule_spec("30m", None).expect("parse once schedule");
        let after = Utc::now();

        let JobScheduleDto::Once { run_at } = schedule else {
            panic!("expected once schedule");
        };
        assert!(run_at >= before + ChronoDuration::minutes(30));
        assert!(run_at <= after + ChronoDuration::minutes(30) + ChronoDuration::seconds(1));
    }

    #[test]
    fn parses_interval_schedule() {
        let schedule = parse_job_schedule_spec("every 30m", None).expect("parse interval schedule");

        let JobScheduleDto::Interval {
            every_ms,
            anchor_ms,
        } = schedule
        else {
            panic!("expected interval schedule");
        };
        assert_eq!(every_ms, 30 * 60 * 1000);
        assert!(anchor_ms > 0);
    }

    #[test]
    fn parses_cron_schedule_with_timezone() {
        let schedule = parse_job_schedule_spec("*/5 * * * *", Some("America/New_York"))
            .expect("parse cron schedule");

        assert!(matches!(
            schedule,
            JobScheduleDto::Cron {
                expr,
                timezone
            } if expr == "*/5 * * * *" && timezone == "America/New_York"
        ));
    }

    #[test]
    fn rejects_invalid_schedule() {
        assert!(parse_job_schedule_spec("not-a-schedule", None).is_err());
    }

    #[test]
    fn configure_command_does_not_require_resolved_work_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let work_root = project.project_root.join("review-work");
        std::fs::create_dir_all(&work_root).expect("work root");
        let reviewer = create_project_instance(
            &project.project_root,
            "reviewer",
            Some(work_root.as_path()),
            false,
        )
        .expect("create reviewer instance");
        std::fs::remove_dir_all(&work_root).expect("break work root");
        let selection = TargetSelection {
            home: None,
            project: Some(project.project_root.clone()),
            instance: Some("reviewer".to_string()),
        };

        let target = resolve_command_target(
            &selection,
            &Command::Configure(ConfigureArgs {
                runtime: Some("codex".to_string()),
            }),
        )
        .expect("configure target should resolve without a work root")
        .expect("configure should resolve target");

        assert_eq!(target.instance_name.as_deref(), Some("reviewer"));
        assert_eq!(target.instance_home.root(), reviewer.home.as_path());
        assert!(target.work_root.is_none());
    }

    #[test]
    fn runtime_opening_commands_require_recorded_home_work_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = temp_dir.path().join("home");
        std::fs::create_dir_all(&home).expect("home");
        let selection = TargetSelection {
            home: Some(home.clone()),
            project: None,
            instance: None,
        };

        let commands = vec![
            (
                "run",
                Command::Run(RunArgs {
                    plain: false,
                    runtime_tui: false,
                    continue_last_session: false,
                    timeout: None,
                    runtime: None,
                }),
            ),
            ("up", Command::Up(AllArgs { all: false })),
            (
                "channel attach",
                Command::Channel {
                    command: ChannelCommand::Attach(ChannelAttachArgs {
                        id: "loopback".to_string(),
                        peer: None,
                        runtime: None,
                    }),
                },
            ),
            (
                "continuity drafts",
                Command::Continuity {
                    command: ContinuityCommand::Drafts {
                        command: ContinuityDraftCommand::Ls(ContinuityDraftListArgs {
                            runtime: None,
                        }),
                    },
                },
            ),
            (
                "job run",
                Command::Job {
                    command: JobCommand::Run(JobRefArgs {
                        job_id: "1".to_string(),
                    }),
                },
            ),
            (
                "job tick",
                Command::Job {
                    command: JobCommand::Tick,
                },
            ),
            (
                "runtime mount add",
                Command::Runtime {
                    command: Box::new(RuntimeCommand::Mount {
                        command: RuntimeMountCommand::Add(RuntimeMountAddArgs {
                            runtime_id: "codex".to_string(),
                            target: "docs".to_string(),
                            source: PathBuf::from("/tmp/docs"),
                            read_only: false,
                            read_write: false,
                        }),
                    }),
                },
            ),
            (
                "runtime mount list",
                Command::Runtime {
                    command: Box::new(RuntimeCommand::Mount {
                        command: RuntimeMountCommand::List(RuntimeMountListArgs {
                            runtime_id: "codex".to_string(),
                        }),
                    }),
                },
            ),
        ];

        for (label, command) in commands {
            let err = match resolve_command_target(&selection, &command) {
                Ok(_) => panic!("{label} should require a work root"),
                Err(err) => err,
            };
            assert!(
                err.to_string()
                    .contains("does not contain a recorded work root"),
                "{label} returned unexpected error: {err}"
            );
        }

        let runtime_list = resolve_command_target(
            &selection,
            &Command::Runtime {
                command: Box::new(RuntimeCommand::Ls),
            },
        )
        .expect("runtime ls should target home without work root")
        .expect("runtime ls should resolve target");
        assert_eq!(runtime_list.instance_home.root(), home.as_path());
        assert!(runtime_list.work_root.is_none());

        let runtime_mount_remove = resolve_command_target(
            &selection,
            &Command::Runtime {
                command: Box::new(RuntimeCommand::Mount {
                    command: RuntimeMountCommand::Remove(RuntimeMountRemoveArgs {
                        runtime_id: "codex".to_string(),
                        target: "docs".to_string(),
                    }),
                }),
            },
        )
        .expect("runtime mount remove should target home without work root")
        .expect("runtime mount remove should resolve target");
        assert_eq!(runtime_mount_remove.instance_home.root(), home.as_path());
        assert!(runtime_mount_remove.work_root.is_none());
    }

    #[test]
    fn run_command_targets_named_project_instance_and_work_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let reviewer = create_project_instance(
            &project.project_root,
            "reviewer",
            Some(project.project_root.as_path()),
            false,
        )
        .expect("create reviewer instance");
        let selection = TargetSelection {
            home: None,
            project: Some(project.project_root.clone()),
            instance: Some("reviewer".to_string()),
        };
        let command = Command::Run(RunArgs {
            plain: false,
            runtime_tui: false,
            continue_last_session: false,
            timeout: None,
            runtime: None,
        });

        let target = resolve_command_target(&selection, &command)
            .expect("run target should resolve")
            .expect("run target should be selected");

        assert_eq!(
            target.project_root.as_deref(),
            Some(project.project_root.as_path())
        );
        assert_eq!(target.instance_name.as_deref(), Some("reviewer"));
        assert_eq!(target.instance_home.root(), reviewer.home.as_path());
        assert_ne!(target.instance_home.root(), project.instance.home.as_path());
        assert_eq!(
            target.require_work_root().expect("run requires work root"),
            reviewer.work_root.as_path()
        );
    }

    #[test]
    fn run_tui_selection_requires_tty_and_honors_plain() {
        let tui_args = RunArgs {
            plain: false,
            runtime_tui: false,
            continue_last_session: false,
            timeout: None,
            runtime: None,
        };
        let plain_args = RunArgs {
            plain: true,
            runtime_tui: false,
            continue_last_session: false,
            timeout: None,
            runtime: None,
        };
        let runtime_tui_args = RunArgs {
            plain: false,
            runtime_tui: true,
            continue_last_session: false,
            timeout: None,
            runtime: None,
        };

        assert!(should_use_run_tui(&tui_args, true, true));
        assert!(!should_use_run_tui(&tui_args, false, true));
        assert!(!should_use_run_tui(&tui_args, true, false));
        assert!(!should_use_run_tui(&plain_args, true, true));
        assert!(!should_use_run_tui(&runtime_tui_args, true, true));
    }

    #[test]
    fn run_runtime_tui_parse_conflicts_with_plain_mode() {
        assert!(Cli::try_parse_from(["lionclaw", "run", "--runtime-tui"]).is_ok());
        assert!(Cli::try_parse_from(["lionclaw", "run", "--plain", "--runtime-tui"]).is_err());
        assert!(
            Cli::try_parse_from(["lionclaw", "run", "--runtime-tui", "--timeout", "30s"]).is_err()
        );
    }

    #[test]
    fn run_tui_detects_empty_project_for_launch_blocker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        std::fs::remove_dir_all(project.instance.home).expect("remove only instance");
        let selection = TargetSelection {
            home: None,
            project: Some(project.project_root.clone()),
            instance: None,
        };

        assert_eq!(
            empty_project_for_tui_launch_blocker(&selection).as_deref(),
            Some(project.project_root.as_path())
        );
    }

    #[test]
    fn connect_pairing_command_targets_selected_home() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("reviewer home"));

        let command = channel_pairing_list_command(&home, "telegram");

        assert!(command.starts_with("lionclaw --home "));
        assert!(command.contains(&home.root().display().to_string()));
        assert!(command.contains("channel pairing list --channel-id telegram"));
        assert!(!command.starts_with("lionclaw channel pairing"));
    }

    #[test]
    fn channel_pairing_link_template_renders_one_use_token() {
        let template = "https://t.me/lionclaw_bot?start={token}";

        let rendered = render_pairing_url(template, "lc_0123456789abcdef").expect("rendered url");

        assert_eq!(
            rendered,
            "https://t.me/lionclaw_bot?start=lc_0123456789abcdef"
        );
    }

    #[test]
    fn channel_pairing_link_template_rejects_unsafe_values() {
        let good = serde_json::json!({
            "pairing_url_template": "https://t.me/lionclaw_bot?start={token}"
        });
        let missing_token = serde_json::json!({
            "pairing_url_template": "https://t.me/lionclaw_bot"
        });
        let non_https = serde_json::json!({
            "pairing_url_template": "tg://resolve?domain=lionclaw_bot&start={token}"
        });
        let multiline = serde_json::json!({
            "pairing_url_template": "https://t.me/lionclaw_bot?start={token}\nnext"
        });

        assert_eq!(
            safe_pairing_url_template(&good).as_deref(),
            Some("https://t.me/lionclaw_bot?start={token}")
        );
        assert!(safe_pairing_url_template(&missing_token).is_none());
        assert!(safe_pairing_url_template(&non_https).is_none());
        assert!(safe_pairing_url_template(&multiline).is_none());
        assert!(render_pairing_url("https://t.me/lionclaw_bot?start={token}", "bad\n").is_none());
    }

    #[test]
    fn channel_pairing_invite_parses_scope_limits_and_bindings() {
        let cli = Cli::try_parse_from([
            "lionclaw",
            "channel",
            "pairing",
            "invite",
            "telegram",
            "--scope",
            "thread",
            "--conversation-ref",
            "chat-1",
            "--thread-ref",
            "topic-42",
            "--expires-in",
            "5m",
            "--max-claims",
            "3",
            "--label",
            "reviewers",
        ])
        .expect("parse pairing invite");

        match cli.command {
            Command::Channel {
                command:
                    ChannelCommand::Pairing {
                        command: ChannelPairingCommand::Invite(args),
                    },
            } => {
                assert_eq!(args.channel_id, "telegram");
                assert_eq!(args.scope, "thread");
                assert_eq!(args.conversation_ref.as_deref(), Some("chat-1"));
                assert_eq!(args.thread_ref.as_deref(), Some("topic-42"));
                assert_eq!(args.expires_in, Some(std::time::Duration::from_secs(300)));
                assert_eq!(args.max_claims, Some(3));
                assert_eq!(args.label.as_deref(), Some("reviewers"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn channel_pairing_block_keeps_uuid_shaped_sender_ref_positional() {
        let sender_ref = Uuid::new_v4().to_string();
        let cli = Cli::try_parse_from([
            "lionclaw",
            "channel",
            "pairing",
            "block",
            "loopback",
            sender_ref.as_str(),
        ])
        .expect("parse sender ref block");

        match cli.command {
            Command::Channel {
                command:
                    ChannelCommand::Pairing {
                        command: ChannelPairingCommand::Block(args),
                    },
            } => {
                assert_eq!(args.channel_id, "loopback");
                assert_eq!(args.sender_ref.as_deref(), Some(sender_ref.as_str()));
                assert!(args.pairing_id.is_none());
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn channel_pairing_block_accepts_explicit_pairing_id() {
        let pairing_id = Uuid::new_v4();
        let pairing_id_raw = pairing_id.to_string();
        let cli = Cli::try_parse_from([
            "lionclaw",
            "channel",
            "pairing",
            "block",
            "loopback",
            "--pairing-id",
            pairing_id_raw.as_str(),
        ])
        .expect("parse pairing id block");

        match cli.command {
            Command::Channel {
                command:
                    ChannelCommand::Pairing {
                        command: ChannelPairingCommand::Block(args),
                    },
            } => {
                assert_eq!(args.channel_id, "loopback");
                assert!(args.sender_ref.is_none());
                assert_eq!(args.pairing_id, Some(pairing_id));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn render_channel_pairing_list_includes_grants() {
        let pairing_id = Uuid::new_v4();
        let grant_id = Uuid::new_v4();
        let now = Utc::now();
        let output = render_channel_pairing_list(&ChannelPairingListResponse {
            pairings: vec![ChannelPairingView {
                pairing_id,
                channel_id: "loopback".to_string(),
                sender_ref: Some("alice".to_string()),
                conversation_ref: Some("chat-1".to_string()),
                thread_ref: None,
                requested_profile: ChannelRoutingProfile::Conversation,
                status: ChannelPairingStatus::Pending,
                label: Some("Alice".to_string()),
                created_at: now,
                expires_at: None,
            }],
            grants: vec![ChannelGrantView {
                grant_id,
                channel_id: "loopback".to_string(),
                sender_ref: Some("alice".to_string()),
                conversation_ref: Some("chat-1".to_string()),
                thread_ref: None,
                routing_profile: ChannelRoutingProfile::Conversation,
                trust_tier: TrustTier::Main,
                status: "approved".to_string(),
                label: Some("Alice".to_string()),
                created_at: now,
                updated_at: now,
                revoked_at: None,
            }],
        });

        assert!(output.contains(&format!("pairing={pairing_id}")));
        assert!(output.contains(&format!("grant={grant_id}")));
        assert!(output.contains("status=approved"));
        assert!(output.contains("trust=main"));
    }

    #[tokio::test]
    async fn noninteractive_configure_without_runtime_fails_with_scriptable_form() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = configure_runtime_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("runtime should be required");

        assert!(err
            .to_string()
            .contains("lionclaw configure --runtime codex"));
        assert!(
            !home.config_path().exists(),
            "configure should not create config when runtime is missing"
        );
    }

    #[tokio::test]
    async fn configure_codex_reports_missing_podman_without_writing_config() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = configure_runtime_with_io_and_engine_resolver(
            &home,
            Some("codex".to_string()),
            false,
            &mut input,
            &mut output,
            |_| normalize_podman_executable("lionclaw-definitely-missing-podman"),
        )
        .await
        .expect_err("missing podman should fail");

        let message = err.to_string();
        assert!(message.contains("Podman is required for OCI confinement"));
        assert!(message.contains("environment running LionClaw"));
        assert!(
            !home.config_path().exists(),
            "configure should not create config when Podman is missing"
        );
    }

    #[tokio::test]
    async fn interactive_configure_requires_runtime_input() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(b"codex\n".to_vec());
        let mut output = Vec::new();

        let outcome = configure_runtime_with_io_and_engine_resolver(
            &home,
            None,
            true,
            &mut input,
            &mut output,
            |_| Ok("/usr/bin/podman".to_string()),
        )
        .await
        .expect("configure runtime");

        assert_eq!(outcome.runtime_id, "codex");
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(config.defaults.runtime.as_deref(), Some("codex"));
        let prompt = String::from_utf8(output).expect("utf8 prompt");
        assert!(prompt.contains("Runtime to configure:"));
    }

    #[tokio::test]
    async fn channel_list_renders_instance_channels() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = OperatorConfig::default();
        config.upsert_channel(crate::operator::config::ManagedChannelConfig {
            id: "loopback".to_string(),
            skill: "loopback".to_string(),
            launch_mode: ChannelLaunchMode::Interactive,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: Vec::new(),
            contact: None,
        });
        config.upsert_channel(crate::operator::config::ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: vec!["TELEGRAM_BOT_TOKEN".to_string()],
            contact: None,
        });
        config.save(&home).await.expect("save config");
        let manager = crate::operator::managed_units::FakeUnitManager::default();

        let rendered = render_instance_channel_list("main", &home, &manager)
            .await
            .expect("channel list");

        assert!(rendered.contains("instance: main"));
        assert!(rendered.contains("loopback"));
        assert!(rendered.contains("interactive"));
        assert!(rendered.contains("telegram"));
        assert!(rendered.contains("background"));
        assert!(rendered.contains("unit"));
        assert!(rendered.contains("not-installed"));
    }

    #[tokio::test]
    async fn channel_list_requires_owned_unit_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        home.ensure_home_id().await.expect("home id");
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let unit = channel_unit_name(&identity, "telegram");
        let mut config = OperatorConfig::default();
        config.upsert_channel(crate::operator::config::ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: vec!["TELEGRAM_BOT_TOKEN".to_string()],
            contact: None,
        });
        config.save(&home).await.expect("save config");
        let manager = crate::operator::managed_units::FakeUnitManager::default();
        manager
            .set_unit_status(&unit, "loaded/active/running")
            .expect("unit status");

        let rendered = render_instance_channel_list("main", &home, &manager)
            .await
            .expect("channel list");

        assert!(rendered.contains("not-installed"));
        assert!(!rendered.contains("loaded/active/running"));
    }

    #[tokio::test]
    async fn channel_remove_deletes_configured_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut config = OperatorConfig::default();
        config.upsert_channel(crate::operator::config::ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: vec!["TELEGRAM_BOT_TOKEN".to_string()],
            contact: None,
        });
        config.save(&home).await.expect("save config");
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let unit = channel_unit_name(&identity, "telegram");
        let foreign_unit = channel_unit_name(&identity, "foreign");
        let mut env = crate::operator::channel_env::ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        crate::operator::channel_env::save_channel_env(&home, "telegram", &env).expect("save env");
        let manager = crate::operator::managed_units::FakeUnitManager::default();
        let channel_unit = render_channel_unit(
            &home,
            &identity,
            &ChannelUnitSpec {
                channel_id: "telegram".to_string(),
                worker_path: temp_dir.path().join("worker"),
                env: Vec::new(),
                channel_env_path: None,
            },
        );
        manager
            .apply_units(&home, &[channel_unit])
            .await
            .expect("apply channel unit");
        manager
            .set_unit_status(&unit, "loaded/active/running")
            .expect("owned status");
        manager
            .set_unit_status(&foreign_unit, "loaded/active/running")
            .expect("foreign status");

        stop_owned_channel_unit(&home, &manager, "telegram")
            .await
            .expect("stop owned unit");
        stop_owned_channel_unit(&home, &manager, "foreign")
            .await
            .expect("skip foreign unit");
        assert!(remove_channel(&home, "telegram")
            .await
            .expect("remove channel"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(config.channels.is_empty());
        assert_eq!(
            manager.unit_status(&unit).await.expect("owned status"),
            "loaded/inactive/dead"
        );
        assert_eq!(
            manager
                .unit_status(&foreign_unit)
                .await
                .expect("foreign status"),
            "loaded/active/running"
        );
        assert_eq!(
            crate::operator::channel_env::load_channel_env(&home, "telegram")
                .expect("load env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("secret-token")
        );
    }

    #[tokio::test]
    async fn load_job_prompt_rejects_both_sources() {
        let file = NamedTempFile::new().expect("create prompt file");
        let err = load_job_prompt(
            Some("inline".to_string()),
            Some(file.path().display().to_string()),
        )
        .await
        .expect_err("expected prompt source conflict");

        assert!(err.to_string().contains("either --prompt or --prompt-file"));
    }

    #[tokio::test]
    async fn load_job_prompt_reads_prompt_file() {
        let file = NamedTempFile::new().expect("create prompt file");
        std::fs::write(file.path(), "  repo status brief  ").expect("write prompt file");

        let prompt = load_job_prompt(None, Some(file.path().display().to_string()))
            .await
            .expect("load prompt file");

        assert_eq!(prompt, "repo status brief");
    }

    fn project_validate_args(root: &std::path::Path) -> ProjectValidateArgs {
        ProjectValidateArgs {
            runtime_id: "codex".to_string(),
            runtime_kind: "codex".to_string(),
            runtime_bin: "codex".to_string(),
            podman_bin: Some("/usr/bin/podman".to_string()),
            runtime_image: "project-runtime:v1".to_string(),
            channel_alias: "test-channel".to_string(),
            channel_source: root
                .join("skills/channel-fixture")
                .to_string_lossy()
                .to_string(),
            channel_id: "test-channel".to_string(),
            launch_mode: "interactive".to_string(),
            project_skills: Vec::new(),
        }
    }

    fn project_runtime(image: &str) -> RuntimeProfileConfig {
        RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: "/usr/bin/podman".to_string(),
                image: Some(image.to_string()),
                read_only_rootfs: false,
                tmpfs: Vec::new(),
                additional_mounts: Vec::new(),
                limits: ExecutionLimits::default(),
            }),
        }
    }

    #[tokio::test]
    async fn project_validate_allows_missing_managed_entries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let args = project_validate_args(temp_dir.path());

        let issues = validate_project_managed_config(&home, &args)
            .await
            .expect("validate project config");

        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn project_validate_reports_existing_runtime_mismatch() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), project_runtime("old-runtime:v1"));
        config.save(&home).await.expect("save config");
        let args = project_validate_args(temp_dir.path());

        let issues = validate_project_managed_config(&home, &args)
            .await
            .expect("validate project config");

        assert!(issues
            .iter()
            .any(|issue| issue.contains("confinement.image")));
    }

    #[tokio::test]
    async fn project_validate_reports_existing_skill_source_mismatch() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let skill_dir = home.skills_dir().join("test-channel");
        std::fs::create_dir_all(&skill_dir).expect("skill dir");
        std::fs::write(
            skill_dir.join(SKILL_INSTALL_METADATA_FILE),
            "source = \"local:/other/channel-fixture\"\n",
        )
        .expect("skill metadata");
        let args = project_validate_args(temp_dir.path());

        let issues = validate_project_managed_config(&home, &args)
            .await
            .expect("validate project config");

        assert!(issues
            .iter()
            .any(|issue| issue.contains("skill \"test-channel\" has source")));
    }

    #[tokio::test]
    async fn project_validate_reports_existing_skill_metadata_missing_source() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let skill_dir = home.skills_dir().join("test-channel");
        std::fs::create_dir_all(&skill_dir).expect("skill dir");
        std::fs::write(
            skill_dir.join(SKILL_INSTALL_METADATA_FILE),
            "reference = \"local\"\n",
        )
        .expect("skill metadata");
        let args = project_validate_args(temp_dir.path());

        let issues = validate_project_managed_config(&home, &args)
            .await
            .expect("validate project config");

        assert!(issues
            .iter()
            .any(|issue| issue.contains("skill \"test-channel\" has source=\"\"")));
    }

    #[test]
    fn builds_codex_runtime_profile_with_oci_confinement() {
        let (_temp_dir, mut args) = runtime_add_args("codex");
        args.image = Some("ghcr.io/lionclaw/codex:v1".to_string());
        args.read_only_rootfs = true;
        args.tmpfs = vec!["/tmp:size=512m".to_string()];
        args.memory_limit = Some("4g".to_string());

        let profile = build_runtime_profile(&args, "codex".to_string()).expect("build profile");
        let RuntimeProfileConfig::Codex {
            executable,
            model,
            confinement,
        } = profile
        else {
            panic!("expected codex profile");
        };

        assert_eq!(executable, "codex");
        assert!(model.is_none());
        match confinement {
            ConfinementConfig::Oci(config) => {
                assert!(config.engine.ends_with("/podman"));
                assert_eq!(config.image.as_deref(), Some("ghcr.io/lionclaw/codex:v1"));
                assert!(config.read_only_rootfs);
                assert_eq!(config.tmpfs, vec!["/tmp:size=512m".to_string()]);
                assert_eq!(config.limits.memory_limit.as_deref(), Some("4g"));
            }
        }
    }

    #[test]
    fn build_confinement_config_requires_image() {
        let (_temp_dir, args) = runtime_add_args("codex");
        let err = build_confinement_config(&args).expect_err("missing image should fail");
        assert!(err.to_string().contains("runtime image is required"));
    }

    #[test]
    fn codex_rejects_opencode_specific_flags() {
        let (_temp_dir, mut args) = runtime_add_args("codex");
        args.agent = Some("planner".to_string());
        args.image = Some("ghcr.io/lionclaw/codex:v1".to_string());

        let err = build_runtime_profile(&args, "codex".to_string()).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("opencode-specific flags are not valid for kind 'codex'"));
    }
}
