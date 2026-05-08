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
use clap::{Args, Parser, Subcommand};
use cron::Schedule;

use crate::{
    contracts::{
        ContinuityDraftActionRequest, ContinuityDraftListRequest, ContinuityPathRequest,
        ContinuitySearchRequest, JobCreateRequest, JobRefRequest, JobRunsRequest, JobScheduleDto,
        TrustTier,
    },
    home::LionClawHome,
    kernel::{
        jobs::normalize_cron_expression,
        runtime::{ConfinementConfig, ExecutionLimits, OciConfinementConfig},
    },
    operator::{
        attach::attach_channel,
        config::{
            derive_skill_alias, normalize_podman_executable, normalize_runtime_command,
            ChannelLaunchMode, OperatorConfig, RuntimeProfileConfig,
        },
        connect::{connect_channel, ConnectEnvInputs, ConnectOutcome},
        reconcile::{
            add_channel, add_skill, down, logs, onboard, open_kernel,
            open_runtime_kernel_for_work_root, pairing_approve, pairing_block, pairing_list,
            remove_channel, remove_skill, resolve_installed_skill_worker_entrypoint,
            resolve_stack_binaries, status_for_work_root, up_for_work_root, OnboardBindSelection,
        },
        run::run_local,
        runtime::{resolve_runtime_id, validate_runtime_availability},
        runtime_integration::{
            configure_runtime_profile_with_engine_resolver, configure_runtime_required_message,
            ConfigureRuntimeOutcome,
        },
        services::{
            channel_unit_name, existing_service_identity, unit_belongs_to_identity, ServiceManager,
            SystemdUserServiceManager,
        },
        snapshot::SKILL_INSTALL_METADATA_FILE,
        status::{
            missing_target_for_status, render_project_status_all, render_target_status,
            validate_status_all_target,
        },
        target::{
            adopt_project_instance, create_project_instance, init_project,
            list_project_instance_statuses, list_project_instances, resolve_existing_project_root,
            resolve_project_setup_root, resolve_target, TargetSelection, WorkRootRequirement,
        },
    },
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
    Onboard(OnboardArgs),
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
    Status(StatusArgs),
    #[command(hide = true)]
    ProjectValidate(ProjectValidateArgs),
    Runtime {
        #[command(subcommand)]
        command: Box<RuntimeCommand>,
    },
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
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
struct RunArgs {
    #[arg(long)]
    continue_last_session: bool,
    #[arg(
        long,
        value_parser = parse_runtime_timeout,
        help = "Runtime safety limit for each turn, such as 30m, 2h, or 7200s"
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
}

#[derive(Debug, Args)]
struct StatusArgs {
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
    home: PathBuf,
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
struct OnboardArgs {
    #[arg(
        long,
        help = "Daemon bind address or 'auto' for an isolated loopback port"
    )]
    bind: Option<String>,
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
    #[arg(long = "terminal-alias")]
    terminal_alias: String,
    #[arg(long = "terminal-source")]
    terminal_source: String,
    #[arg(long = "channel-id")]
    channel_id: String,
    #[arg(long = "launch-mode")]
    launch_mode: String,
    #[arg(long = "project-skill")]
    project_skills: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    Up(ServiceUpArgs),
    Down,
    Status,
    Logs(LogsArgs),
}

#[derive(Debug, Args)]
struct ServiceUpArgs {
    #[arg(long)]
    runtime: Option<String>,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(long, default_value_t = 200)]
    lines: usize,
}

#[derive(Debug, Subcommand)]
enum RuntimeCommand {
    Add(Box<RuntimeAddArgs>),
    Rm(RuntimeRmArgs),
    Ls,
    SetDefault(RuntimeSetDefaultArgs),
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

#[derive(Debug, Subcommand)]
enum SkillCommand {
    Install(SkillAddArgs),
    Add(SkillAddArgs),
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
    #[arg(long, default_value = "service")]
    launch: String,
    #[arg(long = "required-env")]
    required_env: Vec<String>,
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
    Approve(PairingApproveArgs),
    Block(PairingBlockArgs),
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
}

#[derive(Debug, Args)]
struct PairingApproveArgs {
    channel_id: String,
    peer_id: String,
    pairing_code: String,
    #[arg(long, default_value = "main")]
    trust_tier: String,
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
struct PairingBlockArgs {
    channel_id: String,
    peer_id: String,
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
    #[arg(long = "deliver-peer")]
    deliver_peer: Option<String>,
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
    let resolved_target = resolve_command_target(&target_selection, &command)?;
    let home = resolved_target
        .as_ref()
        .map(|target| target.instance_home.clone())
        .unwrap_or(env_home);

    match command {
        Command::Onboard(args) => {
            let bind_selection = args.bind.as_deref().map(parse_onboard_bind).transpose()?;
            let config = onboard(&home, bind_selection).await?;
            println!(
                "onboarded {} with workspace {} on {}",
                home.root().display(),
                config.daemon.workspace,
                config.daemon.bind
            );
        }
        Command::Project { command } => match command {
            ProjectCommand::Init => {
                let project_root = resolve_project_setup_root(&target_selection)?;
                let result = init_project(&project_root)?;
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
                    &args.home,
                    args.work_root.as_deref(),
                    args.create_work_root,
                )?;
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
            let stdin = std::io::stdin();
            let interactive = stdin.is_terminal();
            let mut input = BufReader::new(stdin);
            let stdout = std::io::stdout();
            let mut output = stdout;
            let manager = SystemdUserServiceManager;
            let outcome = connect_channel(
                &target.instance_home,
                &manager,
                target.require_work_root()?,
                &args.channel_or_path,
                ConnectEnvInputs {
                    env_file: args.env_file,
                    from_env: args.from_env,
                },
                interactive,
                interactive,
                &mut input,
                &mut output,
            )
            .await?;
            print_connect_outcome(&outcome);
        }
        Command::Run(args) => {
            let target = resolved_target
                .as_ref()
                .ok_or_else(|| anyhow!("run requires a resolved LionClaw target"))?;
            let timeout_override = args.timeout.map(RuntimeTurnTimeouts::with_hard_timeout);
            run_local(
                &target.instance_home,
                target.require_work_root()?,
                target.instance_name.as_deref(),
                args.runtime,
                args.continue_last_session,
                timeout_override,
            )
            .await?;
        }
        Command::Status(args) => {
            if args.all {
                validate_status_all_target(&target_selection)?;
                let project_root =
                    resolve_existing_project_root(&target_selection).map_err(|_| {
                        anyhow!(
                            "status --all requires a LionClaw project; run from the project root or pass --project PATH"
                        )
                    })?;
                let status = render_project_status_all(&project_root).await?;
                print!("{status}");
            } else {
                let target = resolved_target
                    .as_ref()
                    .ok_or_else(missing_target_for_status)?;
                let status = render_target_status(target).await?;
                print!("{status}");
            }
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
            RuntimeCommand::SetDefault(args) => {
                let mut config = OperatorConfig::load(&home).await?;
                config.set_default_runtime(&args.id)?;
                config.save(&home).await?;
                println!("default runtime set to {}", args.id);
            }
        },
        Command::Service { command } => {
            let manager = SystemdUserServiceManager;
            match command {
                ServiceCommand::Up(args) => {
                    let config = OperatorConfig::load(&home).await?;
                    let runtime_id = resolve_runtime_id(&config, args.runtime.as_deref())?;
                    let binaries = resolve_stack_binaries()?;
                    let work_root = required_command_work_root(&resolved_target, "service up")?;
                    let applied =
                        up_for_work_root(&home, &manager, &runtime_id, &binaries, work_root)
                            .await?;
                    let managed_channels = applied
                        .config
                        .channels
                        .iter()
                        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Service)
                        .count();
                    println!(
                        "started LionClaw services with runtime {runtime_id} ({managed_channels} channels)"
                    );
                }
                ServiceCommand::Down => {
                    down(&home, &manager).await?;
                    println!("stopped managed LionClaw services");
                }
                ServiceCommand::Status => {
                    let work_root = required_command_work_root(&resolved_target, "service status")?;
                    let stack = status_for_work_root(&home, &manager, work_root).await?;
                    println!("daemon: {}", stack.daemon_status);
                    for channel in stack.channels {
                        println!(
                            "channel={} skill={} launch={} unit={} peers(pending={},approved={},blocked={}) inbound={} outbound={}",
                            channel.id,
                            channel.skill,
                            channel.launch_mode,
                            channel.unit_status,
                            channel.pending_peers,
                            channel.approved_peers,
                            channel.blocked_peers,
                            channel.latest_inbound_at.as_deref().unwrap_or("-"),
                            channel.latest_outbound_at.as_deref().unwrap_or("-"),
                        );
                    }
                }
                ServiceCommand::Logs(args) => {
                    let output = logs(&home, &manager, args.lines).await?;
                    print!("{output}");
                }
            }
        }
        Command::Skill { command } => match command {
            SkillCommand::Add(args) | SkillCommand::Install(args) => {
                let alias = args
                    .alias
                    .unwrap_or_else(|| derive_skill_alias(&args.source));
                add_skill(&home, alias.clone(), args.source, args.reference).await?;
                println!("installed skill {alias}");
                print_runtime_state_change_note();
            }
            SkillCommand::Rm(args) => {
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
                add_channel(
                    &home,
                    args.id.clone(),
                    skill.clone(),
                    launch_mode,
                    args.required_env,
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
                    let manager = SystemdUserServiceManager;
                    print!(
                        "{}",
                        render_project_channel_list(&project_root, &manager).await?
                    );
                } else {
                    let target = resolved_target.as_ref().ok_or_else(|| {
                        anyhow!("channel list requires a resolved LionClaw target")
                    })?;
                    let manager = SystemdUserServiceManager;
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
                let manager = SystemdUserServiceManager;
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
                let manager = SystemdUserServiceManager;
                let work_root = required_command_work_root(&resolved_target, "channel attach")?;
                attach_channel(&home, &manager, work_root, args.id, args.peer, args.runtime)
                    .await?;
            }
            ChannelCommand::Pairing { command } => match command {
                ChannelPairingCommand::List(args) => {
                    for peer in pairing_list(&home, args.channel_id).await? {
                        println!(
                            "channel={} peer={} status={} trust={} code={}",
                            peer.channel_id,
                            peer.peer_id,
                            peer.status,
                            peer.trust_tier.as_str(),
                            peer.pairing_code.as_deref().unwrap_or("-"),
                        );
                    }
                }
                ChannelPairingCommand::Approve(args) => {
                    let trust_tier =
                        TrustTier::from_str(&args.trust_tier).map_err(anyhow::Error::msg)?;
                    let peer = pairing_approve(
                        &home,
                        args.channel_id,
                        args.peer_id,
                        args.pairing_code,
                        trust_tier,
                    )
                    .await?;
                    println!(
                        "approved {}:{} ({})",
                        peer.peer.channel_id,
                        peer.peer.peer_id,
                        peer.peer.trust_tier.as_str()
                    );
                }
                ChannelPairingCommand::Block(args) => {
                    let peer = pairing_block(&home, args.channel_id, args.peer_id).await?;
                    println!("blocked {}:{}", peer.peer.channel_id, peer.peer.peer_id);
                }
            },
        },
        Command::Continuity { command } => {
            let config = OperatorConfig::load(&home).await?;
            match command {
                ContinuityCommand::Drafts { command } => {
                    let work_root =
                        required_command_work_root(&resolved_target, "continuity drafts")?;
                    let kernel =
                        open_runtime_kernel_for_work_root(&home, &config, None, work_root, None)
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
                    let kernel =
                        open_runtime_kernel_for_work_root(&home, &config, None, work_root, None)
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
                    let kernel =
                        open_runtime_kernel_for_work_root(&home, &config, None, work_root, None)
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
                        let delivery = match (args.deliver_channel, args.deliver_peer) {
                            (None, None) => None,
                            (Some(channel_id), Some(peer_id)) => {
                                Some(crate::contracts::JobDeliveryTargetDto {
                                    channel_id,
                                    peer_id,
                                })
                            }
                            _ => {
                                return Err(anyhow!(
                                        "--deliver-channel and --deliver-peer must be provided together"
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
                                .map(|value| format!("{}:{}", value.channel_id, value.peer_id))
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

fn resolve_command_target(
    selection: &TargetSelection,
    command: &Command,
) -> Result<Option<crate::operator::target::TargetContext>> {
    let requirement = match command {
        Command::Configure(_) => Some(WorkRootRequirement::Optional),
        Command::Connect(_) => Some(WorkRootRequirement::Required),
        Command::Run(_) => Some(WorkRootRequirement::Required),
        Command::Status(args) => {
            if args.all {
                None
            } else {
                Some(WorkRootRequirement::Optional)
            }
        }
        Command::Service { command } => match command {
            ServiceCommand::Up(_) | ServiceCommand::Status => Some(WorkRootRequirement::Required),
            ServiceCommand::Down | ServiceCommand::Logs(_) => Some(WorkRootRequirement::Optional),
        },
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
        Command::Runtime { .. } | Command::Skill { .. } => Some(WorkRootRequirement::Optional),
        Command::Onboard(_) if selection.home.is_some() => Some(WorkRootRequirement::Optional),
        Command::Onboard(_) if selection.project.is_some() || selection.instance.is_some() => {
            bail!("onboard cannot be combined with --project or --instance; use --home PATH to choose a home")
        }
        Command::Onboard(_)
        | Command::Project { .. }
        | Command::Instance { .. }
        | Command::ProjectValidate(_) => None,
    };

    requirement
        .map(|requirement| resolve_target(selection, requirement))
        .transpose()
}

fn required_command_work_root<'a>(
    resolved_target: &'a Option<crate::operator::target::TargetContext>,
    command: &str,
) -> Result<&'a Path> {
    let target = resolved_target
        .as_ref()
        .ok_or_else(|| anyhow!("{command} requires a resolved LionClaw target"))?;
    target.require_work_root()
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

fn print_connect_outcome(outcome: &ConnectOutcome) {
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
        ChannelLaunchMode::Service => {
            println!("service channel is running");
            println!("pair or approve peers with:");
            println!(
                "  lionclaw channel pairing list --channel-id {}",
                outcome.channel_id
            );
        }
    }
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

async fn render_project_channel_list<M: ServiceManager>(
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

async fn render_instance_channel_list<M: ServiceManager>(
    instance_name: &str,
    home: &LionClawHome,
    manager: &M,
) -> Result<String> {
    let config = OperatorConfig::load(home).await?;
    let mut output = String::new();
    output.push_str(&format!("instance: {instance_name}\n"));
    output.push_str("channel   launch       skill      service\n");
    if config.channels.is_empty() {
        output.push_str("(none)\n");
        return Ok(output);
    }
    let identity = existing_service_identity(home)?;
    for channel in config.channels {
        let service = match channel.launch_mode {
            ChannelLaunchMode::Interactive => "n/a".to_string(),
            ChannelLaunchMode::Service => match identity.as_ref() {
                Some(identity) => {
                    manager
                        .unit_status(&channel_unit_name(identity, &channel.id))
                        .await?
                }
                None => "not-installed".to_string(),
            },
        };
        output.push_str(&format!(
            "{:<9} {:<12} {:<10} {}\n",
            channel.id,
            channel.launch_mode.as_str(),
            channel.skill,
            service
        ));
    }
    Ok(output)
}

async fn stop_owned_channel_unit<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: &str,
) -> Result<()> {
    let Some(identity) = existing_service_identity(home)? else {
        return Ok(());
    };
    let unit = channel_unit_name(&identity, channel_id);
    let unit_path = systemd_user_unit_dir_for_cli()?.join(&unit);
    if !unit_belongs_to_identity(&unit_path, &identity)? {
        return Ok(());
    }
    manager.down_units(&[unit]).await
}

fn systemd_user_unit_dir_for_cli() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".config/systemd/user"))
}

fn print_runtime_state_change_note() {
    println!(
        "direct runs pick this up on the next launch; rerun 'lionclaw connect <channel>' or 'lionclaw service up' if a managed daemon is already running"
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
        if channel.skill != args.terminal_alias {
            issues.push(format!(
                "channel {:?} has skill={:?}; expected {:?}",
                args.channel_id, channel.skill, args.terminal_alias
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
        args.terminal_alias.clone(),
        format!("local:{}", args.terminal_source),
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

fn parse_onboard_bind(raw: &str) -> Result<OnboardBindSelection> {
    let bind = raw.trim();
    if bind.is_empty() {
        return Err(anyhow!("--bind requires a value"));
    }
    if bind == "auto" {
        return Ok(OnboardBindSelection::Auto);
    }
    Ok(OnboardBindSelection::Explicit(bind.to_string()))
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
                    continue_last_session: false,
                    timeout: None,
                    runtime: None,
                }),
            ),
            (
                "service up",
                Command::Service {
                    command: ServiceCommand::Up(ServiceUpArgs { runtime: None }),
                },
            ),
            (
                "service status",
                Command::Service {
                    command: ServiceCommand::Status,
                },
            ),
            (
                "channel attach",
                Command::Channel {
                    command: ChannelCommand::Attach(ChannelAttachArgs {
                        id: "terminal".to_string(),
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
    fn onboard_honors_explicit_home_target() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = temp_dir.path().join("home");
        let selection = TargetSelection {
            home: Some(home.clone()),
            project: None,
            instance: None,
        };
        let command = Command::Onboard(OnboardArgs { bind: None });

        let target = resolve_command_target(&selection, &command)
            .expect("onboard should accept --home")
            .expect("onboard should resolve explicit home");

        assert_eq!(target.instance_home.root(), home.as_path());
        assert!(target.project_root.is_none());
        assert!(target.instance_name.is_none());
        assert!(target.work_root.is_none());
    }

    #[test]
    fn onboard_rejects_project_instance_targets() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let command = Command::Onboard(OnboardArgs { bind: None });
        let selections = [
            TargetSelection {
                home: None,
                project: Some(temp_dir.path().to_path_buf()),
                instance: None,
            },
            TargetSelection {
                home: None,
                project: None,
                instance: Some("main".to_string()),
            },
        ];

        for selection in selections {
            let err = resolve_command_target(&selection, &command)
                .expect_err("onboard should reject project and instance selectors");
            assert!(err.to_string().contains("onboard cannot be combined"));
        }
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
            terminal_alias: "terminal".to_string(),
            terminal_source: root
                .join("skills/channel-terminal")
                .to_string_lossy()
                .to_string(),
            channel_id: "terminal".to_string(),
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
        let skill_dir = home.skills_dir().join("terminal");
        std::fs::create_dir_all(&skill_dir).expect("skill dir");
        std::fs::write(
            skill_dir.join(SKILL_INSTALL_METADATA_FILE),
            "source = \"local:/other/channel-terminal\"\n",
        )
        .expect("skill metadata");
        let args = project_validate_args(temp_dir.path());

        let issues = validate_project_managed_config(&home, &args)
            .await
            .expect("validate project config");

        assert!(issues
            .iter()
            .any(|issue| issue.contains("skill \"terminal\" has source")));
    }

    #[tokio::test]
    async fn project_validate_reports_existing_skill_metadata_missing_source() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let skill_dir = home.skills_dir().join("terminal");
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
            .any(|issue| issue.contains("skill \"terminal\" has source=\"\"")));
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
