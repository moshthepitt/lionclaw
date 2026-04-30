use std::{str::FromStr, time::Duration};

use anyhow::{anyhow, Result};
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
        reconcile::{
            add_channel, add_skill, down, logs, onboard, open_kernel, open_runtime_kernel,
            pairing_approve, pairing_block, pairing_list, remove_channel, remove_skill,
            resolve_installed_skill_worker_entrypoint, resolve_stack_binaries, status, up,
            OnboardBindSelection,
        },
        run::run_local,
        runtime::{
            resolve_runtime_id, validate_runtime_availability,
            validate_runtime_launch_prerequisites,
        },
        services::SystemdUserServiceManager,
    },
    runtime_timeouts::{parse_duration, RuntimeTurnTimeouts},
};

#[derive(Debug, Parser)]
#[command(name = "lionclaw")]
#[command(about = "LionClaw operator CLI")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Onboard(OnboardArgs),
    Run(RunArgs),
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
struct OnboardArgs {
    #[arg(
        long,
        help = "Daemon bind address or 'auto' for an isolated loopback port"
    )]
    bind: Option<String>,
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

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let home = LionClawHome::from_env();

    match cli.command {
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
        Command::Run(args) => {
            let timeout_override = args.timeout.map(RuntimeTurnTimeouts::with_hard_timeout);
            run_local(
                &home,
                args.runtime,
                args.continue_last_session,
                timeout_override,
            )
            .await?;
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
                    let applied = up(&home, &manager, &runtime_id, &binaries).await?;
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
                    let stack = status(&home, &manager).await?;
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
            SkillCommand::Add(args) => {
                let alias = args
                    .alias
                    .unwrap_or_else(|| derive_skill_alias(&args.source));
                add_skill(&home, alias.clone(), args.source, args.reference).await?;
                println!("registered skill {alias}");
            }
            SkillCommand::Rm(args) => {
                let removed = remove_skill(&home, &args.alias).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.alias
                );
            }
            SkillCommand::WorkerPath(args) => {
                let worker = resolve_installed_skill_worker_entrypoint(&home, &args.alias).await?;
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
                    "registered channel {} -> {} ({})",
                    args.id,
                    skill,
                    launch_mode.as_str()
                );
            }
            ChannelCommand::Rm(args) => {
                let removed = remove_channel(&home, &args.id).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.id
                );
            }
            ChannelCommand::Attach(args) => {
                let manager = SystemdUserServiceManager;
                attach_channel(&home, &manager, args.id, args.peer, args.runtime).await?;
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
                    let kernel = open_runtime_kernel(&home, &config, None).await?;
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
                    let kernel = open_runtime_kernel(&home, &config, None).await?;
                    let job_id = parse_job_id(&args.job_id)?;
                    let response = kernel.run_job_now(JobRefRequest { job_id }).await?;
                    println!(
                        "ran {} run={} status={:?}",
                        response.job.job_id, response.run.run_id, response.run.status
                    );
                }
                JobCommand::Tick => {
                    let kernel = open_runtime_kernel(&home, &config, None).await?;
                    let response = kernel.scheduler_tick().await?;
                    println!("claimed {} scheduled runs", response.claimed_runs);
                }
                other => match other {
                    JobCommand::Add(args) => {
                        let args = *args;
                        let runtime_id = resolve_runtime_id(&config, args.runtime.as_deref())?;
                        validate_runtime_launch_prerequisites(&home, &config, &runtime_id).await?;
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

    Ok(())
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
