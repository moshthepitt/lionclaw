use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use clap::{Args, Parser, Subcommand};
use cron::Schedule;

use crate::{
    contracts::{JobCreateRequest, JobRefRequest, JobRunsRequest, JobScheduleDto, TrustTier},
    home::LionClawHome,
    kernel::jobs::normalize_cron_expression,
    operator::{
        attach::attach_channel,
        config::{
            derive_skill_alias, normalize_executable, ChannelLaunchMode, OperatorConfig,
            RuntimeProfileConfig,
        },
        lockfile::OperatorLockfile,
        reconcile::{
            add_channel, add_skill, apply, down, logs, onboard, open_kernel, pairing_approve,
            pairing_block, pairing_list, remove_channel, remove_skill, resolve_stack_binaries,
            status, up, OnboardBindSelection,
        },
        run::run_local,
        runtime::resolve_runtime_id,
        services::SystemdUserServiceManager,
    },
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
    Apply,
    Run(RunArgs),
    Runtime {
        #[command(subcommand)]
        command: RuntimeCommand,
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
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long)]
    continue_last_session: bool,
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
    Add(RuntimeAddArgs),
    Rm(RuntimeRmArgs),
    Ls,
    SetDefault(RuntimeSetDefaultArgs),
}

#[derive(Debug, Args)]
struct RuntimeAddArgs {
    id: String,
    #[arg(long)]
    kind: String,
    #[arg(long = "bin", help = "Runtime command name or executable path")]
    executable: String,
    #[arg(long)]
    model: Option<String>,
    #[arg(long, default_value = "read-only")]
    sandbox: String,
    #[arg(long)]
    no_skip_git_repo_check: bool,
    #[arg(long)]
    no_ephemeral: bool,
    #[arg(long, default_value = "json")]
    format: String,
    #[arg(long)]
    agent: Option<String>,
    #[arg(long = "xdg-data-home")]
    xdg_data_home: Option<String>,
    #[arg(long)]
    continue_last_session: bool,
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
    #[arg(long = "skill")]
    skills: Vec<String>,
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
        Command::Apply => {
            let applied = apply(&home).await?;
            println!(
                "applied {} skills and {} channels",
                applied.lockfile.skills.len(),
                applied.lockfile.channels.len()
            );
        }
        Command::Run(args) => {
            run_local(&home, args.runtime, args.continue_last_session).await?;
        }
        Command::Runtime { command } => match command {
            RuntimeCommand::Add(args) => {
                let executable = normalize_executable(&args.executable)?;
                let profile = build_runtime_profile(&args, executable)?;
                let mut config = OperatorConfig::load(&home).await?;
                config.upsert_runtime(args.id.clone(), profile);
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
                            "{} {} kind={} command={}",
                            marker,
                            id,
                            profile.kind(),
                            profile.executable()
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
                        .filter(|channel| {
                            channel.enabled && channel.launch_mode == ChannelLaunchMode::Service
                        })
                        .count();
                    println!(
                        "started LionClaw services with runtime {} ({} channels)",
                        runtime_id, managed_channels
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
                            "channel={} skill={} launch={} binding={} unit={} peers(pending={},approved={},blocked={}) inbound={} outbound={}",
                            channel.id,
                            channel.skill,
                            channel.launch_mode,
                            channel.binding_enabled,
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
                println!("registered skill {}", alias);
            }
            SkillCommand::Rm(args) => {
                let removed = remove_skill(&home, &args.alias).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.alias
                );
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
        Command::Job { command } => {
            let applied = apply(&home).await?;
            let kernel = open_kernel(&home, &applied.config, None).await?;
            match command {
                JobCommand::Add(args) => {
                    let args = *args;
                    let runtime_id = resolve_runtime_id(&applied.config, args.runtime.as_deref())?;
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
                    let skill_ids = resolve_job_skill_ids(&applied.lockfile, &args.skills)?;
                    let response = kernel
                        .create_job(JobCreateRequest {
                            name: args.name,
                            runtime_id,
                            schedule,
                            prompt_text,
                            skill_ids,
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
                    for job in kernel.list_jobs().await?.jobs {
                        println!(
                            "job={} enabled={} runtime={} next_run_at={} status={}",
                            job.job_id,
                            job.enabled,
                            job.runtime_id,
                            job.next_run_at
                                .map(|value| value.to_rfc3339())
                                .unwrap_or_else(|| "-".to_string()),
                            job.last_status.unwrap_or_else(|| "-".to_string()),
                        );
                    }
                }
                JobCommand::Show(args) => {
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
                    let job_id = parse_job_id(&args.job_id)?;
                    let response = kernel.pause_job(job_id).await?;
                    println!("paused {}", response.job.job_id);
                }
                JobCommand::Resume(args) => {
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
                JobCommand::Run(args) => {
                    let job_id = parse_job_id(&args.job_id)?;
                    let response = kernel.run_job_now(JobRefRequest { job_id }).await?;
                    println!(
                        "ran {} run={} status={:?}",
                        response.job.job_id, response.run.run_id, response.run.status
                    );
                }
                JobCommand::Rm(args) => {
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
                JobCommand::Tick => {
                    let response = kernel.scheduler_tick().await?;
                    println!("claimed {} scheduled runs", response.claimed_runs);
                }
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

fn resolve_job_skill_ids(lockfile: &OperatorLockfile, requested: &[String]) -> Result<Vec<String>> {
    let mut resolved = Vec::new();
    for raw in requested {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("--skill cannot be empty"));
        }
        let skill_id = lockfile
            .find_skill(trimmed)
            .map(|skill| skill.skill_id.clone())
            .unwrap_or_else(|| trimmed.to_string());
        if !resolved.iter().any(|value| value == &skill_id) {
            resolved.push(skill_id);
        }
    }
    Ok(resolved)
}

fn parse_job_id(raw: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(raw.trim()).map_err(|_| anyhow!("invalid job id '{}'", raw))
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
        .ok_or_else(|| anyhow!("invalid duration '{}'", raw))?;
    let (number, unit) = trimmed.split_at(split_at);
    let value = number
        .parse::<u64>()
        .map_err(|_| anyhow!("invalid duration '{}'", raw))?;
    let multiplier = match unit {
        "ms" => 1_u64,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        _ => return Err(anyhow!("invalid duration unit '{}'", raw)),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("duration '{}' is too large", raw))
}

fn build_runtime_profile(
    args: &RuntimeAddArgs,
    executable: String,
) -> Result<RuntimeProfileConfig> {
    match args.kind.trim() {
        "codex" => {
            reject_opencode_only_flags(args)?;
            Ok(RuntimeProfileConfig::Codex {
                executable,
                model: args.model.clone(),
                sandbox: args.sandbox.clone(),
                skip_git_repo_check: !args.no_skip_git_repo_check,
                ephemeral: !args.no_ephemeral,
            })
        }
        "opencode" => {
            reject_codex_only_flags(args)?;
            Ok(RuntimeProfileConfig::OpenCode {
                executable,
                format: args.format.clone(),
                model: args.model.clone(),
                agent: args.agent.clone(),
                xdg_data_home: args.xdg_data_home.clone(),
                continue_last_session: args.continue_last_session,
            })
        }
        other => Err(anyhow!(
            "unsupported runtime kind '{}'; expected 'codex' or 'opencode'",
            other
        )),
    }
}

fn reject_opencode_only_flags(args: &RuntimeAddArgs) -> Result<()> {
    if args.agent.is_some()
        || args.xdg_data_home.is_some()
        || args.continue_last_session
        || args.format != "json"
    {
        return Err(anyhow!(
            "opencode-specific flags are not valid for kind 'codex'"
        ));
    }
    Ok(())
}

fn reject_codex_only_flags(args: &RuntimeAddArgs) -> Result<()> {
    if args.no_skip_git_repo_check || args.no_ephemeral || args.sandbox != "read-only" {
        return Err(anyhow!(
            "codex-specific flags are not valid for kind 'opencode'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use tempfile::NamedTempFile;

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
}
