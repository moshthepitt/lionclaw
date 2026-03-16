use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};

use crate::{
    contracts::TrustTier,
    home::LionClawHome,
    operator::{
        config::{derive_skill_alias, normalize_executable, OperatorConfig, RuntimeProfileConfig},
        reconcile::{
            add_channel, add_skill, apply, down, logs, onboard, pairing_approve, pairing_block,
            pairing_list, remove_channel, remove_skill, resolve_stack_binaries, status, up,
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
    Onboard,
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
}

#[derive(Debug, Args)]
struct RunArgs {
    runtime: Option<String>,
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
    #[arg(long = "required-env")]
    required_env: Vec<String>,
}

#[derive(Debug, Args)]
struct ChannelRmArgs {
    id: String,
}

#[derive(Debug, Subcommand)]
enum ChannelPairingCommand {
    List(PairingListArgs),
    Approve(PairingApproveArgs),
    Block(PairingBlockArgs),
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

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let home = LionClawHome::from_env();

    match cli.command {
        Command::Onboard => {
            let config = onboard(&home).await?;
            println!(
                "onboarded {} with workspace {}",
                home.root().display(),
                config.daemon.workspace
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
            run_local(&home, args.runtime).await?;
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
                    println!(
                        "started LionClaw services with runtime {} ({} channels)",
                        runtime_id,
                        applied.lockfile.channels.len()
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
                            "channel={} skill={} binding={} unit={} peers(pending={},approved={},blocked={}) inbound={} outbound={}",
                            channel.id,
                            channel.skill,
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
                add_channel(&home, args.id.clone(), skill.clone(), args.required_env).await?;
                println!("registered channel {} -> {}", args.id, skill);
            }
            ChannelCommand::Rm(args) => {
                let removed = remove_channel(&home, &args.id).await?;
                println!(
                    "{} {}",
                    if removed { "removed" } else { "left unchanged" },
                    args.id
                );
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
    }

    Ok(())
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
