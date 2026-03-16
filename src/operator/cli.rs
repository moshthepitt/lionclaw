use std::str::FromStr;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::{
    contracts::TrustTier,
    home::LionClawHome,
    operator::{
        config::derive_skill_alias,
        reconcile::{
            add_channel, add_skill, apply, down, logs, onboard, pairing_approve, pairing_block,
            pairing_list, remove_channel, remove_skill, resolve_stack_binaries, status, up,
        },
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
    Up(UpArgs),
    Down,
    Status,
    Logs(LogsArgs),
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    Pairing {
        #[command(subcommand)]
        command: PairingCommand,
    },
}

#[derive(Debug, Args)]
struct UpArgs {
    #[arg(long)]
    runtime: String,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(long, default_value_t = 200)]
    lines: usize,
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
enum PairingCommand {
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
        Command::Up(args) => {
            let manager = SystemdUserServiceManager;
            let binaries = resolve_stack_binaries()?;
            let applied = up(&home, &manager, &args.runtime, &binaries).await?;
            println!(
                "started lionclaw with runtime {} ({} channels)",
                args.runtime,
                applied.lockfile.channels.len()
            );
        }
        Command::Down => {
            let manager = SystemdUserServiceManager;
            down(&home, &manager).await?;
            println!("stopped managed LionClaw services");
        }
        Command::Status => {
            let manager = SystemdUserServiceManager;
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
        Command::Logs(args) => {
            let manager = SystemdUserServiceManager;
            let output = logs(&home, &manager, args.lines).await?;
            print!("{output}");
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
        },
        Command::Pairing { command } => match command {
            PairingCommand::List(args) => {
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
            PairingCommand::Approve(args) => {
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
            PairingCommand::Block(args) => {
                let peer = pairing_block(&home, args.channel_id, args.peer_id).await?;
                println!("blocked {}:{}", peer.peer.channel_id, peer.peer.peer_id);
            }
        },
    }

    Ok(())
}
