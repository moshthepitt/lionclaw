use std::{env, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};

use crate::protocol::CHANNEL_ID;

const DEFAULT_POLL_MS: u64 = 1_000;
const DEFAULT_PULL_LIMIT: usize = 10;
const DEFAULT_LEASE_MS: u64 = 120_000;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub home: PathBuf,
    pub base_url: String,
    pub channel_id: String,
    pub worker_id: String,
    pub once: bool,
    pub poll_interval: Duration,
    pub pull_limit: usize,
    pub lease_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SendConfig {
    pub self_instance: String,
    pub instances_file: PathBuf,
    pub channel_send_socket: PathBuf,
    pub recipients: Vec<String>,
    pub message: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Command {
    Worker(WorkerConfig),
    Send(SendConfig),
    Help,
}

impl Command {
    pub fn from_env_and_args() -> Result<Self> {
        let mut args = env::args().skip(1).collect::<Vec<_>>();
        let command = match args.first().map(String::as_str) {
            Some("worker") => {
                args.remove(0);
                CommandName::Worker
            }
            Some("send") => {
                args.remove(0);
                CommandName::Send
            }
            Some("-h" | "--help") => {
                print_help();
                return Ok(Self::Help);
            }
            _ => CommandName::Worker,
        };

        match command {
            CommandName::Worker => Self::worker_from_args(args),
            CommandName::Send => Self::send_from_args(args),
        }
    }

    fn worker_from_args(args: Vec<String>) -> Result<Self> {
        let mut once = false;
        let mut poll_ms = DEFAULT_POLL_MS;
        let mut pull_limit = DEFAULT_PULL_LIMIT;
        let mut lease_ms = DEFAULT_LEASE_MS;

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--once" => once = true,
                "--poll-ms" => poll_ms = parse_next_u64(&mut args, "--poll-ms")?,
                "--pull-limit" => pull_limit = parse_next_usize(&mut args, "--pull-limit")?,
                "--lease-ms" => lease_ms = parse_next_u64(&mut args, "--lease-ms")?,
                "-h" | "--help" => {
                    print_worker_help();
                    return Ok(Self::Help);
                }
                other => bail!("unknown argument '{other}'"),
            }
        }

        let home = required_env_path("LIONCLAW_HOME")?;
        let base_url = required_env("LIONCLAW_BASE_URL")?;
        let channel_id = env::var("LIONCLAW_CHANNEL_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| CHANNEL_ID.to_string());
        if channel_id != CHANNEL_ID {
            bail!("team-local worker cannot run as channel '{channel_id}'");
        }
        let worker_id = env::var("LIONCLAW_CHANNEL_WORKER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{CHANNEL_ID}:worker"));

        Ok(Self::Worker(WorkerConfig {
            home,
            base_url: normalize_base_url(&base_url),
            channel_id,
            worker_id,
            once,
            poll_interval: Duration::from_millis(poll_ms),
            pull_limit,
            lease_ms,
        }))
    }

    fn send_from_args(args: Vec<String>) -> Result<Self> {
        let mut recipients = Vec::new();
        let mut message_parts = None;
        let mut idempotency_key = None;

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--" => {
                    message_parts = Some(args.collect::<Vec<_>>());
                    break;
                }
                "--idempotency-key" => {
                    idempotency_key = Some(parse_next_string(&mut args, "--idempotency-key")?);
                }
                "-h" | "--help" => {
                    print_send_help();
                    return Ok(Self::Help);
                }
                other if other.starts_with('-') => bail!("unknown argument '{other}'"),
                recipient => recipients.push(recipient.to_string()),
            }
        }

        if recipients.is_empty() {
            bail!("team-local send requires at least one recipient");
        }
        let message = message_parts.map(|parts| parts.join(" "));
        Ok(Self::Send(SendConfig {
            self_instance: required_env("LIONCLAW_PROJECT_INSTANCE")?,
            instances_file: required_env_path("LIONCLAW_PROJECT_INSTANCES_FILE")?,
            channel_send_socket: required_env_path("LIONCLAW_CHANNEL_SEND_SOCKET")?,
            recipients,
            message,
            idempotency_key,
        }))
    }
}

#[derive(Debug, Clone, Copy)]
enum CommandName {
    Worker,
    Send,
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{name} is required"))
}

fn required_env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

fn parse_next_u64(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64> {
    args.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))?
        .parse::<u64>()
        .with_context(|| format!("{flag} must be an unsigned integer"))
}

fn parse_next_usize(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<usize> {
    args.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))?
        .parse::<usize>()
        .with_context(|| format!("{flag} must be an unsigned integer"))
}

fn parse_next_string(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{flag} requires a non-empty value"))
}

fn print_help() {
    println!(
        "lionclaw-channel-team-local worker [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS]\n\
         lionclaw-channel-team-local send [--idempotency-key KEY] <recipient>... [-- MESSAGE]"
    );
}

fn print_worker_help() {
    println!(
        "lionclaw-channel-team-local worker [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS]"
    );
}

fn print_send_help() {
    println!(
        "lionclaw-channel-team-local send [--idempotency-key KEY] <recipient>... [-- MESSAGE]"
    );
}
