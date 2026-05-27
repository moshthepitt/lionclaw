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
pub enum WorkerCommand {
    Run(WorkerConfig),
    Help,
}

impl WorkerCommand {
    pub fn from_env_and_args() -> Result<Self> {
        let mut once = false;
        let mut poll_ms = DEFAULT_POLL_MS;
        let mut pull_limit = DEFAULT_PULL_LIMIT;
        let mut lease_ms = DEFAULT_LEASE_MS;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--once" => once = true,
                "--poll-ms" => poll_ms = parse_next_u64(&mut args, "--poll-ms")?,
                "--pull-limit" => pull_limit = parse_next_usize(&mut args, "--pull-limit")?,
                "--lease-ms" => lease_ms = parse_next_u64(&mut args, "--lease-ms")?,
                "-h" | "--help" => {
                    print_help();
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

        Ok(Self::Run(WorkerConfig {
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

fn print_help() {
    println!(
        "lionclaw-channel-team-local [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS]"
    );
}
