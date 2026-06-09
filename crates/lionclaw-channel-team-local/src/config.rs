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
    pub format_hint: String,
    pub attachments: Vec<SendAttachmentConfig>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SendAttachmentConfig {
    pub path: String,
}

#[derive(Debug, Clone)]
struct ParsedSendArgs {
    recipients: Vec<String>,
    message: Option<String>,
    format_hint: String,
    attachments: Vec<SendAttachmentConfig>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InventoryConfig {
    pub self_instance: String,
    pub instances_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ResolveConfig {
    pub inventory: InventoryConfig,
    pub recipient: String,
}

#[derive(Debug, Clone)]
pub enum Command {
    Worker(WorkerConfig),
    List(InventoryConfig),
    Resolve(ResolveConfig),
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
            Some("list") => {
                args.remove(0);
                CommandName::List
            }
            Some("resolve") => {
                args.remove(0);
                CommandName::Resolve
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
            CommandName::List => Self::list_from_args(args),
            CommandName::Resolve => Self::resolve_from_args(args),
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

    fn list_from_args(args: Vec<String>) -> Result<Self> {
        match args.as_slice() {
            [] => Ok(Self::List(inventory_config_from_env()?)),
            [flag] if matches!(flag.as_str(), "-h" | "--help") => {
                print_list_help();
                Ok(Self::Help)
            }
            [other, ..] => bail!("unknown argument '{other}'"),
        }
    }

    fn resolve_from_args(args: Vec<String>) -> Result<Self> {
        let mut recipient = None;
        for arg in args {
            match arg.as_str() {
                "-h" | "--help" => {
                    print_resolve_help();
                    return Ok(Self::Help);
                }
                other if other.starts_with('-') => bail!("unknown argument '{other}'"),
                other => {
                    if recipient.replace(other.to_string()).is_some() {
                        bail!("team-local resolve accepts exactly one recipient");
                    }
                }
            }
        }

        Ok(Self::Resolve(ResolveConfig {
            inventory: inventory_config_from_env()?,
            recipient: crate::inventory::ensure_single_recipient(recipient)?,
        }))
    }

    fn send_from_args(args: Vec<String>) -> Result<Self> {
        let Some(parsed) = parse_send_args(args)? else {
            return Ok(Self::Help);
        };
        Ok(Self::Send(SendConfig {
            self_instance: required_env("LIONCLAW_PROJECT_INSTANCE")?,
            instances_file: required_env_path("LIONCLAW_PROJECT_INSTANCES_FILE")?,
            channel_send_socket: required_env_path("LIONCLAW_CHANNEL_SEND_SOCKET")?,
            recipients: parsed.recipients,
            message: parsed.message,
            format_hint: parsed.format_hint,
            attachments: parsed.attachments,
            idempotency_key: parsed.idempotency_key,
        }))
    }
}

#[derive(Debug, Clone, Copy)]
enum CommandName {
    Worker,
    List,
    Resolve,
    Send,
}

fn inventory_config_from_env() -> Result<InventoryConfig> {
    Ok(InventoryConfig {
        self_instance: required_env("LIONCLAW_PROJECT_INSTANCE")?,
        instances_file: required_env_path("LIONCLAW_PROJECT_INSTANCES_FILE")?,
    })
}

fn parse_send_args(args: Vec<String>) -> Result<Option<ParsedSendArgs>> {
    let mut recipients = Vec::new();
    let mut message_parts = None;
    let mut format_hint = "markdown".to_string();
    let mut attachments = Vec::new();
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
            "--format" => {
                format_hint = parse_format_hint(&mut args)?;
            }
            "--attachment" => {
                attachments.push(SendAttachmentConfig {
                    path: parse_next_string(&mut args, "--attachment")?,
                });
            }
            "--reply-to-ref" => {
                bail!("team-local send does not support --reply-to-ref; send an addressed message to the instance instead");
            }
            "-h" | "--help" => {
                print_send_help();
                return Ok(None);
            }
            other if other.starts_with('-') => bail!("unknown argument '{other}'"),
            recipient => recipients.push(recipient.to_string()),
        }
    }

    if recipients.is_empty() {
        bail!("team-local send requires at least one recipient");
    }

    Ok(Some(ParsedSendArgs {
        recipients,
        message: message_parts.map(|parts| parts.join(" ")),
        format_hint,
        attachments,
        idempotency_key,
    }))
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

fn parse_format_hint(args: &mut impl Iterator<Item = String>) -> Result<String> {
    let value = parse_next_string(args, "--format")?;
    if matches!(value.as_str(), "plain" | "markdown" | "html") {
        Ok(value)
    } else {
        bail!("--format must be plain, markdown, or html")
    }
}

fn print_help() {
    println!(
        "lionclaw-channel-team-local worker [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS]\n\
         lionclaw-channel-team-local list\n\
         lionclaw-channel-team-local resolve <recipient>\n\
         lionclaw-channel-team-local send [--format plain|markdown|html] [--attachment /runtime/PATH]... [--idempotency-key KEY] <recipient>... [-- MESSAGE]"
    );
}

fn print_worker_help() {
    println!(
        "lionclaw-channel-team-local worker [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS]"
    );
}

fn print_list_help() {
    println!("lionclaw-channel-team-local list");
}

fn print_resolve_help() {
    println!("lionclaw-channel-team-local resolve <recipient>");
}

fn print_send_help() {
    println!(
        "lionclaw-channel-team-local send [--format plain|markdown|html] [--attachment /runtime/PATH]... [--idempotency-key KEY] <recipient>... [-- MESSAGE]"
    );
}

#[cfg(test)]
mod tests {
    use super::parse_send_args;

    #[test]
    fn parses_send_content_flags_and_repeated_attachments() {
        let parsed = parse_send_args(vec![
            "--format".to_string(),
            "html".to_string(),
            "--attachment".to_string(),
            "/runtime/results/report.html".to_string(),
            "--attachment".to_string(),
            "/runtime/results/data.json".to_string(),
            "--idempotency-key".to_string(),
            "turn-1".to_string(),
            "reviewer".to_string(),
            "qa".to_string(),
            "--".to_string(),
            "Report".to_string(),
            "attached.".to_string(),
        ])
        .expect("parse send args")
        .expect("send args");

        assert_eq!(parsed.recipients, ["reviewer", "qa"]);
        assert_eq!(parsed.message.as_deref(), Some("Report attached."));
        assert_eq!(parsed.format_hint, "html");
        assert_eq!(parsed.attachments.len(), 2);
        assert_eq!(parsed.attachments[0].path, "/runtime/results/report.html");
        assert_eq!(parsed.attachments[1].path, "/runtime/results/data.json");
        assert_eq!(parsed.idempotency_key.as_deref(), Some("turn-1"));
    }

    #[test]
    fn parses_send_defaults_for_simple_message() {
        let parsed = parse_send_args(vec![
            "reviewer".to_string(),
            "--".to_string(),
            "Please check this.".to_string(),
        ])
        .expect("parse send args")
        .expect("send args");

        assert_eq!(parsed.recipients, ["reviewer"]);
        assert_eq!(parsed.message.as_deref(), Some("Please check this."));
        assert_eq!(parsed.format_hint, "markdown");
        assert!(parsed.attachments.is_empty());
        assert!(parsed.idempotency_key.is_none());
    }

    #[test]
    fn rejects_reply_ref_flag() {
        let err = parse_send_args(vec![
            "--reply-to-ref".to_string(),
            "source-message".to_string(),
            "reviewer".to_string(),
            "--".to_string(),
            "Reply".to_string(),
        ])
        .expect_err("reply refs are unsupported");

        assert!(err.to_string().contains("does not support --reply-to-ref"));
    }

    #[test]
    fn rejects_invalid_send_format() {
        let err = parse_send_args(vec![
            "--format".to_string(),
            "rtf".to_string(),
            "reviewer".to_string(),
        ])
        .expect_err("invalid format");

        assert!(err.to_string().contains("--format must be"));
    }
}
