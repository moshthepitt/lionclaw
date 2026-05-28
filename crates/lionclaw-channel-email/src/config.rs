use std::{env, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};

use crate::protocol::{mailbox_id_for, normalize_address, CHANNEL_ID};

const DEFAULT_POLL_MS: u64 = 30_000;
const DEFAULT_PULL_LIMIT: usize = 10;
const DEFAULT_LEASE_MS: u64 = 120_000;
const DEFAULT_FETCH_LIMIT: usize = 25;
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 50 * 1024 * 1024;
const DEFAULT_DIGEST_INTERVAL_MS: u64 = 60 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub home: PathBuf,
    pub state_dir: PathBuf,
    pub base_url: String,
    pub channel_id: String,
    pub worker_id: String,
    pub once: bool,
    pub poll_interval: Duration,
    pub pull_limit: usize,
    pub lease_ms: u64,
    pub mailbox: MailboxConfig,
    pub digest: DigestConfig,
}

#[derive(Debug, Clone)]
pub struct MailboxConfig {
    pub mailbox_id: String,
    pub address: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub imap_tls: ImapTlsMode,
    pub imap_username: String,
    pub imap_password: String,
    pub imap_mailbox: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_implicit_tls: bool,
    pub smtp_username: String,
    pub smtp_password: String,
    pub from_name: Option<String>,
    pub fetch_limit: usize,
    pub max_message_bytes: usize,
    pub mark_seen_after_admission: bool,
}

#[derive(Debug, Clone)]
pub struct DigestConfig {
    pub interval: Duration,
    pub admin_to: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImapTlsMode {
    Implicit,
    StartTls,
    Insecure,
}

#[derive(Debug, Clone)]
pub enum WorkerCommand {
    Run(Box<WorkerConfig>),
    Help,
}

impl WorkerCommand {
    pub fn from_env_and_args() -> Result<Self> {
        let mut once = false;
        let mut poll_ms = env_u64("EMAIL_POLL_MS")?.unwrap_or(DEFAULT_POLL_MS);
        let mut pull_limit = env_usize("EMAIL_OUTBOX_PULL_LIMIT")?.unwrap_or(DEFAULT_PULL_LIMIT);
        let mut lease_ms = env_u64("EMAIL_OUTBOX_LEASE_MS")?.unwrap_or(DEFAULT_LEASE_MS);
        let mut fetch_limit = env_usize("EMAIL_FETCH_LIMIT")?.unwrap_or(DEFAULT_FETCH_LIMIT);
        let max_message_bytes =
            env_usize("EMAIL_MAX_MESSAGE_BYTES")?.unwrap_or(DEFAULT_MAX_MESSAGE_BYTES);
        validate_max_message_bytes(max_message_bytes)?;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--once" => once = true,
                "--poll-ms" => poll_ms = parse_next_u64(&mut args, "--poll-ms")?,
                "--pull-limit" => pull_limit = parse_next_usize(&mut args, "--pull-limit")?,
                "--lease-ms" => lease_ms = parse_next_u64(&mut args, "--lease-ms")?,
                "--fetch-limit" => fetch_limit = parse_next_usize(&mut args, "--fetch-limit")?,
                "-h" | "--help" => {
                    print_help();
                    return Ok(Self::Help);
                }
                other => bail!("unknown argument '{other}'"),
            }
        }
        let poll_ms = validate_positive_u64("EMAIL_POLL_MS", poll_ms)?;
        let pull_limit = validate_positive_usize("EMAIL_OUTBOX_PULL_LIMIT", pull_limit)?;
        let lease_ms = validate_positive_u64("EMAIL_OUTBOX_LEASE_MS", lease_ms)?;
        let fetch_limit = validate_positive_usize("EMAIL_FETCH_LIMIT", fetch_limit)?;

        let home = required_env_path("LIONCLAW_HOME")?;
        let base_url = required_env("LIONCLAW_BASE_URL")?;
        let channel_id = env::var("LIONCLAW_CHANNEL_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| CHANNEL_ID.to_string());
        if channel_id != CHANNEL_ID {
            bail!("email worker cannot run as channel '{channel_id}'");
        }
        let worker_id = env::var("LIONCLAW_CHANNEL_WORKER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{CHANNEL_ID}:worker"));

        let address = required_email_address_env("EMAIL_ADDRESS")?;
        let mailbox_id = optional_env("EMAIL_MAILBOX_ID")
            .map(|value| parse_mailbox_id("EMAIL_MAILBOX_ID", &value))
            .transpose()?
            .unwrap_or_else(|| mailbox_id_for(&address));
        validate_mailbox_id("EMAIL_MAILBOX_ID", &mailbox_id)?;
        let state_dir = env::var("EMAIL_STATE_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                home.join("channel-state")
                    .join(CHANNEL_ID)
                    .join(&mailbox_id)
            });

        let imap_host = required_env("EMAIL_IMAP_HOST")?;
        let imap_port = env_u16("EMAIL_IMAP_PORT")?.unwrap_or(993);
        let imap_tls = parse_imap_tls(
            env::var("EMAIL_IMAP_TLS")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .as_deref(),
            imap_port,
        )?;
        let smtp_host = required_env("EMAIL_SMTP_HOST")?;
        let smtp_port = env_u16("EMAIL_SMTP_PORT")?.unwrap_or(587);
        let smtp_implicit_tls = env_bool("EMAIL_SMTP_IMPLICIT_TLS")?.unwrap_or(smtp_port == 465);

        Ok(Self::Run(Box::new(WorkerConfig {
            home,
            state_dir,
            base_url: normalize_base_url(&base_url),
            channel_id,
            worker_id,
            once,
            poll_interval: Duration::from_millis(poll_ms),
            pull_limit,
            lease_ms,
            mailbox: MailboxConfig {
                mailbox_id,
                address,
                imap_host,
                imap_port,
                imap_tls,
                imap_username: required_env("EMAIL_IMAP_USERNAME")?,
                imap_password: required_env("EMAIL_IMAP_PASSWORD")?,
                imap_mailbox: env::var("EMAIL_IMAP_MAILBOX")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "INBOX".to_string()),
                smtp_host,
                smtp_port,
                smtp_implicit_tls,
                smtp_username: required_env("EMAIL_SMTP_USERNAME")?,
                smtp_password: required_env("EMAIL_SMTP_PASSWORD")?,
                from_name: optional_env("EMAIL_FROM_NAME"),
                fetch_limit,
                max_message_bytes,
                mark_seen_after_admission: env_bool("EMAIL_MARK_SEEN_AFTER_ADMISSION")?
                    .unwrap_or(true),
            },
            digest: DigestConfig {
                interval: Duration::from_millis(validate_positive_u64(
                    "EMAIL_DIGEST_INTERVAL_MS",
                    env_u64("EMAIL_DIGEST_INTERVAL_MS")?.unwrap_or(DEFAULT_DIGEST_INTERVAL_MS),
                )?),
                admin_to: optional_email_address_env("EMAIL_ADMIN_DIGEST_TO")?,
            },
        })))
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{name} is required"))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required_email_address_env(name: &str) -> Result<String> {
    parse_email_address(name, &required_env(name)?)
}

fn optional_email_address_env(name: &str) -> Result<Option<String>> {
    optional_env(name)
        .map(|value| parse_email_address(name, &value))
        .transpose()
}

fn parse_email_address(name: &str, raw: &str) -> Result<String> {
    normalize_address(raw).ok_or_else(|| anyhow!("{name} must be a plain email address"))
}

fn parse_mailbox_id(name: &str, raw: &str) -> Result<String> {
    let value = mailbox_id_for(raw);
    validate_mailbox_id(name, &value)?;
    Ok(value)
}

fn validate_mailbox_id(name: &str, value: &str) -> Result<()> {
    if value.is_empty() || matches!(value, "." | "..") {
        bail!("{name} must resolve to a non-empty path-safe mailbox id");
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        bail!("{name} must contain only ASCII letters, numbers, dash, underscore, or dot");
    }
    Ok(())
}

fn required_env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

fn env_bool(name: &str) -> Result<Option<bool>> {
    optional_env(name)
        .map(|raw| match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("{name} must be true or false")),
        })
        .transpose()
}

fn env_u16(name: &str) -> Result<Option<u16>> {
    optional_env(name)
        .map(|raw| {
            raw.parse::<u16>()
                .with_context(|| format!("{name} must be an unsigned integer"))
        })
        .transpose()
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    optional_env(name)
        .map(|raw| {
            raw.parse::<u64>()
                .with_context(|| format!("{name} must be an unsigned integer"))
        })
        .transpose()
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    optional_env(name)
        .map(|raw| {
            raw.parse::<usize>()
                .with_context(|| format!("{name} must be an unsigned integer"))
        })
        .transpose()
}

fn validate_positive_u64(name: &str, value: u64) -> Result<u64> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(value)
}

fn validate_positive_usize(name: &str, value: usize) -> Result<usize> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(value)
}

fn parse_imap_tls(raw: Option<&str>, port: u16) -> Result<ImapTlsMode> {
    let raw = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    match raw.as_deref() {
        Some("implicit") | Some("tls") => Ok(ImapTlsMode::Implicit),
        Some("starttls") => Ok(ImapTlsMode::StartTls),
        Some("insecure") | Some("none") => Ok(ImapTlsMode::Insecure),
        Some(other) => bail!("EMAIL_IMAP_TLS must be implicit, starttls, or insecure, got {other}"),
        None if port == 143 => Ok(ImapTlsMode::StartTls),
        None => Ok(ImapTlsMode::Implicit),
    }
}

pub(crate) fn validate_max_message_bytes(value: usize) -> Result<()> {
    if value == 0 {
        bail!("EMAIL_MAX_MESSAGE_BYTES must be greater than zero");
    }
    if value >= u32::MAX as usize {
        bail!("EMAIL_MAX_MESSAGE_BYTES must be less than {}", u32::MAX);
    }
    Ok(())
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
        "lionclaw-channel-email [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS] [--fetch-limit N]"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_runtime_knob_validation_rejects_zero() {
        let err = validate_positive_u64("EMAIL_POLL_MS", 0).expect_err("zero poll should fail");
        assert!(err.to_string().contains("greater than zero"));

        let err =
            validate_positive_usize("EMAIL_FETCH_LIMIT", 0).expect_err("zero fetch should fail");
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn email_address_config_is_normalized_and_validated() {
        assert_eq!(
            parse_email_address("EMAIL_ADDRESS", " Assistant@Example.COM ").expect("address"),
            "assistant@example.com"
        );
        assert!(parse_email_address("EMAIL_ADDRESS", "not an address").is_err());
    }

    #[test]
    fn mailbox_id_config_is_normalized_and_path_safe() {
        assert_eq!(
            parse_mailbox_id("EMAIL_MAILBOX_ID", " Project/Inbox ").expect("mailbox id"),
            "project-inbox"
        );
        assert!(parse_mailbox_id("EMAIL_MAILBOX_ID", ".").is_err());
        assert!(parse_mailbox_id("EMAIL_MAILBOX_ID", "..").is_err());
        assert!(parse_mailbox_id("EMAIL_MAILBOX_ID", "///").is_err());
    }

    #[test]
    fn imap_tls_mode_is_case_insensitive() {
        assert_eq!(
            parse_imap_tls(Some("STARTTLS"), 993).expect("tls mode"),
            ImapTlsMode::StartTls
        );
    }
}
