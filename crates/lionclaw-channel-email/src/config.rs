use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};

use crate::{
    auth::{MailboxAuthConfig, TokenCommand},
    oauth2::{parse_command_from_args, parse_setup_command_from_args, Oauth2Command},
    protocol::{mailbox_id_for, normalize_address, CHANNEL_ID},
};

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
    pub sender_auth: SenderAuthConfig,
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
    pub imap_mailbox: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_tls: SmtpTlsMode,
    pub smtp_username: String,
    pub auth: MailboxAuthConfig,
    pub from_name: Option<String>,
    pub fetch_limit: usize,
    pub max_message_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct DigestConfig {
    pub interval: Duration,
    pub admin_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SenderAuthConfig {
    AuthenticationResults {
        authserv_id: String,
    },
    #[cfg(test)]
    TrustFromHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImapTlsMode {
    Implicit,
    StartTls,
    Insecure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmtpTlsMode {
    Implicit,
    StartTls,
    Insecure,
}

#[derive(Debug, Clone)]
pub enum WorkerCommand {
    Run(Box<WorkerConfig>),
    Oauth2(Oauth2Command),
    Help,
}

impl WorkerCommand {
    pub fn from_env_and_args() -> Result<Self> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        if args.first().map(String::as_str) == Some("oauth2") {
            return parse_command_from_args(args.into_iter().skip(1).collect())
                .map(|command| command.map_or(Self::Help, Self::Oauth2));
        }
        if args.first().map(String::as_str) == Some("setup") {
            return parse_setup_command_from_args(args.into_iter().skip(1).collect())
                .map(|command| command.map_or(Self::Help, Self::Oauth2));
        }

        let mut once = false;
        let mut poll_ms = env_u64("EMAIL_POLL_MS")?.unwrap_or(DEFAULT_POLL_MS);
        let mut pull_limit = env_usize("EMAIL_OUTBOX_PULL_LIMIT")?.unwrap_or(DEFAULT_PULL_LIMIT);
        let mut lease_ms = env_u64("EMAIL_OUTBOX_LEASE_MS")?.unwrap_or(DEFAULT_LEASE_MS);
        let mut fetch_limit = env_usize("EMAIL_FETCH_LIMIT")?.unwrap_or(DEFAULT_FETCH_LIMIT);
        let max_message_bytes =
            env_usize("EMAIL_MAX_MESSAGE_BYTES")?.unwrap_or(DEFAULT_MAX_MESSAGE_BYTES);
        validate_max_message_bytes(max_message_bytes)?;

        let mut args = args.into_iter();
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
        let state_dir = state_dir_for(&home, &mailbox_id);

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
        let smtp_tls = parse_smtp_tls(
            env::var("EMAIL_SMTP_TLS")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .as_deref(),
            env_bool("EMAIL_SMTP_IMPLICIT_TLS")?,
            smtp_port,
        )?;
        let sender_auth = parse_sender_auth_config(required_env("EMAIL_AUTH_RESULTS_HOST")?)?;
        let auth = parse_mailbox_auth_config(
            required_env("EMAIL_AUTH_MODE")?,
            optional_env("EMAIL_IMAP_PASSWORD"),
            optional_env("EMAIL_SMTP_PASSWORD"),
            optional_env("EMAIL_XOAUTH2_TOKEN_CMD"),
            imap_tls,
            smtp_tls,
        )?;

        Ok(Self::Run(Box::new(WorkerConfig {
            home,
            state_dir,
            base_url: normalize_base_url(&base_url),
            channel_id,
            worker_id,
            sender_auth,
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
                imap_mailbox: env::var("EMAIL_IMAP_MAILBOX")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "INBOX".to_string()),
                smtp_host,
                smtp_port,
                smtp_tls,
                smtp_username: required_env("EMAIL_SMTP_USERNAME")?,
                auth,
                from_name: optional_env("EMAIL_FROM_NAME"),
                fetch_limit,
                max_message_bytes,
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

fn parse_sender_auth_config(authserv_id: String) -> Result<SenderAuthConfig> {
    validate_authserv_id("EMAIL_AUTH_RESULTS_HOST", &authserv_id)?;
    Ok(SenderAuthConfig::AuthenticationResults { authserv_id })
}

fn parse_mailbox_auth_config(
    mode: String,
    imap_password: Option<String>,
    smtp_password: Option<String>,
    token_command: Option<String>,
    imap_tls: ImapTlsMode,
    smtp_tls: SmtpTlsMode,
) -> Result<MailboxAuthConfig> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "basic" => {
            if token_command.is_some() {
                bail!("EMAIL_XOAUTH2_TOKEN_CMD requires EMAIL_AUTH_MODE=xoauth2");
            }
            Ok(MailboxAuthConfig::Basic {
                imap_password: imap_password.ok_or_else(|| {
                    anyhow!("EMAIL_IMAP_PASSWORD is required when EMAIL_AUTH_MODE=basic")
                })?,
                smtp_password: smtp_password.ok_or_else(|| {
                    anyhow!("EMAIL_SMTP_PASSWORD is required when EMAIL_AUTH_MODE=basic")
                })?,
            })
        }
        "xoauth2" => {
            if imap_password.is_some() || smtp_password.is_some() {
                bail!("EMAIL_IMAP_PASSWORD and EMAIL_SMTP_PASSWORD require EMAIL_AUTH_MODE=basic");
            }
            if matches!(imap_tls, ImapTlsMode::Insecure)
                || matches!(smtp_tls, SmtpTlsMode::Insecure)
            {
                bail!("EMAIL_AUTH_MODE=xoauth2 requires TLS for both IMAP and SMTP");
            }
            let raw = token_command.ok_or_else(|| {
                anyhow!("EMAIL_XOAUTH2_TOKEN_CMD is required when EMAIL_AUTH_MODE=xoauth2")
            })?;
            Ok(MailboxAuthConfig::Xoauth2TokenCommand {
                token_command: TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", &raw)?,
            })
        }
        other => bail!("EMAIL_AUTH_MODE must be basic or xoauth2, got {other}"),
    }
}

fn validate_authserv_id(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    if value
        .chars()
        .any(|ch| ch.is_whitespace() || ch.is_control() || matches!(ch, ';' | ':'))
    {
        bail!("{name} must be a plain Authentication-Results authserv-id");
    }
    Ok(())
}

fn required_env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn state_dir_for(home: &Path, mailbox_id: &str) -> PathBuf {
    home.join("channel-state").join(CHANNEL_ID).join(mailbox_id)
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

fn parse_smtp_tls(
    raw: Option<&str>,
    legacy_implicit_tls: Option<bool>,
    port: u16,
) -> Result<SmtpTlsMode> {
    let raw = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    match raw.as_deref() {
        Some("implicit") | Some("tls") => Ok(SmtpTlsMode::Implicit),
        Some("starttls") => Ok(SmtpTlsMode::StartTls),
        Some("insecure") | Some("none") => Ok(SmtpTlsMode::Insecure),
        Some(other) => bail!("EMAIL_SMTP_TLS must be implicit, starttls, or insecure, got {other}"),
        None => match legacy_implicit_tls {
            Some(true) => Ok(SmtpTlsMode::Implicit),
            Some(false) => Ok(SmtpTlsMode::StartTls),
            None if port == 465 => Ok(SmtpTlsMode::Implicit),
            None => Ok(SmtpTlsMode::StartTls),
        },
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
        "lionclaw-channel-email [--once] [--poll-ms MS] [--pull-limit N] [--lease-ms MS] [--fetch-limit N]\n\
         lionclaw-channel-email oauth2 setup --provider gmail --account EMAIL --client-secret-json PATH --env-file email.env\n\
         lionclaw-channel-email oauth2 token --state-file PATH"
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
    fn state_dir_is_derived_from_lionclaw_home_and_mailbox_id() {
        assert_eq!(
            state_dir_for(Path::new("/tmp/lionclaw-home"), "assistant-example-com"),
            PathBuf::from("/tmp/lionclaw-home/channel-state/email/assistant-example-com")
        );
    }

    #[test]
    fn imap_tls_mode_is_case_insensitive() {
        assert_eq!(
            parse_imap_tls(Some("STARTTLS"), 993).expect("tls mode"),
            ImapTlsMode::StartTls
        );
    }

    #[test]
    fn smtp_tls_mode_is_explicit_with_legacy_bool_fallback() {
        assert_eq!(
            parse_smtp_tls(Some("STARTTLS"), None, 587).expect("tls mode"),
            SmtpTlsMode::StartTls
        );
        assert_eq!(
            parse_smtp_tls(Some("none"), None, 25).expect("tls mode"),
            SmtpTlsMode::Insecure
        );
        assert_eq!(
            parse_smtp_tls(None, Some(false), 587).expect("legacy tls mode"),
            SmtpTlsMode::StartTls
        );
        assert_eq!(
            parse_smtp_tls(None, None, 465).expect("default tls mode"),
            SmtpTlsMode::Implicit
        );
    }

    #[test]
    fn sender_auth_requires_provider_auth() {
        assert_eq!(
            parse_sender_auth_config("mx.example.com".to_string()).expect("auth results"),
            SenderAuthConfig::AuthenticationResults {
                authserv_id: "mx.example.com".to_string()
            }
        );
        assert!(parse_sender_auth_config("mx example".to_string()).is_err());
    }

    #[test]
    fn mailbox_auth_basic_requires_passwords() {
        let err = parse_mailbox_auth_config(
            "basic".to_string(),
            Some("imap-secret".to_string()),
            None,
            None,
            ImapTlsMode::Implicit,
            SmtpTlsMode::StartTls,
        )
        .expect_err("missing SMTP password should fail");

        assert!(err.to_string().contains("EMAIL_SMTP_PASSWORD"));
    }

    #[test]
    fn mailbox_auth_xoauth2_requires_token_command_and_tls() {
        let err = parse_mailbox_auth_config(
            "xoauth2".to_string(),
            None,
            None,
            None,
            ImapTlsMode::Implicit,
            SmtpTlsMode::StartTls,
        )
        .expect_err("missing token command should fail");
        assert!(err.to_string().contains("EMAIL_XOAUTH2_TOKEN_CMD"));

        let err = parse_mailbox_auth_config(
            "xoauth2".to_string(),
            None,
            None,
            Some("/usr/local/bin/token".to_string()),
            ImapTlsMode::Insecure,
            SmtpTlsMode::StartTls,
        )
        .expect_err("insecure IMAP should fail");
        assert!(err.to_string().contains("requires TLS"));

        let err = parse_mailbox_auth_config(
            "xoauth2".to_string(),
            Some("imap-secret".to_string()),
            None,
            Some("/usr/local/bin/token".to_string()),
            ImapTlsMode::Implicit,
            SmtpTlsMode::StartTls,
        )
        .expect_err("passwords should not be accepted in xoauth2 mode");
        assert!(err.to_string().contains("EMAIL_AUTH_MODE=basic"));
    }
}
