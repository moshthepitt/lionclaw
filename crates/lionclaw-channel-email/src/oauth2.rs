use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, BufRead, ErrorKind, IsTerminal, Write},
    path::{Component, Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, TimeDelta, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use uuid::Uuid;

use crate::protocol::{mailbox_id_for, normalize_address};

const CALLBACK_PATH: &str = "/oauth2/callback";
const CALLBACK_WAIT: Duration = Duration::from_secs(5 * 60);
const HTTP_READ_LIMIT: usize = 16 * 1024;
const TOKEN_REFRESH_SKEW_SECONDS: i64 = 60;
const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(
    name = "oauth2",
    about = "Set up and refresh XOAUTH2 credentials for the email channel"
)]
struct Oauth2Cli {
    #[command(subcommand)]
    command: Oauth2Command,
}

#[derive(Debug, Parser)]
#[command(
    name = "lionclaw-channel-email setup",
    about = "Run an OAuth2 browser flow and write LionClaw email channel credentials"
)]
struct EmailSetupCli {
    #[arg(
        value_name = "PROVIDER",
        value_enum,
        help = "Provider preset to use, such as gmail or microsoft365"
    )]
    provider_profile: Option<Oauth2Provider>,
    #[command(flatten)]
    args: SetupArgs,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Oauth2Command {
    /// Run an OAuth2 browser flow and write a LionClaw email env file.
    Setup(Box<SetupArgs>),
    /// Print a fresh access token for EMAIL_XOAUTH2_TOKEN_CMD.
    Token(TokenArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum Oauth2Provider {
    Gmail,
    #[value(alias = "microsoft", alias = "outlook")]
    Microsoft365,
    Generic,
}

impl Oauth2Provider {
    fn id(self) -> &'static str {
        match self {
            Self::Gmail => "gmail",
            Self::Microsoft365 => "microsoft365",
            Self::Generic => "generic",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Gmail => "Gmail",
            Self::Microsoft365 => "Microsoft 365",
            Self::Generic => "generic OAuth2",
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyValueArg {
    key: String,
    value: String,
}

impl FromStr for KeyValueArg {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        let Some((key, value)) = raw.split_once('=') else {
            bail!("expected KEY=VALUE");
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
        {
            bail!("OAuth auth parameter names must be plain ASCII names");
        }
        Ok(Self {
            key: key.to_string(),
            value: value.trim().to_string(),
        })
    }
}

#[derive(Clone, Debug, Args)]
pub struct SetupArgs {
    #[arg(
        long,
        value_enum,
        default_value_t = Oauth2Provider::Gmail,
        help = "Provider preset; also accepted positionally by `setup gmail`"
    )]
    provider: Oauth2Provider,
    #[arg(long, value_name = "EMAIL", help = "Mailbox email address")]
    account: String,
    #[arg(
        long = "client-secret-json",
        value_name = "PATH",
        help = "OAuth client JSON downloaded from the provider"
    )]
    client_secret_json: Option<PathBuf>,
    #[arg(long, help = "OAuth client id when not using a client JSON file")]
    client_id: Option<String>,
    #[arg(long, help = "OAuth client secret when the provider requires one")]
    client_secret: Option<String>,
    #[arg(long, default_value = "common", help = "Microsoft tenant id or common")]
    tenant: String,
    #[arg(
        long = "auth-url",
        value_name = "URL",
        help = "Generic OAuth authorization endpoint"
    )]
    authorization_endpoint: Option<String>,
    #[arg(
        long = "token-url",
        value_name = "URL",
        help = "Generic OAuth token endpoint"
    )]
    token_endpoint: Option<String>,
    #[arg(
        long = "scope",
        value_name = "SCOPE",
        help = "OAuth scope; repeat or pass a space-separated list"
    )]
    scopes: Vec<String>,
    #[arg(
        long = "auth-param",
        value_name = "KEY=VALUE",
        help = "Extra authorization URL parameter"
    )]
    auth_params: Vec<KeyValueArg>,
    #[arg(
        long = "auth-results-host",
        value_name = "HOST",
        help = "Trusted Authentication-Results authserv-id for this mailbox"
    )]
    auth_results_host: Option<String>,
    #[arg(long = "imap-host", value_name = "HOST", help = "IMAP server host")]
    imap_host: Option<String>,
    #[arg(long = "imap-port", help = "IMAP server port")]
    imap_port: Option<u16>,
    #[arg(
        long = "imap-tls",
        value_name = "MODE",
        help = "IMAP TLS mode: implicit or starttls"
    )]
    imap_tls: Option<String>,
    #[arg(long = "smtp-host", value_name = "HOST", help = "SMTP server host")]
    smtp_host: Option<String>,
    #[arg(long = "smtp-port", help = "SMTP server port")]
    smtp_port: Option<u16>,
    #[arg(
        long = "smtp-tls",
        value_name = "MODE",
        help = "SMTP TLS mode: implicit or starttls"
    )]
    smtp_tls: Option<String>,
    #[arg(
        long = "admin-to",
        value_name = "EMAIL",
        help = "Optional held-mail digest recipient"
    )]
    admin_to: Option<String>,
    #[arg(
        long = "env-file",
        value_name = "PATH",
        help = "Output env file for direct helper use"
    )]
    env_file: Option<PathBuf>,
    #[arg(
        long = "state-file",
        value_name = "PATH",
        help = "Private OAuth refresh-token state file"
    )]
    state_file: Option<PathBuf>,
    #[arg(long = "host", help = "Loopback callback host")]
    callback_host: Option<String>,
    #[arg(
        long = "port",
        default_value_t = 0,
        help = "Loopback callback port; 0 chooses a free port"
    )]
    callback_port: u16,
    #[arg(
        long,
        help = "Print the authorization URL instead of opening a browser"
    )]
    no_browser: bool,
    #[arg(long, help = "Replace existing env or OAuth state files")]
    force: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TokenArgs {
    #[arg(
        long = "state-file",
        value_name = "PATH",
        help = "Private OAuth refresh-token state file"
    )]
    state_file: PathBuf,
}

#[derive(Debug, Clone)]
struct ProviderDefaults {
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
    scopes: Vec<String>,
    auth_params: Vec<(String, String)>,
    auth_results_host: Option<String>,
    imap_host: Option<String>,
    imap_port: Option<u16>,
    imap_tls: Option<String>,
    smtp_host: Option<String>,
    smtp_port: Option<u16>,
    smtp_tls: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedSetup {
    provider: Oauth2Provider,
    account: String,
    client_id: String,
    client_secret: Option<String>,
    authorization_endpoint: String,
    token_endpoint: String,
    scopes: Vec<String>,
    auth_params: Vec<(String, String)>,
    auth_results_host: String,
    imap_host: String,
    imap_port: u16,
    imap_tls: String,
    smtp_host: String,
    smtp_port: u16,
    smtp_tls: String,
    admin_to: Option<String>,
    state_file: PathBuf,
    env_file: PathBuf,
    callback_host: String,
    callback_port: u16,
    no_browser: bool,
    force: bool,
}

#[derive(Debug, Clone)]
struct OAuthClientFile {
    kind: OAuthClientKind,
    client_id: String,
    client_secret: Option<String>,
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OAuthClientKind {
    Installed,
    Web,
}

#[derive(Debug)]
struct PkcePair {
    verifier: String,
    challenge: String,
}

#[derive(Debug, Deserialize)]
struct OAuthClientJson {
    installed: Option<OAuthClientJsonSection>,
    web: Option<OAuthClientJsonSection>,
}

#[derive(Debug, Deserialize)]
struct OAuthClientJsonSection {
    client_id: String,
    client_secret: Option<String>,
    auth_uri: Option<String>,
    token_uri: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredOAuth2State {
    version: u8,
    provider: String,
    account: String,
    token_endpoint: String,
    client_id: String,
    client_secret: Option<String>,
    refresh_token: String,
    access_token: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    scope: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    error: Option<String>,
    error_description: Option<String>,
}

pub fn parse_command_from_args(args: Vec<String>) -> Result<Option<Oauth2Command>> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push("lionclaw-channel-email oauth2".to_string());
    argv.extend(args);
    match Oauth2Cli::try_parse_from(argv) {
        Ok(cli) => Ok(Some(cli.command)),
        Err(err) if err.kind() == clap::error::ErrorKind::DisplayHelp => {
            err.print()?;
            Ok(None)
        }
        Err(err) if err.kind() == clap::error::ErrorKind::DisplayVersion => {
            err.print()?;
            Ok(None)
        }
        Err(err) => Err(err).context("invalid oauth2 command"),
    }
}

pub fn parse_setup_command_from_args(args: Vec<String>) -> Result<Option<Oauth2Command>> {
    if args.is_empty() && io::stdin().is_terminal() && io::stdout().is_terminal() {
        return prompt_setup_command().map(|args| Some(Oauth2Command::Setup(Box::new(args))));
    }

    let has_positional_provider = args.first().is_some_and(|first| !first.starts_with('-'));
    if has_positional_provider
        && args
            .iter()
            .any(|arg| arg == "--provider" || arg.starts_with("--provider="))
    {
        bail!("email setup provider can be specified either positionally or with --provider, not both");
    }

    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push("lionclaw-channel-email setup".to_string());
    argv.extend(args);
    match EmailSetupCli::try_parse_from(argv) {
        Ok(mut cli) => {
            if let Some(provider) = cli.provider_profile {
                cli.args.provider = provider;
            }
            Ok(Some(Oauth2Command::Setup(Box::new(cli.args))))
        }
        Err(err) if err.kind() == clap::error::ErrorKind::DisplayHelp => {
            err.print()?;
            Ok(None)
        }
        Err(err) if err.kind() == clap::error::ErrorKind::DisplayVersion => {
            err.print()?;
            Ok(None)
        }
        Err(err) => Err(err).context("invalid email setup command"),
    }
}

fn prompt_setup_command() -> Result<SetupArgs> {
    let stdin = io::stdin();
    let mut input = io::BufReader::new(stdin.lock());
    let stdout = io::stdout();
    let mut output = stdout.lock();
    prompt_setup_command_with_io(&mut input, &mut output)
}

fn prompt_setup_command_with_io<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
) -> Result<SetupArgs> {
    writeln!(output, "Email OAuth2 setup")?;
    let provider = prompt_provider(input, output)?;
    let account = prompt_required(input, output, "Mailbox email", None)?;
    let mut setup = SetupArgs {
        provider,
        account,
        client_secret_json: None,
        client_id: None,
        client_secret: None,
        tenant: "common".to_string(),
        authorization_endpoint: None,
        token_endpoint: None,
        scopes: Vec::new(),
        auth_params: Vec::new(),
        auth_results_host: None,
        imap_host: None,
        imap_port: None,
        imap_tls: None,
        smtp_host: None,
        smtp_port: None,
        smtp_tls: None,
        admin_to: prompt_optional(input, output, "Admin digest email")?,
        env_file: None,
        state_file: None,
        callback_host: None,
        callback_port: 0,
        no_browser: false,
        force: false,
    };

    match provider {
        Oauth2Provider::Gmail => {
            setup.client_secret_json = Some(PathBuf::from(prompt_required(
                input,
                output,
                "Google OAuth desktop client JSON",
                None,
            )?));
        }
        Oauth2Provider::Microsoft365 => {
            setup.tenant = prompt_required(input, output, "Microsoft tenant", Some("common"))?;
            setup.client_id = Some(prompt_required(input, output, "OAuth client id", None)?);
            setup.auth_results_host = Some(prompt_required(
                input,
                output,
                "Authentication-Results host",
                None,
            )?);
        }
        Oauth2Provider::Generic => {
            setup.client_id = Some(prompt_required(input, output, "OAuth client id", None)?);
            setup.client_secret = prompt_optional(input, output, "OAuth client secret")?;
            setup.authorization_endpoint =
                Some(prompt_required(input, output, "Authorization URL", None)?);
            setup.token_endpoint = Some(prompt_required(input, output, "Token URL", None)?);
            setup.scopes = vec![prompt_required(input, output, "OAuth scopes", None)?];
            setup.auth_results_host = Some(prompt_required(
                input,
                output,
                "Authentication-Results host",
                None,
            )?);
            setup.imap_host = Some(prompt_required(input, output, "IMAP host", None)?);
            setup.smtp_host = Some(prompt_required(input, output, "SMTP host", None)?);
        }
    }

    Ok(setup)
}

fn prompt_provider<R: BufRead, W: Write>(input: &mut R, output: &mut W) -> Result<Oauth2Provider> {
    loop {
        let raw = prompt_required(
            input,
            output,
            "Provider [gmail, microsoft365, generic]",
            Some("gmail"),
        )?;
        match Oauth2Provider::from_str(&raw, true) {
            Ok(provider) => return Ok(provider),
            Err(_) => writeln!(output, "Provider must be gmail, microsoft365, or generic.")?,
        }
    }
}

fn prompt_required<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    label: &str,
    default: Option<&str>,
) -> Result<String> {
    loop {
        if let Some(default) = default {
            write!(output, "{label} [{default}]: ")?;
        } else {
            write!(output, "{label}: ")?;
        }
        output.flush()?;
        let value = read_prompt_value(input)?;
        let value = value.trim();
        if !value.is_empty() {
            return Ok(value.to_string());
        }
        if let Some(default) = default {
            return Ok(default.to_string());
        }
        writeln!(output, "{label} is required.")?;
    }
}

fn prompt_optional<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    label: &str,
) -> Result<Option<String>> {
    write!(output, "{label} (optional): ")?;
    output.flush()?;
    let value = read_prompt_value(input)?;
    Ok(Some(value.trim().to_string()).filter(|value| !value.is_empty()))
}

fn read_prompt_value<R: BufRead>(input: &mut R) -> Result<String> {
    let mut value = String::new();
    let bytes = input.read_line(&mut value)?;
    if bytes == 0 {
        bail!("email setup was cancelled");
    }
    Ok(value)
}

pub async fn run(command: Oauth2Command) -> Result<()> {
    match command {
        Oauth2Command::Setup(args) => run_setup(*args).await,
        Oauth2Command::Token(args) => run_token(args).await,
    }
}

async fn run_setup(args: SetupArgs) -> Result<()> {
    let resolved = resolve_setup(args)?;
    reserve_private_write_target(&resolved.state_file, resolved.force, "OAuth2 state file")?;
    reserve_private_write_target(&resolved.env_file, resolved.force, "email env file")?;

    let listener = bind_callback_listener(&resolved.callback_host, resolved.callback_port).await?;
    let callback_port = listener
        .local_addr()
        .context("failed to read callback listener address")?
        .port();
    let redirect_uri = format!(
        "http://{}:{}{}",
        resolved.callback_host, callback_port, CALLBACK_PATH
    );
    let csrf_state = random_url_token();
    let pkce = PkcePair::generate();
    let authorization_url = build_authorization_url(&resolved, &redirect_uri, &csrf_state, &pkce);

    println!(
        "Starting {} OAuth2 setup for {}.",
        resolved.provider.display_name(),
        resolved.account
    );
    if resolved.no_browser || !open_browser(&authorization_url) {
        println!("Open this URL in a browser:\n{authorization_url}");
    } else {
        println!("Opened your browser. If it did not appear, open this URL:\n{authorization_url}");
    }

    let code = wait_for_authorization_code(listener, &csrf_state).await?;
    let token = exchange_authorization_code(&resolved, &redirect_uri, &code, &pkce.verifier)
        .await
        .context("OAuth2 authorization code exchange failed")?;
    let refresh_token = token.refresh_token.ok_or_else(|| {
        anyhow!(
            "{} did not return a refresh token; re-run setup and approve offline access",
            resolved.provider.display_name()
        )
    })?;
    let access_token = token
        .access_token
        .ok_or_else(|| anyhow!("OAuth2 token response did not include an access token"))?;

    let state = StoredOAuth2State {
        version: 1,
        provider: resolved.provider.id().to_string(),
        account: resolved.account.clone(),
        token_endpoint: resolved.token_endpoint.clone(),
        client_id: resolved.client_id.clone(),
        client_secret: resolved.client_secret.clone(),
        refresh_token,
        access_token: Some(access_token),
        expires_at: expires_at(token.expires_in),
        scope: resolved.scopes.clone(),
    };
    write_private_json(&resolved.state_file, &state)?;

    let current_exe =
        env::current_exe().context("failed to resolve lionclaw-channel-email executable path")?;
    let env_content = build_email_env_content(&resolved, &current_exe);
    write_private_text(&resolved.env_file, &env_content)?;

    println!("OAuth2 state: {}", resolved.state_file.display());
    println!("Email env: {}", resolved.env_file.display());
    if channel_setup_env_file_from_env().is_none() {
        println!(
            "Next: lionclaw connect email --env-file {}",
            shell_quote_arg(&resolved.env_file.display().to_string())
        );
    }
    Ok(())
}

async fn run_token(args: TokenArgs) -> Result<()> {
    let mut state = read_oauth2_state(&args.state_file)?;
    if let Some(token) = cached_access_token(&state) {
        println!("{token}");
        return Ok(());
    }

    let token = refresh_access_token(&state)
        .await
        .context("OAuth2 token refresh failed")?;
    let access_token = token
        .access_token
        .ok_or_else(|| anyhow!("OAuth2 refresh response did not include an access token"))?;
    if let Some(refresh_token) = token.refresh_token {
        state.refresh_token = refresh_token;
    }
    state.access_token = Some(access_token.clone());
    state.expires_at = expires_at(token.expires_in);
    write_private_json(&args.state_file, &state)?;
    println!("{access_token}");
    Ok(())
}

fn provider_defaults(provider: Oauth2Provider, tenant: &str) -> ProviderDefaults {
    match provider {
        Oauth2Provider::Gmail => ProviderDefaults {
            authorization_endpoint: Some("https://accounts.google.com/o/oauth2/v2/auth".into()),
            token_endpoint: Some("https://oauth2.googleapis.com/token".into()),
            scopes: vec!["https://mail.google.com/".into()],
            auth_params: vec![
                ("access_type".into(), "offline".into()),
                ("prompt".into(), "consent".into()),
            ],
            auth_results_host: Some("mx.google.com".into()),
            imap_host: Some("imap.gmail.com".into()),
            imap_port: Some(993),
            imap_tls: Some("implicit".into()),
            smtp_host: Some("smtp.gmail.com".into()),
            smtp_port: Some(587),
            smtp_tls: Some("starttls".into()),
        },
        Oauth2Provider::Microsoft365 => {
            let tenant = tenant.trim().trim_matches('/');
            let tenant = if tenant.is_empty() { "common" } else { tenant };
            ProviderDefaults {
                authorization_endpoint: Some(format!(
                    "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize"
                )),
                token_endpoint: Some(format!(
                    "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
                )),
                scopes: vec![
                    "offline_access".into(),
                    "https://outlook.office.com/IMAP.AccessAsUser.All".into(),
                    "https://outlook.office.com/SMTP.Send".into(),
                ],
                auth_params: Vec::new(),
                auth_results_host: None,
                imap_host: Some("outlook.office365.com".into()),
                imap_port: Some(993),
                imap_tls: Some("implicit".into()),
                smtp_host: Some("smtp.office365.com".into()),
                smtp_port: Some(587),
                smtp_tls: Some("starttls".into()),
            }
        }
        Oauth2Provider::Generic => ProviderDefaults {
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: Vec::new(),
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: Some(993),
            imap_tls: Some("implicit".into()),
            smtp_host: None,
            smtp_port: Some(587),
            smtp_tls: Some("starttls".into()),
        },
    }
}

fn resolve_setup(args: SetupArgs) -> Result<ResolvedSetup> {
    let account = normalize_address(&args.account)
        .ok_or_else(|| anyhow!("--account must be a plain email address"))?;
    let admin_to = args
        .admin_to
        .as_deref()
        .map(|value| {
            normalize_address(value)
                .ok_or_else(|| anyhow!("--admin-to must be a plain email address"))
        })
        .transpose()?;
    let client_file = args
        .client_secret_json
        .as_deref()
        .map(read_oauth_client_file)
        .transpose()?;
    if args.provider == Oauth2Provider::Gmail
        && client_file
            .as_ref()
            .is_some_and(|client| client.kind != OAuthClientKind::Installed)
    {
        bail!(
            "Gmail OAuth setup requires a Google OAuth Desktop app client JSON with an installed section"
        );
    }
    let defaults = provider_defaults(args.provider, &args.tenant);

    let client_id = first_present(
        args.client_id,
        client_file.as_ref().map(|client| client.client_id.clone()),
    )
    .ok_or_else(|| {
        anyhow!("OAuth2 setup requires --client-id or --client-secret-json with client_id")
    })?;
    let client_secret = first_present(
        args.client_secret,
        client_file
            .as_ref()
            .and_then(|client| client.client_secret.clone()),
    );
    let authorization_endpoint = first_present(
        args.authorization_endpoint,
        client_file
            .as_ref()
            .and_then(|client| client.authorization_endpoint.clone())
            .filter(|_| args.provider == Oauth2Provider::Generic),
    )
    .or(defaults.authorization_endpoint.clone())
    .ok_or_else(|| anyhow!("OAuth2 setup requires --auth-url for --provider generic"))?;
    let token_endpoint = first_present(
        args.token_endpoint,
        client_file
            .as_ref()
            .and_then(|client| client.token_endpoint.clone())
            .filter(|_| args.provider == Oauth2Provider::Generic),
    )
    .or(defaults.token_endpoint.clone())
    .ok_or_else(|| anyhow!("OAuth2 setup requires --token-url for --provider generic"))?;
    validate_oauth_endpoint("--auth-url", &authorization_endpoint)?;
    validate_oauth_endpoint("--token-url", &token_endpoint)?;

    let scopes = normalize_scopes(args.scopes);
    let scopes = if scopes.is_empty() {
        defaults.scopes.clone()
    } else {
        scopes
    };
    if scopes.is_empty() {
        bail!("OAuth2 setup requires at least one --scope");
    }

    let auth_results_host =
        first_present(args.auth_results_host, defaults.auth_results_host.clone()).ok_or_else(
            || anyhow!("OAuth2 setup requires --auth-results-host for this provider"),
        )?;
    let imap_host = first_present(args.imap_host, defaults.imap_host.clone())
        .ok_or_else(|| anyhow!("OAuth2 setup requires --imap-host for this provider"))?;
    let smtp_host = first_present(args.smtp_host, defaults.smtp_host.clone())
        .ok_or_else(|| anyhow!("OAuth2 setup requires --smtp-host for this provider"))?;
    let imap_tls = normalize_secure_tls_mode(
        "--imap-tls",
        &first_present(args.imap_tls, defaults.imap_tls.clone())
            .unwrap_or_else(|| "implicit".into()),
    )?;
    let smtp_tls = normalize_secure_tls_mode(
        "--smtp-tls",
        &first_present(args.smtp_tls, defaults.smtp_tls.clone())
            .unwrap_or_else(|| "starttls".into()),
    )?;

    let state_file = args
        .state_file
        .unwrap_or_else(|| default_state_file(args.provider, &account));
    let env_file = args.env_file.unwrap_or_else(default_env_file);
    let mut auth_params = defaults.auth_params.clone();
    auth_params.extend(args.auth_params.into_iter().map(|arg| (arg.key, arg.value)));

    Ok(ResolvedSetup {
        provider: args.provider,
        account,
        client_id,
        client_secret,
        authorization_endpoint,
        token_endpoint,
        scopes,
        auth_params,
        auth_results_host,
        imap_host,
        imap_port: args.imap_port.or(defaults.imap_port).unwrap_or(993),
        imap_tls,
        smtp_host,
        smtp_port: args.smtp_port.or(defaults.smtp_port).unwrap_or(587),
        smtp_tls,
        admin_to,
        state_file,
        env_file,
        callback_host: args.callback_host.unwrap_or_else(|| match args.provider {
            Oauth2Provider::Microsoft365 => "localhost".to_string(),
            Oauth2Provider::Gmail | Oauth2Provider::Generic => "127.0.0.1".to_string(),
        }),
        callback_port: args.callback_port,
        no_browser: args.no_browser,
        force: args.force,
    })
}

fn first_present<T>(preferred: Option<T>, fallback: Option<T>) -> Option<T> {
    preferred.or(fallback)
}

fn read_oauth_client_file(path: &Path) -> Result<OAuthClientFile> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: OAuthClientJson = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse OAuth client JSON {}", path.display()))?;
    let (kind, section) = if let Some(installed) = parsed.installed {
        (OAuthClientKind::Installed, installed)
    } else if let Some(web) = parsed.web {
        (OAuthClientKind::Web, web)
    } else {
        bail!("OAuth client JSON must contain an installed or web client");
    };
    Ok(OAuthClientFile {
        kind,
        client_id: section.client_id,
        client_secret: section.client_secret,
        authorization_endpoint: section.auth_uri,
        token_endpoint: section.token_uri,
    })
}

fn normalize_scopes(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .flat_map(|value| {
            value
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|scope| !scope.is_empty())
        .collect()
}

fn validate_oauth_endpoint(name: &str, value: &str) -> Result<()> {
    if value.starts_with("https://")
        || value.starts_with("http://127.0.0.1:")
        || value.starts_with("http://localhost:")
    {
        return Ok(());
    }
    bail!("{name} must be an https URL");
}

fn normalize_secure_tls_mode(name: &str, raw: &str) -> Result<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "implicit" | "tls" => Ok("implicit".to_string()),
        "starttls" => Ok("starttls".to_string()),
        "insecure" | "none" => {
            bail!("{name} must use TLS for OAuth2 email setup; got {raw}")
        }
        other => bail!("{name} must be implicit or starttls, got {other}"),
    }
}

fn default_state_file(provider: Oauth2Provider, account: &str) -> PathBuf {
    if let Some(state_dir) = channel_setup_state_dir_from_env() {
        return state_dir
            .join(provider.id())
            .join(format!("{}.json", mailbox_id_for(account)));
    }
    state_root()
        .join("lionclaw")
        .join("email-oauth")
        .join(provider.id())
        .join(format!("{}.json", mailbox_id_for(account)))
}

fn default_env_file() -> PathBuf {
    channel_setup_env_file_from_env().unwrap_or_else(|| PathBuf::from("email.env"))
}

fn channel_setup_env_file_from_env() -> Option<PathBuf> {
    env::var_os("LIONCLAW_CHANNEL_SETUP_ENV_FILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn channel_setup_state_dir_from_env() -> Option<PathBuf> {
    env::var_os("LIONCLAW_CHANNEL_SETUP_STATE_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn state_root() -> PathBuf {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from(".lionclaw-state"))
}

impl PkcePair {
    fn generate() -> Self {
        let verifier = format!(
            "{}{}{}",
            uuid_token_part(),
            uuid_token_part(),
            uuid_token_part()
        );
        let digest = Sha256::digest(verifier.as_bytes());
        let challenge = URL_SAFE_NO_PAD.encode(digest);
        Self {
            verifier,
            challenge,
        }
    }
}

fn random_url_token() -> String {
    format!("{}{}", uuid_token_part(), uuid_token_part())
}

fn uuid_token_part() -> String {
    Uuid::new_v4().simple().to_string()
}

async fn bind_callback_listener(host: &str, port: u16) -> Result<TcpListener> {
    if host != "127.0.0.1" && host != "localhost" {
        bail!("--host must be 127.0.0.1 or localhost");
    }
    TcpListener::bind((host, port))
        .await
        .with_context(|| format!("failed to bind OAuth2 callback listener on {host}:{port}"))
}

fn build_authorization_url(
    setup: &ResolvedSetup,
    redirect_uri: &str,
    state: &str,
    pkce: &PkcePair,
) -> String {
    let mut params = vec![
        ("response_type".to_string(), "code".to_string()),
        ("client_id".to_string(), setup.client_id.clone()),
        ("redirect_uri".to_string(), redirect_uri.to_string()),
        ("scope".to_string(), setup.scopes.join(" ")),
        ("state".to_string(), state.to_string()),
        ("code_challenge".to_string(), pkce.challenge.clone()),
        ("code_challenge_method".to_string(), "S256".to_string()),
        ("login_hint".to_string(), setup.account.clone()),
    ];
    params.extend(setup.auth_params.iter().cloned());
    url_with_query(&setup.authorization_endpoint, &params)
}

fn url_with_query(base: &str, params: &[(String, String)]) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    let query = params
        .iter()
        .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}{separator}{query}")
}

fn url_encode(raw: &str) -> String {
    let mut out = String::new();
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn open_browser(url: &str) -> bool {
    let mut command = browser_command(url);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}

#[cfg(target_os = "macos")]
fn browser_command(url: &str) -> ProcessCommand {
    let mut command = ProcessCommand::new("open");
    command.arg(url);
    command
}

#[cfg(target_os = "windows")]
fn browser_command(url: &str) -> ProcessCommand {
    let mut command = ProcessCommand::new("rundll32");
    command.args(["url.dll,FileProtocolHandler", url]);
    command
}

#[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
fn browser_command(url: &str) -> ProcessCommand {
    let mut command = ProcessCommand::new("xdg-open");
    command.arg(url);
    command
}

async fn wait_for_authorization_code(
    listener: TcpListener,
    expected_state: &str,
) -> Result<String> {
    let deadline = CALLBACK_WAIT;
    timeout(deadline, async {
        loop {
            let (mut stream, _) = listener.accept().await?;
            match handle_callback_connection(&mut stream, expected_state).await {
                Ok(Some(code)) => return Ok(code),
                Ok(None) => {}
                Err(err) => return Err(err),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for OAuth2 browser callback"))?
}

async fn handle_callback_connection(
    stream: &mut TcpStream,
    expected_state: &str,
) -> Result<Option<String>> {
    let mut buffer = vec![0; HTTP_READ_LIMIT];
    let bytes = stream
        .read(&mut buffer)
        .await
        .context("failed to read OAuth2 callback request")?;
    let request = String::from_utf8_lossy(&buffer[..bytes]);
    let Some(first_line) = request.lines().next() else {
        write_callback_response(stream, 400, "Bad request").await?;
        return Ok(None);
    };
    let Some(target) = parse_http_get_target(first_line) else {
        write_callback_response(stream, 404, "Waiting for OAuth2 callback").await?;
        return Ok(None);
    };
    let parsed = parse_callback_target(target);
    if parsed.path != CALLBACK_PATH {
        write_callback_response(stream, 404, "Waiting for OAuth2 callback").await?;
        return Ok(None);
    }
    if let Some(error) = parsed.query.get("error") {
        write_callback_response(stream, 400, "OAuth2 authorization was not completed").await?;
        bail!("OAuth2 provider returned error: {error}");
    }
    let state = parsed
        .query
        .get("state")
        .ok_or_else(|| anyhow!("OAuth2 callback did not include state"))?;
    if state != expected_state {
        write_callback_response(stream, 400, "OAuth2 state did not match").await?;
        bail!("OAuth2 callback state did not match");
    }
    let code = parsed
        .query
        .get("code")
        .ok_or_else(|| anyhow!("OAuth2 callback did not include code"))?
        .clone();
    write_callback_response(
        stream,
        200,
        "OAuth2 setup complete. You can close this tab.",
    )
    .await?;
    Ok(Some(code))
}

fn parse_http_get_target(first_line: &str) -> Option<&str> {
    let mut parts = first_line.split_whitespace();
    match (parts.next(), parts.next(), parts.next()) {
        (Some("GET"), Some(target), Some(_version)) => Some(target),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedCallbackTarget {
    path: String,
    query: BTreeMap<String, String>,
}

fn parse_callback_target(target: &str) -> ParsedCallbackTarget {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    ParsedCallbackTarget {
        path: path.to_string(),
        query: parse_urlencoded_query(query),
    }
}

fn parse_urlencoded_query(query: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        values.insert(percent_decode(key), percent_decode(value));
    }
    values
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                if let (Some(hi), Some(lo)) =
                    (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
                {
                    out.push((hi << 4) | lo);
                    index += 3;
                } else {
                    out.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn write_callback_response(stream: &mut TcpStream, status: u16, text: &str) -> Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "OK",
    };
    let body = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>LionClaw Email OAuth2</title></head><body><p>{text}</p></body></html>"
    );
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("failed to write OAuth2 callback response")
}

async fn exchange_authorization_code(
    setup: &ResolvedSetup,
    redirect_uri: &str,
    code: &str,
    code_verifier: &str,
) -> Result<TokenResponse> {
    let mut form = vec![
        ("grant_type".to_string(), "authorization_code".to_string()),
        ("code".to_string(), code.to_string()),
        ("redirect_uri".to_string(), redirect_uri.to_string()),
        ("client_id".to_string(), setup.client_id.clone()),
        ("code_verifier".to_string(), code_verifier.to_string()),
    ];
    if let Some(secret) = &setup.client_secret {
        form.push(("client_secret".to_string(), secret.clone()));
    }
    post_token_form(&setup.token_endpoint, form).await
}

async fn refresh_access_token(state: &StoredOAuth2State) -> Result<TokenResponse> {
    let mut form = vec![
        ("grant_type".to_string(), "refresh_token".to_string()),
        ("refresh_token".to_string(), state.refresh_token.clone()),
        ("client_id".to_string(), state.client_id.clone()),
    ];
    if let Some(secret) = &state.client_secret {
        form.push(("client_secret".to_string(), secret.clone()));
    }
    post_token_form(&state.token_endpoint, form).await
}

async fn post_token_form(
    token_endpoint: &str,
    form: Vec<(String, String)>,
) -> Result<TokenResponse> {
    let client = reqwest::Client::builder()
        .timeout(TOKEN_REQUEST_TIMEOUT)
        .build()
        .context("failed to build OAuth2 HTTP client")?;
    let response = client
        .post(token_endpoint)
        .form(&form)
        .send()
        .await
        .with_context(|| format!("failed to call OAuth2 token endpoint {token_endpoint}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read OAuth2 token response")?;
    let parsed: TokenResponse = serde_json::from_str(&text).with_context(|| {
        format!(
            "OAuth2 token endpoint returned non-JSON response: {}",
            truncate_for_error(&text)
        )
    })?;
    if !status.is_success() || parsed.error.is_some() {
        let error = parsed.error.as_deref().unwrap_or("token_endpoint_error");
        let description = parsed.error_description.as_deref().unwrap_or("");
        bail!("OAuth2 token endpoint returned {status}: {error} {description}");
    }
    Ok(parsed)
}

fn truncate_for_error(text: &str) -> String {
    const MAX: usize = 512;
    if text.len() <= MAX {
        return text.to_string();
    }
    let end = text
        .char_indices()
        .map(|(index, ch)| index + ch.len_utf8())
        .take_while(|end| *end <= MAX)
        .last()
        .unwrap_or(0);
    format!("{}...", &text[..end])
}

fn expires_at(expires_in: Option<i64>) -> Option<DateTime<Utc>> {
    expires_in.and_then(|seconds| Utc::now().checked_add_signed(TimeDelta::seconds(seconds)))
}

fn cached_access_token(state: &StoredOAuth2State) -> Option<String> {
    let token = state.access_token.as_ref()?;
    let expires_at = state.expires_at?;
    if expires_at - Utc::now() > TimeDelta::seconds(TOKEN_REFRESH_SKEW_SECONDS) {
        Some(token.clone())
    } else {
        None
    }
}

fn read_oauth2_state(path: &Path) -> Result<StoredOAuth2State> {
    ensure_parent_dirs_without_symlinks(path, "OAuth2 state file")?;
    ensure_existing_regular_file(path, "OAuth2 state file")?;
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let state: StoredOAuth2State = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse OAuth2 state {}", path.display()))?;
    if state.version != 1 {
        bail!("unsupported OAuth2 state version {}", state.version);
    }
    Ok(state)
}

fn reserve_private_write_target(path: &Path, force: bool, label: &str) -> Result<()> {
    create_parent_dirs_without_symlinks(path, label)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if !metadata.is_file() {
                bail!("{label} {} is not a file", path.display());
            }
            if !force {
                bail!(
                    "{label} {} already exists; pass --force to replace it",
                    path.display()
                );
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
    Ok(())
}

fn create_parent_dirs_without_symlinks(path: &Path, label: &str) -> Result<()> {
    visit_parent_dirs_without_symlinks(path, label, MissingParentAction::Create)
}

fn ensure_parent_dirs_without_symlinks(path: &Path, label: &str) -> Result<()> {
    visit_parent_dirs_without_symlinks(path, label, MissingParentAction::Reject)
}

#[derive(Clone, Copy)]
enum MissingParentAction {
    Create,
    Reject,
}

fn visit_parent_dirs_without_symlinks(
    path: &Path,
    label: &str,
    missing: MissingParentAction,
) -> Result<()> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    let mut current = PathBuf::new();
    for component in parent.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                bail!(
                    "{label} parent directory {} must not contain '..'",
                    parent.display()
                );
            }
            Component::Normal(name) => {
                current.push(name);
                ensure_private_parent_dir(&current, label, missing)?;
            }
        }
    }
    Ok(())
}

fn ensure_private_parent_dir(path: &Path, label: &str, missing: MissingParentAction) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "{label} parent directory {} must not be a symlink",
                    path.display()
                );
            }
            if !metadata.is_dir() {
                bail!("{label} parent path {} is not a directory", path.display());
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => match missing {
            MissingParentAction::Create => {
                fs::create_dir(path)
                    .with_context(|| format!("failed to create {}", path.display()))?;
                set_private_dir_permissions(path)?;
            }
            MissingParentAction::Reject => {
                bail!("{label} parent directory {} does not exist", path.display());
            }
        },
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let content =
        serde_json::to_string_pretty(value).context("failed to serialize OAuth2 state")?;
    write_private_text(path, &(content + "\n"))
}

fn write_private_text(path: &Path, content: &str) -> Result<()> {
    ensure_parent_dirs_without_symlinks(path, "private file")?;
    ensure_write_target_not_symlink(path, "private file")?;
    let mut file = open_private_file(path)?;
    file.write_all(content.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    set_private_file_permissions(path)
}

fn ensure_existing_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to stat {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("{label} {} is not a file", path.display());
    }
    Ok(())
}

fn ensure_write_target_not_symlink(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if !metadata.is_file() {
                bail!("{label} {} is not a file", path.display());
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_file(path: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))
}

#[cfg(not(unix))]
fn open_private_file(path: &Path) -> Result<fs::File> {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn build_email_env_content(setup: &ResolvedSetup, executable: &Path) -> String {
    let token_command = format!(
        "{} oauth2 token --state-file {}",
        shell_quote_arg(&executable.display().to_string()),
        shell_quote_arg(&setup.state_file.display().to_string())
    );
    let mut values = BTreeMap::new();
    values.insert("EMAIL_ADDRESS", setup.account.clone());
    values.insert("EMAIL_AUTH_RESULTS_HOST", setup.auth_results_host.clone());
    values.insert("EMAIL_AUTH_MODE", "xoauth2".to_string());
    values.insert("EMAIL_XOAUTH2_TOKEN_CMD", token_command);
    values.insert("EMAIL_IMAP_HOST", setup.imap_host.clone());
    values.insert("EMAIL_IMAP_PASSWORD", String::new());
    values.insert("EMAIL_IMAP_PORT", setup.imap_port.to_string());
    values.insert("EMAIL_IMAP_USERNAME", setup.account.clone());
    values.insert("EMAIL_IMAP_TLS", setup.imap_tls.clone());
    values.insert("EMAIL_SMTP_HOST", setup.smtp_host.clone());
    values.insert("EMAIL_SMTP_PASSWORD", String::new());
    values.insert("EMAIL_SMTP_PORT", setup.smtp_port.to_string());
    values.insert("EMAIL_SMTP_USERNAME", setup.account.clone());
    values.insert("EMAIL_SMTP_TLS", setup.smtp_tls.clone());
    if let Some(admin_to) = &setup.admin_to {
        values.insert("EMAIL_ADMIN_DIGEST_TO", admin_to.clone());
    }

    let mut content = String::new();
    for (key, value) in values {
        content.push_str(key);
        content.push('=');
        content.push_str(&escape_env_value(&value));
        content.push('\n');
    }
    content
}

fn escape_env_value(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@'))
    {
        return value.to_string();
    }
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

fn shell_quote_arg(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@' | '+')
        })
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gmail_setup_resolves_full_channel_env() {
        let setup = resolve_setup(SetupArgs {
            provider: Oauth2Provider::Gmail,
            account: "Assistant@Gmail.COM".to_string(),
            client_secret_json: None,
            client_id: Some("client-id".to_string()),
            client_secret: Some("client-secret".to_string()),
            tenant: "common".to_string(),
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: Vec::new(),
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: None,
            imap_tls: None,
            smtp_host: None,
            smtp_port: None,
            smtp_tls: None,
            admin_to: Some("Operator@Example.COM".to_string()),
            env_file: Some(PathBuf::from("email.env")),
            state_file: Some(PathBuf::from("/tmp/oauth-state.json")),
            callback_host: Some("127.0.0.1".to_string()),
            callback_port: 0,
            no_browser: true,
            force: false,
        })
        .expect("setup");

        assert_eq!(setup.account, "assistant@gmail.com");
        assert_eq!(setup.auth_results_host, "mx.google.com");
        assert_eq!(setup.scopes, vec!["https://mail.google.com/"]);
        assert_eq!(setup.admin_to.as_deref(), Some("operator@example.com"));

        let env_content =
            build_email_env_content(&setup, Path::new("/usr/local/bin/lionclaw-channel-email"));
        assert!(env_content.contains("EMAIL_AUTH_MODE=xoauth2\n"));
        assert!(env_content.contains("EMAIL_IMAP_HOST=imap.gmail.com\n"));
        assert!(env_content.contains("EMAIL_IMAP_PASSWORD=\n"));
        assert!(env_content.contains("EMAIL_SMTP_PASSWORD=\n"));
        assert!(env_content.contains("EMAIL_XOAUTH2_TOKEN_CMD=\"/usr/local/bin/lionclaw-channel-email oauth2 token --state-file /tmp/oauth-state.json\"\n"));
    }

    #[test]
    fn generic_setup_requires_provider_specific_values() {
        let err = resolve_setup(SetupArgs {
            provider: Oauth2Provider::Generic,
            account: "assistant@example.com".to_string(),
            client_secret_json: None,
            client_id: Some("client-id".to_string()),
            client_secret: None,
            tenant: "common".to_string(),
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: vec!["mail.read".to_string()],
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: None,
            imap_tls: None,
            smtp_host: None,
            smtp_port: None,
            smtp_tls: None,
            admin_to: None,
            env_file: Some(PathBuf::from("email.env")),
            state_file: None,
            callback_host: Some("127.0.0.1".to_string()),
            callback_port: 0,
            no_browser: true,
            force: false,
        })
        .expect_err("generic setup should require endpoints");

        assert!(err.to_string().contains("--auth-url"));
    }

    #[test]
    fn oauth2_setup_rejects_insecure_tls_modes() {
        let err = resolve_setup(SetupArgs {
            provider: Oauth2Provider::Gmail,
            account: "assistant@gmail.com".to_string(),
            client_secret_json: None,
            client_id: Some("client-id".to_string()),
            client_secret: None,
            tenant: "common".to_string(),
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: Vec::new(),
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: None,
            imap_tls: Some("none".to_string()),
            smtp_host: None,
            smtp_port: None,
            smtp_tls: None,
            admin_to: None,
            env_file: Some(PathBuf::from("email.env")),
            state_file: None,
            callback_host: Some("127.0.0.1".to_string()),
            callback_port: 0,
            no_browser: true,
            force: false,
        })
        .expect_err("OAuth2 setup must not produce non-TLS mail settings");

        assert!(err.to_string().contains("must use TLS"));
    }

    #[test]
    fn oauth2_setup_writes_canonical_tls_modes() {
        let setup = resolve_setup(SetupArgs {
            provider: Oauth2Provider::Gmail,
            account: "assistant@gmail.com".to_string(),
            client_secret_json: None,
            client_id: Some("client-id".to_string()),
            client_secret: None,
            tenant: "common".to_string(),
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: Vec::new(),
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: None,
            imap_tls: Some("TLS".to_string()),
            smtp_host: None,
            smtp_port: None,
            smtp_tls: Some("STARTTLS".to_string()),
            admin_to: None,
            env_file: Some(PathBuf::from("email.env")),
            state_file: None,
            callback_host: Some("127.0.0.1".to_string()),
            callback_port: 0,
            no_browser: true,
            force: false,
        })
        .expect("setup");

        assert_eq!(setup.imap_tls, "implicit");
        assert_eq!(setup.smtp_tls, "starttls");
    }

    #[test]
    fn oauth_client_json_uses_installed_section() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("client.json");
        fs::write(
            &path,
            r#"{
              "installed": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "auth_uri": "https://accounts.example/authorize",
                "token_uri": "https://accounts.example/token"
              }
            }"#,
        )
        .expect("write client");

        let client = read_oauth_client_file(&path).expect("client file");
        assert_eq!(client.kind, OAuthClientKind::Installed);
        assert_eq!(client.client_id, "client-id");
        assert_eq!(client.client_secret.as_deref(), Some("client-secret"));
        assert_eq!(
            client.authorization_endpoint.as_deref(),
            Some("https://accounts.example/authorize")
        );
    }

    #[test]
    fn gmail_setup_rejects_web_client_json() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("client.json");
        fs::write(
            &path,
            r#"{
              "web": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "auth_uri": "https://accounts.example/authorize",
                "token_uri": "https://accounts.example/token"
              }
            }"#,
        )
        .expect("write client");

        let err = resolve_setup(SetupArgs {
            provider: Oauth2Provider::Gmail,
            account: "assistant@gmail.com".to_string(),
            client_secret_json: Some(path),
            client_id: None,
            client_secret: None,
            tenant: "common".to_string(),
            authorization_endpoint: None,
            token_endpoint: None,
            scopes: Vec::new(),
            auth_params: Vec::new(),
            auth_results_host: None,
            imap_host: None,
            imap_port: None,
            imap_tls: None,
            smtp_host: None,
            smtp_port: None,
            smtp_tls: None,
            admin_to: None,
            env_file: Some(PathBuf::from("email.env")),
            state_file: None,
            callback_host: Some("127.0.0.1".to_string()),
            callback_port: 0,
            no_browser: true,
            force: false,
        })
        .expect_err("Gmail setup should require desktop client JSON");

        assert!(err.to_string().contains("Desktop app client JSON"));
    }

    #[test]
    fn setup_wrapper_accepts_provider_profile() {
        let command = parse_setup_command_from_args(vec![
            "gmail".to_string(),
            "--account".to_string(),
            "assistant@gmail.com".to_string(),
            "--client-id".to_string(),
            "client-id".to_string(),
        ])
        .expect("parse setup wrapper")
        .expect("setup command");

        let Oauth2Command::Setup(args) = command else {
            panic!("expected setup command");
        };
        assert_eq!(args.provider, Oauth2Provider::Gmail);
        assert_eq!(args.account, "assistant@gmail.com");
        assert_eq!(args.client_id.as_deref(), Some("client-id"));
    }

    #[test]
    fn setup_wrapper_rejects_ambiguous_provider_selection() {
        let err = parse_setup_command_from_args(vec![
            "gmail".to_string(),
            "--provider".to_string(),
            "generic".to_string(),
            "--account".to_string(),
            "assistant@gmail.com".to_string(),
        ])
        .expect_err("provider should not be ambiguous");

        assert!(err
            .to_string()
            .contains("either positionally or with --provider"));
    }

    #[test]
    fn interactive_gmail_setup_collects_required_values() {
        let mut input = std::io::Cursor::new(
            b"\nassistant@gmail.com\noperator@example.com\n/tmp/client.json\n".to_vec(),
        );
        let mut output = Vec::new();

        let args = prompt_setup_command_with_io(&mut input, &mut output).expect("prompt setup");

        assert_eq!(args.provider, Oauth2Provider::Gmail);
        assert_eq!(args.account, "assistant@gmail.com");
        assert_eq!(args.admin_to.as_deref(), Some("operator@example.com"));
        assert_eq!(
            args.client_secret_json.as_deref(),
            Some(Path::new("/tmp/client.json"))
        );
        assert!(args.client_id.is_none());
    }

    #[test]
    fn interactive_generic_setup_collects_provider_facts() {
        let mut input = std::io::Cursor::new(
            b"generic\nassistant@example.com\n\nclient-id\n\nhttps://accounts.example/authorize\nhttps://accounts.example/token\nimap smtp\nmx.example.com\nimap.example.com\nsmtp.example.com\n".to_vec(),
        );
        let mut output = Vec::new();

        let args = prompt_setup_command_with_io(&mut input, &mut output).expect("prompt setup");

        assert_eq!(args.provider, Oauth2Provider::Generic);
        assert_eq!(args.account, "assistant@example.com");
        assert_eq!(args.client_id.as_deref(), Some("client-id"));
        assert!(args.client_secret.is_none());
        assert_eq!(
            args.authorization_endpoint.as_deref(),
            Some("https://accounts.example/authorize")
        );
        assert_eq!(args.scopes, vec!["imap smtp"]);
        assert_eq!(args.imap_host.as_deref(), Some("imap.example.com"));
        assert_eq!(args.smtp_host.as_deref(), Some("smtp.example.com"));
    }

    #[test]
    fn callback_query_decodes_code_and_state() {
        let parsed = parse_callback_target("/oauth2/callback?code=a%2Fb%2Bc&state=state+one");

        assert_eq!(parsed.path, CALLBACK_PATH);
        assert_eq!(parsed.query.get("code").map(String::as_str), Some("a/b+c"));
        assert_eq!(
            parsed.query.get("state").map(String::as_str),
            Some("state one")
        );
    }

    #[test]
    fn cached_access_token_requires_unexpired_token() {
        let state = StoredOAuth2State {
            version: 1,
            provider: "gmail".to_string(),
            account: "assistant@gmail.com".to_string(),
            token_endpoint: "https://oauth2.googleapis.com/token".to_string(),
            client_id: "client-id".to_string(),
            client_secret: None,
            refresh_token: "refresh".to_string(),
            access_token: Some("access".to_string()),
            expires_at: Some(Utc::now() + TimeDelta::seconds(120)),
            scope: vec!["https://mail.google.com/".to_string()],
        };

        assert_eq!(cached_access_token(&state).as_deref(), Some("access"));
    }

    #[test]
    fn token_error_truncation_preserves_utf8_boundaries() {
        let message = "é".repeat(300);

        let truncated = truncate_for_error(&message);

        assert!(truncated.ends_with("..."));
        assert!(truncated.len() <= 515);
    }

    #[tokio::test]
    async fn refresh_access_token_posts_refresh_grant() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = vec![0; 4096];
            let bytes = stream.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..bytes]);
            assert!(request.contains("grant_type=refresh_token"));
            assert!(request.contains("refresh_token=refresh-token"));
            let body = r#"{"access_token":"new-access-token","expires_in":3600}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        let state = StoredOAuth2State {
            version: 1,
            provider: "generic".to_string(),
            account: "assistant@example.com".to_string(),
            token_endpoint: format!("http://127.0.0.1:{port}/token"),
            client_id: "client-id".to_string(),
            client_secret: Some("client-secret".to_string()),
            refresh_token: "refresh-token".to_string(),
            access_token: None,
            expires_at: None,
            scope: vec!["mail".to_string()],
        };

        let response = refresh_access_token(&state).await.expect("refresh");

        assert_eq!(response.access_token.as_deref(), Some("new-access-token"));
        server.await.expect("server task");
    }

    #[cfg(unix)]
    #[test]
    fn oauth_state_file_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let target = temp_dir.path().join("target.json");
        let link = temp_dir.path().join("state.json");
        fs::write(
            &target,
            r#"{
              "version": 1,
              "provider": "gmail",
              "account": "assistant@gmail.com",
              "token_endpoint": "https://oauth2.googleapis.com/token",
              "client_id": "client-id",
              "client_secret": null,
              "refresh_token": "refresh",
              "access_token": null,
              "expires_at": null,
              "scope": ["https://mail.google.com/"]
            }"#,
        )
        .expect("write target");
        symlink(&target, &link).expect("symlink");

        let err = read_oauth2_state(&link).expect_err("symlink state should fail");

        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn oauth_private_write_rejects_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside");
        fs::create_dir(&outside).expect("outside dir");
        let link = temp_dir.path().join("state");
        symlink(&outside, &link).expect("state symlink");

        let err = reserve_private_write_target(
            &link.join("gmail/state.json"),
            false,
            "OAuth2 state file",
        )
        .expect_err("symlinked parent should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.join("gmail/state.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn oauth_private_write_rechecks_parent_before_writing() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("state/gmail/state.json");
        reserve_private_write_target(&path, false, "OAuth2 state file").expect("reserve state");
        fs::remove_dir(temp_dir.path().join("state/gmail")).expect("remove provider dir");
        fs::remove_dir(temp_dir.path().join("state")).expect("remove state dir");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(outside.join("gmail")).expect("outside dirs");
        symlink(&outside, temp_dir.path().join("state")).expect("state symlink");

        let err = write_private_text(&path, "{}\n").expect_err("symlinked parent should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.join("gmail/state.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn oauth_state_file_rejects_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(outside.join("gmail")).expect("outside dirs");
        fs::write(
            outside.join("gmail/state.json"),
            r#"{
              "version": 1,
              "provider": "gmail",
              "account": "assistant@gmail.com",
              "token_endpoint": "https://oauth2.googleapis.com/token",
              "client_id": "client-id",
              "client_secret": null,
              "refresh_token": "refresh",
              "access_token": null,
              "expires_at": null,
              "scope": ["https://mail.google.com/"]
            }"#,
        )
        .expect("write outside state");
        symlink(&outside, temp_dir.path().join("state")).expect("state symlink");

        let err = read_oauth2_state(&temp_dir.path().join("state/gmail/state.json"))
            .expect_err("symlinked parent should fail");

        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn oauth_private_write_creates_new_parent_dirs_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().join("state/gmail/state.json");

        reserve_private_write_target(&path, false, "OAuth2 state file").expect("reserve state");

        let state_mode = fs::metadata(temp_dir.path().join("state"))
            .expect("state metadata")
            .permissions()
            .mode();
        let provider_mode = fs::metadata(temp_dir.path().join("state/gmail"))
            .expect("provider metadata")
            .permissions()
            .mode();
        assert_eq!(state_mode & 0o077, 0);
        assert_eq!(provider_mode & 0o077, 0);
    }
}
