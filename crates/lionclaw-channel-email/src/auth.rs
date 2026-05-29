use std::{
    env,
    ffi::OsString,
    fmt,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

const TOKEN_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const TOKEN_COMMAND_MAX_STDOUT: usize = 16 * 1024;
const TOKEN_COMMAND_ENV_ALLOWLIST: &[&str] = &[
    "ALL_PROXY",
    "APPDATA",
    "CURL_CA_BUNDLE",
    "HOME",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOCALAPPDATA",
    "NO_PROXY",
    "PATH",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SystemRoot",
    "USERPROFILE",
    "WINDIR",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_STATE_HOME",
    "all_proxy",
    "http_proxy",
    "https_proxy",
    "no_proxy",
];

#[derive(Clone)]
pub enum MailboxAuthConfig {
    Basic {
        imap_password: String,
        smtp_password: String,
    },
    Xoauth2TokenCommand {
        token_command: TokenCommand,
    },
}

impl MailboxAuthConfig {
    pub fn mode_name(&self) -> &'static str {
        match self {
            Self::Basic { .. } => "basic",
            Self::Xoauth2TokenCommand { .. } => "xoauth2",
        }
    }
}

impl fmt::Debug for MailboxAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Basic { .. } => formatter
                .debug_struct("Basic")
                .field("imap_password", &"<redacted>")
                .field("smtp_password", &"<redacted>")
                .finish(),
            Self::Xoauth2TokenCommand { token_command } => formatter
                .debug_struct("Xoauth2TokenCommand")
                .field("token_command", token_command)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub struct TokenCommand {
    executable: PathBuf,
    args: Vec<String>,
}

impl TokenCommand {
    pub fn parse(env_name: &str, raw: &str) -> Result<Self> {
        let parts = shlex::split(raw)
            .ok_or_else(|| anyhow!("{env_name} contains invalid shell-style quoting"))?;
        if parts.is_empty() {
            bail!("{env_name} must start with an absolute executable path");
        }
        for part in &parts {
            if part.chars().any(|ch| ch == '\0' || ch.is_control()) {
                bail!("{env_name} must not contain control characters");
            }
        }

        let executable = PathBuf::from(&parts[0]);
        validate_executable_path(env_name, &executable)?;
        Ok(Self {
            executable,
            args: parts.into_iter().skip(1).collect(),
        })
    }

    pub async fn access_token(&self) -> Result<String> {
        let mut child = Command::new(&self.executable)
            .args(&self.args)
            .env_clear()
            .envs(token_command_env())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start EMAIL_XOAUTH2_TOKEN_CMD executable {}",
                    self.executable.display()
                )
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture EMAIL_XOAUTH2_TOKEN_CMD stdout"))?;
        let mut output = Vec::new();
        let mut limited_stdout = stdout.take((TOKEN_COMMAND_MAX_STDOUT + 1) as u64);
        match timeout(
            TOKEN_COMMAND_TIMEOUT,
            limited_stdout.read_to_end(&mut output),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                let _ = kill_child(&mut child).await;
                return Err(err).context("failed to read EMAIL_XOAUTH2_TOKEN_CMD stdout");
            }
            Err(_) => {
                let _ = kill_child(&mut child).await;
                bail!(
                    "EMAIL_XOAUTH2_TOKEN_CMD timed out after {} seconds",
                    TOKEN_COMMAND_TIMEOUT.as_secs()
                );
            }
        }

        if output.len() > TOKEN_COMMAND_MAX_STDOUT {
            let _ = kill_child(&mut child).await;
            bail!(
                "EMAIL_XOAUTH2_TOKEN_CMD stdout exceeded {} bytes",
                TOKEN_COMMAND_MAX_STDOUT
            );
        }

        let status = match timeout(TOKEN_COMMAND_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(err)) => return Err(err).context("failed to wait for EMAIL_XOAUTH2_TOKEN_CMD"),
            Err(_) => {
                let _ = kill_child(&mut child).await;
                bail!(
                    "EMAIL_XOAUTH2_TOKEN_CMD did not exit after stdout closed within {} seconds",
                    TOKEN_COMMAND_TIMEOUT.as_secs()
                );
            }
        };
        if !status.success() {
            bail!("EMAIL_XOAUTH2_TOKEN_CMD exited with status {status}");
        }

        let token = String::from_utf8(output)
            .context("EMAIL_XOAUTH2_TOKEN_CMD stdout must be UTF-8 access-token text")?;
        let token = token.trim().to_string();
        validate_access_token(&token)?;
        Ok(token)
    }
}

impl fmt::Debug for TokenCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenCommand")
            .field("executable", &self.executable)
            .field("args", &format_args!("{} arg(s)", self.args.len()))
            .finish()
    }
}

fn validate_executable_path(env_name: &str, executable: &Path) -> Result<()> {
    if !executable.is_absolute() {
        bail!("{env_name} executable must be an absolute path");
    }
    if executable
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        bail!("{env_name} executable path must not contain '.' or '..' components");
    }
    Ok(())
}

fn validate_access_token(token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("EMAIL_XOAUTH2_TOKEN_CMD returned an empty access token");
    }
    if token.chars().any(char::is_whitespace) {
        bail!("EMAIL_XOAUTH2_TOKEN_CMD access token must not contain whitespace");
    }
    if token.chars().any(char::is_control) {
        bail!("EMAIL_XOAUTH2_TOKEN_CMD access token must not contain control characters");
    }
    Ok(())
}

async fn kill_child(child: &mut tokio::process::Child) -> Result<()> {
    child.start_kill().context("failed to kill token command")?;
    let _ = child.wait().await;
    Ok(())
}

fn token_command_env() -> Vec<(&'static str, OsString)> {
    TOKEN_COMMAND_ENV_ALLOWLIST
        .iter()
        .filter_map(|name| env::var_os(name).map(|value| (*name, value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn token_command_requires_absolute_executable() {
        let err = TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", "helper --token")
            .expect_err("relative helper should fail");

        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn token_command_preserves_quoted_arguments_without_shell() {
        let command = TokenCommand::parse(
            "EMAIL_XOAUTH2_TOKEN_CMD",
            "/usr/local/bin/helper --scope 'https://mail.google.com/'",
        )
        .expect("token command");

        assert_eq!(command.executable, PathBuf::from("/usr/local/bin/helper"));
        assert_eq!(
            command.args,
            vec![
                "--scope".to_string(),
                "https://mail.google.com/".to_string()
            ]
        );
    }

    #[test]
    fn access_token_rejects_whitespace() {
        let err = validate_access_token("abc def").expect_err("space should fail");

        assert!(err.to_string().contains("whitespace"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn token_command_reads_single_token_line() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let helper = temp_dir.path().join("token-helper");
        std::fs::write(&helper, "#!/bin/sh\nprintf 'access-token-123\\n'\n").expect("write helper");
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
            .expect("chmod helper");

        let command = TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", &helper.display().to_string())
            .expect("token command");

        assert_eq!(
            command.access_token().await.expect("access token"),
            "access-token-123"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn token_command_uses_explicit_environment_allowlist() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = EnvRestore::set([
            ("EMAIL_IMAP_PASSWORD", Some("imap-secret")),
            ("LIONCLAW_BASE_URL", Some("http://127.0.0.1:8787")),
            ("HTTPS_PROXY", Some("http://proxy.example:8080")),
        ]);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let helper = temp_dir.path().join("token-helper");
        std::fs::write(
            &helper,
            "#!/bin/sh\n\
             test -z \"${EMAIL_IMAP_PASSWORD+x}\" || exit 41\n\
             test -z \"${LIONCLAW_BASE_URL+x}\" || exit 42\n\
             test \"${HTTPS_PROXY:-}\" = 'http://proxy.example:8080' || exit 43\n\
             printf 'access-token-123\\n'\n",
        )
        .expect("write helper");
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
            .expect("chmod helper");

        let command = TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", &helper.display().to_string())
            .expect("token command");

        assert_eq!(
            command.access_token().await.expect("access token"),
            "access-token-123"
        );
    }

    struct EnvRestore {
        _guard: MutexGuard<'static, ()>,
        original: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvRestore {
        fn set<const N: usize>(values: [(&'static str, Option<&str>); N]) -> Self {
            let guard = ENV_LOCK.lock().expect("env lock");
            let original = values
                .iter()
                .map(|(name, _)| (*name, env::var_os(name)))
                .collect::<Vec<_>>();
            for (name, value) in values {
                if let Some(value) = value {
                    env::set_var(name, value);
                } else {
                    env::remove_var(name);
                }
            }
            Self {
                _guard: guard,
                original,
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (name, value) in self.original.drain(..) {
                if let Some(value) = value {
                    env::set_var(name, value);
                } else {
                    env::remove_var(name);
                }
            }
        }
    }
}
