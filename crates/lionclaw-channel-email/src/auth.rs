use std::{
    env,
    ffi::OsString,
    fmt,
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    time::timeout,
};

use crate::diagnostics::render_operator_diagnostic;

const TOKEN_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const TOKEN_COMMAND_MAX_STDOUT: usize = 16 * 1024;
const TOKEN_COMMAND_MAX_STDERR: usize = 16 * 1024;
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
            .stderr(Stdio::piped())
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
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("failed to capture EMAIL_XOAUTH2_TOKEN_CMD stderr"))?;
        let output = match timeout(
            TOKEN_COMMAND_TIMEOUT,
            collect_token_command_output(&mut child, stdout, stderr),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                let _ = kill_child(&mut child).await;
                return Err(err);
            }
            Err(_) => {
                let _ = kill_child(&mut child).await;
                bail!(
                    "EMAIL_XOAUTH2_TOKEN_CMD timed out after {} seconds",
                    TOKEN_COMMAND_TIMEOUT.as_secs()
                );
            }
        };
        if !output.status.success() {
            if let Some(diagnostic) = output.stderr.render() {
                bail!(
                    "EMAIL_XOAUTH2_TOKEN_CMD exited with status {}; stderr: {diagnostic}",
                    output.status
                );
            }
            bail!(
                "EMAIL_XOAUTH2_TOKEN_CMD exited with status {}",
                output.status
            );
        }

        let token = String::from_utf8(output.stdout)
            .context("EMAIL_XOAUTH2_TOKEN_CMD stdout must be UTF-8 access-token text")?;
        let token = token.trim().to_string();
        validate_access_token("EMAIL_XOAUTH2_TOKEN_CMD", &token)?;
        Ok(token)
    }
}

struct TokenCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: TokenCommandDiagnostic,
}

struct TokenCommandDiagnostic {
    bytes: Vec<u8>,
    truncated: bool,
}

impl TokenCommandDiagnostic {
    fn render(&self) -> Option<String> {
        let text = String::from_utf8_lossy(&self.bytes);
        render_operator_diagnostic(&text, self.truncated)
    }
}

async fn collect_token_command_output<R, E>(
    child: &mut Child,
    stdout: R,
    stderr: E,
) -> Result<TokenCommandOutput>
where
    R: AsyncRead + Unpin,
    E: AsyncRead + Unpin,
{
    let (stdout, stderr, status) = tokio::try_join!(
        read_token_stdout(stdout),
        read_token_stderr(stderr),
        async {
            child
                .wait()
                .await
                .context("failed to wait for EMAIL_XOAUTH2_TOKEN_CMD")
        }
    )?;
    Ok(TokenCommandOutput {
        status,
        stdout,
        stderr,
    })
}

async fn read_token_stdout<R>(mut reader: R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        let bytes = reader
            .read(&mut chunk)
            .await
            .context("failed to read EMAIL_XOAUTH2_TOKEN_CMD stdout")?;
        if bytes == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(bytes) > TOKEN_COMMAND_MAX_STDOUT {
            bail!(
                "EMAIL_XOAUTH2_TOKEN_CMD stdout exceeded {} bytes",
                TOKEN_COMMAND_MAX_STDOUT
            );
        }
        output.extend_from_slice(&chunk[..bytes]);
    }
}

async fn read_token_stderr<R>(mut reader: R) -> Result<TokenCommandDiagnostic>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 512];
    loop {
        let read = reader
            .read(&mut chunk)
            .await
            .context("failed to read EMAIL_XOAUTH2_TOKEN_CMD stderr")?;
        if read == 0 {
            return Ok(TokenCommandDiagnostic { bytes, truncated });
        }
        let remaining = TOKEN_COMMAND_MAX_STDERR.saturating_sub(bytes.len());
        if remaining > 0 {
            let kept = remaining.min(read);
            bytes.extend_from_slice(&chunk[..kept]);
        }
        if read > remaining {
            truncated = true;
        }
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

pub(crate) fn validate_access_token(source: &str, token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("{source} returned an empty access token");
    }
    if token.chars().any(char::is_whitespace) {
        bail!("{source} access token must not contain whitespace");
    }
    if token.chars().any(char::is_control) {
        bail!("{source} access token must not contain control characters");
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
        let err = validate_access_token("EMAIL_XOAUTH2_TOKEN_CMD", "abc def")
            .expect_err("space should fail");

        assert!(err.to_string().contains("whitespace"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn token_command_reads_single_token_line() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let helper = temp_dir.path().join("token-helper");
        write_token_helper(&helper, "#!/bin/sh\nprintf 'access-token-123\\n'\n");

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
        let _guard = EnvRestore::set([(
            "LIONCLAW_TEST_EMAIL_TOKEN_SECRET",
            Some("should-not-reach-token-helper"),
        )]);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let helper = temp_dir.path().join("token-helper");
        write_token_helper(
            &helper,
            "#!/bin/sh\n\
             test -z \"${LIONCLAW_TEST_EMAIL_TOKEN_SECRET+x}\" || exit 41\n\
             test -n \"${PATH:-}\" || exit 42\n\
             printf 'access-token-123\\n'\n",
        );

        let command = TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", &helper.display().to_string())
            .expect("token command");

        assert_eq!(
            command.access_token().await.expect("access token"),
            "access-token-123"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn token_command_failure_includes_redacted_stderr_diagnostic() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let helper = temp_dir.path().join("token-helper");
        write_token_helper(
            &helper,
            "#!/bin/sh\n\
             printf 'invalid_grant refresh_token=secret-refresh-token revoked\\n' >&2\n\
             exit 17\n",
        );

        let command = TokenCommand::parse("EMAIL_XOAUTH2_TOKEN_CMD", &helper.display().to_string())
            .expect("token command");
        let err = command
            .access_token()
            .await
            .expect_err("failing token helper should include stderr");
        let message = err.to_string();

        assert!(message.contains("exit status"));
        assert!(message.contains("invalid_grant"));
        assert!(message.contains("revoked"));
        assert!(message.contains("[redacted]"));
        assert!(!message.contains("secret-refresh-token"));
    }

    #[test]
    fn token_command_diagnostic_redaction_keeps_safe_context() {
        let diagnostic = TokenCommandDiagnostic {
            bytes: br#"authorization: Bearer secret
error=invalid_grant&refresh_token=secret-refresh-token client_secret:"secret-client" revoked
"#
            .to_vec(),
            truncated: true,
        };

        let rendered = diagnostic.render().expect("diagnostic");

        assert!(rendered.contains("invalid_grant"));
        assert!(rendered.contains("revoked"));
        assert!(rendered.contains("[redacted]"));
        assert!(rendered.contains("[truncated]"));
        assert!(!rendered.contains("secret-refresh-token"));
        assert!(!rendered.contains("secret-client"));
        assert!(!rendered.contains("Bearer secret"));
    }

    #[cfg(unix)]
    fn write_token_helper(path: &Path, content: &str) {
        use std::{io::Write as _, os::unix::fs::PermissionsExt};

        let temp_path = path.with_extension("tmp");
        let mut file = std::fs::File::create(&temp_path).expect("create helper temp");
        file.write_all(content.as_bytes())
            .expect("write helper temp");
        file.sync_all().expect("sync helper temp");
        drop(file);
        std::fs::set_permissions(&temp_path, std::fs::Permissions::from_mode(0o700))
            .expect("chmod helper");
        std::fs::rename(&temp_path, path).expect("publish helper");
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
