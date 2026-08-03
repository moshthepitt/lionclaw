//! Repository-pinned command transport for the confined everyday orchestrator.
//!
//! The runtime can invoke the projected `lionclaw` client, but it never sees
//! the host executable or the `.lionclaw` store. Each request crosses one
//! private Unix socket and is re-entered through the ordinary host CLI.

use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{error::ErrorKind, Parser};
use serde::{Deserialize, Serialize};

use crate::cli::{
    Cli, Command, EnvironmentCommand, MissionCommand, MissionSkillCommand, PlanCommand,
    SkillCommand, TeamCommand, TypeCommand,
};

pub(crate) const SOCKET_MOUNT_TARGET: &str = "/runtime/lionclaw/operator.sock";
const MAX_ARGUMENTS: usize = 1_024;
const MAX_ARGUMENT_BYTES: usize = 256 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 8;
const MAX_REQUEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const GENERIC_METHODS: [&str; 5] = [
    "design",
    "optimization",
    "research",
    "review",
    "software-dev",
];

#[derive(Debug, Deserialize, Serialize)]
struct BridgeRequest {
    token: String,
    args: Vec<String>,
    stdin: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct BridgeResponse {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

pub(crate) struct OperatorBridge {
    _socket_dir: tempfile::TempDir,
    socket_path: PathBuf,
    token: String,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone)]
enum CommandBackend {
    Process(PathBuf),
    InProcess(crate::cli::MissionTransports),
}

impl OperatorBridge {
    pub(crate) fn start(
        repo: &Path,
        transports: Option<crate::cli::MissionTransports>,
    ) -> Result<Self> {
        let backend = match transports {
            Some(transports) => CommandBackend::InProcess(transports),
            None => CommandBackend::Process(
                std::env::current_exe().context("resolving the host LionClaw executable")?,
            ),
        };
        Self::start_with_backend(repo, backend)
    }

    #[cfg(test)]
    fn start_with_executable(repo: &Path, executable: PathBuf) -> Result<Self> {
        Self::start_with_backend(repo, CommandBackend::Process(executable))
    }

    fn start_with_backend(repo: &Path, backend: CommandBackend) -> Result<Self> {
        let repo = repo
            .canonicalize()
            .with_context(|| format!("resolving bridge repository '{}'", repo.display()))?;
        let socket_dir = tempfile::Builder::new()
            .prefix("lionclaw-operator-")
            .tempdir()
            .context("creating the operator bridge directory")?;
        let socket_path = socket_dir.path().join("operator.sock");
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("binding operator bridge '{}'", socket_path.display()))?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
            .context("protecting the operator bridge socket")?;
        listener
            .set_nonblocking(true)
            .context("configuring the operator bridge socket")?;

        let token = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
            socket_path.to_string_lossy().as_bytes(),
        ));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_token = token.clone();
        let worker = std::thread::Builder::new()
            .name("lionclaw-operator-bridge".to_string())
            .spawn(move || {
                serve(listener, backend, &repo, &worker_token, worker_stop);
            })
            .context("starting the operator bridge")?;

        Ok(Self {
            _socket_dir: socket_dir,
            socket_path,
            token,
            stop,
            worker: Some(worker),
        })
    }

    pub(crate) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub(crate) fn client_script(&self) -> Result<String> {
        let token = serde_json::to_string(&self.token)?;
        Ok(format!(
            r#"#!/usr/bin/env node
"use strict";
const net = require("node:net");

(async () => {{
  const chunks = [];
  if (!process.stdin.isTTY) {{
    for await (const chunk of process.stdin) chunks.push(chunk);
  }}
  const request = JSON.stringify({{
    token: {token},
    args: process.argv.slice(2),
    stdin: Buffer.concat(chunks).toString("utf8"),
  }}) + "\n";
  const response = await new Promise((resolve, reject) => {{
    let data = "";
    const socket = net.createConnection({socket});
    socket.setEncoding("utf8");
    socket.on("connect", () => socket.end(request));
    socket.on("data", (chunk) => {{
      data += chunk;
      if (data.length > {maximum}) {{
        socket.destroy(new Error("LionClaw bridge response exceeded its limit"));
      }}
    }});
    socket.on("end", () => resolve(data));
    socket.on("error", reject);
  }});
  const result = JSON.parse(response);
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  process.exitCode = Number.isInteger(result.exit_code) ? result.exit_code : 1;
}})().catch((error) => {{
  process.stderr.write(`lionclaw bridge failed: ${{error.message}}\n`);
  process.exitCode = 1;
}});
"#,
            socket = serde_json::to_string(SOCKET_MOUNT_TARGET)?,
            maximum = MAX_OUTPUT_BYTES * 2,
        ))
    }
}

impl Drop for OperatorBridge {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn serve(
    listener: UnixListener,
    backend: CommandBackend,
    repo: &Path,
    token: &str,
    stop: Arc<AtomicBool>,
) {
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handlers = Vec::new();
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _address)) => {
                if active.fetch_add(1, Ordering::AcqRel) >= MAX_CONCURRENT_REQUESTS {
                    active.fetch_sub(1, Ordering::AcqRel);
                    let _ = refuse_busy(stream);
                    continue;
                }
                let backend = backend.clone();
                let repo = repo.to_path_buf();
                let token = token.to_string();
                let request_active = Arc::clone(&active);
                let request_stop = Arc::clone(&stop);
                let spawned = std::thread::Builder::new()
                    .name("lionclaw-operator-command".to_string())
                    .spawn(move || {
                        let _active = ActiveRequest(request_active);
                        let _ = handle_connection(stream, &backend, &repo, &token, &request_stop);
                    });
                match spawned {
                    Ok(handler) => handlers.push(handler),
                    Err(_) => {
                        active.fetch_sub(1, Ordering::AcqRel);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
        reap_finished_handlers(&mut handlers);
    }
    for handler in handlers {
        let _ = handler.join();
    }
}

fn reap_finished_handlers(handlers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < handlers.len() {
        if handlers[index].is_finished() {
            let handler = handlers.swap_remove(index);
            let _ = handler.join();
        } else {
            index += 1;
        }
    }
}

struct ActiveRequest(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn refuse_busy(mut stream: UnixStream) -> Result<()> {
    serde_json::to_writer(
        &mut stream,
        &BridgeResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: "lionclaw bridge refused command: concurrent request limit reached\n"
                .to_string(),
        },
    )
    .context("writing busy operator bridge response")
}

fn handle_connection(
    mut stream: UnixStream,
    backend: &CommandBackend,
    repo: &Path,
    token: &str,
    stop: &AtomicBool,
) -> Result<()> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("setting operator bridge request timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("setting operator bridge response timeout")?;
    let response = match read_request(&stream).and_then(|request| {
        if request.token != token {
            bail!("operator bridge authentication refused");
        }
        execute_request(backend, repo, request, stop)
    }) {
        Ok(response) => response,
        Err(error) => BridgeResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: format!("lionclaw bridge refused command: {error:#}\n"),
        },
    };
    serde_json::to_writer(&mut stream, &response).context("writing operator bridge response")?;
    stream.flush().context("flushing operator bridge response")
}

fn read_request(stream: &UnixStream) -> Result<BridgeRequest> {
    let mut bytes = Vec::new();
    BufReader::new(stream)
        .take(MAX_REQUEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("reading operator bridge request")?;
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        bail!("operator bridge request exceeded its limit");
    }
    serde_json::from_slice(&bytes).context("decoding operator bridge request")
}

fn execute_request(
    backend: &CommandBackend,
    repo: &Path,
    request: BridgeRequest,
    stop: &AtomicBool,
) -> Result<BridgeResponse> {
    let args = validate_and_normalize_args(request.args)?;
    match backend {
        CommandBackend::Process(executable) => {
            execute_process_request(executable, repo, args, request.stdin, stop)
        }
        CommandBackend::InProcess(transports) => {
            execute_in_process_request(repo, args, request.stdin, transports.clone(), stop)
        }
    }
}

fn execute_process_request(
    executable: &Path,
    repo: &Path,
    args: Vec<String>,
    stdin_text: String,
    stop: &AtomicBool,
) -> Result<BridgeResponse> {
    let mut command = ProcessCommand::new(executable);
    command
        .args(&args)
        .current_dir(repo)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    crate::cli::isolate_driver_process_group(&mut command);
    let mut child = command.spawn().with_context(|| {
        format!(
            "launching host LionClaw executable '{}'",
            executable.display()
        )
    })?;
    let stdin = child
        .stdin
        .take()
        .context("host LionClaw stdin is absent")?;
    let stdin_bytes = stdin_text.into_bytes();
    let stdin_writer = std::thread::spawn(move || -> Result<()> {
        let mut stdin = stdin;
        stdin
            .write_all(&stdin_bytes)
            .context("forwarding LionClaw command stdin")
    });
    let stdout = child
        .stdout
        .take()
        .context("host LionClaw stdout is absent")?;
    let stderr = child
        .stderr
        .take()
        .context("host LionClaw stderr is absent")?;
    let (overflow_tx, overflow_rx) = mpsc::channel();
    let stdout_reader = bounded_output_reader(stdout, "stdout", overflow_tx.clone());
    let stderr_reader = bounded_output_reader(stderr, "stderr", overflow_tx);

    let mut interruption = None;
    let status = loop {
        if stop.load(Ordering::Acquire) {
            crate::cli::terminate_driver_process_group(&mut child)?;
            interruption = Some("operator bridge stopped with a command in flight");
            break None;
        }
        if let Ok(stream) = overflow_rx.try_recv() {
            crate::cli::terminate_driver_process_group(&mut child)?;
            interruption = Some(match stream {
                "stdout" => "host LionClaw stdout exceeded its limit",
                _ => "host LionClaw stderr exceeded its limit",
            });
            break None;
        }
        if let Some(status) = child
            .try_wait()
            .context("waiting for host LionClaw command")?
        {
            break Some(status);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdin_result = stdin_writer
        .join()
        .map_err(|_| anyhow::anyhow!("host LionClaw stdin writer panicked"))?;
    let stdout = join_output_reader(stdout_reader, "stdout")?;
    let stderr = join_output_reader(stderr_reader, "stderr")?;
    if let Some(interruption) = interruption {
        bail!("{interruption}");
    }
    stdin_result?;
    let status = status.context("host LionClaw command ended without a status")?;
    Ok(BridgeResponse {
        exit_code: status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    })
}

fn execute_in_process_request(
    repo: &Path,
    mut args: Vec<String>,
    stdin: String,
    transports: crate::cli::MissionTransports,
    stop: &AtomicBool,
) -> Result<BridgeResponse> {
    if stop.load(Ordering::Acquire) {
        bail!("operator bridge stopped with a command in flight");
    }
    let mut stdin_file = None;
    for index in 1..args.len() {
        if args[index - 1] == "--file" && args[index] == "-" {
            let mut file =
                tempfile::NamedTempFile::new().context("creating in-process bridge stdin file")?;
            file.write_all(stdin.as_bytes())
                .context("writing in-process bridge stdin")?;
            args[index] = file.path().to_string_lossy().into_owned();
            stdin_file = Some(file);
            break;
        }
    }
    if args.first().is_some_and(|command| command == "mission")
        && args.get(1).is_none_or(|command| command != "type")
    {
        args.push("--repo".to_string());
        args.push(repo.to_string_lossy().into_owned());
    }
    let cli =
        Cli::try_parse_from(std::iter::once("lionclaw").chain(args.iter().map(String::as_str)))
            .context("parsing validated in-process bridge command")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building in-process bridge runtime")?;
    let exit = runtime.block_on(crate::cli::run_with_transports(cli, transports))?;
    drop(stdin_file);
    let exit_code = if exit == std::process::ExitCode::SUCCESS {
        0
    } else if exit == std::process::ExitCode::FAILURE {
        1
    } else {
        i32::from(crate::everyday::NONTERMINAL_EXIT_CODE)
    };
    Ok(BridgeResponse {
        exit_code,
        stdout: String::new(),
        stderr: String::new(),
    })
}

fn bounded_output_reader(
    mut stream: impl Read + Send + 'static,
    label: &'static str,
    overflow: mpsc::Sender<&'static str>,
) -> JoinHandle<Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0u8; 8 * 1024];
        loop {
            let read = stream
                .read(&mut buffer)
                .with_context(|| format!("reading host LionClaw {label}"))?;
            if read == 0 {
                return Ok(output);
            }
            if output.len().saturating_add(read) > MAX_OUTPUT_BYTES {
                let _ = overflow.send(label);
                bail!("host LionClaw {label} exceeded its limit");
            }
            output.extend_from_slice(&buffer[..read]);
        }
    })
}

fn join_output_reader(reader: JoinHandle<Result<Vec<u8>>>, label: &str) -> Result<Vec<u8>> {
    reader
        .join()
        .map_err(|_| anyhow::anyhow!("host LionClaw {label} reader panicked"))?
}

fn validate_and_normalize_args(args: Vec<String>) -> Result<Vec<String>> {
    if args.len() > MAX_ARGUMENTS
        || args.iter().map(String::len).sum::<usize>() > MAX_ARGUMENT_BYTES
    {
        bail!("operator bridge command arguments exceeded their limit");
    }
    let args = normalize_workspace_file_args(args)?;
    let parsed =
        Cli::try_parse_from(std::iter::once("lionclaw").chain(args.iter().map(String::as_str)));
    let cli = match parsed {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            return Ok(args);
        }
        Err(error) => bail!("invalid LionClaw command: {}", error.render().ansi()),
    };
    validate_cli(&cli)?;
    Ok(args)
}

fn normalize_workspace_file_args(mut args: Vec<String>) -> Result<Vec<String>> {
    const PATH_FLAGS: [&str; 4] = ["--feedback-file", "--file", "--instructions-file", "--path"];
    for index in 0..args.len() {
        if ["--repo", "--state-dir", "--handshake"].contains(&args[index].as_str())
            || ["--repo=", "--state-dir=", "--handshake="]
                .iter()
                .any(|prefix| args[index].starts_with(prefix))
        {
            bail!("the everyday bridge pins the repository and host state");
        }
        if index > 0 && PATH_FLAGS.contains(&args[index - 1].as_str()) {
            args[index] = normalize_workspace_file(&args[index])?;
            continue;
        }
        for flag in PATH_FLAGS {
            if let Some(value) = args[index].strip_prefix(&format!("{flag}=")) {
                args[index] = format!("{flag}={}", normalize_workspace_file(value)?);
                break;
            }
        }
    }
    Ok(args)
}

fn normalize_workspace_file(raw: &str) -> Result<String> {
    if raw == "-" {
        return Ok(raw.to_string());
    }
    let path = Path::new(raw);
    let relative = if path.is_absolute() {
        path.strip_prefix(lionclaw_confinement::WORKSPACE_MOUNT_TARGET)
            .with_context(|| format!("file path '{raw}' is outside the pinned repository"))?
    } else {
        path
    };
    let safe = lionclaw_runtime_api::safe_relative_path(relative)
        .filter(|path| !path.as_os_str().is_empty())
        .with_context(|| format!("file path '{raw}' is not a repository-relative file"))?;
    safe.to_str()
        .map(str::to_string)
        .with_context(|| format!("file path '{raw}' is not valid UTF-8"))
}

fn validate_cli(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::Install(args) if args.mission_types.is_empty() => Ok(()),
        Command::Doctor(_) | Command::Man(_) => Ok(()),
        Command::Mission(command) => validate_mission_command(command),
        Command::Install(_) => bail!("the everyday bridge installs only bundled mission types"),
        Command::Run(_) => bail!("nested `lionclaw run` is not available through the bridge"),
        Command::NetworkProxy(_) | Command::NetworkProxyHealth(_) => {
            bail!("network proxy is OCI boundary plumbing, not a bridge command")
        }
        Command::Skill(SkillCommand::Add(_) | SkillCommand::Remove(_)) => {
            bail!("installed mission types are host-owned through the everyday bridge")
        }
    }
}

fn validate_mission_command(command: &MissionCommand) -> Result<()> {
    match command {
        MissionCommand::Start(args) => {
            require_generic_method(&args.mission_type)?;
            require_pinned_repo(args.repo.as_deref())
        }
        MissionCommand::Advance(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Status(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Guide(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Environment(EnvironmentCommand::Show(args)) => {
            require_pinned_repo(args.repo.as_deref())
        }
        MissionCommand::Environment(EnvironmentCommand::Use(args)) => {
            require_pinned_repo(args.repo.as_deref())
        }
        MissionCommand::Report(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Apply(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Log(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Inbox(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Send(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Plan(PlanCommand::Show(args)) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Plan(PlanCommand::Propose(args)) => {
            require_pinned_repo(args.repo.as_deref())
        }
        MissionCommand::Team(command) => validate_team_command(command),
        MissionCommand::Skill(MissionSkillCommand::Add(args)) => {
            require_pinned_repo(args.repo.as_deref())?;
            if args.git.is_some() {
                bail!("the everyday bridge accepts only a repository-local --path skill source");
            }
            Ok(())
        }
        MissionCommand::Decide(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Finish(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Abort(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Stop(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Extend(args) => require_pinned_repo(args.repo.as_deref()),
        MissionCommand::Continue(args) => require_pinned_repo(args.control.repo.as_deref()),
        MissionCommand::Type(TypeCommand::List(_)) => Ok(()),
        MissionCommand::Type(TypeCommand::Show(args)) => require_generic_method(&args.mission_type),
        MissionCommand::Type(TypeCommand::Check(args)) => {
            require_generic_method(&args.mission_type)
        }
        MissionCommand::Driver(_) | MissionCommand::DriverStderr(_) => {
            bail!("internal mission driver commands are not available through the bridge")
        }
        MissionCommand::SelfTest(_) => {
            bail!("mission self-test is not available through the everyday bridge")
        }
    }
}

fn validate_team_command(command: &TeamCommand) -> Result<()> {
    match command {
        TeamCommand::Show(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::Add(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::Reassign(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::Retire(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::SetRuntime(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::GuideSet(args) => require_pinned_repo(args.repo.as_deref()),
        TeamCommand::AssignSkill(args) => require_pinned_repo(args.repo.as_deref()),
    }
}

fn require_pinned_repo(repo: Option<&Path>) -> Result<()> {
    if repo.is_some() {
        bail!("the everyday bridge pins the repository");
    }
    Ok(())
}

fn require_generic_method(method: &str) -> Result<()> {
    if !GENERIC_METHODS.contains(&method) {
        bail!("mission type '{method}' is not one of the five installed generic methods");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_files_are_normalized_but_host_paths_and_parent_traversal_are_refused() {
        assert_eq!(
            validate_and_normalize_args(vec![
                "mission".into(),
                "plan".into(),
                "propose".into(),
                "--file".into(),
                "/workspace/.tmp/proposal.json".into(),
            ])
            .unwrap(),
            ["mission", "plan", "propose", "--file", ".tmp/proposal.json",]
        );
        assert!(validate_and_normalize_args(vec![
            "mission".into(),
            "plan".into(),
            "propose".into(),
            "--file".into(),
            "../proposal.json".into(),
        ])
        .is_err());
        assert!(validate_and_normalize_args(vec![
            "mission".into(),
            "guide".into(),
            "--repo".into(),
            "/etc".into(),
        ])
        .is_err());
    }

    #[test]
    fn bridge_exposes_validated_mission_surface_without_nested_launch_or_custom_types() {
        validate_and_normalize_args(vec![
            "mission".into(),
            "start".into(),
            "--type".into(),
            "research".into(),
            "--objective".into(),
            "inspect the repository".into(),
        ])
        .unwrap();
        validate_and_normalize_args(vec![
            "mission".into(),
            "decide".into(),
            "m000000000000".into(),
            "plan-1".into(),
            "approve".into(),
            "--justification".into(),
            "the current advertised choice permits it".into(),
        ])
        .unwrap();
        assert!(validate_and_normalize_args(vec!["run".into(), "codex".into()]).is_err());
        assert!(validate_and_normalize_args(vec![
            "mission".into(),
            "start".into(),
            "--type".into(),
            "/tmp/custom".into(),
            "--objective".into(),
            "escape".into(),
        ])
        .is_err());
    }

    #[test]
    fn bridge_refuses_host_local_git_skill_sources() {
        assert!(validate_and_normalize_args(vec![
            "mission".into(),
            "skill".into(),
            "add".into(),
            "--git".into(),
            "file:///home/operator/private-skill".into(),
            "--rev".into(),
            "main".into(),
        ])
        .unwrap_err()
        .to_string()
        .contains("repository-local"));
    }

    #[test]
    fn private_socket_round_trip_forwards_argv_without_a_shell() {
        let repo = tempfile::tempdir().unwrap();
        let bridge =
            OperatorBridge::start_with_executable(repo.path(), PathBuf::from("/bin/echo")).unwrap();
        let request = BridgeRequest {
            token: bridge.token.clone(),
            args: vec![
                "mission".into(),
                "type".into(),
                "list".into(),
                "--json".into(),
            ],
            stdin: String::new(),
        };
        let mut stream = UnixStream::connect(bridge.socket_path()).unwrap();
        serde_json::to_writer(&mut stream, &request).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let response: BridgeResponse = serde_json::from_reader(stream).unwrap();

        assert_eq!(response.exit_code, 0);
        assert_eq!(response.stdout, "mission type list --json\n");
        assert!(response.stderr.is_empty());
    }

    #[test]
    fn dropping_bridge_kills_and_reaps_an_inflight_host_command() {
        let repo = tempfile::tempdir().unwrap();
        let fixture = tempfile::tempdir().unwrap();
        let marker = fixture.path().join("pid");
        let completed = fixture.path().join("completed");
        let executable = fixture.path().join("slow-command");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nsleep 30\nprintf done > '{}'\n",
                marker.display(),
                completed.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let bridge = OperatorBridge::start_with_executable(repo.path(), executable).unwrap();
        let socket = bridge.socket_path().to_path_buf();
        let token = bridge.token.clone();
        let request = std::thread::spawn(move || {
            let request = BridgeRequest {
                token,
                args: vec![
                    "mission".into(),
                    "type".into(),
                    "list".into(),
                    "--json".into(),
                ],
                stdin: String::new(),
            };
            let mut stream = UnixStream::connect(socket).unwrap();
            serde_json::to_writer(&mut stream, &request).unwrap();
            stream.shutdown(std::net::Shutdown::Write).unwrap();
            serde_json::from_reader::<_, BridgeResponse>(stream)
        });
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let pid = std::fs::read_to_string(&marker).expect("host command started");

        drop(bridge);

        request.join().unwrap().unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        assert!(!completed.exists());
    }

    #[test]
    fn projected_executable_is_a_portable_client_not_the_host_binary() {
        let repo = tempfile::tempdir().unwrap();
        let bridge =
            OperatorBridge::start_with_executable(repo.path(), PathBuf::from("/bin/true")).unwrap();
        let script = bridge.client_script().unwrap();

        assert!(script.starts_with("#!/usr/bin/env node\n"));
        assert!(script.contains(SOCKET_MOUNT_TARGET));
        assert!(!script.contains("/bin/true"));
    }
}
