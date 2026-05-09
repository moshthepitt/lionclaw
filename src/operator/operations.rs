use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
    process::Stdio,
};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::home::LionClawHome;

use super::{
    config::{ChannelLaunchMode, OperatorConfig},
    reconcile::{down, resolve_stack_binaries, up_for_work_root},
    redaction::SecretRedactor,
    runtime::resolve_runtime_id,
    services::{channel_unit_name, daemon_unit_name, existing_service_identity, ServiceManager},
    target::{list_project_instance_statuses, InstanceStatusEntry},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackOperation {
    Up,
    Down,
}

#[derive(Debug, Clone)]
pub struct InstanceOperationResult {
    pub instance: String,
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ProjectOperationReport {
    pub results: Vec<InstanceOperationResult>,
}

impl ProjectOperationReport {
    pub fn has_failures(&self) -> bool {
        self.results.iter().any(|result| !result.ok)
    }

    pub fn render(&self) -> String {
        if self.results.is_empty() {
            return "no project instances found\n".to_string();
        }
        let mut output = String::new();
        for result in &self.results {
            let status = if result.ok { "ok" } else { "error" };
            output.push_str(&format!(
                "{status}: {}: {}\n",
                result.instance, result.message
            ));
        }
        output
    }
}

pub async fn up_instance<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    work_root: &Path,
) -> Result<String> {
    let config = OperatorConfig::load(home).await?;
    let runtime_id = resolve_runtime_id(&config, None).map_err(|_| {
        anyhow!("no default runtime configured for selected instance\nRun:\n  lionclaw configure --runtime codex")
    })?;
    let binaries = resolve_stack_binaries()?;
    let state = up_for_work_root(home, manager, &runtime_id, &binaries, work_root).await?;
    let worker_count = state
        .config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Service)
        .count();
    Ok(format!(
        "started daemon and {worker_count} service worker(s) with runtime {runtime_id}"
    ))
}

pub async fn down_instance<M: ServiceManager>(home: &LionClawHome, manager: &M) -> Result<String> {
    down(home, manager).await?;
    Ok("stopped owned managed units".to_string())
}

pub async fn operate_project_instances<M: ServiceManager>(
    project_root: &Path,
    manager: &M,
    operation: StackOperation,
) -> ProjectOperationReport {
    let entries = match list_project_instance_statuses(project_root) {
        Ok(entries) => entries,
        Err(err) => {
            return ProjectOperationReport {
                results: vec![InstanceOperationResult {
                    instance: "project".to_string(),
                    ok: false,
                    message: err.to_string(),
                }],
            };
        }
    };

    let mut results = Vec::new();
    for entry in entries {
        let name = entry.name.clone();
        let home = LionClawHome::new(entry.home.clone());
        let outcome = match operation {
            StackOperation::Up => match entry.work_root.as_deref() {
                Some(work_root) => up_instance(&home, manager, work_root).await,
                None => Err(anyhow!(
                    "{}",
                    entry
                        .work_root_finding
                        .unwrap_or_else(|| "work root is not configured".to_string())
                )),
            },
            StackOperation::Down => down_instance(&home, manager).await,
        };
        match outcome {
            Ok(message) => results.push(InstanceOperationResult {
                instance: name,
                ok: true,
                message,
            }),
            Err(err) => results.push(InstanceOperationResult {
                instance: name,
                ok: false,
                message: err.to_string(),
            }),
        }
    }

    ProjectOperationReport { results }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFilter {
    Default,
    Daemon,
    Workers,
    Worker(String),
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    pub follow: bool,
    pub tail: usize,
    pub since: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogComponent {
    pub home: LionClawHome,
    pub instance: Option<String>,
    pub component: String,
    pub unit: String,
}

impl LogComponent {
    fn prefix(&self) -> String {
        match self.instance.as_deref() {
            Some(instance) => format!("{instance}/{}", self.component),
            None => self.component.clone(),
        }
    }
}

pub async fn selected_log_components(
    home: &LionClawHome,
    instance: Option<&str>,
    filter: &LogFilter,
) -> Result<Vec<LogComponent>> {
    let Some(identity) = existing_service_identity(home)? else {
        return Ok(Vec::new());
    };
    let config = OperatorConfig::load(home).await?;
    let instance = instance.map(str::to_string);
    let daemon = LogComponent {
        home: home.clone(),
        instance: instance.clone(),
        component: "daemon".to_string(),
        unit: daemon_unit_name(&identity),
    };
    let worker_components = config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Service)
        .map(|channel| LogComponent {
            home: home.clone(),
            instance: instance.clone(),
            component: channel.id.clone(),
            unit: channel_unit_name(&identity, &channel.id),
        })
        .collect::<Vec<_>>();

    match filter {
        LogFilter::Default => {
            let mut components = vec![daemon];
            components.extend(worker_components);
            Ok(components)
        }
        LogFilter::Daemon => Ok(vec![daemon]),
        LogFilter::Workers => Ok(worker_components),
        LogFilter::Worker(channel_id) => {
            let Some(channel) = config
                .channels
                .iter()
                .find(|channel| &channel.id == channel_id)
            else {
                return Err(anyhow!(
                    "channel '{channel_id}' is not configured for the selected instance"
                ));
            };
            if channel.launch_mode != ChannelLaunchMode::Service {
                return Err(anyhow!(
                    "channel '{channel_id}' is configured as {}; logs --worker targets background channel workers only",
                    channel.launch_mode.as_str()
                ));
            }
            Ok(vec![LogComponent {
                home: home.clone(),
                instance,
                component: channel.id.clone(),
                unit: channel_unit_name(&identity, &channel.id),
            }])
        }
    }
}

pub async fn project_log_components(
    project_root: &Path,
    filter: &LogFilter,
) -> Result<Vec<LogComponent>> {
    let mut components = Vec::new();
    for entry in list_project_instance_statuses(project_root)? {
        let home = LionClawHome::new(entry.home);
        components.extend(selected_log_components(&home, Some(&entry.name), filter).await?);
    }
    Ok(components)
}

pub async fn write_journal_logs<W: Write>(
    components: &[LogComponent],
    options: &LogOptions,
    prefix_lines: bool,
    writer: &mut W,
) -> Result<()> {
    if components.is_empty() {
        return Ok(());
    }

    let homes = unique_component_homes(components);
    let redactor = SecretRedactor::from_homes(homes.iter())?;
    let render = LogRenderContext::new(components, prefix_lines);

    if options.follow {
        stream_journal_logs(components, options, &redactor, &render, writer).await
    } else {
        let output = read_journal_logs(components, options).await?;
        for raw in output.lines() {
            if let Some(line) = render.render(raw, &redactor) {
                writeln!(writer, "{line}")?;
            }
        }
        Ok(())
    }
}

fn unique_component_homes(components: &[LogComponent]) -> Vec<LionClawHome> {
    let mut seen = BTreeSet::new();
    let mut homes = Vec::new();
    for component in components {
        let root = component.home.root();
        if seen.insert(root.clone()) {
            homes.push(component.home.clone());
        }
    }
    homes
}

async fn read_journal_logs(components: &[LogComponent], options: &LogOptions) -> Result<String> {
    let output = journal_command(components, options)
        .output()
        .await
        .context("failed to run journalctl")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.contains("No journal files were found") || stderr.contains("No entries") {
            return Ok(String::new());
        }
        return Err(anyhow!("journalctl failed: {stderr}"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn stream_journal_logs<W: Write>(
    components: &[LogComponent],
    options: &LogOptions,
    redactor: &SecretRedactor,
    render: &LogRenderContext,
    writer: &mut W,
) -> Result<()> {
    let mut child = journal_command(components, options)
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to run journalctl")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("journalctl stdout was not captured"))?;
    let mut lines = BufReader::new(stdout).lines();
    while let Some(raw) = lines
        .next_line()
        .await
        .context("failed to read journalctl output")?
    {
        if let Some(line) = render.render(&raw, redactor) {
            writeln!(writer, "{line}")?;
            writer.flush()?;
        }
    }
    let status = child
        .wait()
        .await
        .context("failed to wait for journalctl")?;
    if !status.success() {
        return Err(anyhow!("journalctl exited with status {status}"));
    }
    Ok(())
}

fn journal_command(components: &[LogComponent], options: &LogOptions) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("journalctl");
    command
        .arg("--user")
        .arg("--no-pager")
        .arg("--output=json")
        .arg("--lines")
        .arg(options.tail.to_string());
    if options.follow {
        command.arg("--follow");
    }
    if let Some(since) = options.since.as_deref() {
        command.arg("--since").arg(since);
    }
    for unit in components {
        command.arg("-u").arg(&unit.unit);
    }
    command
}

struct LogRenderContext {
    prefixes_by_unit: BTreeMap<String, String>,
    prefix_lines: bool,
}

impl LogRenderContext {
    fn new(components: &[LogComponent], prefix_lines: bool) -> Self {
        let prefixes_by_unit = components
            .iter()
            .map(|component| (component.unit.clone(), component.prefix()))
            .collect();
        Self {
            prefixes_by_unit,
            prefix_lines,
        }
    }

    fn render(&self, raw: &str, redactor: &SecretRedactor) -> Option<String> {
        let parsed = serde_json::from_str::<Value>(raw).ok();
        let decoded_message = parsed.as_ref().and_then(journal_message);
        let redacted = match decoded_message.as_deref() {
            Some(message) => redactor.redact(message),
            None => redactor.redact(raw),
        };
        if !self.prefix_lines {
            return Some(redacted);
        }

        let prefix = parsed
            .as_ref()
            .and_then(record_unit)
            .and_then(|unit| self.prefixes_by_unit.get(unit))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        Some(format!("{prefix} | {redacted}"))
    }
}

fn record_unit(record: &Value) -> Option<&str> {
    journal_field(record, "_SYSTEMD_USER_UNIT")
        .or_else(|| journal_field(record, "UNIT"))
        .or_else(|| journal_field(record, "SYSLOG_IDENTIFIER"))
}

fn journal_message(record: &Value) -> Option<String> {
    match record.get("MESSAGE")? {
        Value::String(value) => Some(value.clone()),
        Value::Array(values) => {
            if let Some(value) = values.iter().find_map(|value| value.as_str()) {
                return Some(value.to_string());
            }
            let bytes = values
                .iter()
                .map(|value| value.as_u64().and_then(|byte| u8::try_from(byte).ok()))
                .collect::<Option<Vec<_>>>()?;
            String::from_utf8(bytes).ok()
        }
        _ => None,
    }
}

fn journal_field<'a>(record: &'a Value, key: &str) -> Option<&'a str> {
    match record.get(key)? {
        Value::String(value) => Some(value.as_str()),
        Value::Array(values) => values.iter().find_map(|value| value.as_str()),
        _ => None,
    }
}

pub fn no_managed_units_message(all: bool) -> &'static str {
    if all {
        "no LionClaw-managed background units found for this project; run lionclaw up or lionclaw connect <channel>"
    } else {
        "no LionClaw-managed background units found for the selected instance; run lionclaw up or lionclaw connect <channel>"
    }
}

pub fn work_root_for_operation(entry: &InstanceStatusEntry) -> Result<&Path> {
    entry.work_root.as_deref().ok_or_else(|| {
        anyhow!(
            "{}",
            entry
                .work_root_finding
                .clone()
                .unwrap_or_else(|| "work root is not configured".to_string())
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::channel_env::{save_channel_env, ChannelEnv};

    #[test]
    fn renders_prefixed_redacted_journal_json() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut env = ChannelEnv::new();
        env.insert("TOKEN".to_string(), "secret-token".to_string());
        save_channel_env(&home, "telegram", &env).expect("save env");
        let component = LogComponent {
            home,
            instance: Some("main".to_string()),
            component: "telegram".to_string(),
            unit: "lionclaw-channel-uuid-telegram.service".to_string(),
        };
        let redactor = SecretRedactor::from_homes([&component.home]).expect("redactor");
        let render = LogRenderContext::new(&[component], true);

        let line = render
            .render(
                r#"{"MESSAGE":"worker printed secret-token","_SYSTEMD_USER_UNIT":"lionclaw-channel-uuid-telegram.service"}"#,
                &redactor,
            )
            .expect("line");

        assert_eq!(line, "main/telegram | worker printed [REDACTED]");
    }

    #[test]
    fn renders_journal_byte_array_message() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let component = LogComponent {
            home,
            instance: Some("main".to_string()),
            component: "daemon".to_string(),
            unit: "lionclaw-daemon.service".to_string(),
        };
        let redactor = SecretRedactor::from_homes([&component.home]).expect("redactor");
        let render = LogRenderContext::new(&[component], true);

        let line = render
            .render(
                r#"{"MESSAGE":[83,116,97,114,116,101,100],"_SYSTEMD_USER_UNIT":"lionclaw-daemon.service"}"#,
                &redactor,
            )
            .expect("line");

        assert_eq!(line, "main/daemon | Started");
    }
}
