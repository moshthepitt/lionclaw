use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::{
    home::LionClawHome,
    kernel::skills::validate_skill_alias,
    operator::{
        channel_env::validate_channel_env_contract,
        channel_metadata::{load_channel_metadata, validate_channel_id},
        command_display::{lionclaw_home_command_prefix, shell_quote_arg},
        config::{ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        managed_units::{
            daemon_unit_name, existing_unit_identity, unit_file_metadata, unit_status_is_active,
            UnitManager,
        },
        runtime_integration::runtime_auth_guidance,
        target::{
            discover_diagnostic_project_root, instance_home_path, instances_dir_path,
            project_dir_path, project_file_path, validate_home_target_exclusive,
            validate_instance_name, TargetSelection,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindingSeverity {
    Error,
    Warning,
}

impl FindingSeverity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorFinding {
    pub severity: FindingSeverity,
    pub subject: String,
    pub message: String,
    pub inspect: Option<String>,
    pub repair: Option<String>,
}

impl DoctorFinding {
    fn error(subject: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: FindingSeverity::Error,
            subject: subject.into(),
            message: message.into(),
            inspect: None,
            repair: None,
        }
    }

    fn warning(subject: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: FindingSeverity::Warning,
            subject: subject.into(),
            message: message.into(),
            inspect: None,
            repair: None,
        }
    }

    fn with_inspect(mut self, command: impl Into<String>) -> Self {
        self.inspect = Some(command.into());
        self
    }

    fn with_repair(mut self, command: impl Into<String>) -> Self {
        self.repair = Some(command.into());
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct DoctorReport {
    pub findings: Vec<DoctorFinding>,
}

impl DoctorReport {
    pub fn has_errors(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.severity == FindingSeverity::Error)
    }

    pub fn render(&self) -> String {
        if self.findings.is_empty() {
            return "doctor: no errors or warnings\n".to_string();
        }

        let mut output = String::new();
        for finding in &self.findings {
            output.push_str(&format!(
                "{}: {}\n{}\n",
                finding.severity.as_str(),
                finding.subject,
                finding.message
            ));
            if let Some(inspect) = finding.inspect.as_deref() {
                output.push_str(&format!("inspect: {inspect}\n"));
            }
            if let Some(repair) = finding.repair.as_deref() {
                output.push_str(&format!("repair: {repair}\n"));
            }
            output.push('\n');
        }
        output
    }

    fn push(&mut self, finding: DoctorFinding) {
        self.findings.push(finding);
    }

    fn extend(&mut self, findings: Vec<DoctorFinding>) {
        self.findings.extend(findings);
    }
}

#[derive(Debug, Deserialize)]
struct DiagnosticProjectFile {
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    default_instance: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticDefaultInstance<'a> {
    Missing,
    Invalid,
    Valid(&'a str),
}

#[derive(Debug, Deserialize)]
struct DiagnosticInstanceFile {
    #[serde(default)]
    version: Option<u32>,
    work_root: PathBuf,
}

#[derive(Debug, Clone)]
struct DoctorCommands {
    selected_prefix: String,
    project_prefix: Option<String>,
    instance: String,
    home: PathBuf,
}

impl DoctorCommands {
    fn for_target(project_root: Option<&Path>, instance: &str, home: &Path) -> Self {
        let project_prefix = project_root.map(|project_root| {
            format!(
                "lionclaw --project {}",
                shell_quote_arg(&project_root.display().to_string())
            )
        });
        let selected_prefix = match project_prefix.as_deref() {
            Some(prefix) => format!("{prefix} --instance {}", shell_quote_arg(instance)),
            None => lionclaw_home_command_prefix(&LionClawHome::new(home.to_path_buf())),
        };
        Self {
            selected_prefix,
            project_prefix,
            instance: instance.to_string(),
            home: home.to_path_buf(),
        }
    }

    fn selected(&self, command: &str) -> String {
        format!("{} {command}", self.selected_prefix)
    }

    fn create_instance(&self) -> String {
        match self.project_prefix.as_deref() {
            Some(prefix) => format!(
                "{prefix} instance create {}",
                shell_quote_arg(&self.instance)
            ),
            None => "lionclaw project init".to_string(),
        }
    }

    fn adopt_work_root(&self) -> String {
        let home = shell_quote_arg(&self.home.display().to_string());
        match self.project_prefix.as_deref() {
            Some(prefix) => format!(
                "{prefix} instance adopt {} {home} --work-root PATH",
                shell_quote_arg(&self.instance)
            ),
            None => format!(
                "lionclaw instance adopt {} {home} --work-root PATH",
                shell_quote_arg(&self.instance)
            ),
        }
    }
}

pub async fn run_doctor<M: UnitManager>(
    selection: &TargetSelection,
    all: bool,
    manager: &M,
) -> Result<DoctorReport> {
    if all && (selection.home.is_some() || selection.instance.is_some()) {
        bail!("doctor --all requires a project context and cannot be combined with --home or --instance");
    }
    validate_home_target_exclusive(selection)?;
    let selected_instance = selection
        .instance
        .as_deref()
        .filter(|value| !value.is_empty());
    if let Some(name) = selected_instance {
        validate_instance_name(name)?;
    }

    if let Some(home) = selection.home.as_deref() {
        let home = absolute_path(home)?;
        return inspect_direct_home(&home, manager).await;
    }

    let project_selection = TargetSelection {
        home: None,
        project: selection.project.clone(),
        instance: None,
    };
    let project_root = discover_diagnostic_project_root(&project_selection)?;
    inspect_project(&project_root, selected_instance, all, manager).await
}

async fn inspect_project<M: UnitManager>(
    project_root: &Path,
    selected_instance: Option<&str>,
    all: bool,
    manager: &M,
) -> Result<DoctorReport> {
    let mut report = DoctorReport::default();
    let project_file = project_file_path(project_root);
    let metadata_dir_ok = match inspect_project_metadata_dir(project_root) {
        Ok(()) => true,
        Err(finding) => {
            report.push(finding);
            false
        }
    };
    let project_config = if metadata_dir_ok {
        match read_project_file(project_root) {
            Ok(config) => Some(config),
            Err(finding) => {
                report.push(finding);
                None
            }
        }
    } else {
        None
    };

    let (instances, instances_loaded) = if metadata_dir_ok {
        match discover_project_instance_homes(project_root) {
            Ok(discovery) => {
                report.extend(discovery.findings);
                (discovery.homes, true)
            }
            Err(finding) => {
                report.push(finding);
                (BTreeMap::new(), false)
            }
        }
    } else {
        (BTreeMap::new(), false)
    };
    let default_instance = project_config
        .as_ref()
        .map(|config| inspect_default_instance(project_root, config, &mut report))
        .unwrap_or(DiagnosticDefaultInstance::Missing);

    let selected_names = if !instances_loaded {
        Vec::new()
    } else if all {
        if instances.is_empty() && project_config.is_some() {
            report.push(project_has_no_instances_finding(project_root));
        }
        instances.keys().cloned().collect::<Vec<_>>()
    } else if let Some(name) = selected_instance {
        validate_instance_name(name)?;
        vec![name.to_string()]
    } else {
        selected_project_instance_names(project_root, default_instance, &instances, &mut report)
    };

    if let DiagnosticDefaultInstance::Valid(default_instance) = default_instance {
        if instances_loaded && !instances.contains_key(default_instance) {
            report.push(
                DoctorFinding::error(
                    format!("default instance \"{default_instance}\" is configured but missing"),
                    format!(
                        "{} points at an instance that is not present under {}",
                        project_file.display(),
                        instances_dir_path(project_root).display()
                    ),
                )
                .with_repair(project_command(
                    project_root,
                    &format!("instance create {}", shell_quote_arg(default_instance)),
                )),
            );
        }
    }

    for name in selected_names {
        let home = instances
            .get(&name)
            .cloned()
            .unwrap_or_else(|| instance_home_path(project_root, &name));
        inspect_instance(Some(project_root), &name, &home, manager)
            .await
            .into_iter()
            .for_each(|finding| report.push(finding));
    }

    report.extend(inspect_project_units(project_root, &instances)?);
    Ok(report)
}

fn inspect_default_instance<'a>(
    project_root: &Path,
    config: &'a DiagnosticProjectFile,
    report: &mut DoctorReport,
) -> DiagnosticDefaultInstance<'a> {
    let Some(name) = config.default_instance.as_deref() else {
        return DiagnosticDefaultInstance::Missing;
    };
    if let Err(err) = validate_instance_name(name) {
        report.push(
            DoctorFinding::error(
                format!("default instance \"{name}\" is invalid"),
                format!("{}: {err}", project_file_path(project_root).display()),
            )
            .with_repair(format!(
                "edit {}",
                project_file_path(project_root).display()
            )),
        );
        return DiagnosticDefaultInstance::Invalid;
    }
    DiagnosticDefaultInstance::Valid(name)
}

fn selected_project_instance_names(
    project_root: &Path,
    default_instance: DiagnosticDefaultInstance<'_>,
    instances: &BTreeMap<String, PathBuf>,
    report: &mut DoctorReport,
) -> Vec<String> {
    match default_instance {
        DiagnosticDefaultInstance::Valid(default_instance) => {
            return vec![default_instance.to_string()];
        }
        DiagnosticDefaultInstance::Invalid => return Vec::new(),
        DiagnosticDefaultInstance::Missing => {}
    }

    match instances.keys().cloned().collect::<Vec<_>>().as_slice() {
        [only] => vec![only.clone()],
        [] => {
            report.push(project_has_no_instances_finding(project_root));
            Vec::new()
        }
        _ => {
            report.push(
                DoctorFinding::error(
                    "project has multiple instances and no default_instance",
                    format!(
                        "set default_instance in {} or rerun doctor with --instance NAME",
                        project_file_path(project_root).display()
                    ),
                )
                .with_repair(format!(
                    "edit {}",
                    project_file_path(project_root).display()
                )),
            );
            Vec::new()
        }
    }
}

fn project_has_no_instances_finding(project_root: &Path) -> DoctorFinding {
    DoctorFinding::error(
        "project has no instances",
        format!(
            "{} contains no instance homes",
            instances_dir_path(project_root).display()
        ),
    )
    .with_repair(project_command(project_root, "instance create main"))
}

async fn inspect_direct_home<M: UnitManager>(home: &Path, manager: &M) -> Result<DoctorReport> {
    let mut report = DoctorReport::default();
    let name = "direct-home";
    for finding in inspect_instance(None, name, home, manager).await {
        report.push(finding);
    }
    Ok(report)
}

async fn inspect_instance<M: UnitManager>(
    project_root: Option<&Path>,
    name: &str,
    home: &Path,
    manager: &M,
) -> Vec<DoctorFinding> {
    let mut findings = Vec::new();
    let commands = DoctorCommands::for_target(project_root, name, home);
    let home = match inspect_home_path(name, home, &commands, &mut findings) {
        Some(home) => home,
        None => return findings,
    };
    inspect_instance_work_root(project_root, name, &home, &commands, &mut findings);

    let lion_home = LionClawHome::new(home.clone());
    let config = match OperatorConfig::load(&lion_home).await {
        Ok(config) => config,
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    format!("operator config is invalid for instance \"{name}\""),
                    err.to_string(),
                )
                .with_repair(commands.selected("configure --runtime codex")),
            );
            return findings;
        }
    };

    inspect_runtime_config(name, &commands, &config, &mut findings);
    inspect_channels(&lion_home, name, &commands, &config, &mut findings);
    inspect_expected_units(&lion_home, name, &commands, &config, manager, &mut findings).await;
    inspect_owned_stale_units(&lion_home, name, &commands, &config, manager, &mut findings).await;
    findings
}

fn inspect_home_path(
    name: &str,
    home: &Path,
    commands: &DoctorCommands,
    findings: &mut Vec<DoctorFinding>,
) -> Option<PathBuf> {
    let metadata = match fs::symlink_metadata(home) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            findings.push(
                DoctorFinding::error(
                    format!("instance \"{name}\" home is missing"),
                    format!("{} does not exist", home.display()),
                )
                .with_repair(commands.create_instance()),
            );
            return None;
        }
        Err(err) => {
            findings.push(DoctorFinding::error(
                format!("instance \"{name}\" home cannot be inspected"),
                err.to_string(),
            ));
            return None;
        }
    };
    if metadata.file_type().is_symlink() {
        findings.push(DoctorFinding::error(
            format!("instance \"{name}\" home is a symlink"),
            format!("{} must be a real directory", home.display()),
        ));
        return None;
    }
    if !metadata.is_dir() {
        findings.push(DoctorFinding::error(
            format!("instance \"{name}\" home is not a directory"),
            format!("{} is not a directory", home.display()),
        ));
        return None;
    }
    match fs::canonicalize(home) {
        Ok(home) => Some(home),
        Err(err) => {
            findings.push(DoctorFinding::error(
                format!("instance \"{name}\" home cannot be resolved"),
                err.to_string(),
            ));
            None
        }
    }
}

fn inspect_instance_work_root(
    project_root: Option<&Path>,
    name: &str,
    home: &Path,
    commands: &DoctorCommands,
    findings: &mut Vec<DoctorFinding>,
) {
    let instance_config = home.join("config/instance.toml");
    let parsed = match read_instance_file(&instance_config) {
        Ok(Some(config)) => config,
        Ok(None) => {
            findings.push(
                DoctorFinding::error(
                    format!("instance \"{name}\" does not record a work root"),
                    format!("{} is missing", instance_config.display()),
                )
                .with_repair(commands.adopt_work_root()),
            );
            return;
        }
        Err(finding) => {
            findings.push(finding);
            return;
        }
    };

    if parsed.version.unwrap_or(1) != 1 {
        findings.push(DoctorFinding::error(
            format!("instance \"{name}\" config version is unsupported"),
            format!("{} must use version = 1", instance_config.display()),
        ));
    }

    let work_root = if parsed.work_root.is_absolute() {
        parsed.work_root
    } else {
        findings.push(
            DoctorFinding::error(
                format!("instance \"{name}\" work root is not canonical"),
                format!(
                    "{} records {}",
                    instance_config.display(),
                    parsed.work_root.display()
                ),
            )
            .with_repair(commands.adopt_work_root()),
        );
        return;
    };

    let metadata = match fs::metadata(&work_root) {
        Ok(metadata) => metadata,
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    format!("instance \"{name}\" work root is missing"),
                    format!("{}: {err}", work_root.display()),
                )
                .with_repair(commands.adopt_work_root()),
            );
            return;
        }
    };
    if !metadata.is_dir() {
        findings.push(DoctorFinding::error(
            format!("instance \"{name}\" work root is not a directory"),
            format!("{} is not a directory", work_root.display()),
        ));
    }
    let canonical_work_root = match fs::canonicalize(&work_root) {
        Ok(path) => path,
        Err(err) => {
            findings.push(DoctorFinding::error(
                format!("instance \"{name}\" work root is not canonical"),
                format!("{}: {err}", work_root.display()),
            ));
            return;
        }
    };
    if let Some(project_root) = project_root {
        let canonical_project_root =
            fs::canonicalize(project_root).unwrap_or_else(|_| project_root.to_path_buf());
        if !canonical_work_root.starts_with(&canonical_project_root) {
            findings.push(DoctorFinding::error(
                format!("instance \"{name}\" work root escapes project root"),
                format!(
                    "{} is outside {}",
                    canonical_work_root.display(),
                    canonical_project_root.display()
                ),
            ));
        }
        let metadata_root = canonical_project_root.join(".lionclaw");
        if canonical_work_root == metadata_root || canonical_work_root.starts_with(&metadata_root) {
            findings.push(DoctorFinding::error(
                format!("instance \"{name}\" work root points inside LionClaw metadata"),
                format!(
                    "{} is inside {}",
                    canonical_work_root.display(),
                    metadata_root.display()
                ),
            ));
        }
    }
}

fn inspect_runtime_config(
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    findings: &mut Vec<DoctorFinding>,
) {
    let Some(runtime_id) = config.defaults.runtime.as_deref() else {
        findings.push(
            DoctorFinding::error(
                format!("instance \"{name}\" has no default runtime"),
                "runtime setup is required before run/up/connect can launch workers",
            )
            .with_repair(commands.selected("configure --runtime codex")),
        );
        return;
    };

    let Some(profile) = config.runtimes.get(runtime_id) else {
        findings.push(
            DoctorFinding::error(
                format!("default runtime \"{runtime_id}\" is missing for instance \"{name}\""),
                "defaults.runtime points at a profile that is not configured",
            )
            .with_repair(commands.selected("configure --runtime codex")),
        );
        return;
    };

    if let Err(err) = profile.validate() {
        findings.push(
            DoctorFinding::error(
                format!("runtime profile \"{runtime_id}\" is invalid for instance \"{name}\""),
                err.to_string(),
            )
            .with_repair(commands.selected("configure --runtime codex")),
        );
    }

    if let Some(guidance) = runtime_auth_guidance(profile) {
        findings.push(DoctorFinding::warning(
            format!("runtime auth for \"{runtime_id}\" is not refreshed by doctor"),
            guidance,
        ));
    }
}

fn inspect_channels(
    home: &LionClawHome,
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    findings: &mut Vec<DoctorFinding>,
) {
    for channel in &config.channels {
        if let Err(err) = validate_channel_id(&channel.id) {
            findings.push(DoctorFinding::error(
                format!("channel id is invalid for instance \"{name}\""),
                err.to_string(),
            ));
        }
        let skill_dir = home.skills_dir().join(&channel.skill);
        if let Err(err) = validate_skill_alias(&channel.skill) {
            findings.push(DoctorFinding::error(
                format!("channel \"{}\" references invalid skill alias", channel.id),
                err.to_string(),
            ));
            continue;
        }
        match fs::symlink_metadata(&skill_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                findings.push(DoctorFinding::error(
                    format!("channel \"{}\" skill is a symlink", channel.id),
                    format!(
                        "{} must be a real installed skill directory",
                        skill_dir.display()
                    ),
                ));
                continue;
            }
            Ok(metadata) if !metadata.is_dir() => {
                findings.push(DoctorFinding::error(
                    format!("channel \"{}\" skill is not a directory", channel.id),
                    format!("{} is not a directory", skill_dir.display()),
                ));
                continue;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                findings.push(
                    DoctorFinding::error(
                        format!("channel \"{}\" references missing skill", channel.id),
                        format!("installed skill alias \"{}\" is missing", channel.skill),
                    )
                    .with_repair(
                        commands.selected(&format!("connect {}", shell_quote_arg(&channel.id))),
                    ),
                );
                continue;
            }
            Err(err) => {
                findings.push(DoctorFinding::error(
                    format!("channel \"{}\" skill cannot be inspected", channel.id),
                    err.to_string(),
                ));
                continue;
            }
        }

        match load_channel_metadata(&skill_dir) {
            Ok(metadata) => inspect_channel_metadata_match(commands, channel, metadata, findings),
            Err(err) => findings.push(
                DoctorFinding::error(
                    format!("channel \"{}\" metadata is invalid", channel.id),
                    err.to_string(),
                )
                .with_repair(
                    commands.selected(&format!("connect {}", shell_quote_arg(&channel.id))),
                ),
            ),
        }

        if let Err(err) = validate_channel_env_contract(home, &channel.id, &channel.required_env) {
            findings.push(
                DoctorFinding::error(
                    format!("channel \"{}\" environment is invalid", channel.id),
                    err.to_string(),
                )
                .with_repair(format!(
                    "{} --env-file {}",
                    commands.selected(&format!("connect {}", shell_quote_arg(&channel.id))),
                    shell_quote_arg(&format!("./{}.env", channel.id))
                )),
            );
        }
    }
}

fn inspect_channel_metadata_match(
    commands: &DoctorCommands,
    channel: &ManagedChannelConfig,
    metadata: crate::operator::channel_metadata::ChannelMetadata,
    findings: &mut Vec<DoctorFinding>,
) {
    let expected_env = channel
        .required_env
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual_env = metadata.env.iter().cloned().collect::<BTreeSet<_>>();
    if metadata.id != channel.id
        || metadata.launch != channel.launch_mode
        || metadata.worker != channel.worker
        || actual_env != expected_env
    {
        findings.push(
            DoctorFinding::error(
                format!(
                    "channel \"{}\" metadata does not match configured channel facts",
                    channel.id
                ),
                format!(
                    "metadata declares id={} launch={} worker={} env={:?}",
                    metadata.id,
                    metadata.launch.as_str(),
                    metadata.worker,
                    metadata.env
                ),
            )
            .with_repair(commands.selected(&format!("connect {}", shell_quote_arg(&channel.id)))),
        );
    }
}

async fn inspect_expected_units<M: UnitManager>(
    home: &LionClawHome,
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    manager: &M,
    findings: &mut Vec<DoctorFinding>,
) {
    let background_channels = config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Background)
        .collect::<Vec<_>>();
    let owned_units = match manager.owned_units(home) {
        Ok(units) => units,
        Err(err) => {
            findings.push(DoctorFinding::error(
                format!("managed unit ownership is invalid for instance \"{name}\""),
                err.to_string(),
            ));
            return;
        }
    };
    if background_channels.is_empty() && owned_units.is_empty() {
        return;
    }

    if let Some(unit) = owned_units.daemon() {
        inspect_unit_status(name, "daemon", unit, commands, manager, findings).await;
    } else {
        findings.push(
            DoctorFinding::error(
                format!("managed daemon unit is missing for instance \"{name}\""),
                "no owned systemd unit metadata was found for the selected home",
            )
            .with_repair(commands.selected("up")),
        );
    }

    for channel in background_channels {
        let subject = format!("worker \"{}\"", channel.id);
        if let Some(unit) = owned_units.channel(&channel.id) {
            inspect_unit_status(name, &subject, unit, commands, manager, findings).await;
        } else {
            findings.push(
                DoctorFinding::error(
                    format!("{subject} is not running for instance \"{name}\""),
                    "no owned systemd unit metadata was found for the selected home",
                )
                .with_repair(commands.selected("up")),
            );
        }
    }
}

async fn inspect_unit_status<M: UnitManager>(
    instance: &str,
    subject: &str,
    unit: &str,
    commands: &DoctorCommands,
    manager: &M,
    findings: &mut Vec<DoctorFinding>,
) {
    match manager.unit_status(unit).await {
        Ok(status) if unit_status_is_active(&status) => {}
        Ok(status) => findings.push(
            DoctorFinding::error(
                format!("{subject} is not running for instance \"{instance}\""),
                format!("{unit}: {status}"),
            )
            .with_inspect(commands.selected("logs"))
            .with_repair(commands.selected("up")),
        ),
        Err(err) => findings.push(
            DoctorFinding::error(
                format!("{subject} status could not be read for instance \"{instance}\""),
                err.to_string(),
            )
            .with_inspect(format!("systemctl --user show {unit}")),
        ),
    }
}

async fn inspect_owned_stale_units<M: UnitManager>(
    home: &LionClawHome,
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    manager: &M,
    findings: &mut Vec<DoctorFinding>,
) {
    let Some(identity) = existing_unit_identity(home).ok().flatten() else {
        return;
    };
    let daemon_unit = daemon_unit_name(&identity);
    let expected_channels = config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Background)
        .map(|channel| channel.id.as_str())
        .collect::<BTreeSet<_>>();
    for unit_path in user_lionclaw_unit_files().unwrap_or_default() {
        if !unit_path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.starts_with("lionclaw") && name.ends_with(".service"))
        {
            continue;
        }
        let Some(unit_name) = unit_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Ok(Some(metadata)) = unit_file_metadata(&unit_path) else {
            continue;
        };
        if !metadata.belongs_to_identity(&identity) {
            continue;
        }
        if unit_name == daemon_unit {
            continue;
        }
        if metadata
            .channel_id
            .as_deref()
            .is_some_and(|channel_id| expected_channels.contains(channel_id))
        {
            continue;
        }
        let status = manager
            .unit_status(unit_name)
            .await
            .unwrap_or_else(|_| "unknown".to_string());
        findings.push(
            DoctorFinding::warning(
                format!("stale managed unit for instance \"{name}\""),
                format!("{unit_name}: {status}"),
            )
            .with_repair(commands.selected("up")),
        );
    }
}

fn inspect_project_units(
    project_root: &Path,
    instances: &BTreeMap<String, PathBuf>,
) -> Result<Vec<DoctorFinding>> {
    let mut findings = Vec::new();
    let known_homes = instances
        .values()
        .filter_map(|path| fs::canonicalize(path).ok())
        .collect::<BTreeSet<_>>();
    let known_identities = known_homes
        .iter()
        .filter_map(|home| {
            existing_unit_identity(&LionClawHome::new(home.clone()))
                .ok()
                .flatten()
                .map(|identity| (home.clone(), identity))
        })
        .collect::<BTreeMap<_, _>>();
    for unit_path in user_lionclaw_unit_files()? {
        let Some(unit_name) = unit_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !unit_name.starts_with("lionclaw") || !unit_name.ends_with(".service") {
            continue;
        }
        let metadata = match unit_file_metadata(&unit_path)? {
            Some(metadata) => metadata,
            None => {
                findings.push(DoctorFinding::warning(
                    "unowned LionClaw-looking unit",
                    format!(
                        "{} is not a regular unit file with readable LionClaw metadata",
                        unit_path.display()
                    ),
                ));
                continue;
            }
        };
        let recorded_home = match metadata.home_root.as_ref() {
            Some(home) => home,
            None => {
                findings.push(DoctorFinding::warning(
                    "unowned LionClaw-looking unit",
                    format!(
                        "{} has no X-LionClaw-HomeRoot metadata",
                        unit_path.display()
                    ),
                ));
                continue;
            }
        };
        if !recorded_home.starts_with(project_root) {
            continue;
        }
        if metadata.unit_group_id.is_none() {
            findings.push(DoctorFinding::warning(
                "incomplete LionClaw unit metadata",
                format!(
                    "{} records {} but has no X-LionClaw-UnitGroupId metadata",
                    unit_path.display(),
                    recorded_home.display()
                ),
            ));
            continue;
        }
        if let Some(identity) = known_identities.get(recorded_home) {
            if metadata.belongs_to_identity(identity) {
                continue;
            }
            findings.push(DoctorFinding::warning(
                "LionClaw unit metadata does not match instance identity",
                format!(
                    "{} records {} but is not owned by that instance's unit group",
                    unit_path.display(),
                    recorded_home.display()
                ),
            ));
            continue;
        }
        if known_homes.contains(recorded_home) {
            findings.push(DoctorFinding::warning(
                "LionClaw unit points at instance without unit identity",
                format!(
                    "{} records {} but the instance has no unit group id",
                    unit_path.display(),
                    recorded_home.display()
                ),
            ));
            continue;
        }
        findings.push(DoctorFinding::warning(
            "ghost LionClaw unit points into this project",
            format!(
                "{} records missing or unregistered home {}",
                unit_path.display(),
                recorded_home.display()
            ),
        ));
    }
    Ok(findings)
}

fn inspect_project_metadata_dir(project_root: &Path) -> std::result::Result<(), DoctorFinding> {
    let path = project_dir_path(project_root);
    let metadata = fs::symlink_metadata(&path).map_err(|err| {
        DoctorFinding::error(
            "project metadata directory is missing or unreadable",
            format!("{}: {err}", path.display()),
        )
        .with_repair("lionclaw project init")
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            "project metadata directory is a symlink",
            format!("{} must be a real directory", path.display()),
        ));
    }
    if !metadata.is_dir() {
        return Err(DoctorFinding::error(
            "project metadata path is not a directory",
            format!("{} is not a directory", path.display()),
        ));
    }
    Ok(())
}

fn read_project_file(
    project_root: &Path,
) -> std::result::Result<DiagnosticProjectFile, DoctorFinding> {
    let path = project_file_path(project_root);
    let metadata = fs::symlink_metadata(&path).map_err(|err| {
        DoctorFinding::error(
            "project file is missing or unreadable",
            format!("{}: {err}", path.display()),
        )
        .with_repair("lionclaw project init")
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            "project file is a symlink",
            format!("{} must be a regular file", path.display()),
        ));
    }
    if !metadata.is_file() {
        return Err(DoctorFinding::error(
            "project file is not a regular file",
            format!("{} is not a file", path.display()),
        ));
    }
    let content = fs::read_to_string(&path).map_err(|err| {
        DoctorFinding::error(
            "project file cannot be read",
            format!("{}: {err}", path.display()),
        )
    })?;
    let config: DiagnosticProjectFile = toml::from_str(&content).map_err(|err| {
        DoctorFinding::error(
            "project file cannot be parsed",
            format!("{}: {err}", path.display()),
        )
    })?;
    if !matches!(config.version, None | Some(1)) {
        return Err(DoctorFinding::error(
            "project file version is unsupported",
            format!("{} must use version = 1", path.display()),
        ));
    }
    Ok(config)
}

fn read_instance_file(
    path: &Path,
) -> std::result::Result<Option<DiagnosticInstanceFile>, DoctorFinding> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(DoctorFinding::error(
                "instance config cannot be read",
                format!("{}: {err}", path.display()),
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            "instance config is a symlink",
            format!("{} must be a regular file", path.display()),
        ));
    }
    if !metadata.is_file() {
        return Err(DoctorFinding::error(
            "instance config is not a regular file",
            format!("{} is not a file", path.display()),
        ));
    }
    let content = fs::read_to_string(path).map_err(|err| {
        DoctorFinding::error(
            "instance config cannot be read",
            format!("{}: {err}", path.display()),
        )
    })?;
    toml::from_str(&content).map(Some).map_err(|err| {
        DoctorFinding::error(
            "instance config cannot be parsed",
            format!("{}: {err}", path.display()),
        )
    })
}

#[derive(Debug, Default)]
struct InstanceHomeDiscovery {
    homes: BTreeMap<String, PathBuf>,
    findings: Vec<DoctorFinding>,
}

fn discover_project_instance_homes(
    project_root: &Path,
) -> std::result::Result<InstanceHomeDiscovery, DoctorFinding> {
    let instances_dir = instances_dir_path(project_root);
    let metadata = fs::symlink_metadata(&instances_dir).map_err(|err| {
        DoctorFinding::error(
            "instances directory is missing or unreadable",
            format!("{}: {err}", instances_dir.display()),
        )
        .with_repair("lionclaw instance create main")
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            "instances directory is a symlink",
            format!("{} must be a real directory", instances_dir.display()),
        ));
    }
    if !metadata.is_dir() {
        return Err(DoctorFinding::error(
            "instances path is not a directory",
            format!("{} is not a directory", instances_dir.display()),
        ));
    }

    let mut discovery = InstanceHomeDiscovery::default();
    for entry in fs::read_dir(&instances_dir)
        .with_context(|| format!("failed to read {}", instances_dir.display()))
        .map_err(|err| {
            DoctorFinding::error("instances directory cannot be read", err.to_string())
        })?
    {
        let entry = entry.map_err(|err| {
            DoctorFinding::error("instances directory cannot be read", err.to_string())
        })?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            discovery.findings.push(DoctorFinding::error(
                "instance entry name is not UTF-8",
                format!(
                    "{} cannot be addressed as a LionClaw instance",
                    path.display()
                ),
            ));
            continue;
        };
        if let Err(err) = validate_instance_name(name) {
            discovery.findings.push(DoctorFinding::error(
                format!("instance entry \"{name}\" has invalid name"),
                format!("{}: {err}", path.display()),
            ));
            continue;
        }
        discovery.homes.insert(name.to_string(), path);
    }
    Ok(discovery)
}

fn project_command(project_root: &Path, command: &str) -> String {
    format!(
        "lionclaw --project {} {command}",
        shell_quote_arg(&project_root.display().to_string())
    )
}

fn user_lionclaw_unit_files() -> Result<Vec<PathBuf>> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    let dir = PathBuf::from(home).join(".config/systemd/user");
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed to iterate {}", dir.display()))?;
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{symlink, PermissionsExt};

    use super::*;
    use crate::kernel::runtime::{ConfinementConfig, OciConfinementConfig};
    use crate::operator::config::RuntimeProfileConfig;
    use crate::operator::managed_units::{
        channel_unit_name, ensure_unit_identity, FakeUnitManager,
    };

    #[test]
    fn doctor_commands_keep_project_repairs_project_scoped() {
        let commands = DoctorCommands::for_target(
            Some(Path::new("/repo")),
            "reviewer",
            Path::new("/repo/.lionclaw/instances/reviewer"),
        );

        assert_eq!(
            commands.selected("up"),
            "lionclaw --project /repo --instance reviewer up"
        );
        assert_eq!(
            commands.adopt_work_root(),
            "lionclaw --project /repo instance adopt reviewer /repo/.lionclaw/instances/reviewer --work-root PATH"
        );
    }

    #[test]
    fn doctor_report_exit_state_follows_errors_only() {
        let warning = DoctorReport {
            findings: vec![DoctorFinding::warning("legacy unit", "left alone")],
        };
        assert!(!warning.has_errors());

        let error = DoctorReport {
            findings: vec![DoctorFinding::error("missing runtime", "configure first")],
        };
        assert!(error.has_errors());
        assert!(error.render().contains("error: missing runtime"));
    }

    #[tokio::test]
    async fn doctor_reports_missing_project_file_inside_existing_metadata_dir() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir(temp_dir.path().join(".lionclaw")).expect("metadata dir");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "project file is missing or unreadable"));
    }

    #[tokio::test]
    async fn doctor_all_reports_project_with_no_instances() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(instances_dir_path(temp_dir.path())).expect("instances dir");
        fs::write(project_file_path(temp_dir.path()), "version = 1\n").expect("project file");

        let report = inspect_project(temp_dir.path(), None, true, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report.has_errors());
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "project has no instances"));
    }

    #[tokio::test]
    async fn doctor_rejects_home_with_project_or_instance_selectors() {
        let selections = [
            TargetSelection {
                home: Some(PathBuf::from("home")),
                project: Some(PathBuf::from("project")),
                instance: None,
            },
            TargetSelection {
                home: Some(PathBuf::from("home")),
                project: None,
                instance: Some("main".to_string()),
            },
        ];

        for selection in selections {
            let err = run_doctor(&selection, false, &FakeUnitManager::default())
                .await
                .expect_err("conflicting doctor target should fail");

            assert!(err
                .to_string()
                .contains("--home cannot be combined with --project or --instance"));
        }
    }

    #[tokio::test]
    async fn doctor_rejects_path_like_instance_selector() {
        let selection = TargetSelection {
            home: None,
            project: None,
            instance: Some("../../some-home".to_string()),
        };

        let err = run_doctor(&selection, false, &FakeUnitManager::default())
            .await
            .expect_err("path-like instance selector should fail");

        assert!(err
            .to_string()
            .contains("instance name '../../some-home' is not path-safe"));
    }

    #[tokio::test]
    async fn doctor_reports_invalid_project_default_instance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(instances_dir_path(temp_dir.path())).expect("instances dir");
        fs::write(
            project_file_path(temp_dir.path()),
            "version = 1\ndefault_instance = \"../../some-home\"\n",
        )
        .expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report.has_errors());
        assert!(report.findings.iter().any(|finding| {
            finding.subject == "default instance \"../../some-home\" is invalid"
        }));
    }

    #[tokio::test]
    async fn doctor_all_reports_invalid_instance_entries_without_inspecting_them() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let instances_dir = instances_dir_path(temp_dir.path());
        fs::create_dir_all(instances_dir.join("main")).expect("valid instance");
        fs::create_dir_all(instances_dir.join("bad name")).expect("invalid instance");
        fs::create_dir_all(instances_dir.join(".hidden")).expect("hidden instance");
        fs::write(project_file_path(temp_dir.path()), "version = 1\n").expect("project file");

        let report = inspect_project(temp_dir.path(), None, true, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report.has_errors());
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instance entry \"bad name\" has invalid name"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instance entry \".hidden\" has invalid name"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instance \"main\" does not record a work root"));
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.subject.starts_with("instance \"bad name\"")));
    }

    #[tokio::test]
    async fn doctor_reports_no_usable_instances_when_only_invalid_entries_exist() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let instances_dir = instances_dir_path(temp_dir.path());
        fs::create_dir_all(instances_dir.join("bad name")).expect("invalid instance");
        fs::write(project_file_path(temp_dir.path()), "version = 1\n").expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report.has_errors());
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instance entry \"bad name\" has invalid name"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "project has no instances"));
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.subject.starts_with("instance \"bad name\"")));
    }

    #[tokio::test]
    async fn doctor_does_not_inspect_selected_instance_when_instances_dir_is_invalid() {
        let temp_dir = project_with_symlinked_instances(None);

        let report = inspect_project(
            temp_dir.path(),
            Some("main"),
            false,
            &FakeUnitManager::default(),
        )
        .await
        .expect("doctor report");

        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instances directory is a symlink"));
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.subject.contains("instance \"main\"")));
    }

    #[tokio::test]
    async fn doctor_does_not_inspect_default_instance_when_instances_dir_is_invalid() {
        let temp_dir = project_with_symlinked_instances(Some("main"));

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject == "instances directory is a symlink"));
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.subject.contains("instance \"main\"")));
    }

    fn project_with_symlinked_instances(default_instance: Option<&str>) -> tempfile::TempDir {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = project_dir_path(temp_dir.path());
        let outside_instances = temp_dir.path().join("outside-instances");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");
        fs::create_dir_all(outside_instances.join("main")).expect("outside instance dir");
        symlink(&outside_instances, instances_dir_path(temp_dir.path()))
            .expect("symlink instances dir");

        let mut project_file = "version = 1\n".to_string();
        if let Some(default_instance) = default_instance {
            project_file.push_str(&format!("default_instance = \"{default_instance}\"\n"));
        }
        fs::write(project_file_path(temp_dir.path()), project_file).expect("project file");
        temp_dir
    }

    #[test]
    fn doctor_does_not_execute_configured_oci_engine() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let engine = temp_dir.path().join("podman");
        let marker = temp_dir.path().join("podman-was-run");
        fs::write(
            &engine,
            format!(
                "#!/usr/bin/env bash\nset -euo pipefail\ntouch '{}'\n",
                marker.display()
            ),
        )
        .expect("write fake podman");
        fs::set_permissions(&engine, fs::Permissions::from_mode(0o755)).expect("chmod podman");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.to_string_lossy().to_string(),
                    image: Some("lionclaw-runtime:v1".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        let commands = DoctorCommands::for_target(None, "main", temp_dir.path());
        let mut findings = Vec::new();

        inspect_runtime_config("main", &commands, &config, &mut findings);

        assert!(!marker.exists());
        assert!(!findings
            .iter()
            .any(|finding| finding.subject.contains("runtime image")));
    }

    #[test]
    fn doctor_reports_work_root_symlink_escape() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project_root = temp_dir.path().join("project");
        let outside_root = temp_dir.path().join("outside");
        let home = project_root.join(".lionclaw/instances/main");
        let link = project_root.join("work");
        fs::create_dir_all(home.join("config")).expect("create instance config dir");
        fs::create_dir_all(&outside_root).expect("create outside root");
        symlink(&outside_root, &link).expect("symlink outside root");
        let escaped_work_root = link
            .to_string_lossy()
            .replace('\\', "\\\\")
            .replace('"', "\\\"");
        fs::write(
            home.join("config/instance.toml"),
            format!("version = 1\nwork_root = \"{escaped_work_root}\"\n"),
        )
        .expect("write instance config");
        let commands = DoctorCommands::for_target(Some(&project_root), "main", &home);
        let mut findings = Vec::new();

        inspect_instance_work_root(Some(&project_root), "main", &home, &commands, &mut findings);

        assert!(findings
            .iter()
            .any(|finding| finding.subject == "instance \"main\" work root escapes project root"));
    }

    #[tokio::test]
    async fn doctor_does_not_accept_unowned_derived_unit_status() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        home.ensure_base_dirs().await.expect("base dirs");
        home.ensure_home_id().await.expect("home id");
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: Vec::new(),
        });
        let manager = FakeUnitManager::default();
        manager
            .set_unit_status(daemon_unit_name(&identity), "loaded/active/running")
            .expect("set daemon status");
        manager
            .set_unit_status(
                channel_unit_name(&identity, "telegram"),
                "loaded/active/running",
            )
            .expect("set worker status");
        let home_root = home.root();
        let commands = DoctorCommands::for_target(None, "direct-home", &home_root);
        let mut findings = Vec::new();

        inspect_expected_units(
            &home,
            "direct-home",
            &commands,
            &config,
            &manager,
            &mut findings,
        )
        .await;

        assert!(findings.iter().any(|finding| finding.subject
            == "managed daemon unit is missing for instance \"direct-home\""));
        assert!(findings.iter().any(|finding| finding.subject
            == "worker \"telegram\" is not running for instance \"direct-home\""));
    }
}
