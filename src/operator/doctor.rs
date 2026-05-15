use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::{
    applied::{compute_daemon_fingerprint, AppliedState},
    home::{runtime_project_partition_key, LionClawHome},
    kernel::skills::validate_skill_alias,
    operator::{
        channel_env::validate_channel_env_contract,
        channel_metadata::{load_channel_metadata, validate_channel_id},
        command_display::{lionclaw_home_command_prefix, shell_quote_arg},
        config::{
            normalize_podman_executable, podman_executable_inspect_command,
            podman_executable_repair_note, ChannelLaunchMode, ManagedChannelConfig, OperatorConfig,
        },
        daemon_probe::{classify_daemon, DaemonClassification},
        managed_units::{
            daemon_unit_name, existing_unit_identity, unit_file_metadata, unit_status_is_active,
            UnitManager,
        },
        runtime::resolve_runtime_execution_context,
        runtime_integration::{runtime_auth_guidance, DEFAULT_OCI_ENGINE},
        target::{
            discover_diagnostic_project_root, instance_home_path, instances_dir_path,
            normalize_project_default_instance, project_dir_path, project_file_path,
            validate_home_target_exclusive, validate_instance_name, TargetSelection,
        },
    },
};

const UNRESOLVED_WORK_ROOT_NOTE: &str =
    "choose the intended work root; doctor cannot infer a safe path";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FindingKind {
    ConfiguredBindOccupied,
    ConfiguredBindForeignHome,
    ConfiguredBindOwnerMismatch,
    ConfiguredBindIncompatible,
    ConfiguredBindStaleManaged,
    ConfiguredBindUnclassifiable,
    ProjectMetadata,
    ProjectFile,
    DefaultInstance,
    ProjectInstanceSelection,
    InstancesDirectory,
    InstanceEntry,
    InstanceHome,
    InstanceWorkRoot,
    InstanceConfig,
    Runtime,
    RuntimeAuth,
    Channel,
    ManagedUnit,
    ProjectUnit,
}

impl FindingKind {
    fn id(self) -> &'static str {
        match self {
            Self::ConfiguredBindOccupied => "LC-D001",
            Self::ConfiguredBindForeignHome => "LC-D002",
            Self::ConfiguredBindOwnerMismatch => "LC-D003",
            Self::ConfiguredBindIncompatible => "LC-D004",
            Self::ConfiguredBindStaleManaged => "LC-D008",
            Self::ConfiguredBindUnclassifiable => "LC-D009",
            Self::ProjectMetadata => "LC-D010",
            Self::ProjectFile => "LC-D011",
            Self::DefaultInstance => "LC-D012",
            Self::ProjectInstanceSelection => "LC-D013",
            Self::InstancesDirectory => "LC-D020",
            Self::InstanceEntry => "LC-D021",
            Self::InstanceHome => "LC-D030",
            Self::InstanceWorkRoot => "LC-D040",
            Self::InstanceConfig => "LC-D041",
            Self::Runtime => "LC-D050",
            Self::RuntimeAuth => "LC-D051",
            Self::Channel => "LC-D060",
            Self::ManagedUnit => "LC-D070",
            Self::ProjectUnit => "LC-D080",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Self::ConfiguredBindOccupied => {
                "no listener, or the owned managed daemon for this instance"
            }
            Self::ConfiguredBindForeignHome => {
                "configured bind is free or served by the owned managed daemon for this instance"
            }
            Self::ConfiguredBindOwnerMismatch => {
                "configured bind is free or served by the owned managed daemon for this instance and work root"
            }
            Self::ConfiguredBindIncompatible => {
                "configured bind is free or served by a compatible owned managed daemon"
            }
            Self::ConfiguredBindStaleManaged => {
                "configured bind is served by the owned managed daemon with current selected-instance config"
            }
            Self::ConfiguredBindUnclassifiable => {
                "configured bind can be classified read-only for the selected instance"
            }
            Self::ProjectMetadata => {
                "project root contains a real .lionclaw metadata directory"
            }
            Self::ProjectFile => {
                "project metadata contains a readable version = 1 project.toml file"
            }
            Self::DefaultInstance => {
                "project default_instance is path-safe and names an existing instance"
            }
            Self::ProjectInstanceSelection => {
                "project selects exactly one default instance or an explicit --instance target"
            }
            Self::InstancesDirectory => {
                "project metadata contains a readable real instances directory"
            }
            Self::InstanceEntry => "instance directory entries have valid path-safe names",
            Self::InstanceHome => "instance home is a real readable directory",
            Self::InstanceWorkRoot => {
                "instance records a canonical work root inside the project root and outside .lionclaw"
            }
            Self::InstanceConfig => {
                "instance config is a readable regular version = 1 TOML file"
            }
            Self::Runtime => "selected instance has a valid configured runtime profile",
            Self::RuntimeAuth => "runtime authentication state is handled outside doctor",
            Self::Channel => {
                "configured channels reference installed skills with matching metadata and valid private env state"
            }
            Self::ManagedUnit => {
                "owned managed background units exist and are active when background operation is configured"
            }
            Self::ProjectUnit => {
                "LionClaw-looking user units are owned by a known project instance or left untouched as warnings"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorRunbook {
    pub inspect: Box<str>,
    pub note: Option<Box<str>>,
    pub repair: Option<Box<str>>,
}

impl DoctorRunbook {
    fn new(inspect: Box<str>) -> Self {
        Self {
            inspect,
            note: None,
            repair: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorFinding {
    pub id: &'static str,
    pub severity: FindingSeverity,
    pub subject: Box<str>,
    pub target: Box<str>,
    pub expected: Box<str>,
    pub observed: Box<str>,
    pub runbook: Box<DoctorRunbook>,
}

impl DoctorFinding {
    fn error(
        kind: FindingKind,
        subject: impl Into<String>,
        target: impl Into<String>,
        observed: impl Into<String>,
    ) -> Self {
        Self::new(FindingSeverity::Error, kind, subject, target, observed)
    }

    fn warning(
        kind: FindingKind,
        subject: impl Into<String>,
        target: impl Into<String>,
        observed: impl Into<String>,
    ) -> Self {
        Self::new(FindingSeverity::Warning, kind, subject, target, observed)
    }

    fn new(
        severity: FindingSeverity,
        kind: FindingKind,
        subject: impl Into<String>,
        target: impl Into<String>,
        observed: impl Into<String>,
    ) -> Self {
        let target = target.into();
        Self {
            subject: into_boxed_str(subject),
            id: kind.id(),
            severity,
            target: target.clone().into_boxed_str(),
            expected: kind.expected().into(),
            observed: into_boxed_str(observed),
            runbook: Box::new(DoctorRunbook::new(default_inspect_command(&target))),
        }
    }

    fn with_inspect(mut self, command: impl Into<String>) -> Self {
        self.runbook.inspect = command.into().into_boxed_str();
        self
    }

    fn with_note(mut self, note: impl Into<String>) -> Self {
        self.runbook.note = Some(note.into().into_boxed_str());
        self
    }

    fn with_repair(mut self, command: impl Into<String>) -> Self {
        self.runbook.repair = Some(command.into().into_boxed_str());
        self
    }
}

fn into_boxed_str(value: impl Into<String>) -> Box<str> {
    value.into().into_boxed_str()
}

fn default_inspect_command(target: &str) -> Box<str> {
    if target_looks_like_path(target) {
        format!("ls -ld {}", shell_quote_arg(target)).into_boxed_str()
    } else {
        "lionclaw status".into()
    }
}

fn target_looks_like_path(target: &str) -> bool {
    target.starts_with('/') || target.starts_with('.') || target.contains('/')
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
                "[{}] {}: {}\n",
                finding.id,
                finding.severity.as_str(),
                finding.subject
            ));
            output.push_str(&format!("target: {}\n", finding.target));
            output.push_str(&format!("expected: {}\n", finding.expected));
            output.push_str(&format!("observed: {}\n", finding.observed));
            output.push_str(&format!("inspect: {}\n", finding.runbook.inspect));
            if let Some(note) = finding.runbook.note.as_deref() {
                output.push_str(&format!("note: {note}\n"));
            }
            if let Some(repair) = finding.runbook.repair.as_deref() {
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

impl<'a> DiagnosticDefaultInstance<'a> {
    fn valid_name(self) -> Option<&'a str> {
        match self {
            Self::Valid(name) => Some(name),
            Self::Missing | Self::Invalid => None,
        }
    }
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
        }
    }

    fn selected(&self, command: &str) -> String {
        format!("{} {command}", self.selected_prefix)
    }

    fn create_instance_repair(&self) -> Option<String> {
        self.project_prefix.as_deref().map(|prefix| {
            format!(
                "{prefix} instance create {}",
                shell_quote_arg(&self.instance)
            )
        })
    }

    fn unresolved_work_root_note(&self) -> &'static str {
        UNRESOLVED_WORK_ROOT_NOTE
    }
}

fn with_optional_repair(finding: DoctorFinding, repair: Option<String>) -> DoctorFinding {
    match repair {
        Some(command) => finding.with_repair(command),
        None => finding,
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

    let default_instance = project_config
        .as_ref()
        .map(|config| inspect_default_instance(project_root, config, &mut report))
        .unwrap_or(DiagnosticDefaultInstance::Missing);
    let instance_repair_target = default_instance.valid_name().unwrap_or("main");

    let (instances, instances_loaded) = if metadata_dir_ok {
        match discover_project_instance_homes(project_root, instance_repair_target) {
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

    let selected_names = if !instances_loaded {
        Vec::new()
    } else if all {
        if instances.is_empty() && project_config.is_some() {
            report.push(project_has_no_instances_finding(
                project_root,
                instance_repair_target,
            ));
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
                    FindingKind::DefaultInstance,
                    format!("default instance \"{default_instance}\" is configured but missing"),
                    project_file.display().to_string(),
                    format!(
                        "{} points at an instance that is not present under {}",
                        project_file.display(),
                        instances_dir_path(project_root).display()
                    ),
                )
                .with_inspect(project_command(project_root, "instance list"))
                .with_repair(project_create_instance_command(
                    project_root,
                    default_instance,
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
                FindingKind::DefaultInstance,
                format!("default instance \"{name}\" is invalid"),
                project_file_path(project_root).display().to_string(),
                format!("{}: {err}", project_file_path(project_root).display()),
            )
            .with_inspect(project_command(project_root, "instance list"))
            .with_note(format!(
                "edit {} so default_instance is a valid instance name",
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
            report.push(project_has_no_instances_finding(project_root, "main"));
            Vec::new()
        }
        _ => {
            report.push(
                DoctorFinding::error(
                    FindingKind::ProjectInstanceSelection,
                    "project has multiple instances and no default_instance",
                    project_file_path(project_root).display().to_string(),
                    format!(
                        "set default_instance in {} or rerun doctor with --instance NAME",
                        project_file_path(project_root).display()
                    ),
                )
                .with_inspect(project_command(project_root, "instance list"))
                .with_note(format!(
                    "choose a default_instance in {} or rerun doctor with --instance NAME",
                    project_file_path(project_root).display()
                )),
            );
            Vec::new()
        }
    }
}

fn project_has_no_instances_finding(project_root: &Path, repair_instance: &str) -> DoctorFinding {
    DoctorFinding::error(
        FindingKind::ProjectInstanceSelection,
        "project has no instances",
        instances_dir_path(project_root).display().to_string(),
        format!(
            "{} contains no instance homes",
            instances_dir_path(project_root).display()
        ),
    )
    .with_inspect(project_command(project_root, "instance list"))
    .with_repair(project_create_instance_command(
        project_root,
        repair_instance,
    ))
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
    let work_root = inspect_instance_work_root(project_root, name, &home, &commands, &mut findings);

    let lion_home = LionClawHome::new(home.clone());
    let config = match OperatorConfig::load(&lion_home).await {
        Ok(config) => config,
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::Runtime,
                    format!("operator config is invalid for instance \"{name}\""),
                    lion_home.config_path().display().to_string(),
                    err.to_string(),
                )
                .with_inspect(commands.selected("status"))
                .with_repair(commands.selected("configure --runtime codex")),
            );
            return findings;
        }
    };

    inspect_runtime_config(name, &commands, &config, &mut findings);
    inspect_channels(&lion_home, name, &commands, &config, &mut findings);
    inspect_configured_bind(
        &lion_home,
        name,
        &commands,
        &config,
        work_root.as_deref(),
        manager,
        &mut findings,
    )
    .await;
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
            let finding = DoctorFinding::error(
                FindingKind::InstanceHome,
                format!("instance \"{name}\" home is missing"),
                home.display().to_string(),
                format!("{} does not exist", home.display()),
            );
            findings.push(with_optional_repair(
                finding,
                commands.create_instance_repair(),
            ));
            return None;
        }
        Err(err) => {
            findings.push(DoctorFinding::error(
                FindingKind::InstanceHome,
                format!("instance \"{name}\" home cannot be inspected"),
                home.display().to_string(),
                err.to_string(),
            ));
            return None;
        }
    };
    if metadata.file_type().is_symlink() {
        findings.push(DoctorFinding::error(
            FindingKind::InstanceHome,
            format!("instance \"{name}\" home is a symlink"),
            home.display().to_string(),
            format!("{} must be a real directory", home.display()),
        ));
        return None;
    }
    if !metadata.is_dir() {
        findings.push(DoctorFinding::error(
            FindingKind::InstanceHome,
            format!("instance \"{name}\" home is not a directory"),
            home.display().to_string(),
            format!("{} is not a directory", home.display()),
        ));
        return None;
    }
    match fs::canonicalize(home) {
        Ok(home) => Some(home),
        Err(err) => {
            findings.push(DoctorFinding::error(
                FindingKind::InstanceHome,
                format!("instance \"{name}\" home cannot be resolved"),
                home.display().to_string(),
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
) -> Option<PathBuf> {
    let instance_config = home.join("config/instance.toml");
    let parsed = match read_instance_file(&instance_config) {
        Ok(Some(config)) => config,
        Ok(None) => {
            let finding = DoctorFinding::error(
                FindingKind::InstanceWorkRoot,
                format!("instance \"{name}\" does not record a work root"),
                instance_config.display().to_string(),
                format!("{} is missing", instance_config.display()),
            )
            .with_note(commands.unresolved_work_root_note());
            findings.push(finding);
            return None;
        }
        Err(finding) => {
            findings.push(finding);
            return None;
        }
    };

    if parsed.version.unwrap_or(1) != 1 {
        findings.push(DoctorFinding::error(
            FindingKind::InstanceConfig,
            format!("instance \"{name}\" config version is unsupported"),
            instance_config.display().to_string(),
            format!("{} must use version = 1", instance_config.display()),
        ));
    }

    let work_root = if parsed.work_root.is_absolute() {
        parsed.work_root
    } else {
        let finding = DoctorFinding::error(
            FindingKind::InstanceWorkRoot,
            format!("instance \"{name}\" work root is not canonical"),
            instance_config.display().to_string(),
            format!(
                "{} records {}",
                instance_config.display(),
                parsed.work_root.display()
            ),
        )
        .with_note(commands.unresolved_work_root_note());
        findings.push(finding);
        return None;
    };

    let metadata = match fs::metadata(&work_root) {
        Ok(metadata) => metadata,
        Err(err) => {
            let finding = DoctorFinding::error(
                FindingKind::InstanceWorkRoot,
                format!("instance \"{name}\" work root is missing"),
                work_root.display().to_string(),
                format!("{}: {err}", work_root.display()),
            )
            .with_note(commands.unresolved_work_root_note());
            findings.push(finding);
            return None;
        }
    };
    if !metadata.is_dir() {
        findings.push(DoctorFinding::error(
            FindingKind::InstanceWorkRoot,
            format!("instance \"{name}\" work root is not a directory"),
            work_root.display().to_string(),
            format!("{} is not a directory", work_root.display()),
        ));
        return None;
    }
    let canonical_work_root = match fs::canonicalize(&work_root) {
        Ok(path) => path,
        Err(err) => {
            findings.push(DoctorFinding::error(
                FindingKind::InstanceWorkRoot,
                format!("instance \"{name}\" work root is not canonical"),
                work_root.display().to_string(),
                format!("{}: {err}", work_root.display()),
            ));
            return None;
        }
    };
    let mut valid = true;
    if let Some(project_root) = project_root {
        let canonical_project_root =
            fs::canonicalize(project_root).unwrap_or_else(|_| project_root.to_path_buf());
        if !canonical_work_root.starts_with(&canonical_project_root) {
            valid = false;
            findings.push(DoctorFinding::error(
                FindingKind::InstanceWorkRoot,
                format!("instance \"{name}\" work root escapes project root"),
                canonical_work_root.display().to_string(),
                format!(
                    "{} is outside {}",
                    canonical_work_root.display(),
                    canonical_project_root.display()
                ),
            ));
        }
        let metadata_root = canonical_project_root.join(".lionclaw");
        if canonical_work_root == metadata_root || canonical_work_root.starts_with(&metadata_root) {
            valid = false;
            findings.push(DoctorFinding::error(
                FindingKind::InstanceWorkRoot,
                format!("instance \"{name}\" work root points inside LionClaw metadata"),
                canonical_work_root.display().to_string(),
                format!(
                    "{} is inside {}",
                    canonical_work_root.display(),
                    metadata_root.display()
                ),
            ));
        }
    }
    valid.then_some(canonical_work_root)
}

fn inspect_runtime_config(
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    findings: &mut Vec<DoctorFinding>,
) {
    inspect_runtime_config_with_podman_resolver(name, commands, config, findings, || {
        normalize_podman_executable(DEFAULT_OCI_ENGINE)
    });
}

fn inspect_runtime_config_with_podman_resolver<F>(
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    findings: &mut Vec<DoctorFinding>,
    resolve_default_podman: F,
) where
    F: FnOnce() -> Result<String>,
{
    let default_runtime_repair = commands.selected("configure --runtime codex");
    let Some(runtime_id) = config.defaults.runtime.as_deref() else {
        if let Err(err) = resolve_default_podman() {
            findings.push(podman_blocks_runtime_configure_finding(
                name,
                default_runtime_repair,
                err,
            ));
            return;
        }

        findings.push(
            DoctorFinding::error(
                FindingKind::Runtime,
                format!("instance \"{name}\" has no default runtime"),
                format!("instance {name} runtime defaults"),
                "runtime setup is required before run/up/connect can launch workers",
            )
            .with_inspect(commands.selected("status"))
            .with_repair(default_runtime_repair),
        );
        return;
    };

    let Some(profile) = config.runtimes.get(runtime_id) else {
        if let Err(err) = resolve_default_podman() {
            findings.push(podman_blocks_runtime_configure_finding(
                name,
                default_runtime_repair,
                err,
            ));
            return;
        }

        findings.push(
            DoctorFinding::error(
                FindingKind::Runtime,
                format!("default runtime \"{runtime_id}\" is missing for instance \"{name}\""),
                format!("instance {name} runtime profile {runtime_id}"),
                "defaults.runtime points at a profile that is not configured",
            )
            .with_inspect(commands.selected("status"))
            .with_repair(default_runtime_repair),
        );
        return;
    };

    if let Err(err) = profile.validate() {
        findings.push(
            DoctorFinding::error(
                FindingKind::Runtime,
                format!("runtime profile \"{runtime_id}\" is invalid for instance \"{name}\""),
                format!("instance {name} runtime profile {runtime_id}"),
                err.to_string(),
            )
            .with_inspect(commands.selected("status"))
            .with_repair(default_runtime_repair),
        );
    }

    if let Some(guidance) = runtime_auth_guidance(profile) {
        findings.push(
            DoctorFinding::warning(
                FindingKind::RuntimeAuth,
                format!("runtime auth for \"{runtime_id}\" is not refreshed by doctor"),
                format!("instance {name} runtime auth {runtime_id}"),
                guidance,
            )
            .with_inspect(commands.selected("status")),
        );
    }
}

fn podman_blocks_runtime_configure_finding(
    name: &str,
    repair_command: String,
    err: anyhow::Error,
) -> DoctorFinding {
    DoctorFinding::error(
        FindingKind::Runtime,
        format!("instance \"{name}\" cannot configure Codex because Podman is unavailable"),
        format!("instance {name} OCI engine {DEFAULT_OCI_ENGINE}"),
        err.to_string(),
    )
    .with_inspect(podman_executable_inspect_command(DEFAULT_OCI_ENGINE))
    .with_note(format!(
        "{} before running the repair command",
        podman_executable_repair_note()
    ))
    .with_repair(repair_command)
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
                FindingKind::Channel,
                format!("channel id is invalid for instance \"{name}\""),
                format!("instance {name} channel {}", channel.id),
                err.to_string(),
            ));
        }
        let skill_dir = home.skills_dir().join(&channel.skill);
        if let Err(err) = validate_skill_alias(&channel.skill) {
            findings.push(DoctorFinding::error(
                FindingKind::Channel,
                format!("channel \"{}\" references invalid skill alias", channel.id),
                skill_dir.display().to_string(),
                err.to_string(),
            ));
            continue;
        }
        match fs::symlink_metadata(&skill_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                findings.push(DoctorFinding::error(
                    FindingKind::Channel,
                    format!("channel \"{}\" skill is a symlink", channel.id),
                    skill_dir.display().to_string(),
                    format!(
                        "{} must be a real installed skill directory",
                        skill_dir.display()
                    ),
                ));
                continue;
            }
            Ok(metadata) if !metadata.is_dir() => {
                findings.push(DoctorFinding::error(
                    FindingKind::Channel,
                    format!("channel \"{}\" skill is not a directory", channel.id),
                    skill_dir.display().to_string(),
                    format!("{} is not a directory", skill_dir.display()),
                ));
                continue;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                findings.push(
                    DoctorFinding::error(
                        FindingKind::Channel,
                        format!("channel \"{}\" references missing skill", channel.id),
                        skill_dir.display().to_string(),
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
                    FindingKind::Channel,
                    format!("channel \"{}\" skill cannot be inspected", channel.id),
                    skill_dir.display().to_string(),
                    err.to_string(),
                ));
                continue;
            }
        }

        match load_channel_metadata(&skill_dir) {
            Ok(metadata) => inspect_channel_metadata_match(commands, channel, metadata, findings),
            Err(err) => findings.push(
                DoctorFinding::error(
                    FindingKind::Channel,
                    format!("channel \"{}\" metadata is invalid", channel.id),
                    skill_dir.display().to_string(),
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
                    FindingKind::Channel,
                    format!("channel \"{}\" environment is invalid", channel.id),
                    home.channel_env_path(&channel.id).display().to_string(),
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
                FindingKind::Channel,
                format!(
                    "channel \"{}\" metadata does not match configured channel facts",
                    channel.id
                ),
                format!("channel {}", channel.id),
                format!(
                    "metadata declares id={} launch={} worker={} env={:?}",
                    metadata.id,
                    metadata.launch.as_str(),
                    metadata.worker,
                    metadata.env
                ),
            )
            .with_inspect(commands.selected("channel list"))
            .with_repair(commands.selected(&format!("connect {}", shell_quote_arg(&channel.id)))),
        );
    }
}

async fn inspect_configured_bind<M: UnitManager>(
    home: &LionClawHome,
    name: &str,
    commands: &DoctorCommands,
    config: &OperatorConfig,
    work_root: Option<&Path>,
    manager: &M,
    findings: &mut Vec<DoctorFinding>,
) {
    if !config.daemon.bind_configured {
        return;
    }

    let bind = config.daemon.bind.as_str();
    let target = format!("instance {name} configured bind {bind}");
    let Some(work_root) = work_root else {
        findings.push(
            DoctorFinding::error(
                FindingKind::ConfiguredBindUnclassifiable,
                "configured bind cannot be classified",
                target.clone(),
                "selected instance work root is not valid enough to compute daemon project scope",
            )
            .with_inspect(commands.selected("status")),
        );
        return;
    };

    let home_id = match home.read_home_id().await {
        Ok(Some(home_id)) => home_id,
        Ok(None) => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindUnclassifiable,
                    "configured bind cannot be classified",
                    target.clone(),
                    format!("{} is missing", home.home_id_path().display()),
                )
                .with_inspect(commands.selected("status")),
            );
            return;
        }
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindUnclassifiable,
                    "configured bind cannot be classified",
                    target.clone(),
                    err.to_string(),
                )
                .with_inspect(commands.selected("status")),
            );
            return;
        }
    };

    let applied_state = match AppliedState::from_home_read_only(home, config) {
        Ok(applied_state) => applied_state,
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindUnclassifiable,
                    "configured bind cannot be classified",
                    target.clone(),
                    err.to_string(),
                )
                .with_inspect(commands.selected("status")),
            );
            return;
        }
    };

    let daemon_fingerprint = match expected_daemon_fingerprint(home, config, &applied_state).await {
        Ok(daemon_fingerprint) => daemon_fingerprint,
        Err(err) => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindUnclassifiable,
                    "configured bind cannot be classified",
                    target.clone(),
                    err.to_string(),
                )
                .with_inspect(commands.selected("status")),
            );
            return;
        }
    };
    let project_scope = runtime_project_partition_key(Some(work_root));
    let classification =
        match classify_daemon(bind, &home_id, &project_scope, &daemon_fingerprint).await {
            Ok(classification) => classification,
            Err(err) => {
                findings.push(
                    DoctorFinding::error(
                        FindingKind::ConfiguredBindUnclassifiable,
                        "configured bind cannot be classified",
                        target.clone(),
                        err.to_string(),
                    )
                    .with_inspect(inspect_listener_command(bind)),
                );
                return;
            }
        };

    match classification {
        DaemonClassification::Absent => {}
        DaemonClassification::SameHome => {
            if !owned_managed_daemon_is_active(home, manager).await {
                findings.push(foreground_daemon_finding(name, bind));
            }
        }
        DaemonClassification::SameHomeDifferentConfig => {
            if owned_managed_daemon_is_active(home, manager).await {
                findings.push(
                    DoctorFinding::error(
                        FindingKind::ConfiguredBindStaleManaged,
                        "managed daemon is running stale configuration",
                        target.clone(),
                        format!(
                            "{bind} is served by this instance, but daemon fingerprint differs from current config"
                        ),
                    )
                    .with_inspect(commands.selected("status"))
                    .with_repair(commands.selected("up")),
                );
            } else {
                findings.push(foreground_daemon_finding(name, bind));
            }
        }
        DaemonClassification::SameHomeDifferentProject => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindOwnerMismatch,
                    "configured bind is served by this home for a different project",
                    target.clone(),
                    format!("{bind} is served by this LionClaw home for a different project/work-root scope"),
                )
                .with_inspect(inspect_listener_command(bind))
                .with_note("stop the daemon shown by inspect"),
            );
        }
        DaemonClassification::ForeignHome(info) => {
            let foreign_home = shell_quote_arg(&info.home_root);
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindForeignHome,
                    "configured bind is occupied by another LionClaw home",
                    target.clone(),
                    format!("{bind} is served by {}", info.home_root),
                )
                .with_inspect(format!("lionclaw --home {foreign_home} status"))
                .with_repair(format!("lionclaw --home {foreign_home} down")),
            );
        }
        DaemonClassification::IncompatibleLionClaw => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindIncompatible,
                    "configured bind is served by an older LionClaw daemon",
                    target.clone(),
                    format!("{bind} responded like LionClaw but not with the current daemon info contract"),
                )
                .with_inspect(inspect_listener_command(bind))
                .with_note("stop the older LionClaw daemon shown by inspect"),
            );
        }
        DaemonClassification::UnknownListener => {
            findings.push(
                DoctorFinding::error(
                    FindingKind::ConfiguredBindOccupied,
                    "configured bind is occupied",
                    target.clone(),
                    format!("{bind} is used by a non-LionClaw process"),
                )
                .with_inspect(inspect_listener_command(bind))
                .with_note("stop the process shown by inspect"),
            );
        }
    }
}

async fn expected_daemon_fingerprint(
    home: &LionClawHome,
    config: &OperatorConfig,
    applied_state: &AppliedState,
) -> Result<String> {
    let runtime_context =
        resolve_runtime_execution_context(home, config, config.defaults.runtime.as_deref()).await?;
    Ok(compute_daemon_fingerprint(
        &runtime_context.daemon_config_fingerprint,
        applied_state,
    ))
}

async fn owned_managed_daemon_is_active<M: UnitManager>(home: &LionClawHome, manager: &M) -> bool {
    let Ok(units) = manager.owned_units(home) else {
        return false;
    };
    let Some(unit) = units.daemon() else {
        return false;
    };
    manager
        .unit_status(unit)
        .await
        .is_ok_and(|status| unit_status_is_active(&status))
}

fn foreground_daemon_finding(name: &str, bind: &str) -> DoctorFinding {
    DoctorFinding::error(
        FindingKind::ConfiguredBindOwnerMismatch,
        "configured bind is served by an unmanaged foreground daemon",
        format!("instance {name} configured bind {bind}"),
        format!("{bind} is served by this LionClaw home, but not by the owned managed unit"),
    )
    .with_inspect(inspect_listener_command(bind))
    .with_note("stop the foreground daemon shown by inspect")
}

fn inspect_listener_command(bind: &str) -> String {
    bind.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .map(|port| format!("ss -ltnp '( sport = :{port} )'"))
        .unwrap_or_else(|| "ss -ltnp".to_string())
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
            findings.push(
                DoctorFinding::error(
                    FindingKind::ManagedUnit,
                    format!("managed unit ownership is invalid for instance \"{name}\""),
                    home.root().display().to_string(),
                    err.to_string(),
                )
                .with_inspect(commands.selected("status")),
            );
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
                FindingKind::ManagedUnit,
                format!("managed daemon unit is missing for instance \"{name}\""),
                format!("instance {name} managed daemon unit"),
                "no owned systemd unit metadata was found for the selected home",
            )
            .with_inspect(commands.selected("status"))
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
                    FindingKind::ManagedUnit,
                    format!("{subject} is not running for instance \"{name}\""),
                    format!("instance {name} {subject} unit"),
                    "no owned systemd unit metadata was found for the selected home",
                )
                .with_inspect(commands.selected("status"))
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
                FindingKind::ManagedUnit,
                format!("{subject} is not running for instance \"{instance}\""),
                unit.to_string(),
                format!("{unit}: {status}"),
            )
            .with_inspect(commands.selected("logs"))
            .with_repair(commands.selected("up")),
        ),
        Err(err) => findings.push(
            DoctorFinding::error(
                FindingKind::ManagedUnit,
                format!("{subject} status could not be read for instance \"{instance}\""),
                unit.to_string(),
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
                FindingKind::ManagedUnit,
                format!("stale managed unit for instance \"{name}\""),
                unit_name.to_string(),
                format!("{unit_name}: {status}"),
            )
            .with_inspect(format!("systemctl --user show {unit_name}"))
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
                findings.push(
                    DoctorFinding::warning(
                        FindingKind::ProjectUnit,
                        "unowned LionClaw-looking unit",
                        unit_path.display().to_string(),
                        format!(
                            "{} is not a regular unit file with readable LionClaw metadata",
                            unit_path.display()
                        ),
                    )
                    .with_inspect(format!("systemctl --user show {unit_name}")),
                );
                continue;
            }
        };
        let recorded_home = match metadata.home_root.as_ref() {
            Some(home) => home,
            None => {
                findings.push(
                    DoctorFinding::warning(
                        FindingKind::ProjectUnit,
                        "unowned LionClaw-looking unit",
                        unit_path.display().to_string(),
                        format!(
                            "{} has no X-LionClaw-HomeRoot metadata",
                            unit_path.display()
                        ),
                    )
                    .with_inspect(format!("systemctl --user show {unit_name}")),
                );
                continue;
            }
        };
        if !recorded_home.starts_with(project_root) {
            continue;
        }
        if metadata.unit_group_id.is_none() {
            findings.push(
                DoctorFinding::warning(
                    FindingKind::ProjectUnit,
                    "incomplete LionClaw unit metadata",
                    unit_path.display().to_string(),
                    format!(
                        "{} records {} but has no X-LionClaw-UnitGroupId metadata",
                        unit_path.display(),
                        recorded_home.display()
                    ),
                )
                .with_inspect(format!("systemctl --user show {unit_name}")),
            );
            continue;
        }
        if let Some(identity) = known_identities.get(recorded_home) {
            if metadata.belongs_to_identity(identity) {
                continue;
            }
            findings.push(
                DoctorFinding::warning(
                    FindingKind::ProjectUnit,
                    "LionClaw unit metadata does not match instance identity",
                    unit_path.display().to_string(),
                    format!(
                        "{} records {} but is not owned by that instance's unit group",
                        unit_path.display(),
                        recorded_home.display()
                    ),
                )
                .with_inspect(format!("systemctl --user show {unit_name}")),
            );
            continue;
        }
        if known_homes.contains(recorded_home) {
            findings.push(
                DoctorFinding::warning(
                    FindingKind::ProjectUnit,
                    "LionClaw unit points at instance without unit identity",
                    unit_path.display().to_string(),
                    format!(
                        "{} records {} but the instance has no unit group id",
                        unit_path.display(),
                        recorded_home.display()
                    ),
                )
                .with_inspect(format!("systemctl --user show {unit_name}")),
            );
            continue;
        }
        findings.push(
            DoctorFinding::warning(
                FindingKind::ProjectUnit,
                "ghost LionClaw unit points into this project",
                unit_path.display().to_string(),
                format!(
                    "{} records missing or unregistered home {}",
                    unit_path.display(),
                    recorded_home.display()
                ),
            )
            .with_inspect(format!("systemctl --user show {unit_name}")),
        );
    }
    Ok(findings)
}

fn inspect_project_metadata_dir(project_root: &Path) -> std::result::Result<(), DoctorFinding> {
    let path = project_dir_path(project_root);
    let metadata = fs::symlink_metadata(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            return DoctorFinding::error(
                FindingKind::ProjectMetadata,
                "no LionClaw project found",
                project_root.display().to_string(),
                format!("no .lionclaw metadata found at {}", path.display()),
            )
            .with_inspect(default_inspect_command(&path.display().to_string()))
            .with_repair(project_command(project_root, "project init"));
        }

        DoctorFinding::error(
            FindingKind::ProjectMetadata,
            "project metadata directory is missing or unreadable",
            path.display().to_string(),
            format!("{}: {err}", path.display()),
        )
        .with_repair(project_command(project_root, "project init"))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            FindingKind::ProjectMetadata,
            "project metadata directory is a symlink",
            path.display().to_string(),
            format!("{} must be a real directory", path.display()),
        ));
    }
    if !metadata.is_dir() {
        return Err(DoctorFinding::error(
            FindingKind::ProjectMetadata,
            "project metadata path is not a directory",
            path.display().to_string(),
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
            FindingKind::ProjectFile,
            "project file is missing or unreadable",
            path.display().to_string(),
            format!("{}: {err}", path.display()),
        )
        .with_repair(project_command(project_root, "project init"))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            FindingKind::ProjectFile,
            "project file is a symlink",
            path.display().to_string(),
            format!("{} must be a regular file", path.display()),
        ));
    }
    if !metadata.is_file() {
        return Err(DoctorFinding::error(
            FindingKind::ProjectFile,
            "project file is not a regular file",
            path.display().to_string(),
            format!("{} is not a file", path.display()),
        ));
    }
    let content = fs::read_to_string(&path).map_err(|err| {
        DoctorFinding::error(
            FindingKind::ProjectFile,
            "project file cannot be read",
            path.display().to_string(),
            format!("{}: {err}", path.display()),
        )
    })?;
    let config: DiagnosticProjectFile = toml::from_str(&content).map_err(|err| {
        DoctorFinding::error(
            FindingKind::ProjectFile,
            "project file cannot be parsed",
            path.display().to_string(),
            format!("{}: {err}", path.display()),
        )
    })?;
    if !matches!(config.version, None | Some(1)) {
        return Err(DoctorFinding::error(
            FindingKind::ProjectFile,
            "project file version is unsupported",
            path.display().to_string(),
            format!("{} must use version = 1", path.display()),
        ));
    }
    Ok(DiagnosticProjectFile {
        version: config.version,
        default_instance: normalize_project_default_instance(config.default_instance),
    })
}

fn read_instance_file(
    path: &Path,
) -> std::result::Result<Option<DiagnosticInstanceFile>, DoctorFinding> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(DoctorFinding::error(
                FindingKind::InstanceConfig,
                "instance config cannot be read",
                path.display().to_string(),
                format!("{}: {err}", path.display()),
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            FindingKind::InstanceConfig,
            "instance config is a symlink",
            path.display().to_string(),
            format!("{} must be a regular file", path.display()),
        ));
    }
    if !metadata.is_file() {
        return Err(DoctorFinding::error(
            FindingKind::InstanceConfig,
            "instance config is not a regular file",
            path.display().to_string(),
            format!("{} is not a file", path.display()),
        ));
    }
    let content = fs::read_to_string(path).map_err(|err| {
        DoctorFinding::error(
            FindingKind::InstanceConfig,
            "instance config cannot be read",
            path.display().to_string(),
            format!("{}: {err}", path.display()),
        )
    })?;
    toml::from_str(&content).map(Some).map_err(|err| {
        DoctorFinding::error(
            FindingKind::InstanceConfig,
            "instance config cannot be parsed",
            path.display().to_string(),
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
    repair_instance: &str,
) -> std::result::Result<InstanceHomeDiscovery, DoctorFinding> {
    let instances_dir = instances_dir_path(project_root);
    let metadata = fs::symlink_metadata(&instances_dir).map_err(|err| {
        DoctorFinding::error(
            FindingKind::InstancesDirectory,
            "instances directory is missing or unreadable",
            instances_dir.display().to_string(),
            format!("{}: {err}", instances_dir.display()),
        )
        .with_repair(project_create_instance_command(
            project_root,
            repair_instance,
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(DoctorFinding::error(
            FindingKind::InstancesDirectory,
            "instances directory is a symlink",
            instances_dir.display().to_string(),
            format!("{} must be a real directory", instances_dir.display()),
        ));
    }
    if !metadata.is_dir() {
        return Err(DoctorFinding::error(
            FindingKind::InstancesDirectory,
            "instances path is not a directory",
            instances_dir.display().to_string(),
            format!("{} is not a directory", instances_dir.display()),
        ));
    }

    let mut discovery = InstanceHomeDiscovery::default();
    for entry in fs::read_dir(&instances_dir)
        .with_context(|| format!("failed to read {}", instances_dir.display()))
        .map_err(|err| {
            DoctorFinding::error(
                FindingKind::InstancesDirectory,
                "instances directory cannot be read",
                instances_dir.display().to_string(),
                err.to_string(),
            )
        })?
    {
        let entry = entry.map_err(|err| {
            DoctorFinding::error(
                FindingKind::InstancesDirectory,
                "instances directory cannot be read",
                instances_dir.display().to_string(),
                err.to_string(),
            )
        })?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            discovery.findings.push(DoctorFinding::error(
                FindingKind::InstanceEntry,
                "instance entry name is not UTF-8",
                path.display().to_string(),
                format!(
                    "{} cannot be addressed as a LionClaw instance",
                    path.display()
                ),
            ));
            continue;
        };
        if let Err(err) = validate_instance_name(name) {
            discovery.findings.push(DoctorFinding::error(
                FindingKind::InstanceEntry,
                format!("instance entry \"{name}\" has invalid name"),
                path.display().to_string(),
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

fn project_create_instance_command(project_root: &Path, instance: &str) -> String {
    project_command(
        project_root,
        &format!("instance create {}", shell_quote_arg(instance)),
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

    use axum::{http::StatusCode, routing::get, Json, Router};

    use super::*;
    use crate::contracts::DaemonInfoResponse;
    use crate::kernel::runtime::{ConfinementConfig, OciConfinementConfig};
    use crate::operator::config::{daemon_compat_fingerprint, RuntimeProfileConfig};
    use crate::operator::managed_units::{
        channel_unit_name, daemon_unit_name, ensure_unit_identity, render_daemon_unit,
        DaemonUnitSpec, FakeUnitManager, UnitManager,
    };

    async fn configured_bind_fixture() -> (
        tempfile::TempDir,
        LionClawHome,
        PathBuf,
        DoctorCommands,
        OperatorConfig,
    ) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let work_root = temp_dir.path().join("work");
        fs::create_dir(&work_root).expect("work root");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        home.ensure_base_dirs().await.expect("base dirs");
        home.ensure_home_id().await.expect("home id");
        let home_root = home.root();
        let commands = DoctorCommands::for_target(None, "direct-home", &home_root);
        let mut config = OperatorConfig::default();
        config.daemon.bind_configured = true;
        (temp_dir, home, work_root, commands, config)
    }

    fn raw_daemon_fingerprint(home: &LionClawHome, config: &OperatorConfig) -> String {
        let applied_state =
            AppliedState::from_home_read_only(home, config).expect("read applied state");
        compute_daemon_fingerprint(&daemon_compat_fingerprint(config), &applied_state)
    }

    async fn current_daemon_fingerprint(home: &LionClawHome, config: &OperatorConfig) -> String {
        let applied_state =
            AppliedState::from_home_read_only(home, config).expect("read applied state");
        expected_daemon_fingerprint(home, config, &applied_state)
            .await
            .expect("daemon fingerprint")
    }

    fn daemon_info(
        home: &LionClawHome,
        bind: &str,
        home_id: String,
        project_scope: String,
        daemon_fingerprint: String,
    ) -> DaemonInfoResponse {
        DaemonInfoResponse {
            daemon: "lionclawd".to_string(),
            status: "ok".to_string(),
            home_id,
            home_root: home.root().display().to_string(),
            bind_addr: bind.to_string(),
            project_scope,
            daemon_fingerprint,
        }
    }

    async fn spawn_daemon_info_server(
        info: DaemonInfoResponse,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind daemon info server");
        let bind = listener.local_addr().expect("daemon info addr").to_string();
        let app = Router::new().route(
            "/v0/daemon/info",
            get({
                let info = info.clone();
                move || {
                    let info = info.clone();
                    async move { Json(info) }
                }
            }),
        );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve daemon info app");
        });
        (bind, handle)
    }

    async fn spawn_incompatible_lionclaw_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind incompatible daemon server");
        let bind = listener.local_addr().expect("daemon addr").to_string();
        let app = Router::new()
            .route("/v0/daemon/info", get(|| async { StatusCode::NOT_FOUND }))
            .route(
                "/health",
                get(|| async { Json(serde_json::json!({ "daemon": "lionclawd" })) }),
            );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve incompatible daemon app");
        });
        (bind, handle)
    }

    async fn mark_owned_daemon_active(
        home: &LionClawHome,
        manager: &FakeUnitManager,
        bind: &str,
        work_root: &Path,
        daemon_fingerprint: &str,
    ) {
        let identity = ensure_unit_identity(home).expect("unit identity");
        let daemon = render_daemon_unit(
            home,
            &identity,
            Path::new("/tmp/lionclawd"),
            DaemonUnitSpec {
                bind_addr: bind,
                runtime_id: "codex",
                workspace: crate::home::DEFAULT_WORKSPACE,
                project_workspace_root: work_root,
                daemon_fingerprint,
                codex_home_override: None,
            },
        );
        manager
            .apply_units(home, &[daemon])
            .await
            .expect("apply daemon unit");
        manager
            .set_unit_status(daemon_unit_name(&identity), "loaded/active/running")
            .expect("set daemon status");
    }

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
            commands.unresolved_work_root_note(),
            UNRESOLVED_WORK_ROOT_NOTE
        );
        assert_eq!(
            commands.create_instance_repair().as_deref(),
            Some("lionclaw --project /repo instance create reviewer")
        );
    }

    #[test]
    fn doctor_commands_omit_project_repairs_for_direct_home_targets() {
        let commands =
            DoctorCommands::for_target(None, "direct-home", Path::new("/tmp/lionclaw-home"));

        assert_eq!(
            commands.selected("status"),
            "lionclaw --home /tmp/lionclaw-home status"
        );
        assert!(commands.create_instance_repair().is_none());
        assert_eq!(
            commands.unresolved_work_root_note(),
            UNRESOLVED_WORK_ROOT_NOTE
        );
    }

    #[test]
    fn doctor_report_exit_state_follows_errors_only() {
        let warning = DoctorReport {
            findings: vec![DoctorFinding::warning(
                FindingKind::ProjectUnit,
                "legacy unit",
                "lionclaw-old.service",
                "left alone",
            )],
        };
        assert!(!warning.has_errors());

        let error = DoctorReport {
            findings: vec![DoctorFinding::error(
                FindingKind::Runtime,
                "missing runtime",
                "instance main runtime defaults",
                "configure first",
            )],
        };
        assert!(error.has_errors());
        let rendered = error.render();
        assert!(rendered.contains("[LC-D050] error: missing runtime"));
        assert!(rendered.contains("target: instance main runtime defaults"));
        assert!(
            rendered.contains("expected: selected instance has a valid configured runtime profile")
        );
        assert!(rendered.contains("observed: configure first"));
        assert!(rendered.contains("inspect: lionclaw status"));
    }

    #[test]
    fn doctor_reports_missing_podman_before_configure_repair() {
        let commands = DoctorCommands::for_target(
            Some(Path::new("/repo")),
            "main",
            Path::new("/repo/.lionclaw/instances/main"),
        );
        let config = OperatorConfig::default();
        let mut findings = Vec::new();

        inspect_runtime_config_with_podman_resolver(
            "main",
            &commands,
            &config,
            &mut findings,
            || {
                Err(anyhow::anyhow!(
                    "Podman is required for OCI confinement, but host executable `podman` is not available in the environment running LionClaw"
                ))
            },
        );

        let finding = findings.first().expect("runtime finding");
        assert_eq!(finding.id, "LC-D050");
        assert_eq!(
            finding.subject.as_ref(),
            "instance \"main\" cannot configure Codex because Podman is unavailable"
        );
        assert_eq!(finding.target.as_ref(), "instance main OCI engine podman");
        assert_eq!(finding.runbook.inspect.as_ref(), "command -v podman");
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some("Install Podman or run LionClaw where Podman is available before running the repair command")
        );
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some("lionclaw --project /repo --instance main configure --runtime codex")
        );
    }

    #[tokio::test]
    async fn doctor_reports_configured_bind_classification_blockers_as_runbook_findings() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        home.ensure_base_dirs().await.expect("base dirs");
        let home_root = home.root();
        let commands = DoctorCommands::for_target(None, "direct-home", &home_root);
        let mut config = OperatorConfig::default();
        config.daemon.bind = "127.0.0.1:38999".to_string();
        config.daemon.bind_configured = true;
        let manager = FakeUnitManager::default();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            None,
            &manager,
            &mut findings,
        )
        .await;

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D009");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert_eq!(
            finding.target.as_ref(),
            "instance direct-home configured bind 127.0.0.1:38999"
        );
        assert_eq!(
            finding.expected.as_ref(),
            "configured bind can be classified read-only for the selected instance"
        );
        assert_eq!(
            finding.observed.as_ref(),
            "selected instance work root is not valid enough to compute daemon project scope"
        );
        let expected_inspect = commands.selected("status");
        assert_eq!(finding.runbook.inspect.as_ref(), expected_inspect.as_str());
    }

    #[tokio::test]
    async fn doctor_configured_bind_does_not_materialize_applied_skills() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let work_root = temp_dir.path().join("work");
        fs::create_dir(&work_root).expect("work root");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        home.ensure_base_dirs().await.expect("base dirs");
        home.ensure_home_id().await.expect("home id");
        let skill = home.skills_dir().join("visible");
        fs::create_dir(&skill).expect("skill dir");
        fs::write(
            skill.join("SKILL.md"),
            "---\nname: visible\ndescription: visible\n---\n",
        )
        .expect("skill metadata");
        let home_root = home.root();
        let commands = DoctorCommands::for_target(None, "direct-home", &home_root);
        let mut config = OperatorConfig::default();
        config.daemon.bind = "127.0.0.1:0".to_string();
        config.daemon.bind_configured = true;
        let manager = FakeUnitManager::default();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &manager,
            &mut findings,
        )
        .await;

        assert!(findings.is_empty());
        assert!(!home.skills_dir().join(".applied").exists());
    }

    #[tokio::test]
    async fn doctor_reports_invalid_configured_bind_as_unclassifiable() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        config.daemon.bind = "not a bind".to_string();
        let manager = FakeUnitManager::default();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &manager,
            &mut findings,
        )
        .await;

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D009");
        assert_eq!(
            finding.subject.as_ref(),
            "configured bind cannot be classified"
        );
        assert!(finding
            .observed
            .contains("configured daemon bind 'not a bind' cannot be resolved"));
        assert_eq!(finding.runbook.inspect.as_ref(), "ss -ltnp");
    }

    #[tokio::test]
    async fn doctor_reports_non_lionclaw_listener_on_configured_bind() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let work_root = temp_dir.path().join("work");
        fs::create_dir(&work_root).expect("work root");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        home.ensure_base_dirs().await.expect("base dirs");
        home.ensure_home_id().await.expect("home id");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let bind = listener.local_addr().expect("listener addr").to_string();
        let accept_task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        });
        let home_root = home.root();
        let commands = DoctorCommands::for_target(None, "direct-home", &home_root);
        let mut config = OperatorConfig::default();
        config.daemon.bind = bind.clone();
        config.daemon.bind_configured = true;
        let manager = FakeUnitManager::default();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &manager,
            &mut findings,
        )
        .await;
        accept_task.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D001");
        assert_eq!(finding.subject.as_ref(), "configured bind is occupied");
        assert_eq!(
            finding.target.as_ref(),
            format!("instance direct-home configured bind {bind}")
        );
        assert_eq!(
            finding.expected.as_ref(),
            "no listener, or the owned managed daemon for this instance"
        );
        assert_eq!(
            finding.observed.as_ref(),
            format!("{bind} is used by a non-LionClaw process")
        );
        let expected_inspect = inspect_listener_command(&bind);
        assert_eq!(finding.runbook.inspect.as_ref(), expected_inspect.as_str());
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some("stop the process shown by inspect")
        );
        assert_eq!(finding.runbook.repair.as_deref(), None);
    }

    #[tokio::test]
    async fn doctor_reports_foreign_home_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        let home_id = home.ensure_home_id().await.expect("home id");
        let project_scope = runtime_project_partition_key(Some(&work_root));
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config).await;
        let mut info = daemon_info(
            &home,
            "127.0.0.1:0",
            "foreign-home-id".to_string(),
            project_scope,
            daemon_fingerprint,
        );
        info.home_root = "/tmp/other-home".to_string();
        let (bind, server) = spawn_daemon_info_server(info).await;
        config.daemon.bind = bind.clone();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &FakeUnitManager::default(),
            &mut findings,
        )
        .await;
        server.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(home_id, home.read_home_id().await.unwrap().unwrap());
        assert_eq!(finding.id, "LC-D002");
        assert_eq!(
            finding.runbook.inspect.as_ref(),
            "lionclaw --home /tmp/other-home status"
        );
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some("lionclaw --home /tmp/other-home down")
        );
    }

    #[tokio::test]
    async fn doctor_reports_same_home_different_project_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        let home_id = home.ensure_home_id().await.expect("home id");
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config).await;
        let info = daemon_info(
            &home,
            "127.0.0.1:0",
            home_id,
            "different-project-scope".to_string(),
            daemon_fingerprint,
        );
        let (bind, server) = spawn_daemon_info_server(info).await;
        config.daemon.bind = bind.clone();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &FakeUnitManager::default(),
            &mut findings,
        )
        .await;
        server.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D003");
        assert_eq!(
            finding.subject.as_ref(),
            "configured bind is served by this home for a different project"
        );
        assert_eq!(
            finding.runbook.inspect.as_ref(),
            inspect_listener_command(&bind).as_str()
        );
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some("stop the daemon shown by inspect")
        );
        assert_eq!(finding.runbook.repair.as_deref(), None);
    }

    #[tokio::test]
    async fn doctor_reports_same_home_foreground_daemon_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        let home_id = home.ensure_home_id().await.expect("home id");
        let project_scope = runtime_project_partition_key(Some(&work_root));
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config).await;
        let info = daemon_info(
            &home,
            "127.0.0.1:0",
            home_id,
            project_scope,
            daemon_fingerprint,
        );
        let (bind, server) = spawn_daemon_info_server(info).await;
        config.daemon.bind = bind.clone();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &FakeUnitManager::default(),
            &mut findings,
        )
        .await;
        server.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D003");
        assert_eq!(
            finding.subject.as_ref(),
            "configured bind is served by an unmanaged foreground daemon"
        );
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some("stop the foreground daemon shown by inspect")
        );
        assert_eq!(finding.runbook.repair.as_deref(), None);
    }

    #[tokio::test]
    async fn doctor_matches_contextual_daemon_fingerprint_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    image: None,
                    ..OciConfinementConfig::default()
                }),
            },
        );
        let home_id = home.ensure_home_id().await.expect("home id");
        let project_scope = runtime_project_partition_key(Some(&work_root));
        let raw_fingerprint = raw_daemon_fingerprint(&home, &config);
        let contextual_fingerprint = current_daemon_fingerprint(&home, &config).await;
        assert_ne!(raw_fingerprint, contextual_fingerprint);
        let info = daemon_info(
            &home,
            "127.0.0.1:0",
            home_id,
            project_scope,
            contextual_fingerprint.clone(),
        );
        let (bind, server) = spawn_daemon_info_server(info).await;
        config.daemon.bind = bind.clone();
        let manager = FakeUnitManager::default();
        mark_owned_daemon_active(&home, &manager, &bind, &work_root, &contextual_fingerprint).await;
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &manager,
            &mut findings,
        )
        .await;
        server.abort();

        assert!(findings.is_empty(), "{findings:#?}");
    }

    #[tokio::test]
    async fn doctor_reports_stale_managed_daemon_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        let home_id = home.ensure_home_id().await.expect("home id");
        let project_scope = runtime_project_partition_key(Some(&work_root));
        let current_fingerprint = current_daemon_fingerprint(&home, &config).await;
        let info = daemon_info(
            &home,
            "127.0.0.1:0",
            home_id,
            project_scope,
            "stale-daemon-fingerprint".to_string(),
        );
        let (bind, server) = spawn_daemon_info_server(info).await;
        config.daemon.bind = bind.clone();
        let manager = FakeUnitManager::default();
        mark_owned_daemon_active(&home, &manager, &bind, &work_root, &current_fingerprint).await;
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &manager,
            &mut findings,
        )
        .await;
        server.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D008");
        assert_eq!(
            finding.subject.as_ref(),
            "managed daemon is running stale configuration"
        );
        assert_eq!(
            finding.runbook.inspect.as_ref(),
            commands.selected("status").as_str()
        );
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(commands.selected("up").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_reports_incompatible_lionclaw_on_configured_bind() {
        let (_temp_dir, home, work_root, commands, mut config) = configured_bind_fixture().await;
        let (bind, server) = spawn_incompatible_lionclaw_server().await;
        config.daemon.bind = bind.clone();
        let mut findings = Vec::new();

        inspect_configured_bind(
            &home,
            "direct-home",
            &commands,
            &config,
            Some(&work_root),
            &FakeUnitManager::default(),
            &mut findings,
        )
        .await;
        server.abort();

        let finding = findings.first().expect("configured bind finding");
        assert_eq!(finding.id, "LC-D004");
        assert_eq!(
            finding.subject.as_ref(),
            "configured bind is served by an older LionClaw daemon"
        );
        assert_eq!(
            finding.runbook.inspect.as_ref(),
            inspect_listener_command(&bind).as_str()
        );
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some("stop the older LionClaw daemon shown by inspect")
        );
        assert_eq!(finding.runbook.repair.as_deref(), None);
    }

    #[tokio::test]
    async fn doctor_reports_missing_project_file_inside_existing_metadata_dir() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir(temp_dir.path().join(".lionclaw")).expect("metadata dir");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "project file is missing or unreadable");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_command(temp_dir.path(), "project init").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_reports_absent_project_metadata_as_no_project_found() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "no LionClaw project found");
        assert_eq!(finding.id, "LC-D010");
        assert_eq!(
            finding.target.as_ref(),
            temp_dir.path().display().to_string()
        );
        assert_eq!(
            finding.observed.as_ref(),
            format!(
                "no .lionclaw metadata found at {}",
                project_dir_path(temp_dir.path()).display()
            )
        );
        assert_eq!(
            finding.runbook.inspect.as_ref(),
            format!(
                "ls -ld {}",
                shell_quote_arg(&project_dir_path(temp_dir.path()).display().to_string())
            )
        );
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_command(temp_dir.path(), "project init").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_reports_existing_bad_project_metadata_as_metadata_error() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::write(project_dir_path(temp_dir.path()), "not a directory").expect("metadata file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "project metadata path is not a directory");
        assert_eq!(
            finding.target.as_ref(),
            project_dir_path(temp_dir.path()).display().to_string()
        );
        assert_eq!(finding.runbook.repair.as_deref(), None);
    }

    #[tokio::test]
    async fn doctor_scopes_missing_instances_repair_to_project() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(project_dir_path(temp_dir.path())).expect("metadata dir");
        fs::write(project_file_path(temp_dir.path()), "version = 1\n").expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "instances directory is missing or unreadable");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "main").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_missing_instances_repair_uses_valid_default_instance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(project_dir_path(temp_dir.path())).expect("metadata dir");
        fs::write(
            project_file_path(temp_dir.path()),
            "version = 1\ndefault_instance = \"reviewer\"\n",
        )
        .expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "instances directory is missing or unreadable");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "reviewer").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_normalizes_default_instance_before_repair_selection() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(project_dir_path(temp_dir.path())).expect("metadata dir");
        fs::write(
            project_file_path(temp_dir.path()),
            "version = 1\ndefault_instance = \" reviewer \"\n",
        )
        .expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "instances directory is missing or unreadable");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "reviewer").as_str())
        );
        assert!(!report.findings.iter().any(
            |finding| finding.subject.as_ref() == "default instance \" reviewer \" is invalid"
        ));
    }

    #[tokio::test]
    async fn doctor_treats_blank_default_instance_as_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(project_dir_path(temp_dir.path())).expect("metadata dir");
        fs::write(
            project_file_path(temp_dir.path()),
            "version = 1\ndefault_instance = \"   \"\n",
        )
        .expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "instances directory is missing or unreadable");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "main").as_str())
        );
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.subject.as_ref() == "default instance \"   \" is invalid"));
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
        let finding = finding(&report, "project has no instances");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "main").as_str())
        );
    }

    #[tokio::test]
    async fn doctor_all_empty_instances_repair_uses_valid_default_instance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fs::create_dir_all(instances_dir_path(temp_dir.path())).expect("instances dir");
        fs::write(
            project_file_path(temp_dir.path()),
            "version = 1\ndefault_instance = \"reviewer\"\n",
        )
        .expect("project file");

        let report = inspect_project(temp_dir.path(), None, true, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(&report, "project has no instances");
        assert_eq!(
            finding.runbook.repair.as_deref(),
            Some(project_create_instance_command(temp_dir.path(), "reviewer").as_str())
        );
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
        let finding = finding(&report, "default instance \"../../some-home\" is invalid");
        assert!(finding.runbook.repair.is_none());
        let expected_note = format!(
            "edit {} so default_instance is a valid instance name",
            project_file_path(temp_dir.path()).display()
        );
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some(expected_note.as_str())
        );
    }

    #[tokio::test]
    async fn doctor_reports_multiple_instances_without_default_as_manual_selection() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let instances_dir = instances_dir_path(temp_dir.path());
        fs::create_dir_all(instances_dir.join("main")).expect("main instance");
        fs::create_dir_all(instances_dir.join("reviewer")).expect("reviewer instance");
        fs::write(project_file_path(temp_dir.path()), "version = 1\n").expect("project file");

        let report = inspect_project(temp_dir.path(), None, false, &FakeUnitManager::default())
            .await
            .expect("doctor report");

        let finding = finding(
            &report,
            "project has multiple instances and no default_instance",
        );
        assert!(finding.runbook.repair.is_none());
        let expected_note = format!(
            "choose a default_instance in {} or rerun doctor with --instance NAME",
            project_file_path(temp_dir.path()).display()
        );
        assert_eq!(
            finding.runbook.note.as_deref(),
            Some(expected_note.as_str())
        );
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
            .any(|finding| finding.subject.as_ref()
                == "instance entry \"bad name\" has invalid name"));
        assert!(report.findings.iter().any(
            |finding| finding.subject.as_ref() == "instance entry \".hidden\" has invalid name"
        ));
        let work_root_finding = finding(&report, "instance \"main\" does not record a work root");
        assert!(work_root_finding.runbook.repair.is_none());
        assert_eq!(
            work_root_finding.runbook.note.as_deref(),
            Some(UNRESOLVED_WORK_ROOT_NOTE)
        );
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
            .any(|finding| finding.subject.as_ref()
                == "instance entry \"bad name\" has invalid name"));
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.subject.as_ref() == "project has no instances"));
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
            .any(|finding| finding.subject.as_ref() == "instances directory is a symlink"));
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
            .any(|finding| finding.subject.as_ref() == "instances directory is a symlink"));
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

        assert!(findings.iter().any(|finding| finding.subject.as_ref()
            == "instance \"main\" work root escapes project root"));
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

        assert!(findings.iter().any(|finding| finding.subject.as_ref()
            == "managed daemon unit is missing for instance \"direct-home\""));
        assert!(findings.iter().any(|finding| finding.subject.as_ref()
            == "worker \"telegram\" is not running for instance \"direct-home\""));
    }

    fn finding<'a>(report: &'a DoctorReport, subject: &str) -> &'a DoctorFinding {
        report
            .findings
            .iter()
            .find(|finding| finding.subject.as_ref() == subject)
            .expect("doctor finding")
    }
}
