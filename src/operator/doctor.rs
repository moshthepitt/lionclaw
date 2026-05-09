use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::{
    home::LionClawHome,
    kernel::{runtime::ConfinementConfig, skills::validate_skill_alias},
    operator::{
        channel_env::validate_channel_env_contract,
        channel_metadata::{load_channel_metadata, validate_channel_id},
        command_display::{lionclaw_home_command_prefix, shell_quote_arg},
        config::{ChannelLaunchMode, ManagedChannelConfig, OperatorConfig, RuntimeProfileConfig},
        managed_units::{
            channel_unit_name, daemon_unit_name, existing_unit_identity, unit_channel_id,
            unit_recorded_home_root, unit_status_is_active, UnitIdentity, UnitManager,
        },
        runtime_integration::runtime_auth_guidance,
        target::{
            discover_project_root, instance_home_path, instances_dir_path, project_file_path,
            TargetSelection,
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

    if let Some(home) = selection.home.as_deref() {
        let home = absolute_path(home)?;
        return inspect_direct_home(&home, manager).await;
    }

    let project_selection = TargetSelection {
        home: None,
        project: selection.project.clone(),
        instance: None,
    };
    let project_root = discover_project_root(&project_selection)?;
    inspect_project(
        &project_root,
        selection
            .instance
            .as_deref()
            .filter(|value| !value.is_empty()),
        all,
        manager,
    )
    .await
}

async fn inspect_project<M: UnitManager>(
    project_root: &Path,
    selected_instance: Option<&str>,
    all: bool,
    manager: &M,
) -> Result<DoctorReport> {
    let mut report = DoctorReport::default();
    let project_file = project_file_path(project_root);
    let project_config = match read_project_file(project_root) {
        Ok(config) => Some(config),
        Err(finding) => {
            report.push(finding);
            None
        }
    };

    let instances = match read_instance_homes(project_root) {
        Ok(instances) => instances,
        Err(finding) => {
            report.push(finding);
            BTreeMap::new()
        }
    };

    let selected_names = if all {
        instances.keys().cloned().collect::<Vec<_>>()
    } else if let Some(name) = selected_instance {
        vec![name.to_string()]
    } else {
        selected_project_instance_names(
            project_root,
            project_config.as_ref(),
            &instances,
            &mut report,
        )
    };

    if let Some(config) = project_config.as_ref() {
        if let Some(default_instance) = config.default_instance.as_deref() {
            if !instances.contains_key(default_instance) {
                report.push(
                    DoctorFinding::error(
                        format!(
                            "default instance \"{default_instance}\" is configured but missing"
                        ),
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

fn selected_project_instance_names(
    project_root: &Path,
    project_config: Option<&DiagnosticProjectFile>,
    instances: &BTreeMap<String, PathBuf>,
    report: &mut DoctorReport,
) -> Vec<String> {
    if let Some(default_instance) = project_config
        .and_then(|config| config.default_instance.as_deref())
        .filter(|value| !value.is_empty())
    {
        return vec![default_instance.to_string()];
    }

    match instances.keys().cloned().collect::<Vec<_>>().as_slice() {
        [only] => vec![only.clone()],
        [] => {
            report.push(
                DoctorFinding::error(
                    "project has no instances",
                    format!(
                        "{} contains no instance homes",
                        instances_dir_path(project_root).display()
                    ),
                )
                .with_repair(project_command(project_root, "instance create main")),
            );
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

    inspect_oci_image_readiness(runtime_id, profile, findings);
}

fn inspect_oci_image_readiness(
    runtime_id: &str,
    profile: &RuntimeProfileConfig,
    findings: &mut Vec<DoctorFinding>,
) {
    let ConfinementConfig::Oci(oci) = profile.confinement();
    let Some(image) = oci.image.as_deref() else {
        return;
    };
    if profile.validate().is_err() {
        return;
    }
    let output = Command::new(&oci.engine)
        .args(["image", "exists", image])
        .output();
    match output {
        Ok(output) if output.status.success() => {}
        Ok(_) => findings.push(
            DoctorFinding::error(
                format!("runtime image for \"{runtime_id}\" is not available locally"),
                format!("OCI image {image} was not found by {}", oci.engine),
            )
            .with_repair(format!(
                "podman build -t {image} -f containers/runtime/Containerfile ."
            )),
        ),
        Err(err) => findings.push(DoctorFinding::warning(
            format!("runtime image for \"{runtime_id}\" could not be checked"),
            format!("{} image exists {image}: {err}", oci.engine),
        )),
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
    let service_channels = config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Background)
        .collect::<Vec<_>>();
    if service_channels.is_empty() && existing_unit_identity(home).ok().flatten().is_none() {
        return;
    }
    let identity = match existing_unit_identity(home) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            findings.push(
                DoctorFinding::error(
                    format!("managed daemon unit is missing for instance \"{name}\""),
                    "background channels are configured but this home has no managed unit identity",
                )
                .with_repair(commands.selected("up")),
            );
            return;
        }
        Err(err) => {
            findings.push(DoctorFinding::error(
                format!("managed unit identity is invalid for instance \"{name}\""),
                err.to_string(),
            ));
            return;
        }
    };

    inspect_unit_status(
        name,
        "daemon",
        &daemon_unit_name(&identity),
        commands,
        manager,
        findings,
    )
    .await;
    for channel in service_channels {
        inspect_unit_status(
            name,
            &format!("worker \"{}\"", channel.id),
            &channel_unit_name(&identity, &channel.id),
            commands,
            manager,
            findings,
        )
        .await;
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
    let expected = expected_unit_names(config, &identity);
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
        let Ok(recorded_home) = unit_recorded_home_root(&unit_path) else {
            continue;
        };
        if recorded_home.as_deref() != Some(identity.home_root.as_path()) {
            continue;
        }
        if expected.contains(unit_name) {
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
    for unit_path in user_lionclaw_unit_files()? {
        let Some(unit_name) = unit_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !unit_name.starts_with("lionclaw") || !unit_name.ends_with(".service") {
            continue;
        }
        let recorded_home = match unit_recorded_home_root(&unit_path)? {
            Some(home) => home,
            None => {
                findings.push(DoctorFinding::warning(
                    "legacy or unowned LionClaw-looking unit",
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
        if known_homes.contains(&recorded_home) {
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
        if unit_channel_id(&unit_path)?.is_some() {
            continue;
        }
    }
    Ok(findings)
}

fn expected_unit_names(config: &OperatorConfig, identity: &UnitIdentity) -> BTreeSet<String> {
    let mut expected = BTreeSet::from([daemon_unit_name(identity)]);
    for channel in &config.channels {
        if channel.launch_mode == ChannelLaunchMode::Background {
            expected.insert(channel_unit_name(identity, &channel.id));
        }
    }
    expected
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

fn read_instance_homes(
    project_root: &Path,
) -> std::result::Result<BTreeMap<String, PathBuf>, DoctorFinding> {
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

    let mut homes = BTreeMap::new();
    for entry in fs::read_dir(&instances_dir)
        .with_context(|| format!("failed to read {}", instances_dir.display()))
        .map_err(|err| {
            DoctorFinding::error("instances directory cannot be read", err.to_string())
        })?
    {
        let entry = entry.map_err(|err| {
            DoctorFinding::error("instances directory cannot be read", err.to_string())
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        homes.insert(name, entry.path());
    }
    Ok(homes)
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
    use std::os::unix::fs::symlink;

    use super::*;

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
}
