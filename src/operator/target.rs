use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::home::LionClawHome;

pub const PROJECT_DIR: &str = ".lionclaw";
pub const PROJECT_FILE: &str = "project.toml";
pub const INSTANCES_DIR: &str = "instances";
pub const DEFAULT_INSTANCE: &str = "main";

const INSTANCE_CONFIG_FILE: &str = "instance.toml";

#[derive(Debug, Clone, Default)]
pub struct TargetSelection {
    pub home: Option<PathBuf>,
    pub project: Option<PathBuf>,
    pub instance: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkRootRequirement {
    Optional,
    Required,
}

#[derive(Debug, Clone)]
pub struct TargetContext {
    pub project_root: Option<PathBuf>,
    pub instance_name: Option<String>,
    pub instance_home: LionClawHome,
    pub work_root: Option<PathBuf>,
}

impl TargetContext {
    pub fn require_work_root(&self) -> Result<&Path> {
        self.work_root
            .as_deref()
            .ok_or_else(|| anyhow!("target does not have a resolved work root"))
    }
}

#[derive(Debug, Clone)]
pub struct ProjectInitResult {
    pub project_root: PathBuf,
    pub instance: InstanceRecord,
}

#[derive(Debug, Clone)]
pub struct InstanceRecord {
    pub name: String,
    pub home: PathBuf,
    pub work_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct InstanceListEntry {
    pub name: String,
    pub is_default: bool,
    pub home: PathBuf,
    pub work_root: Option<PathBuf>,
    pub shared_work_root_count: usize,
}

#[derive(Debug, Clone)]
pub struct WorkRootInspection {
    pub work_root: Option<PathBuf>,
    pub finding: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InstanceStatusEntry {
    pub name: String,
    pub is_default: bool,
    pub home: PathBuf,
    pub work_root: Option<PathBuf>,
    pub work_root_finding: Option<String>,
    pub shared_work_root_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectFileConfig {
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    default_instance: Option<String>,
}

#[derive(Debug, Clone)]
struct ProjectConfig {
    default_instance: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstanceFileConfig {
    #[serde(default = "config_version")]
    version: u32,
    work_root: PathBuf,
}

pub fn resolve_target(
    selection: &TargetSelection,
    work_root: WorkRootRequirement,
) -> Result<TargetContext> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    resolve_target_from_cwd(selection, work_root, &cwd)
}

pub fn resolve_project_setup_root(selection: &TargetSelection) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    resolve_project_setup_root_from_cwd(selection, &cwd)
}

pub fn resolve_existing_project_root(selection: &TargetSelection) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    if selection.home.is_some() || selection.instance.is_some() {
        bail!("project instance commands cannot be combined with --home or --instance");
    }
    resolve_project_root_from_cwd(selection.project.as_deref(), &cwd)
}

pub fn discover_project_root(selection: &TargetSelection) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    if selection.home.is_some() || selection.instance.is_some() {
        bail!("project-wide commands cannot be combined with --home or --instance");
    }
    discover_project_root_from_cwd(selection.project.as_deref(), &cwd)
}

pub fn init_project(project_root: &Path) -> Result<ProjectInitResult> {
    let project_root = canonical_existing_dir(project_root, "project root")?;
    let project_dir = project_dir(&project_root);
    let project_file = project_file(&project_root);
    match fs::symlink_metadata(&project_file) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "project config {} must not be a symlink",
                    project_file.display()
                );
            }
            bail!(
                "LionClaw project already exists at {}; use 'lionclaw instance create <name>' to add an instance",
                project_file.display()
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to stat project config {}", project_file.display())
            });
        }
    }
    if project_dir.exists() {
        ensure_directory_not_symlink(&project_dir, "project metadata directory")?;
    } else {
        fs::create_dir(&project_dir)
            .with_context(|| format!("failed to create {}", project_dir.display()))?;
    }

    ensure_instances_dir(&project_root)?;
    let config = ProjectFileConfig {
        version: Some(1),
        default_instance: Some(DEFAULT_INSTANCE.to_string()),
    };
    write_toml_file(&project_file, &config, "project config")?;
    let instance =
        create_project_instance(&project_root, DEFAULT_INSTANCE, Some(Path::new(".")), false)?;

    Ok(ProjectInitResult {
        project_root,
        instance,
    })
}

pub fn create_project_instance(
    project_root: &Path,
    name: &str,
    requested_work_root: Option<&Path>,
    create_work_root: bool,
) -> Result<InstanceRecord> {
    let project_root = canonical_project_root(project_root)?;
    validate_instance_name(name)?;
    ensure_instances_dir(&project_root)?;
    let home = instance_home(&project_root, name);
    if home.exists() {
        bail!(
            "instance '{name}' already exists at {}; choose another name or target it with --instance {name}",
            home.display()
        );
    }

    let work_root = resolve_project_work_root(
        &project_root,
        requested_work_root.unwrap_or_else(|| Path::new(".")),
        create_work_root,
    )?;
    let lionclaw_home = LionClawHome::new(home.clone());
    ensure_instance_base_dirs(&lionclaw_home)?;
    save_instance_config(home.as_path(), &work_root)?;

    Ok(InstanceRecord {
        name: name.to_string(),
        home,
        work_root,
    })
}

pub fn adopt_project_instance(
    project_root: &Path,
    name: &str,
    source_home: &Path,
    requested_work_root: Option<&Path>,
    create_work_root: bool,
) -> Result<InstanceRecord> {
    let project_root = canonical_project_root(project_root)?;
    validate_instance_name(name)?;
    ensure_instances_dir(&project_root)?;
    let source_home = absolutize_from(&std::env::current_dir()?, source_home);
    ensure_directory_not_symlink(&source_home, "source instance home")?;
    let source_home = fs::canonicalize(&source_home)
        .with_context(|| format!("failed to resolve {}", source_home.display()))?;
    let target_home = instance_home(&project_root, name);
    if target_home.exists() && source_home != target_home {
        bail!(
            "instance '{name}' already exists at {}; choose another name",
            target_home.display()
        );
    }

    let work_root = match requested_work_root {
        Some(path) => resolve_project_work_root(&project_root, path, create_work_root)?,
        None => {
            let Some(config) = load_instance_config(source_home.as_path())? else {
                bail!(
                    "adopted home {} does not record a work root; rerun with --work-root PATH",
                    source_home.display()
                );
            };
            validate_recorded_project_work_root(&project_root, &config.work_root)?
        }
    };

    if source_home != target_home {
        if let Some(parent) = target_home.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::rename(&source_home, &target_home).with_context(|| {
            format!(
                "failed to move adopted home {} to {}",
                source_home.display(),
                target_home.display()
            )
        })?;
    }
    let lionclaw_home = LionClawHome::new(target_home.clone());
    ensure_instance_base_dirs(&lionclaw_home)?;
    save_instance_config(target_home.as_path(), &work_root)?;

    Ok(InstanceRecord {
        name: name.to_string(),
        home: target_home,
        work_root,
    })
}

pub fn list_project_instances(project_root: &Path) -> Result<Vec<InstanceListEntry>> {
    let project_root = canonical_project_root(project_root)?;
    let config = load_project_config(&project_root)?;
    let names = list_instance_names(&project_root)?;
    let mut work_roots_by_name = BTreeMap::new();
    let mut counts = BTreeMap::<PathBuf, usize>::new();
    for name in &names {
        let home = instance_home(&project_root, name);
        let work_root = load_instance_config(&home)?
            .map(|config| validate_recorded_project_work_root(&project_root, &config.work_root))
            .transpose()?;
        if let Some(work_root) = work_root {
            *counts.entry(work_root.clone()).or_insert(0) += 1;
            work_roots_by_name.insert(name.clone(), work_root);
        }
    }

    Ok(names
        .into_iter()
        .map(|name| {
            let home = instance_home(&project_root, &name);
            let work_root = work_roots_by_name.get(&name).cloned();
            let shared_work_root_count = work_root
                .as_ref()
                .and_then(|path| counts.get(path))
                .copied()
                .unwrap_or(0);
            InstanceListEntry {
                is_default: config.default_instance.as_deref() == Some(name.as_str()),
                name,
                home,
                work_root,
                shared_work_root_count,
            }
        })
        .collect())
}

pub fn list_project_instance_statuses(project_root: &Path) -> Result<Vec<InstanceStatusEntry>> {
    let project_root = canonical_project_root(project_root)?;
    let config = load_project_config(&project_root)?;
    let names = list_instance_names(&project_root)?;
    let mut inspections_by_name = BTreeMap::new();
    let mut counts = BTreeMap::<PathBuf, usize>::new();

    for name in &names {
        let home = instance_home(&project_root, name);
        let inspection = inspect_project_instance_work_root(&project_root, &home);
        if let Some(work_root) = &inspection.work_root {
            *counts.entry(work_root.clone()).or_insert(0) += 1;
        }
        inspections_by_name.insert(name.clone(), inspection);
    }

    Ok(names
        .into_iter()
        .map(|name| {
            let home = instance_home(&project_root, &name);
            let inspection =
                inspections_by_name
                    .remove(&name)
                    .unwrap_or_else(|| WorkRootInspection {
                        work_root: None,
                        finding: Some("work root was not inspected".to_string()),
                    });
            let shared_work_root_count = inspection
                .work_root
                .as_ref()
                .and_then(|path| counts.get(path))
                .copied()
                .unwrap_or(0);
            InstanceStatusEntry {
                is_default: config.default_instance.as_deref() == Some(name.as_str()),
                name,
                home,
                work_root: inspection.work_root,
                work_root_finding: inspection.finding,
                shared_work_root_count,
            }
        })
        .collect())
}

pub fn inspect_target_work_root(target: &TargetContext) -> WorkRootInspection {
    match (&target.project_root, &target.instance_name) {
        (Some(project_root), Some(_)) => {
            inspect_project_instance_work_root(project_root, &target.instance_home.root())
        }
        _ => inspect_home_work_root(&target.instance_home.root()),
    }
}

fn resolve_target_from_cwd(
    selection: &TargetSelection,
    work_root: WorkRootRequirement,
    cwd: &Path,
) -> Result<TargetContext> {
    if let Some(home) = selection.home.as_deref() {
        if selection.project.is_some() || selection.instance.is_some() {
            bail!("--home cannot be combined with --project or --instance");
        }
        let home = absolutize_from(cwd, home);
        let work_root = match work_root {
            WorkRootRequirement::Optional => None,
            WorkRootRequirement::Required => Some(resolve_home_work_root(&home)?),
        };
        return Ok(TargetContext {
            project_root: None,
            instance_name: None,
            instance_home: LionClawHome::new(home),
            work_root,
        });
    }

    let project_root = resolve_project_root_from_cwd(selection.project.as_deref(), cwd)?;
    let config = load_project_config(&project_root)?;
    let instance_name =
        resolve_instance_name(&project_root, &config, selection.instance.as_deref())?;
    let home = resolve_project_instance_home(&project_root, &instance_name)?;
    let work_root = match work_root {
        WorkRootRequirement::Optional => None,
        WorkRootRequirement::Required => Some(resolve_project_instance_work_root(
            &project_root,
            &instance_name,
            &home,
        )?),
    };

    Ok(TargetContext {
        project_root: Some(project_root),
        instance_name: Some(instance_name),
        instance_home: LionClawHome::new(home),
        work_root,
    })
}

fn resolve_project_setup_root_from_cwd(selection: &TargetSelection, cwd: &Path) -> Result<PathBuf> {
    if selection.home.is_some() || selection.instance.is_some() {
        bail!("project setup commands cannot be combined with --home or --instance");
    }
    match selection.project.as_deref() {
        Some(project) => canonical_existing_dir(&absolutize_from(cwd, project), "project root"),
        None => canonical_existing_dir(cwd, "project root"),
    }
}

fn resolve_project_root_from_cwd(project: Option<&Path>, cwd: &Path) -> Result<PathBuf> {
    if let Some(project) = project {
        return canonical_project_root(&absolutize_from(cwd, project));
    }

    let cwd = canonical_existing_dir(cwd, "current directory")?;
    if let Some(instance_home) = containing_project_instance_home(&cwd) {
        bail!(
            "This looks like a LionClaw instance home, not a project root.\nRun from the project root, or use:\n  lionclaw --project <path> --instance <name> run\n  lionclaw --home {} run",
            instance_home.display()
        );
    }

    for candidate in [Some(cwd.as_path()), cwd.parent()].into_iter().flatten() {
        if project_file(candidate).exists() {
            return canonical_project_root(candidate);
        }
    }

    bail!(
        "no LionClaw project found from {}; run from the project root, pass --project PATH, or pass --home PATH",
        cwd.display()
    )
}

fn discover_project_root_from_cwd(project: Option<&Path>, cwd: &Path) -> Result<PathBuf> {
    if let Some(project) = project {
        return canonical_existing_dir(&absolutize_from(cwd, project), "project root");
    }

    let cwd = canonical_existing_dir(cwd, "current directory")?;
    if let Some(_instance_home) = containing_project_instance_home(&cwd) {
        bail!(
            "This looks like a LionClaw instance home, not a project root.\nRun from the project root, or use --project <path>."
        );
    }

    for candidate in [Some(cwd.as_path()), cwd.parent()].into_iter().flatten() {
        if project_file(candidate).exists() {
            return canonical_existing_dir(candidate, "project root");
        }
    }

    bail!(
        "no LionClaw project found from {}; run from the project root or pass --project PATH",
        cwd.display()
    )
}

fn resolve_instance_name(
    project_root: &Path,
    config: &ProjectConfig,
    requested: Option<&str>,
) -> Result<String> {
    if let Some(name) = requested.map(str::trim).filter(|value| !value.is_empty()) {
        validate_instance_name(name)?;
        return Ok(name.to_string());
    }

    if let Some(name) = config.default_instance.as_deref() {
        validate_instance_name(name)?;
        require_instances_dir(project_root)?;
        if !instance_home(project_root, name).exists() {
            bail!(
                "default instance '{name}' is configured but missing; repair with 'lionclaw instance create {name}' or update {}",
                project_file(project_root).display()
            );
        }
        return Ok(name.to_string());
    }

    let names = list_instance_names(project_root)?;
    match names.as_slice() {
        [name] => Ok(name.clone()),
        [] => bail!(
            "project has no instances; repair with 'lionclaw instance create {DEFAULT_INSTANCE}'"
        ),
        _ => bail!(
            "project has multiple instances and no default_instance; rerun with --instance NAME or set default_instance in {}",
            project_file(project_root).display()
        ),
    }
}

fn resolve_project_instance_home(project_root: &Path, instance_name: &str) -> Result<PathBuf> {
    require_instances_dir(project_root)?;
    let home = instance_home(project_root, instance_name);
    if !home.exists() {
        bail!(
            "instance '{instance_name}' is missing at {}; repair with 'lionclaw instance create {instance_name}'",
            home.display()
        );
    }
    ensure_directory_not_symlink(&home, "instance home")?;
    fs::canonicalize(&home).with_context(|| format!("failed to resolve {}", home.display()))
}

fn resolve_project_instance_work_root(
    project_root: &Path,
    instance_name: &str,
    home: &Path,
) -> Result<PathBuf> {
    let Some(config) = load_instance_config(home)? else {
        bail!(
            "instance '{instance_name}' does not record a work root; repair with 'lionclaw instance adopt {instance_name} {} --work-root PATH'",
            home.display()
        );
    };
    validate_recorded_project_work_root(project_root, &config.work_root)
}

fn resolve_home_work_root(home: &Path) -> Result<PathBuf> {
    let Some(config) = load_instance_config(home)? else {
        bail!(
            "--home {} does not contain a recorded work root; use a project instance or configure the home with 'lionclaw instance adopt <name> {} --work-root PATH'",
            home.display(),
            home.display()
        );
    };
    if !config.work_root.is_absolute() {
        bail!(
            "recorded work root {} is not canonical; repair the instance with an explicit --work-root PATH",
            config.work_root.display()
        );
    }
    let metadata = fs::metadata(&config.work_root)
        .with_context(|| format!("failed to stat work root {}", config.work_root.display()))?;
    if !metadata.is_dir() {
        bail!(
            "recorded work root {} is not a directory",
            config.work_root.display()
        );
    }
    fs::canonicalize(&config.work_root)
        .with_context(|| format!("failed to resolve work root {}", config.work_root.display()))
}

fn inspect_project_instance_work_root(project_root: &Path, home: &Path) -> WorkRootInspection {
    match load_instance_config(home) {
        Ok(Some(config)) => {
            match validate_recorded_project_work_root(project_root, &config.work_root) {
                Ok(work_root) => WorkRootInspection {
                    work_root: Some(work_root),
                    finding: None,
                },
                Err(err) => WorkRootInspection {
                    work_root: None,
                    finding: Some(err.to_string()),
                },
            }
        }
        Ok(None) => WorkRootInspection {
            work_root: None,
            finding: Some(format!(
                "instance home {} does not record a work root",
                home.display()
            )),
        },
        Err(err) => WorkRootInspection {
            work_root: None,
            finding: Some(err.to_string()),
        },
    }
}

fn inspect_home_work_root(home: &Path) -> WorkRootInspection {
    match resolve_home_work_root(home) {
        Ok(work_root) => WorkRootInspection {
            work_root: Some(work_root),
            finding: None,
        },
        Err(err) => WorkRootInspection {
            work_root: None,
            finding: Some(err.to_string()),
        },
    }
}

fn load_project_config(project_root: &Path) -> Result<ProjectConfig> {
    validate_project_metadata_dir(project_root)?;
    let path = project_file(project_root);
    ensure_regular_file_not_symlink(&path, "project config")?;
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: ProjectFileConfig =
        toml::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))?;
    if !matches!(parsed.version, None | Some(1)) {
        bail!(
            "unsupported LionClaw project config version {:?} in {}; expected version = 1",
            parsed.version,
            path.display()
        );
    }
    let default_instance = parsed
        .default_instance
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Ok(ProjectConfig { default_instance })
}

fn load_instance_config(home: &Path) -> Result<Option<InstanceFileConfig>> {
    let path = instance_config_path(home);
    if !path.exists() {
        return Ok(None);
    }
    ensure_regular_file_not_symlink(&path, "instance config")?;
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: InstanceFileConfig =
        toml::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))?;
    if parsed.version != 1 {
        bail!(
            "unsupported LionClaw instance config version {} in {}; expected version = 1",
            parsed.version,
            path.display()
        );
    }
    Ok(Some(parsed))
}

fn save_instance_config(home: &Path, work_root: &Path) -> Result<()> {
    let config = InstanceFileConfig {
        version: 1,
        work_root: work_root.to_path_buf(),
    };
    write_toml_file(&instance_config_path(home), &config, "instance config")
}

fn write_toml_file<T: Serialize>(path: &Path, value: &T, label: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        ensure_directory_not_symlink(parent, &format!("{label} directory"))?;
    }
    ensure_file_write_target_not_symlink(path, label)?;
    let content =
        toml::to_string_pretty(value).with_context(|| format!("failed to encode {label}"))?;
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))
}

fn resolve_project_work_root(
    project_root: &Path,
    requested: &Path,
    create_work_root: bool,
) -> Result<PathBuf> {
    let requested = absolutize_from(project_root, requested);
    if requested.exists() {
        let canonical = fs::canonicalize(&requested)
            .with_context(|| format!("failed to resolve work root {}", requested.display()))?;
        return validate_recorded_project_work_root(project_root, &canonical);
    }
    if !create_work_root {
        bail!(
            "work root {} does not exist; create it first or rerun with --create-work-root",
            requested.display()
        );
    }
    validate_missing_work_root_candidate(project_root, &requested)?;
    fs::create_dir_all(&requested)
        .with_context(|| format!("failed to create work root {}", requested.display()))?;
    let canonical = fs::canonicalize(&requested)
        .with_context(|| format!("failed to resolve work root {}", requested.display()))?;
    validate_recorded_project_work_root(project_root, &canonical)
}

fn validate_recorded_project_work_root(project_root: &Path, work_root: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(work_root)
        .with_context(|| format!("failed to resolve work root {}", work_root.display()))?;
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("failed to stat work root {}", canonical.display()))?;
    if !metadata.is_dir() {
        bail!("work root {} is not a directory", canonical.display());
    }
    if !canonical.starts_with(project_root) {
        bail!(
            "work root {} is outside project root {}; choose a directory inside the project",
            canonical.display(),
            project_root.display()
        );
    }
    let metadata_root = project_dir(project_root);
    if canonical == metadata_root || canonical.starts_with(&metadata_root) {
        bail!(
            "work root {} is inside LionClaw metadata {}; choose a project content directory",
            canonical.display(),
            metadata_root.display()
        );
    }
    Ok(canonical)
}

fn validate_missing_work_root_candidate(project_root: &Path, candidate: &Path) -> Result<()> {
    ensure_path_has_no_parent_components(candidate)?;
    if !candidate.starts_with(project_root) {
        bail!(
            "work root {} is outside project root {}; choose a directory inside the project",
            candidate.display(),
            project_root.display()
        );
    }
    let metadata_root = project_dir(project_root);
    if candidate == metadata_root || candidate.starts_with(&metadata_root) {
        bail!(
            "work root {} is inside LionClaw metadata {}; choose a project content directory",
            candidate.display(),
            metadata_root.display()
        );
    }
    let ancestor = deepest_existing_ancestor(candidate)?;
    let canonical_ancestor = fs::canonicalize(&ancestor)
        .with_context(|| format!("failed to resolve {}", ancestor.display()))?;
    if !canonical_ancestor.starts_with(project_root) {
        bail!(
            "work root {} would escape project root through {}",
            candidate.display(),
            ancestor.display()
        );
    }
    Ok(())
}

fn ensure_path_has_no_parent_components(path: &Path) -> Result<()> {
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        bail!("work root {} must not contain '..'", path.display());
    }
    Ok(())
}

fn deepest_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut current = path;
    loop {
        if current.exists() {
            return Ok(current.to_path_buf());
        }
        current = current
            .parent()
            .ok_or_else(|| anyhow!("no existing ancestor for {}", path.display()))?;
    }
}

fn list_instance_names(project_root: &Path) -> Result<Vec<String>> {
    let instances = instances_dir(project_root);
    if !instances.exists() {
        return Ok(Vec::new());
    }
    ensure_directory_not_symlink(&instances, "instances directory")?;
    let mut names = Vec::new();
    for entry in fs::read_dir(&instances)
        .with_context(|| format!("failed to read {}", instances.display()))?
    {
        let entry = entry.with_context(|| format!("failed to iterate {}", instances.display()))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("instance home {} must not be a symlink", path.display());
        }
        if !metadata.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        validate_instance_name(name)?;
        names.push(name.to_string());
    }
    names.sort();
    Ok(names)
}

fn canonical_project_root(project_root: &Path) -> Result<PathBuf> {
    let project_root = canonical_existing_dir(project_root, "project root")?;
    load_project_config(&project_root)?;
    Ok(project_root)
}

fn canonical_existing_dir(path: &Path, label: &str) -> Result<PathBuf> {
    ensure_directory_not_symlink(path, label)?;
    fs::canonicalize(path).with_context(|| format!("failed to resolve {label} {}", path.display()))
}

fn validate_project_metadata_dir(project_root: &Path) -> Result<()> {
    ensure_directory_not_symlink(&project_dir(project_root), "project metadata directory")
}

fn ensure_instances_dir(project_root: &Path) -> Result<()> {
    validate_project_metadata_dir(project_root)?;
    let instances = instances_dir(project_root);
    match fs::symlink_metadata(&instances) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "instances directory {} must not be a symlink",
                    instances.display()
                );
            }
            if !metadata.is_dir() {
                bail!(
                    "instances directory {} is not a directory",
                    instances.display()
                );
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&instances)
                .with_context(|| format!("failed to create {}", instances.display()))?;
            ensure_directory_not_symlink(&instances, "instances directory")
        }
        Err(err) => Err(err)
            .with_context(|| format!("failed to stat instances directory {}", instances.display())),
    }
}

fn require_instances_dir(project_root: &Path) -> Result<()> {
    validate_project_metadata_dir(project_root)?;
    let instances = instances_dir(project_root);
    let metadata = fs::symlink_metadata(&instances)
        .with_context(|| format!("failed to stat instances directory {}", instances.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "instances directory {} must not be a symlink",
            instances.display()
        );
    }
    if !metadata.is_dir() {
        bail!(
            "instances directory {} is not a directory",
            instances.display()
        );
    }
    Ok(())
}

fn ensure_directory_not_symlink(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to stat {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("{label} {} is not a directory", path.display());
    }
    Ok(())
}

fn ensure_regular_file_not_symlink(path: &Path, label: &str) -> Result<()> {
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

fn ensure_file_write_target_not_symlink(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if metadata.is_dir() {
                bail!("{label} {} is not a file", path.display());
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {label} {}", path.display())),
    }
}

fn validate_instance_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    if name != trimmed {
        bail!("instance name '{name}' has surrounding whitespace");
    }
    if trimmed.is_empty() {
        bail!("instance name is required");
    }
    if matches!(trimmed, "." | "..") || trimmed.starts_with('.') {
        bail!("instance name '{trimmed}' is not path-safe");
    }
    if trimmed
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')))
    {
        bail!(
            "instance name '{trimmed}' may only contain ASCII letters, numbers, '.', '_' and '-'"
        );
    }
    Ok(())
}

fn containing_project_instance_home(cwd: &Path) -> Option<PathBuf> {
    for ancestor in cwd.ancestors() {
        let parent = ancestor.parent()?;
        if parent.file_name().and_then(|value| value.to_str()) != Some(INSTANCES_DIR) {
            continue;
        }
        let metadata_dir = parent.parent()?;
        if metadata_dir.file_name().and_then(|value| value.to_str()) == Some(PROJECT_DIR) {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn project_dir(project_root: &Path) -> PathBuf {
    project_root.join(PROJECT_DIR)
}

fn project_file(project_root: &Path) -> PathBuf {
    project_dir(project_root).join(PROJECT_FILE)
}

fn instances_dir(project_root: &Path) -> PathBuf {
    project_dir(project_root).join(INSTANCES_DIR)
}

fn instance_home(project_root: &Path, name: &str) -> PathBuf {
    instances_dir(project_root).join(name)
}

pub fn project_dir_path(project_root: &Path) -> PathBuf {
    project_dir(project_root)
}

pub fn project_file_path(project_root: &Path) -> PathBuf {
    project_file(project_root)
}

pub fn instances_dir_path(project_root: &Path) -> PathBuf {
    instances_dir(project_root)
}

pub fn instance_home_path(project_root: &Path, name: &str) -> PathBuf {
    instance_home(project_root, name)
}

fn instance_config_path(home: &Path) -> PathBuf {
    home.join("config").join(INSTANCE_CONFIG_FILE)
}

fn absolutize_from(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn config_version() -> u32 {
    1
}

fn ensure_instance_base_dirs(home: &LionClawHome) -> Result<()> {
    for path in [
        home.root(),
        home.db_dir(),
        home.config_dir(),
        home.channel_env_dir(),
        home.skills_dir(),
        home.runtime_dir(),
        home.logs_dir(),
        home.units_dir(),
        home.units_env_dir(),
        home.workspace_dir(crate::home::DEFAULT_WORKSPACE),
    ] {
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
    }
    #[cfg(unix)]
    {
        use std::{fs::Permissions, os::unix::fs::PermissionsExt};

        fs::set_permissions(home.config_dir(), Permissions::from_mode(0o700))
            .with_context(|| format!("failed to chmod {}", home.config_dir().display()))?;
    }
    if !home.home_id_path().exists() {
        fs::write(home.home_id_path(), format!("{}\n", uuid::Uuid::new_v4()))
            .with_context(|| format!("failed to write {}", home.home_id_path().display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        adopt_project_instance, create_project_instance, init_project, list_project_instances,
        resolve_project_setup_root_from_cwd, resolve_target_from_cwd, TargetSelection,
        WorkRootRequirement, DEFAULT_INSTANCE, PROJECT_DIR,
    };

    #[test]
    fn project_init_creates_default_instance_with_project_work_root() {
        let temp_dir = tempdir().expect("temp dir");
        let result = init_project(temp_dir.path()).expect("init project");

        assert_eq!(
            result.project_root,
            temp_dir.path().canonicalize().expect("canonical")
        );
        assert_eq!(result.instance.name, DEFAULT_INSTANCE);
        assert!(temp_dir.path().join(".lionclaw/project.toml").exists());
        assert!(temp_dir
            .path()
            .join(".lionclaw/instances/main/config/instance.toml")
            .exists());
        assert_eq!(result.instance.work_root, result.project_root);
    }

    #[test]
    fn resolver_discovers_project_from_root_and_child_only() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let child = temp_dir.path().join("child");
        let deep = child.join("deep");
        fs::create_dir_all(&deep).expect("deep");

        let from_root = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Required,
            temp_dir.path(),
        )
        .expect("root target");
        let from_child = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Required,
            &child,
        )
        .expect("child target");
        let err = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Required,
            &deep,
        )
        .expect_err("deep discovery should fail");

        assert_eq!(from_root.project_root, from_child.project_root);
        assert_eq!(from_child.work_root, from_root.work_root);
        assert!(err.to_string().contains("no LionClaw project found"));
    }

    #[test]
    fn explicit_project_and_instance_resolve_selected_home_and_work_root() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        fs::create_dir_all(temp_dir.path().join("docs")).expect("docs");
        create_project_instance(
            temp_dir.path(),
            "docs",
            Some(std::path::Path::new("./docs")),
            false,
        )
        .expect("create docs instance");

        let target = resolve_target_from_cwd(
            &TargetSelection {
                home: None,
                project: Some(temp_dir.path().to_path_buf()),
                instance: Some("docs".to_string()),
            },
            WorkRootRequirement::Required,
            temp_dir.path(),
        )
        .expect("resolve target");

        assert_eq!(target.instance_name.as_deref(), Some("docs"));
        assert_eq!(
            target.work_root.as_deref(),
            Some(
                temp_dir
                    .path()
                    .join("docs")
                    .canonicalize()
                    .expect("canonical")
                    .as_path()
            )
        );
    }

    #[test]
    fn home_mode_requires_recorded_work_root_when_needed() {
        let temp_dir = tempdir().expect("temp dir");
        let home = temp_dir.path().join("home");
        fs::create_dir_all(&home).expect("home");

        let err = resolve_target_from_cwd(
            &TargetSelection {
                home: Some(home),
                project: None,
                instance: None,
            },
            WorkRootRequirement::Required,
            temp_dir.path(),
        )
        .expect_err("missing work root should fail");

        assert!(err
            .to_string()
            .contains("does not contain a recorded work root"));
    }

    #[test]
    fn missing_default_instance_fails_without_magic_fallback() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        fs::remove_dir_all(temp_dir.path().join(".lionclaw/instances/main"))
            .expect("remove default");
        fs::create_dir_all(temp_dir.path().join(".lionclaw/instances/reviewer")).expect("reviewer");

        let err = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Optional,
            temp_dir.path(),
        )
        .expect_err("missing default should fail");

        assert!(err
            .to_string()
            .contains("default instance 'main' is configured but missing"));
    }

    #[test]
    fn single_instance_fallback_requires_no_default() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        fs::write(
            temp_dir.path().join(".lionclaw/project.toml"),
            "version = 1\n",
        )
        .expect("clear default");
        fs::rename(
            temp_dir.path().join(".lionclaw/instances/main"),
            temp_dir.path().join(".lionclaw/instances/reviewer"),
        )
        .expect("rename instance");

        let target = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Optional,
            temp_dir.path(),
        )
        .expect("single instance fallback");

        assert_eq!(target.instance_name.as_deref(), Some("reviewer"));
    }

    #[test]
    fn multiple_instances_without_default_are_ambiguous() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        fs::write(
            temp_dir.path().join(".lionclaw/project.toml"),
            "version = 1\n",
        )
        .expect("clear default");
        create_project_instance(temp_dir.path(), "reviewer", None, false).expect("create reviewer");

        let err = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Optional,
            temp_dir.path(),
        )
        .expect_err("ambiguous");

        assert!(err.to_string().contains("multiple instances"));
    }

    #[test]
    fn work_root_creation_is_explicit_and_project_local() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let err = create_project_instance(
            temp_dir.path(),
            "docs",
            Some(std::path::Path::new("docs")),
            false,
        )
        .expect_err("missing work root should fail");
        assert!(err.to_string().contains("does not exist"));

        let instance = create_project_instance(
            temp_dir.path(),
            "docs",
            Some(std::path::Path::new("docs")),
            true,
        )
        .expect("create work root");
        assert_eq!(
            instance.work_root,
            temp_dir
                .path()
                .join("docs")
                .canonicalize()
                .expect("canonical")
        );
    }

    #[test]
    fn invalid_work_roots_are_rejected() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let outside = tempdir().expect("outside");
        let file = temp_dir.path().join("file.txt");
        fs::write(&file, "file").expect("file");

        let outside_err =
            create_project_instance(temp_dir.path(), "outside", Some(outside.path()), false)
                .expect_err("outside root");
        let metadata_err = create_project_instance(
            temp_dir.path(),
            "metadata",
            Some(std::path::Path::new(PROJECT_DIR)),
            false,
        )
        .expect_err("metadata root");
        let file_err =
            create_project_instance(temp_dir.path(), "file", Some(file.as_path()), false)
                .expect_err("file root");

        assert!(outside_err.to_string().contains("outside project root"));
        assert!(metadata_err
            .to_string()
            .contains("inside LionClaw metadata"));
        assert!(file_err.to_string().contains("not a directory"));
    }

    #[cfg(unix)]
    #[test]
    fn adopt_rejects_symlinked_instance_config_before_write() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let home = temp_dir.path().join(".lionclaw/instances/adopted");
        let config_dir = home.join("config");
        fs::create_dir_all(&config_dir).expect("config dir");
        let outside_target = temp_dir.path().join("outside-instance.toml");
        symlink(&outside_target, config_dir.join("instance.toml")).expect("instance symlink");

        let err = adopt_project_instance(
            temp_dir.path(),
            "adopted",
            home.as_path(),
            Some(std::path::Path::new(".")),
            false,
        )
        .expect_err("symlinked instance config should fail");

        assert!(err.to_string().contains("instance config"));
        assert!(err.to_string().contains("must not be a symlink"));
        assert!(
            !outside_target.exists(),
            "instance adopt must not follow config/instance.toml symlinks"
        );
    }

    #[cfg(unix)]
    #[test]
    fn project_init_rejects_dangling_project_config_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let metadata_dir = temp_dir.path().join(".lionclaw");
        fs::create_dir(&metadata_dir).expect("metadata dir");
        let outside_target = temp_dir.path().join("outside-project.toml");
        symlink(&outside_target, metadata_dir.join("project.toml")).expect("project symlink");

        let err =
            init_project(temp_dir.path()).expect_err("dangling project config symlink should fail");

        assert!(err.to_string().contains("project config"));
        assert!(err.to_string().contains("must not be a symlink"));
        assert!(
            !outside_target.exists(),
            "project init must not follow dangling project.toml symlinks"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_work_root_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let outside = tempdir().expect("outside");
        symlink(outside.path(), temp_dir.path().join("escape")).expect("symlink");

        let err = create_project_instance(
            temp_dir.path(),
            "escape",
            Some(std::path::Path::new("escape")),
            false,
        )
        .expect_err("symlink escape");

        assert!(err.to_string().contains("outside project root"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_instances_directory_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let outside = tempdir().expect("outside");
        let instances = temp_dir.path().join(".lionclaw/instances");
        fs::remove_dir_all(&instances).expect("remove instances");
        symlink(outside.path(), &instances).expect("symlink instances");

        let create_err = create_project_instance(temp_dir.path(), "reviewer", None, false)
            .expect_err("symlinked instances dir should reject create");
        let resolve_err = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Required,
            temp_dir.path(),
        )
        .expect_err("symlinked instances dir should reject resolution");

        assert!(create_err.to_string().contains("must not be a symlink"));
        assert!(resolve_err.to_string().contains("must not be a symlink"));
        assert!(
            !outside.path().join("reviewer").exists(),
            "instance create must not follow .lionclaw/instances symlinks"
        );
    }

    #[test]
    fn instance_list_marks_default_and_shared_work_roots() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        create_project_instance(temp_dir.path(), "reviewer", None, false).expect("create reviewer");

        let entries = list_project_instances(temp_dir.path()).expect("list");

        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .any(|entry| entry.name == "main" && entry.is_default));
        assert!(entries
            .iter()
            .all(|entry| entry.shared_work_root_count == 2));
    }

    #[test]
    fn run_resolution_rejects_cwd_inside_instance_home() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let cwd = temp_dir.path().join(".lionclaw/instances/main");

        let err = resolve_target_from_cwd(
            &TargetSelection::default(),
            WorkRootRequirement::Required,
            &cwd,
        )
        .expect_err("instance home cwd should fail");

        assert!(err.to_string().contains("LionClaw instance home"));
    }

    #[test]
    fn project_setup_uses_explicit_project_or_cwd() {
        let temp_dir = tempdir().expect("temp dir");
        let child = temp_dir.path().join("child");
        fs::create_dir_all(&child).expect("child");

        let explicit = resolve_project_setup_root_from_cwd(
            &TargetSelection {
                project: Some(temp_dir.path().to_path_buf()),
                ..TargetSelection::default()
            },
            &child,
        )
        .expect("explicit project");
        let implicit = resolve_project_setup_root_from_cwd(&TargetSelection::default(), &child)
            .expect("implicit project");

        assert_eq!(explicit, temp_dir.path().canonicalize().expect("canonical"));
        assert_eq!(implicit, child.canonicalize().expect("canonical child"));
    }

    #[test]
    fn adopt_requires_or_records_work_root() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        let source = tempdir().expect("source home");

        let err = adopt_project_instance(temp_dir.path(), "adopted", source.path(), None, false)
            .expect_err("missing work root");
        assert!(err.to_string().contains("does not record a work root"));

        let source = tempdir().expect("source home");
        let adopted = adopt_project_instance(
            temp_dir.path(),
            "adopted",
            source.path(),
            Some(std::path::Path::new(".")),
            false,
        )
        .expect("adopt with work root");
        assert_eq!(
            adopted.work_root,
            temp_dir.path().canonicalize().expect("canonical")
        );
    }
}
