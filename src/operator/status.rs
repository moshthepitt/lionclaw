use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};

use crate::home::LionClawHome;

use super::{
    config::{ManagedChannelConfig, OperatorConfig},
    managed_units::{SystemdUserUnitManager, UnitManager},
    runtime_integration::{runtime_auth_guidance, runtime_profile_facts},
    runtime_mounts::validate_runtime_profile_mounts,
    target::{
        inspect_target_work_root, list_project_instance_statuses, TargetContext, TargetSelection,
    },
};

pub async fn render_target_status(target: &TargetContext) -> Result<String> {
    render_target_status_with_manager(target, &SystemdUserUnitManager).await
}

pub async fn render_target_status_with_manager<M: UnitManager>(
    target: &TargetContext,
    manager: &M,
) -> Result<String> {
    let work_root = inspect_target_work_root(target);
    let config = OperatorConfig::load(&target.instance_home).await?;
    let managed_units = load_managed_unit_snapshot(&target.instance_home, &config, manager).await?;
    let name = target
        .instance_name
        .clone()
        .unwrap_or_else(|| "direct-home".to_string());
    let shared_count = match target.project_root.as_deref() {
        Some(project_root) => list_project_instance_statuses(project_root)?
            .into_iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.shared_work_root_count)
            .unwrap_or(0),
        None => 0,
    };

    Ok(render_instance_status(InstanceRenderInput {
        project_root: target.project_root.clone(),
        name,
        is_default: false,
        home: target.instance_home.root(),
        work_root: work_root.work_root,
        work_root_finding: work_root.finding,
        shared_work_root_count: shared_count,
        config,
        managed_units,
        include_project_header: true,
        selected: true,
    }))
}

pub async fn render_project_status_all(project_root: &Path) -> Result<String> {
    render_project_status_all_with_manager(project_root, &SystemdUserUnitManager).await
}

pub async fn render_project_status_all_with_manager<M: UnitManager>(
    project_root: &Path,
    manager: &M,
) -> Result<String> {
    let entries = list_project_instance_statuses(project_root)?;
    let project_root = project_root.canonicalize()?;
    let mut output = format!("project: {}\n", project_root.display());
    if entries.is_empty() {
        output.push_str("\nreadiness: run blocked\n");
        output.push_str("  project has no instances\n");
        return Ok(output);
    }

    for entry in entries {
        let config = OperatorConfig::load(&LionClawHome::new(entry.home.clone())).await?;
        let managed_units =
            load_managed_unit_snapshot(&LionClawHome::new(entry.home.clone()), &config, manager)
                .await?;
        output.push('\n');
        output.push_str(&render_instance_status(InstanceRenderInput {
            project_root: Some(project_root.clone()),
            name: entry.name,
            is_default: entry.is_default,
            home: entry.home,
            work_root: entry.work_root,
            work_root_finding: entry.work_root_finding,
            shared_work_root_count: entry.shared_work_root_count,
            config,
            managed_units,
            include_project_header: false,
            selected: false,
        }));
    }

    Ok(output)
}

pub fn validate_status_all_target(selection: &TargetSelection) -> Result<()> {
    if selection.home.is_some() {
        bail!("status --all requires a project context; run from the project root or pass --project PATH");
    }
    if selection.instance.is_some() {
        bail!("status --all is project-wide and cannot be combined with --instance");
    }
    Ok(())
}

struct InstanceRenderInput {
    project_root: Option<PathBuf>,
    name: String,
    is_default: bool,
    home: PathBuf,
    work_root: Option<PathBuf>,
    work_root_finding: Option<String>,
    shared_work_root_count: usize,
    config: OperatorConfig,
    managed_units: ManagedUnitSnapshot,
    include_project_header: bool,
    selected: bool,
}

#[derive(Debug, Clone)]
struct ManagedUnitSnapshot {
    daemon: String,
    workers: Vec<ManagedWorkerStatus>,
}

#[derive(Debug, Clone)]
struct ManagedWorkerStatus {
    channel_id: String,
    status: String,
}

fn render_instance_status(input: InstanceRenderInput) -> String {
    let mut output = String::new();
    if input.include_project_header {
        output.push_str(&format!(
            "project: {}\n",
            input
                .project_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        output.push_str(&format!("selected instance: {}\n", input.name));
    } else {
        let marker = if input.is_default { "*" } else { " " };
        output.push_str(&format!("{marker} {}\n", input.name));
    }

    output.push_str(&format!("  home: {}\n", input.home.display()));
    output.push_str(&format!(
        "  work root: {}\n",
        format_work_root(input.work_root.as_deref(), input.shared_work_root_count)
    ));
    output.push_str(&format!(
        "  default runtime: {}\n",
        input
            .config
            .defaults
            .runtime
            .as_deref()
            .unwrap_or("not configured")
    ));
    push_runtime_status(&mut output, &input.config);
    push_channel_status(&mut output, &input.config.channels);
    push_managed_unit_status(&mut output, &input.managed_units);
    push_readiness(&mut output, &input);

    if input.selected {
        output.push_str("  scope: selected target\n");
    }

    output
}

async fn load_managed_unit_snapshot<M: UnitManager>(
    home: &LionClawHome,
    config: &OperatorConfig,
    manager: &M,
) -> Result<ManagedUnitSnapshot> {
    let owned_units = manager.owned_units(home)?;
    let daemon = match owned_units.daemon() {
        Some(unit) => manager.unit_status(unit).await?,
        None => "not-installed".to_string(),
    };
    let mut workers = Vec::new();
    for channel in config
        .channels
        .iter()
        .filter(|channel| channel.launch_mode == super::config::ChannelLaunchMode::Background)
    {
        let status = match owned_units.channel(&channel.id) {
            Some(unit) => manager.unit_status(unit).await?,
            None => "not-installed".to_string(),
        };
        workers.push(ManagedWorkerStatus {
            channel_id: channel.id.clone(),
            status,
        });
    }
    Ok(ManagedUnitSnapshot { daemon, workers })
}

fn push_runtime_status(output: &mut String, config: &OperatorConfig) {
    let Some(runtime_id) = config.defaults.runtime.as_deref() else {
        output.push_str("  runtime: not configured\n");
        return;
    };
    let Some(profile) = config.runtimes.get(runtime_id) else {
        output.push_str(&format!("  runtime: {runtime_id} missing profile\n"));
        return;
    };
    let facts = runtime_profile_facts(profile);
    output.push_str(&format!(
        "  runtime: {runtime_id} kind={} bin={} confinement={}\n",
        facts.kind, facts.executable, facts.confinement
    ));
    if let Some(engine) = facts.engine {
        output.push_str(&format!("    engine: {engine}\n"));
    }
    if let Some(image) = facts.image {
        output.push_str(&format!("    image: {image}\n"));
    }
    if let Some(guidance) = runtime_auth_guidance(profile) {
        output.push_str(&format!("    auth: {guidance}\n"));
    }
}

fn push_channel_status(output: &mut String, channels: &[ManagedChannelConfig]) {
    if channels.is_empty() {
        output.push_str("  channels: none\n");
        return;
    }
    output.push_str("  channels:\n");
    for channel in channels {
        output.push_str(&format!(
            "    {} skill={} launch={}\n",
            channel.id,
            channel.skill,
            channel.launch_mode.as_str()
        ));
    }
}

fn push_managed_unit_status(output: &mut String, units: &ManagedUnitSnapshot) {
    output.push_str(&format!("  managed daemon: {}\n", units.daemon));
    if units.workers.is_empty() {
        output.push_str("  managed workers: none\n");
        return;
    }
    output.push_str("  managed workers:\n");
    for worker in &units.workers {
        output.push_str(&format!("    {}: {}\n", worker.channel_id, worker.status));
    }
}

fn push_readiness(output: &mut String, input: &InstanceRenderInput) {
    let findings = readiness_findings(input);
    if findings.len() == 1 && findings.first().is_some_and(|finding| finding == "ready") {
        output.push_str("  readiness: ready\n");
        return;
    }
    output.push_str("  readiness: run blocked\n");
    for finding in findings {
        output.push_str(&format!("    {finding}\n"));
    }
}

fn readiness_findings(input: &InstanceRenderInput) -> Vec<String> {
    let mut findings = Vec::new();
    if let Some(finding) = input.work_root_finding.as_deref() {
        findings.push(format!("work root: {finding}"));
    }

    match input.config.defaults.runtime.as_deref() {
        Some(runtime_id) if input.config.runtimes.contains_key(runtime_id) => {}
        Some(runtime_id) => findings.push(format!(
            "default runtime profile \"{runtime_id}\" is not configured"
        )),
        None => findings.push("no default runtime configured".to_string()),
    }

    let home = LionClawHome::new(input.home.clone());
    for (runtime_id, profile) in &input.config.runtimes {
        if let Err(err) = validate_runtime_profile_mounts(
            &home,
            input.project_root.as_deref(),
            input.work_root.as_deref(),
            profile,
        ) {
            findings.push(format!(
                "runtime profile \"{runtime_id}\" invalid mounts: {err}"
            ));
        } else if let Err(err) = profile.validate() {
            findings.push(format!("runtime profile \"{runtime_id}\" invalid: {err}"));
        }
    }

    if findings.is_empty() {
        findings.push("ready".to_string());
    }
    findings
}

fn format_work_root(work_root: Option<&Path>, shared_count: usize) -> String {
    let Some(work_root) = work_root else {
        return "not configured".to_string();
    };
    if shared_count > 1 {
        format!(
            "{} (shared by {shared_count} instances)",
            work_root.display()
        )
    } else {
        work_root.display().to_string()
    }
}

pub fn missing_target_for_status() -> anyhow::Error {
    anyhow!("status requires a resolved LionClaw target")
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use super::*;
    use crate::kernel::runtime::{ConfinementConfig, MountAccess, MountSpec};
    use crate::operator::{
        managed_units::{daemon_unit_name, ensure_unit_identity, FakeUnitManager},
        runtime_integration::configure_runtime_profile_with_engine_resolver,
        target::{create_project_instance, init_project},
    };

    fn fake_podman(root: &std::path::Path) -> std::path::PathBuf {
        let path = root.join("podman");
        fs::write(&path, "#!/usr/bin/env bash\nexit 0\n").expect("write fake podman");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod fake podman");
        path
    }

    fn configure_test_codex(config: &mut OperatorConfig, engine: &std::path::Path) {
        configure_runtime_profile_with_engine_resolver(config, "codex", |_| {
            Ok(engine.to_string_lossy().to_string())
        })
        .expect("configure codex");
    }

    #[tokio::test]
    async fn status_reports_default_target_runtime_and_work_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home.clone());
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        configure_test_codex(&mut config, &fake_podman(temp_dir.path()));
        config.save(&home).await.expect("save config");
        let target = TargetContext {
            project_root: Some(project.project_root.clone()),
            instance_name: Some("main".to_string()),
            instance_home: home,
            work_root: None,
        };

        let output = render_target_status(&target).await.expect("status");

        assert!(output.contains(&format!("project: {}", project.project_root.display())));
        assert!(output.contains("selected instance: main"));
        assert!(output.contains("default runtime: codex"));
        assert!(output.contains("runtime: codex kind=codex bin=codex"));
        assert!(output.contains("image: lionclaw-runtime:v1"));
        assert!(output.contains("readiness: ready"));
    }

    #[tokio::test]
    async fn status_reports_invalid_configured_runtime_mounts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home.clone());
        let private = home.root().join("config/private");
        fs::create_dir_all(&private).expect("private mount source");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        configure_test_codex(&mut config, &fake_podman(temp_dir.path()));
        let runtime = config.runtimes.get_mut("codex").expect("runtime");
        let ConfinementConfig::Oci(oci) = runtime.confinement_mut();
        oci.additional_mounts.push(MountSpec {
            source: private,
            target: "/mnt/private".to_string(),
            access: MountAccess::ReadOnly,
        });
        config.save(&home).await.expect("save config");
        let target = TargetContext {
            project_root: Some(project.project_root.clone()),
            instance_name: Some("main".to_string()),
            instance_home: home,
            work_root: Some(project.instance.work_root.clone()),
        };

        let output = render_target_status(&target).await.expect("status");

        assert!(output.contains("readiness: run blocked"));
        assert!(output.contains("invalid mounts"));
        assert!(output.contains("selected instance home"));
    }

    #[tokio::test]
    async fn status_reports_invalid_non_default_runtime_mounts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home.clone());
        let missing = temp_dir.path().join("missing-docs");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        configure_test_codex(&mut config, &fake_podman(temp_dir.path()));
        let reviewer = config.runtimes.get("codex").expect("codex").clone();
        config.upsert_runtime("reviewer".to_string(), reviewer);
        let runtime = config.runtimes.get_mut("reviewer").expect("runtime");
        let ConfinementConfig::Oci(oci) = runtime.confinement_mut();
        oci.additional_mounts.push(MountSpec {
            source: missing,
            target: "/mnt/docs".to_string(),
            access: MountAccess::ReadOnly,
        });
        config.save(&home).await.expect("save config");
        let target = TargetContext {
            project_root: Some(project.project_root.clone()),
            instance_name: Some("main".to_string()),
            instance_home: home,
            work_root: Some(project.instance.work_root.clone()),
        };

        let output = render_target_status(&target).await.expect("status");

        assert!(output.contains("readiness: run blocked"));
        assert!(output.contains("runtime profile \"reviewer\" invalid mounts"));
        assert!(output.contains("runtime mount target '/mnt/docs'"));
    }

    #[tokio::test]
    async fn status_reports_named_instance_only() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let reviewer = create_project_instance(&project.project_root, "reviewer", None, false)
            .expect("reviewer");
        let home = LionClawHome::new(reviewer.home.clone());
        let target = TargetContext {
            project_root: Some(project.project_root.clone()),
            instance_name: Some("reviewer".to_string()),
            instance_home: home,
            work_root: None,
        };

        let output = render_target_status(&target).await.expect("status");

        assert!(output.contains("selected instance: reviewer"));
        assert!(!output.contains("selected instance: main"));
        assert!(output.contains("default runtime: not configured"));
        assert!(output.contains("no default runtime configured"));
    }

    #[tokio::test]
    async fn status_does_not_report_unowned_derived_unit_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home.clone());
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let manager = FakeUnitManager::default();
        manager
            .set_unit_status(daemon_unit_name(&identity), "loaded/active/running")
            .expect("set daemon status");
        let target = TargetContext {
            project_root: Some(project.project_root.clone()),
            instance_name: Some("main".to_string()),
            instance_home: home,
            work_root: None,
        };

        let output = render_target_status_with_manager(&target, &manager)
            .await
            .expect("status");

        assert!(output.contains("managed daemon: not-installed"));
        assert!(!output.contains("managed daemon: loaded/active/running"));
    }

    #[tokio::test]
    async fn status_all_reports_project_instances_and_shared_work_roots() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        create_project_instance(&project.project_root, "reviewer", None, false).expect("reviewer");
        let home = LionClawHome::new(project.instance.home.clone());
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        configure_test_codex(&mut config, &fake_podman(temp_dir.path()));
        config.save(&home).await.expect("save config");

        let output = render_project_status_all(&project.project_root)
            .await
            .expect("status all");

        assert!(output.contains("* main"));
        assert!(output.contains("  reviewer"));
        assert!(output.contains("shared by 2 instances"));
        assert!(output.contains("default runtime: codex"));
        assert!(output.contains("default runtime: not configured"));
    }

    #[test]
    fn status_all_rejects_non_project_wide_targeting() {
        let selection = TargetSelection {
            home: Some(PathBuf::from("/tmp/home")),
            project: None,
            instance: None,
        };
        let err = validate_status_all_target(&selection).expect_err("home mode should fail");
        assert!(err.to_string().contains("requires a project context"));

        let selection = TargetSelection {
            home: None,
            project: None,
            instance: Some("reviewer".to_string()),
        };
        let err = validate_status_all_target(&selection).expect_err("instance should fail");
        assert!(err
            .to_string()
            .contains("cannot be combined with --instance"));
    }
}
