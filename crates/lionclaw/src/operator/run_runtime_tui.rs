use std::{io::Write, path::Path};

use anyhow::{bail, Result};

use crate::{
    home::LionClawHome,
    kernel::AttachedRuntimeLaunchInput,
    operator::{
        config::OperatorConfig,
        reconcile::open_runtime_kernel_for_work_root,
        run::{
            kernel_to_anyhow, local_peer_id_for_project, resolve_repl_session,
            resolve_run_runtime_id,
        },
        runtime::validate_runtime_launch_prerequisites_for_work_root,
    },
    project_inventory::ProjectInstanceRuntimeContext,
};

pub(crate) struct RunRuntimeTuiInvocation<'a> {
    pub(crate) home: &'a LionClawHome,
    pub(crate) work_root: &'a Path,
    pub(crate) instance_name: Option<&'a str>,
    pub(crate) project_instance_runtime: Option<ProjectInstanceRuntimeContext>,
    pub(crate) requested_runtime: Option<String>,
    pub(crate) continue_last_session: bool,
}

pub(crate) async fn run_runtime_tui(invocation: RunRuntimeTuiInvocation<'_>) -> Result<()> {
    let RunRuntimeTuiInvocation {
        home,
        work_root,
        instance_name,
        project_instance_runtime,
        requested_runtime,
        continue_last_session,
    } = invocation;
    let config = OperatorConfig::load(home).await?;
    let runtime_id = resolve_run_runtime_id(
        &config,
        requested_runtime.as_deref(),
        instance_name.unwrap_or("selected home"),
    )?;
    let project_root = project_instance_runtime
        .as_ref()
        .map(|context| context.project_root.as_path());
    validate_runtime_launch_prerequisites_for_work_root(
        home,
        &config,
        &runtime_id,
        project_root,
        Some(work_root),
    )
    .await?;
    print_runtime_tui_prepare_message(&runtime_id)?;
    let kernel = open_runtime_kernel_for_work_root(
        home,
        &config,
        Some(runtime_id.clone()),
        work_root,
        project_instance_runtime,
        None,
    )
    .await?;
    let peer_id = local_peer_id_for_project(work_root);
    let session = resolve_repl_session(&kernel, &peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?;
    let output = kernel
        .execute_attached_runtime_launch(AttachedRuntimeLaunchInput {
            session_id: session.session_id,
            runtime_id: runtime_id.clone(),
        })
        .await
        .map_err(kernel_to_anyhow)?;
    if output.success() {
        Ok(())
    } else {
        bail!("runtime TUI exited with {}", output.status_description())
    }
}

fn print_runtime_tui_prepare_message(runtime_id: &str) -> Result<()> {
    let mut stderr = std::io::stderr().lock();
    writeln!(stderr, "LionClaw: preparing {runtime_id} runtime UI...")?;
    stderr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{run_runtime_tui, RunRuntimeTuiInvocation};
    use crate::{
        home::{LionClawHome, DEFAULT_WORKSPACE},
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
        operator::config::{OperatorConfig, RuntimeProfileConfig},
        workspace::GENERATED_AGENTS_FILE,
    };

    #[tokio::test]
    async fn runtime_tui_validates_profile_before_opening_kernel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let work_root = temp_dir.path().join("workspace");
        tokio::fs::create_dir(&work_root).await.expect("workspace");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::new(
                "codex",
                String::new(),
                ConfinementConfig::Oci(OciConfinementConfig {
                    engine: "podman".to_string(),
                    image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            ),
        );
        config.save(&home).await.expect("save config");

        let err = run_runtime_tui(RunRuntimeTuiInvocation {
            home: &home,
            work_root: &work_root,
            instance_name: Some("main"),
            project_instance_runtime: None,
            requested_runtime: None,
            continue_last_session: false,
        })
        .await
        .expect_err("invalid runtime profile should fail before launch");

        assert!(err
            .to_string()
            .contains("configured runtime profile 'codex' is invalid"));
        assert!(
            !home
                .runtime_project_dir("codex", DEFAULT_WORKSPACE, &work_root)
                .join(GENERATED_AGENTS_FILE)
                .exists(),
            "invalid runtime TUI launch must not prepare runtime state"
        );
    }
}
