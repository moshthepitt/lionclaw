use std::{io::Write, path::Path};

use anyhow::{bail, Context, Result};

use crate::{
    home::LionClawHome,
    kernel::{runtime::execute_attached, AttachedRuntimeLaunchInput},
    operator::{
        config::OperatorConfig,
        reconcile::{open_runtime_kernel_for_work_root, render_runtime_cache_for_work_root},
        run::{
            kernel_to_anyhow, local_peer_id_for_project, resolve_repl_session,
            resolve_run_runtime_id,
        },
    },
    project_inventory::ProjectInstanceRuntimeContext,
    runtime_timeouts::RuntimeTurnTimeouts,
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
    print_runtime_tui_prepare_message(&runtime_id)?;
    render_runtime_cache_for_work_root(home, &config, &runtime_id, work_root).await?;

    let kernel = open_runtime_kernel_for_work_root(
        home,
        &config,
        Some(runtime_id.clone()),
        work_root,
        project_instance_runtime,
        Some(RuntimeTurnTimeouts::interactive()),
    )
    .await?;
    let peer_id = local_peer_id_for_project(work_root);
    let session = resolve_repl_session(&kernel, &peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?;
    let request = kernel
        .prepare_attached_runtime_launch(AttachedRuntimeLaunchInput {
            session_id: session.session_id,
            runtime_id: runtime_id.clone(),
            timeout_ms: None,
        })
        .await
        .map_err(kernel_to_anyhow)?;
    let plan = request.plan.clone();
    let output = match execute_attached(request).await {
        Ok(output) => output,
        Err(err) => {
            if let Err(finish_err) = kernel
                .finish_attached_runtime_launch(session.session_id, &runtime_id, &plan, None, None)
                .await
            {
                return Err(kernel_to_anyhow(finish_err)).context(format!(
                    "runtime TUI launch failed before exit handling: {err}"
                ));
            }
            return Err(err);
        }
    };
    kernel
        .finish_attached_runtime_launch(
            session.session_id,
            &runtime_id,
            &plan,
            output.exit_code,
            output.exit_signal,
        )
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
