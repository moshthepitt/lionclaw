//! The confined role runner: turns a moat-vetted plan into one full
//! autonomous agent run inside a container, and captures its handoff.

mod executor;
mod handoff;
mod native_home_auth;
mod prepared_input;
mod role_runner;

pub use executor::MissionProgramExecutor;
pub(crate) use prepared_input::{prepare_inputs, PreparedInputs};
pub use role_runner::OciRoleRunner;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use lionclaw_confinement::{
    MountAccess, MountSpec, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET,
};

use crate::config::RuntimeSkillsDir;
use crate::mission_type::SkillPackage;
use crate::model::{EffectId, TaskId};
use crate::ports::ExecutionControl;

pub(crate) async fn await_controlled<T, F, M>(
    future: F,
    mut control: tokio::sync::watch::Receiver<ExecutionControl>,
    map_control: M,
) -> std::result::Result<T, lionclaw_runtime_api::TypedFailure>
where
    F: std::future::Future<Output = std::result::Result<T, lionclaw_runtime_api::TypedFailure>>,
    M: Fn(&ExecutionControl) -> Option<lionclaw_runtime_api::TypedFailure>,
{
    tokio::pin!(future);
    if let Some(failure) = map_control(&control.borrow().clone()) {
        return Err(failure);
    }
    let mut control_open = true;
    loop {
        tokio::select! {
            biased;
            changed = control.changed(), if control_open => {
                if changed.is_err() {
                    control_open = false;
                } else if let Some(failure) = map_control(&control.borrow().clone()) {
                    return Err(failure);
                }
            },
            result = &mut future => return result,
        }
    }
}

/// Container mount targets the mission owns.
pub const HANDOFF_MOUNT_TARGET: &str = "/mission/handoff";
pub const SCRATCH_MOUNT_TARGET: &str = "/scratch";

/// Effect-scoped resources. Cleanup may remove this entire tree after any
/// outcome; no recoverable task work lives here.
pub struct EffectDirs {
    pub root: PathBuf,
    pub handoff: PathBuf,
    pub read_scratch: PathBuf,
    pub runtime: PathBuf,
    pub runtime_home: PathBuf,
}

impl EffectDirs {
    pub fn prepare(
        state_dir: &Path,
        mission_id: &str,
        effect_id: &EffectId,
    ) -> std::io::Result<Self> {
        let root = state_dir
            .join("missions")
            .join(mission_id)
            .join("effects")
            .join(effect_id.as_str());
        let dirs = Self {
            handoff: root.join("handoff"),
            read_scratch: root.join("scratch"),
            runtime: root.join("runtime"),
            runtime_home: root.join("runtime-home"),
            root,
        };
        for dir in [
            &dirs.handoff,
            &dirs.read_scratch,
            &dirs.runtime,
            &dirs.runtime_home,
        ] {
            std::fs::create_dir_all(dir)?;
        }
        Ok(dirs)
    }

    pub fn effect_mounts(&self, scratch: &Path) -> Vec<MountSpec> {
        vec![
            rw(&self.handoff, HANDOFF_MOUNT_TARGET),
            rw(scratch, SCRATCH_MOUNT_TARGET),
            rw(&self.runtime, RUNTIME_MOUNT_TARGET),
            rw(&self.runtime_home, RUNTIME_HOME_MOUNT_TARGET),
        ]
    }
}

/// Durable task-owned writer resources. `work` and `scratch` survive every
/// effect outcome and are removed only by explicit mission cleanup.
pub struct TaskDirs {
    pub root: PathBuf,
    pub work: PathBuf,
    pub scratch: PathBuf,
}

impl TaskDirs {
    pub fn prepare(state_dir: &Path, mission_id: &str, task_id: &TaskId) -> std::io::Result<Self> {
        let root = state_dir
            .join("missions")
            .join(mission_id)
            .join("tasks")
            .join(task_id.as_str());
        let dirs = Self {
            work: root.join("work"),
            scratch: root.join("scratch"),
            root,
        };
        std::fs::create_dir_all(&dirs.scratch)?;
        Ok(dirs)
    }
}

fn rw(source: &Path, target: &str) -> MountSpec {
    MountSpec {
        source: source.to_path_buf(),
        target: target.to_string(),
        access: MountAccess::ReadWrite,
    }
}

pub(crate) fn prepare_skill_mounts(
    runtime_home: &Path,
    skills: &[SkillPackage],
    skills_dir: Option<&RuntimeSkillsDir>,
) -> Result<Vec<MountSpec>> {
    let Some(skills_dir) = skills_dir else {
        if skills.is_empty() {
            return Ok(Vec::new());
        }
        anyhow::bail!("runtime profile has no skills-dir for mission-assigned skills");
    };
    skills
        .iter()
        .map(|skill| {
            let mountpoint = skills_dir.host_mountpoint(runtime_home, &skill.name)?;
            std::fs::create_dir_all(&mountpoint).with_context(|| {
                format!(
                    "creating native skill mountpoint '{}'",
                    mountpoint.display()
                )
            })?;
            Ok(MountSpec {
                source: skill.root.clone(),
                target: skills_dir.mount_target(&skill.name)?,
                access: MountAccess::ReadOnly,
            })
        })
        .collect()
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn stopped(control: &ExecutionControl) -> Option<lionclaw_runtime_api::TypedFailure> {
        match control {
            ExecutionControl::Stop(reason) => {
                let mut evidence = lionclaw_runtime_api::TypedFailureEvidence::new(
                    Some("test.stopped".into()),
                    reason,
                );
                evidence.stop_reason = Some(reason.clone());
                Some(lionclaw_runtime_api::TypedFailure::OperatorStopped {
                    evidence: Box::new(evidence),
                })
            }
            ExecutionControl::DeadlineExhausted => {
                Some(lionclaw_runtime_api::TypedFailure::DeadlineExhausted {
                    evidence: Box::new(lionclaw_runtime_api::TypedFailureEvidence::new(
                        Some("test.deadline".into()),
                        "deadline",
                    )),
                })
            }
            ExecutionControl::RunUntil(_) => None,
        }
    }

    #[tokio::test]
    async fn controlled_setup_is_dropped_on_stop_and_ignores_deadline_extensions() {
        let (control_tx, control_rx) = tokio::sync::watch::channel(ExecutionControl::RunUntil(10));
        let dropped = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let future = {
            let dropped = dropped.clone();
            let entered = entered.clone();
            async move {
                let _dropped = Dropped(dropped);
                entered.notify_one();
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };
        let controlled = tokio::spawn(await_controlled(future, control_rx, stopped));
        entered.notified().await;
        control_tx.send_replace(ExecutionControl::RunUntil(20));
        tokio::task::yield_now().await;
        assert!(
            !controlled.is_finished(),
            "an extension does not cancel setup"
        );
        control_tx.send_replace(ExecutionControl::Stop("operator".into()));

        assert!(matches!(
            controlled.await.unwrap(),
            Err(lionclaw_runtime_api::TypedFailure::OperatorStopped { .. })
        ));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn closed_control_channel_does_not_starve_completion() {
        let (control_tx, control_rx) = tokio::sync::watch::channel(ExecutionControl::RunUntil(10));
        drop(control_tx);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            await_controlled(async { Ok(7) }, control_rx, stopped),
        )
        .await
        .expect("closed control channel must not cause a busy loop")
        .unwrap();
        assert_eq!(result, 7);
    }

    #[tokio::test]
    async fn pending_durable_control_wins_simultaneous_future_completion() {
        let (control_tx, control_rx) = tokio::sync::watch::channel(ExecutionControl::RunUntil(10));
        let mut first_poll = true;
        let future = std::future::poll_fn(move |context| {
            if first_poll {
                first_poll = false;
                control_tx.send_replace(ExecutionControl::Stop("already recorded".into()));
                context.waker().wake_by_ref();
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        });

        let failure = await_controlled(future, control_rx, stopped)
            .await
            .expect_err("durable stop must win when completion is also ready");

        assert!(matches!(
            failure,
            lionclaw_runtime_api::TypedFailure::OperatorStopped { .. }
        ));
        assert_eq!(
            failure.evidence().stop_reason.as_deref(),
            Some("already recorded")
        );
    }
}
