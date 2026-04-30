use std::{
    fs,
    path::{Path, PathBuf},
};

use lionclaw::{
    applied::AppliedState,
    home::LionClawHome,
    kernel::{Kernel, KernelOptions},
    operator::{
        config::ChannelLaunchMode,
        reconcile::{add_channel, add_skill, onboard, remove_skill},
    },
};
use tempfile::TempDir;

pub struct TestHome {
    temp_dir: TempDir,
    home: LionClawHome,
}

#[allow(dead_code)]
impl TestHome {
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");
        Self { temp_dir, home }
    }

    pub fn temp_dir(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn home(&self) -> &LionClawHome {
        &self.home
    }

    pub async fn install_skill(&self, alias: &str, source: &Path) {
        add_skill(
            &self.home,
            alias.to_string(),
            source.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
    }

    pub async fn remove_skill(&self, alias: &str) -> bool {
        remove_skill(&self.home, alias).await.expect("remove skill")
    }

    pub async fn add_channel(
        &self,
        channel_id: &str,
        skill_alias: &str,
        launch_mode: ChannelLaunchMode,
    ) {
        add_channel(
            &self.home,
            channel_id.to_string(),
            skill_alias.to_string(),
            launch_mode,
            Vec::new(),
        )
        .await
        .expect("add channel");
    }

    pub async fn kernel(&self) -> Kernel {
        self.kernel_with_options(KernelOptions::default()).await
    }

    pub async fn kernel_with_options(&self, mut options: KernelOptions) -> Kernel {
        if options.workspace_root.is_none() {
            options.workspace_root = Some(self.home.workspace_dir("main"));
        }
        if options.project_workspace_root.is_none() {
            options.project_workspace_root = Some(self.home.workspace_dir("main"));
        }
        options.applied_state = AppliedState::load(&self.home)
            .await
            .expect("load applied state");
        Kernel::new_with_options(&self.home.db_path(), options)
            .await
            .expect("kernel init")
    }
}

pub fn write_skill_source(
    root: &Path,
    skill_name: &str,
    description: &str,
    with_worker: bool,
) -> PathBuf {
    let skill_dir = root.join("skill-sources").join(skill_name);
    if skill_dir.exists() {
        fs::remove_dir_all(&skill_dir).expect("clear skill source");
    }
    fs::create_dir_all(&skill_dir).expect("create skill source dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: {skill_name}\ndescription: {description}\n---\n"),
    )
    .expect("write skill md");

    if with_worker {
        let worker = skill_dir.join("scripts/worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("create scripts dir");
        write_executable(&worker, "#!/usr/bin/env bash\n");
    }

    skill_dir
}

pub fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).expect("write executable");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod executable");
    }
}
