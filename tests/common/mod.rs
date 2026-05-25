use std::{
    fs,
    path::{Path, PathBuf},
};

use lionclaw::{
    applied::{AppliedSkill, AppliedState},
    home::LionClawHome,
    kernel::{Kernel, KernelOptions},
    operator::{
        channel_metadata::discover_channel_skill,
        config::ChannelLaunchMode,
        reconcile::{add_channel, add_channel_with_worker, add_skill, remove_skill},
        target::init_project,
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
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home);
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

    pub async fn install_channel_fixture(&self, alias: &str, channel_id: &str) {
        let source = write_channel_fixture_source(self.temp_dir(), alias, channel_id);
        let discovered = discover_channel_skill(&self.home, &source.display().to_string())
            .expect("discover generated channel fixture");
        assert_eq!(discovered.metadata.id, channel_id);
        assert_eq!(discovered.metadata.env, Vec::<String>::new());

        add_skill(
            &self.home,
            alias.to_string(),
            discovered.skill_dir.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install channel fixture skill");
        add_channel_with_worker(
            &self.home,
            discovered.metadata.id.clone(),
            alias.to_string(),
            discovered.metadata.launch,
            discovered.metadata.worker.clone(),
            discovered.metadata.env.clone(),
            None,
        )
        .await
        .expect("bind channel fixture");
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

    pub async fn installed_skill(&self, alias: &str) -> AppliedSkill {
        AppliedState::load(&self.home)
            .await
            .expect("load applied state")
            .skill_by_alias(alias)
            .cloned()
            .expect("installed skill")
    }

    pub async fn installed_skill_id(&self, alias: &str) -> String {
        self.installed_skill(alias).await.skill_id
    }

    pub async fn installed_skills(&self) -> Vec<AppliedSkill> {
        AppliedState::load(&self.home)
            .await
            .expect("load applied state")
            .skills()
            .to_vec()
    }
}

#[allow(dead_code)]
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

fn write_channel_fixture_source(root: &Path, alias: &str, channel_id: &str) -> PathBuf {
    let skill_dir = root.join("channel-fixtures").join(alias);
    if skill_dir.exists() {
        fs::remove_dir_all(&skill_dir).expect("clear channel fixture source");
    }
    fs::create_dir_all(skill_dir.join("scripts")).expect("create channel fixture source");
    fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: {alias}\ndescription: test-only loopback channel fixture\n---\n"),
    )
    .expect("write channel fixture skill md");
    fs::write(
        skill_dir.join("lionclaw.toml"),
        format!(
            "version = 1\n\n[channel]\nid = \"{channel_id}\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = []\n"
        ),
    )
    .expect("write channel fixture metadata");
    write_executable(
        &skill_dir.join("scripts/worker"),
        "#!/bin/sh\nprintf '%s\\n' 'test-only channel fixture worker'\n",
    );
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
