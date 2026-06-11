use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

const SHELL_COMMAND_SCRIPT: &str = "private-context-command";

pub(crate) struct ShellCommandFixture {
    pub(crate) command_path: PathBuf,
    pub(crate) skill_root: PathBuf,
    pub(crate) state_dir: PathBuf,
}

pub(crate) fn write_shell_command_fixture(root: &Path, body: &str) -> ShellCommandFixture {
    let skill_root = root.join("skill");
    let state_dir = root.join("state");
    let scripts_dir = skill_root.join("scripts");
    fs::create_dir_all(&scripts_dir).expect("scripts");
    fs::write(scripts_dir.join(SHELL_COMMAND_SCRIPT), body).expect("script");
    ShellCommandFixture {
        command_path: command_runner(),
        skill_root,
        state_dir,
    }
}

fn command_runner() -> PathBuf {
    let runner =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/private-context-command-runner");
    assert!(runner.is_file(), "missing {}", runner.display());
    let mode = runner
        .metadata()
        .expect("runner metadata")
        .permissions()
        .mode();
    assert!(
        mode & 0o111 != 0,
        "runner is not executable: {}",
        runner.display()
    );
    runner
}
