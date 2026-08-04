use std::io::Read;
use std::process::{Command, Stdio};

use clap::CommandFactory;
use lionclaw::cli::{Cli, DoctorCheck, DoctorReport, DoctorStatus};

fn lionclaw() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lionclaw"))
}

#[test]
fn version_is_the_cargo_package_version() {
    let output = lionclaw().arg("--version").output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("lionclaw {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn doctor_renderers_share_the_typed_result() {
    let report = DoctorReport {
        schema: "lionclaw.doctor.v1",
        ok: false,
        checks: vec![
            DoctorCheck {
                name: "git".to_string(),
                status: DoctorStatus::Pass,
                detail: None,
            },
            DoctorCheck {
                name: "runtime profiles".to_string(),
                status: DoctorStatus::Fail,
                detail: Some("invalid configuration".to_string()),
            },
        ],
    };

    assert_eq!(
        report.render_human(),
        "PASS git\nFAIL runtime profiles — invalid configuration\n"
    );
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["schema"], "lionclaw.doctor.v1");
    assert!(!json["ok"].as_bool().unwrap());
    assert_eq!(json["checks"][0]["status"], "pass");
    assert_eq!(json["checks"][1]["status"], "fail");
}

#[test]
fn clean_home_doctor_json_is_parseable_and_truthfully_fails() {
    let home = tempfile::tempdir().unwrap();
    let output = lionclaw()
        .args(["doctor", "--json"])
        .env("LIONCLAW_HOME", home.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());

    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["schema"], "lionclaw.doctor.v1");
    assert!(!report["ok"].as_bool().unwrap());
    let checks = report["checks"].as_array().unwrap();
    assert!(checks
        .iter()
        .any(|check| { check["name"] == "mission types installed" && check["status"] == "fail" }));
    let rendered = String::from_utf8(output.stdout)
        .unwrap()
        .to_ascii_lowercase();
    for secret_name in ["token", "api_key", "password", "credential"] {
        assert!(!rendered.contains(secret_name));
    }
}

#[test]
fn doctor_json_does_not_echo_invalid_configuration_source() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("runtimes.toml"),
        "[runtimes.test]\n\
         driver = \"acp\"\n\
         command = \"test\"\n\
         environment = { API_KEY = \"TOP_SECRET_SENTINEL\", bad = }\n",
    )
    .unwrap();
    let bundle = home.path().join("mission-types/broken");
    std::fs::create_dir_all(&bundle).unwrap();
    std::fs::write(
        bundle.join("mission.toml"),
        "[mission-type]\nname = \"broken\"\nimage = \"BUNDLE_SECRET_SENTINEL\"\nbad =\n",
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "--json"])
        .env("LIONCLAW_HOME", home.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));

    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains("TOP_SECRET_SENTINEL"));
    assert!(!rendered.contains("BUNDLE_SECRET_SENTINEL"));
    let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    let runtime_check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime profiles")
        .unwrap();
    assert_eq!(runtime_check["status"], "fail");
    assert!(runtime_check["detail"]
        .as_str()
        .unwrap()
        .contains("could not load"));
    let bundle_check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "mission type 'broken'")
        .unwrap();
    assert_eq!(bundle_check["status"], "fail");
    assert!(bundle_check["detail"]
        .as_str()
        .unwrap()
        .contains("could not load"));
}

#[test]
fn man_uses_the_visible_clap_command_tree() {
    let root = lionclaw().arg("man").output().unwrap();
    assert!(root.status.success());
    assert!(String::from_utf8(root.stdout)
        .unwrap()
        .contains("lionclaw run"));

    let nested = lionclaw()
        .args(["man", "mission", "plan", "show"])
        .output()
        .unwrap();
    assert!(nested.status.success());
    let nested = String::from_utf8(nested.stdout).unwrap();
    assert!(nested.contains(".TH lionclaw-mission-plan-show 1"));
    assert!(nested.contains(&format!("\"lionclaw {}\"", env!("CARGO_PKG_VERSION"))));
    assert!(
        nested.contains("lionclaw\\-mission\\-plan\\-show \\- Show the current or pending plan")
    );
    assert!(nested.contains("\\fBlionclaw mission plan show\\fR"));
    assert!(nested.contains("current or pending plan"));

    for path in [
        &["man", "unknown"][..],
        &["man", "__network-proxy"][..],
        &["man", "run", "extra"][..],
    ] {
        let rejected = lionclaw().args(path).output().unwrap();
        assert!(!rejected.status.success(), "accepted {path:?}");
        assert!(rejected.stdout.is_empty());
        assert!(String::from_utf8(rejected.stderr)
            .unwrap()
            .contains("unknown command path"));
    }
}

#[test]
fn touched_help_is_command_first_and_states_defaults_and_exit_behavior() {
    let mut root = Cli::command();
    let root_help = root.render_long_help().to_string();
    assert!(root_help.starts_with("Run real agents"));
    assert!(root_help.contains("returns 0 only for a done mission"));

    let run = Cli::command().find_subcommand("run").unwrap().clone();
    let mut run = run;
    let run_help = run.render_long_help().to_string();
    assert!(run_help.starts_with("Launch or resume"));
    assert!(run_help.contains("[default: codex]"));
    assert!(run_help.contains("returns 0 only for a done mission"));

    let mut doctor = Cli::command().find_subcommand("doctor").unwrap().clone();
    let doctor_help = doctor.render_long_help().to_string();
    assert!(doctor_help.starts_with("Check local tools"));
    assert!(doctor_help.contains("Returns 0 when every check passes"));

    let mut man = Cli::command().find_subcommand("man").unwrap().clone();
    let man_help = man.render_long_help().to_string();
    assert!(man_help.starts_with("Render a manual page"));
    assert!(man_help.contains("With no command path"));
    assert!(man_help.contains("Unknown, hidden, and non-command paths fail"));
}

#[test]
fn touched_output_commands_are_quiet_on_a_closed_pipe() {
    let home = tempfile::tempdir().unwrap();
    for args in [
        &["--help"][..],
        &["--version"][..],
        &["man", "mission", "plan"][..],
        &["doctor", "--json"][..],
    ] {
        let mut child = lionclaw()
            .args(args)
            .env("LIONCLAW_HOME", home.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        drop(child.stdout.take());
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        let status = child.wait().unwrap();
        assert!(
            status.success() || args.first() == Some(&"doctor"),
            "{args:?}"
        );
        assert!(stderr.is_empty(), "{args:?}: {stderr}");
    }
}
