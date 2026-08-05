use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use clap::CommandFactory;
use lionclaw::cli::{Cli, DoctorCheck, DoctorReport, DoctorStatus};

fn lionclaw() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lionclaw"))
}

fn output_with_wall_timeout(mut command: Command) -> Output {
    command
        .process_group(0)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    for _ in 0..100 {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if let Some(pid) = rustix::process::Pid::from_raw(child.id() as i32) {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    let _ = child.wait();
    panic!("command exceeded 10-second test deadline");
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
                retryable: false,
                repair: None,
            },
            DoctorCheck {
                name: "runtime profiles".to_string(),
                status: DoctorStatus::Fail,
                detail: Some("invalid configuration".to_string()),
                retryable: false,
                repair: Some("fix the runtime configuration".to_string()),
            },
        ],
    };

    assert_eq!(
        report.render_human(),
        concat!(
            "PASS git\n",
            "FAIL runtime profiles — invalid configuration\n",
            "  retryable: no\n",
            "  repair: fix the runtime configuration\n",
        )
    );
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["schema"], "lionclaw.doctor.v1");
    assert!(!json["ok"].as_bool().unwrap());
    assert_eq!(json["checks"][0]["status"], "pass");
    assert_eq!(json["checks"][1]["status"], "fail");
    assert_eq!(json["checks"][1]["retryable"], false);
    assert_eq!(json["checks"][1]["repair"], "fix the runtime configuration");
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
    assert!(checks.iter().all(|check| check["retryable"].is_boolean()));
    assert!(checks
        .iter()
        .filter(|check| check["status"] == "fail")
        .all(|check| check["repair"]
            .as_str()
            .is_some_and(|repair| !repair.is_empty())));
    assert!(checks.iter().any(|check| {
        check["name"] == "lionclaw binary"
            && check["status"] == "pass"
            && check["detail"] == format!("lionclaw {}", env!("CARGO_PKG_VERSION"))
    }));
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
fn doctor_bounds_a_hanging_tool_probe() {
    let home = tempfile::tempdir().unwrap();
    let tools = tempfile::tempdir().unwrap();
    let git = tools.path().join("git");
    std::fs::write(&git, "#!/bin/sh\n/bin/sleep 30\n").unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let mut command = lionclaw();
    command
        .args(["doctor", "--json"])
        .env("PATH", path)
        .env("LIONCLAW_HOME", home.path());
    let output = output_with_wall_timeout(command);

    assert_eq!(output.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let git = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "git")
        .unwrap();
    assert_eq!(git["status"], "fail");
    assert_eq!(git["retryable"], false);
    assert!(git["detail"].as_str().unwrap().contains("timed out"));
    assert!(git["repair"].as_str().unwrap().contains("git --version"));
}

#[test]
fn doctor_bounds_a_noisy_tool_probe() {
    let home = tempfile::tempdir().unwrap();
    let tools = tempfile::tempdir().unwrap();
    let git = tools.path().join("git");
    std::fs::write(
        &git,
        "#!/bin/sh\nprintf 'git version bounded\\n'\n/bin/head -c 9437184 /dev/zero\n",
    )
    .unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = lionclaw()
        .args(["doctor", "--json"])
        .env("PATH", path)
        .env("LIONCLAW_HOME", home.path())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.len() < 64 * 1024);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let git = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "git")
        .unwrap();
    assert_eq!(git["status"], "pass");
    assert_eq!(git["detail"], "git version bounded");
}

#[test]
fn doctor_reports_selected_runtime_auth_readiness_without_exposing_it() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let user_home = tempfile::tempdir().unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.test]\n\
         driver = \"acp\"\n\
         command = \"test\"\n\
         auth = { kind = \"native-home\", source = \"~/.test-auth\", target = \".test-auth\", required-files = [\"auth.json\"] }\n",
    )
    .unwrap();

    let run = || {
        lionclaw()
            .args(["doctor", "test", "--json"])
            .env("HOME", user_home.path())
            .env_remove("CODEX_HOME")
            .env("LIONCLAW_HOME", lionclaw_home.path())
            .output()
            .unwrap()
    };
    let missing = run();
    assert_eq!(missing.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&missing.stdout).unwrap();
    let auth = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' authentication")
        .unwrap();
    assert_eq!(auth["status"], "fail");
    assert_eq!(auth["retryable"], false);
    assert!(auth["repair"]
        .as_str()
        .unwrap()
        .contains("repair or replace"));

    std::fs::create_dir(user_home.path().join(".test-auth")).unwrap();
    std::fs::write(
        user_home.path().join(".test-auth/auth.json"),
        "TOP_SECRET_SENTINEL",
    )
    .unwrap();
    let ready = run();
    let rendered = String::from_utf8(ready.stdout).unwrap();
    assert!(!rendered.contains("TOP_SECRET_SENTINEL"));
    let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    let auth = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' authentication")
        .unwrap();
    assert_eq!(auth["status"], "pass");
}

#[test]
fn doctor_does_not_refresh_or_rewrite_codex_auth() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.codex]\n\
         driver = \"codex\"\n\
         command = \"codex\"\n\
         auth = \"codex\"\n\
         model-network = { mode = \"allow\", destinations = [{ host = \"api.openai.com\", ports = [443] }] }\n",
    )
    .unwrap();
    let auth_path = codex_home.path().join("auth.json");
    let auth = br#"{
      "last_refresh": "2000-01-01T00:00:00Z",
      "tokens": {
        "access_token": "expired-token",
        "refresh_token": "TOP_SECRET_REFRESH_SENTINEL"
      }
    }"#;
    std::fs::write(&auth_path, auth).unwrap();

    let output = lionclaw()
        .args(["doctor", "codex", "--json"])
        .env("CODEX_HOME", codex_home.path())
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let readiness = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'codex' authentication")
        .unwrap();

    assert_eq!(readiness["status"], "pass");
    assert_eq!(std::fs::read(&auth_path).unwrap(), auth);
    assert!(!String::from_utf8(output.stdout)
        .unwrap()
        .contains("TOP_SECRET_REFRESH_SENTINEL"));
}

#[test]
fn doctor_rejects_codex_auth_when_the_profile_denies_model_network() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.codex]\n\
         driver = \"codex\"\n\
         command = \"codex\"\n\
         auth = \"codex\"\n\
         model-network = { mode = \"deny\" }\n",
    )
    .unwrap();
    std::fs::write(
        codex_home.path().join("auth.json"),
        r#"{"OPENAI_API_KEY":"TOP_SECRET_API_KEY"}"#,
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "codex", "--json"])
        .env("CODEX_HOME", codex_home.path())
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let auth = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'codex' authentication")
        .unwrap();

    assert_eq!(auth["status"], "fail");
    assert_eq!(auth["retryable"], false);
    assert!(auth["detail"].as_str().unwrap().contains("network"));
    assert!(auth["repair"].as_str().unwrap().contains("runtime profile"));
    assert!(!String::from_utf8(output.stdout)
        .unwrap()
        .contains("TOP_SECRET_API_KEY"));
}

#[test]
fn doctor_reports_malformed_codex_auth_as_invalid_credentials() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.codex]\n\
         driver = \"codex\"\n\
         command = \"codex\"\n\
         auth = \"codex\"\n\
         model-network = { mode = \"allow\", destinations = [{ host = \"api.openai.com\", ports = [443] }] }\n",
    )
    .unwrap();
    std::fs::write(
        codex_home.path().join("auth.json"),
        "{ malformed TOP_SECRET_AUTH_SENTINEL",
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "codex", "--json"])
        .env("CODEX_HOME", codex_home.path())
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let auth = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'codex' authentication")
        .unwrap();

    assert_eq!(auth["status"], "fail");
    assert_eq!(auth["retryable"], false);
    assert!(auth["detail"].as_str().unwrap().contains("invalid"));
    assert!(auth["repair"].as_str().unwrap().contains("codex"));
    assert!(!String::from_utf8(output.stdout)
        .unwrap()
        .contains("TOP_SECRET_AUTH_SENTINEL"));
}

#[test]
fn doctor_checks_the_selected_runtime_profiles_configured_engine_and_image() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let engine_dir = tempfile::tempdir().unwrap();
    let engine = engine_dir.path().join("configured-engine");
    std::fs::write(
        &engine,
        "#!/bin/sh\n[ \"$1 $2 $3\" = \"image exists selected-runtime:missing\" ] && exit 1\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        format!(
            "[runtimes.test]\n\
             driver = \"acp\"\n\
             command = \"test\"\n\
             confinement = {{ backend = \"podman\", engine = {:?}, image = \"selected-runtime:missing\" }}\n",
            engine.display().to_string()
        ),
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "test", "--json"])
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let image = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' image")
        .expect("selected runtime image check");

    assert_eq!(image["status"], "fail");
    assert_eq!(image["retryable"], false);
    assert!(image["repair"]
        .as_str()
        .unwrap()
        .contains("selected-runtime:missing"));
}

#[test]
fn doctor_distinguishes_an_unavailable_engine_from_a_missing_image() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let missing_engine = lionclaw_home.path().join("missing-engine");
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        format!(
            "[runtimes.test]\n\
             driver = \"acp\"\n\
             command = \"test\"\n\
             confinement = {{ backend = \"podman\", engine = {:?}, image = \"selected-runtime:present\" }}\n",
            missing_engine.display().to_string()
        ),
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "test", "--json"])
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let image = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' image")
        .expect("selected runtime image check");

    assert_eq!(image["status"], "fail");
    assert_eq!(image["retryable"], false);
    assert!(image["detail"].as_str().unwrap().contains("engine"));
    assert!(image["repair"].as_str().unwrap().contains("engine"));
    assert!(!image["repair"].as_str().unwrap().contains("provide image"));
}

#[test]
fn doctor_reports_a_present_image_that_cannot_be_inspected() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let engine = lionclaw_home.path().join("broken-engine");
    std::fs::write(
        &engine,
        "#!/bin/sh\n\
         if [ \"$1 $2\" = \"image exists\" ]; then exit 0; fi\n\
         if [ \"$1 $2\" = \"image inspect\" ]; then exit 42; fi\n\
         exit 2\n",
    )
    .unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        format!(
            "[runtimes.test]\n\
             driver = \"acp\"\n\
             command = \"test\"\n\
             confinement = {{ backend = \"podman\", engine = {:?}, image = \"broken:image\" }}\n",
            engine.display().to_string()
        ),
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "test", "--json"])
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let image = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' image")
        .expect("selected runtime image check");

    assert_eq!(image["status"], "fail");
    assert_eq!(image["retryable"], false);
    assert!(image["detail"].as_str().unwrap().contains("identity"));
    assert!(image["repair"].as_str().unwrap().contains("rebuild"));
    assert!(!image["repair"].as_str().unwrap().contains("install"));
}

#[test]
fn doctor_rejects_a_selected_runtime_profile_without_an_image() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.test]\n\
         driver = \"acp\"\n\
         command = \"test\"\n",
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "test", "--json"])
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let image = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "runtime 'test' image")
        .expect("selected runtime image check");

    assert_eq!(image["status"], "fail");
    assert_eq!(image["retryable"], false);
    assert!(image["detail"].as_str().unwrap().contains("no image"));
    assert!(image["repair"]
        .as_str()
        .unwrap()
        .contains("runtime profile"));
}

#[test]
fn doctor_rejects_a_mission_team_that_uses_multiple_oci_engines() {
    let lionclaw_home = tempfile::tempdir().unwrap();
    let install = lionclaw()
        .arg("install")
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    assert!(install.status.success());

    let implementer = lionclaw_home
        .path()
        .join("mission-types/software-dev/roles/implementer.md");
    let role = std::fs::read_to_string(&implementer).unwrap();
    std::fs::write(
        &implementer,
        role.replacen("runtime: codex", "runtime: alternate", 1),
    )
    .unwrap();
    std::fs::write(
        lionclaw_home.path().join("runtimes.toml"),
        "[runtimes.test]\n\
         driver = \"acp\"\n\
         command = \"test\"\n\
         confinement = { backend = \"podman\", engine = \"/bin/true\", image = \"runtime:test\" }\n\
         \n\
         [runtimes.codex]\n\
         driver = \"acp\"\n\
         command = \"test\"\n\
         confinement = { backend = \"podman\", engine = \"/bin/true\", image = \"runtime:test\" }\n\
         \n\
         [runtimes.alternate]\n\
         driver = \"acp\"\n\
         command = \"test\"\n\
         confinement = { backend = \"podman\", engine = \"/bin/false\", image = \"runtime:test\" }\n",
    )
    .unwrap();

    let output = lionclaw()
        .args(["doctor", "test", "--json"])
        .env("LIONCLAW_HOME", lionclaw_home.path())
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let mission_type = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "mission type 'software-dev'")
        .expect("software-dev mission type check");

    assert_eq!(mission_type["status"], "fail");
    assert!(mission_type["detail"]
        .as_str()
        .unwrap()
        .contains("incompatible"));
    assert!(mission_type["repair"]
        .as_str()
        .unwrap()
        .contains("runtimes.toml"));
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
