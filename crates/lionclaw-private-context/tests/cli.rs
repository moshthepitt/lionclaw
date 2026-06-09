use std::{
    io::Write,
    process::{Command, Stdio},
};

use tempfile::tempdir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_lionclaw-private-context")
}

#[test]
fn context_requires_skill_state_dir() {
    let output = Command::new(binary())
        .args(["context", "memory", "list"])
        .env_remove("LIONCLAW_SKILL_STATE_DIR")
        .output()
        .expect("run context command");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr utf8");
    assert!(stderr.contains("LIONCLAW_SKILL_STATE_DIR is required"));
}

#[test]
fn recorder_accepts_one_request_and_writes_empty_stdout() {
    let state = tempdir().expect("state dir");
    let mut child = Command::new(binary())
        .arg("recorder")
        .env("LIONCLAW_SKILL_STATE_DIR", state.path())
        .env("LIONCLAW_PRIVATE_CONTEXT_ID", "lionclaw-private-context")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn recorder");
    child
        .stdin
        .as_mut()
        .expect("recorder stdin")
        .write_all(
            br#"{
                "session_id":"11111111-1111-1111-1111-111111111111",
                "turn_id":"22222222-2222-2222-2222-222222222222",
                "sequence_no":1,
                "runtime_id":"codex",
                "trust_tier":"main",
                "history_policy":"interactive",
                "surface":"program_turn",
                "project_scope":"alpha",
                "transcript":{
                    "user":{
                        "text":"remember: use deterministic recorder tests",
                        "included_bytes":42,
                        "original_bytes":42
                    },
                    "assistant":{
                        "text":"noted",
                        "included_bytes":5,
                        "original_bytes":5
                    }
                }
            }"#,
        )
        .expect("write request");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait recorder");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}
