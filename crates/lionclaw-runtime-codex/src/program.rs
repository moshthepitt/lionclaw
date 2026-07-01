use lionclaw_runtime_api::{RuntimeMcpServerSpec, RuntimeProgramSpec};

use crate::driver::{codex_runtime_auth_kind, CodexRuntimeConfig};

const LIONCLAW_RUNTIME_CONTEXT_PATH: &str = "/runtime/AGENTS.generated.md";
const CODEX_RUNTIME_WORKSPACE_PATH: &str = "/workspace";
const CODEX_TRUSTED_LEVEL: &str = "trusted";

pub(crate) fn build_codex_app_server_program(
    config: &CodexRuntimeConfig,
    mcp_servers: &[RuntimeMcpServerSpec],
) -> RuntimeProgramSpec {
    let mut args = codex_runtime_config_override_args();
    args.extend(codex_mcp_server_override_args(mcp_servers));
    args.push("app-server".to_string());

    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: Vec::new(),
        stdin: String::new(),
        auth: Some(codex_runtime_auth_kind()),
    }
}

pub(crate) fn build_codex_terminal_program(config: &CodexRuntimeConfig) -> RuntimeProgramSpec {
    let mut args = vec![
        "--sandbox".to_string(),
        "danger-full-access".to_string(),
        "--ask-for-approval".to_string(),
        "never".to_string(),
    ];
    args.extend(codex_terminal_config_override_args());
    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.clone());
    }

    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: Vec::new(),
        stdin: String::new(),
        auth: Some(codex_runtime_auth_kind()),
    }
}

fn codex_terminal_config_override_args() -> Vec<String> {
    let mut args = codex_runtime_config_override_args();
    args.push("-c".to_string());
    args.push(format!(
        "model_instructions_file=\"{LIONCLAW_RUNTIME_CONTEXT_PATH}\""
    ));
    args
}

fn codex_runtime_config_override_args() -> Vec<String> {
    vec![
        "-c".to_string(),
        "check_for_update_on_startup=false".to_string(),
        "-c".to_string(),
        format!(
            "projects.\"{CODEX_RUNTIME_WORKSPACE_PATH}\".trust_level=\"{CODEX_TRUSTED_LEVEL}\""
        ),
    ]
}

pub(crate) fn codex_mcp_server_override_args(servers: &[RuntimeMcpServerSpec]) -> Vec<String> {
    let mut args = Vec::new();
    for server in servers {
        let name = codex_toml_key_segment(&server.name);
        args.push("-c".to_string());
        args.push(format!(
            "mcp_servers.{name}.command={}",
            codex_toml_string(&server.command)
        ));
        args.push("-c".to_string());
        args.push(format!(
            "mcp_servers.{name}.args={}",
            codex_toml_string_array(&server.args)
        ));
    }
    args
}

fn codex_toml_key_segment(raw: &str) -> String {
    codex_toml_string(raw)
}

fn codex_toml_string(raw: &str) -> String {
    let mut encoded = String::with_capacity(raw.len() + 2);
    encoded.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            '\u{08}' => encoded.push_str("\\b"),
            '\u{0C}' => encoded.push_str("\\f"),
            ch if ch.is_control() => encoded.push_str(&format!("\\u{:04X}", ch as u32)),
            ch => encoded.push(ch),
        }
    }
    encoded.push('"');
    encoded
}

fn codex_toml_string_array(values: &[String]) -> String {
    let mut encoded = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            encoded.push(',');
        }
        encoded.push_str(&codex_toml_string(value));
    }
    encoded.push(']');
    encoded
}
