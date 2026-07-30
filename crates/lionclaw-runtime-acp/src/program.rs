use serde_json::{json, Value};

use lionclaw_runtime_api::{RuntimeMcpServerSpec, RuntimeProgramSpec, RuntimeTerminalProgramInput};

use crate::driver::AcpRuntimeConfig;

pub(crate) fn build_acp_program(config: &AcpRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: config.args.clone(),
        environment: config.environment.clone(),
        stdin: String::new(),
        auth: config.auth.clone(),
    }
}

pub(crate) fn build_acp_terminal_program(
    config: &AcpRuntimeConfig,
    input: &RuntimeTerminalProgramInput,
) -> RuntimeProgramSpec {
    let mut args = config.terminal.args.clone();
    if input.resume {
        args.extend(config.terminal.resume_args.clone());
    }
    if let Some(argument) = &config.terminal.message_arg {
        args.push(argument.clone());
        args.push(input.bootstrap_message.clone());
    }
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: config.environment.clone(),
        stdin: String::new(),
        auth: config.auth.clone(),
    }
}

pub(crate) fn acp_mcp_servers(servers: &[RuntimeMcpServerSpec]) -> Value {
    Value::Array(
        servers
            .iter()
            .map(|server| {
                json!({
                    "name": server.name,
                    "command": server.command,
                    "args": server.args,
                    "env": [],
                })
            })
            .collect(),
    )
}
