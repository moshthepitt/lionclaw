use anyhow::Result;
use lionclaw_runtime_api::{RuntimeControlExecution, RuntimeControlOutcome, RuntimeEventSender};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::adapter::CodexRuntimeAdapter;
use crate::app_server::{finish_app_server_session, CodexAppServerClient, CodexAppServerEventSink};

impl CodexRuntimeAdapter {
    pub(crate) async fn run_app_server_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let command = execution.input.command_name.as_str();
        match command {
            "model" | "models" => self.run_model_list_control(execution, events).await,
            "permissions" | "permission" => Ok(RuntimeControlOutcome::InteractiveOnly {
                message:
                    "Codex permission changes must be made through LionClaw runtime policy, not a runtime slash command."
                        .to_string(),
            }),
            "rename" | "name" | "compact" => self.run_thread_control(execution, events).await,
            "review" => Ok(RuntimeControlOutcome::Unsupported {
                message: "Codex review control is not exposed through LionClaw yet.".to_string(),
            }),
            _ => Ok(RuntimeControlOutcome::Unsupported {
                message: format!("Codex does not support native control command '/{command}'"),
            }),
        }
    }

    async fn run_model_list_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            input,
            mut executor,
            ..
        } = execution;
        let include_hidden = match model_list_include_hidden(&input.arguments) {
            Ok(include_hidden) => include_hidden,
            Err(outcome) => return Ok(outcome),
        };
        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self
            .start_app_server_transport(executor.as_mut(), &[])
            .await?;
        let mut client = CodexAppServerClient::new(transport);
        let sink = CodexAppServerEventSink::runtime(&events);
        let result = async {
            client.initialize(sink, &thread_state).await?;
            let response = client
                .request(
                    "model/list",
                    json!({
                        "includeHidden": include_hidden,
                    }),
                    sink,
                    &thread_state,
                )
                .await?;

            Ok(RuntimeControlOutcome::Handled {
                message: describe_model_list_response(&response),
            })
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn run_thread_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            input,
            mut executor,
            ..
        } = execution;
        let Some(saved_thread_id) = self.current_thread_id(&input.runtime_session_id)? else {
            return Ok(RuntimeControlOutcome::Failed {
                code: Some("runtime.control.thread_required".to_string()),
                message: format!(
                    "Codex control '/{}' needs an existing Codex thread; send a prompt first.",
                    input.command_name
                ),
            });
        };
        if let Some(outcome) =
            invalid_thread_control_arguments(&input.command_name, &input.arguments)
        {
            return Ok(outcome);
        }

        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self
            .start_app_server_transport(executor.as_mut(), &[])
            .await?;
        let mut client = CodexAppServerClient::new(transport);
        let sink = CodexAppServerEventSink::runtime(&events);

        let result = async {
            client.initialize(sink, &thread_state).await?;
            let thread_id = self
                .resume_app_server_thread(&mut client, &saved_thread_id, sink, &thread_state)
                .await?;

            let outcome = match input.command_name.as_str() {
                "rename" | "name" => {
                    let name = input.arguments.trim();
                    client
                        .request(
                            "thread/name/set",
                            json!({
                                "threadId": thread_id,
                                "name": name,
                            }),
                            sink,
                            &thread_state,
                        )
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: format!("Renamed Codex thread to '{name}'."),
                    }
                }
                "compact" => {
                    let (interrupt_tx, mut interrupt_rx) = mpsc::unbounded_channel();
                    client
                        .request(
                            "thread/compact/start",
                            json!({
                                "threadId": thread_id,
                            }),
                            sink,
                            &thread_state,
                        )
                        .await?;
                    client
                        .wait_for_context_compaction_completed(
                            &thread_id,
                            interrupt_tx,
                            &mut interrupt_rx,
                            sink,
                            &thread_state,
                        )
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: "Compacted Codex thread.".to_string(),
                    }
                }
                other => RuntimeControlOutcome::Unsupported {
                    message: format!("Codex does not support native control command '/{other}'"),
                },
            };

            Ok(outcome)
        }
        .await;

        finish_app_server_session(client, result).await
    }
}

pub(crate) fn invalid_thread_control_arguments(
    command_name: &str,
    arguments: &str,
) -> Option<RuntimeControlOutcome> {
    let arguments = arguments.trim();
    match command_name {
        "rename" | "name" if arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex rename requires a non-empty name.",
        )),
        "compact" if !arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex compact does not accept arguments.",
        )),
        _ => None,
    }
}

pub(crate) fn model_list_include_hidden(
    arguments: &str,
) -> std::result::Result<bool, RuntimeControlOutcome> {
    let mut include_hidden = false;
    for argument in arguments.split_whitespace() {
        match argument {
            "--hidden" | "--include-hidden" => include_hidden = true,
            argument if argument.starts_with('-') => {
                return Err(invalid_arguments_outcome(
                    "Codex model listing only accepts --hidden or --include-hidden.",
                ));
            }
            _ => {
                return Err(RuntimeControlOutcome::InteractiveOnly {
                    message:
                        "Codex model selection is interactive; configure the LionClaw runtime model instead."
                            .to_string(),
                });
            }
        }
    }
    Ok(include_hidden)
}

fn invalid_arguments_outcome(message: &str) -> RuntimeControlOutcome {
    RuntimeControlOutcome::Failed {
        code: Some("runtime.control.invalid_arguments".to_string()),
        message: message.to_string(),
    }
}

pub(crate) fn describe_model_list_response(response: &Value) -> String {
    let models: Vec<String> = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(model_display_name)
        .take(20)
        .collect();

    if models.is_empty() {
        return "Codex returned no available models.".to_string();
    }

    format!("Available Codex models: {}.", models.join(", "))
}

fn model_display_name(value: &Value) -> Option<String> {
    value
        .get("displayName")
        .and_then(Value::as_str)
        .or_else(|| value.get("model").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .or_else(|| value.get("name").and_then(Value::as_str))
        .or_else(|| value.get("label").and_then(Value::as_str))
        .filter(|name| !name.trim().is_empty())
        .map(|name| name.trim().to_string())
}
