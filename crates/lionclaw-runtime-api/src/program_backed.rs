use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use crate::{
    adapter::{RuntimeAdapter, RuntimeProgramTurnExecution, RuntimeTurnInput},
    capability::RuntimeTurnResult,
    context::RuntimeExecutionContext,
    event::{RuntimeEvent, RuntimeTurnJournalSender, TurnEvent},
    program::{ExecutionOutput, RuntimeProgramExecutor},
};

pub trait RuntimeProgramOutputParser: Send {
    fn parse_line(&mut self, line: &str) -> Vec<RuntimeEvent>;
    fn finish(&mut self) -> Vec<RuntimeEvent> {
        Vec::new()
    }
}

pub async fn execute_program_backed_turn<A>(
    adapter: &A,
    execution: RuntimeProgramTurnExecution,
    journal: RuntimeTurnJournalSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let RuntimeProgramTurnExecution {
        input,
        context,
        mut executor,
    } = execution;

    execute_program_backed_turn_with_executor(adapter, executor.as_mut(), input, &context, journal)
        .await
}

async fn execute_program_backed_turn_with_executor<A>(
    adapter: &A,
    executor: &mut dyn RuntimeProgramExecutor,
    input: RuntimeTurnInput,
    context: &RuntimeExecutionContext,
    journal: RuntimeTurnJournalSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let mut attempted_retry = false;
    let mut current_input = input;

    loop {
        let attempt =
            run_program_backed_attempt(adapter, executor, &current_input, context, &journal)
                .await?;

        if attempt.output.success() {
            flush_buffered_program_output_events(&journal, attempt.buffered_errors);
            return finish_program_backed_turn(
                adapter,
                attempt.output,
                attempt.last_error_text.as_deref(),
                attempt.saw_done,
                &journal,
            );
        }

        if !attempted_retry
            && adapter.prepare_program_retry_after_failure(
                &current_input,
                &attempt.output,
                attempt.last_error_text.as_deref(),
                &journal,
            )?
        {
            attempted_retry = true;
            if let Some(fresh_prompt) = current_input.fresh_prompt.take() {
                current_input.prompt = fresh_prompt;
            }
            continue;
        }

        flush_buffered_program_output_events(&journal, attempt.buffered_errors);
        return finish_program_backed_turn(
            adapter,
            attempt.output,
            attempt.last_error_text.as_deref(),
            attempt.saw_done,
            &journal,
        );
    }
}

struct ProgramBackedAttemptOutcome {
    buffered_errors: Option<Vec<RuntimeEvent>>,
    output: ExecutionOutput,
    saw_done: bool,
    last_error_text: Option<String>,
}

async fn run_program_backed_attempt<A>(
    adapter: &A,
    executor: &mut dyn RuntimeProgramExecutor,
    input: &RuntimeTurnInput,
    context: &RuntimeExecutionContext,
    journal: &RuntimeTurnJournalSender,
) -> Result<ProgramBackedAttemptOutcome>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let program = adapter.build_turn_program(input, context)?;
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    let execution = executor.execute_streaming(program, stdout_tx);
    tokio::pin!(execution);

    let mut buffered_errors = input.fresh_prompt.is_some().then(Vec::new);
    let mut saw_done = false;
    let mut last_error_text: Option<String> = None;
    let mut output_parser = adapter.program_output_parser(input);

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                match maybe_line {
                    Some(line) => observe_program_output_line(
                        adapter,
                        &mut output_parser,
                        journal,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    ),
                    None => {
                        finish_program_output_parser(
                            &mut output_parser,
                            journal,
                            &mut buffered_errors,
                            &mut saw_done,
                            &mut last_error_text,
                        );
                        let output = execution.await?;
                        return Ok(ProgramBackedAttemptOutcome {
                            buffered_errors,
                            output,
                            saw_done,
                            last_error_text,
                        });
                    }
                }
            }
            output = &mut execution => {
                let output = output?;
                while let Some(line) = stdout_rx.recv().await {
                    observe_program_output_line(
                        adapter,
                        &mut output_parser,
                        journal,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    );
                }
                finish_program_output_parser(
                    &mut output_parser,
                    journal,
                    &mut buffered_errors,
                    &mut saw_done,
                    &mut last_error_text,
                );
                return Ok(ProgramBackedAttemptOutcome {
                    buffered_errors,
                    output,
                    saw_done,
                    last_error_text,
                });
            }
        }
    }
}

fn observe_program_output_line<A>(
    adapter: &A,
    output_parser: &mut Option<Box<dyn RuntimeProgramOutputParser>>,
    journal: &RuntimeTurnJournalSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    line: &str,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let parsed_events = if let Some(parser) = output_parser.as_mut() {
        parser.parse_line(line)
    } else {
        adapter.parse_program_output_line(line)
    };

    observe_program_output_events(
        journal,
        buffered_errors,
        parsed_events,
        saw_done,
        last_error_text,
    );
}

fn finish_program_output_parser(
    output_parser: &mut Option<Box<dyn RuntimeProgramOutputParser>>,
    journal: &RuntimeTurnJournalSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    if let Some(parser) = output_parser.as_mut() {
        observe_program_output_events(
            journal,
            buffered_errors,
            parser.finish(),
            saw_done,
            last_error_text,
        );
    }
}

fn observe_program_output_events(
    journal: &RuntimeTurnJournalSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    parsed_events: Vec<RuntimeEvent>,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    for event in parsed_events {
        if matches!(event, RuntimeEvent::Done) {
            *saw_done = true;
        }
        if let RuntimeEvent::Error { text, .. } = &event {
            *last_error_text = Some(text.clone());
        }
        if matches!(event, RuntimeEvent::Error { .. }) {
            if let Some(buffer) = buffered_errors.as_mut() {
                buffer.push(event);
            } else {
                drop(journal.send(TurnEvent::canonical(event)));
            }
        } else {
            drop(journal.send(TurnEvent::canonical(event)));
        }
    }
}

fn flush_buffered_program_output_events(
    journal: &RuntimeTurnJournalSender,
    buffered_errors: Option<Vec<RuntimeEvent>>,
) {
    if let Some(buffered_errors) = buffered_errors {
        for event in buffered_errors {
            drop(journal.send(TurnEvent::canonical(event)));
        }
    }
}

fn finish_program_backed_turn<A>(
    adapter: &A,
    output: ExecutionOutput,
    observed_error_text: Option<&str>,
    saw_done: bool,
    journal: &RuntimeTurnJournalSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    if !output.success() {
        return Err(anyhow!(
            adapter.format_program_exit_error(&output, observed_error_text)
        ));
    }

    if !saw_done {
        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)));
    }

    Ok(RuntimeTurnResult {
        capability_requests: Vec::new(),
    })
}
