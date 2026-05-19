use super::*;

pub(super) async fn resolve_console_instances(
    target: &TargetContext,
) -> Result<(Option<PathBuf>, Vec<InstanceSummary>, usize)> {
    if let Some(project_root) = target.project_root.as_ref() {
        let entries = list_project_instance_statuses(project_root)?;
        let mut instances = Vec::new();
        for entry in entries {
            let home = entry.home;
            let default_runtime = load_default_runtime(&home).await;
            instances.push(InstanceSummary {
                name: Some(entry.name),
                is_default: entry.is_default,
                home,
                work_root: entry.work_root,
                work_root_finding: entry.work_root_finding,
                shared_work_root_count: entry.shared_work_root_count,
                default_runtime,
            });
        }
        if instances.is_empty() {
            return Err(anyhow!(
                "project has no instances; run lionclaw instance create main"
            ));
        }

        let selected_name = target.instance_name.as_deref();
        let selected_index = instances
            .iter()
            .position(|entry| entry.name.as_deref() == selected_name)
            .or_else(|| instances.iter().position(|entry| entry.is_default))
            .unwrap_or(0);
        Ok((Some(project_root.clone()), instances, selected_index))
    } else {
        let inspection = inspect_target_work_root(target);
        let summary = InstanceSummary {
            name: target.instance_name.clone(),
            is_default: false,
            home: target.instance_home.root(),
            work_root: inspection.work_root,
            work_root_finding: inspection.finding,
            shared_work_root_count: 0,
            default_runtime: load_default_runtime(&target.instance_home.root()).await,
        };
        Ok((None, vec![summary], 0))
    }
}

async fn load_default_runtime(home: &std::path::Path) -> Option<String> {
    OperatorConfig::load(&LionClawHome::new(home.to_path_buf()))
        .await
        .ok()
        .and_then(|config| config.defaults.runtime)
}

pub(super) async fn open_selected_instance(
    summary: InstanceSummary,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> SelectedInstanceState {
    match try_open_selected_instance(
        summary.clone(),
        requested_runtime,
        continue_last_session,
        timeout_override,
    )
    .await
    {
        Ok(ready) => SelectedInstanceState::Ready(Box::new(ready)),
        Err(err) => SelectedInstanceState::Blocked {
            blocker: LaunchBlocker::for_instance(summary.display_name(), err.to_string()),
            summary,
        },
    }
}

pub(super) async fn load_project_objects(selected: &SelectedInstanceState) -> ProjectObjects {
    let SelectedInstanceState::Ready(ready) = selected else {
        return ProjectObjects::unavailable("Launch blocked");
    };

    ProjectObjects {
        sessions: load_project_sessions(ready).await,
    }
}

async fn load_project_sessions(ready: &ReadyInstance) -> ProjectObjectSection<ProjectSessionItem> {
    match ready
        .kernel
        .list_recent_sessions(LOCAL_CLI_CHANNEL_ID, &ready.peer_id, PROJECT_SESSION_LIMIT)
        .await
    {
        Ok(sessions) => ProjectObjectSection::ready(
            project_session_items(sessions, ready.session_id),
            "No sessions yet",
        ),
        Err(err) => ProjectObjectSection::Error(format!("Sessions unavailable: {err}")),
    }
}

fn project_session_items(
    sessions: Vec<crate::kernel::sessions::Session>,
    current_session_id: Uuid,
) -> Vec<ProjectSessionItem> {
    let mut items = sessions
        .into_iter()
        .map(|session| ProjectSessionItem {
            session_id: session.session_id,
            turn_count: session.turn_count,
            current: session.session_id == current_session_id,
        })
        .collect::<Vec<_>>();

    match items.iter().position(|item| item.current) {
        Some(index) => {
            let current = items.remove(index);
            items.insert(0, current);
        }
        None => items.insert(
            0,
            ProjectSessionItem {
                session_id: current_session_id,
                turn_count: 0,
                current: true,
            },
        ),
    }
    items.truncate(PROJECT_SESSION_LIMIT);
    items
}

async fn try_open_selected_instance(
    summary: InstanceSummary,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> Result<ReadyInstance> {
    if let Some(finding) = summary.work_root_finding.as_deref() {
        return Err(anyhow!(finding.to_string()));
    }
    let work_root = summary
        .work_root
        .as_deref()
        .ok_or_else(|| anyhow!("instance does not have a resolved work root"))?;
    let home = LionClawHome::new(summary.home.clone());
    let config = OperatorConfig::load(&home).await?;
    let runtime_id = resolve_run_runtime_id(
        &config,
        requested_runtime.as_deref(),
        summary.display_name(),
    )?;
    validate_runtime_launch_prerequisites(&home, &config, &runtime_id).await?;
    render_runtime_cache_for_work_root(&home, &config, &runtime_id, work_root).await?;
    let effective_timeouts = timeout_override.unwrap_or_else(RuntimeTurnTimeouts::interactive);
    let kernel = open_runtime_kernel_for_work_root(
        &home,
        &config,
        Some(runtime_id.clone()),
        work_root,
        Some(effective_timeouts),
    )
    .await?;
    let peer_id = local_peer_id_for_project(work_root);
    let session_id = resolve_repl_session(&kernel, &peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?
        .session_id;
    let runtime_kind = config
        .runtime(&runtime_id)
        .map(|runtime| runtime.kind().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let boundary = resolve_boundary_summary(&config, effective_timeouts)?;

    Ok(ReadyInstance {
        summary,
        runtime_id,
        runtime_kind,
        runtime_override: requested_runtime,
        boundary,
        kernel,
        session_id,
        peer_id,
    })
}

fn resolve_boundary_summary(
    config: &OperatorConfig,
    timeouts: RuntimeTurnTimeouts,
) -> Result<BoundarySummary> {
    let (preset_name, preset) = resolve_execution_preset(
        ExecutionPlanPurpose::Interactive,
        None,
        config.defaults.preset.as_deref(),
        &config.presets,
    )
    .map_err(|err| anyhow!(err))?;
    Ok(BoundarySummary {
        workspace: preset.workspace_access.as_str().to_string(),
        network: preset.network_mode.as_str().to_string(),
        secrets: if preset.mount_runtime_secrets {
            "staged".to_string()
        } else {
            "off".to_string()
        },
        turn_timeout: if timeouts.idle == timeouts.hard {
            crate::runtime_timeouts::format_duration(timeouts.hard)
        } else {
            format!(
                "{}/{}",
                crate::runtime_timeouts::format_duration(timeouts.idle),
                crate::runtime_timeouts::format_duration(timeouts.hard)
            )
        },
        preset: preset_name,
    })
}

pub(super) fn push_history_turn(transcript: &mut Vec<TranscriptLine>, turn: &SessionTurnView) {
    transcript.push(TranscriptLine::new(
        TranscriptLineKind::User,
        turn.display_user_text.clone(),
    ));
    if matches!(
        turn.status,
        SessionTurnStatus::TimedOut
            | SessionTurnStatus::Failed
            | SessionTurnStatus::Cancelled
            | SessionTurnStatus::Interrupted
    ) && !turn.assistant_text.trim().is_empty()
    {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Status,
            partial_history_marker(turn.status),
        ));
    }
    if !turn.assistant_text.trim().is_empty() {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Answer,
            turn.assistant_text.clone(),
        ));
    }
    if let Some(error_text) = turn.error_text.as_deref() {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Error,
            error_text.to_string(),
        ));
    }
}

pub(super) enum StreamedSubmission {
    Prompt(String),
    Action(SessionActionKind),
}

pub(super) struct TurnOutcome {
    pub(super) response: SessionTurnResponse,
    pub(super) answer_seen: bool,
}

pub(super) enum BackendEvent {
    Stream(StreamEventDto),
    TurnFinished(Result<TurnOutcome, String>),
    SessionReset(Result<Uuid, String>),
}

pub(super) fn spawn_streamed_turn(
    kernel: Kernel,
    session_id: Uuid,
    runtime_id: String,
    submission: StreamedSubmission,
    backend_tx: mpsc::UnboundedSender<BackendEvent>,
) -> (JoinHandle<()>, oneshot::Sender<String>) {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        let mut answer_seen = false;
        match submission {
            StreamedSubmission::Prompt(prompt) => {
                let request = SessionTurnRequest {
                    session_id,
                    user_text: prompt,
                    runtime_id: Some(runtime_id),
                    runtime_working_dir: None,
                    runtime_timeout_ms: None,
                    runtime_env_passthrough: None,
                };
                let turn_future =
                    kernel.turn_session_streaming_cancellable(request, stream_tx, cancel_rx);
                tokio::pin!(turn_future);
                run_stream_future(
                    &mut turn_future,
                    &mut stream_rx,
                    &backend_tx,
                    &mut answer_seen,
                )
                .await;
            }
            StreamedSubmission::Action(action) => {
                let turn_future = kernel.run_session_action_streaming_cancellable(
                    session_id,
                    action,
                    Some(runtime_id),
                    stream_tx,
                    cancel_rx,
                );
                tokio::pin!(turn_future);
                run_stream_future(
                    &mut turn_future,
                    &mut stream_rx,
                    &backend_tx,
                    &mut answer_seen,
                )
                .await;
            }
        }
    });
    (handle, cancel_tx)
}

async fn run_stream_future<F>(
    turn_future: &mut std::pin::Pin<&mut F>,
    stream_rx: &mut mpsc::UnboundedReceiver<StreamEventDto>,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    answer_seen: &mut bool,
) where
    F: std::future::Future<Output = Result<SessionTurnResponse, crate::kernel::KernelError>>,
{
    let result = loop {
        tokio::select! {
            result = &mut *turn_future => {
                break result;
            }
            maybe_event = stream_rx.recv() => {
                let Some(event) = maybe_event else {
                    continue;
                };
                if is_answer_delta(&event) {
                    *answer_seen = true;
                }
                if backend_tx.send(BackendEvent::Stream(event)).is_err() {
                    return;
                }
            }
        }
    };

    while let Ok(event) = stream_rx.try_recv() {
        if is_answer_delta(&event) {
            *answer_seen = true;
        }
        if backend_tx.send(BackendEvent::Stream(event)).is_err() {
            return;
        }
    }

    let result = result
        .map(|response| TurnOutcome {
            response,
            answer_seen: *answer_seen,
        })
        .map_err(|err| err.to_string());
    drop(backend_tx.send(BackendEvent::TurnFinished(result)));
}

fn is_answer_delta(event: &StreamEventDto) -> bool {
    matches!(
        (&event.kind, &event.lane),
        (
            StreamEventKindDto::MessageDelta,
            Some(StreamLaneDto::Answer)
        )
    ) && event.text.as_deref().is_some_and(|text| !text.is_empty())
}

pub(super) fn push_stream_event(
    transcript: &mut Vec<TranscriptLine>,
    activity: &mut ActivitySummary,
    event: &StreamEventDto,
) {
    activity.record_stream_event(event);
    match (&event.kind, &event.lane, event.text.as_deref()) {
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Answer), Some(text)) => {
            append_transcript_delta(transcript, TranscriptLineKind::Answer, text);
        }
        (StreamEventKindDto::MessageBoundary, Some(StreamLaneDto::Answer), _) => {
            append_transcript_boundary(transcript, TranscriptLineKind::Answer);
        }
        (StreamEventKindDto::TurnCompleted, _, _)
        | (StreamEventKindDto::Done, _, _)
        | (_, _, None)
        | (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Reasoning), Some(_))
        | (StreamEventKindDto::MessageBoundary, _, _)
        | (StreamEventKindDto::Status, _, Some(_))
        | (StreamEventKindDto::Error, _, Some(_)) => {}
        (_, _, Some(_)) => {}
    }
}

pub(super) fn append_transcript_delta(
    transcript: &mut Vec<TranscriptLine>,
    kind: TranscriptLineKind,
    text: &str,
) {
    if let Some(last) = transcript.last_mut() {
        if last.kind == kind {
            append_delta_text(&mut last.text, text);
            return;
        }
    }
    transcript.push(TranscriptLine::new(kind, text));
}

fn append_delta_text(existing: &mut String, delta: &str) {
    append_streamed_text_delta(existing, delta);
}

fn append_transcript_boundary(transcript: &mut [TranscriptLine], kind: TranscriptLineKind) {
    let Some(last) = transcript.last_mut() else {
        return;
    };
    if last.kind == kind {
        append_streamed_text_boundary(&mut last.text);
    }
}
