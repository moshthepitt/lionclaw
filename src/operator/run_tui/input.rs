use super::*;

pub(super) async fn handle_terminal_event(
    app: &mut ConsoleApp,
    event: Event,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
) {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            handle_key(app, key, backend_tx).await;
        }
        Event::Paste(text) => {
            app.focus = Focus::Composer;
            let char_count = text.chars().count();
            app.composer.insert_str(&text);
            app.status = format!("pasted {char_count} characters");
        }
        _ => {}
    }
}

pub(super) async fn handle_key(
    app: &mut ConsoleApp,
    key: KeyEvent,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
) {
    if app.overlay.is_some() {
        handle_overlay_key(app, key).await;
        return;
    }

    if is_composer_newline_key(key) {
        app.focus = Focus::Composer;
        app.composer.insert_newline();
        return;
    }

    if let Some(command) = global_command_for(key) {
        handle_global_command(app, command);
        return;
    }

    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => {
            app.composer.clear();
            app.status = "composer cleared".to_string();
        }
        (KeyCode::Enter, _) if app.focus == Focus::Project => {
            app.activate_project_cursor().await;
        }
        (KeyCode::Enter, _) => app.submit_composer(backend_tx),
        (KeyCode::Up, _) if app.focus == Focus::Project => {
            app.move_project_cursor(-1);
        }
        (KeyCode::Down, _) if app.focus == Focus::Project => {
            app.move_project_cursor(1);
        }
        (KeyCode::Up, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_up(1);
        }
        (KeyCode::PageUp, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_up(app.transcript_scroll.page_size);
        }
        (KeyCode::Down, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_down(1);
        }
        (KeyCode::PageDown, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_down(app.transcript_scroll.page_size);
        }
        (KeyCode::Home, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_to_top();
        }
        (KeyCode::End, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_to_bottom();
        }
        (KeyCode::Left, _) if app.focus == Focus::Inspectors => {
            app.previous_inspector_subject();
        }
        (KeyCode::Right, _) if app.focus == Focus::Inspectors => {
            app.next_inspector_subject();
        }
        (KeyCode::Up, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_up(1);
        }
        (KeyCode::PageUp, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_up(app.activity_scroll.page_size);
        }
        (KeyCode::Down, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_down(1);
        }
        (KeyCode::PageDown, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_down(app.activity_scroll.page_size);
        }
        (KeyCode::Home, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_to_top();
        }
        (KeyCode::End, _)
            if app.focus == Focus::Inspectors
                && app.inspector_subject == InspectorSubject::Activity =>
        {
            app.scroll_activity_to_bottom();
        }
        _ if app.focus == Focus::Composer => {
            app.composer.handle_key(key);
        }
        _ if key_starts_composer(key) => {
            app.focus = Focus::Composer;
            app.composer.handle_key(key);
        }
        _ => {}
    }
}

pub(super) fn global_command_for(key: KeyEvent) -> Option<GlobalCommand> {
    GLOBAL_KEY_BINDINGS
        .iter()
        .find(|binding| binding.chord.matches(key))
        .map(|binding| binding.command)
}

fn handle_global_command(app: &mut ConsoleApp, command: GlobalCommand) {
    match command {
        GlobalCommand::Commands => app.overlay = Some(Overlay::Help),
        GlobalCommand::NextFocus => {
            app.focus = app.focus.next(app.project_mode());
            app.status = format!("focus: {}", app.focus.label());
        }
        GlobalCommand::PreviousFocus => {
            app.focus = app.focus.previous(app.project_mode());
            app.status = format!("focus: {}", app.focus.label());
        }
        GlobalCommand::InterruptOrConfirmExit => {
            if app.active() {
                if let Some(cancellation) = app.active_turn_cancel.take() {
                    if cancellation.request("turn interrupted from operator console") {
                        app.status = "stopping turn".to_string();
                    } else {
                        app.status = "interrupt already requested".to_string();
                    }
                } else {
                    app.status = "interrupt already requested".to_string();
                }
            } else {
                app.overlay = Some(Overlay::ExitConfirm);
            }
        }
        GlobalCommand::Exit => {
            if app.active() {
                app.status = "finish the active turn before exiting".to_string();
            } else {
                app.should_quit = true;
            }
        }
    }
}

fn is_composer_newline_key(key: KeyEvent) -> bool {
    (key.code == KeyCode::Enter
        && (key.modifiers.contains(KeyModifiers::SHIFT)
            || key.modifiers.contains(KeyModifiers::ALT)))
        || (key.code == KeyCode::Char('j') && key.modifiers == KeyModifiers::CONTROL)
}

fn key_starts_composer(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char(_))
        && !key.modifiers.contains(KeyModifiers::CONTROL)
        && !key.modifiers.contains(KeyModifiers::ALT)
}

async fn handle_overlay_key(app: &mut ConsoleApp, key: KeyEvent) {
    match app.overlay {
        Some(Overlay::ExitConfirm) => match (key.code, key.modifiers) {
            (KeyCode::Char('y'), KeyModifiers::NONE) => app.should_quit = true,
            (KeyCode::Esc | KeyCode::Enter, _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                app.overlay = None
            }
            _ => {}
        },
        Some(Overlay::InstanceSwitchConfirm { target_index }) => match (key.code, key.modifiers) {
            (KeyCode::Char('y'), KeyModifiers::NONE) => {
                app.overlay = None;
                app.switch_selected_confirmed(target_index).await;
            }
            (KeyCode::Esc | KeyCode::Enter, _)
            | (KeyCode::Char('n'), KeyModifiers::NONE)
            | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                app.overlay = None;
                app.status = "instance switch cancelled".to_string();
            }
            _ => {}
        },
        Some(Overlay::SessionSwitchConfirm { session_id }) => match (key.code, key.modifiers) {
            (KeyCode::Char('y'), KeyModifiers::NONE) => {
                app.overlay = None;
                app.switch_selected_session(session_id, true).await;
            }
            (KeyCode::Esc | KeyCode::Enter, _)
            | (KeyCode::Char('n'), KeyModifiers::NONE)
            | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                app.overlay = None;
                app.status = "session switch cancelled".to_string();
            }
            _ => {}
        },
        Some(Overlay::Help) => match (key.code, key.modifiers) {
            (KeyCode::Esc | KeyCode::Enter, _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                app.overlay = None
            }
            _ => {}
        },
        None => {}
    }
}
