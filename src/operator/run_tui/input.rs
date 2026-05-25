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
            app.focus = Focus::Run;
            let char_count = text.chars().count();
            app.composer.insert_str(&text);
            app.status = format!("pasted {char_count} characters");
        }
        Event::Mouse(mouse) => match mouse.kind {
            MouseEventKind::ScrollUp => scroll_up(app, mouse_scroll_amount(app)),
            MouseEventKind::ScrollDown => scroll_down(app, mouse_scroll_amount(app)),
            _ => {}
        },
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
        app.focus = Focus::Run;
        app.composer.insert_newline();
        return;
    }

    if let Some(command) = global_command_for(key) {
        handle_global_command(app, command);
        return;
    }

    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => {
            if app.focus == Focus::Controls {
                app.leave_controls();
            } else {
                app.composer.clear();
                app.status = "prompt cleared".to_string();
            }
        }
        (KeyCode::Enter, _)
            if app.focus == Focus::Controls && app.control_pane == ControlPane::Project =>
        {
            app.activate_project_cursor().await;
        }
        (KeyCode::Enter, _) if app.focus == Focus::Controls => {}
        (KeyCode::Enter, _) => app.submit_composer(backend_tx),
        (KeyCode::Up, _)
            if app.focus == Focus::Controls && app.control_pane == ControlPane::Project =>
        {
            app.move_project_cursor(-1);
        }
        (KeyCode::Down, _)
            if app.focus == Focus::Controls && app.control_pane == ControlPane::Project =>
        {
            app.move_project_cursor(1);
        }
        (KeyCode::Left, _) if app.focus == Focus::Controls || app.composer.is_blank() => {
            app.previous_control_pane();
        }
        (KeyCode::Right, _) if app.focus == Focus::Controls || app.composer.is_blank() => {
            app.next_control_pane();
        }
        (KeyCode::Up, _) if app.focus == Focus::Run && app.composer.is_blank() => {
            scroll_up(app, 1);
        }
        (KeyCode::Down, _) if app.focus == Focus::Run && app.composer.is_blank() => {
            scroll_down(app, 1);
        }
        (KeyCode::PageUp, _) => {
            let amount = active_page_size(app);
            scroll_up(app, amount);
        }
        (KeyCode::PageDown, _) => {
            let amount = active_page_size(app);
            scroll_down(app, amount);
        }
        (KeyCode::Home, _) if app.focus == Focus::Run && app.composer.is_blank() => {
            scroll_to_top(app);
        }
        (KeyCode::End, _) if app.focus == Focus::Run && app.composer.is_blank() => {
            scroll_to_bottom(app);
        }
        (KeyCode::Home | KeyCode::End, _) if app.focus == Focus::Run => {
            app.composer.handle_key(key);
        }
        (KeyCode::Home, _) => scroll_to_top(app),
        (KeyCode::End, _) => scroll_to_bottom(app),
        _ if app.focus == Focus::Run => {
            app.composer.handle_key(key);
        }
        _ if key_starts_composer(key) => {
            app.focus = Focus::Run;
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
        GlobalCommand::FocusControls => app.focus_controls(),
        GlobalCommand::ToggleMaximize => app.toggle_maximize(),
        GlobalCommand::NextFocus => app.focus_next(),
        GlobalCommand::PreviousFocus => app.focus_previous(),
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

fn active_page_size(app: &ConsoleApp) -> usize {
    app.selected_scroll().page_size
}

fn mouse_scroll_amount(app: &ConsoleApp) -> usize {
    (active_page_size(app) / 3).max(1)
}

fn scroll_up(app: &mut ConsoleApp, amount: usize) {
    app.selected_scroll_mut().scroll_up(amount);
}

fn scroll_down(app: &mut ConsoleApp, amount: usize) {
    app.selected_scroll_mut().scroll_down(amount);
}

fn scroll_to_top(app: &mut ConsoleApp) {
    app.selected_scroll_mut().scroll_to_top();
}

fn scroll_to_bottom(app: &mut ConsoleApp) {
    app.selected_scroll_mut().scroll_to_bottom();
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
