mod collect;
mod model;
mod ui;

use std::io::Write;
use std::io::stdout;
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use collect::{Args, CollectorCommand, spawn_collector};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use model::{AppState, FocusPane, PopupKind};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

fn main() -> Result<()> {
    let args = Args::parse();
    let refresh_every = Duration::from_millis(args.refresh_ms);

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Some(Terminal::new(backend)?);
    terminal.as_mut().expect("terminal").clear()?;

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (command_tx, command_rx) = mpsc::channel();
    let collector = spawn_collector(args.clone(), snapshot_tx, command_rx);

    let result = run_app(
        &mut terminal,
        &snapshot_rx,
        &command_tx,
        refresh_every,
        args.active_only,
        args.custom_tool_command.clone(),
    );

    let _ = command_tx.send(CollectorCommand::Quit);
    let _ = collector.join();
    if let Some(mut terminal) = terminal {
        restore_terminal(&mut terminal)?;
    }
    result
}

fn run_app(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    snapshot_rx: &mpsc::Receiver<model::ClusterSnapshot>,
    command_tx: &mpsc::Sender<CollectorCommand>,
    refresh_every: Duration,
    active_only: bool,
    custom_tool_command: Option<String>,
) -> Result<()> {
    let mut state = AppState::new(refresh_every, active_only, custom_tool_command);

    loop {
        while let Ok(snapshot) = snapshot_rx.try_recv() {
            state.latest = Some(snapshot);
        }

        let visible_len = state
            .latest
            .as_ref()
            .map(|snapshot| ui::visible_node_count(snapshot, &state))
            .unwrap_or(0);
        if visible_len == 0 {
            state.selected_node = 0;
        } else {
            state.selected_node = state.selected_node.min(visible_len.saturating_sub(1));
        }
        let job_len = state
            .latest
            .as_ref()
            .map(|snapshot| ui::visible_job_count(snapshot, &state))
            .unwrap_or(0);
        if job_len == 0 {
            state.selected_job = 0;
        } else {
            state.selected_job = state.selected_job.min(job_len.saturating_sub(1));
        }

        terminal
            .as_mut()
            .expect("terminal initialized")
            .draw(|frame| ui::draw(frame, &state))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if handle_filter_input(&mut state, key.code) {
                    continue;
                }
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    break;
                }
                if handle_popup_input(terminal, &mut state, &command_tx, key.code)? {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('R') => {
                        let _ = command_tx.send(CollectorCommand::RefreshNow);
                    }
                    KeyCode::Char('a') => {
                        state.show_active_only = !state.show_active_only;
                        state.selected_node = 0;
                    }
                    KeyCode::Char('n') => {
                        launch_remote_tool(terminal, &mut state, "nvtop")?;
                    }
                    KeyCode::Char('b') => {
                        launch_remote_tool(terminal, &mut state, "btop")?;
                    }
                    KeyCode::Char('h') => {
                        launch_remote_tool(terminal, &mut state, "htop")?;
                    }
                    KeyCode::Char('r') => {
                        launch_custom_tool(terminal, &mut state)?;
                    }
                    KeyCode::Char('c') => {
                        prompt_cancel_selected_job(&mut state)?;
                    }
                    KeyCode::Enter => {
                        launch_remote_shell(terminal, &mut state)?;
                    }
                    KeyCode::Char('u') => {
                        state.filter_input = Some(state.user_filter.clone().unwrap_or_default());
                        state.notice =
                            Some("type a username and press Enter, or Esc to cancel".into());
                    }
                    KeyCode::Char('m') => {
                        toggle_mine_filter(&mut state);
                    }
                    KeyCode::Char('t') => {
                        state.popup = Some(PopupKind::Tools);
                        state.notice = Some("tools: h=htop, b=btop, n=nvtop, r=run".into());
                    }
                    KeyCode::Char('?') => {
                        state.popup = Some(PopupKind::Help);
                    }
                    KeyCode::Tab => {
                        state.focus.toggle();
                    }
                    KeyCode::Char('s') => {
                        state.sort_mode = state.sort_mode.next();
                    }
                    KeyCode::Char('S') => {
                        state.descending = !state.descending;
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        move_selection(&mut state, 1);
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        move_selection(&mut state, -1);
                    }
                    KeyCode::PageDown => {
                        move_selection(&mut state, 10);
                    }
                    KeyCode::PageUp => {
                        move_selection(&mut state, -10);
                    }
                    KeyCode::Home => match state.focus {
                        FocusPane::Nodes => state.selected_node = 0,
                        FocusPane::Jobs => state.selected_job = 0,
                    },
                    KeyCode::End => match state.focus {
                        FocusPane::Nodes => state.selected_node = usize::MAX,
                        FocusPane::Jobs => state.selected_job = usize::MAX,
                    },
                    _ => {}
                }
            }
        }
    }

    state.save_persisted();
    Ok(())
}

fn move_selection(state: &mut AppState, delta: isize) {
    match state.focus {
        FocusPane::Nodes => {
            state.selected_node = state.selected_node.saturating_add_signed(delta);
        }
        FocusPane::Jobs => {
            state.selected_job = state.selected_job.saturating_add_signed(delta);
        }
    }
}

fn handle_popup_input(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
    command_tx: &mpsc::Sender<CollectorCommand>,
    key: KeyCode,
) -> Result<bool> {
    let Some(popup) = state.popup else {
        return Ok(false);
    };

    match popup {
        PopupKind::Tools => match key {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('t') => {
                state.popup = None;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                state.selected_tool = (state.selected_tool + 1).min(3);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                state.selected_tool = state.selected_tool.saturating_sub(1);
            }
            KeyCode::Char('h') => {
                state.popup = None;
                launch_remote_tool(terminal, state, "htop")?;
            }
            KeyCode::Char('b') => {
                state.popup = None;
                launch_remote_tool(terminal, state, "btop")?;
            }
            KeyCode::Char('n') => {
                state.popup = None;
                launch_remote_tool(terminal, state, "nvtop")?;
            }
            KeyCode::Char('r') => {
                state.popup = None;
                launch_custom_tool(terminal, state)?;
            }
            KeyCode::Enter => {
                state.popup = None;
                launch_tool_for_index(terminal, state)?;
            }
            KeyCode::Char('?') => {
                state.popup = Some(PopupKind::Help);
            }
            _ => {}
        },
        PopupKind::CancelJobConfirm => match key {
            KeyCode::Char('y') => {
                state.popup = None;
                cancel_pending_job(state, command_tx)?;
            }
            KeyCode::Char('n') | KeyCode::Esc | KeyCode::Char('q') => {
                let job_id = state
                    .pending_cancel_job
                    .take()
                    .unwrap_or_else(|| "?".into());
                state.popup = None;
                state.notice = Some(format!("cancel aborted for job {job_id}"));
            }
            _ => {}
        },
        PopupKind::Help => match key {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('?') => {
                state.popup = None;
            }
            KeyCode::Char('t') => {
                state.popup = Some(PopupKind::Tools);
            }
            _ => {}
        },
    }

    Ok(true)
}

fn launch_tool_for_index(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
) -> Result<()> {
    match state.selected_tool {
        0 => launch_remote_tool(terminal, state, "htop"),
        1 => launch_remote_tool(terminal, state, "btop"),
        2 => launch_remote_tool(terminal, state, "nvtop"),
        _ => launch_custom_tool(terminal, state),
    }
}

fn handle_filter_input(state: &mut AppState, key: KeyCode) -> bool {
    let Some(buffer) = state.filter_input.as_mut() else {
        return false;
    };
    match key {
        KeyCode::Esc => {
            state.filter_input = None;
            state.notice = Some("user filter unchanged".into());
        }
        KeyCode::Enter => {
            let input = state.filter_input.take().unwrap_or_default();
            apply_user_filter(state, input.trim());
        }
        KeyCode::Backspace => {
            buffer.pop();
        }
        KeyCode::Char(ch) if !ch.is_control() => {
            buffer.push(ch);
        }
        _ => {}
    }
    true
}

fn apply_user_filter(state: &mut AppState, filter: &str) {
    state.user_filter = if filter.is_empty() {
        None
    } else {
        Some(filter.to_string())
    };
    state.selected_node = 0;
    state.selected_job = 0;
    state.notice = match state.user_filter.as_ref() {
        Some(filter) => Some(format!("filtering for user {filter}")),
        None => Some("cleared user filter".into()),
    };
}

fn toggle_mine_filter(state: &mut AppState) {
    if state.user_filter.as_deref() == Some(state.current_user.as_str()) {
        apply_user_filter(state, "");
    } else {
        let current_user = state.current_user.clone();
        apply_user_filter(state, &current_user);
    }
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    terminal.backend_mut().flush()?;
    Ok(())
}

fn enter_terminal(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> Result<()> {
    enable_raw_mode()?;
    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
    terminal.clear()?;
    terminal.backend_mut().flush()?;
    Ok(())
}

fn launch_remote_tool(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
    tool: &str,
) -> Result<()> {
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return Ok(());
    };
    let target = match ui::selected_target_for_launch(snapshot, state) {
        Ok(target) => target,
        Err(message) => {
            state.notice = Some(message);
            return Ok(());
        }
    };

    launch_remote_exec(
        terminal,
        state,
        &target.name,
        &target.addr,
        tool,
        &format!("exec {tool}"),
    )
}

fn launch_custom_tool(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
) -> Result<()> {
    let Some(command) = state.custom_tool_command.clone() else {
        state.notice = Some("run command not configured; use --custom-tool-command".into());
        return Ok(());
    };
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return Ok(());
    };
    let target = match ui::selected_target_for_launch(snapshot, state) {
        Ok(target) => target,
        Err(message) => {
            state.notice = Some(message);
            return Ok(());
        }
    };

    launch_remote_exec(
        terminal,
        state,
        &target.name,
        &target.addr,
        "run command",
        &command,
    )
}

fn prompt_cancel_selected_job(state: &mut AppState) -> Result<()> {
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return Ok(());
    };
    let Some(job_id) = ui::selected_job_id(snapshot, state) else {
        state.notice = Some(match state.focus {
            FocusPane::Jobs => "no job selected".into(),
            FocusPane::Nodes => "job cancel only works in the jobs pane".into(),
        });
        return Ok(());
    };
    if !matches!(state.focus, FocusPane::Jobs) {
        state.notice = Some("job cancel only works in the jobs pane".into());
        return Ok(());
    }
    state.pending_cancel_job = Some(job_id);
    state.popup = Some(PopupKind::CancelJobConfirm);
    Ok(())
}

fn cancel_pending_job(
    state: &mut AppState,
    command_tx: &mpsc::Sender<CollectorCommand>,
) -> Result<()> {
    let Some(job_id) = state.pending_cancel_job.take() else {
        state.notice = Some("no pending job cancel".into());
        return Ok(());
    };

    let output = Command::new("scancel").arg(&job_id).output();
    match output {
        Ok(output) if output.status.success() => {
            state.notice = Some(format!("cancel requested for job {job_id}"));
            let _ = command_tx.send(CollectorCommand::RefreshNow);
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            state.notice = Some(if stderr.is_empty() {
                format!("scancel failed for job {job_id}")
            } else {
                format!("scancel failed for job {job_id}: {stderr}")
            });
        }
        Err(error) => {
            state.notice = Some(format!("scancel launch failed for job {job_id}: {error}"));
        }
    }
    Ok(())
}

fn launch_remote_shell(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
) -> Result<()> {
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return Ok(());
    };
    let target = match ui::selected_target_for_launch(snapshot, state) {
        Ok(target) => target,
        Err(message) => {
            state.notice = Some(message);
            return Ok(());
        }
    };

    let mut owned_terminal = terminal.take().expect("terminal initialized");
    restore_terminal(&mut owned_terminal)?;
    drop(owned_terminal);
    stdout().flush()?;
    let status = Command::new("ssh")
        .args([
            "-tt",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            target.addr.as_str(),
        ])
        .status();
    let mut new_terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    enter_terminal(&mut new_terminal)?;
    *terminal = Some(new_terminal);

    match status {
        Ok(status) if status.success() => {
            state.notice = Some(format!("ssh closed on {}", target.name));
        }
        Ok(status) => {
            state.notice = Some(format!(
                "ssh exited with status {} on {}",
                status, target.name
            ));
        }
        Err(error) => {
            state.notice = Some(format!("ssh launch failed on {}: {error}", target.name));
        }
    }
    Ok(())
}

fn launch_remote_exec(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
    target_name: &str,
    target_addr: &str,
    label: &str,
    remote_command: &str,
) -> Result<()> {
    let mut owned_terminal = terminal.take().expect("terminal initialized");
    restore_terminal(&mut owned_terminal)?;
    drop(owned_terminal);
    stdout().flush()?;
    let status = Command::new("ssh")
        .args([
            "-tt",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            target_addr,
            &format!("bash -lc {}", shell_quote(remote_command)),
        ])
        .status();
    let mut new_terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    enter_terminal(&mut new_terminal)?;
    *terminal = Some(new_terminal);

    match status {
        Ok(status) if status.success() => {
            state.notice = Some(format!("{label} closed on {target_name}"));
        }
        Ok(status) => {
            state.notice = Some(format!(
                "{label} exited with status {} on {}",
                status, target_name
            ));
        }
        Err(error) => {
            state.notice = Some(format!("{label} launch failed on {target_name}: {error}"));
        }
    }
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
