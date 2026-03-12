mod collect;
mod model;
mod service;
mod ui;

use std::io::Write;
use std::io::stdout;
use std::os::unix::io::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use collect::{Args, CollectorCommand};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use glob::glob;
use model::{AppState, FocusPane, PopupKind};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use service::{
    UiUpdate, ensure_collector_service, run_collector_server, spawn_collector_client,
    stop_collector_service,
};

fn main() -> Result<()> {
    let args = Args::parse();
    let mode_count = [args.collector_only, args.connect_only, args.stop_collector]
        .into_iter()
        .filter(|enabled| *enabled)
        .count();
    if mode_count > 1 {
        bail!("--collector-only, --connect-only, and --stop-collector are mutually exclusive");
    }
    if args.collector_only {
        return run_collector_server(args);
    }
    if args.stop_collector {
        return stop_collector_service(&args);
    }

    let connection = ensure_collector_service(&args)?;
    let refresh_every = Duration::from_millis(connection.welcome.config.refresh_ms);
    let collector_endpoint = format!("{}:{}", args.collector_host, args.collector_port);
    let collector_mode = if args.connect_only {
        "connect".to_string()
    } else {
        "shared".to_string()
    };
    let initial_notice = connection.mismatch_warning.or_else(|| {
        if connection.welcome.started_collector {
            Some(format!(
                "started collector on {}:{}",
                args.collector_host, args.collector_port
            ))
        } else {
            Some(format!(
                "connected to collector on {}:{}",
                args.collector_host, args.collector_port
            ))
        }
    });

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Some(Terminal::new(backend)?);
    terminal.as_mut().expect("terminal").clear()?;

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (notice_tx, notice_rx) = mpsc::channel();
    let (command_tx, command_rx) = mpsc::channel();
    let collector_client = spawn_collector_client(args.clone(), snapshot_tx, notice_tx, command_rx);

    let result = run_app(
        &mut terminal,
        &snapshot_rx,
        &notice_rx,
        &command_tx,
        refresh_every,
        args.active_only,
        args.custom_tool_command.clone(),
        collector_endpoint,
        collector_mode,
        initial_notice,
    );

    let _ = command_tx.send(CollectorCommand::Quit);
    let _ = collector_client.join();
    if let Some(mut terminal) = terminal {
        restore_terminal(&mut terminal)?;
    }
    result
}

fn run_app(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    snapshot_rx: &mpsc::Receiver<UiUpdate>,
    notice_rx: &mpsc::Receiver<String>,
    command_tx: &mpsc::Sender<CollectorCommand>,
    refresh_every: Duration,
    active_only: bool,
    custom_tool_command: Option<String>,
    collector_endpoint: String,
    collector_mode: String,
    initial_notice: Option<String>,
) -> Result<()> {
    let mut state = AppState::new(
        refresh_every,
        active_only,
        custom_tool_command,
        collector_endpoint,
        collector_mode,
    );
    state.notice = initial_notice;

    loop {
        while let Ok(update) = snapshot_rx.try_recv() {
            state.gpu_tracker = update.tracker;
            state.latest = Some(update.snapshot);
        }
        while let Ok(notice) = notice_rx.try_recv() {
            state.notice = Some(notice);
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
                    KeyCode::Char('l') => {
                        launch_selected_job_logs(terminal, &mut state)?;
                    }
                    KeyCode::Char('c') => {
                        prompt_cancel_selected_job(&mut state)?;
                    }
                    KeyCode::Enter => match state.focus {
                        FocusPane::Jobs => {
                            drill_into_job_nodes(&mut state);
                        }
                        FocusPane::Nodes => {
                            launch_remote_shell(terminal, &mut state)?;
                        }
                    },
                    KeyCode::Esc => {
                        if state.job_node_filter.is_some() {
                            state.job_node_filter = None;
                            state.focus = FocusPane::Jobs;
                            state.notice = Some("back to jobs".into());
                        }
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
                        state.job_node_filter = None;
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

fn drill_into_job_nodes(state: &mut AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return;
    };
    let Some(job) = ui::selected_job_for_drill(snapshot, state) else {
        state.notice = Some("no job selected".into());
        return;
    };
    let hosts = ui::job_node_names(&job);
    if hosts.is_empty() {
        state.notice = Some(format!("job {} has no assigned nodes", job.id));
        return;
    }
    state.notice = Some(format!(
        "job {} → {} node(s)  Esc=back",
        job.id,
        hosts.len()
    ));
    state.job_node_filter = Some(hosts);
    state.focus = FocusPane::Nodes;
    state.selected_node = 0;
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

fn drain_pending_events() -> Result<()> {
    while event::poll(Duration::from_millis(0))? {
        let _ = event::read()?;
    }
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
    let job_id = if matches!(state.focus, FocusPane::Jobs) || state.job_node_filter.is_some() {
        ui::selected_job_id(snapshot, state).unwrap_or_default()
    } else {
        String::new()
    };
    let remote_command = format!(
        "export NODE_NAME={} JOB_ID={}; {}",
        shell_quote(&target.name),
        shell_quote(&job_id),
        command
    );

    launch_remote_exec(
        terminal,
        state,
        &target.name,
        &target.addr,
        "run command",
        &remote_command,
    )
}

fn launch_selected_job_logs(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
) -> Result<()> {
    let Some(snapshot) = state.latest.as_ref() else {
        state.notice = Some("no cluster snapshot yet".into());
        return Ok(());
    };
    if !matches!(state.focus, FocusPane::Jobs) && state.job_node_filter.is_none() {
        state.notice = Some("logs only work from a selected job".into());
        return Ok(());
    }
    let Some(job_id) = ui::selected_job_id(snapshot, state) else {
        state.notice = Some("no job selected".into());
        return Ok(());
    };
    let node_name = ui::selected_target_for_launch(snapshot, state)
        .map(|target| target.name)
        .unwrap_or_default();
    let paths = resolve_job_log_paths(&job_id, &node_name)?;
    if paths.is_empty() {
        state.notice = Some(format!("no StdOut/StdErr path found for job {job_id}"));
        return Ok(());
    }

    launch_local_exec(
        terminal,
        state,
        &format!("logs for job {job_id}"),
        "tail",
        std::iter::once("-F".to_string()).chain(paths).collect(),
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

fn resolve_job_log_paths(job_id: &str, node_name: &str) -> Result<Vec<String>> {
    let scontrol = Command::new("scontrol")
        .args(["show", "job", "-o", job_id])
        .output();
    if let Ok(output) = scontrol {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(paths) = parse_job_log_paths_from_scontrol(&stdout, job_id, node_name) {
                if !paths.is_empty() {
                    return Ok(paths);
                }
            }
        }
    }

    let sacct = Command::new("sacct")
        .args([
            "-X",
            "-n",
            "-P",
            "-j",
            job_id,
            "-o",
            "JobIDRaw,StdOut,StdErr",
        ])
        .output();
    if let Ok(output) = sacct {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let paths = parse_job_log_paths_from_sacct(&stdout, job_id, node_name);
            if !paths.is_empty() {
                return Ok(paths);
            }
        }
    }

    Ok(Vec::new())
}

fn parse_job_log_paths_from_scontrol(
    output: &str,
    job_id: &str,
    node_name: &str,
) -> Option<Vec<String>> {
    let line = output.lines().find(|line| !line.trim().is_empty())?;
    let fields = parse_slurm_kv_line(line);
    let job_name = fields.get("JobName").cloned().unwrap_or_default();
    let user_name = fields
        .get("UserId")
        .map(|value| value.split('(').next().unwrap_or_default().to_string())
        .unwrap_or_default();
    let batch_host = fields.get("BatchHost").cloned().unwrap_or_default();
    let node_name = if node_name.is_empty() {
        batch_host.as_str()
    } else {
        node_name
    };
    Some(unique_log_paths([
        fields
            .get("StdOut")
            .map(|path| resolve_slurm_log_path(path, job_id, &job_name, &user_name, node_name)),
        fields
            .get("StdErr")
            .map(|path| resolve_slurm_log_path(path, job_id, &job_name, &user_name, node_name)),
    ]))
}

fn parse_job_log_paths_from_sacct(output: &str, job_id: &str, node_name: &str) -> Vec<String> {
    let mut best = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let columns: Vec<_> = line.split('|').collect();
        if columns.len() < 3 {
            continue;
        }
        if columns[0] != job_id && !columns[0].starts_with(&format!("{job_id}.")) {
            continue;
        }
        let paths = unique_log_paths([
            Some(resolve_slurm_log_path(
                columns[1], job_id, "", "", node_name,
            )),
            Some(resolve_slurm_log_path(
                columns[2], job_id, "", "", node_name,
            )),
        ]);
        if !paths.is_empty() {
            best = paths;
            if columns[0] == job_id {
                break;
            }
        }
    }
    best
}

fn unique_log_paths(paths: [Option<String>; 2]) -> Vec<String> {
    let mut unique = Vec::new();
    for path in paths.into_iter().flatten() {
        let trimmed = path.trim();
        if trimmed.is_empty()
            || trimmed == "(null)"
            || trimmed.eq_ignore_ascii_case("none")
            || trimmed == "/dev/null"
        {
            continue;
        }
        let expanded = expand_slurm_log_path(trimmed);
        let candidates = if expanded.is_empty() && !trimmed.contains('%') {
            vec![trimmed.to_string()]
        } else {
            expanded
        };
        for candidate in candidates {
            if unique.iter().any(|existing| existing == &candidate) {
                continue;
            }
            unique.push(candidate);
        }
    }
    unique
}

fn resolve_slurm_log_path(
    path: &str,
    job_id: &str,
    job_name: &str,
    user_name: &str,
    node_name: &str,
) -> String {
    let (array_job_id, array_task_id) = job_id
        .split_once('_')
        .map(|(left, right)| (left, right))
        .unwrap_or((job_id, "0"));
    path.replace("%j", job_id)
        .replace("%J", job_id)
        .replace("%A", array_job_id)
        .replace("%a", array_task_id)
        .replace("%x", job_name)
        .replace("%u", user_name)
        .replace("%N", node_name)
        .replace("%n", "0")
        .replace("%%", "%")
}

fn expand_slurm_log_path(path: &str) -> Vec<String> {
    if !path.contains('%') {
        return Vec::new();
    }

    let pattern = slurm_template_to_glob(path);
    let mut matches = Vec::new();
    if let Ok(paths) = glob(&pattern) {
        for candidate in paths.flatten() {
            if let Some(path) = candidate.to_str() {
                matches.push(path.to_string());
            }
        }
    }
    matches.sort();
    matches
}

fn slurm_template_to_glob(path: &str) -> String {
    let mut pattern = String::with_capacity(path.len());
    let mut chars = path.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            match chars.next() {
                Some('%') => pattern.push('%'),
                Some(_) => pattern.push('*'),
                None => pattern.push('%'),
            }
            continue;
        }

        match ch {
            '*' | '?' | '[' | ']' | '{' | '}' => {
                pattern.push('\\');
                pattern.push(ch);
            }
            _ => pattern.push(ch),
        }
    }
    pattern
}

fn parse_slurm_kv_line(line: &str) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    let mut current_key: Option<String> = None;
    for token in line.split_whitespace() {
        if let Some((key, value)) = token.split_once('=') {
            current_key = Some(key.to_string());
            map.insert(key.to_string(), value.to_string());
        } else if let Some(key) = current_key.as_ref() {
            if let Some(existing) = map.get_mut(key) {
                existing.push(' ');
                existing.push_str(token);
            }
        }
    }
    map
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
    drain_pending_events()?;
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

fn launch_local_exec(
    terminal: &mut Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
    state: &mut AppState,
    label: &str,
    program: &str,
    args: Vec<String>,
) -> Result<()> {
    let mut owned_terminal = terminal.take().expect("terminal initialized");
    restore_terminal(&mut owned_terminal)?;
    drop(owned_terminal);
    stdout().flush()?;
    let stdin = std::io::stdin();
    let tty_fd = stdin.as_raw_fd();
    let mut child = spawn_foreground_child(program, &args)?;
    set_foreground_pgrp(tty_fd, child.id() as libc::pid_t)?;
    let status = child.wait();
    let _ = set_foreground_pgrp(tty_fd, current_pgrp());
    let mut new_terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    enter_terminal(&mut new_terminal)?;
    drain_pending_events()?;
    *terminal = Some(new_terminal);

    match status {
        Ok(status) if status.success() => {
            state.notice = Some(format!("{label} closed"));
        }
        Ok(status) => {
            state.notice = Some(format!("{label} exited with status {status}"));
        }
        Err(error) => {
            state.notice = Some(format!("{label} launch failed: {error}"));
        }
    }
    Ok(())
}

fn spawn_foreground_child(program: &str, args: &[String]) -> Result<Child> {
    let mut command = Command::new(program);
    command.args(args);
    // Put the viewer in its own process group so terminal signals only hit it.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(command.spawn()?)
}

fn current_pgrp() -> libc::pid_t {
    unsafe { libc::getpgrp() }
}

fn set_foreground_pgrp(tty_fd: i32, pgrp: libc::pid_t) -> Result<()> {
    unsafe {
        let previous = libc::signal(libc::SIGTTOU, libc::SIG_IGN);
        let rc = libc::tcsetpgrp(tty_fd, pgrp);
        libc::signal(libc::SIGTTOU, previous);
        if rc != 0 {
            return Err(std::io::Error::last_os_error().into());
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
    drain_pending_events()?;
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
