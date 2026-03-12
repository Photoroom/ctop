use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, Gauge, Paragraph, Row, Table, Wrap,
};
use ratatui::{Frame, layout::Alignment};

use crate::model::{
    AppState, ClusterSnapshot, FilesystemUsage, FocusPane, JobSummary, NodeSnapshot, PopupKind,
    SortMode,
};

const BG: Color = Color::Rgb(11, 17, 24);
const PANEL: Color = Color::Rgb(16, 26, 37);
const TEAL: Color = Color::Rgb(73, 214, 193);
const SKY: Color = Color::Rgb(107, 170, 255);
const GOLD: Color = Color::Rgb(255, 188, 92);
const ROSE: Color = Color::Rgb(255, 111, 145);
const MUTED: Color = Color::Rgb(148, 167, 181);
const TEXT: Color = Color::Rgb(225, 232, 239);

#[derive(Clone, Debug)]
pub struct LaunchTarget {
    pub name: String,
    pub addr: String,
}

pub fn draw(frame: &mut Frame, state: &AppState) {
    frame.render_widget(
        Block::default().style(Style::default().bg(BG)),
        frame.area(),
    );

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Min(14),
            Constraint::Length(2),
        ])
        .split(frame.area());

    draw_header(frame, layout[0], state);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(71), Constraint::Percentage(29)])
        .split(layout[1]);
    let tables = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(body[0]);
    draw_nodes(frame, tables[0], state);
    draw_jobs(frame, tables[1], state);
    draw_selected(frame, body[1], state);
    draw_footer(frame, layout[2], state);
    draw_popup(frame, state);
}

fn draw_header(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        let empty = Paragraph::new("Waiting for first cluster sample...")
            .style(Style::default().fg(TEXT).bg(PANEL))
            .block(panel("ctop", true));
        frame.render_widget(empty, area);
        return;
    };

    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(17),
            Constraint::Percentage(17),
            Constraint::Percentage(17),
            Constraint::Percentage(17),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
        ])
        .split(area);

    let summary = &snapshot.summary;
    let cpu_alloc_pct = ratio(summary.cpu_alloc, summary.cpu_total);
    let mem_used_pct = summary
        .mem_used_mb
        .map(|used| ratio(used, summary.mem_total_mb))
        .unwrap_or(0.0);
    let gpu_alloc_pct = ratio(summary.gpu_alloc, summary.gpu_total);
    let gpu_util_pct = summary.gpu_util_pct.unwrap_or(0.0);

    let cards = [
        metric_gauge(
            "Cluster CPU",
            summary.cpu_busy_pct.unwrap_or(cpu_alloc_pct),
            format!(
                "{:.0}% busy  {} / {} alloc",
                summary.cpu_busy_pct.unwrap_or(cpu_alloc_pct),
                summary.cpu_alloc,
                summary.cpu_total
            ),
            SKY,
        ),
        metric_gauge(
            "Memory",
            mem_used_pct,
            format!(
                "{} / {} used",
                format_bytes(summary.mem_used_mb.unwrap_or(0) * 1024 * 1024),
                format_bytes(summary.mem_total_mb * 1024 * 1024)
            ),
            TEAL,
        ),
        metric_gauge(
            "GPU Alloc",
            gpu_alloc_pct,
            format!("{} / {} alloc", summary.gpu_alloc, summary.gpu_total),
            GOLD,
        ),
        metric_gauge(
            "GPU Util",
            gpu_util_pct,
            format!(
                "{:.0}% util  {} / {} mem",
                gpu_util_pct,
                format_bytes(summary.gpu_mem_used_mb * 1024 * 1024),
                format_bytes(summary.gpu_mem_total_mb * 1024 * 1024)
            ),
            ROSE,
        ),
        metric_text(
            "Fabric",
            vec![
                Line::from(vec![
                    "RX ".fg(MUTED),
                    format_bytes_rate(summary.net_rx_bps.unwrap_or(0.0)).fg(TEXT),
                ]),
                Line::from(vec![
                    "TX ".fg(MUTED),
                    format_bytes_rate(summary.net_tx_bps.unwrap_or(0.0)).fg(TEXT),
                ]),
            ],
        ),
        metric_text(
            "Disk",
            vec![
                disk_header_line(summary.home_usage.as_ref(), "home"),
                disk_header_line(summary.data_usage.as_ref(), "data"),
                Line::from(vec![
                    "Nodes ".fg(MUTED),
                    format!(
                        "up {}  act {}  samp {}",
                        summary.node_total.saturating_sub(summary.node_down),
                        summary.node_active,
                        summary.sampled_nodes
                    )
                    .fg(TEXT),
                ]),
            ],
        ),
    ];

    for (area, card) in panels.iter().copied().zip(cards) {
        match card {
            HeaderCard::Gauge(widget) => frame.render_widget(widget, area),
            HeaderCard::Text(widget) => frame.render_widget(widget, area),
        }
    }
}

enum HeaderCard<'a> {
    Gauge(Gauge<'a>),
    Text(Paragraph<'a>),
}

fn metric_gauge<'a>(title: &'a str, percent: f64, label: String, color: Color) -> HeaderCard<'a> {
    let gauge = Gauge::default()
        .block(panel(title, false))
        .gauge_style(Style::default().fg(color).bg(PANEL))
        .style(Style::default().bg(PANEL))
        .percent(percent.clamp(0.0, 100.0).round() as u16)
        .label(Span::styled(
            label,
            Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
        ));
    HeaderCard::Gauge(gauge)
}

fn metric_text<'a>(title: &'a str, lines: Vec<Line<'a>>) -> HeaderCard<'a> {
    let widget = Paragraph::new(lines)
        .alignment(Alignment::Left)
        .style(Style::default().bg(PANEL))
        .block(panel(title, false));
    HeaderCard::Text(widget)
}

fn draw_nodes(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        return;
    };

    let nodes = visible_nodes(snapshot, state);
    let visible_rows = table_visible_rows(area);
    let (start, end) = visible_window(nodes.len(), state.selected_node, visible_rows);
    let header = Row::new(vec![
        "Node", "State", "CPU%", "A/T CPU", "Mem%", "GPU%", "A/T GPU", "Net",
    ])
    .style(
        Style::default()
            .fg(MUTED)
            .bg(PANEL)
            .add_modifier(Modifier::BOLD),
    )
    .height(1);

    let rows = nodes[start..end].iter().enumerate().map(|(offset, node)| {
        let index = start + offset;
        let selected = index == state.selected_node.min(nodes.len().saturating_sub(1));
        let style = if selected {
            Style::default().fg(TEXT).bg(Color::Rgb(27, 41, 57))
        } else {
            Style::default().fg(TEXT).bg(PANEL)
        };
        Row::new(vec![
            Cell::from(node.name.clone()),
            Cell::from(state_badge(node)),
            Cell::from(percent_label(node.cpu_busy_pct)),
            Cell::from(format!("{}/{}", node.cpu_alloc, node.cpu_total)),
            Cell::from(percent_label(node.mem_used_pct())),
            Cell::from(percent_label(node.gpu_util_avg())),
            Cell::from(format!("{}/{}", node.gpu_alloc, node.gpu_total)),
            Cell::from(flow_label(node.net_rx_bps, node.net_tx_bps)),
        ])
        .style(style)
        .height(1)
    });

    let nodes_title = format!(
        "nodes {}",
        if matches!(state.focus, FocusPane::Nodes) {
            "active"
        } else {
            ""
        }
    );
    let nodes_title = filtered_title(nodes_title.trim_end(), state);
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .block(panel(&nodes_title, matches!(state.focus, FocusPane::Nodes)))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn draw_jobs(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        return;
    };

    let jobs = filtered_jobs(snapshot, state);
    let visible_rows = table_visible_rows(area);
    let (start, end) = visible_window(jobs.len(), state.selected_job, visible_rows);
    let header = Row::new(vec![
        "Job", "User", "State", "Where", "Elapsed", "Nodes", "CPUs", "GRES", "NodeList",
    ])
    .style(
        Style::default()
            .fg(MUTED)
            .bg(PANEL)
            .add_modifier(Modifier::BOLD),
    );

    let rows = jobs[start..end].iter().enumerate().map(|(offset, job)| {
        let index = start + offset;
        let selected = index == state.selected_job.min(jobs.len().saturating_sub(1));
        let style = if selected {
            Style::default().fg(TEXT).bg(Color::Rgb(27, 41, 57))
        } else {
            Style::default().fg(TEXT).bg(PANEL)
        };
        Row::new(vec![
            Cell::from(job.id.clone()),
            Cell::from(job.user.clone()),
            Cell::from(job.state.clone()),
            Cell::from(job.location.clone()),
            Cell::from(job.elapsed.clone()),
            Cell::from(job.nodes.to_string()),
            Cell::from(job.cpus.to_string()),
            Cell::from(job.gres.clone()),
            Cell::from(job.node_list.clone()),
        ])
        .style(style)
    });

    let jobs_title = job_title(jobs.len(), start, end, state);
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(14),
            Constraint::Min(12),
        ],
    )
    .header(header)
    .block(panel(&jobs_title, matches!(state.focus, FocusPane::Jobs)))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn draw_selected(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        return;
    };

    let detail = selected_detail_lines(snapshot, state);
    let detail_widget = Paragraph::new(detail)
        .wrap(Wrap { trim: false })
        .style(Style::default().fg(TEXT).bg(PANEL))
        .block(panel("selected node", true));
    frame.render_widget(detail_widget, area);
}

fn draw_footer(frame: &mut Frame, area: Rect, state: &AppState) {
    if let Some(input) = state.filter_input.as_ref() {
        let prompt = Line::from(vec![
            Span::styled(
                " user ",
                Style::default()
                    .fg(BG)
                    .bg(ROSE)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{input}_"),
                Style::default()
                    .fg(TEXT)
                    .bg(BG)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                " Enter ",
                Style::default()
                    .fg(BG)
                    .bg(TEAL)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("apply ", Style::default().fg(MUTED).bg(BG)),
            Span::styled(
                " Esc ",
                Style::default().fg(BG).bg(SKY).add_modifier(Modifier::BOLD),
            ),
            Span::styled("cancel", Style::default().fg(MUTED).bg(BG)),
        ]);
        let footer = Paragraph::new(prompt).alignment(Alignment::Left);
        frame.render_widget(footer, area);
        return;
    }

    let footer_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);

    let mut top_spans = vec![
        Span::styled(
            " q ",
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("quit ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " Tab ",
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("pane ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " ↑↓/jk ",
            Style::default()
                .fg(BG)
                .bg(Color::Rgb(120, 134, 205))
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("move ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " Enter ",
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("ssh ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " t ",
            Style::default()
                .fg(BG)
                .bg(GOLD)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("tools ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " r ",
            Style::default()
                .fg(BG)
                .bg(GOLD)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("run ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " c ",
            Style::default()
                .fg(BG)
                .bg(ROSE)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("cancel ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " ? ",
            Style::default()
                .fg(BG)
                .bg(ROSE)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("help ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " u ",
            Style::default()
                .fg(BG)
                .bg(ROSE)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("user ", Style::default().fg(MUTED).bg(BG)),
        Span::styled(
            " m ",
            Style::default().fg(BG).bg(SKY).add_modifier(Modifier::BOLD),
        ),
        Span::styled("mine", Style::default().fg(MUTED).bg(BG)),
    ];

    let mut status_spans = Vec::new();
    if let Some(snapshot) = state.latest.as_ref() {
        status_spans.push(Span::styled(
            format!(
                "sort={} {}  refresh={}ms  age={}ms",
                state.sort_mode.label(),
                if state.descending { "desc" } else { "asc" },
                state.refresh_every.as_millis(),
                Instant::now()
                    .saturating_duration_since(snapshot.collected_at)
                    .as_millis()
            ),
            Style::default().fg(TEXT).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!("focus={}", state.focus.label()),
            Style::default().fg(GOLD).bg(BG),
        ));
        if let Some(filter) = state.user_filter.as_ref() {
            status_spans.push(Span::raw("   "));
            status_spans.push(Span::styled(
                format!("user={filter}"),
                Style::default().fg(TEAL).bg(BG),
            ));
        }
        if !snapshot.errors.is_empty() {
            status_spans.push(Span::raw("   "));
            status_spans.push(Span::styled(
                snapshot.errors.join(" | "),
                Style::default().fg(ROSE).bg(BG),
            ));
        }
    }
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(0)])
        .split(footer_rows[0]);
    if !status_spans.is_empty() {
        top_spans.push(Span::raw("   "));
        top_spans.extend(status_spans);
    }
    frame.render_widget(
        Paragraph::new(Line::from(top_spans)).alignment(Alignment::Left),
        top[0],
    );

    let notice = state.notice.as_ref().map(|notice| {
        Paragraph::new(Line::from(vec![Span::styled(
            notice.clone(),
            Style::default().fg(SKY).bg(BG),
        )]))
        .alignment(Alignment::Right)
    });
    if let Some(notice) = notice {
        frame.render_widget(notice, footer_rows[1]);
    }
}

fn draw_popup(frame: &mut Frame, state: &AppState) {
    match state.popup {
        Some(PopupKind::Tools) => draw_tools_popup(frame, state),
        Some(PopupKind::Help) => draw_help_popup(frame),
        Some(PopupKind::CancelJobConfirm) => draw_cancel_popup(frame, state),
        None => {}
    }
}

fn draw_tools_popup(frame: &mut Frame, state: &AppState) {
    let area = centered_rect(72, 12, frame.area());
    let target = state
        .latest
        .as_ref()
        .and_then(|snapshot| selected_target_for_launch(snapshot, state).ok())
        .map(|target| target.name)
        .unwrap_or_else(|| "no target".into());
    let tools = [
        ("h", "htop", "cpu, memory, processes"),
        ("b", "btop", "system overview"),
        ("n", "nvtop", "gpu view"),
        (
            "r",
            "run",
            state
                .custom_tool_command
                .as_deref()
                .unwrap_or("not configured"),
        ),
    ];
    let mut lines = vec![
        Line::from(vec!["target ".fg(MUTED), target.fg(TEXT).bold()]),
        Line::from(""),
    ];
    for (index, (key, name, description)) in tools.iter().enumerate() {
        let selected = index == state.selected_tool.min(tools.len().saturating_sub(1));
        let marker = if selected { ">" } else { " " };
        lines.push(Line::from(vec![
            marker.fg(GOLD),
            " ".into(),
            format!("[{key}]").fg(TEAL),
            " ".into(),
            (*name).fg(TEXT).bold(),
            "  ".into(),
            (*description).fg(MUTED),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        "Enter".fg(TEAL).bold(),
        " launch   ".into(),
        "Esc".fg(SKY).bold(),
        " close   ".into(),
        "j/k".fg(GOLD).bold(),
        " move".fg(MUTED),
    ]));

    frame.render_widget(Clear, area);
    let popup = Paragraph::new(lines)
        .block(panel("tools", true))
        .style(Style::default().fg(TEXT).bg(PANEL))
        .alignment(Alignment::Left);
    frame.render_widget(popup, area);
}

fn draw_help_popup(frame: &mut Frame) {
    let area = centered_rect(90, 24, frame.area());
    let lines = vec![
        help_section("Navigate"),
        help_entry(&[("tab", TEAL)], "switch between nodes and jobs"),
        help_entry(
            &[("up/down", TEAL), ("j/k", TEAL)],
            "move the active selection",
        ),
        help_entry(&[("pgup/pgdn", TEAL)], "jump through the focused table"),
        help_entry(&[("home/end", TEAL)], "jump to the first or last row"),
        help_entry(
            &[("enter", TEAL)],
            "ssh into the selected node or the selected job's node",
        ),
        Line::from(""),
        help_section("Tools"),
        help_entry(
            &[("h", GOLD), ("b", GOLD), ("n", GOLD)],
            "launch htop, btop, or nvtop on the selected node",
        ),
        help_entry(&[("r", GOLD)], "run the configured custom command"),
        help_entry(&[("t", GOLD)], "open the tools popup"),
        Line::from(""),
        help_section("Jobs"),
        help_entry(&[("c", ROSE)], "cancel the selected job from the jobs pane"),
        help_entry(
            &[("y", TEAL), ("n", ROSE)],
            "confirm or reject the cancel dialog",
        ),
        help_entry(&[("R", GOLD)], "refresh now"),
        Line::from(""),
        help_section("View"),
        help_entry(&[("s", SKY)], "cycle the node sort key"),
        help_entry(&[("S", SKY)], "flip sort direction"),
        help_entry(&[("a", SKY)], "toggle active-only nodes"),
        help_entry(&[("u", SKY)], "edit the username filter"),
        help_entry(&[("m", SKY)], "toggle your own username filter"),
        Line::from(""),
        help_section("General"),
        help_entry(&[("?", ROSE)], "open or close this help dialog"),
        help_entry(&[("esc", ROSE)], "close popups or cancel inline input"),
        help_entry(&[("q", ROSE), ("ctrl-c", ROSE)], "quit ctop"),
    ];

    frame.render_widget(Clear, area);
    let popup = Paragraph::new(lines)
        .block(panel("help", true))
        .style(Style::default().fg(TEXT).bg(PANEL))
        .wrap(Wrap { trim: false });
    frame.render_widget(popup, area);
}

fn help_section(title: &'static str) -> Line<'static> {
    Line::from(vec![Span::styled(
        format!(" {title} "),
        Style::default()
            .fg(BG)
            .bg(Color::Rgb(90, 108, 125))
            .add_modifier(Modifier::BOLD),
    )])
}

fn help_entry(keys: &[(&'static str, Color)], description: &'static str) -> Line<'static> {
    let mut spans = Vec::new();
    spans.push(" ".into());
    let mut key_width = 1usize;
    for (index, (key, color)) in keys.iter().enumerate() {
        if index > 0 {
            spans.push(" ".into());
            key_width += 1;
        }
        spans.push(Span::styled(
            format!(" <{key}> "),
            Style::default()
                .fg(BG)
                .bg(*color)
                .add_modifier(Modifier::BOLD),
        ));
        key_width += key.len() + 4;
    }
    spans.push(" ".repeat(26usize.saturating_sub(key_width)).into());
    spans.push(description.fg(MUTED));
    Line::from(spans)
}

fn draw_cancel_popup(frame: &mut Frame, state: &AppState) {
    let area = centered_rect(60, 6, frame.area());
    let job_id = state.pending_cancel_job.as_deref().unwrap_or("?");
    let lines = vec![
        Line::from(format!("Do you really want to cancel job {job_id}?")),
        Line::from(""),
        Line::from(vec![
            "<y> ".fg(TEAL).bold(),
            "yes(y)".fg(MUTED),
            "   ".into(),
            "<n> ".fg(ROSE).bold(),
            "no(n)".fg(MUTED),
        ]),
    ];

    frame.render_widget(Clear, area);
    let popup = Paragraph::new(lines)
        .block(panel("cancel job", true))
        .style(Style::default().fg(TEXT).bg(PANEL))
        .alignment(Alignment::Center);
    frame.render_widget(popup, area);
}

fn visible_nodes<'a>(snapshot: &'a ClusterSnapshot, state: &AppState) -> Vec<&'a NodeSnapshot> {
    let filtered_jobs = filtered_jobs(snapshot, state);
    let visible_names = if state.user_filter.is_some() {
        let names: BTreeSet<_> = filtered_jobs
            .iter()
            .flat_map(|job| job_hosts(job))
            .collect();
        Some(names)
    } else {
        None
    };
    let mut nodes: Vec<_> = snapshot
        .nodes
        .iter()
        .filter(|node| {
            (!state.show_active_only || node.is_active())
                && visible_names
                    .as_ref()
                    .is_none_or(|names| names.contains(node.name.as_str()))
        })
        .collect();
    nodes.sort_by(|left, right| compare_nodes(left, right, state.sort_mode, state.descending));
    nodes
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(height.min(area.height)),
            Constraint::Fill(1),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(width.min(area.width)),
            Constraint::Fill(1),
        ])
        .split(vertical[1])[1]
}

fn filtered_jobs<'a>(snapshot: &'a ClusterSnapshot, state: &AppState) -> Vec<&'a JobSummary> {
    snapshot
        .jobs
        .iter()
        .filter(|job| {
            state
                .user_filter
                .as_ref()
                .is_none_or(|filter| job.user == *filter)
        })
        .collect()
}

fn selected_node<'a>(snapshot: &'a ClusterSnapshot, state: &AppState) -> Option<&'a NodeSnapshot> {
    let nodes = visible_nodes(snapshot, state);
    nodes
        .get(state.selected_node.min(nodes.len().saturating_sub(1)))
        .copied()
}

fn selected_detail_node<'a>(
    snapshot: &'a ClusterSnapshot,
    state: &AppState,
) -> Option<&'a NodeSnapshot> {
    match state.focus {
        FocusPane::Nodes => selected_node(snapshot, state),
        FocusPane::Jobs => {
            let job = selected_job(snapshot, state)?;
            let host = job_hosts(job).into_iter().next()?;
            snapshot.nodes.iter().find(|node| node.name == host)
        }
    }
}

fn selected_job<'a>(snapshot: &'a ClusterSnapshot, state: &AppState) -> Option<&'a JobSummary> {
    let jobs = filtered_jobs(snapshot, state);
    jobs.get(state.selected_job.min(jobs.len().saturating_sub(1)))
        .copied()
}

pub fn selected_job_id(snapshot: &ClusterSnapshot, state: &AppState) -> Option<String> {
    selected_job(snapshot, state).map(|job| job.id.clone())
}

pub fn selected_target_for_launch(
    snapshot: &ClusterSnapshot,
    state: &AppState,
) -> Result<LaunchTarget, String> {
    match state.focus {
        FocusPane::Nodes => selected_node(snapshot, state)
            .map(|node| LaunchTarget {
                name: node.name.clone(),
                addr: node.addr.clone(),
            })
            .ok_or_else(|| "no node selected".into()),
        FocusPane::Jobs => {
            let job = selected_job(snapshot, state).ok_or_else(|| "no job selected".to_string())?;
            let host = job_hosts(job)
                .into_iter()
                .next()
                .ok_or_else(|| format!("job {} has no assigned node", job.id))?;
            let addr = snapshot
                .nodes
                .iter()
                .find(|node| node.name == host)
                .map(|node| node.addr.clone())
                .unwrap_or_else(|| host.clone());
            Ok(LaunchTarget { name: host, addr })
        }
    }
}

pub fn visible_node_count(snapshot: &ClusterSnapshot, state: &AppState) -> usize {
    visible_nodes(snapshot, state).len()
}

pub fn visible_job_count(snapshot: &ClusterSnapshot, state: &AppState) -> usize {
    filtered_jobs(snapshot, state).len()
}

fn table_visible_rows(area: Rect) -> usize {
    area.height.saturating_sub(3) as usize
}

fn visible_window(total: usize, selected: usize, visible_rows: usize) -> (usize, usize) {
    if total == 0 || visible_rows == 0 {
        return (0, 0);
    }
    if total <= visible_rows {
        return (0, total);
    }
    let selected = selected.min(total.saturating_sub(1));
    let half = visible_rows / 2;
    let mut start = selected.saturating_sub(half);
    if start + visible_rows > total {
        start = total.saturating_sub(visible_rows);
    }
    (start, (start + visible_rows).min(total))
}

fn job_title(total: usize, start: usize, end: usize, state: &AppState) -> String {
    if total == 0 {
        return filtered_title("jobs", state);
    }
    filtered_title(
        &format!(
            "jobs {}-{} / {}{}",
            start + 1,
            end,
            total,
            if matches!(state.focus, FocusPane::Jobs) {
                " active"
            } else {
                ""
            }
        ),
        state,
    )
}

fn filtered_title(base: &str, state: &AppState) -> String {
    match state.user_filter.as_ref() {
        Some(filter) => format!("{base} [{filter}]"),
        None => base.into(),
    }
}

fn job_hosts(job: &JobSummary) -> Vec<String> {
    let raw = if job.node_list.trim().is_empty()
        || job.node_list == "(null)"
        || job.node_list == "n/a"
        || job.node_list == "N/A"
    {
        if job.location.starts_with('(') {
            ""
        } else {
            job.location.as_str()
        }
    } else {
        job.node_list.as_str()
    };
    expand_nodelist(raw)
}

fn expand_nodelist(input: &str) -> Vec<String> {
    split_top_level(input.trim(), ',')
        .into_iter()
        .flat_map(expand_component)
        .filter(|value| !value.is_empty())
        .collect()
}

fn expand_component(component: &str) -> Vec<String> {
    let component = component.trim();
    if component.is_empty() {
        return Vec::new();
    }
    let Some(open) = component.find('[') else {
        return vec![component.to_string()];
    };
    let Some(close) = component[open + 1..].find(']') else {
        return vec![component.to_string()];
    };
    let close = open + 1 + close;
    let prefix = &component[..open];
    let inside = &component[open + 1..close];
    let suffix = &component[close + 1..];
    let suffixes = if suffix.is_empty() {
        vec![String::new()]
    } else {
        expand_component(suffix)
    };
    let mut expanded = Vec::new();
    for segment in split_top_level(inside, ',') {
        for piece in expand_range(segment) {
            for tail in &suffixes {
                expanded.push(format!("{prefix}{piece}{tail}"));
            }
        }
    }
    expanded
}

fn expand_range(segment: &str) -> Vec<String> {
    let segment = segment.trim();
    let Some((start, end)) = segment.split_once('-') else {
        return vec![segment.to_string()];
    };
    if let (Ok(start_num), Ok(end_num)) = (start.parse::<u32>(), end.parse::<u32>()) {
        let width = start.len().max(end.len());
        if start_num <= end_num {
            return (start_num..=end_num)
                .map(|value| format!("{value:0width$}"))
                .collect();
        }
        return (end_num..=start_num)
            .rev()
            .map(|value| format!("{value:0width$}"))
            .collect();
    }
    vec![segment.to_string()]
}

fn split_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, ch) in input.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            _ if ch == delimiter && depth == 0 => {
                parts.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn compare_nodes(
    left: &NodeSnapshot,
    right: &NodeSnapshot,
    mode: SortMode,
    descending: bool,
) -> Ordering {
    let ordering = match mode {
        SortMode::Name => left.name.cmp(&right.name),
        SortMode::State => left.display_state().cmp(right.display_state()),
        SortMode::CpuBusy => ord_option(left.cpu_busy_pct, right.cpu_busy_pct),
        SortMode::CpuAlloc => left.cpu_alloc.cmp(&right.cpu_alloc),
        SortMode::Memory => ord_option(left.mem_used_pct(), right.mem_used_pct()),
        SortMode::GpuUtil => ord_option(left.gpu_util_avg(), right.gpu_util_avg()),
        SortMode::Network => ord_option(
            Some(left.net_rx_bps.unwrap_or(0.0) + left.net_tx_bps.unwrap_or(0.0)),
            Some(right.net_rx_bps.unwrap_or(0.0) + right.net_tx_bps.unwrap_or(0.0)),
        ),
        SortMode::Disk => Ordering::Equal,
    };
    let ordering = ordering.then_with(|| left.name.cmp(&right.name));
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn ord_option(left: Option<f64>, right: Option<f64>) -> Ordering {
    left.unwrap_or(-1.0)
        .partial_cmp(&right.unwrap_or(-1.0))
        .unwrap_or(Ordering::Equal)
}

fn selected_detail_lines(snapshot: &ClusterSnapshot, state: &AppState) -> Vec<Line<'static>> {
    let selected_job = matches!(state.focus, FocusPane::Jobs)
        .then(|| selected_job(snapshot, state))
        .flatten();
    let Some(node) = selected_detail_node(snapshot, state) else {
        return missing_selected_lines(snapshot, state, selected_job);
    };
    detail_lines(snapshot, node, selected_job)
}

fn missing_selected_lines(
    _snapshot: &ClusterSnapshot,
    state: &AppState,
    selected_job: Option<&JobSummary>,
) -> Vec<Line<'static>> {
    match (state.focus.clone(), selected_job) {
        (FocusPane::Nodes, _) => vec![Line::from("No nodes to display")],
        (FocusPane::Jobs, None) => vec![Line::from("No jobs to display")],
        (FocusPane::Jobs, Some(job)) => {
            let hosts = job_hosts(job);
            let assigned = hosts.first().cloned().unwrap_or_else(|| "pending".into());
            vec![
                Line::from(vec![
                    Span::styled(
                        format!("job {}", job.id),
                        Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("  {}", job.state),
                        Style::default().fg(job_state_color(&job.state)),
                    ),
                ]),
                Line::from(vec!["User ".fg(MUTED), job.user.clone().fg(TEXT)]),
                Line::from(vec!["Node ".fg(MUTED), assigned.fg(TEXT)]),
                Line::from(""),
                Line::from("This job does not have a node in the current snapshot."),
            ]
        }
    }
}

fn detail_lines(
    snapshot: &ClusterSnapshot,
    node: &NodeSnapshot,
    selected_job: Option<&JobSummary>,
) -> Vec<Line<'static>> {
    let jobs_on_node = jobs_for_node(snapshot, &node.name);
    let user_count = jobs_on_node
        .iter()
        .map(|job| job.user.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                node.name.clone(),
                Style::default().fg(TEXT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  {}", node.display_state()),
                Style::default().fg(state_color(node.display_state())),
            ),
        ]),
        Line::from(vec!["Addr ".fg(MUTED), node.addr.clone().fg(TEXT)]),
        Line::from(vec![
            "Partitions ".fg(MUTED),
            node.partitions.clone().fg(TEXT),
        ]),
    ];

    if let Some(job) = selected_job {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            "Job ".fg(MUTED),
            job.id.clone().fg(TEXT).bold(),
            "  ".into(),
            job.user.clone().fg(SKY),
            "  ".into(),
            job.state.clone().fg(job_state_color(&job.state)),
        ]));
        lines.push(Line::from(vec![
            "Span ".fg(MUTED),
            format!(
                "{}  {} nodes  {} cpus  {}",
                job.elapsed, job.nodes, job.cpus, job.gres
            )
            .fg(TEXT),
        ]));
    }

    let mem_used_bytes = node
        .mem_total_mb
        .saturating_sub(node.mem_available_mb.unwrap_or(node.mem_total_mb))
        * 1024
        * 1024;
    lines.push(Line::from(""));
    lines.push(percent_bar_line(
        "CPU",
        node.cpu_busy_pct,
        format!(
            "{}/{} alloc  load {:.1}",
            node.cpu_alloc, node.cpu_total, node.cpu_load
        ),
    ));
    lines.push(percent_bar_line(
        "MEM",
        node.mem_used_pct(),
        format!(
            "{}/{} used",
            format_bytes(mem_used_bytes),
            format_bytes(node.mem_total_mb * 1024 * 1024)
        ),
    ));
    lines.push(percent_bar_line(
        "GPU",
        if node.gpu_total == 0 {
            None
        } else {
            node.gpu_util_avg()
        },
        format!(
            "{}/{} alloc  {} gpus",
            node.gpu_alloc,
            node.gpu_total,
            node.gpu_samples.len()
        ),
    ));
    if node.gpu_mem_total_mb() > 0 {
        lines.push(percent_bar_line(
            "VRAM",
            Some(ratio(node.gpu_mem_used_mb(), node.gpu_mem_total_mb())),
            format!(
                "{}/{}",
                format_bytes(node.gpu_mem_used_mb() * 1024 * 1024),
                format_bytes(node.gpu_mem_total_mb() * 1024 * 1024)
            ),
        ));
    }
    lines.push(Line::from(vec![
        "Net ".fg(MUTED),
        flow_label(node.net_rx_bps, node.net_tx_bps).fg(TEXT),
    ]));
    lines.push(Line::from(vec![
        "Sample ".fg(MUTED),
        node.last_remote_sample
            .map(|sample| {
                format!(
                    "{}ms ago",
                    Instant::now().saturating_duration_since(sample).as_millis()
                )
            })
            .unwrap_or_else(|| "remote metrics pending".into())
            .fg(TEXT),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        "Jobs ".fg(MUTED),
        format!("{} on node", jobs_on_node.len()).fg(TEXT).bold(),
        "  ".into(),
        format!("{} users", user_count).fg(SKY),
    ]));

    if jobs_on_node.is_empty() {
        lines.push(Line::from(vec![
            "  ".into(),
            "No jobs currently mapped to this node.".fg(MUTED),
        ]));
    } else {
        for job in jobs_on_node.iter().take(6) {
            lines.push(Line::from(vec![
                "  ".into(),
                job.id.clone().fg(TEXT).bold(),
                " ".into(),
                job.user.clone().fg(SKY),
                " ".into(),
                job.state.clone().fg(job_state_color(&job.state)),
                "  ".into(),
                job.elapsed.clone().fg(MUTED),
            ]));
        }
        if jobs_on_node.len() > 6 {
            lines.push(Line::from(vec![
                "  ".into(),
                format!("+{} more jobs", jobs_on_node.len() - 6).fg(MUTED),
            ]));
        }
    }

    if let Some(first_gpu) = node.gpu_samples.first() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            "Lead GPU ".fg(MUTED),
            format!(
                "{}  idx={}  power={}",
                first_gpu.name.clone(),
                first_gpu.index,
                first_gpu
                    .power_watts
                    .map(|value| format!("{value:.0}W"))
                    .unwrap_or_else(|| "-".into())
            )
            .fg(TEXT),
        ]));
    }

    for gpu in node.gpu_samples.iter().skip(1).take(2) {
        lines.push(Line::from(vec![
            "GPU ".fg(MUTED),
            format!(
                "#{} {}  {:.0}%  {}/{}",
                gpu.index,
                gpu.name,
                gpu.utilization_pct,
                format_bytes(gpu.memory_used_mb * 1024 * 1024),
                format_bytes(gpu.memory_total_mb * 1024 * 1024)
            )
            .fg(TEXT),
        ]));
    }

    lines
}

fn jobs_for_node<'a>(snapshot: &'a ClusterSnapshot, node_name: &str) -> Vec<&'a JobSummary> {
    snapshot
        .jobs
        .iter()
        .filter(|job| job_hosts(job).iter().any(|host| host == node_name))
        .collect()
}

fn percent_bar_line(label: &str, percent: Option<f64>, detail: String) -> Line<'static> {
    let mut spans = vec![format!("{label:>4} ").fg(MUTED)];
    match percent {
        Some(percent) => {
            let bar_width: usize = 12;
            let filled = ((percent.clamp(0.0, 100.0) / 100.0) * bar_width as f64).round() as usize;
            let color = usage_color(percent);
            spans.push("[".fg(MUTED));
            spans.push("█".repeat(filled).fg(color));
            spans.push(
                "░"
                    .repeat(bar_width.saturating_sub(filled))
                    .fg(Color::Rgb(55, 70, 82)),
            );
            spans.push("] ".fg(MUTED));
            spans.push(format!("{percent:>4.0}%").fg(color).bold());
            spans.push("  ".into());
            spans.push(detail.fg(TEXT));
        }
        None => {
            spans.push("n/a".fg(MUTED));
            spans.push("  ".into());
            spans.push(detail.fg(TEXT));
        }
    }
    Line::from(spans)
}

fn panel(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if focused {
            TEAL
        } else {
            Color::Rgb(44, 66, 92)
        }))
        .title(Line::from(vec![
            Span::styled(" ", Style::default().bg(PANEL)),
            Span::styled(
                title.to_uppercase(),
                Style::default()
                    .fg(TEXT)
                    .bg(PANEL)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" ", Style::default().bg(PANEL)),
        ]))
        .style(Style::default().bg(PANEL))
}

fn percent_label(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:>5.1}"))
        .unwrap_or_else(|| "  n/a".into())
}

fn flow_label(rx: Option<f64>, tx: Option<f64>) -> String {
    match (rx, tx) {
        (Some(rx), Some(tx)) => {
            format!("{} / {}", format_bytes_rate(rx), format_bytes_rate(tx))
        }
        _ => "-".into(),
    }
}

fn disk_header_line(usage: Option<&FilesystemUsage>, label: &'static str) -> Line<'static> {
    let mut spans = vec![format!("{label:>4} ").fg(MUTED)];
    match usage {
        Some(usage) => {
            let bar_width: usize = 8;
            let filled =
                ((usage.used_pct.clamp(0.0, 100.0) / 100.0) * bar_width as f64).round() as usize;
            let color = usage_color(usage.used_pct);
            spans.push("[".fg(MUTED));
            spans.push("█".repeat(filled).fg(color));
            spans.push(
                "░"
                    .repeat(bar_width.saturating_sub(filled))
                    .fg(Color::Rgb(55, 70, 82)),
            );
            spans.push("] ".fg(MUTED));
            spans.push(format!("{:.0}%", usage.used_pct).fg(color).bold());
            spans.push(" ".into());
            spans.push(format!("{}/{}", usage.used_human, usage.size_human).fg(TEXT));
        }
        None => spans.push("n/a".fg(MUTED)),
    }
    Line::from(spans)
}

fn usage_color(value: f64) -> Color {
    if value >= 85.0 {
        ROSE
    } else if value >= 70.0 {
        GOLD
    } else {
        TEAL
    }
}

fn ratio(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (used as f64 / total as f64) * 100.0
    }
}

fn format_bytes_rate(bytes_per_sec: f64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec.max(0.0) as u64))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}{}", UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

fn state_badge(node: &NodeSnapshot) -> String {
    node.display_state().to_string()
}

fn state_color(state: &str) -> Color {
    match state {
        "ALLOCATED" | "MIXED" | "COMPLETING" => GOLD,
        "IDLE" => TEAL,
        "DOWN" | "FAIL" | "DRAIN" | "DRAINED" => ROSE,
        _ => SKY,
    }
}

fn job_state_color(state: &str) -> Color {
    match state {
        "RUNNING" | "COMPLETING" | "CONFIGURING" => TEAL,
        "PENDING" | "SUSPENDED" => GOLD,
        "CANCELLED" | "FAILED" | "TIMEOUT" | "NODE_FAIL" | "OUT_OF_MEMORY" => ROSE,
        _ => SKY,
    }
}
