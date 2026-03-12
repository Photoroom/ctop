use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};
use ratatui::{Frame, layout::Alignment};

use crate::model::{
    AppState, ClusterSnapshot, FilesystemUsage, FocusPane, GpuSample, GpuUtilTracker, JobSummary,
    NodeSnapshot, PopupKind, SortMode,
};

const BG: Color = Color::Rgb(11, 17, 24);
const PANEL: Color = Color::Rgb(16, 26, 37);
const TEAL: Color = Color::Rgb(73, 214, 193);
const SKY: Color = Color::Rgb(107, 170, 255);
const GOLD: Color = Color::Rgb(255, 188, 92);
const ROSE: Color = Color::Rgb(255, 111, 145);
const MUTED: Color = Color::Rgb(148, 167, 181);
const TEXT: Color = Color::Rgb(225, 232, 239);
const OWN_JOB: Color = Color::Rgb(164, 211, 255);

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
            Constraint::Length(5),
            Constraint::Min(14),
            Constraint::Length(2),
        ])
        .split(frame.area());

    draw_header(frame, layout[0], state);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(layout[1]);
    draw_jobs(frame, body[0], state);
    draw_nodes(frame, body[1], state);
    draw_footer(frame, layout[2], state);
    draw_popup(frame, state);
}

fn draw_header(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = panel("Cluster Stats", false);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(snapshot) = state.latest.as_ref() else {
        let empty = Paragraph::new("Waiting for first cluster sample...")
            .style(Style::default().fg(TEXT).bg(PANEL))
            .alignment(Alignment::Center);
        frame.render_widget(empty, inner);
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
        .split(inner);

    let summary = &snapshot.summary;
    let cpu_alloc_pct = ratio(summary.cpu_alloc, summary.cpu_total);
    let mem_used_pct = summary
        .mem_used_mb
        .map(|used| ratio(used, summary.mem_total_mb))
        .unwrap_or(0.0);
    let gpu_alloc_pct = ratio(summary.gpu_alloc, summary.gpu_total);
    let gpu_util_pct = summary.gpu_util_pct.unwrap_or(0.0);

    let cards = [
        header_panel(
            "Cluster CPU".to_string(),
            vec![
                compact_bar_line(
                    summary.cpu_busy_pct.unwrap_or(cpu_alloc_pct),
                    SKY,
                    format!("{:.0}%", summary.cpu_busy_pct.unwrap_or(cpu_alloc_pct)),
                ),
                Line::from(vec![
                    format!("{} / {} alloc", summary.cpu_alloc, summary.cpu_total).fg(TEXT),
                ]),
                Line::from(""),
            ],
        ),
        header_panel(
            "Memory".to_string(),
            vec![
                compact_bar_line(mem_used_pct, TEAL, format!("{:.0}%", mem_used_pct)),
                Line::from(vec![
                    format!(
                        "{} / {} used",
                        format_bytes(summary.mem_used_mb.unwrap_or(0) * 1024 * 1024),
                        format_bytes(summary.mem_total_mb * 1024 * 1024)
                    )
                    .fg(TEXT),
                ]),
            ],
        ),
        header_panel(
            "GPU Alloc".to_string(),
            vec![
                compact_bar_line(gpu_alloc_pct, GOLD, format!("{:.0}%", gpu_alloc_pct)),
                Line::from(vec![
                    format!("{} / {} alloc", summary.gpu_alloc, summary.gpu_total).fg(TEXT),
                ]),
            ],
        ),
        header_panel(
            "GPU Util".to_string(),
            vec![
                compact_bar_line(gpu_util_pct, ROSE, format!("{:.0}%", gpu_util_pct)),
                Line::from(vec![
                    format!(
                        "{} / {} mem",
                        format_bytes(summary.gpu_mem_used_mb * 1024 * 1024),
                        format_bytes(summary.gpu_mem_total_mb * 1024 * 1024)
                    )
                    .fg(TEXT),
                ]),
            ],
        ),
        header_panel(
            "Network".to_string(),
            vec![
                Line::from(vec![
                    "DL ".fg(MUTED),
                    format_bytes_rate(summary.net_rx_bps.unwrap_or(0.0)).fg(TEXT),
                ]),
                Line::from(vec![
                    "UL ".fg(MUTED),
                    format_bytes_rate(summary.net_tx_bps.unwrap_or(0.0)).fg(TEXT),
                ]),
            ],
        ),
        header_panel(
            "Disk".to_string(),
            vec![
                compact_filesystem_line("home", summary.home_usage.as_ref()),
                compact_filesystem_line("data", summary.data_usage.as_ref()),
            ],
        ),
    ];

    for (panel_area, widget) in panels.iter().copied().zip(cards) {
        frame.render_widget(widget, panel_area);
    }
}

fn header_panel<'a>(title: String, mut lines: Vec<Line<'a>>) -> Paragraph<'a> {
    let mut content = vec![Line::from(vec![Span::styled(
        truncate_str(&title, 18),
        Style::default()
            .fg(TEXT)
            .bg(PANEL)
            .add_modifier(Modifier::BOLD),
    )])];
    content.append(&mut lines);
    Paragraph::new(content)
        .alignment(Alignment::Left)
        .style(Style::default().fg(TEXT).bg(PANEL))
}

fn compact_bar_line(label_pct: f64, color: Color, suffix: String) -> Line<'static> {
    let bar_width: usize = 6;
    let filled = ((label_pct.clamp(0.0, 100.0) / 100.0) * bar_width as f64).round() as usize;
    Line::from(vec![
        "▐".repeat(filled).fg(color),
        "▁"
            .repeat(bar_width.saturating_sub(filled))
            .fg(Color::Rgb(55, 70, 82)),
        " ".into(),
        suffix.fg(TEXT),
    ])
}

fn compact_filesystem_line(label: &'static str, usage: Option<&FilesystemUsage>) -> Line<'static> {
    let mut spans = vec![format!("{label} ").fg(MUTED)];
    match usage {
        Some(usage) => {
            let bar_width: usize = 6;
            let filled =
                ((usage.used_pct.clamp(0.0, 100.0) / 100.0) * bar_width as f64).round() as usize;
            let color = usage_color(usage.used_pct);
            spans.push("▐".repeat(filled).fg(color));
            spans.push(
                "▁"
                    .repeat(bar_width.saturating_sub(filled))
                    .fg(Color::Rgb(55, 70, 82)),
            );
            spans.push(" ".into());
            spans.push(format!("{:.0}%", usage.used_pct).fg(color).bold());
            spans.push(" ".into());
            spans.push(format!("{}/{}", usage.used_human, usage.size_human).fg(TEXT));
        }
        None => spans.push("n/a".fg(MUTED)),
    }
    Line::from(spans)
}

fn draw_nodes(frame: &mut Frame, area: Rect, state: &AppState) {
    let Some(snapshot) = state.latest.as_ref() else {
        return;
    };

    let nodes = visible_nodes(snapshot, state);
    let show_gpu_detail = matches!(state.focus, FocusPane::Jobs) || state.job_node_filter.is_some();

    // Build flat list of rows: node rows interleaved with GPU sub-rows when relevant.
    let mut all_rows: Vec<Row> = Vec::new();
    let sel = state.selected_node.min(nodes.len().saturating_sub(1));
    for (index, node) in nodes.iter().enumerate() {
        let selected = index == sel;
        let bg = if selected {
            Color::Rgb(27, 41, 57)
        } else {
            PANEL
        };
        let style = Style::default().fg(TEXT).bg(bg);
        let eff = node_gpu_efficiency(node, &state.gpu_tracker);
        let warmup = state.gpu_tracker.warmup_secs_left();
        let (eff_label, eff_color) =
            display_efficiency_warmup(eff, warmup, state.police_mode, false);
        let vram_pct = if node.gpu_mem_total_mb() > 0 {
            Some(ratio(node.gpu_mem_used_mb(), node.gpu_mem_total_mb()))
        } else {
            None
        };
        let node_power = node_power_label(node);
        let dl_rate = compact_rate_label(node.net_rx_bps);
        let ul_rate = compact_rate_label(node.net_tx_bps);
        all_rows.push(
            Row::new(vec![
                Cell::from(node.name.clone()),
                Cell::from(state_badge(node)),
                Cell::from(percent_label(node.cpu_busy_pct)),
                Cell::from(percent_label(node.mem_used_pct())),
                Cell::from(percent_label(node.gpu_util_avg())),
                Cell::from(percent_label(vram_pct)),
                Cell::from(node_power),
                Cell::from(Span::styled(
                    eff_label,
                    Style::default().fg(eff_color).bg(bg),
                )),
                Cell::from(dl_rate),
                Cell::from(ul_rate),
            ])
            .style(style)
            .height(1),
        );
        // Add per-GPU sub-rows.
        if show_gpu_detail && !node.gpu_samples.is_empty() {
            let gpu_bg = if selected {
                Color::Rgb(22, 34, 48)
            } else {
                Color::Rgb(12, 20, 30)
            };
            for gpu in &node.gpu_samples {
                let gpu_eff = gpu_sample_efficiency(gpu);
                let (ge_label, ge_color) =
                    display_efficiency(gpu_eff, state.police_mode, true);
                let vram_gpu_pct = if gpu.memory_total_mb > 0 {
                    format!("{:.0}%", ratio(gpu.memory_used_mb, gpu.memory_total_mb))
                } else {
                    "-".to_string()
                };
                let gpu_power = match (gpu.power_watts, gpu.power_limit_watts) {
                    (Some(d), Some(l)) => format!("{:.0}/{:.0}W", d, l),
                    (Some(d), None) => format!("{:.0}W", d),
                    _ => "-".to_string(),
                };
                let gpu_style = Style::default().fg(MUTED).bg(gpu_bg);
                all_rows.push(
                    Row::new(vec![
                        Cell::from(format!("  GPU#{}", gpu.index)),
                        Cell::from(truncate_str(&gpu.name, 10)),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(format!("{:.0}%", gpu.utilization_pct)),
                        Cell::from(vram_gpu_pct),
                        Cell::from(gpu_power),
                        Cell::from(Span::styled(
                            ge_label,
                            Style::default().fg(ge_color).bg(gpu_bg),
                        )),
                        Cell::from(""),
                        Cell::from(""),
                    ])
                    .style(gpu_style)
                    .height(1),
                );
            }
        }
    }

    let header = Row::new(vec![
        "Node", "State", "CPU%", "Mem%", "GPU%", "VRAM%", "Power", "GPUeff", "DL", "UL",
    ])
    .style(
        Style::default()
            .fg(MUTED)
            .bg(PANEL)
            .add_modifier(Modifier::BOLD),
    )
    .height(1);

    let nodes_title = if state.job_node_filter.is_some() {
        format!("job nodes ({}) Esc=back", nodes.len())
    } else if matches!(state.focus, FocusPane::Jobs) {
        format!("job workers ({})", nodes.len())
    } else {
        "nodes".to_string()
    };
    let nodes_title = filtered_title(nodes_title.trim_end(), state);

    // Scroll: count total display rows and apply windowing.
    let visible_rows = table_visible_rows(area);
    let total_rows = all_rows.len();
    let start = if total_rows > visible_rows {
        total_rows.saturating_sub(visible_rows).min(
            // Find the first row index that corresponds to the selected node,
            // then try to keep it visible.
            all_rows.len(), // fallback
        )
    } else {
        0
    };
    let display_rows: Vec<Row> = all_rows
        .into_iter()
        .skip(start)
        .take(visible_rows)
        .collect();

    let table = Table::new(
        display_rows,
        [
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Min(7),
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
        "Job", "Name", "User", "State", "Elapsed", "N", "CPU%", "Mem%", "VRAM%", "GRES", "GPUeff",
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
        let eff = job_gpu_efficiency(job, &state.gpu_tracker);
        let warmup = if job.gres.contains("gpu") && job.state == "RUNNING" {
            state.gpu_tracker.warmup_secs_left()
        } else {
            None
        };
        let (eff_label, eff_color) = format_efficiency_warmup(eff, warmup);
        let bg = if selected {
            Color::Rgb(27, 41, 57)
        } else {
            PANEL
        };
        let base_fg = if state.police_mode {
            match eff {
                Some((score, _)) => efficiency_color(score),
                None => TEXT,
            }
        } else if job.user == state.current_user {
            OWN_JOB
        } else {
            TEXT
        };
        let eff_fg = if state.police_mode {
            eff_color
        } else if job.user == state.current_user {
            OWN_JOB
        } else {
            TEXT
        };
        let style = Style::default().fg(base_fg).bg(bg);
        let (cpu_pct, mem_pct, vram_pct) = job_resource_pcts(job, snapshot);
        Row::new(vec![
            Cell::from(job.id.clone()),
            Cell::from(truncate_str(&job.name, 16)),
            Cell::from(job.user.clone()),
            Cell::from(job.state.clone()),
            Cell::from(job.elapsed.clone()),
            Cell::from(job.nodes.to_string()),
            Cell::from(percent_label(cpu_pct)),
            Cell::from(percent_label(mem_pct)),
            Cell::from(percent_label(vram_pct)),
            Cell::from(job.gres.clone()),
            Cell::from(Span::styled(eff_label, Style::default().fg(eff_fg).bg(bg))),
        ])
        .style(style)
    });

    let jobs_title = job_title(jobs.len(), start, end, state);
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(18),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(3),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(12),
            Constraint::Min(8),
        ],
    )
    .header(header)
    .block(panel(&jobs_title, matches!(state.focus, FocusPane::Jobs)))
    .column_spacing(1);
    frame.render_widget(table, area);
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

    let top_spans = vec![
        footer_chip("q", TEAL),
        Span::styled("quit  ", Style::default().fg(MUTED).bg(BG)),
        footer_chip("t", GOLD),
        Span::styled("tools  ", Style::default().fg(MUTED).bg(BG)),
        footer_chip("?", ROSE),
        Span::styled("help", Style::default().fg(MUTED).bg(BG)),
    ];

    let mut status_spans = Vec::new();
    if let Some(snapshot) = state.latest.as_ref() {
        status_spans.push(Span::styled(
            format!(
                "↕ {} {}",
                state.sort_mode.label(),
                if state.descending { "desc" } else { "asc" },
            ),
            Style::default().fg(TEXT).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!("◫ {}", state.focus.label()),
            Style::default().fg(GOLD).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!("↻ {}ms", state.refresh_every.as_millis()),
            Style::default().fg(TEAL).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!(
                "◷ {}ms",
                Instant::now()
                    .saturating_duration_since(snapshot.collected_at)
                    .as_millis()
            ),
            Style::default().fg(TEXT).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!("⌁ {}", state.collector_endpoint),
            Style::default().fg(SKY).bg(BG),
        ));
        status_spans.push(Span::raw("   "));
        status_spans.push(Span::styled(
            format!("◎ {}", state.collector_mode),
            Style::default().fg(MUTED).bg(BG),
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
    frame.render_widget(
        Paragraph::new(Line::from(top_spans)).alignment(Alignment::Left),
        footer_rows[0],
    );
    frame.render_widget(
        Paragraph::new(Line::from(status_spans)).alignment(Alignment::Right),
        footer_rows[0],
    );

    if let Some(notice) = state.notice.as_ref() {
        frame.render_widget(
            Paragraph::new(Line::from(vec![Span::styled(
                notice.clone(),
                Style::default().fg(SKY).bg(BG),
            )]))
            .alignment(Alignment::Left),
            footer_rows[1],
        );
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
    let area = centered_rect(72, 13, frame.area());
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
    lines.push(Line::from(vec![
        "vars ".fg(MUTED),
        "$NODE_NAME".fg(SKY),
        "  ".into(),
        "$JOB_ID".fg(SKY),
    ]));

    frame.render_widget(Clear, area);
    let popup = Paragraph::new(lines)
        .block(panel("tools", true))
        .style(Style::default().fg(TEXT).bg(PANEL))
        .alignment(Alignment::Left);
    frame.render_widget(popup, area);
}

fn draw_help_popup(frame: &mut Frame) {
    let area = centered_rect(96, 36, frame.area());
    let lines = vec![
        help_section("Navigate"),
        help_entry(&["tab"], "switch between nodes and jobs"),
        help_entry(&["up/down", "j/k"], "move the active selection"),
        help_entry(&["pgup/pgdn"], "jump through the focused table"),
        help_entry(&["home/end"], "jump to the first or last row"),
        Line::from(""),
        help_section("Node Tools"),
        help_entry(&["enter"], "ssh to the selected node or job node"),
        help_entry(&["t"], "open the tools popup"),
        help_entry(
            &["h", "b", "n"],
            "launch htop, btop, or nvtop on the selected node",
        ),
        help_entry(&["r"], "run the configured custom command"),
        help_entry(
            &["$NODE_NAME", "$JOB_ID"],
            "available inside the custom command",
        ),
        Line::from(""),
        help_section("Jobs"),
        help_entry(&["l"], "open the selected job log"),
        help_entry(&["c"], "cancel the selected job from the jobs pane"),
        help_entry(&["R"], "refresh now"),
        Line::from(""),
        help_section("Filters & View"),
        help_entry(&["s"], "cycle the sort key"),
        help_entry(&["S"], "flip sort direction"),
        help_entry(&["a"], "toggle active-only nodes"),
        help_entry(&["u"], "user filter: set or clear a username"),
        help_entry(&["m"], "mine mode: show only your jobs and nodes"),
        help_entry(&["p"], "police mode: color jobs by GPU efficiency"),
        Line::from(""),
        help_section("General"),
        help_entry(&["?"], "open or close this help dialog"),
        help_entry(&["esc"], "close popups or cancel inline input"),
        help_entry(&["q", "ctrl-c"], "quit ctop"),
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
            .fg(TEXT)
            .bg(Color::Rgb(58, 70, 84))
            .add_modifier(Modifier::BOLD),
    )])
}

fn help_entry(keys: &[&'static str], description: &'static str) -> Line<'static> {
    let mut spans = Vec::new();
    spans.push(" ".into());
    let mut key_width = 1usize;
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            spans.push(" ".into());
            key_width += 1;
        }
        spans.push(Span::styled(
            format!(" <{key}> "),
            Style::default()
                .fg(TEXT)
                .bg(Color::Rgb(74, 88, 102))
                .add_modifier(Modifier::BOLD),
        ));
        key_width += key.len() + 4;
    }
    spans.push(" ".repeat(22usize.saturating_sub(key_width)).into());
    spans.push(description.fg(TEXT));
    Line::from(spans)
}

fn footer_chip(label: &str, color: Color) -> Span<'static> {
    Span::styled(
        format!(" {label} "),
        Style::default()
            .fg(BG)
            .bg(color)
            .add_modifier(Modifier::BOLD),
    )
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
    let filtered = filtered_jobs(snapshot, state);

    // When focus is on Jobs, automatically show only the selected job's nodes.
    let auto_job_nodes: Option<BTreeSet<String>> =
        if matches!(state.focus, FocusPane::Jobs) && state.job_node_filter.is_none() {
            let jobs = &filtered;
            let idx = state.selected_job.min(jobs.len().saturating_sub(1));
            jobs.get(idx)
                .map(|job| job_hosts(job).into_iter().collect())
        } else {
            None
        };

    let visible_names = if state.user_filter.is_some() {
        let names: BTreeSet<_> = filtered.iter().flat_map(|job| job_hosts(job)).collect();
        Some(names)
    } else {
        None
    };
    let mut nodes: Vec<_> = snapshot
        .nodes
        .iter()
        .filter(|node| {
            // Job drill-down filter takes priority: show only nodes of the selected job.
            if let Some(job_nodes) = state.job_node_filter.as_ref() {
                return job_nodes.iter().any(|name| name == &node.name);
            }
            // Auto-filter to selected job's nodes when focus is on Jobs pane.
            if let Some(ref auto_nodes) = auto_job_nodes {
                return auto_nodes.contains(node.name.as_str());
            }
            (!state.show_active_only || node.is_active())
                && visible_names
                    .as_ref()
                    .is_none_or(|names| names.contains(node.name.as_str()))
        })
        .collect();
    nodes.sort_by(|left, right| {
        compare_nodes(
            left,
            right,
            state.sort_mode,
            state.descending,
            &state.gpu_tracker,
        )
    });
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
    let mut jobs: Vec<_> = snapshot
        .jobs
        .iter()
        .filter(|job| {
            state
                .user_filter
                .as_ref()
                .is_none_or(|filter| job.user == *filter)
        })
        .collect();
    jobs.sort_by(|left, right| compare_jobs(left, right, snapshot, state));
    jobs
}

fn selected_node<'a>(snapshot: &'a ClusterSnapshot, state: &AppState) -> Option<&'a NodeSnapshot> {
    let nodes = visible_nodes(snapshot, state);
    nodes
        .get(state.selected_node.min(nodes.len().saturating_sub(1)))
        .copied()
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

/// Returns (cpu_busy%, mem_used%, vram%) averaged across a job's nodes.
fn job_resource_pcts(
    job: &JobSummary,
    snapshot: &ClusterSnapshot,
) -> (Option<f64>, Option<f64>, Option<f64>) {
    if job.state != "RUNNING" {
        return (None, None, None);
    }
    let hosts = job_hosts(job);
    if hosts.is_empty() {
        return (None, None, None);
    }
    let mut cpu_sum = 0.0;
    let mut cpu_n = 0_usize;
    let mut mem_sum = 0.0;
    let mut mem_n = 0_usize;
    let mut vram_used = 0_u64;
    let mut vram_total = 0_u64;
    for node in &snapshot.nodes {
        if !hosts.iter().any(|h| h == &node.name) {
            continue;
        }
        if let Some(cpu) = node.cpu_busy_pct {
            cpu_sum += cpu;
            cpu_n += 1;
        }
        if let Some(mem) = node.mem_used_pct() {
            mem_sum += mem;
            mem_n += 1;
        }
        vram_used += node.gpu_mem_used_mb();
        vram_total += node.gpu_mem_total_mb();
    }
    let cpu = (cpu_n > 0).then(|| cpu_sum / cpu_n as f64);
    let mem = (mem_n > 0).then(|| mem_sum / mem_n as f64);
    let vram = (vram_total > 0).then(|| ratio(vram_used, vram_total));
    (cpu, mem, vram)
}

/// Returns the 1-minute rolling average GPU utilization (0–100) across all
/// nodes of a running GPU job, or `None` if the job has no GPUs, the tracker
/// has no samples, or the warmup period has not elapsed yet.
fn job_gpu_util(job: &JobSummary, tracker: &GpuUtilTracker) -> Option<f64> {
    if job.state != "RUNNING" || !job.gres.contains("gpu") {
        return None;
    }
    if !tracker.is_warmed_up() {
        return None;
    }
    let hosts = job_hosts(job);
    if hosts.is_empty() {
        return None;
    }
    let mut total_util = 0.0;
    let mut sampled = 0_usize;
    for host in &hosts {
        if let Some(util) = tracker.node_avg(host) {
            total_util += util;
            sampled += 1;
        }
    }
    (sampled > 0).then(|| total_util / sampled as f64)
}

/// Compute GPU efficiency for a job: returns (score 0-100, warning flag).
/// Score is primarily driven by power usage. Warning when high util + low power.
fn job_gpu_efficiency(job: &JobSummary, tracker: &GpuUtilTracker) -> Option<(f64, bool)> {
    if job.state != "RUNNING" || !job.gres.contains("gpu") {
        return None;
    }
    if !tracker.is_warmed_up() {
        return None;
    }
    let hosts = job_hosts(job);
    if hosts.is_empty() {
        return None;
    }
    let mut total_power = 0.0;
    let mut total_util = 0.0;
    let mut power_count = 0_usize;
    let mut util_count = 0_usize;
    for host in &hosts {
        if let Some(pwr) = tracker.node_power_avg(host) {
            total_power += pwr;
            power_count += 1;
        }
        if let Some(util) = tracker.node_avg(host) {
            total_util += util;
            util_count += 1;
        }
    }
    if power_count == 0 && util_count == 0 {
        return None;
    }
    let avg_power = if power_count > 0 {
        total_power / power_count as f64
    } else {
        0.0
    };
    let avg_util = if util_count > 0 {
        total_util / util_count as f64
    } else {
        0.0
    };
    let (score, warning) = compute_efficiency_score(avg_util, avg_power, power_count > 0);
    Some((score, warning))
}

/// Compute GPU efficiency for a single node.
fn node_gpu_efficiency(node: &NodeSnapshot, tracker: &GpuUtilTracker) -> Option<(f64, bool)> {
    if node.gpu_total == 0 || node.gpu_alloc == 0 {
        return None;
    }
    if !tracker.is_warmed_up() {
        return None;
    }
    let avg_util = tracker.node_avg(&node.name)?;
    let avg_power = tracker.node_power_avg(&node.name);
    let has_power = avg_power.is_some();
    let avg_power = avg_power.unwrap_or(0.0);
    let (score, warning) = compute_efficiency_score(avg_util, avg_power, has_power);
    Some((score, warning))
}

/// Core efficiency scoring.
/// - Power usage is the primary signal (score = power_pct).
/// - Warning flag: high GPU util (>80%) but low power (<40%) is suspicious.
/// - If no power data, falls back to GPU utilization.
fn compute_efficiency_score(gpu_util: f64, power_pct: f64, has_power: bool) -> (f64, bool) {
    if !has_power {
        // No power data — fall back to GPU utilization as the score.
        return (gpu_util, false);
    }
    let warning = gpu_util > 80.0 && power_pct < 40.0;
    (power_pct, warning)
}

/// Per-GPU efficiency from instantaneous power_draw / power_limit.
fn gpu_sample_efficiency(gpu: &GpuSample) -> Option<(f64, bool)> {
    let (draw, limit) = match (gpu.power_watts, gpu.power_limit_watts) {
        (Some(d), Some(l)) if l > 0.0 => (d, l),
        _ => return None,
    };
    let power_pct = (draw / limit) * 100.0;
    let warning = gpu.utilization_pct > 80.0 && power_pct < 40.0;
    Some((power_pct, warning))
}

/// Maps efficiency score (0-100) to a color: green = good (high), red = bad (low).
/// Non-linear color mapping for efficiency score (0-100).
/// Uses a power curve so that 80-100% has visible green gradient,
/// while lower values quickly shift to red.
fn efficiency_color(score: f64) -> Color {
    let t = (score / 100.0).clamp(0.0, 1.0);
    // Power curve: t^0.5 spreads the green range at the top,
    // compresses the red range at the bottom.
    let curved = t.sqrt();
    let r = ((1.0 - curved) * 255.0).round() as u8;
    let g = (curved * 255.0).round() as u8;
    Color::Rgb(r, g, 60)
}

const SPINNER: &[char] = &['\u{25d0}', '\u{25d3}', '\u{25d1}', '\u{25d2}'];

fn warmup_spinner() -> char {
    // Cycle through spinner frames based on wall-clock seconds.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    SPINNER[(secs / 250) as usize % SPINNER.len()]
}

fn format_efficiency_warmup(eff: Option<(f64, bool)>, warmup_secs: Option<u64>) -> (String, Color) {
    match eff {
        Some((score, warning)) => {
            let show_warning = warning || score < 50.0;
            if show_warning {
                let label = format!("\u{26a0} {:.0}%", score);
                (label, Color::Rgb(255, 200, 0))
            } else {
                let label = format!("{:.0}%", score);
                (label, efficiency_color(score))
            }
        }
        None => match warmup_secs {
            Some(secs) if secs > 0 => {
                let label = format!("{} {}s", warmup_spinner(), secs);
                (label, Color::Rgb(100, 140, 180))
            }
            _ => ("-".to_string(), MUTED),
        },
    }
}

fn display_efficiency(
    eff: Option<(f64, bool)>,
    police_mode: bool,
    prefer_muted_empty: bool,
) -> (String, Color) {
    display_efficiency_warmup(eff, None, police_mode, prefer_muted_empty)
}

fn display_efficiency_warmup(
    eff: Option<(f64, bool)>,
    warmup_secs: Option<u64>,
    police_mode: bool,
    prefer_muted_empty: bool,
) -> (String, Color) {
    let (label, color) = format_efficiency_warmup(eff, warmup_secs);
    if police_mode {
        return (label, color);
    }
    let neutral = if label == "-" && prefer_muted_empty {
        MUTED
    } else if warmup_secs.unwrap_or(0) > 0 {
        MUTED
    } else {
        TEXT
    };
    (label, neutral)
}

/// Format total power draw / total power limit across all GPUs on a node.
fn node_power_label(node: &NodeSnapshot) -> String {
    if node.gpu_samples.is_empty() {
        return "-".to_string();
    }
    let mut draw_sum = 0.0;
    let mut limit_sum = 0.0;
    let mut has_draw = false;
    let mut has_limit = false;
    for gpu in &node.gpu_samples {
        if let Some(d) = gpu.power_watts {
            draw_sum += d;
            has_draw = true;
        }
        if let Some(l) = gpu.power_limit_watts {
            limit_sum += l;
            has_limit = true;
        }
    }
    if has_draw && has_limit {
        format!("{:.0}/{:.0}W", draw_sum, limit_sum)
    } else if has_draw {
        format!("{:.0}W", draw_sum)
    } else {
        "-".to_string()
    }
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}~", &s[..max_len - 1])
    }
}

/// Returns a clone of the currently selected job (for drill-down from main).
pub fn selected_job_for_drill(snapshot: &ClusterSnapshot, state: &AppState) -> Option<JobSummary> {
    selected_job(snapshot, state).cloned()
}

/// Returns the expanded list of node names for a job.
pub fn job_node_names(job: &JobSummary) -> Vec<String> {
    job_hosts(job)
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
    tracker: &GpuUtilTracker,
) -> Ordering {
    let ordering = match mode {
        SortMode::Name => left.name.cmp(&right.name),
        SortMode::State => left.display_state().cmp(right.display_state()),
        SortMode::CpuBusy => ord_option(left.cpu_busy_pct, right.cpu_busy_pct),
        SortMode::CpuAlloc => left.cpu_alloc.cmp(&right.cpu_alloc),
        SortMode::Memory => ord_option(left.mem_used_pct(), right.mem_used_pct()),
        SortMode::GpuUtil => ord_option(left.gpu_util_avg(), right.gpu_util_avg()),
        SortMode::GpuEfficiency => ord_option(
            node_gpu_efficiency(left, tracker).map(|(score, _)| score),
            node_gpu_efficiency(right, tracker).map(|(score, _)| score),
        ),
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

fn compare_jobs(
    left: &JobSummary,
    right: &JobSummary,
    snapshot: &ClusterSnapshot,
    state: &AppState,
) -> Ordering {
    let ordering = match state.sort_mode {
        SortMode::Name => left.name.cmp(&right.name),
        SortMode::State => left.state.cmp(&right.state),
        SortMode::CpuBusy => {
            let (left_cpu, _, _) = job_resource_pcts(left, snapshot);
            let (right_cpu, _, _) = job_resource_pcts(right, snapshot);
            ord_option(left_cpu, right_cpu)
        }
        SortMode::CpuAlloc => left.cpus.cmp(&right.cpus),
        SortMode::Memory => {
            let (_, left_mem, _) = job_resource_pcts(left, snapshot);
            let (_, right_mem, _) = job_resource_pcts(right, snapshot);
            ord_option(left_mem, right_mem)
        }
        SortMode::GpuUtil => ord_option(
            job_gpu_util(left, &state.gpu_tracker),
            job_gpu_util(right, &state.gpu_tracker),
        ),
        SortMode::GpuEfficiency => ord_option(
            job_gpu_efficiency(left, &state.gpu_tracker).map(|(score, _)| score),
            job_gpu_efficiency(right, &state.gpu_tracker).map(|(score, _)| score),
        ),
        SortMode::Network | SortMode::Disk => left.id.cmp(&right.id),
    };
    let ordering = ordering.then_with(|| left.id.cmp(&right.id));
    if state.descending {
        ordering.reverse()
    } else {
        ordering
    }
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

fn compact_rate_label(bytes_per_sec: Option<f64>) -> String {
    let bytes_per_sec = bytes_per_sec.unwrap_or(0.0).max(0.0);
    if bytes_per_sec <= 0.0 {
        return "-".to_string();
    }

    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut value = bytes_per_sec;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if value >= 100.0 || unit == 0 {
        format!("{value:.0}{}", UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
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
