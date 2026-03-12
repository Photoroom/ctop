use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct GpuSample {
    pub index: usize,
    pub name: String,
    pub utilization_pct: f64,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub power_watts: Option<f64>,
    pub power_limit_watts: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct FilesystemUsage {
    pub size_human: String,
    pub used_human: String,
    pub used_pct: f64,
}

#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub name: String,
    pub addr: String,
    pub state: String,
    pub partitions: String,
    pub cpu_total: u32,
    pub cpu_alloc: u32,
    pub cpu_load: f64,
    pub cpu_busy_pct: Option<f64>,
    pub mem_total_mb: u64,
    pub mem_available_mb: Option<u64>,
    pub gpu_total: u32,
    pub gpu_alloc: u32,
    pub gpu_samples: Vec<GpuSample>,
    pub disk_read_bps: Option<f64>,
    pub disk_write_bps: Option<f64>,
    pub net_rx_bps: Option<f64>,
    pub net_tx_bps: Option<f64>,
    pub last_remote_sample: Option<Instant>,
}

impl NodeSnapshot {
    pub fn mem_used_pct(&self) -> Option<f64> {
        let available = self.mem_available_mb?;
        if self.mem_total_mb == 0 {
            return None;
        }
        let used = self.mem_total_mb.saturating_sub(available);
        Some((used as f64 / self.mem_total_mb as f64) * 100.0)
    }

    pub fn gpu_util_avg(&self) -> Option<f64> {
        if self.gpu_samples.is_empty() {
            return None;
        }
        let total = self
            .gpu_samples
            .iter()
            .map(|sample| sample.utilization_pct)
            .sum::<f64>();
        Some(total / self.gpu_samples.len() as f64)
    }

    pub fn gpu_mem_used_mb(&self) -> u64 {
        self.gpu_samples
            .iter()
            .map(|sample| sample.memory_used_mb)
            .sum()
    }

    pub fn gpu_mem_total_mb(&self) -> u64 {
        self.gpu_samples
            .iter()
            .map(|sample| sample.memory_total_mb)
            .sum()
    }

    /// Average power usage as a percentage of power limit across all GPUs.
    pub fn gpu_power_pct(&self) -> Option<f64> {
        let mut total_draw = 0.0;
        let mut total_limit = 0.0;
        let mut count = 0;
        for sample in &self.gpu_samples {
            if let (Some(draw), Some(limit)) = (sample.power_watts, sample.power_limit_watts) {
                if limit > 0.0 {
                    total_draw += draw;
                    total_limit += limit;
                    count += 1;
                }
            }
        }
        (count > 0).then(|| (total_draw / total_limit) * 100.0)
    }

    pub fn display_state(&self) -> &str {
        self.state
            .split('+')
            .next()
            .filter(|state| !state.is_empty())
            .unwrap_or(self.state.as_str())
    }

    pub fn is_active(&self) -> bool {
        let state = self.display_state();
        matches!(
            state,
            "ALLOCATED" | "MIXED" | "COMPLETING" | "DRAIN" | "DRAINING"
        ) || self.cpu_alloc > 0
            || self.gpu_alloc > 0
    }
}

#[derive(Clone, Debug)]
pub struct JobSummary {
    pub id: String,
    pub name: String,
    pub user: String,
    pub state: String,
    pub location: String,
    pub elapsed: String,
    pub nodes: u32,
    pub cpus: u32,
    pub gres: String,
    pub node_list: String,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterSummary {
    pub node_total: usize,
    pub node_active: usize,
    pub node_down: usize,
    pub sampled_nodes: usize,
    pub cpu_total: u64,
    pub cpu_alloc: u64,
    pub cpu_busy_pct: Option<f64>,
    pub mem_total_mb: u64,
    pub mem_used_mb: Option<u64>,
    pub gpu_total: u64,
    pub gpu_alloc: u64,
    pub gpu_util_pct: Option<f64>,
    pub gpu_mem_used_mb: u64,
    pub gpu_mem_total_mb: u64,
    pub home_usage: Option<FilesystemUsage>,
    pub data_usage: Option<FilesystemUsage>,
    pub disk_read_bps: Option<f64>,
    pub disk_write_bps: Option<f64>,
    pub net_rx_bps: Option<f64>,
    pub net_tx_bps: Option<f64>,
    pub states: BTreeMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct ClusterSnapshot {
    pub collected_at: Instant,
    pub nodes: Vec<NodeSnapshot>,
    pub jobs: Vec<JobSummary>,
    pub summary: ClusterSummary,
    pub errors: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SortMode {
    Name,
    State,
    CpuBusy,
    CpuAlloc,
    Memory,
    GpuUtil,
    GpuEfficiency,
    Network,
    Disk,
}

impl SortMode {
    pub fn next(self) -> Self {
        match self {
            Self::Name => Self::CpuBusy,
            Self::CpuBusy => Self::Memory,
            Self::Memory => Self::GpuUtil,
            Self::GpuUtil => Self::GpuEfficiency,
            Self::GpuEfficiency => Self::Network,
            Self::Network => Self::CpuAlloc,
            Self::Disk => Self::CpuAlloc,
            Self::CpuAlloc => Self::State,
            Self::State => Self::Name,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::State => "state",
            Self::CpuBusy => "cpu busy",
            Self::CpuAlloc => "cpu alloc",
            Self::Memory => "memory",
            Self::GpuUtil => "gpu util",
            Self::GpuEfficiency => "gpu eff",
            Self::Network => "network",
            Self::Disk => "disk shared",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FocusPane {
    Nodes,
    Jobs,
}

impl FocusPane {
    pub fn toggle(&mut self) {
        *self = match self {
            Self::Nodes => Self::Jobs,
            Self::Jobs => Self::Nodes,
        };
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::Jobs => "jobs",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PopupKind {
    Tools,
    Help,
    CancelJobConfirm,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedState {
    show_active_only: bool,
    user_filter: Option<String>,
    sort_mode: SortMode,
    descending: bool,
    focus: FocusPane,
}

/// Rolling window of GPU utilization samples per node, used to compute a
/// 1-minute average and to enforce a warmup period before coloring jobs.
const GPU_UTIL_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Default)]
pub struct GpuUtilTracker {
    /// Per-node ring buffer of (timestamp, avg_gpu_util%) samples.
    history: HashMap<String, VecDeque<(Instant, f64)>>,
    /// Per-node ring buffer of (timestamp, avg_power_pct%) samples.
    power_history: HashMap<String, VecDeque<(Instant, f64)>>,
    /// The instant the very first sample was recorded.
    first_sample: Option<Instant>,
}

impl GpuUtilTracker {
    /// Ingest GPU utilization and power from the latest snapshot.
    pub fn record(&mut self, snapshot: &ClusterSnapshot) {
        let now = snapshot.collected_at;
        if self.first_sample.is_none() {
            self.first_sample = Some(now);
        }
        for node in &snapshot.nodes {
            if let Some(util) = node.gpu_util_avg() {
                let ring = self.history.entry(node.name.clone()).or_default();
                ring.push_back((now, util));
                while ring
                    .front()
                    .is_some_and(|(ts, _)| now.duration_since(*ts) > GPU_UTIL_WINDOW)
                {
                    ring.pop_front();
                }
            }
            if let Some(power_pct) = node.gpu_power_pct() {
                let ring = self.power_history.entry(node.name.clone()).or_default();
                ring.push_back((now, power_pct));
                while ring
                    .front()
                    .is_some_and(|(ts, _)| now.duration_since(*ts) > GPU_UTIL_WINDOW)
                {
                    ring.pop_front();
                }
            }
        }
    }

    /// Returns `true` once we have been collecting for at least 1 minute.
    pub fn is_warmed_up(&self) -> bool {
        self.first_sample
            .is_some_and(|first| Instant::now().duration_since(first) >= GPU_UTIL_WINDOW)
    }

    /// Seconds remaining in warmup, or 0 if warmed up. Returns None if no
    /// samples have been recorded yet.
    pub fn warmup_secs_left(&self) -> Option<u64> {
        let first = self.first_sample?;
        let elapsed = Instant::now().duration_since(first);
        if elapsed >= GPU_UTIL_WINDOW {
            Some(0)
        } else {
            Some((GPU_UTIL_WINDOW - elapsed).as_secs())
        }
    }

    /// Returns the rolling 1-minute average GPU utilization for `node_name`,
    /// or `None` if no samples exist.
    pub fn node_avg(&self, node_name: &str) -> Option<f64> {
        let ring = self.history.get(node_name)?;
        if ring.is_empty() {
            return None;
        }
        let sum: f64 = ring.iter().map(|(_, util)| util).sum();
        Some(sum / ring.len() as f64)
    }

    /// Returns the rolling 1-minute average power percentage for `node_name`.
    pub fn node_power_avg(&self, node_name: &str) -> Option<f64> {
        let ring = self.power_history.get(node_name)?;
        if ring.is_empty() {
            return None;
        }
        let sum: f64 = ring.iter().map(|(_, pct)| pct).sum();
        Some(sum / ring.len() as f64)
    }
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub latest: Option<ClusterSnapshot>,
    pub selected_node: usize,
    pub selected_job: usize,
    pub show_active_only: bool,
    pub user_filter: Option<String>,
    pub filter_input: Option<String>,
    pub current_user: String,
    pub sort_mode: SortMode,
    pub descending: bool,
    pub refresh_every: Duration,
    pub focus: FocusPane,
    pub popup: Option<PopupKind>,
    pub selected_tool: usize,
    pub pending_cancel_job: Option<String>,
    pub custom_tool_command: Option<String>,
    pub notice: Option<String>,
    pub gpu_tracker: GpuUtilTracker,
    /// When set, the nodes pane is filtered to only show nodes belonging to
    /// this job (drill-down from the jobs pane via Enter).
    pub job_node_filter: Option<Vec<String>>,
}

impl AppState {
    pub fn new(
        refresh_every: Duration,
        show_active_only: bool,
        custom_tool_command: Option<String>,
    ) -> Self {
        let mut state = Self {
            latest: None,
            selected_node: 0,
            selected_job: 0,
            show_active_only,
            user_filter: None,
            filter_input: None,
            current_user: env::var("USER")
                .or_else(|_| env::var("LOGNAME"))
                .unwrap_or_else(|_| "unknown".into()),
            sort_mode: SortMode::CpuBusy,
            descending: true,
            refresh_every,
            focus: FocusPane::Jobs,
            popup: None,
            selected_tool: 0,
            pending_cancel_job: None,
            custom_tool_command,
            notice: None,
            gpu_tracker: GpuUtilTracker::default(),
            job_node_filter: None,
        };
        state.load_persisted(show_active_only);
        state
    }

    pub fn save_persisted(&self) {
        let Some(path) = persisted_state_path() else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        let payload = PersistedState {
            show_active_only: self.show_active_only,
            user_filter: self.user_filter.clone(),
            sort_mode: self.sort_mode,
            descending: self.descending,
            focus: self.focus.clone(),
        };
        let Ok(serialized) = serde_json::to_string_pretty(&payload) else {
            return;
        };
        if fs::create_dir_all(parent).is_err() {
            return;
        }
        let _ = fs::write(path, serialized);
    }

    fn load_persisted(&mut self, cli_active_only: bool) {
        let Some(path) = persisted_state_path() else {
            return;
        };
        let Ok(raw) = fs::read_to_string(path) else {
            return;
        };
        let Ok(saved) = serde_json::from_str::<PersistedState>(&raw) else {
            return;
        };
        self.show_active_only = cli_active_only || saved.show_active_only;
        self.user_filter = saved.user_filter;
        self.sort_mode = saved.sort_mode;
        self.descending = saved.descending;
    }
}

fn persisted_state_path() -> Option<PathBuf> {
    if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
        let trimmed = config_home.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed).join("ctop/state.json"));
        }
    }
    let home = env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".config/ctop/state.json"))
}
