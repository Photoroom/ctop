use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;

use crate::model::{
    ClusterSnapshot, ClusterSummary, FilesystemUsage, GpuSample, JobSummary, NodeSnapshot,
};

const REMOTE_PROBE_READY: &str = "__CTOP_READY__";
const REMOTE_PROBE_END: &str = "__CTOP_SAMPLE_END__";

const PERSISTENT_REMOTE_SCRIPT: &str = r#"
sample() {
printf 'HOST=%s\n' "$(hostname -s 2>/dev/null || hostname)"
awk '/^cpu /{printf "CPU=%s %s %s %s %s %s %s %s\n", $2,$3,$4,$5,$6,$7,$8,$9}' /proc/stat
awk '/MemTotal|MemAvailable/{printf "MEM=%s %s\n", $1, $2}' /proc/meminfo
awk 'NR > 2 {gsub(":", "", $1); print "NET=" $1, $2, $10}' /proc/net/dev
awk '$3 ~ /^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme[0-9]+n[0-9]+|md[0-9]+)$/ {print "DISK=" $3, $6, $10}' /proc/diskstats
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,power.draw,power.limit \
    --format=csv,noheader,nounits 2>/dev/null | sed 's/ *, */,/g' | sed 's/^/GPU=/'
fi
printf '__CTOP_SAMPLE_END__\n'
}
printf '__CTOP_READY__\n'
while IFS= read -r command; do
  case "$command" in
    sample) sample ;;
    quit) exit 0 ;;
  esac
done
"#;

#[derive(Clone, Debug, Parser)]
#[command(author, version, about = "Cluster-wide Slurm TUI monitor")]
pub struct Args {
    #[arg(long, default_value_t = 2000)]
    pub refresh_ms: u64,

    #[arg(long, default_value_t = 4)]
    pub remote_timeout_secs: u64,

    #[arg(long, default_value_t = 4)]
    pub scheduler_timeout_secs: u64,

    #[arg(long, default_value_t = 64)]
    pub max_sampled_nodes: usize,

    #[arg(long, default_value_t = 200)]
    pub max_jobs: usize,

    #[arg(long)]
    pub active_only: bool,

    #[arg(long)]
    pub no_remote: bool,

    #[arg(long)]
    pub custom_tool_command: Option<String>,
}

#[derive(Clone, Debug)]
pub enum CollectorCommand {
    RefreshNow,
    Quit,
}

#[derive(Clone, Debug)]
struct SchedulerNode {
    name: String,
    addr: String,
    state: String,
    partitions: String,
    cpu_total: u32,
    cpu_alloc: u32,
    cpu_load: f64,
    mem_total_mb: u64,
    mem_available_mb: Option<u64>,
    gpu_total: u32,
    gpu_alloc: u32,
}

#[derive(Clone, Debug, Default)]
struct RemoteSample {
    host: String,
    cpu: Option<CpuCounters>,
    mem_total_mb: Option<u64>,
    mem_available_mb: Option<u64>,
    disks: HashMap<String, DiskCounters>,
    nets: HashMap<String, NetCounters>,
    gpus: Vec<GpuSample>,
}

#[derive(Clone, Copy, Debug)]
struct CpuCounters {
    idle: u64,
    total: u64,
}

#[derive(Clone, Copy, Debug)]
struct DiskCounters {
    read_bytes: u64,
    write_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct NetCounters {
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Clone, Debug)]
struct PreviousRemote {
    collected_at: Instant,
    cpu: CpuCounters,
    disks: HashMap<String, DiskCounters>,
    nets: HashMap<String, NetCounters>,
    mem_total_mb: Option<u64>,
    mem_available_mb: Option<u64>,
    gpus: Vec<GpuSample>,
    cpu_busy_pct: Option<f64>,
    disk_read_bps: Option<f64>,
    disk_write_bps: Option<f64>,
    net_rx_bps: Option<f64>,
    net_tx_bps: Option<f64>,
}

pub struct Collector {
    args: Args,
    previous_remote: HashMap<String, PreviousRemote>,
    probes: HashMap<String, PersistentProbe>,
}

#[derive(Clone, Debug)]
struct RemoteTarget {
    name: String,
    addr: String,
}

enum ProbeRequest {
    Sample(mpsc::Sender<Result<RemoteSample>>),
    Shutdown,
}

struct PersistentProbe {
    tx: mpsc::Sender<ProbeRequest>,
    _join: thread::JoinHandle<()>,
}

struct ProbeSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Collector {
    pub fn new(args: Args) -> Self {
        Self {
            args,
            previous_remote: HashMap::new(),
            probes: HashMap::new(),
        }
    }

    pub fn collect(&mut self) -> ClusterSnapshot {
        let collected_at = Instant::now();
        let mut errors = Vec::new();

        let scheduler_nodes = match collect_scheduler_nodes(self.args.scheduler_timeout_secs) {
            Ok(nodes) => nodes,
            Err(error) => {
                return ClusterSnapshot {
                    collected_at,
                    nodes: Vec::new(),
                    jobs: Vec::new(),
                    summary: ClusterSummary::default(),
                    errors: vec![format!("scheduler probe failed: {error:#}")],
                };
            }
        };

        let jobs = match collect_jobs(self.args.scheduler_timeout_secs, self.args.max_jobs) {
            Ok(jobs) => jobs,
            Err(error) => {
                errors.push(format!("job probe failed: {error:#}"));
                Vec::new()
            }
        };

        let remote_targets = select_remote_targets(&scheduler_nodes, self.args.max_sampled_nodes);
        if self.args.no_remote {
            self.sync_probes(&[]);
        } else {
            self.sync_probes(&remote_targets);
        }
        let (remote_samples, remote_errors) = if self.args.no_remote || remote_targets.is_empty() {
            (HashMap::new(), Vec::new())
        } else {
            self.collect_remote_samples(&remote_targets)
        };
        errors.extend(remote_errors);

        let mut nodes = Vec::with_capacity(scheduler_nodes.len());
        for scheduler in scheduler_nodes {
            let remote = remote_samples.get(&scheduler.name);
            let previous = self.previous_remote.get(&scheduler.name);
            let node = merge_node_snapshot(collected_at, scheduler, remote, previous);
            if let Some(remote) = remote {
                self.previous_remote.insert(
                    node.name.clone(),
                    PreviousRemote {
                        collected_at,
                        cpu: remote.cpu.unwrap_or_else(|| {
                            previous
                                .map(|prev| prev.cpu)
                                .unwrap_or(CpuCounters { idle: 0, total: 0 })
                        }),
                        disks: remote.disks.clone(),
                        nets: remote.nets.clone(),
                        mem_total_mb: remote.mem_total_mb,
                        mem_available_mb: remote.mem_available_mb,
                        gpus: remote.gpus.clone(),
                        cpu_busy_pct: node.cpu_busy_pct,
                        disk_read_bps: node.disk_read_bps,
                        disk_write_bps: node.disk_write_bps,
                        net_rx_bps: node.net_rx_bps,
                        net_tx_bps: node.net_tx_bps,
                    },
                );
            }
            nodes.push(node);
        }

        let mut summary = build_summary(&nodes);
        let (home_usage, data_usage) = collect_local_filesystems();
        summary.home_usage = home_usage;
        summary.data_usage = data_usage;
        ClusterSnapshot {
            collected_at,
            nodes,
            jobs,
            summary,
            errors,
        }
    }

    fn sync_probes(&mut self, targets: &[RemoteTarget]) {
        let active: std::collections::BTreeSet<_> =
            targets.iter().map(|target| target.name.clone()).collect();
        let stale: Vec<_> = self
            .probes
            .keys()
            .filter(|name| !active.contains(*name))
            .cloned()
            .collect();
        for name in stale {
            if let Some(probe) = self.probes.remove(&name) {
                probe.shutdown();
            }
        }
    }

    fn collect_remote_samples(
        &mut self,
        targets: &[RemoteTarget],
    ) -> (HashMap<String, RemoteSample>, Vec<String>) {
        let mut pending = Vec::with_capacity(targets.len());
        for target in targets.iter().cloned() {
            let probe = self.probes.entry(target.name.clone()).or_insert_with(|| {
                PersistentProbe::spawn(target.clone(), self.args.remote_timeout_secs)
            });
            let (reply_tx, reply_rx) = mpsc::channel();
            if probe.tx.send(ProbeRequest::Sample(reply_tx)).is_err() {
                let failed = self.probes.remove(&target.name).expect("probe exists");
                failed.shutdown();
                let restarted =
                    PersistentProbe::spawn(target.clone(), self.args.remote_timeout_secs);
                let tx = restarted.tx.clone();
                self.probes.insert(target.name.clone(), restarted);
                let (retry_tx, retry_rx) = mpsc::channel();
                if tx.send(ProbeRequest::Sample(retry_tx)).is_err() {
                    pending.push((
                        target.name.clone(),
                        Err(anyhow!("probe worker unavailable")),
                    ));
                } else {
                    pending.push((target.name.clone(), Ok(retry_rx)));
                }
            } else {
                pending.push((target.name.clone(), Ok(reply_rx)));
            }
        }

        let mut samples = HashMap::new();
        let mut errors = Vec::new();
        for (name, receiver) in pending {
            let receiver = match receiver {
                Ok(receiver) => receiver,
                Err(error) => {
                    errors.push(format!("{name}: {error:#}"));
                    continue;
                }
            };
            match receiver.recv_timeout(Duration::from_secs(self.args.remote_timeout_secs)) {
                Ok(Ok(sample)) => {
                    samples.insert(name, sample);
                }
                Ok(Err(error)) => {
                    errors.push(format!("{name}: {error:#}"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    errors.push(format!("{name}: persistent probe timed out"));
                    if let Some(probe) = self.probes.remove(&name) {
                        probe.shutdown();
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    errors.push(format!("{name}: persistent probe disconnected"));
                    if let Some(probe) = self.probes.remove(&name) {
                        probe.shutdown();
                    }
                }
            }
        }

        (samples, errors)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for (_, probe) in self.probes.drain() {
            probe.shutdown();
        }
    }
}

impl PersistentProbe {
    fn spawn(target: RemoteTarget, timeout_secs: u64) -> Self {
        let (tx, rx) = mpsc::channel();
        let thread_target = target.clone();
        let join = thread::spawn(move || probe_worker(thread_target, timeout_secs, rx));
        Self { tx, _join: join }
    }

    fn shutdown(self) {
        let _ = self.tx.send(ProbeRequest::Shutdown);
    }
}

fn probe_worker(target: RemoteTarget, timeout_secs: u64, rx: mpsc::Receiver<ProbeRequest>) {
    let mut session = None;
    while let Ok(request) = rx.recv() {
        match request {
            ProbeRequest::Sample(reply_tx) => {
                let result = collect_persistent_remote_sample(&target, timeout_secs, &mut session)
                    .or_else(|_| {
                        session = None;
                        collect_persistent_remote_sample(&target, timeout_secs, &mut session)
                    });
                let _ = reply_tx.send(result);
            }
            ProbeRequest::Shutdown => {
                if let Some(mut session) = session.take() {
                    let _ = writeln!(session.stdin, "quit");
                    let _ = session.stdin.flush();
                    let _ = session.child.kill();
                    let _ = session.child.wait();
                }
                break;
            }
        }
    }
}

fn collect_persistent_remote_sample(
    target: &RemoteTarget,
    timeout_secs: u64,
    session: &mut Option<ProbeSession>,
) -> Result<RemoteSample> {
    if session.is_none() {
        *session = Some(start_probe_session(target, timeout_secs)?);
    }
    let session = session.as_mut().expect("session initialized");
    writeln!(session.stdin, "sample")
        .and_then(|_| session.stdin.flush())
        .with_context(|| {
            format!(
                "sending sample request to {} ({})",
                target.name, target.addr
            )
        })?;

    let output = read_probe_response(session, target)?;
    parse_remote_sample(&output)
}

fn start_probe_session(target: &RemoteTarget, timeout_secs: u64) -> Result<ProbeSession> {
    let connect_timeout_secs = timeout_secs.clamp(1, 3);
    let mut child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-o",
            &format!("ConnectTimeout={connect_timeout_secs}"),
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=1",
            target.addr.as_str(),
            "bash",
            "-lc",
            PERSISTENT_REMOTE_SCRIPT,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| {
            format!(
                "starting persistent probe for {} ({})",
                target.name, target.addr
            )
        })?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("persistent probe stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("persistent probe stdout unavailable"))?;
    let mut stdout = BufReader::new(stdout);
    let mut ready = String::new();
    let bytes = stdout
        .read_line(&mut ready)
        .with_context(|| format!("waiting for persistent probe ready on {}", target.name))?;
    if bytes == 0 || ready.trim() != REMOTE_PROBE_READY {
        bail!("persistent probe did not become ready");
    }

    Ok(ProbeSession {
        child,
        stdin,
        stdout,
    })
}

fn read_probe_response(session: &mut ProbeSession, target: &RemoteTarget) -> Result<String> {
    let mut output = String::new();
    loop {
        let mut line = String::new();
        let bytes = session
            .stdout
            .read_line(&mut line)
            .with_context(|| format!("reading persistent probe output from {}", target.name))?;
        if bytes == 0 {
            bail!("persistent probe ended unexpectedly");
        }
        if line.trim() == REMOTE_PROBE_END {
            break;
        }
        output.push_str(&line);
    }
    Ok(output)
}

fn collect_scheduler_nodes(timeout_secs: u64) -> Result<Vec<SchedulerNode>> {
    let output = run_command(
        "timeout",
        &[
            format!("{timeout_secs}s"),
            "scontrol".into(),
            "show".into(),
            "node".into(),
            "-o".into(),
        ],
    )
    .context("running scontrol show node -o")?;

    let mut nodes = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let fields = parse_slurm_kv_line(line);
        let name = fields
            .get("NodeName")
            .cloned()
            .ok_or_else(|| anyhow!("missing NodeName"))?;
        let addr = fields
            .get("NodeAddr")
            .cloned()
            .unwrap_or_else(|| name.clone());
        let state = fields
            .get("State")
            .cloned()
            .unwrap_or_else(|| "UNKNOWN".into());
        let partitions = fields
            .get("Partitions")
            .cloned()
            .unwrap_or_else(|| "-".into());
        let cpu_total = parse_u32(fields.get("CPUTot"))?;
        let cpu_alloc = parse_u32(fields.get("CPUAlloc"))?;
        let cpu_load = parse_f64(fields.get("CPULoad")).unwrap_or_default();
        let mem_total_mb = parse_u64(fields.get("RealMemory")).unwrap_or_default();
        let mem_available_mb = fields
            .get("FreeMem")
            .and_then(|raw| raw.trim().parse::<u64>().ok());
        let (gpu_total, gpu_alloc) = parse_gpu_tres(
            fields.get("CfgTRES").map(String::as_str),
            fields.get("AllocTRES").map(String::as_str),
        );

        nodes.push(SchedulerNode {
            name,
            addr,
            state,
            partitions,
            cpu_total,
            cpu_alloc,
            cpu_load,
            mem_total_mb,
            mem_available_mb,
            gpu_total,
            gpu_alloc,
        });
    }

    if nodes.is_empty() {
        bail!("no nodes returned by scontrol");
    }
    nodes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(nodes)
}

fn collect_jobs(timeout_secs: u64, max_jobs: usize) -> Result<Vec<JobSummary>> {
    let format = "%i|%j|%u|%T|%R|%M|%D|%C|%b|%N";
    let output = run_command(
        "timeout",
        &[
            format!("{timeout_secs}s"),
            "squeue".into(),
            "-h".into(),
            "-o".into(),
            format.into(),
        ],
    )
    .context("running squeue")?;

    let mut jobs = Vec::new();
    for line in output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(max_jobs)
    {
        let columns: Vec<_> = line.split('|').collect();
        if columns.len() < 10 {
            continue;
        }
        jobs.push(JobSummary {
            id: columns[0].to_string(),
            name: columns[1].to_string(),
            user: columns[2].to_string(),
            state: columns[3].to_string(),
            location: columns[4].to_string(),
            elapsed: columns[5].to_string(),
            nodes: columns[6].parse().unwrap_or(0),
            cpus: columns[7].parse().unwrap_or(0),
            gres: columns[8].to_string(),
            node_list: columns[9].to_string(),
        });
    }
    Ok(jobs)
}

fn select_remote_targets(nodes: &[SchedulerNode], max_sampled_nodes: usize) -> Vec<RemoteTarget> {
    let mut ordered = nodes.to_vec();
    ordered.sort_by_key(|node| {
        (
            node.cpu_alloc == 0 && node.gpu_alloc == 0,
            !matches!(
                node.state.split('+').next().unwrap_or("UNKNOWN"),
                "ALLOCATED" | "MIXED" | "IDLE" | "COMPLETING"
            ),
            Reverse(node.gpu_alloc),
            Reverse(node.cpu_alloc),
            node.name.clone(),
        )
    });
    ordered
        .into_iter()
        .take(max_sampled_nodes)
        .map(|node| RemoteTarget {
            name: node.name,
            addr: node.addr,
        })
        .collect()
}

fn parse_remote_sample(output: &str) -> Result<RemoteSample> {
    let mut sample = RemoteSample::default();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if let Some(host) = line.strip_prefix("HOST=") {
            sample.host = host.trim().to_string();
            continue;
        }
        if let Some(cpu) = line.strip_prefix("CPU=") {
            let numbers: Vec<u64> = cpu
                .split_whitespace()
                .filter_map(|value| value.parse::<u64>().ok())
                .collect();
            if numbers.len() >= 4 {
                let idle = numbers[3].saturating_add(*numbers.get(4).unwrap_or(&0));
                let total = numbers.iter().sum();
                sample.cpu = Some(CpuCounters { idle, total });
            }
            continue;
        }
        if let Some(mem) = line.strip_prefix("MEM=") {
            let mut parts = mem.split_whitespace();
            let label = parts.next().unwrap_or_default();
            let value = parts
                .next()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0);
            let value_mb = value / 1024;
            match label.trim_end_matches(':') {
                "MemTotal" => sample.mem_total_mb = Some(value_mb),
                "MemAvailable" => sample.mem_available_mb = Some(value_mb),
                _ => {}
            }
            continue;
        }
        if let Some(net) = line.strip_prefix("NET=") {
            let parts: Vec<_> = net.split_whitespace().collect();
            if parts.len() >= 3 {
                let name = parts[0].to_string();
                if keep_network_device(&name) {
                    sample.nets.insert(
                        name,
                        NetCounters {
                            rx_bytes: parts[1].parse().unwrap_or(0),
                            tx_bytes: parts[2].parse().unwrap_or(0),
                        },
                    );
                }
            }
            continue;
        }
        if let Some(disk) = line.strip_prefix("DISK=") {
            let parts: Vec<_> = disk.split_whitespace().collect();
            if parts.len() >= 3 {
                sample.disks.insert(
                    parts[0].to_string(),
                    DiskCounters {
                        read_bytes: parts[1].parse::<u64>().unwrap_or(0).saturating_mul(512),
                        write_bytes: parts[2].parse::<u64>().unwrap_or(0).saturating_mul(512),
                    },
                );
            }
            continue;
        }
        if let Some(gpu) = line.strip_prefix("GPU=") {
            if gpu.contains("No devices were found") {
                continue;
            }
            let parts: Vec<_> = gpu.split(',').map(str::trim).collect();
            if parts.len() >= 6 {
                sample.gpus.push(GpuSample {
                    index: parts[0].parse().unwrap_or(0),
                    name: parts[1].to_string(),
                    utilization_pct: parts[2].parse().unwrap_or(0.0),
                    memory_used_mb: parts[3].parse().unwrap_or(0),
                    memory_total_mb: parts[4].parse().unwrap_or(0),
                    power_watts: parts[5].parse().ok(),
                    power_limit_watts: parts.get(6).and_then(|s| s.parse().ok()),
                });
            }
            continue;
        }
    }

    if sample.host.is_empty() {
        bail!("remote sample did not include HOST line");
    }
    Ok(sample)
}

fn merge_node_snapshot(
    collected_at: Instant,
    scheduler: SchedulerNode,
    remote: Option<&RemoteSample>,
    previous: Option<&PreviousRemote>,
) -> NodeSnapshot {
    let cpu_busy_pct = match (remote.and_then(|sample| sample.cpu), previous) {
        (Some(cpu), Some(prev)) => Some(cpu_usage(prev.cpu, cpu)),
        (Some(_), None) => None,
        (None, Some(prev)) => prev.cpu_busy_pct,
        (None, None) => None,
    };
    let disk_rates = match (remote, previous) {
        (Some(sample), Some(prev)) => Some((
            counter_delta_disk(
                &prev.disks,
                &sample.disks,
                collected_at.saturating_duration_since(prev.collected_at),
            ),
            counter_delta_disk_write(
                &prev.disks,
                &sample.disks,
                collected_at.saturating_duration_since(prev.collected_at),
            ),
        )),
        (Some(_), None) => Some((0.0, 0.0)),
        (None, Some(prev)) => Some((
            prev.disk_read_bps.unwrap_or(0.0),
            prev.disk_write_bps.unwrap_or(0.0),
        )),
        (None, None) => None,
    };
    let net_rates = match (remote, previous) {
        (Some(sample), Some(prev)) => Some((
            counter_delta_net(
                &prev.nets,
                &sample.nets,
                collected_at.saturating_duration_since(prev.collected_at),
            ),
            counter_delta_net_tx(
                &prev.nets,
                &sample.nets,
                collected_at.saturating_duration_since(prev.collected_at),
            ),
        )),
        (Some(_), None) => Some((0.0, 0.0)),
        (None, Some(prev)) => Some((
            prev.net_rx_bps.unwrap_or(0.0),
            prev.net_tx_bps.unwrap_or(0.0),
        )),
        (None, None) => None,
    };
    let mem_total_mb = remote
        .and_then(|sample| sample.mem_total_mb)
        .or_else(|| previous.and_then(|prev| prev.mem_total_mb))
        .unwrap_or(scheduler.mem_total_mb);
    let mem_available_mb = remote
        .and_then(|sample| sample.mem_available_mb)
        .or_else(|| previous.and_then(|prev| prev.mem_available_mb))
        .or(scheduler.mem_available_mb);
    let gpu_samples = remote
        .map(|sample| sample.gpus.clone())
        .filter(|samples| !samples.is_empty())
        .or_else(|| previous.map(|prev| prev.gpus.clone()))
        .unwrap_or_default();
    let last_remote_sample = remote
        .map(|_| collected_at)
        .or_else(|| previous.map(|prev| prev.collected_at));

    NodeSnapshot {
        name: scheduler.name,
        addr: scheduler.addr,
        state: scheduler.state,
        partitions: scheduler.partitions,
        cpu_total: scheduler.cpu_total,
        cpu_alloc: scheduler.cpu_alloc,
        cpu_load: scheduler.cpu_load,
        cpu_busy_pct: cpu_busy_pct.or_else(|| {
            if scheduler.cpu_total == 0 {
                None
            } else {
                Some((scheduler.cpu_load / scheduler.cpu_total as f64) * 100.0)
            }
        }),
        mem_total_mb,
        mem_available_mb,
        gpu_total: scheduler.gpu_total,
        gpu_alloc: scheduler.gpu_alloc,
        gpu_samples,
        disk_read_bps: disk_rates.map(|value| value.0),
        disk_write_bps: disk_rates.map(|value| value.1),
        net_rx_bps: net_rates.map(|value| value.0),
        net_tx_bps: net_rates.map(|value| value.1),
        last_remote_sample,
    }
}

fn build_summary(nodes: &[NodeSnapshot]) -> ClusterSummary {
    let mut summary = ClusterSummary::default();
    let mut cpu_busy_weighted = 0.0;
    let mut cpu_busy_weight = 0.0;
    let mut gpu_util_weighted = 0.0;
    let mut gpu_util_weight = 0.0;
    let mut mem_used_mb = 0_u64;
    let mut mem_nodes = 0_u64;
    let mut disk_read_bps = 0.0;
    let mut disk_write_bps = 0.0;
    let mut net_rx_bps = 0.0;
    let mut net_tx_bps = 0.0;
    let mut disk_nodes = 0_u64;
    let mut net_nodes = 0_u64;

    summary.node_total = nodes.len();
    for node in nodes {
        let primary_state = node.display_state().to_string();
        *summary.states.entry(primary_state.clone()).or_insert(0) += 1;
        if node.is_active() {
            summary.node_active += 1;
        }
        if matches!(
            primary_state.as_str(),
            "DOWN" | "DRAIN" | "DRAINED" | "FAIL"
        ) {
            summary.node_down += 1;
        }
        if node.last_remote_sample.is_some() {
            summary.sampled_nodes += 1;
        }
        summary.cpu_total += node.cpu_total as u64;
        summary.cpu_alloc += node.cpu_alloc as u64;
        summary.mem_total_mb += node.mem_total_mb;
        summary.gpu_total += node.gpu_total as u64;
        summary.gpu_alloc += node.gpu_alloc as u64;
        summary.gpu_mem_used_mb += node.gpu_mem_used_mb();
        summary.gpu_mem_total_mb += node.gpu_mem_total_mb();

        if let Some(cpu_busy) = node.cpu_busy_pct {
            cpu_busy_weighted += cpu_busy * node.cpu_total as f64;
            cpu_busy_weight += node.cpu_total as f64;
        }
        if let Some(mem_pct) = node.mem_used_pct() {
            mem_used_mb = mem_used_mb
                .saturating_add(((mem_pct / 100.0) * node.mem_total_mb as f64).round() as u64);
            mem_nodes += 1;
        }
        if let Some(gpu_util) = node.gpu_util_avg() {
            let weight = node.gpu_samples.len() as f64;
            gpu_util_weighted += gpu_util * weight;
            gpu_util_weight += weight;
        }
        if let Some(value) = node.disk_read_bps {
            disk_read_bps += value;
            disk_nodes += 1;
        }
        if let Some(value) = node.disk_write_bps {
            disk_write_bps += value;
        }
        if let Some(value) = node.net_rx_bps {
            net_rx_bps += value;
            net_nodes += 1;
        }
        if let Some(value) = node.net_tx_bps {
            net_tx_bps += value;
        }
    }

    summary.cpu_busy_pct = (cpu_busy_weight > 0.0).then_some(cpu_busy_weighted / cpu_busy_weight);
    summary.mem_used_mb = (mem_nodes > 0).then_some(mem_used_mb);
    summary.gpu_util_pct = (gpu_util_weight > 0.0).then_some(gpu_util_weighted / gpu_util_weight);
    summary.disk_read_bps = (disk_nodes > 0).then_some(disk_read_bps);
    summary.disk_write_bps = (disk_nodes > 0).then_some(disk_write_bps);
    summary.net_rx_bps = (net_nodes > 0).then_some(net_rx_bps);
    summary.net_tx_bps = (net_nodes > 0).then_some(net_tx_bps);
    summary
}

fn parse_slurm_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
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

fn parse_gpu_tres(cfg_tres: Option<&str>, alloc_tres: Option<&str>) -> (u32, u32) {
    let total = parse_tres_value(cfg_tres, "gres/gpu").unwrap_or(0) as u32;
    let alloc = parse_tres_value(alloc_tres, "gres/gpu").unwrap_or(0) as u32;
    (total, alloc)
}

fn parse_tres_value(raw: Option<&str>, key: &str) -> Option<u64> {
    raw?.split(',').find_map(|entry| {
        let (name, value) = entry.split_once('=')?;
        (name == key).then(|| value.parse::<u64>().ok()).flatten()
    })
}

fn parse_u32(raw: Option<&String>) -> Result<u32> {
    Ok(raw
        .map(String::as_str)
        .unwrap_or("0")
        .trim()
        .parse::<u32>()?)
}

fn parse_u64(raw: Option<&String>) -> Result<u64> {
    Ok(raw
        .map(String::as_str)
        .unwrap_or("0")
        .trim()
        .parse::<u64>()?)
}

fn parse_f64(raw: Option<&String>) -> Result<f64> {
    Ok(raw
        .map(String::as_str)
        .unwrap_or("0")
        .trim()
        .parse::<f64>()?)
}

fn cpu_usage(previous: CpuCounters, current: CpuCounters) -> f64 {
    let total_delta = current.total.saturating_sub(previous.total) as f64;
    let idle_delta = current.idle.saturating_sub(previous.idle) as f64;
    if total_delta <= f64::EPSILON {
        0.0
    } else {
        ((total_delta - idle_delta) / total_delta) * 100.0
    }
}

fn counter_delta_disk(
    previous: &HashMap<String, DiskCounters>,
    current: &HashMap<String, DiskCounters>,
    elapsed: Duration,
) -> f64 {
    let seconds = elapsed.as_secs_f64().max(0.001);
    current
        .iter()
        .map(|(name, counters)| {
            let previous = previous.get(name).copied().unwrap_or(DiskCounters {
                read_bytes: 0,
                write_bytes: 0,
            });
            counters.read_bytes.saturating_sub(previous.read_bytes) as f64 / seconds
        })
        .sum()
}

fn counter_delta_disk_write(
    previous: &HashMap<String, DiskCounters>,
    current: &HashMap<String, DiskCounters>,
    elapsed: Duration,
) -> f64 {
    let seconds = elapsed.as_secs_f64().max(0.001);
    current
        .iter()
        .map(|(name, counters)| {
            let previous = previous.get(name).copied().unwrap_or(DiskCounters {
                read_bytes: 0,
                write_bytes: 0,
            });
            counters.write_bytes.saturating_sub(previous.write_bytes) as f64 / seconds
        })
        .sum()
}

fn counter_delta_net(
    previous: &HashMap<String, NetCounters>,
    current: &HashMap<String, NetCounters>,
    elapsed: Duration,
) -> f64 {
    let seconds = elapsed.as_secs_f64().max(0.001);
    current
        .iter()
        .map(|(name, counters)| {
            let previous = previous.get(name).copied().unwrap_or(NetCounters {
                rx_bytes: 0,
                tx_bytes: 0,
            });
            counters.rx_bytes.saturating_sub(previous.rx_bytes) as f64 / seconds
        })
        .sum()
}

fn counter_delta_net_tx(
    previous: &HashMap<String, NetCounters>,
    current: &HashMap<String, NetCounters>,
    elapsed: Duration,
) -> f64 {
    let seconds = elapsed.as_secs_f64().max(0.001);
    current
        .iter()
        .map(|(name, counters)| {
            let previous = previous.get(name).copied().unwrap_or(NetCounters {
                rx_bytes: 0,
                tx_bytes: 0,
            });
            counters.tx_bytes.saturating_sub(previous.tx_bytes) as f64 / seconds
        })
        .sum()
}

fn keep_network_device(name: &str) -> bool {
    !matches!(name, "lo")
        && !name.starts_with("docker")
        && !name.starts_with("br-")
        && !name.starts_with("veth")
        && !name.starts_with("virbr")
        && !name.starts_with("flannel")
        && !name.starts_with("cni")
}

fn collect_local_filesystems() -> (Option<FilesystemUsage>, Option<FilesystemUsage>) {
    (
        collect_local_filesystem("/home"),
        collect_local_filesystem("/mnt/data"),
    )
}

fn collect_local_filesystem(path: &str) -> Option<FilesystemUsage> {
    let output = Command::new("df").args(["-hP", path]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().nth(1)?;
    let mut parts = line.split_whitespace();
    let _filesystem = parts.next()?;
    let size_human = parts.next()?.to_string();
    let used_human = parts.next()?.to_string();
    let _avail_human = parts.next()?.to_string();
    let used_pct = parts.next()?.trim_end_matches('%').parse::<f64>().ok()?;
    let _mount = parts.next()?;

    Some(FilesystemUsage {
        size_human,
        used_human,
        used_pct,
    })
}

fn run_command(program: &str, args: &[String]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("spawning {program}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{program} failed: {}", stderr.trim());
    }
    Ok(String::from_utf8(output.stdout)?)
}

pub fn spawn_collector(
    args: Args,
    tx: std::sync::mpsc::Sender<ClusterSnapshot>,
    rx: std::sync::mpsc::Receiver<CollectorCommand>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut collector = Collector::new(args.clone());
        loop {
            if tx.send(collector.collect()).is_err() {
                break;
            }

            match rx.recv_timeout(Duration::from_millis(args.refresh_ms)) {
                Ok(CollectorCommand::RefreshNow) => continue,
                Ok(CollectorCommand::Quit) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    })
}
