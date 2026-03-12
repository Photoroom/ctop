use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::collect::{Args, CollectorCommand, spawn_collector};
use crate::model::{
    ClusterSnapshot, ClusterSummary, FilesystemUsage, GpuSample, GpuUtilTracker,
    GpuUtilTrackerSnapshot, JobSummary, NodeSnapshot,
};

const CONNECT_RETRY: Duration = Duration::from_millis(200);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const START_TIMEOUT: Duration = Duration::from_secs(5);
const SNAPSHOT_POLL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Default)]
struct SharedState {
    latest: Option<CollectorUpdateWire>,
    active_clients: usize,
    last_client_disconnect: Option<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectorConfig {
    pub refresh_ms: u64,
    pub remote_timeout_secs: u64,
    pub scheduler_timeout_secs: u64,
    pub max_sampled_nodes: usize,
    pub max_jobs: usize,
    pub no_remote: bool,
    pub collector_idle_timeout_secs: Option<u64>,
    pub collector_protect_shutdown: bool,
}

impl CollectorConfig {
    pub fn from_args(args: &Args) -> Self {
        Self {
            refresh_ms: args.refresh_ms,
            remote_timeout_secs: args.remote_timeout_secs,
            scheduler_timeout_secs: args.scheduler_timeout_secs,
            max_sampled_nodes: args.max_sampled_nodes,
            max_jobs: args.max_jobs,
            no_remote: args.no_remote,
            collector_idle_timeout_secs: args.collector_idle_timeout_secs,
            collector_protect_shutdown: args.collector_protect_shutdown,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CollectorWelcome {
    pub config: CollectorConfig,
    pub started_collector: bool,
}

#[derive(Clone, Debug)]
pub struct CollectorConnection {
    pub welcome: CollectorWelcome,
    pub mismatch_warning: Option<String>,
}

#[derive(Clone, Debug)]
pub struct UiUpdate {
    pub snapshot: ClusterSnapshot,
    pub tracker: GpuUtilTracker,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientRequest {
    Hello { requested: CollectorConfig },
    GetSnapshot,
    RefreshNow,
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerResponse {
    Hello {
        config: CollectorConfig,
    },
    Snapshot {
        snapshot: Option<CollectorUpdateWire>,
    },
    Ack,
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClusterSnapshotWire {
    collected_at_ms: u64,
    nodes: Vec<NodeSnapshotWire>,
    jobs: Vec<JobSummaryWire>,
    summary: ClusterSummaryWire,
    errors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CollectorUpdateWire {
    snapshot: ClusterSnapshotWire,
    tracker: GpuUtilTrackerSnapshot,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NodeSnapshotWire {
    name: String,
    addr: String,
    state: String,
    cpu_total: u32,
    cpu_alloc: u32,
    cpu_busy_pct: Option<f64>,
    mem_total_mb: u64,
    mem_available_mb: Option<u64>,
    gpu_total: u32,
    gpu_alloc: u32,
    gpu_samples: Vec<GpuSampleWire>,
    disk_read_bps: Option<f64>,
    disk_write_bps: Option<f64>,
    net_rx_bps: Option<f64>,
    net_tx_bps: Option<f64>,
    last_remote_sample_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GpuSampleWire {
    index: usize,
    name: String,
    utilization_pct: f64,
    memory_used_mb: u64,
    memory_total_mb: u64,
    power_watts: Option<f64>,
    power_limit_watts: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FilesystemUsageWire {
    used_pct: f64,
    size_human: String,
    used_human: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct JobSummaryWire {
    id: String,
    name: String,
    user: String,
    state: String,
    location: String,
    elapsed: String,
    nodes: u32,
    cpus: u32,
    gres: String,
    node_list: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClusterSummaryWire {
    node_total: usize,
    node_active: usize,
    node_down: usize,
    sampled_nodes: usize,
    cpu_total: u64,
    cpu_alloc: u64,
    cpu_busy_pct: Option<f64>,
    mem_total_mb: u64,
    mem_used_mb: Option<u64>,
    gpu_total: u64,
    gpu_alloc: u64,
    gpu_util_pct: Option<f64>,
    gpu_mem_used_mb: u64,
    gpu_mem_total_mb: u64,
    home_usage: Option<FilesystemUsageWire>,
    data_usage: Option<FilesystemUsageWire>,
    disk_read_bps: Option<f64>,
    disk_write_bps: Option<f64>,
    net_rx_bps: Option<f64>,
    net_tx_bps: Option<f64>,
    states: std::collections::BTreeMap<String, usize>,
}

pub fn run_collector_server(args: Args) -> Result<()> {
    let bind_addr = collector_socket_addr(&args)
        .with_context(|| format!("resolving collector address {}", collector_endpoint(&args)))?;
    let listener = TcpListener::bind(bind_addr)
        .with_context(|| format!("binding collector on {}", collector_endpoint(&args)))?;
    listener
        .set_nonblocking(true)
        .context("setting collector listener nonblocking")?;

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (command_tx, command_rx) = mpsc::channel();
    let collector = spawn_collector(args.clone(), snapshot_tx, command_rx);
    let config = CollectorConfig::from_args(&args);
    let shared = Arc::new(Mutex::new(SharedState::default()));
    let mut gpu_tracker = GpuUtilTracker::default();
    let idle_timeout = args.collector_idle_timeout_secs.map(Duration::from_secs);

    loop {
        while let Ok(snapshot) = snapshot_rx.try_recv() {
            gpu_tracker.record(&snapshot);
            if let Ok(mut state) = shared.lock() {
                state.latest = Some(CollectorUpdateWire::from_model(&snapshot, &gpu_tracker));
            }
        }

        match listener.accept() {
            Ok((stream, _)) => {
                let shared = Arc::clone(&shared);
                let command_tx = command_tx.clone();
                let config = config.clone();
                thread::spawn(move || {
                    let _ = handle_client(stream, shared, command_tx, config);
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error).context("accepting collector client"),
        }

        if collector.is_finished() {
            break;
        }
        if idle_timeout.is_some_and(|timeout| collector_should_exit_for_idle(&shared, timeout)) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let _ = command_tx.send(CollectorCommand::Quit);
    let _ = collector.join();
    Ok(())
}

pub fn ensure_collector_service(args: &Args) -> Result<CollectorConnection> {
    let requested = CollectorConfig::from_args(args);
    let mut started_collector = false;

    match connect_and_handshake(args, &requested) {
        Ok(config) => {
            return Ok(CollectorConnection {
                mismatch_warning: config_mismatch_notice(&requested, &config),
                welcome: CollectorWelcome {
                    config,
                    started_collector,
                },
            });
        }
        Err(error) if args.connect_only || !is_local_host(&args.collector_host) => {
            return Err(error);
        }
        Err(_) => {}
    }

    spawn_local_collector(args)?;
    started_collector = true;

    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        match connect_and_handshake(args, &requested) {
            Ok(config) => {
                return Ok(CollectorConnection {
                    mismatch_warning: None,
                    welcome: CollectorWelcome {
                        config,
                        started_collector,
                    },
                });
            }
            Err(error) if Instant::now() >= deadline => {
                return Err(error).context("waiting for collector service to start");
            }
            Err(_) => thread::sleep(CONNECT_RETRY),
        }
    }
}

pub fn stop_collector_service(args: &Args) -> Result<()> {
    let mut writer = match connect_stream(args) {
        Ok(stream) => stream,
        Err(error) if is_missing_collector_error(&error) => {
            println!(
                "no collector running on {}:{}",
                args.collector_host, args.collector_port
            );
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let reader_stream = writer.try_clone().context("cloning collector stream")?;
    let mut reader = BufReader::new(reader_stream);
    match request(&mut writer, &mut reader, &ClientRequest::Shutdown) {
        Ok(ServerResponse::Ack) => {
            println!(
                "stopped collector on {}:{}",
                args.collector_host, args.collector_port
            );
            Ok(())
        }
        Err(error) if error.to_string().contains("cannot be shut down externally") => {
            println!(
                "collector on {}:{} cannot be shut down externally",
                args.collector_host, args.collector_port
            );
            Ok(())
        }
        Err(error) => Err(error),
        Ok(response) => Err(anyhow!(
            "unexpected collector shutdown response: {response:?}"
        )),
    }
}

pub fn spawn_collector_client(
    args: Args,
    snapshot_tx: mpsc::Sender<UiUpdate>,
    notice_tx: mpsc::Sender<String>,
    command_rx: mpsc::Receiver<CollectorCommand>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || collector_client_loop(args, snapshot_tx, notice_tx, command_rx))
}

fn collector_client_loop(
    args: Args,
    snapshot_tx: mpsc::Sender<UiUpdate>,
    notice_tx: mpsc::Sender<String>,
    command_rx: mpsc::Receiver<CollectorCommand>,
) {
    let requested = CollectorConfig::from_args(&args);
    let mut last_snapshot_ms = None;
    let mut had_connection = false;

    'outer: loop {
        let mut writer = match connect_stream(&args) {
            Ok(stream) => {
                if had_connection {
                    let _ = notice_tx.send(format!(
                        "reconnected to collector on {}:{}",
                        args.collector_host, args.collector_port
                    ));
                }
                stream
            }
            Err(error) if args.connect_only || !is_local_host(&args.collector_host) => {
                let _ = notice_tx.send(format!("collector connect failed: {error:#}"));
                break;
            }
            Err(_) => match ensure_collector_service(&args) {
                Ok(connection) => {
                    if connection.welcome.started_collector {
                        let _ = notice_tx.send(format!(
                            "started collector on {}:{}",
                            args.collector_host, args.collector_port
                        ));
                    } else if had_connection {
                        let _ = notice_tx.send(format!(
                            "reconnected to collector on {}:{}",
                            args.collector_host, args.collector_port
                        ));
                    }
                    match connect_stream(&args) {
                        Ok(stream) => stream,
                        Err(error) => {
                            let _ = notice_tx.send(format!("collector unavailable: {error:#}"));
                            thread::sleep(Duration::from_secs(1));
                            continue;
                        }
                    }
                }
                Err(error) => {
                    let _ = notice_tx.send(format!("collector unavailable: {error:#}"));
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            },
        };

        let reader_stream = match writer.try_clone() {
            Ok(stream) => stream,
            Err(error) => {
                let _ = notice_tx.send(format!("collector stream clone failed: {error}"));
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let mut reader = BufReader::new(reader_stream);

        if request(
            &mut writer,
            &mut reader,
            &ClientRequest::Hello {
                requested: requested.clone(),
            },
        )
        .is_err()
        {
            let _ = notice_tx.send("collector hello failed; reconnecting".into());
            thread::sleep(Duration::from_secs(1));
            continue;
        }
        had_connection = true;

        loop {
            match command_rx.try_recv() {
                Ok(CollectorCommand::RefreshNow) => {
                    let _ = request(&mut writer, &mut reader, &ClientRequest::RefreshNow);
                }
                Ok(CollectorCommand::Quit) => break 'outer,
                Err(mpsc::TryRecvError::Disconnected) => break 'outer,
                Err(mpsc::TryRecvError::Empty) => {}
            }

            let response = request(&mut writer, &mut reader, &ClientRequest::GetSnapshot);
            match response {
                Ok(ServerResponse::Snapshot {
                    snapshot: Some(snapshot),
                }) => {
                    if last_snapshot_ms != Some(snapshot.snapshot.collected_at_ms) {
                        last_snapshot_ms = Some(snapshot.snapshot.collected_at_ms);
                        if snapshot_tx.send(snapshot.into_update()).is_err() {
                            break 'outer;
                        }
                    }
                }
                Ok(ServerResponse::Snapshot { snapshot: None }) => {}
                Ok(_) => {}
                Err(_) => {
                    let _ = notice_tx.send("collector disconnected; reconnecting".into());
                    thread::sleep(Duration::from_secs(1));
                    continue 'outer;
                }
            }

            thread::sleep(SNAPSHOT_POLL);
        }
    }
}

fn handle_client(
    mut stream: TcpStream,
    shared: Arc<Mutex<SharedState>>,
    command_tx: mpsc::Sender<CollectorCommand>,
    config: CollectorConfig,
) -> Result<()> {
    let _client = ClientSession::new(&shared);
    let reader_stream = stream.try_clone().context("cloning collector stream")?;
    let mut reader = BufReader::new(reader_stream);
    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .context("reading client request")?;
        if bytes == 0 {
            return Ok(());
        }
        let request: ClientRequest =
            serde_json::from_str(line.trim()).context("decoding client request")?;
        match request {
            ClientRequest::Hello { .. } => {
                write_response(
                    &mut stream,
                    &ServerResponse::Hello {
                        config: config.clone(),
                    },
                )?;
            }
            ClientRequest::GetSnapshot => {
                let snapshot = shared.lock().ok().and_then(|state| state.latest.clone());
                write_response(&mut stream, &ServerResponse::Snapshot { snapshot })?;
            }
            ClientRequest::RefreshNow => {
                let _ = command_tx.send(CollectorCommand::RefreshNow);
                write_response(&mut stream, &ServerResponse::Ack)?;
            }
            ClientRequest::Shutdown => {
                if config.collector_protect_shutdown {
                    write_response(
                        &mut stream,
                        &ServerResponse::Error {
                            message: "collector cannot be shut down externally".into(),
                        },
                    )?;
                    return Ok(());
                }
                let _ = command_tx.send(CollectorCommand::Quit);
                write_response(&mut stream, &ServerResponse::Ack)?;
                return Ok(());
            }
        }
    }
}

fn connect_and_handshake(args: &Args, requested: &CollectorConfig) -> Result<CollectorConfig> {
    let mut writer = connect_stream(args)?;
    let reader_stream = writer.try_clone().context("cloning collector stream")?;
    let mut reader = BufReader::new(reader_stream);
    match request(
        &mut writer,
        &mut reader,
        &ClientRequest::Hello {
            requested: requested.clone(),
        },
    )? {
        ServerResponse::Hello { config } => Ok(config),
        response => Err(anyhow!("unexpected collector hello response: {response:?}")),
    }
}

fn connect_stream(args: &Args) -> Result<TcpStream> {
    let addr = collector_socket_addr(args)
        .with_context(|| format!("resolving collector address {}", collector_endpoint(args)))?;
    let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
        .with_context(|| format!("connecting to collector at {}", collector_endpoint(args)))?;
    stream
        .set_nodelay(true)
        .context("setting collector stream nodelay")?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .context("setting collector stream read timeout")?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .context("setting collector stream write timeout")?;
    Ok(stream)
}

fn spawn_local_collector(args: &Args) -> Result<()> {
    let exe = std::env::current_exe().context("resolving current ctop executable")?;
    let mut command = Command::new(exe);
    let idle_timeout_secs = args.collector_idle_timeout_secs.unwrap_or(600);
    command
        .arg("--collector-only")
        .arg("--collector-host")
        .arg(&args.collector_host)
        .arg("--collector-port")
        .arg(args.collector_port.to_string())
        .arg("--refresh-ms")
        .arg(args.refresh_ms.to_string())
        .arg("--remote-timeout-secs")
        .arg(args.remote_timeout_secs.to_string())
        .arg("--scheduler-timeout-secs")
        .arg(args.scheduler_timeout_secs.to_string())
        .arg("--max-sampled-nodes")
        .arg(args.max_sampled_nodes.to_string())
        .arg("--max-jobs")
        .arg(args.max_jobs.to_string())
        .arg("--collector-idle-timeout-secs")
        .arg(idle_timeout_secs.to_string());
    if args.collector_protect_shutdown {
        command.arg("--collector-protect-shutdown");
    }
    if args.no_remote {
        command.arg("--no-remote");
    }
    unsafe {
        command.pre_exec(|| {
            let rc = libc::setsid();
            if rc == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("starting local collector process")?;
    Ok(())
}

fn request(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    request: &ClientRequest,
) -> Result<ServerResponse> {
    let payload = serde_json::to_string(request).context("encoding collector request")?;
    writer
        .write_all(payload.as_bytes())
        .and_then(|_| writer.write_all(b"\n"))
        .and_then(|_| writer.flush())
        .context("sending collector request")?;

    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .context("reading collector response")?;
    if bytes == 0 {
        return Err(anyhow!("collector connection closed"));
    }
    let response: ServerResponse =
        serde_json::from_str(line.trim()).context("decoding collector response")?;
    match response {
        ServerResponse::Error { message } => Err(anyhow!(message)),
        other => Ok(other),
    }
}

fn write_response(stream: &mut TcpStream, response: &ServerResponse) -> Result<()> {
    let payload = serde_json::to_string(response).context("encoding collector response")?;
    stream
        .write_all(payload.as_bytes())
        .and_then(|_| stream.write_all(b"\n"))
        .and_then(|_| stream.flush())
        .context("writing collector response")
}

fn config_mismatch_notice(requested: &CollectorConfig, actual: &CollectorConfig) -> Option<String> {
    if requested == actual {
        return None;
    }
    Some(format!(
        "collector already running with refresh={}ms, remote_timeout={}s, scheduler_timeout={}s, sampled_nodes={}, jobs={}, no_remote={}, idle_timeout={}, protect_shutdown={} (requested refresh={}ms, remote_timeout={}s, scheduler_timeout={}s, sampled_nodes={}, jobs={}, no_remote={}, idle_timeout={}, protect_shutdown={})",
        actual.refresh_ms,
        actual.remote_timeout_secs,
        actual.scheduler_timeout_secs,
        actual.max_sampled_nodes,
        actual.max_jobs,
        actual.no_remote,
        actual
            .collector_idle_timeout_secs
            .map(|secs| secs.to_string())
            .unwrap_or_else(|| "none".into()),
        actual.collector_protect_shutdown,
        requested.refresh_ms,
        requested.remote_timeout_secs,
        requested.scheduler_timeout_secs,
        requested.max_sampled_nodes,
        requested.max_jobs,
        requested.no_remote,
        requested
            .collector_idle_timeout_secs
            .map(|secs| secs.to_string())
            .unwrap_or_else(|| "none".into()),
        requested.collector_protect_shutdown,
    ))
}

fn collector_endpoint(args: &Args) -> String {
    format!("{}:{}", args.collector_host, args.collector_port)
}

fn collector_socket_addr(args: &Args) -> Result<SocketAddr> {
    collector_endpoint(args)
        .to_socket_addrs()
        .context("resolving socket addresses")?
        .next()
        .ok_or_else(|| anyhow!("no socket address resolved"))
}

fn is_local_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn is_missing_collector_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionAborted
            )
        })
    })
}

fn collector_should_exit_for_idle(shared: &Arc<Mutex<SharedState>>, timeout: Duration) -> bool {
    let Ok(state) = shared.lock() else {
        return false;
    };
    if state.active_clients > 0 {
        return false;
    }
    state
        .last_client_disconnect
        .is_some_and(|last| last.elapsed() >= timeout)
}

struct ClientSession {
    shared: Arc<Mutex<SharedState>>,
}

impl ClientSession {
    fn new(shared: &Arc<Mutex<SharedState>>) -> Self {
        if let Ok(mut state) = shared.lock() {
            state.active_clients += 1;
            state.last_client_disconnect = None;
        }
        Self {
            shared: Arc::clone(shared),
        }
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.lock() {
            state.active_clients = state.active_clients.saturating_sub(1);
            if state.active_clients == 0 {
                state.last_client_disconnect = Some(Instant::now());
            }
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn instant_to_epoch_ms(ts: Instant, now: Instant, now_ms: u64) -> u64 {
    let delta = now.saturating_duration_since(ts).as_millis() as u64;
    now_ms.saturating_sub(delta)
}

fn epoch_ms_to_instant(ts_ms: u64, now: Instant, now_ms: u64) -> Instant {
    let delta = now_ms.saturating_sub(ts_ms);
    now.checked_sub(Duration::from_millis(delta)).unwrap_or(now)
}

impl ClusterSnapshotWire {
    fn from_model(snapshot: &ClusterSnapshot) -> Self {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        Self {
            collected_at_ms: instant_to_epoch_ms(snapshot.collected_at, now, now_ms),
            nodes: snapshot
                .nodes
                .iter()
                .cloned()
                .map(|node| NodeSnapshotWire::from_model(node, now, now_ms))
                .collect(),
            jobs: snapshot
                .jobs
                .iter()
                .cloned()
                .map(JobSummaryWire::from)
                .collect(),
            summary: snapshot.summary.clone().into(),
            errors: snapshot.errors.clone(),
        }
    }

    fn into_model(self) -> ClusterSnapshot {
        let now = Instant::now();
        let now_ms = now_unix_ms();
        ClusterSnapshot {
            collected_at: epoch_ms_to_instant(self.collected_at_ms, now, now_ms),
            nodes: self
                .nodes
                .into_iter()
                .map(|node| node.into_model(now, now_ms))
                .collect(),
            jobs: self.jobs.into_iter().map(JobSummary::from).collect(),
            summary: self.summary.into(),
            errors: self.errors,
        }
    }
}

impl CollectorUpdateWire {
    fn from_model(snapshot: &ClusterSnapshot, tracker: &GpuUtilTracker) -> Self {
        Self {
            snapshot: ClusterSnapshotWire::from_model(snapshot),
            tracker: tracker.export_snapshot(),
        }
    }

    fn into_update(self) -> UiUpdate {
        UiUpdate {
            snapshot: self.snapshot.into_model(),
            tracker: GpuUtilTracker::from_snapshot(self.tracker),
        }
    }
}

impl NodeSnapshotWire {
    fn from_model(node: NodeSnapshot, now: Instant, now_ms: u64) -> Self {
        Self {
            name: node.name,
            addr: node.addr,
            state: node.state,
            cpu_total: node.cpu_total,
            cpu_alloc: node.cpu_alloc,
            cpu_busy_pct: node.cpu_busy_pct,
            mem_total_mb: node.mem_total_mb,
            mem_available_mb: node.mem_available_mb,
            gpu_total: node.gpu_total,
            gpu_alloc: node.gpu_alloc,
            gpu_samples: node
                .gpu_samples
                .into_iter()
                .map(GpuSampleWire::from)
                .collect(),
            disk_read_bps: node.disk_read_bps,
            disk_write_bps: node.disk_write_bps,
            net_rx_bps: node.net_rx_bps,
            net_tx_bps: node.net_tx_bps,
            last_remote_sample_ms: node
                .last_remote_sample
                .map(|ts| instant_to_epoch_ms(ts, now, now_ms)),
        }
    }

    fn into_model(self, now: Instant, now_ms: u64) -> NodeSnapshot {
        NodeSnapshot {
            name: self.name,
            addr: self.addr,
            state: self.state,
            cpu_total: self.cpu_total,
            cpu_alloc: self.cpu_alloc,
            cpu_busy_pct: self.cpu_busy_pct,
            mem_total_mb: self.mem_total_mb,
            mem_available_mb: self.mem_available_mb,
            gpu_total: self.gpu_total,
            gpu_alloc: self.gpu_alloc,
            gpu_samples: self.gpu_samples.into_iter().map(GpuSample::from).collect(),
            disk_read_bps: self.disk_read_bps,
            disk_write_bps: self.disk_write_bps,
            net_rx_bps: self.net_rx_bps,
            net_tx_bps: self.net_tx_bps,
            last_remote_sample: self
                .last_remote_sample_ms
                .map(|ts| epoch_ms_to_instant(ts, now, now_ms)),
        }
    }
}

impl From<GpuSample> for GpuSampleWire {
    fn from(sample: GpuSample) -> Self {
        Self {
            index: sample.index,
            name: sample.name,
            utilization_pct: sample.utilization_pct,
            memory_used_mb: sample.memory_used_mb,
            memory_total_mb: sample.memory_total_mb,
            power_watts: sample.power_watts,
            power_limit_watts: sample.power_limit_watts,
        }
    }
}

impl From<GpuSampleWire> for GpuSample {
    fn from(sample: GpuSampleWire) -> Self {
        Self {
            index: sample.index,
            name: sample.name,
            utilization_pct: sample.utilization_pct,
            memory_used_mb: sample.memory_used_mb,
            memory_total_mb: sample.memory_total_mb,
            power_watts: sample.power_watts,
            power_limit_watts: sample.power_limit_watts,
        }
    }
}

impl From<FilesystemUsage> for FilesystemUsageWire {
    fn from(usage: FilesystemUsage) -> Self {
        Self {
            used_pct: usage.used_pct,
            size_human: usage.size_human,
            used_human: usage.used_human,
        }
    }
}

impl From<FilesystemUsageWire> for FilesystemUsage {
    fn from(usage: FilesystemUsageWire) -> Self {
        Self {
            used_pct: usage.used_pct,
            size_human: usage.size_human,
            used_human: usage.used_human,
        }
    }
}

impl From<JobSummary> for JobSummaryWire {
    fn from(job: JobSummary) -> Self {
        Self {
            id: job.id,
            name: job.name,
            user: job.user,
            state: job.state,
            location: job.location,
            elapsed: job.elapsed,
            nodes: job.nodes,
            cpus: job.cpus,
            gres: job.gres,
            node_list: job.node_list,
        }
    }
}

impl From<JobSummaryWire> for JobSummary {
    fn from(job: JobSummaryWire) -> Self {
        Self {
            id: job.id,
            name: job.name,
            user: job.user,
            state: job.state,
            location: job.location,
            elapsed: job.elapsed,
            nodes: job.nodes,
            cpus: job.cpus,
            gres: job.gres,
            node_list: job.node_list,
        }
    }
}

impl From<ClusterSummary> for ClusterSummaryWire {
    fn from(summary: ClusterSummary) -> Self {
        Self {
            node_total: summary.node_total,
            node_active: summary.node_active,
            node_down: summary.node_down,
            sampled_nodes: summary.sampled_nodes,
            cpu_total: summary.cpu_total,
            cpu_alloc: summary.cpu_alloc,
            cpu_busy_pct: summary.cpu_busy_pct,
            mem_total_mb: summary.mem_total_mb,
            mem_used_mb: summary.mem_used_mb,
            gpu_total: summary.gpu_total,
            gpu_alloc: summary.gpu_alloc,
            gpu_util_pct: summary.gpu_util_pct,
            gpu_mem_used_mb: summary.gpu_mem_used_mb,
            gpu_mem_total_mb: summary.gpu_mem_total_mb,
            home_usage: summary.home_usage.map(FilesystemUsageWire::from),
            data_usage: summary.data_usage.map(FilesystemUsageWire::from),
            disk_read_bps: summary.disk_read_bps,
            disk_write_bps: summary.disk_write_bps,
            net_rx_bps: summary.net_rx_bps,
            net_tx_bps: summary.net_tx_bps,
            states: summary.states,
        }
    }
}

impl From<ClusterSummaryWire> for ClusterSummary {
    fn from(summary: ClusterSummaryWire) -> Self {
        Self {
            node_total: summary.node_total,
            node_active: summary.node_active,
            node_down: summary.node_down,
            sampled_nodes: summary.sampled_nodes,
            cpu_total: summary.cpu_total,
            cpu_alloc: summary.cpu_alloc,
            cpu_busy_pct: summary.cpu_busy_pct,
            mem_total_mb: summary.mem_total_mb,
            mem_used_mb: summary.mem_used_mb,
            gpu_total: summary.gpu_total,
            gpu_alloc: summary.gpu_alloc,
            gpu_util_pct: summary.gpu_util_pct,
            gpu_mem_used_mb: summary.gpu_mem_used_mb,
            gpu_mem_total_mb: summary.gpu_mem_total_mb,
            home_usage: summary.home_usage.map(FilesystemUsage::from),
            data_usage: summary.data_usage.map(FilesystemUsage::from),
            disk_read_bps: summary.disk_read_bps,
            disk_write_bps: summary.disk_write_bps,
            net_rx_bps: summary.net_rx_bps,
            net_tx_bps: summary.net_tx_bps,
            states: summary.states,
        }
    }
}
