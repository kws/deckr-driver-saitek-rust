#![cfg_attr(test, allow(dead_code))]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use deckr::hardware::runtime::{
    HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome,
    HardwareManagerRuntime as ManagedHardwareRuntime, HardwareResetFuture, HardwareResetHandler,
};
use deckr::lanes::{DeckrMessage, HardwareMessageBody};
use deckr::nats::{DeckrRuntime, NatsDeckrRuntime, NatsStateStore};
use deckr::state::StateMaintenancePolicy;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::backend::{Backend, DeviceCandidate, DeviceHandle, UsbBackend};
use crate::descriptor::{
    device_descriptor, translate_hid_event, DEFAULT_PAGE_ID, RASTER_CAPABILITY_ID,
    SCREEN_CONTROL_ID,
};
use crate::image::encoded_image_to_fip_frame;
use crate::protocol::{changed_events, decode_hid_mask, ByteOrder};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const USB_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BACKOFF_SECS: u64 = 10;
const COMMAND_QUEUE_CAPACITY: usize = 32;

type ManagedNatsHardwareRuntime =
    ManagedHardwareRuntime<NatsStateStore, NatsStateStore, NatsStateStore, NatsDeckrRuntime>;

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    SetRasterFrame {
        control_id: String,
        encoding: String,
        image: Vec<u8>,
    },
    ClearRaster {
        control_id: String,
    },
    ResetDevice,
    Stop,
}

impl RuntimeCommand {
    fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }

    fn is_reset(&self) -> bool {
        matches!(self, Self::ResetDevice)
    }

    fn raster_control_id(&self) -> Option<&str> {
        match self {
            Self::SetRasterFrame { control_id, .. } | Self::ClearRaster { control_id } => {
                Some(control_id)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandSendError;

struct CommandQueueInner {
    state: StdMutex<CommandQueueState>,
    available: Condvar,
}

struct CommandQueueState {
    queue: VecDeque<RuntimeCommand>,
    sender_count: usize,
    receiver_alive: bool,
}

struct CommandSender {
    inner: Arc<CommandQueueInner>,
}

struct CommandReceiver {
    inner: Arc<CommandQueueInner>,
}

fn command_channel() -> (CommandSender, CommandReceiver) {
    let inner = Arc::new(CommandQueueInner {
        state: StdMutex::new(CommandQueueState {
            queue: VecDeque::new(),
            sender_count: 1,
            receiver_alive: true,
        }),
        available: Condvar::new(),
    });
    (
        CommandSender {
            inner: inner.clone(),
        },
        CommandReceiver { inner },
    )
}

impl std::fmt::Debug for CommandSender {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandSender { .. }")
    }
}

impl Clone for CommandSender {
    fn clone(&self) -> Self {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        state.sender_count += 1;
        drop(state);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for CommandSender {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        state.sender_count = state.sender_count.saturating_sub(1);
        if state.sender_count == 0 {
            self.inner.available.notify_all();
        }
    }
}

impl CommandSender {
    fn send(&self, command: RuntimeCommand) -> std::result::Result<(), CommandSendError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        if !state.receiver_alive {
            return Err(CommandSendError);
        }

        if command.is_stop() {
            state.queue.clear();
            state.queue.push_front(command);
            self.inner.available.notify_all();
            return Ok(());
        }

        if state.queue.iter().any(RuntimeCommand::is_stop) {
            return Ok(());
        }

        if command.is_reset() {
            state.queue.clear();
            state.queue.push_front(command);
            self.inner.available.notify_one();
            return Ok(());
        }

        enqueue_bounded_command(&mut state.queue, command);
        self.inner.available.notify_one();
        Ok(())
    }
}

impl Drop for CommandReceiver {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        state.receiver_alive = false;
        self.inner.available.notify_all();
    }
}

impl CommandReceiver {
    fn try_recv(&self) -> std::result::Result<RuntimeCommand, TryRecvError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        if let Some(command) = state.queue.pop_front() {
            return Ok(command);
        }
        if state.sender_count == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<RuntimeCommand, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("command queue mutex should not be poisoned");
        loop {
            if let Some(command) = state.queue.pop_front() {
                return Ok(command);
            }
            if state.sender_count == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .inner
                .available
                .wait_timeout(state, remaining)
                .expect("command queue mutex should not be poisoned");
            state = next_state;
        }
    }
}

fn enqueue_bounded_command(queue: &mut VecDeque<RuntimeCommand>, command: RuntimeCommand) {
    if let Some(control_id) = command.raster_control_id().map(ToOwned::to_owned) {
        queue.retain(|queued| queued.raster_control_id() != Some(control_id.as_str()));
    }
    while queue.len() >= COMMAND_QUEUE_CAPACITY {
        if let Some(index) = queue
            .iter()
            .position(|queued| matches!(queued, RuntimeCommand::SetRasterFrame { .. }))
        {
            queue.remove(index);
        } else if let Some(index) = queue
            .iter()
            .position(|queued| !queued.is_stop() && !queued.is_reset())
        {
            queue.remove(index);
        } else {
            break;
        }
    }
    if queue.len() < COMMAND_QUEUE_CAPACITY {
        queue.push_back(command);
    }
}

#[derive(Debug, Default)]
struct SaitekHardwareHandler {
    command_map: Mutex<HashMap<String, CommandSender>>,
}

impl SaitekHardwareHandler {
    async fn register_device(&self, device_id: String, command_tx: CommandSender) {
        self.command_map.lock().await.insert(device_id, command_tx);
    }

    async fn remove_device(&self, device_id: &str) {
        self.command_map.lock().await.remove(device_id);
    }

    async fn sender(&self, device_id: &str) -> Option<CommandSender> {
        self.command_map.lock().await.get(device_id).cloned()
    }
}

impl HardwareCommandHandler for SaitekHardwareHandler {
    fn handle_hardware_command<'a>(&'a self, message: DeckrMessage) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
            let body = message.hardware_body()?;
            let device_id = body.device_ref().device_id.clone();
            let command = match runtime_command_from_body(body) {
                Ok(command) => command,
                Err(error) => {
                    tracing::debug!(%error, "unsupported hardware command");
                    return Ok(HardwareCommandOutcome::Unsupported);
                }
            };
            let Some(sender) = self.sender(&device_id).await else {
                tracing::debug!(%device_id, "hardware command targets a disconnected device");
                return Ok(HardwareCommandOutcome::Stale);
            };
            if sender.send(command).is_err() {
                tracing::debug!(%device_id, "hardware command queue is disconnected");
                return Ok(HardwareCommandOutcome::Stale);
            }
            Ok(HardwareCommandOutcome::Handled)
        })
    }
}

impl HardwareResetHandler for SaitekHardwareHandler {
    fn reset_hardware_device<'a>(&'a self, device_id: &'a str) -> HardwareResetFuture<'a> {
        Box::pin(async move {
            if let Some(sender) = self.sender(device_id).await {
                let _ = sender.send(RuntimeCommand::ResetDevice);
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
enum WorkerEvent {
    Connected {
        path_key: String,
        device_id: String,
        command_tx: CommandSender,
        device: deckr::lanes::DeviceDescriptor,
    },
    Input {
        device_id: String,
        body: HardwareMessageBody,
    },
    Disconnected {
        path_key: String,
        device_id: String,
    },
    Failed {
        path_key: String,
        error: String,
    },
}

#[derive(Debug, Clone)]
enum WorkerReport {
    Connected {
        worker_id: u64,
        path_key: String,
        device_id: String,
        command_tx: CommandSender,
        device: deckr::lanes::DeviceDescriptor,
    },
    Input {
        worker_id: u64,
        path_key: String,
        device_id: String,
        body: HardwareMessageBody,
    },
    Disconnected {
        worker_id: u64,
        path_key: String,
        device_id: String,
    },
    Failed {
        worker_id: u64,
        path_key: String,
        error: String,
    },
}

pub struct SaitekRemoteManager {
    nats_url: String,
    manager_id: String,
    session_id: String,
    backend: Arc<dyn Backend>,
    state_policy: StateMaintenancePolicy,
}

impl SaitekRemoteManager {
    pub fn new(nats_url: String, manager_id: String) -> Result<Self> {
        Ok(Self::with_backend_and_state_policy(
            nats_url,
            manager_id,
            Arc::new(UsbBackend),
            StateMaintenancePolicy::from_env()?,
        ))
    }

    pub fn with_backend(nats_url: String, manager_id: String, backend: Arc<dyn Backend>) -> Self {
        Self::with_backend_and_state_policy(
            nats_url,
            manager_id,
            backend,
            StateMaintenancePolicy::default(),
        )
    }

    fn with_backend_and_state_policy(
        nats_url: String,
        manager_id: String,
        backend: Arc<dyn Backend>,
        state_policy: StateMaintenancePolicy,
    ) -> Self {
        Self {
            nats_url,
            manager_id,
            session_id: Uuid::new_v4().to_string(),
            backend,
            state_policy,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut backoff = 1u64;
        loop {
            match self.run_connected_session().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    error!(
                        "NATS manager {} disconnected; retrying in {}s: {error:#}",
                        self.manager_id, backoff
                    );
                }
            }
            time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    async fn run_connected_session(&self) -> Result<()> {
        let deckr_runtime = DeckrRuntime::connect(self.nats_url.as_str())
            .await
            .with_context(|| format!("connecting manager {} to NATS", self.manager_id))?;
        info!(
            "Connected manager {} to NATS at {}",
            self.manager_id, self.nats_url
        );
        info!(
            "Saitek state maintenance requested cadences: beacon renewal={}s, Concord token refresh={}s, routing reconciliation={}s",
            self.state_policy.renewal_interval.as_secs(),
            self.state_policy.concord_token_refresh_interval.as_secs(),
            self.state_policy.reconcile_interval.as_secs()
        );

        let hardware_handler = Arc::new(SaitekHardwareHandler::default());
        let command_handler: Arc<dyn HardwareCommandHandler> = hardware_handler.clone();
        let reset_handler: Arc<dyn HardwareResetHandler> = hardware_handler.clone();
        let hardware_runtime = ManagedHardwareRuntime::from_deckr_runtime(
            &deckr_runtime,
            self.manager_id.clone(),
            self.session_id.clone(),
            BTreeMap::new(),
            command_handler,
            Some(reset_handler),
            self.state_policy.clone(),
        )
        .await
        .context("starting managed Saitek hardware runtime")?;
        let (supervisor_event_tx, supervisor_event_rx) =
            tokio_mpsc::unbounded_channel::<WorkerReport>();
        let (manager_event_tx, manager_event_rx) = tokio_mpsc::unbounded_channel::<WorkerEvent>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let supervisor = Supervisor::new(
            self.manager_id.clone(),
            self.backend.clone(),
            supervisor_event_tx,
            supervisor_event_rx,
            manager_event_tx,
        );

        let mut supervisor_handle = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
        let mut hardware_tasks = JoinSet::<deckr::Result<()>>::new();
        hardware_runtime
            .start(&mut hardware_tasks)
            .await
            .context("starting managed Saitek hardware runtime tasks")?;
        let mut tasks = JoinSet::<Result<()>>::new();
        tasks.spawn(worker_event_loop(
            hardware_runtime.clone(),
            hardware_handler.clone(),
            manager_event_rx,
        ));

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for shutdown signal")?;
                info!("Shutting down Saitek manager {}", self.manager_id);
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                hardware_tasks.abort_all();
                let _ = supervisor_handle.await;
                hardware_runtime.stop().await?;
                Ok(())
            }
            result = &mut supervisor_handle => {
                tasks.abort_all();
                hardware_tasks.abort_all();
                let _ = hardware_runtime.stop().await;
                result.context("joining device supervisor")??;
                bail!("device supervisor stopped unexpectedly")
            }
            result = tasks.join_next() => {
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                hardware_tasks.abort_all();
                let _ = supervisor_handle.await;
                let _ = hardware_runtime.stop().await;
                match result {
                    Some(Ok(Ok(()))) => bail!("manager runtime task stopped unexpectedly"),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(error).context("joining manager runtime task"),
                    None => bail!("manager runtime tasks stopped unexpectedly"),
                }
            }
            result = hardware_tasks.join_next() => {
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                hardware_tasks.abort_all();
                let _ = supervisor_handle.await;
                let _ = hardware_runtime.stop().await;
                match result {
                    Some(Ok(Ok(()))) => bail!("managed Saitek hardware runtime task stopped unexpectedly"),
                    Some(Ok(Err(error))) => Err(error).context("managed Saitek hardware runtime task"),
                    Some(Err(error)) => Err(error).context("joining managed Saitek hardware runtime task"),
                    None => bail!("managed Saitek hardware runtime tasks stopped unexpectedly"),
                }
            }
        }
    }
}

async fn worker_event_loop(
    hardware_runtime: ManagedNatsHardwareRuntime,
    hardware_handler: Arc<SaitekHardwareHandler>,
    mut worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
) -> Result<()> {
    while let Some(event) = worker_rx.recv().await {
        match event {
            WorkerEvent::Connected {
                path_key,
                device_id,
                command_tx,
                device,
            } => {
                debug!("Saitek device connected path={path_key} device={device_id}");
                hardware_handler
                    .register_device(device_id.clone(), command_tx)
                    .await;
                hardware_runtime.set_device(device).await?;
            }
            WorkerEvent::Input { device_id, body } => {
                debug!("Routing Saitek hardware input for {device_id}");
                hardware_runtime.handle_hardware_message(body).await?;
            }
            WorkerEvent::Disconnected {
                path_key,
                device_id,
            } => {
                debug!("Saitek device disconnected path={path_key} device={device_id}");
                hardware_handler.remove_device(&device_id).await;
                hardware_runtime
                    .remove_device(&device_id, "disconnected")
                    .await?;
            }
            WorkerEvent::Failed { path_key, error } => {
                warn!("Device worker {path_key} failed: {error}");
            }
        }
    }
    bail!("device worker event stream closed")
}

fn runtime_command_from_body(body: HardwareMessageBody) -> Result<RuntimeCommand> {
    match body {
        HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            params,
            ..
        } if capability_id == RASTER_CAPABILITY_ID && command_type == "set_frame" => {
            let control_id = control_id.context("raster set_frame requires controlId")?;
            let image = params
                .get("image")
                .and_then(|value| value.as_str())
                .context("controlCommand set_frame requires image string")?;
            let encoding = params
                .get("encoding")
                .and_then(|value| value.as_str())
                .context("controlCommand set_frame requires encoding string")?;
            if !matches!(encoding, "jpeg" | "png") {
                bail!("controlCommand set_frame encoding must be jpeg or png");
            }
            Ok(RuntimeCommand::SetRasterFrame {
                control_id,
                encoding: encoding.to_string(),
                image: STANDARD
                    .decode(image.as_bytes())
                    .context("decoding controlCommand image")?,
            })
        }
        HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            params,
            ..
        } if capability_id == RASTER_CAPABILITY_ID && command_type == "clear" => {
            let control_id = control_id.context("raster clear requires controlId")?;
            ensure_empty_params(&params, "raster clear")?;
            Ok(RuntimeCommand::ClearRaster { control_id })
        }
        HardwareMessageBody::ControlCommand {
            capability_id,
            command_type,
            ..
        } => {
            bail!("unsupported controlCommand {capability_id}/{command_type}")
        }
        _ => bail!("not a runtime command"),
    }
}

fn ensure_empty_params(
    params: &serde_json::Map<String, serde_json::Value>,
    command: &str,
) -> Result<()> {
    if !params.is_empty() {
        bail!("{command} requires empty params")
    }
    Ok(())
}

struct Supervisor {
    manager_id: String,
    backend: Arc<dyn Backend>,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerReport>,
    worker_rx: tokio_mpsc::UnboundedReceiver<WorkerReport>,
    manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    next_worker_id: u64,
    launched_workers: HashMap<String, LaunchedWorker>,
    active_workers: HashMap<String, ActiveWorker>,
}

#[derive(Debug, Clone)]
struct LaunchedWorker {
    worker_id: u64,
    command_tx: CommandSender,
}

#[derive(Debug, Clone)]
struct ActiveWorker {
    worker_id: u64,
    device_id: String,
    command_tx: CommandSender,
}

impl Supervisor {
    fn new(
        manager_id: String,
        backend: Arc<dyn Backend>,
        worker_tx: tokio_mpsc::UnboundedSender<WorkerReport>,
        worker_rx: tokio_mpsc::UnboundedReceiver<WorkerReport>,
        manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    ) -> Self {
        Self {
            manager_id,
            backend,
            worker_tx,
            worker_rx,
            manager_tx,
            next_worker_id: 0,
            launched_workers: HashMap::new(),
            active_workers: HashMap::new(),
        }
    }

    async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        let mut discovery = time::interval(DISCOVERY_INTERVAL);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                _ = discovery.tick() => {
                    let descriptors = enumerate_canonical(self.backend.clone()).await?;
                    self.reconcile_usb_presence(descriptors);
                }
                maybe_event = self.worker_rx.recv() => {
                    let Some(event) = maybe_event else { continue; };
                    self.handle_worker_report(event);
                }
            }
        }

        self.stop_all_workers();
        Ok(())
    }

    fn reconcile_usb_presence(&mut self, descriptors: Vec<DeviceCandidate>) {
        let present_paths = descriptors
            .iter()
            .map(DeviceCandidate::path_key)
            .collect::<HashSet<_>>();

        let removed_active_paths = self
            .active_workers
            .keys()
            .filter(|path_key| !present_paths.contains(*path_key))
            .cloned()
            .collect::<Vec<_>>();
        for path_key in removed_active_paths {
            let Some(active) = self.active_workers.remove(&path_key) else {
                continue;
            };
            let _ = active.command_tx.send(RuntimeCommand::Stop);
            let _ = self.manager_tx.send(WorkerEvent::Disconnected {
                path_key,
                device_id: active.device_id,
            });
        }

        let removed_launched_paths = self
            .launched_workers
            .keys()
            .filter(|path_key| !present_paths.contains(*path_key))
            .cloned()
            .collect::<Vec<_>>();
        for path_key in removed_launched_paths {
            if let Some(launched) = self.launched_workers.remove(&path_key) {
                let _ = launched.command_tx.send(RuntimeCommand::Stop);
            }
        }

        for descriptor in descriptors {
            let path_key = descriptor.path_key();
            if self.active_workers.contains_key(&path_key)
                || self.launched_workers.contains_key(&path_key)
            {
                continue;
            }
            self.next_worker_id += 1;
            let worker_id = self.next_worker_id;
            let (command_tx, command_rx) = command_channel();
            self.launched_workers.insert(
                path_key,
                LaunchedWorker {
                    worker_id,
                    command_tx: command_tx.clone(),
                },
            );
            spawn_device_worker(
                worker_id,
                self.manager_id.clone(),
                self.backend.clone(),
                descriptor,
                self.worker_tx.clone(),
                command_tx,
                command_rx,
            );
        }
    }

    fn handle_worker_report(&mut self, report: WorkerReport) {
        match report {
            WorkerReport::Connected {
                worker_id,
                path_key,
                device_id,
                command_tx,
                device,
            } => {
                let Some(launched) = self.launched_workers.get(&path_key) else {
                    let _ = command_tx.send(RuntimeCommand::Stop);
                    return;
                };
                if launched.worker_id != worker_id {
                    let _ = command_tx.send(RuntimeCommand::Stop);
                    return;
                }
                let launched = self.launched_workers.remove(&path_key).expect("checked");
                self.active_workers.insert(
                    path_key.clone(),
                    ActiveWorker {
                        worker_id,
                        device_id: device_id.clone(),
                        command_tx: launched.command_tx,
                    },
                );
                let _ = self.manager_tx.send(WorkerEvent::Connected {
                    path_key,
                    device_id,
                    command_tx,
                    device,
                });
            }
            WorkerReport::Input {
                worker_id,
                path_key,
                device_id,
                body,
            } => {
                let Some(active) = self.active_workers.get(&path_key) else {
                    return;
                };
                if active.worker_id != worker_id || active.device_id != device_id {
                    return;
                }
                let _ = self.manager_tx.send(WorkerEvent::Input { device_id, body });
            }
            WorkerReport::Disconnected {
                worker_id,
                path_key,
                device_id,
            } => {
                if let Some(active) = self.active_workers.get(&path_key) {
                    if active.worker_id == worker_id && active.device_id == device_id {
                        self.active_workers.remove(&path_key);
                        let _ = self.manager_tx.send(WorkerEvent::Disconnected {
                            path_key,
                            device_id,
                        });
                    }
                    return;
                }
                if self
                    .launched_workers
                    .get(&path_key)
                    .is_some_and(|launched| launched.worker_id == worker_id)
                {
                    self.launched_workers.remove(&path_key);
                }
            }
            WorkerReport::Failed {
                worker_id,
                path_key,
                error,
            } => {
                if let Some(active) = self.active_workers.get(&path_key) {
                    if active.worker_id == worker_id {
                        let active = self.active_workers.remove(&path_key).expect("checked");
                        let _ = self.manager_tx.send(WorkerEvent::Disconnected {
                            path_key: path_key.clone(),
                            device_id: active.device_id,
                        });
                        let _ = self
                            .manager_tx
                            .send(WorkerEvent::Failed { path_key, error });
                    }
                    return;
                }
                if self
                    .launched_workers
                    .get(&path_key)
                    .is_some_and(|launched| launched.worker_id == worker_id)
                {
                    self.launched_workers.remove(&path_key);
                    let _ = self
                        .manager_tx
                        .send(WorkerEvent::Failed { path_key, error });
                }
            }
        }
    }

    fn stop_all_workers(&mut self) {
        for launched in self.launched_workers.values() {
            let _ = launched.command_tx.send(RuntimeCommand::Stop);
        }
        for active in self.active_workers.values() {
            let _ = active.command_tx.send(RuntimeCommand::Stop);
        }
        self.launched_workers.clear();
        self.active_workers.clear();
    }
}

async fn enumerate_canonical(backend: Arc<dyn Backend>) -> Result<Vec<DeviceCandidate>> {
    let mut rows = tokio::task::spawn_blocking(move || backend.enumerate())
        .await
        .context("joining enumerate task")??;

    rows.sort_by_key(|descriptor| (descriptor.bus_number, descriptor.address));
    rows.dedup_by_key(|descriptor| (descriptor.bus_number, descriptor.address));
    Ok(rows)
}

fn spawn_device_worker(
    worker_id: u64,
    manager_id: String,
    backend: Arc<dyn Backend>,
    descriptor: DeviceCandidate,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerReport>,
    command_tx: CommandSender,
    command_rx: CommandReceiver,
) {
    let path_key = descriptor.path_key();
    thread::spawn(move || {
        if let Err(error) = device_worker(
            backend,
            worker_id,
            manager_id,
            descriptor,
            worker_tx.clone(),
            command_tx,
            command_rx,
        ) {
            let _ = worker_tx.send(WorkerReport::Failed {
                worker_id,
                path_key,
                error: format!("{error:#}"),
            });
        }
    });
}

fn device_worker(
    backend: Arc<dyn Backend>,
    worker_id: u64,
    manager_id: String,
    descriptor: DeviceCandidate,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerReport>,
    command_tx: CommandSender,
    command_rx: CommandReceiver,
) -> Result<()> {
    let path_key = descriptor.path_key();
    let local_device_id = descriptor.hardware_id();
    let fingerprint = local_device_id.clone();
    let mut handle = backend.open(&descriptor, USB_TIMEOUT)?;
    let runtime_descriptor = if handle.has_hid_input() {
        descriptor.clone()
    } else {
        descriptor.without_hid_input()
    };
    let probe_reply = handle.probe()?;
    debug!(
        "Saitek FIP probe reply request=0x{:02x} header_error=0x{:08x} request_error=0x{:08x}",
        probe_reply.request, probe_reply.header_error, probe_reply.request_error
    );

    worker_tx
        .send(WorkerReport::Connected {
            worker_id,
            path_key: path_key.clone(),
            device_id: local_device_id.clone(),
            command_tx: command_tx.clone(),
            device: device_descriptor(&runtime_descriptor, &local_device_id, &fingerprint),
        })
        .ok();

    let mut previous_mask = 0u16;
    loop {
        while let Ok(command) = command_rx.try_recv() {
            if matches!(command, RuntimeCommand::Stop) {
                let _ = handle.clear_image(DEFAULT_PAGE_ID);
                return Ok(());
            }
            if let Err(error) = apply_runtime_command(&mut *handle, command) {
                let _ = worker_tx.send(WorkerReport::Disconnected {
                    worker_id,
                    path_key: path_key.clone(),
                    device_id: local_device_id.clone(),
                });
                return Err(error);
            }
        }

        match command_rx.recv_timeout(Duration::from_millis(0)) {
            Ok(RuntimeCommand::Stop) => {
                let _ = handle.clear_image(DEFAULT_PAGE_ID);
                return Ok(());
            }
            Ok(command) => {
                if let Err(error) = apply_runtime_command(&mut *handle, command) {
                    let _ = worker_tx.send(WorkerReport::Disconnected {
                        worker_id,
                        path_key: path_key.clone(),
                        device_id: local_device_id.clone(),
                    });
                    return Err(error);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = handle.clear_image(DEFAULT_PAGE_ID);
                return Ok(());
            }
            Err(RecvTimeoutError::Timeout) => {}
        }

        let Some(report) = handle.read_hid_report(READ_TIMEOUT)? else {
            if !handle.has_hid_input() {
                thread::sleep(READ_TIMEOUT);
            }
            continue;
        };
        if report.is_empty() {
            continue;
        }

        let mask = decode_hid_mask(&report, 0, ByteOrder::Big)?;
        for event in changed_events(previous_mask, mask) {
            if let Some(body) =
                translate_hid_event(event, &manager_id, &local_device_id, &fingerprint)
            {
                let _ = worker_tx.send(WorkerReport::Input {
                    worker_id,
                    path_key: path_key.clone(),
                    device_id: local_device_id.clone(),
                    body,
                });
            }
        }
        previous_mask = mask;
    }
}

fn apply_runtime_command(handle: &mut dyn DeviceHandle, command: RuntimeCommand) -> Result<()> {
    match command {
        RuntimeCommand::SetRasterFrame {
            control_id,
            encoding,
            image,
        } => {
            if control_id != SCREEN_CONTROL_ID {
                warn!("Ignoring raster set_frame for unknown control {control_id}");
                return Ok(());
            }
            if !matches!(encoding.as_str(), "jpeg" | "png") {
                bail!("unsupported raster encoding {encoding}");
            }
            let frame = encoded_image_to_fip_frame(&image)?;
            handle.send_image(&frame, DEFAULT_PAGE_ID)?;
        }
        RuntimeCommand::ClearRaster { control_id } => {
            if control_id != SCREEN_CONTROL_ID {
                warn!("Ignoring raster clear for unknown control {control_id}");
                return Ok(());
            }
            handle.clear_image(DEFAULT_PAGE_ID)?;
        }
        RuntimeCommand::ResetDevice => {
            handle.clear_image(DEFAULT_PAGE_ID)?;
        }
        RuntimeCommand::Stop => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use crate::protocol::{FipControlPacket, REQ_CLEAR_IMAGE, REQ_PROBE, REQ_SET_IMAGE};
    use deckr::authority::ContractPointer;
    use deckr::concord::{ConcordCoordinator, ContractHandle, ContractState, CreateContractSpec};
    use deckr::endpoint::{hardware_manager_address, EndpointAddress};
    use deckr::hardware::runtime::{
        HardwareLaneTransport, HardwareManagerRuntimeSpec, HardwareMessageStream,
    };
    use deckr::lanes::{DeviceDescriptor, DeviceRef};
    use deckr::profiles::hardware::{
        HardwareClaimDevice, HardwareClaimTerms, HARDWARE_CLAIM_PROFILE_ID,
    };
    use deckr::state::MemoryStateStore;

    type TestHardwareRuntime = ManagedHardwareRuntime<
        MemoryStateStore,
        MemoryStateStore,
        MemoryStateStore,
        TestHardwareLane,
    >;

    #[derive(Clone)]
    struct TestHardwareLane {
        published: Arc<Mutex<Vec<DeckrMessage>>>,
        inbound_tx: tokio_mpsc::UnboundedSender<DeckrMessage>,
        inbound_rx: Arc<Mutex<Option<tokio_mpsc::UnboundedReceiver<DeckrMessage>>>>,
    }

    impl Default for TestHardwareLane {
        fn default() -> Self {
            let (inbound_tx, inbound_rx) = tokio_mpsc::unbounded_channel();
            Self {
                published: Arc::default(),
                inbound_tx,
                inbound_rx: Arc::new(Mutex::new(Some(inbound_rx))),
            }
        }
    }

    impl TestHardwareLane {
        async fn published(&self) -> Vec<DeckrMessage> {
            self.published.lock().await.clone()
        }

        fn publish_inbound(&self, message: DeckrMessage) {
            self.inbound_tx
                .send(message)
                .expect("test hardware lane should be open");
        }
    }

    impl HardwareLaneTransport for TestHardwareLane {
        async fn publish_hardware_message(&self, message: DeckrMessage) -> deckr::Result<()> {
            self.published.lock().await.push(message);
            Ok(())
        }

        async fn subscribe_hardware_messages<'a>(
            &'a self,
            _endpoint: &'a EndpointAddress,
        ) -> deckr::Result<HardwareMessageStream> {
            let rx = self.inbound_rx.lock().await.take().ok_or_else(|| {
                deckr::Error::Invalid("test hardware lane already subscribed".to_string())
            })?;
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|message| (Ok(message), rx))
            });
            Ok(Box::pin(stream) as HardwareMessageStream)
        }
    }

    struct ManagedHarness {
        runtime: TestHardwareRuntime,
        concord: ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
        token_state: MemoryStateStore,
        lane: TestHardwareLane,
        handler: Arc<SaitekHardwareHandler>,
    }

    async fn managed_harness() -> ManagedHarness {
        let beacon_state = MemoryStateStore::ttl_bound(30).unwrap();
        let contract_state = MemoryStateStore::new();
        let token_state = MemoryStateStore::ttl_bound(30).unwrap();
        let lane = TestHardwareLane::default();
        let handler = Arc::new(SaitekHardwareHandler::default());
        let concord = ConcordCoordinator::new(contract_state.clone(), token_state.clone());
        let runtime = ManagedHardwareRuntime::new(HardwareManagerRuntimeSpec {
            manager_id: "saitek-main".to_string(),
            session_id: "manager-session".to_string(),
            labels: BTreeMap::new(),
            beacon_state,
            concord_contract_state: contract_state,
            concord_token_state: token_state.clone(),
            lane: lane.clone(),
            maintenance_policy: StateMaintenancePolicy {
                renewal_interval: Duration::from_secs(3600),
                concord_token_refresh_interval: Duration::from_secs(3600),
                reconcile_interval: Duration::from_millis(20),
            },
            command_handler: handler.clone(),
            reset_handler: Some(handler.clone()),
        })
        .await
        .unwrap();

        ManagedHarness {
            runtime,
            concord,
            token_state,
            lane,
            handler,
        }
    }

    fn manager_endpoint() -> EndpointAddress {
        EndpointAddress::parse(hardware_manager_address("saitek-main")).unwrap()
    }

    async fn create_claim(
        concord: &ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
        contract_id: &str,
        device_id: &str,
        fingerprint: Option<&str>,
    ) -> ContractHandle {
        let controller = EndpointAddress::parse("controller:main").unwrap();
        let manager = manager_endpoint();
        let terms = HardwareClaimTerms {
            profile: HARDWARE_CLAIM_PROFILE_ID.to_string(),
            claim_id: format!("claim-{contract_id}"),
            controller_endpoint: controller.clone(),
            manager_endpoint: manager.clone(),
            devices: vec![HardwareClaimDevice {
                device_ref: DeviceRef {
                    manager_id: "saitek-main".to_string(),
                    device_id: device_id.to_string(),
                    fingerprint: fingerprint.map(ToString::to_string),
                },
                instance_count: 1,
            }],
        };
        let contract = concord
            .create_contract(CreateContractSpec {
                participants: vec![controller.clone(), manager],
                contract_id: Some(contract_id.to_string()),
                generation: 1,
                profile: Some(HARDWARE_CLAIM_PROFILE_ID.to_string()),
                terms: Some(terms.to_value().unwrap()),
                created_by: Some(controller.clone()),
                supersedes: None,
            })
            .await
            .unwrap();
        concord
            .attach(
                &contract,
                &controller,
                "controller-session",
                Some(format!("controller-token-{contract_id}")),
            )
            .await
            .unwrap();
        for _ in 0..100 {
            if concord
                .participant_token(&contract, &manager_endpoint())
                .await
                .unwrap()
                .is_some()
            {
                return contract;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("manager token was not attached for {contract_id}");
    }

    async fn start_managed_runtime(h: &ManagedHarness) -> JoinSet<deckr::Result<()>> {
        let mut tasks = JoinSet::new();
        h.runtime.start(&mut tasks).await.unwrap();
        tasks
    }

    async fn stop_managed_runtime(h: &ManagedHarness, mut tasks: JoinSet<deckr::Result<()>>) {
        h.runtime.stop().await.unwrap();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    async fn wait_for_published(h: &ManagedHarness, count: usize) -> Vec<DeckrMessage> {
        for _ in 0..100 {
            let published = h.lane.published().await;
            if published.len() >= count {
                return published;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("hardware lane did not publish {count} messages");
    }

    async fn wait_for_command_rejection_after(
        h: &ManagedHarness,
        after_count: usize,
        expected_reason: &str,
        expected_message: &str,
    ) {
        for _ in 0..100 {
            let published = h.lane.published().await;
            if published.iter().skip(after_count).any(|published_message| {
                matches!(
                    published_message.hardware_body(),
                    Ok(HardwareMessageBody::CommandRejected {
                        reason,
                        message,
                        ..
                    }) if reason == expected_reason
                        && message.as_deref() == Some(expected_message)
                )
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("hardware lane did not publish {expected_reason} rejection");
    }

    fn wait_for_runtime_command(
        command_rx: &CommandReceiver,
        expected: &str,
        predicate: impl Fn(&RuntimeCommand) -> bool,
    ) -> RuntimeCommand {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut skipped_count = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for {expected} after skipping {skipped_count} commands"
            );
            match command_rx.recv_timeout(remaining) {
                Ok(command) if predicate(&command) => return command,
                Ok(_) => skipped_count += 1,
                Err(error) => panic!(
                    "failed waiting for {expected} after skipping {skipped_count} commands: {error:?}"
                ),
            }
        }
    }

    fn test_device_descriptor(device_id: &str, fingerprint: &str) -> DeviceDescriptor {
        device_descriptor(&sample_candidate(), device_id, fingerprint)
    }

    #[derive(Clone)]
    struct FakeBackend {
        enumerate_rows: Arc<StdMutex<Vec<DeviceCandidate>>>,
        device: Arc<StdMutex<FakeDeviceState>>,
        open_count: Arc<StdMutex<usize>>,
    }

    struct FakeDeviceState {
        reports: VecDeque<Vec<u8>>,
        sent_frames: Vec<Vec<u8>>,
        commands: Vec<u32>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                enumerate_rows: Arc::new(StdMutex::new(vec![sample_candidate()])),
                device: Arc::new(StdMutex::new(FakeDeviceState {
                    reports: VecDeque::new(),
                    sent_frames: Vec::new(),
                    commands: Vec::new(),
                })),
                open_count: Arc::new(StdMutex::new(0)),
            }
        }

        fn push_report(&self, report: Vec<u8>) {
            self.device.lock().unwrap().reports.push_back(report);
        }

        fn sent_frames(&self) -> Vec<Vec<u8>> {
            self.device.lock().unwrap().sent_frames.clone()
        }

        fn commands(&self) -> Vec<u32> {
            self.device.lock().unwrap().commands.clone()
        }

        fn open_count(&self) -> usize {
            *self.open_count.lock().unwrap()
        }
    }

    fn sample_candidate() -> DeviceCandidate {
        DeviceCandidate {
            bus_number: 1,
            address: 2,
            vendor_id: 0x06a3,
            product_id: 0xa2ae,
            manufacturer: Some("Logitech".to_string()),
            product: Some("Flight Instrument Panel".to_string()),
            serial_number: Some("serial".to_string()),
            vendor_interface: 1,
            vendor_bulk_out: 0x02,
            vendor_bulk_in: 0x82,
            vendor_out_packet_size: 512,
            hid_interface: Some(0),
            hid_interrupt_in: Some(0x81),
            hid_read_size: 2,
        }
    }

    impl Backend for FakeBackend {
        fn enumerate(&self) -> Result<Vec<DeviceCandidate>> {
            Ok(self.enumerate_rows.lock().unwrap().clone())
        }

        fn open(
            &self,
            _candidate: &DeviceCandidate,
            _timeout: Duration,
        ) -> Result<Box<dyn DeviceHandle>> {
            *self.open_count.lock().unwrap() += 1;
            Ok(Box::new(FakeHandle {
                state: self.device.clone(),
            }))
        }
    }

    struct FakeHandle {
        state: Arc<StdMutex<FakeDeviceState>>,
    }

    impl DeviceHandle for FakeHandle {
        fn has_hid_input(&self) -> bool {
            true
        }

        fn probe(&mut self) -> Result<FipControlPacket> {
            self.state.lock().unwrap().commands.push(REQ_PROBE);
            Ok(FipControlPacket {
                request: REQ_PROBE,
                ..FipControlPacket::default()
            })
        }

        fn clear_image(&mut self, _page: u32) -> Result<FipControlPacket> {
            self.state.lock().unwrap().commands.push(REQ_CLEAR_IMAGE);
            Ok(FipControlPacket {
                request: REQ_CLEAR_IMAGE,
                ..FipControlPacket::default()
            })
        }

        fn send_image(&mut self, frame: &[u8], page: u32) -> Result<FipControlPacket> {
            let mut state = self.state.lock().unwrap();
            state.commands.push(REQ_SET_IMAGE);
            state.sent_frames.push(frame.to_vec());
            Ok(FipControlPacket {
                request: REQ_SET_IMAGE,
                page,
                ..FipControlPacket::default()
            })
        }

        fn set_led(&mut self, _page: u32, _index: u32, _value: bool) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }

        fn read_hid_report(&mut self, _timeout: Duration) -> Result<Option<Vec<u8>>> {
            if let Some(report) = self.state.lock().unwrap().reports.pop_front() {
                return Ok(Some(report));
            }
            thread::sleep(_timeout);
            Ok(None)
        }
    }

    fn raster_command(command_type: &str) -> HardwareMessageBody {
        let mut params = serde_json::Map::new();
        if command_type == "set_frame" {
            params.insert(
                "image".to_string(),
                serde_json::Value::String(STANDARD.encode(make_png())),
            );
            params.insert(
                "encoding".to_string(),
                serde_json::Value::String("png".to_string()),
            );
        }
        HardwareMessageBody::ControlCommand {
            device_ref: DeviceRef {
                manager_id: "saitek-main".to_string(),
                device_id: "fip".to_string(),
                fingerprint: None,
            },
            control_id: Some(SCREEN_CONTROL_ID.to_string()),
            capability_id: RASTER_CAPABILITY_ID.to_string(),
            command_type: command_type.to_string(),
            params,
        }
    }

    fn raster_command_for_device(command_type: &str, device_id: &str) -> HardwareMessageBody {
        let mut body = raster_command(command_type);
        if let HardwareMessageBody::ControlCommand { device_ref, .. } = &mut body {
            device_ref.device_id = device_id.to_string();
            device_ref.fingerprint = Some(device_id.to_string());
        }
        body
    }

    fn contract_pointer(contract_id: &str) -> ContractPointer {
        ContractPointer {
            contract_id: contract_id.to_string(),
            generation: 1,
        }
    }

    fn make_png() -> Vec<u8> {
        use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb};
        use std::io::Cursor;

        let image = ImageBuffer::from_pixel(
            crate::protocol::WIDTH as u32,
            crate::protocol::HEIGHT as u32,
            Rgb([0, 0, 0]),
        );
        let mut png = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut png, ImageFormat::Png)
            .unwrap();
        png.into_inner()
    }

    fn supervisor_for_tests() -> (Supervisor, tokio_mpsc::UnboundedReceiver<WorkerEvent>) {
        let (worker_tx, worker_rx) = tokio_mpsc::unbounded_channel::<WorkerReport>();
        let (manager_tx, manager_rx) = tokio_mpsc::unbounded_channel::<WorkerEvent>();
        (
            Supervisor::new(
                "saitek-main".to_string(),
                Arc::new(FakeBackend::new()),
                worker_tx,
                worker_rx,
                manager_tx,
            ),
            manager_rx,
        )
    }

    async fn recv_worker_report(
        worker_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerReport>,
    ) -> WorkerReport {
        tokio::time::timeout(Duration::from_secs(3), worker_rx.recv())
            .await
            .expect("worker report should arrive")
            .expect("worker report channel should stay open")
    }

    async fn recv_worker_event(
        manager_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerEvent>,
    ) -> WorkerEvent {
        tokio::time::timeout(Duration::from_secs(3), manager_rx.recv())
            .await
            .expect("worker event should arrive")
            .expect("worker event channel should stay open")
    }

    async fn forward_worker_event(h: &ManagedHarness, event: WorkerEvent) {
        match event {
            WorkerEvent::Connected {
                device_id,
                command_tx,
                device,
                ..
            } => {
                h.handler
                    .register_device(device_id.clone(), command_tx)
                    .await;
                h.runtime.set_device(device).await.unwrap();
            }
            WorkerEvent::Input { body, .. } => {
                h.runtime.handle_hardware_message(body).await.unwrap();
            }
            WorkerEvent::Disconnected { device_id, .. } => {
                h.handler.remove_device(&device_id).await;
                h.runtime
                    .remove_device(&device_id, "disconnected")
                    .await
                    .unwrap();
            }
            WorkerEvent::Failed { error, .. } => panic!("device worker failed: {error}"),
        }
    }

    async fn wait_for_command(backend: &FakeBackend, request: u32) {
        for _ in 0..100 {
            if backend.commands().contains(&request) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fake backend did not record command 0x{request:02x}");
    }

    fn assert_next_disconnected(
        manager_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerEvent>,
        expected_path: &str,
        expected_device: &str,
    ) {
        match manager_rx.try_recv().unwrap() {
            WorkerEvent::Disconnected {
                path_key,
                device_id,
            } => {
                assert_eq!(path_key, expected_path);
                assert_eq!(device_id, expected_device);
            }
            other => panic!("expected disconnected event, got {other:?}"),
        }
    }

    #[test]
    fn command_queue_keeps_latest_raster_frame_per_control() {
        let (command_tx, command_rx) = command_channel();

        for value in 0..COMMAND_QUEUE_CAPACITY * 4 {
            command_tx
                .send(RuntimeCommand::SetRasterFrame {
                    control_id: SCREEN_CONTROL_ID.to_string(),
                    encoding: "png".to_string(),
                    image: vec![value as u8],
                })
                .unwrap();
        }

        match command_rx.try_recv().unwrap() {
            RuntimeCommand::SetRasterFrame {
                control_id, image, ..
            } => {
                assert_eq!(control_id, SCREEN_CONTROL_ID);
                assert_eq!(image, vec![(COMMAND_QUEUE_CAPACITY * 4 - 1) as u8]);
            }
            other => panic!("expected latest raster frame, got {other:?}"),
        }
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn command_queue_preserves_stop_priority() {
        let (command_tx, command_rx) = command_channel();

        for index in 0..COMMAND_QUEUE_CAPACITY * 4 {
            command_tx
                .send(RuntimeCommand::SetRasterFrame {
                    control_id: format!("control-{index}"),
                    encoding: "png".to_string(),
                    image: vec![index as u8],
                })
                .unwrap();
        }
        command_tx.send(RuntimeCommand::Stop).unwrap();

        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::Stop
        ));
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn command_queue_preserves_reset_priority() {
        let (command_tx, command_rx) = command_channel();

        command_tx
            .send(RuntimeCommand::SetRasterFrame {
                control_id: SCREEN_CONTROL_ID.to_string(),
                encoding: "png".to_string(),
                image: b"stale".to_vec(),
            })
            .unwrap();
        command_tx.send(RuntimeCommand::ResetDevice).unwrap();

        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::ResetDevice
        ));
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn supervisor_disconnects_active_worker_when_path_disappears() {
        let (mut supervisor, mut manager_rx) = supervisor_for_tests();
        let path_key = sample_candidate().path_key();
        let device_id = sample_candidate().hardware_id();
        let (command_tx, command_rx) = command_channel();
        supervisor.active_workers.insert(
            path_key.clone(),
            ActiveWorker {
                worker_id: 1,
                device_id: device_id.clone(),
                command_tx,
            },
        );

        supervisor.reconcile_usb_presence(vec![]);

        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::Stop
        ));
        assert_next_disconnected(&mut manager_rx, &path_key, &device_id);
        assert!(supervisor.active_workers.is_empty());
        assert!(manager_rx.try_recv().is_err());
    }

    #[test]
    fn supervisor_disconnects_active_worker_before_forwarding_failure() {
        let (mut supervisor, mut manager_rx) = supervisor_for_tests();
        let path_key = sample_candidate().path_key();
        let device_id = sample_candidate().hardware_id();
        let (command_tx, _command_rx) = command_channel();
        supervisor.active_workers.insert(
            path_key.clone(),
            ActiveWorker {
                worker_id: 7,
                device_id: device_id.clone(),
                command_tx,
            },
        );

        supervisor.handle_worker_report(WorkerReport::Failed {
            worker_id: 7,
            path_key: path_key.clone(),
            error: "read failed".to_string(),
        });

        assert_next_disconnected(&mut manager_rx, &path_key, &device_id);
        match manager_rx.try_recv().unwrap() {
            WorkerEvent::Failed {
                path_key: failed_path,
                error,
            } => {
                assert_eq!(failed_path, path_key);
                assert_eq!(error, "read failed");
            }
            other => panic!("expected failed event, got {other:?}"),
        }
        assert!(supervisor.active_workers.is_empty());
    }

    #[test]
    fn supervisor_ignores_stale_worker_events_after_reconnect_same_device() {
        let (mut supervisor, mut manager_rx) = supervisor_for_tests();
        let path_key = sample_candidate().path_key();
        let device_id = sample_candidate().hardware_id();
        let (old_command_tx, old_command_rx) = command_channel();
        supervisor.active_workers.insert(
            path_key.clone(),
            ActiveWorker {
                worker_id: 1,
                device_id: device_id.clone(),
                command_tx: old_command_tx,
            },
        );
        supervisor.reconcile_usb_presence(vec![]);
        assert!(matches!(
            old_command_rx.try_recv().unwrap(),
            RuntimeCommand::Stop
        ));
        assert_next_disconnected(&mut manager_rx, &path_key, &device_id);

        let (new_command_tx, _new_command_rx) = command_channel();
        supervisor.active_workers.insert(
            path_key.clone(),
            ActiveWorker {
                worker_id: 2,
                device_id: device_id.clone(),
                command_tx: new_command_tx,
            },
        );

        supervisor.handle_worker_report(WorkerReport::Disconnected {
            worker_id: 1,
            path_key: path_key.clone(),
            device_id: device_id.clone(),
        });
        supervisor.handle_worker_report(WorkerReport::Failed {
            worker_id: 1,
            path_key: path_key.clone(),
            error: "late failure".to_string(),
        });

        assert!(manager_rx.try_recv().is_err());
        let active = supervisor.active_workers.get(&path_key).unwrap();
        assert_eq!(active.worker_id, 2);
        assert_eq!(active.device_id, device_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fake_usb_worker_routes_claimed_input_through_managed_runtime() {
        let h = managed_harness().await;
        let tasks = start_managed_runtime(&h).await;
        let backend = FakeBackend::new();
        let candidate = sample_candidate();
        let path_key = candidate.path_key();
        let device_id = candidate.hardware_id();
        let (worker_tx, worker_rx) = tokio_mpsc::unbounded_channel::<WorkerReport>();
        let (manager_tx, mut manager_rx) = tokio_mpsc::unbounded_channel::<WorkerEvent>();
        let mut supervisor = Supervisor::new(
            "saitek-main".to_string(),
            Arc::new(backend.clone()),
            worker_tx,
            worker_rx,
            manager_tx,
        );

        supervisor.reconcile_usb_presence(vec![candidate]);
        let report = recv_worker_report(&mut supervisor.worker_rx).await;
        supervisor.handle_worker_report(report);
        let connected = recv_worker_event(&mut manager_rx).await;
        match &connected {
            WorkerEvent::Connected {
                path_key: actual_path,
                device_id: actual_device,
                ..
            } => {
                assert_eq!(actual_path, &path_key);
                assert_eq!(actual_device, &device_id);
            }
            other => panic!("expected connected worker event, got {other:?}"),
        }
        forward_worker_event(&h, connected).await;
        assert_eq!(backend.open_count(), 1);
        assert_eq!(backend.commands(), [REQ_PROBE]);

        create_claim(&h.concord, "claim-a", &device_id, Some(&device_id)).await;
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                &device_id,
                contract_pointer("claim-a"),
                raster_command_for_device("set_frame", &device_id),
            )
            .unwrap(),
        );
        wait_for_command(&backend, REQ_SET_IMAGE).await;
        assert_eq!(backend.open_count(), 1);
        assert_eq!(backend.sent_frames()[0].len(), crate::protocol::FRAME_BYTES);

        backend.push_report(vec![0x01, 0x00]);
        let report = recv_worker_report(&mut supervisor.worker_rx).await;
        supervisor.handle_worker_report(report);
        let input = recv_worker_event(&mut manager_rx).await;
        forward_worker_event(&h, input).await;

        let published = wait_for_published(&h, 1).await;
        let routed = published.last().unwrap();
        assert_eq!(routed.recipient_endpoint(), Some("controller:main"));
        assert_eq!(
            routed.recipient_session_id.as_deref(),
            Some("controller-session")
        );
        assert_eq!(routed.contract.as_ref(), Some(&contract_pointer("claim-a")));
        match routed.hardware_body().unwrap() {
            HardwareMessageBody::ControlInput {
                device_ref,
                control_id,
                capability_id,
                event_type,
                value,
                ..
            } => {
                assert_eq!(device_ref.device_id, device_id);
                assert_eq!(device_ref.fingerprint.as_deref(), Some(device_id.as_str()));
                assert_eq!(control_id, "s1");
                assert_eq!(capability_id, "button.momentary");
                assert_eq!(event_type, "down");
                assert_eq!(value, Some(serde_json::json!({"eventType": "down"})));
            }
            other => panic!("expected routed control input, got {other:?}"),
        }

        supervisor.stop_all_workers();
        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_runtime_accepts_claim_and_delivers_command_without_duplicate_token() {
        let h = managed_harness().await;
        let tasks = start_managed_runtime(&h).await;
        let (command_tx, command_rx) = command_channel();
        h.handler
            .register_device("fip".to_string(), command_tx)
            .await;
        h.runtime
            .set_device(test_device_descriptor("fip", "fingerprint:fip"))
            .await
            .unwrap();
        let contract = create_claim(&h.concord, "claim-a", "fip", Some("fingerprint:fip")).await;

        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-a"),
                raster_command("set_frame"),
            )
            .unwrap(),
        );

        wait_for_runtime_command(&command_rx, "raster frame", |command| {
            matches!(command, RuntimeCommand::SetRasterFrame { .. })
        });
        let token = h
            .concord
            .participant_token(&contract, &manager_endpoint())
            .await
            .unwrap()
            .expect("manager token should be attached");
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-a"),
                raster_command("set_frame"),
            )
            .unwrap(),
        );
        wait_for_runtime_command(&command_rx, "raster frame", |command| {
            matches!(command, RuntimeCommand::SetRasterFrame { .. })
        });
        let refreshed = h
            .concord
            .participant_token(&contract, &manager_endpoint())
            .await
            .unwrap()
            .expect("manager token should still be attached");
        assert_eq!(refreshed.key, token.key);
        assert_eq!(refreshed.refresh_seq, token.refresh_seq);

        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_runtime_rejects_wrong_controller_and_unsupported_command() {
        let h = managed_harness().await;
        let tasks = start_managed_runtime(&h).await;
        let (command_tx, command_rx) = command_channel();
        h.handler
            .register_device("fip".to_string(), command_tx)
            .await;
        h.runtime
            .set_device(test_device_descriptor("fip", "fingerprint:fip"))
            .await
            .unwrap();
        create_claim(&h.concord, "claim-a", "fip", Some("fingerprint:fip")).await;

        let published_before = h.lane.published().await.len();
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "other",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-a"),
                raster_command("set_frame"),
            )
            .unwrap(),
        );
        wait_for_command_rejection_after(
            &h,
            published_before,
            "unauthorized",
            "Hardware command unauthorized",
        )
        .await;
        assert!(command_rx.try_recv().is_err());

        let published_before = h.lane.published().await.len();
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-a"),
                raster_command("unsupported"),
            )
            .unwrap(),
        );
        wait_for_command_rejection_after(
            &h,
            published_before,
            "unsupported",
            "Hardware command unsupported",
        )
        .await;

        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_runtime_resets_on_lost_claim_and_cancels_on_disconnect() {
        let h = managed_harness().await;
        let tasks = start_managed_runtime(&h).await;
        let (command_tx, command_rx) = command_channel();
        h.handler
            .register_device("fip".to_string(), command_tx)
            .await;
        h.runtime
            .set_device(test_device_descriptor("fip", "fingerprint:fip"))
            .await
            .unwrap();
        let first = create_claim(&h.concord, "claim-a", "fip", Some("fingerprint:fip")).await;
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-a"),
                raster_command("set_frame"),
            )
            .unwrap(),
        );
        wait_for_runtime_command(&command_rx, "raster frame", |command| {
            matches!(command, RuntimeCommand::SetRasterFrame { .. })
        });

        h.concord
            .cancel(
                &first,
                &EndpointAddress::parse("controller:main").unwrap(),
                Some("transferred".to_string()),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        wait_for_runtime_command(&command_rx, "reset device", |command| {
            matches!(command, RuntimeCommand::ResetDevice)
        });

        let second = create_claim(&h.concord, "claim-b", "fip", Some("fingerprint:fip")).await;
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer("claim-b"),
                raster_command("set_frame"),
            )
            .unwrap(),
        );
        wait_for_runtime_command(&command_rx, "raster frame", |command| {
            matches!(command, RuntimeCommand::SetRasterFrame { .. })
        });
        h.handler.remove_device("fip").await;
        h.runtime
            .remove_device("fip", "disconnected")
            .await
            .unwrap();
        let record = h.concord.contract_record(&second).await.unwrap().unwrap();
        assert_eq!(record.state, ContractState::Cancelled);
        assert_eq!(
            record.cancel_reason.as_deref(),
            Some("hardware device fip disconnected")
        );

        stop_managed_runtime(&h, tasks).await;
    }

    #[test]
    fn runtime_command_from_hardware_body_maps_outputs() {
        assert!(matches!(
            runtime_command_from_body(raster_command("set_frame")).unwrap(),
            RuntimeCommand::SetRasterFrame { .. }
        ));
        assert!(matches!(
            runtime_command_from_body(raster_command("clear")).unwrap(),
            RuntimeCommand::ClearRaster { .. }
        ));
    }

    #[test]
    fn runtime_command_from_hardware_body_rejects_under_shaped_params() {
        let mut missing_encoding = raster_command("set_frame");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut missing_encoding {
            params.remove("encoding");
        }
        assert!(runtime_command_from_body(missing_encoding).is_err());

        let mut invalid_encoding = raster_command("set_frame");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut invalid_encoding {
            params.insert(
                "encoding".to_string(),
                serde_json::Value::String("gif".to_string()),
            );
        }
        assert!(runtime_command_from_body(invalid_encoding).is_err());

        let mut non_empty_clear = raster_command("clear");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut non_empty_clear {
            params.insert("unexpected".to_string(), serde_json::Value::Bool(true));
        }
        assert!(runtime_command_from_body(non_empty_clear).is_err());
    }

    #[test]
    fn reset_device_clears_screen() {
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };

        apply_runtime_command(&mut handle, RuntimeCommand::ResetDevice).unwrap();

        assert_eq!(backend.commands(), [REQ_CLEAR_IMAGE]);
    }

    #[test]
    fn set_raster_frame_converts_png_and_sends_image() {
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };

        apply_runtime_command(
            &mut handle,
            RuntimeCommand::SetRasterFrame {
                control_id: SCREEN_CONTROL_ID.to_string(),
                encoding: "png".to_string(),
                image: make_png(),
            },
        )
        .unwrap();

        assert_eq!(backend.commands(), [REQ_SET_IMAGE]);
        assert_eq!(backend.sent_frames()[0].len(), crate::protocol::FRAME_BYTES);
    }
}
