#![cfg_attr(test, allow(dead_code))]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
#[cfg(test)]
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::thread;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use deckr::hardware::runtime::{
    HardwareCommandContext, HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome,
    HardwareManagerRuntime as ManagedHardwareRuntime, HardwareResetFuture, HardwareResetHandler,
};
use deckr::lanes::HardwareMessageBody;
use deckr::lifecycle::Cancellation;
use deckr::nats::{DeckrRuntime, NatsDeckrRuntime, NatsStateStore};
use deckr::state::StateMaintenancePolicy;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::backend::{Backend, DeviceCandidate, DeviceHandle, UsbBackend};
use crate::descriptor::{
    device_descriptor, led_index, translate_hid_event, DEFAULT_PAGE_ID, LED_CAPABILITY_ID,
    RASTER_CAPABILITY_ID, SCREEN_CONTROL_ID,
};
use crate::image::encoded_image_to_fip_frame;
use crate::protocol::{changed_events, decode_hid_mask, ByteOrder};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const USB_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BACKOFF_SECS: u64 = 10;
const COMMAND_QUEUE_CAPACITY: usize = 32;
const OUTPUT_BATCH_SIZE: usize = 4;
const BUSY_READ_TIMEOUT: Duration = Duration::from_millis(1);

type ManagedNatsHardwareRuntime =
    ManagedHardwareRuntime<NatsStateStore, NatsStateStore, NatsStateStore, NatsDeckrRuntime>;

#[derive(Debug, Clone)]
enum HardwareOutput {
    SetLed {
        control_id: String,
        on: bool,
    },
    SetRasterFrame {
        control_id: String,
        encoding: String,
        image: Vec<u8>,
    },
    ClearRaster {
        control_id: String,
    },
}

#[derive(Clone)]
struct AuthorizedOutput {
    output: HardwareOutput,
    context: Box<HardwareCommandContext>,
}

impl std::fmt::Debug for AuthorizedOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedOutput")
            .field("output", &self.output)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
enum RuntimeCommand {
    Hardware(AuthorizedOutput),
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

    fn is_raster_frame(&self) -> bool {
        matches!(
            self,
            Self::Hardware(AuthorizedOutput {
                output: HardwareOutput::SetRasterFrame { .. },
                ..
            })
        )
    }

    fn output_control_id(&self) -> Option<&str> {
        match self {
            Self::Hardware(AuthorizedOutput { output, .. }) => match output {
                HardwareOutput::SetRasterFrame { control_id, .. }
                | HardwareOutput::ClearRaster { control_id }
                | HardwareOutput::SetLed { control_id, .. } => Some(control_id),
            },
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
    stopping: bool,
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
            stopping: false,
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
            state.stopping = true;
            state.queue.clear();
            state.queue.push_front(command);
            self.inner.available.notify_all();
            return Ok(());
        }

        if state.stopping {
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

    #[cfg(test)]
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
    if let Some(control_id) = command.output_control_id().map(ToOwned::to_owned) {
        queue.retain(|queued| queued.output_control_id() != Some(control_id.as_str()));
    }
    while queue.len() >= COMMAND_QUEUE_CAPACITY {
        if let Some(index) = queue.iter().position(RuntimeCommand::is_raster_frame) {
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
    fn handle_authorized_hardware_command<'a>(
        &'a self,
        context: HardwareCommandContext,
    ) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
            context.authorize()?;
            let body = context.message().hardware_body()?;
            let device_id = body.device_ref().device_id.clone();
            let command = match hardware_output_from_body(body) {
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
            context.authorize()?;
            if sender
                .send(RuntimeCommand::Hardware(AuthorizedOutput {
                    output: command,
                    context: Box::new(context),
                }))
                .is_err()
            {
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
        device: Box<deckr::lanes::DeviceDescriptor>,
    },
    Input {
        device_id: String,
        body: Box<HardwareMessageBody>,
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
        device: Box<deckr::lanes::DeviceDescriptor>,
    },
    Input {
        worker_id: u64,
        path_key: String,
        device_id: String,
        body: Box<HardwareMessageBody>,
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
        self.run_until(shutdown_signal()).await
    }

    async fn run_until(&self, signal: impl Future<Output = Result<()>>) -> Result<()> {
        let shutdown = Cancellation::default();
        let run = retry_sessions(&self.manager_id, &shutdown, || {
            self.run_connected_session(&shutdown)
        });
        tokio::pin!(run);
        tokio::select! {
            biased;
            result = signal => {
                info!("Shutting down Saitek manager {}", self.manager_id);
                shutdown.cancel();
                // Shutdown remains terminal even when remote cleanup fails.
                let cleanup = run.await;
                result.and(cleanup)
            }
            result = &mut run => result,
        }
    }

    async fn run_connected_session(&self, shutdown: &Cancellation) -> Result<()> {
        let deckr_runtime = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            result = DeckrRuntime::connect(self.nats_url.as_str()) => {
                result.with_context(|| format!("connecting manager {} to NATS", self.manager_id))?
            }
        };
        info!("Connected manager {} to NATS", self.manager_id);

        let hardware_handler = Arc::new(SaitekHardwareHandler::default());
        let command_handler: Arc<dyn HardwareCommandHandler> = hardware_handler.clone();
        let reset_handler: Arc<dyn HardwareResetHandler> = hardware_handler.clone();
        let setup = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            result = ManagedHardwareRuntime::from_deckr_runtime(
                &deckr_runtime,
                self.manager_id.clone(),
                self.session_id.clone(),
                BTreeMap::new(),
                command_handler,
                Some(reset_handler),
                self.state_policy.clone(),
            ) => Some(result),
        };
        let hardware_runtime = match setup {
            Some(Ok(runtime)) => runtime,
            result => {
                deckr_runtime.nats().close_sources().await;
                return result
                    .transpose()
                    .map(|_| ())
                    .context("starting managed Saitek hardware runtime");
            }
        };
        let mut hardware_tasks = JoinSet::<deckr::Result<()>>::new();
        let startup = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            result = hardware_runtime.start(&mut hardware_tasks) => Some(result),
        };
        if !matches!(startup, Some(Ok(()))) {
            let cleanup = hardware_runtime.stop().await;
            while hardware_tasks.join_next().await.is_some() {}
            deckr_runtime.nats().close_sources().await;
            return startup
                .transpose()
                .map(|_| ())
                .and(cleanup)
                .context("starting managed Saitek hardware runtime tasks");
        }

        let (supervisor_event_tx, supervisor_event_rx) = tokio_mpsc::unbounded_channel();
        let (manager_event_tx, manager_event_rx) = tokio_mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let supervisor = Supervisor::new(
            self.manager_id.clone(),
            self.backend.clone(),
            supervisor_event_tx,
            supervisor_event_rx,
            manager_event_tx,
        );
        let mut supervisor_handle = tokio::spawn(supervisor.run(shutdown_rx));
        let mut tasks = JoinSet::<Result<()>>::new();
        tasks.spawn(worker_event_loop(
            hardware_runtime.clone(),
            hardware_handler,
            manager_event_rx,
        ));

        let mut supervisor_finished = false;
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => Ok(()),
            result = &mut supervisor_handle => {
                supervisor_finished = true;
                result.context("joining device supervisor")
                    .and_then(|result| result)
                    .and_then(|_| Err(anyhow::anyhow!("device supervisor stopped unexpectedly")))
            }
            result = tasks.join_next() => match result {
                Some(Ok(Err(error))) => Err(error),
                Some(Err(error)) => Err(error).context("joining manager runtime task"),
                _ => Err(anyhow::anyhow!("manager runtime task stopped unexpectedly")),
            },
            result = hardware_tasks.join_next() => match result {
                Some(Ok(Err(error))) => Err(error).context("managed Saitek hardware runtime task"),
                Some(Err(error)) => Err(error).context("joining managed Saitek hardware runtime task"),
                _ => Err(anyhow::anyhow!("managed Saitek hardware runtime task stopped unexpectedly")),
            },
        };
        let _ = shutdown_tx.send(());
        tasks.abort_all();
        // Revoke protocol authority while the USB workers finish their local resets.
        // The core owns its task shutdown and bounds remote cleanup independently.
        let (hardware_cleanup, usb_cleanup) = tokio::join!(hardware_runtime.stop(), async {
            if supervisor_finished {
                return Ok(());
            }
            supervisor_handle
                .await
                .context("joining device supervisor")?
        });
        while tasks.join_next().await.is_some() {}
        while hardware_tasks.join_next().await.is_some() {}
        deckr_runtime.nats().close_sources().await;
        result
            .and(usb_cleanup)
            .and(hardware_cleanup.map_err(Into::into))
    }
}

async fn retry_sessions<F, Fut>(
    manager_id: &str,
    shutdown: &Cancellation,
    mut session: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = 1u64;
    while !shutdown.is_cancelled() {
        match session().await {
            Ok(()) => return Ok(()),
            Err(error) if shutdown.is_cancelled() => return Err(error),
            Err(error) => error!("Manager {manager_id} stopped; retrying in {backoff}s: {error:#}"),
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = time::sleep(Duration::from_secs(backoff)) => {},
        }
        backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt = signal(SignalKind::interrupt()).context("installing SIGINT handler")?;
        let mut terminate =
            signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
        Ok(())
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("waiting for shutdown signal")
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
                hardware_runtime.set_device(*device).await?;
            }
            WorkerEvent::Input { device_id, body } => {
                debug!("Routing Saitek hardware input for {device_id}");
                hardware_runtime.handle_hardware_message(*body).await?;
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

fn hardware_output_from_body(body: HardwareMessageBody) -> Result<HardwareOutput> {
    match body {
        HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            params,
            ..
        } if capability_id == LED_CAPABILITY_ID && command_type == "set" => {
            let control_id = control_id.context("LED set requires controlId")?;
            if led_index(&control_id).is_none() {
                bail!("unknown LED control {control_id}");
            }
            let on = params
                .get("on")
                .and_then(|value| value.as_bool())
                .context("LED set requires boolean on")?;
            if params.len() != 1 {
                bail!("LED set only accepts on");
            }
            Ok(HardwareOutput::SetLed { control_id, on })
        }
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
            Ok(HardwareOutput::SetRasterFrame {
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
            Ok(HardwareOutput::ClearRaster { control_id })
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
    workers: Vec<OwnedWorker>,
}

struct OwnedWorker {
    worker_id: u64,
    path_key: String,
    command_tx: CommandSender,
    thread: thread::JoinHandle<()>,
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
            workers: Vec::new(),
        }
    }

    async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        let mut discovery = time::interval(DISCOVERY_INTERVAL);

        let result = loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break Ok(());
                }
                _ = discovery.tick() => {
                    let descriptors = tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => break Ok(()),
                        result = enumerate_canonical(self.backend.clone()) => match result {
                            Ok(descriptors) => descriptors,
                            Err(error) => break Err(error),
                        }
                    };
                    self.reap_finished_workers();
                    self.reconcile_usb_presence(descriptors);
                }
                maybe_event = self.worker_rx.recv() => {
                    let Some(event) = maybe_event else { continue; };
                    self.handle_worker_report(event);
                }
            }
        };

        let cleanup = self.stop_all_workers().await;
        result.and(cleanup)
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
                || self
                    .workers
                    .iter()
                    .any(|worker| worker.path_key == path_key)
            {
                continue;
            }
            self.next_worker_id += 1;
            let worker_id = self.next_worker_id;
            let (command_tx, command_rx) = command_channel();
            self.launched_workers.insert(
                path_key.clone(),
                LaunchedWorker {
                    worker_id,
                    command_tx: command_tx.clone(),
                },
            );
            let worker_sender = command_tx.clone();
            let thread = spawn_device_worker(
                worker_id,
                self.manager_id.clone(),
                self.backend.clone(),
                descriptor,
                self.worker_tx.clone(),
                command_tx,
                command_rx,
            );
            self.workers.push(OwnedWorker {
                worker_id,
                path_key,
                command_tx: worker_sender,
                thread,
            });
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

    fn request_stop_all_workers(&self) {
        for worker in &self.workers {
            let _ = worker.command_tx.send(RuntimeCommand::Stop);
        }
        for launched in self.launched_workers.values() {
            let _ = launched.command_tx.send(RuntimeCommand::Stop);
        }
        for active in self.active_workers.values() {
            let _ = active.command_tx.send(RuntimeCommand::Stop);
        }
    }

    fn reap_finished_workers(&mut self) {
        let mut index = 0;
        while index < self.workers.len() {
            if !self.workers[index].thread.is_finished() {
                index += 1;
                continue;
            }
            let worker = self.workers.swap_remove(index);
            if worker.thread.join().is_err() {
                self.handle_worker_report(WorkerReport::Failed {
                    worker_id: worker.worker_id,
                    path_key: worker.path_key,
                    error: "USB worker panicked".into(),
                });
            }
        }
    }

    async fn stop_all_workers(&mut self) -> Result<()> {
        self.request_stop_all_workers();
        let workers = std::mem::take(&mut self.workers);
        let result = tokio::task::spawn_blocking(move || {
            let mut panicked = false;
            for worker in workers {
                panicked |= worker.thread.join().is_err();
            }
            if panicked {
                bail!("USB worker panicked during shutdown");
            }
            Ok(())
        })
        .await
        .context("joining USB workers")?;
        self.launched_workers.clear();
        self.active_workers.clear();
        result
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // Cancellation still requests stop; ordinary exits also await every join.
        self.request_stop_all_workers();
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
) -> thread::JoinHandle<()> {
    let path_key = descriptor.path_key();
    let device_id = descriptor.hardware_id();
    thread::spawn(move || {
        let report = match device_worker(
            backend,
            worker_id,
            manager_id,
            descriptor,
            worker_tx.clone(),
            command_tx,
            command_rx,
        ) {
            Err(error) => WorkerReport::Failed {
                worker_id,
                path_key,
                error: format!("{error:#}"),
            },
            Ok(()) => WorkerReport::Disconnected {
                worker_id,
                path_key,
                device_id,
            },
        };
        let _ = worker_tx.send(report);
    })
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
    clear_leds(&mut *handle)?;
    debug!(
        "Saitek FIP probe reply request=0x{:02x} header_error=0x{:08x} request_error=0x{:08x}",
        probe_reply.request, probe_reply.header_error, probe_reply.request_error
    );

    if worker_tx
        .send(WorkerReport::Connected {
            worker_id,
            path_key: path_key.clone(),
            device_id: local_device_id.clone(),
            command_tx,
            device: Box::new(device_descriptor(
                &runtime_descriptor,
                &local_device_id,
                &fingerprint,
            )),
        })
        .is_err()
    {
        return reset_device(&mut *handle);
    }

    let mut previous_mask = 0u16;
    loop {
        let mut outputs = 0;
        for _ in 0..OUTPUT_BATCH_SIZE {
            match command_rx.try_recv() {
                Ok(RuntimeCommand::Stop) | Err(TryRecvError::Disconnected) => {
                    return reset_device(&mut *handle);
                }
                Ok(command) => {
                    // The wrapper reports one Failed transition, preserving its cause.
                    apply_runtime_command(&mut *handle, command)?;
                    outputs += 1;
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        let read_timeout = if outputs > 0 {
            BUSY_READ_TIMEOUT
        } else {
            READ_TIMEOUT
        };
        let Some(report) = handle.read_hid_report(read_timeout)? else {
            if !handle.has_hid_input() {
                thread::sleep(read_timeout);
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
                    body: Box::new(body),
                });
            }
        }
        previous_mask = mask;
    }
}

fn apply_runtime_command(handle: &mut dyn DeviceHandle, command: RuntimeCommand) -> Result<()> {
    match command {
        RuntimeCommand::Hardware(AuthorizedOutput { output, context }) => {
            apply_hardware_output(handle, output, || context.authorize())?;
        }
        RuntimeCommand::ResetDevice => {
            reset_device(handle)?;
        }
        RuntimeCommand::Stop => {}
    }
    Ok(())
}

fn clear_leds(handle: &mut dyn DeviceHandle) -> Result<()> {
    for index in 1..=8 {
        handle.set_led(DEFAULT_PAGE_ID, index, false)?;
    }
    Ok(())
}

fn reset_device(handle: &mut dyn DeviceHandle) -> Result<()> {
    // Try both even when one reset fails.
    let leds = clear_leds(handle);
    let screen = handle.clear_image(DEFAULT_PAGE_ID);
    leds?;
    screen?;
    Ok(())
}

fn apply_hardware_output(
    handle: &mut dyn DeviceHandle,
    output: HardwareOutput,
    mut authorize: impl FnMut() -> deckr::Result<()>,
) -> Result<()> {
    // Authority loss discards this command; it is not a USB failure.
    if let Err(error) = authorize() {
        debug!(%error, "discarding hardware command without current authority");
        return Ok(());
    }
    match output {
        HardwareOutput::SetLed { control_id, on } => {
            let index = led_index(&control_id).context("unknown LED control")?;
            handle.set_led(DEFAULT_PAGE_ID, index, on)?;
            tracing::trace!(%control_id, on, "USB lamp output completed");
        }
        HardwareOutput::SetRasterFrame {
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
            if let Err(error) = authorize() {
                debug!(%error, "discarding raster frame after authority loss during conversion");
                return Ok(());
            }
            handle.send_image(&frame, DEFAULT_PAGE_ID)?;
            tracing::trace!(%control_id, "USB raster frame completed");
        }
        HardwareOutput::ClearRaster { control_id } => {
            if control_id != SCREEN_CONTROL_ID {
                warn!("Ignoring raster clear for unknown control {control_id}");
                return Ok(());
            }
            handle.clear_image(DEFAULT_PAGE_ID)?;
            tracing::trace!(%control_id, "USB raster clear completed");
        }
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
    use deckr::lanes::{DeckrMessage, DeviceDescriptor, DeviceRef};
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
                participant_sessions: BTreeMap::from([
                    (controller.to_string(), "controller-session".to_string()),
                    (manager.to_string(), "manager-session".to_string()),
                ]),
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
        let deadline = Instant::now() + Duration::from_secs(5);
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

    fn assert_no_queued_runtime_command(
        command_rx: &CommandReceiver,
        unexpected: &str,
        predicate: impl Fn(&RuntimeCommand) -> bool,
    ) {
        while let Ok(command) = command_rx.try_recv() {
            assert!(
                !predicate(&command),
                "unexpected {unexpected} command queued: {command:?}"
            );
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
        leds: Vec<(u32, u32, bool)>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                enumerate_rows: Arc::new(StdMutex::new(vec![sample_candidate()])),
                device: Arc::new(StdMutex::new(FakeDeviceState {
                    reports: VecDeque::new(),
                    sent_frames: Vec::new(),
                    commands: Vec::new(),
                    leds: Vec::new(),
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

        fn set_led(&mut self, page: u32, index: u32, value: bool) -> Result<FipControlPacket> {
            self.state.lock().unwrap().leds.push((page, index, value));
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

    fn led_command(control: &str, on: serde_json::Value) -> HardwareMessageBody {
        let mut body = raster_command("clear");
        if let HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            params,
            ..
        } = &mut body
        {
            *control_id = Some(control.to_string());
            *capability_id = LED_CAPABILITY_ID.to_string();
            *command_type = "set".to_string();
            params.insert("on".into(), on);
        }
        body
    }

    #[test]
    fn led_commands_require_a_real_lamp_and_boolean_state() {
        assert!(hardware_output_from_body(led_command("s6", serde_json::json!(true))).is_ok());
        for body in [
            led_command("screen", serde_json::json!(true)),
            led_command("s6", serde_json::json!(1)),
            led_command("s6", serde_json::json!("true")),
        ] {
            assert!(hardware_output_from_body(body).is_err());
        }
    }

    async fn queued_led(
        h: &ManagedHarness,
        command_rx: &CommandReceiver,
        contract_id: &str,
        on: bool,
    ) -> RuntimeCommand {
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer(contract_id),
                led_command("s6", serde_json::json!(on)),
            )
            .unwrap(),
        );
        wait_for_runtime_command(command_rx, "LED output", |command| {
            matches!(command, RuntimeCommand::Hardware(_))
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn led_output_is_fenced_and_reset_turns_lamps_off() {
        let (h, tasks, command_tx, command_rx, first) = output_harness().await;
        let on = queued_led(&h, &command_rx, "claim-a", true).await;
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };
        apply_runtime_command(&mut handle, on.clone()).unwrap();
        assert_eq!(
            backend.device.lock().unwrap().leds,
            [(DEFAULT_PAGE_ID, 6, true)]
        );
        h.concord
            .cancel(
                &first,
                &EndpointAddress::parse("controller:main").unwrap(),
                Some("done".into()),
            )
            .await
            .unwrap();
        let reset = wait_for_runtime_command(&command_rx, "reset", RuntimeCommand::is_reset);
        apply_runtime_command(&mut handle, reset).unwrap();
        create_claim(&h.concord, "claim-b", "fip", Some("fingerprint:fip")).await;
        apply_runtime_command(&mut handle, on).unwrap();
        assert_eq!(backend.device.lock().unwrap().leds.len(), 9);
        assert!(backend.device.lock().unwrap().leds[1..]
            .iter()
            .all(|(_, _, on)| !on));

        let new_on = queued_led(&h, &command_rx, "claim-b", true).await;
        let new_off = queued_led(&h, &command_rx, "claim-b", false).await;
        command_tx.send(new_on).unwrap();
        command_tx.send(new_off).unwrap();
        apply_runtime_command(&mut handle, command_rx.try_recv().unwrap()).unwrap();
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(
            backend.device.lock().unwrap().leds.last(),
            Some(&(DEFAULT_PAGE_ID, 6, false))
        );
        stop_managed_runtime(&h, tasks).await;
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
                h.runtime.set_device(*device).await.unwrap();
            }
            WorkerEvent::Input { body, .. } => {
                h.runtime.handle_hardware_message(*body).await.unwrap();
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

    async fn queued_output(
        h: &ManagedHarness,
        command_rx: &CommandReceiver,
        contract_id: &str,
        command_type: &str,
    ) -> RuntimeCommand {
        h.lane.publish_inbound(
            DeckrMessage::hardware_command(
                "main",
                "controller-session",
                "saitek-main",
                "manager-session",
                "fip",
                contract_pointer(contract_id),
                raster_command(command_type),
            )
            .unwrap(),
        );
        wait_for_runtime_command(command_rx, "hardware output", |command| {
            matches!(command, RuntimeCommand::Hardware(_))
        })
    }

    async fn output_harness() -> (
        ManagedHarness,
        JoinSet<deckr::Result<()>>,
        CommandSender,
        CommandReceiver,
        ContractHandle,
    ) {
        let h = managed_harness().await;
        let tasks = start_managed_runtime(&h).await;
        let (command_tx, command_rx) = command_channel();
        h.handler
            .register_device("fip".into(), command_tx.clone())
            .await;
        h.runtime
            .set_device(test_device_descriptor("fip", "fingerprint:fip"))
            .await
            .unwrap();
        let claim = create_claim(&h.concord, "claim-a", "fip", Some("fingerprint:fip")).await;
        (h, tasks, command_tx, command_rx, claim)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_queue_keeps_latest_raster_frame_per_control() {
        let (h, tasks, command_tx, command_rx, _) = output_harness().await;
        let RuntimeCommand::Hardware(template) =
            queued_output(&h, &command_rx, "claim-a", "set_frame").await
        else {
            unreachable!()
        };

        for value in 0..COMMAND_QUEUE_CAPACITY * 4 {
            command_tx
                .send(RuntimeCommand::Hardware(AuthorizedOutput {
                    output: HardwareOutput::SetRasterFrame {
                        control_id: SCREEN_CONTROL_ID.to_string(),
                        encoding: "png".to_string(),
                        image: vec![value as u8],
                    },
                    context: template.context.clone(),
                }))
                .unwrap();
        }

        match command_rx.try_recv().unwrap() {
            RuntimeCommand::Hardware(AuthorizedOutput {
                output:
                    HardwareOutput::SetRasterFrame {
                        control_id, image, ..
                    },
                ..
            }) => {
                assert_eq!(control_id, SCREEN_CONTROL_ID);
                assert_eq!(image, vec![(COMMAND_QUEUE_CAPACITY * 4 - 1) as u8]);
            }
            other => panic!("expected latest raster frame, got {other:?}"),
        }
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_queue_preserves_stop_priority() {
        let (h, tasks, command_tx, command_rx, _) = output_harness().await;
        let RuntimeCommand::Hardware(template) =
            queued_output(&h, &command_rx, "claim-a", "set_frame").await
        else {
            unreachable!()
        };

        for index in 0..COMMAND_QUEUE_CAPACITY * 4 {
            command_tx
                .send(RuntimeCommand::Hardware(AuthorizedOutput {
                    output: HardwareOutput::SetRasterFrame {
                        control_id: format!("control-{index}"),
                        encoding: "png".to_string(),
                        image: vec![index as u8],
                    },
                    context: template.context.clone(),
                }))
                .unwrap();
        }
        command_tx.send(RuntimeCommand::Stop).unwrap();
        command_tx.send(RuntimeCommand::Hardware(template)).unwrap();

        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::Stop
        ));
        // A reset racing with shutdown must not revive the drained queue.
        command_tx.send(RuntimeCommand::ResetDevice).unwrap();
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_queue_preserves_reset_priority() {
        let (h, tasks, command_tx, command_rx, _) = output_harness().await;
        let command = queued_output(&h, &command_rx, "claim-a", "set_frame").await;
        command_tx.send(command).unwrap();
        command_tx.send(RuntimeCommand::ResetDevice).unwrap();

        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::ResetDevice
        ));
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        stop_managed_runtime(&h, tasks).await;
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

        supervisor.stop_all_workers().await.unwrap();
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

        wait_for_runtime_command(&command_rx, "raster frame", RuntimeCommand::is_raster_frame);
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
        wait_for_runtime_command(&command_rx, "raster frame", RuntimeCommand::is_raster_frame);
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
        assert_no_queued_runtime_command(
            &command_rx,
            "raster frame",
            RuntimeCommand::is_raster_frame,
        );

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
        assert_no_queued_runtime_command(
            &command_rx,
            "raster frame",
            RuntimeCommand::is_raster_frame,
        );

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
        wait_for_runtime_command(&command_rx, "raster frame", RuntimeCommand::is_raster_frame);

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
        wait_for_runtime_command(&command_rx, "raster frame", RuntimeCommand::is_raster_frame);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_queued_outputs_stay_revoked_after_successor_claim() {
        let (h, tasks, command_tx, command_rx, first) = output_harness().await;
        // Hold dequeued work so a reset cannot remove it from the queue.
        let old_frame = queued_output(&h, &command_rx, "claim-a", "set_frame").await;
        let old_clear = queued_output(&h, &command_rx, "claim-a", "clear").await;
        h.concord
            .cancel(
                &first,
                &EndpointAddress::parse("controller:main").unwrap(),
                Some("transferred".into()),
            )
            .await
            .unwrap();
        wait_for_runtime_command(&command_rx, "reset device", RuntimeCommand::is_reset);
        create_claim(&h.concord, "claim-b", "fip", Some("fingerprint:fip")).await;

        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };
        apply_runtime_command(&mut handle, old_frame.clone()).unwrap();
        apply_runtime_command(&mut handle, old_clear).unwrap();
        assert!(backend.commands().is_empty());

        let new_frame = queued_output(&h, &command_rx, "claim-b", "set_frame").await;
        command_tx.send(old_frame).unwrap();
        command_tx.send(new_frame).unwrap();
        // Coalescing must retain the new command's authority along with its image.
        apply_runtime_command(&mut handle, command_rx.try_recv().unwrap()).unwrap();
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        let new_clear = queued_output(&h, &command_rx, "claim-b", "clear").await;
        apply_runtime_command(&mut handle, new_clear).unwrap();
        assert_eq!(backend.commands(), [REQ_SET_IMAGE, REQ_CLEAR_IMAGE]);
        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_outputs_are_discarded_after_device_or_runtime_authority_loss() {
        for cause in ["replacement", "disconnect", "shutdown", "token loss"] {
            let (h, tasks, _command_tx, command_rx, claim) = output_harness().await;
            let frame = queued_output(&h, &command_rx, "claim-a", "set_frame").await;
            let clear = queued_output(&h, &command_rx, "claim-a", "clear").await;
            match cause {
                "replacement" => {
                    h.runtime
                        .set_device(test_device_descriptor("fip", "replacement"))
                        .await
                        .unwrap();
                }
                "disconnect" => {
                    h.handler.remove_device("fip").await;
                    h.runtime
                        .remove_device("fip", "disconnected")
                        .await
                        .unwrap();
                }
                "shutdown" => h.runtime.stop().await.unwrap(),
                "token loss" => {
                    let token = h
                        .concord
                        .participant_token(&claim, &manager_endpoint())
                        .await
                        .unwrap()
                        .unwrap();
                    h.concord.withdraw(&token).await.unwrap();
                }
                _ => unreachable!(),
            }
            let RuntimeCommand::Hardware(authorized) = &frame else {
                unreachable!()
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                while authorized.context.authorize().is_ok() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("queued context should lose authority");

            let backend = FakeBackend::new();
            let mut handle = FakeHandle {
                state: backend.device.clone(),
            };
            apply_runtime_command(&mut handle, frame).unwrap();
            apply_runtime_command(&mut handle, clear).unwrap();
            assert!(
                backend.commands().is_empty(),
                "{cause} must fence USB output"
            );
            apply_runtime_command(&mut handle, RuntimeCommand::ResetDevice).unwrap();
            assert_eq!(backend.commands(), [REQ_CLEAR_IMAGE]);
            stop_managed_runtime(&h, tasks).await;
        }
    }

    #[test]
    fn raster_authority_is_rechecked_after_image_conversion() {
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };
        let mut checks = 0;
        apply_hardware_output(
            &mut handle,
            hardware_output_from_body(raster_command("set_frame")).unwrap(),
            || {
                checks += 1;
                if checks == 1 {
                    Ok(())
                } else {
                    Err(deckr::Error::Unauthorized(
                        "claim ended during conversion".into(),
                    ))
                }
            },
        )
        .unwrap();
        assert_eq!(checks, 2);
        assert!(backend.commands().is_empty());
    }

    #[test]
    fn unauthorized_frame_is_discarded_before_decoding() {
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };
        apply_hardware_output(
            &mut handle,
            HardwareOutput::SetRasterFrame {
                control_id: SCREEN_CONTROL_ID.into(),
                encoding: "png".into(),
                image: b"invalid image".to_vec(),
            },
            || Err(deckr::Error::Unauthorized("claim ended in queue".into())),
        )
        .unwrap();
        assert!(backend.commands().is_empty());
    }

    #[test]
    fn hardware_output_from_body_maps_outputs() {
        assert!(matches!(
            hardware_output_from_body(raster_command("set_frame")).unwrap(),
            HardwareOutput::SetRasterFrame { .. }
        ));
        assert!(matches!(
            hardware_output_from_body(raster_command("clear")).unwrap(),
            HardwareOutput::ClearRaster { .. }
        ));
    }

    #[test]
    fn hardware_output_from_body_rejects_under_shaped_params() {
        let mut missing_encoding = raster_command("set_frame");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut missing_encoding {
            params.remove("encoding");
        }
        assert!(hardware_output_from_body(missing_encoding).is_err());

        let mut invalid_encoding = raster_command("set_frame");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut invalid_encoding {
            params.insert(
                "encoding".to_string(),
                serde_json::Value::String("gif".to_string()),
            );
        }
        assert!(hardware_output_from_body(invalid_encoding).is_err());

        let mut non_empty_clear = raster_command("clear");
        if let HardwareMessageBody::ControlCommand { params, .. } = &mut non_empty_clear {
            params.insert("unexpected".to_string(), serde_json::Value::Bool(true));
        }
        assert!(hardware_output_from_body(non_empty_clear).is_err());
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

        apply_hardware_output(
            &mut handle,
            HardwareOutput::SetRasterFrame {
                control_id: SCREEN_CONTROL_ID.to_string(),
                encoding: "png".to_string(),
                image: make_png(),
            },
            || Ok(()),
        )
        .unwrap();

        assert_eq!(backend.commands(), [REQ_SET_IMAGE]);
        assert_eq!(backend.sent_frames()[0].len(), crate::protocol::FRAME_BYTES);
    }
    struct EnumerationFailure;
    impl Backend for EnumerationFailure {
        fn enumerate(&self) -> Result<Vec<DeviceCandidate>> {
            bail!("injected enumeration failure")
        }
        fn open(&self, _: &DeviceCandidate, _: Duration) -> Result<Box<dyn DeviceHandle>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn enumeration_failure_stops_existing_worker() {
        let (worker_tx, worker_rx) = tokio_mpsc::unbounded_channel();
        let (manager_tx, _manager_rx) = tokio_mpsc::unbounded_channel();
        let mut supervisor = Supervisor::new(
            "test".into(),
            Arc::new(EnumerationFailure),
            worker_tx,
            worker_rx,
            manager_tx,
        );
        let (command_tx, command_rx) = command_channel();
        // External senders can outlive the supervisor, so disconnect is insufficient.
        let _worker_owned_sender = command_tx.clone();
        supervisor.active_workers.insert(
            "path".into(),
            ActiveWorker {
                worker_id: 1,
                device_id: "fip".into(),
                command_tx,
            },
        );
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        assert!(supervisor.run(shutdown_rx).await.is_err());
        assert!(
            matches!(command_rx.try_recv(), Ok(RuntimeCommand::Stop)),
            "enumeration failure left the worker running without a Stop command"
        );
    }

    struct ReplenishingHandle {
        fail_on_output: bool,
        sender: CommandSender,
        output: RuntimeCommand,
        writes: Arc<std::sync::atomic::AtomicUsize>,
        reads: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl DeviceHandle for ReplenishingHandle {
        fn has_hid_input(&self) -> bool {
            true
        }
        fn probe(&mut self) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn clear_image(&mut self, _: u32) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn send_image(&mut self, _: &[u8], _: u32) -> Result<FipControlPacket> {
            unreachable!()
        }
        fn set_led(&mut self, _: u32, _: u32, on: bool) -> Result<FipControlPacket> {
            use std::sync::atomic::Ordering::SeqCst;
            if on {
                if self.fail_on_output {
                    bail!("injected USB bulk OUT failure");
                }
                let count = self.writes.fetch_add(1, SeqCst) + 1;
                self.sender
                    .send(if count < 64 {
                        self.output.clone()
                    } else {
                        RuntimeCommand::Stop
                    })
                    .unwrap();
            }
            Ok(FipControlPacket::default())
        }
        fn read_hid_report(&mut self, _: Duration) -> Result<Option<Vec<u8>>> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(vec![1, 0]))
        }
    }
    struct ReplenishingBackend {
        handle: StdMutex<Option<ReplenishingHandle>>,
    }
    impl Backend for ReplenishingBackend {
        fn enumerate(&self) -> Result<Vec<DeviceCandidate>> {
            Ok(vec![sample_candidate()])
        }
        fn open(&self, _: &DeviceCandidate, _: Duration) -> Result<Box<dyn DeviceHandle>> {
            Ok(Box::new(self.handle.lock().unwrap().take().unwrap()))
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn busy_outputs_do_not_starve_hid() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let (h, tasks, _, rx, _) = output_harness().await;
        let RuntimeCommand::Hardware(template) = queued_output(&h, &rx, "claim-a", "clear").await
        else {
            unreachable!()
        };
        let (sender, receiver) = command_channel();
        let output = RuntimeCommand::Hardware(AuthorizedOutput {
            context: template.context,
            output: HardwareOutput::SetLed {
                control_id: "s1".into(),
                on: true,
            },
        });
        sender.send(output.clone()).unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(ReplenishingBackend {
            handle: StdMutex::new(Some(ReplenishingHandle {
                fail_on_output: false,
                sender: sender.clone(),
                output,
                writes: writes.clone(),
                reads: reads.clone(),
            })),
        });
        let (reports, _rx) = tokio_mpsc::unbounded_channel();
        tokio::task::spawn_blocking(move || {
            device_worker(
                backend,
                1,
                "saitek-main".into(),
                sample_candidate(),
                reports,
                sender,
                receiver,
            )
        })
        .await
        .unwrap()
        .unwrap();
        stop_managed_runtime(&h, tasks).await;
        assert_eq!(writes.load(SeqCst), 64);
        assert!(
            reads.load(SeqCst) > 0,
            "64 consecutive outputs completed without a single HID read"
        );
    }

    struct BlockingReadState {
        entered: std::sync::atomic::AtomicBool,
        release: std::sync::atomic::AtomicBool,
        dropped: std::sync::atomic::AtomicBool,
    }
    struct BlockingReadHandle(Arc<BlockingReadState>);
    impl Drop for BlockingReadHandle {
        fn drop(&mut self) {
            self.0
                .dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl DeviceHandle for BlockingReadHandle {
        fn has_hid_input(&self) -> bool {
            true
        }
        fn probe(&mut self) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn clear_image(&mut self, _: u32) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn send_image(&mut self, _: &[u8], _: u32) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn set_led(&mut self, _: u32, _: u32, _: bool) -> Result<FipControlPacket> {
            Ok(FipControlPacket::default())
        }
        fn read_hid_report(&mut self, _: Duration) -> Result<Option<Vec<u8>>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.0.entered.store(true, SeqCst);
            while !self.0.release.load(SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(None)
        }
    }
    struct BlockingReadBackend(Arc<BlockingReadState>);
    impl Backend for BlockingReadBackend {
        fn enumerate(&self) -> Result<Vec<DeviceCandidate>> {
            Ok(vec![sample_candidate()])
        }
        fn open(&self, _: &DeviceCandidate, _: Duration) -> Result<Box<dyn DeviceHandle>> {
            Ok(Box::new(BlockingReadHandle(self.0.clone())))
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supervisor_shutdown_waits_for_usb_handle_release() {
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
        let state = Arc::new(BlockingReadState {
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
        });
        let (worker_tx, worker_rx) = tokio_mpsc::unbounded_channel();
        let (manager_tx, _manager_rx) = tokio_mpsc::unbounded_channel();
        let mut supervisor = Supervisor::new(
            "test".into(),
            Arc::new(BlockingReadBackend(state.clone())),
            worker_tx,
            worker_rx,
            manager_tx,
        );
        supervisor.reconcile_usb_presence(vec![sample_candidate()]);
        while !state.entered.load(SeqCst) {
            time::sleep(Duration::from_millis(1)).await;
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        shutdown_tx.send(()).unwrap();
        let stopping = tokio::spawn(supervisor.run(shutdown_rx));
        time::sleep(Duration::from_millis(20)).await;
        let returned_early = stopping.is_finished();
        state.release.store(true, SeqCst);
        stopping.await.unwrap().unwrap();
        time::timeout(Duration::from_secs(1), async {
            while !state.dropped.load(SeqCst) {
                time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            !returned_early,
            "supervisor returned while USB worker still held its device handle"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn usb_output_failure_reports_disconnect_and_cause_once() {
        use std::sync::atomic::AtomicUsize;
        let (h, tasks, _, rx, _) = output_harness().await;
        let RuntimeCommand::Hardware(template) = queued_output(&h, &rx, "claim-a", "clear").await
        else {
            unreachable!()
        };
        let (sender, receiver) = command_channel();
        let output = RuntimeCommand::Hardware(AuthorizedOutput {
            context: template.context,
            output: HardwareOutput::SetLed {
                control_id: "s1".into(),
                on: true,
            },
        });
        sender.send(output.clone()).unwrap();
        let backend = Arc::new(ReplenishingBackend {
            handle: StdMutex::new(Some(ReplenishingHandle {
                fail_on_output: true,
                sender: sender.clone(),
                output,
                writes: Arc::new(AtomicUsize::new(0)),
                reads: Arc::new(AtomicUsize::new(0)),
            })),
        });
        let (reports, mut report_rx) = tokio_mpsc::unbounded_channel();
        let worker = spawn_device_worker(
            7,
            "saitek-main".into(),
            backend,
            sample_candidate(),
            reports,
            sender.clone(),
            receiver,
        );
        let (mut supervisor, mut manager_rx) = supervisor_for_tests();
        supervisor.launched_workers.insert(
            sample_candidate().path_key(),
            LaunchedWorker {
                worker_id: 7,
                command_tx: sender,
            },
        );
        supervisor.handle_worker_report(recv_worker_report(&mut report_rx).await);
        assert!(matches!(
            manager_rx.try_recv(),
            Ok(WorkerEvent::Connected { .. })
        ));
        supervisor.handle_worker_report(recv_worker_report(&mut report_rx).await);
        worker.join().unwrap();
        assert_next_disconnected(
            &mut manager_rx,
            &sample_candidate().path_key(),
            &sample_candidate().hardware_id(),
        );
        let WorkerEvent::Failed { error, .. } = manager_rx.try_recv().unwrap() else {
            panic!("missing failure cause")
        };
        assert!(error.contains("injected USB bulk OUT failure"));
        assert!(report_rx.try_recv().is_err());
        assert!(manager_rx.try_recv().is_err());
        stop_managed_runtime(&h, tasks).await;
    }

    #[tokio::test]
    async fn shutdown_cleanup_error_does_not_start_another_session() {
        let shutdown = Cancellation::default();
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result = retry_sessions("test", &shutdown, || async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            shutdown.cancel();
            bail!("injected cleanup timeout")
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("cleanup timeout"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_interrupts_retry_backoff() {
        let shutdown = Cancellation::default();
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let retry = retry_sessions("test", &shutdown, || async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            bail!("injected session failure")
        });
        let cancel = async {
            time::sleep(Duration::from_millis(10)).await;
            shutdown.cancel();
        };
        let (result, ()) = time::timeout(Duration::from_millis(500), async {
            tokio::join!(retry, cancel)
        })
        .await
        .unwrap();
        result.unwrap();
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_interrupts_initial_connection() {
        let manager = SaitekRemoteManager::with_backend(
            "nats://127.0.0.1:1".into(),
            "test".into(),
            Arc::new(FakeBackend::new()),
        );
        time::timeout(
            Duration::from_secs(1),
            manager.run_until(async {
                time::sleep(Duration::from_millis(10)).await;
                Ok(())
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
