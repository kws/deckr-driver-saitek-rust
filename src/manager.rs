use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use deckr::beacon::{AdvertisementHandle, BeaconAdvertiser};
use deckr::concord::{
    ConcordCoordinator, ConcordManagedContract, ConcordParticipantManager, ContractHandle,
    ContractRecord,
};
use deckr::endpoint::{hardware_manager_address, EndpointAddress};
use deckr::hardware::{hardware_beacon_payload, HardwareClaimRouting};
use deckr::lanes::{
    DeckrMessage, HardwareMessageBody, HARDWARE_MESSAGES_LANE as WIRE_HARDWARE_LANE,
};
use deckr::nats::{
    ConcordStateChangeSource, ConcordStateChangeStream, NatsDeckrRuntime, NatsStateStore,
};
use deckr::profiles::hardware::{
    HardwareBeaconPayload, HardwareClaimTerms, HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
};
use deckr::state::{StateMaintenancePolicy, StateStore};
use futures_util::StreamExt;
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
const WATCH_RETRY_SECONDS: u64 = 1;
const WATCH_DEBOUNCE_INTERVAL: Duration = Duration::from_millis(100);

type HardwareClaimManager = ConcordParticipantManager<NatsStateStore, NatsStateStore>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutingReconcileMode {
    Full,
    ManagedOnly,
}

#[derive(Debug, Default)]
struct ConcordChangeBatch {
    saw_contract: bool,
    saw_token: bool,
}

impl ConcordChangeBatch {
    fn record(&mut self, source: ConcordStateChangeSource) {
        match source {
            ConcordStateChangeSource::Contracts => self.saw_contract = true,
            ConcordStateChangeSource::Tokens => self.saw_token = true,
        }
    }

    fn reconcile_mode(&self) -> (RoutingReconcileMode, &'static str) {
        if self.saw_contract {
            (RoutingReconcileMode::Full, "contract watch")
        } else if self.saw_token {
            (RoutingReconcileMode::ManagedOnly, "token watch")
        } else {
            (RoutingReconcileMode::ManagedOnly, "empty watch batch")
        }
    }
}

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

#[derive(Debug, Clone)]
enum WorkerEvent {
    Connected {
        path_key: String,
        device_id: String,
        command_tx: Sender<RuntimeCommand>,
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
        command_tx: Sender<RuntimeCommand>,
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
        let runtime = Arc::new(
            NatsDeckrRuntime::connect(&self.nats_url)
                .await
                .with_context(|| format!("connecting manager {} to NATS", self.manager_id))?,
        );
        info!(
            "Connected manager {} to NATS at {}",
            self.manager_id, self.nats_url
        );
        info!(
            "Saitek state maintenance intervals: beacon renewal={}s, Concord token refresh={}s, routing reconciliation={}s",
            self.state_policy.renewal_interval.as_secs(),
            self.state_policy.concord_token_refresh_interval.as_secs(),
            self.state_policy.reconcile_interval.as_secs()
        );

        let shared = Arc::new(Mutex::new(ManagerState::new(
            self.manager_id.clone(),
            self.session_id.clone(),
        )));
        let claim_manager = Arc::new(Mutex::new(
            ConcordParticipantManager::new(
                ConcordCoordinator::new(
                    runtime.concord_contracts().clone(),
                    runtime.concord_tokens().clone(),
                ),
                EndpointAddress::parse(hardware_manager_address(&self.manager_id))?,
                self.session_id.clone(),
            )?
            .profile(HARDWARE_CLAIM_PROFILE_ID.to_string())
            .token_refresh_interval(self.state_policy.concord_token_refresh_interval),
        ));
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

        publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;

        let mut supervisor_handle = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
        let mut tasks = JoinSet::<Result<()>>::new();
        tasks.spawn(worker_event_loop(
            runtime.clone(),
            shared.clone(),
            manager_event_rx,
            claim_manager.clone(),
        ));
        tasks.spawn(inbound_command_loop(runtime.clone(), shared.clone()));
        tasks.spawn(hardware_advertisement_loop(
            runtime.clone(),
            shared.clone(),
            self.state_policy.renewal_interval,
        ));
        tasks.spawn(concord_state_watch_loop(
            runtime.clone(),
            shared.clone(),
            claim_manager.clone(),
        ));
        tasks.spawn(concord_token_renewal_loop(
            shared.clone(),
            claim_manager.clone(),
            self.state_policy.concord_token_refresh_interval,
        ));
        tasks.spawn(routing_reconciliation_loop(
            shared.clone(),
            claim_manager.clone(),
            self.state_policy.reconcile_interval,
        ));

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for shutdown signal")?;
                info!("Shutting down Saitek manager {}", self.manager_id);
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                let _ = supervisor_handle.await;
                withdraw_hardware_advertisement_safely(runtime, shared).await;
                Ok(())
            }
            result = &mut supervisor_handle => {
                tasks.abort_all();
                result.context("joining device supervisor")??;
                bail!("device supervisor stopped unexpectedly")
            }
            result = tasks.join_next() => {
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                let _ = supervisor_handle.await;
                match result {
                    Some(Ok(Ok(()))) => bail!("manager runtime task stopped unexpectedly"),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(error).context("joining manager runtime task"),
                    None => bail!("manager runtime tasks stopped unexpectedly"),
                }
            }
        }
    }
}

struct ManagerState {
    manager_id: String,
    endpoint: String,
    session_id: String,
    advertisement_id: String,
    devices: BTreeMap<String, deckr::lanes::DeviceDescriptor>,
    command_map: HashMap<String, Sender<RuntimeCommand>>,
    routing: HardwareClaimRouting,
    advertisement_handle: Option<AdvertisementHandle>,
    advertisement_dirty: bool,
}

impl ManagerState {
    fn new(manager_id: String, session_id: String) -> Self {
        let endpoint = hardware_manager_address(&manager_id);
        let advertisement_id = format!("hardware-{manager_id}-{session_id}");
        Self {
            manager_id,
            endpoint,
            session_id,
            advertisement_id,
            devices: BTreeMap::new(),
            command_map: HashMap::new(),
            routing: HardwareClaimRouting::default(),
            advertisement_handle: None,
            advertisement_dirty: false,
        }
    }

    fn hardware_payload(&self) -> Result<HardwareBeaconPayload> {
        Ok(hardware_beacon_payload(
            &self.manager_id,
            EndpointAddress::parse(&self.endpoint)?,
            &self.session_id,
            BTreeMap::new(),
            &self.devices,
            &self.routing.claimed_device_ids(),
        )?)
    }
}

async fn hardware_advertisement_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    renewal_interval: Duration,
) -> Result<()> {
    loop {
        publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
        time::sleep(renewal_interval).await;
    }
}

async fn publish_hardware_advertisement_safely(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) {
    if let Err(error) = publish_hardware_advertisement(runtime, shared.clone()).await {
        shared.lock().await.advertisement_dirty = true;
        warn!(
            "Saitek hardware Beacon advertisement is unavailable; heartbeat will retry: {error:#}"
        );
    }
}

async fn publish_hardware_advertisement(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let (advertiser, current_handle) = {
        let state = shared.lock().await;
        let payload = state.hardware_payload()?.to_value()?;
        let advertiser = BeaconAdvertiser::new(
            runtime.beacon_advertisements().clone(),
            HARDWARE_FEATURE_ID,
            EndpointAddress::parse(&state.endpoint)?,
            state.session_id.clone(),
        )
        .advertisement_id(state.advertisement_id.clone())
        .payload(payload);
        (advertiser, state.advertisement_handle.clone())
    };
    let handle = advertiser
        .publish_or_refresh(current_handle.as_ref())
        .await?;
    let mut state = shared.lock().await;
    state.advertisement_handle = Some(handle);
    state.advertisement_dirty = false;
    Ok(())
}

async fn withdraw_hardware_advertisement_safely(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) {
    let (advertiser, handle) = {
        let state = shared.lock().await;
        let Some(handle) = state.advertisement_handle.clone() else {
            return;
        };
        let payload = match state.hardware_payload() {
            Ok(payload) => match payload.to_value() {
                Ok(value) => value,
                Err(error) => {
                    warn!("Failed to build final Saitek hardware advertisement withdrawal payload: {error:#}");
                    return;
                }
            },
            Err(error) => {
                warn!("Failed to build final Saitek hardware advertisement withdrawal payload: {error:#}");
                return;
            }
        };
        let advertiser = match EndpointAddress::parse(&state.endpoint) {
            Ok(endpoint) => BeaconAdvertiser::new(
                runtime.beacon_advertisements().clone(),
                HARDWARE_FEATURE_ID,
                endpoint,
                state.session_id.clone(),
            )
            .advertisement_id(state.advertisement_id.clone())
            .payload(payload),
            Err(error) => {
                warn!("Failed to withdraw Saitek hardware advertisement: {error:#}");
                return;
            }
        };
        (advertiser, handle)
    };
    if let Err(error) = advertiser.withdraw(&handle).await {
        warn!("Failed to withdraw Saitek hardware advertisement: {error:#}");
    }
}

async fn concord_state_watch_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    claim_manager: Arc<Mutex<HardwareClaimManager>>,
) -> Result<()> {
    loop {
        let mut stream = match runtime.watch_concord_changes().await {
            Ok(stream) => stream,
            Err(error) => {
                warn!("Saitek Concord state watch is unavailable; watch will retry: {error:#}");
                time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
                continue;
            }
        };
        loop {
            match next_concord_change_batch(&mut stream).await {
                Ok(batch) => {
                    let (mode, reason) = batch.reconcile_mode();
                    reconcile_routing_current_state(
                        shared.clone(),
                        claim_manager.clone(),
                        reason,
                        mode,
                    )
                    .await?
                }
                Err(error) => {
                    warn!("Saitek Concord state watch is unavailable; watch will retry: {error:#}");
                    time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
                    break;
                }
            }
        }
    }
}

async fn next_concord_change_batch(
    stream: &mut ConcordStateChangeStream,
) -> Result<ConcordChangeBatch> {
    let first = stream.next().await?;
    debug!(
        "Saitek Concord {} changed at {}",
        first.source.reason(),
        first.key
    );
    let mut batch = ConcordChangeBatch::default();
    batch.record(first.source);
    let debounce = time::sleep(WATCH_DEBOUNCE_INTERVAL);
    tokio::pin!(debounce);

    loop {
        tokio::select! {
            () = &mut debounce => return Ok(batch),
            change = stream.next() => {
                let change = change?;
                debug!(
                    "Saitek Concord {} changed at {}",
                    change.source.reason(),
                    change.key
                );
                batch.record(change.source);
            }
        }
    }
}

async fn concord_token_renewal_loop<C, T>(
    shared: Arc<Mutex<ManagerState>>,
    claim_manager: Arc<Mutex<ConcordParticipantManager<C, T>>>,
    renewal_interval: Duration,
) -> Result<()>
where
    C: StateStore,
    T: StateStore,
{
    loop {
        time::sleep(renewal_interval).await;
        if let Err(error) = reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "token renewal",
            RoutingReconcileMode::ManagedOnly,
        )
        .await
        {
            warn!("Saitek Concord token renewal is unavailable; renewal will retry: {error:#}");
        }
    }
}

async fn routing_reconciliation_loop(
    shared: Arc<Mutex<ManagerState>>,
    claim_manager: Arc<Mutex<HardwareClaimManager>>,
    reconcile_interval: Duration,
) -> Result<()> {
    loop {
        if let Err(error) = reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "broker snapshot",
            RoutingReconcileMode::Full,
        )
        .await
        {
            warn!("Saitek routing current state unavailable; reconciliation will retry: {error:#}");
        }
        time::sleep(reconcile_interval).await;
    }
}

async fn reconcile_routing_current_state<C, T>(
    shared: Arc<Mutex<ManagerState>>,
    claim_manager: Arc<Mutex<ConcordParticipantManager<C, T>>>,
    reason: &'static str,
    mode: RoutingReconcileMode,
) -> Result<()>
where
    C: StateStore,
    T: StateStore,
{
    let (manager_id, manager_endpoint, current_devices) = {
        let state = shared.lock().await;
        (
            state.manager_id.clone(),
            state.endpoint.clone(),
            state.devices.clone(),
        )
    };
    let manager_endpoint = EndpointAddress::parse(&manager_endpoint)?;
    let known_devices = current_devices.keys().cloned().collect::<BTreeSet<_>>();

    let managed_contracts = {
        let mut claim_manager = claim_manager.lock().await;
        match mode {
            RoutingReconcileMode::Full => {
                claim_manager
                    .reconcile(
                        |contract, record| -> deckr::Result<bool> {
                            accept_current_hardware_claim(
                                contract,
                                record,
                                &manager_endpoint,
                                &manager_id,
                                &current_devices,
                            )
                        },
                        None,
                    )
                    .await?
            }
            RoutingReconcileMode::ManagedOnly => claim_manager.reconcile_managed(None).await?,
        }
    };

    debug!("Reconciling Saitek routing current state via {reason}");
    let (senders_to_reset, ignored_claims) = {
        let mut state = shared.lock().await;
        let reconcile =
            state
                .routing
                .reconcile_claims(&managed_contracts, &manager_endpoint, &known_devices);
        let senders = reconcile
            .reset_devices
            .into_iter()
            .filter_map(|device_id| state.command_map.get(&device_id).cloned())
            .collect::<Vec<_>>();
        (senders, reconcile.ignored_claims)
    };
    for ignored in ignored_claims {
        warn!(
            "Ignoring invalid Saitek hardware claim contract {}: {}",
            ignored.contract_key, ignored.reason
        );
    }
    for sender in senders_to_reset {
        let _ = sender.send(RuntimeCommand::ResetDevice);
    }
    Ok(())
}

fn accept_current_hardware_claim(
    contract: &ContractHandle,
    record: &ContractRecord,
    manager_endpoint: &EndpointAddress,
    manager_id: &str,
    current_devices: &BTreeMap<String, deckr::lanes::DeviceDescriptor>,
) -> deckr::Result<bool> {
    if !contract.participants.contains(manager_endpoint) {
        return Ok(false);
    }
    let Some(terms_value) = record.terms.clone() else {
        return Ok(false);
    };
    let terms = match HardwareClaimTerms::from_value(terms_value) {
        Ok(terms) => terms,
        Err(error) => {
            warn!(
                "Ignoring invalid Saitek hardware claim contract {}: {error}",
                contract.key
            );
            return Ok(false);
        }
    };
    if &terms.manager_endpoint != manager_endpoint {
        return Ok(false);
    }
    if terms.manager_endpoint.endpoint_id() != manager_id {
        return Ok(false);
    }
    for device in &terms.devices {
        let device_ref = &device.device_ref;
        if device_ref.manager_id != manager_id {
            return Ok(false);
        }
        let Some(current) = current_devices.get(&device_ref.device_id) else {
            debug!(
                "Rejecting Saitek hardware claim contract {} for absent device {}",
                contract.key, device_ref.device_id
            );
            return Ok(false);
        };
        if device_ref
            .fingerprint
            .as_ref()
            .is_some_and(|fingerprint| fingerprint != &current.fingerprint)
        {
            debug!(
                "Rejecting Saitek hardware claim contract {} for fingerprint mismatch on device {}",
                contract.key, device_ref.device_id
            );
            return Ok(false);
        }
    }
    Ok(true)
}

async fn worker_event_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    mut worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
    claim_manager: Arc<Mutex<HardwareClaimManager>>,
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
                {
                    let mut state = shared.lock().await;
                    state.devices.insert(device_id.clone(), device);
                    state.command_map.insert(device_id.clone(), command_tx);
                }
                publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
                reconcile_routing_current_state(
                    shared.clone(),
                    claim_manager.clone(),
                    "device connected",
                    RoutingReconcileMode::Full,
                )
                .await?;
            }
            WorkerEvent::Input { device_id, body } => {
                if !matches!(body, HardwareMessageBody::ControlInput { .. }) {
                    continue;
                }
                let route = {
                    let state = shared.lock().await;
                    state.routing.claim_recipient(&device_id).map(|recipient| {
                        (
                            state.session_id.clone(),
                            recipient.endpoint.to_string(),
                            recipient.session_id.to_string(),
                        )
                    })
                };
                let Some((manager_session_id, recipient_endpoint, recipient_session_id)) = route
                else {
                    debug!("Dropping unclaimed Saitek input for {device_id}");
                    continue;
                };
                let manager_id = match &body {
                    HardwareMessageBody::ControlInput { device_ref, .. } => {
                        device_ref.manager_id.clone()
                    }
                    _ => unreachable!(),
                };
                let message = DeckrMessage::hardware_input_to(
                    &manager_id,
                    &manager_session_id,
                    &device_id,
                    &recipient_endpoint,
                    &recipient_session_id,
                    body,
                )?;
                runtime.publish(&message).await?;
            }
            WorkerEvent::Disconnected {
                path_key,
                device_id,
            } => {
                debug!("Saitek device disconnected path={path_key} device={device_id}");
                let managed_contracts = { claim_manager.lock().await.managed_contracts() };
                {
                    let mut state = shared.lock().await;
                    state.devices.remove(&device_id);
                    state.command_map.remove(&device_id);
                    state.routing.remove_device(&device_id);
                }
                publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
                cancel_managed_device_claims(
                    claim_manager.clone(),
                    managed_contracts,
                    &device_id,
                    "Saitek",
                )
                .await?;
                reconcile_routing_current_state(
                    shared.clone(),
                    claim_manager.clone(),
                    "device disconnected",
                    RoutingReconcileMode::Full,
                )
                .await?;
            }
            WorkerEvent::Failed { path_key, error } => {
                warn!("Device worker {path_key} failed: {error}");
            }
        }
    }
    bail!("device worker event stream closed")
}

async fn cancel_managed_device_claims<C, T>(
    claim_manager: Arc<Mutex<ConcordParticipantManager<C, T>>>,
    managed_contracts: Vec<ConcordManagedContract>,
    device_id: &str,
    log_label: &'static str,
) -> Result<()>
where
    C: StateStore,
    T: StateStore,
{
    for managed in managed_contracts {
        if !managed_claims_device(&managed, device_id) {
            continue;
        }
        let contract_key = managed.contract.key.clone();
        let cancel_result = {
            let manager = claim_manager.lock().await;
            manager
                .cancel(
                    &managed.contract,
                    Some(format!("hardware device {device_id} disconnected")),
                )
                .await
        };
        match cancel_result {
            Ok(true) => {
                claim_manager.lock().await.release(&contract_key);
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    "Failed to cancel {log_label} hardware claim contract {} for disconnected device {device_id}: {error:#}",
                    managed.contract.key
                );
            }
        }
    }
    Ok(())
}

fn managed_claims_device(managed: &ConcordManagedContract, device_id: &str) -> bool {
    let Some(terms_value) = managed.record.terms.clone() else {
        return false;
    };
    let Ok(terms) = HardwareClaimTerms::from_value(terms_value) else {
        return false;
    };
    terms
        .devices
        .iter()
        .any(|device| device.device_ref.device_id == device_id)
}

async fn inbound_command_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let mut subscriber = runtime.subscribe_hardware_messages().await?;
    while let Some(message) = subscriber.next().await {
        match runtime.message_from_nats(message) {
            Ok(envelope) => route_inbound_command(shared.clone(), envelope).await?,
            Err(error) => debug!("Dropping invalid NATS Deckr lane message: {error:#}"),
        }
    }
    bail!("hardware_messages subscription ended")
}

async fn route_inbound_command(
    shared: Arc<Mutex<ManagerState>>,
    envelope: DeckrMessage,
) -> Result<()> {
    if envelope.lane != WIRE_HARDWARE_LANE || envelope.is_expired() {
        return Ok(());
    }
    let body = match envelope.hardware_body() {
        Ok(body) => body,
        Err(error) => {
            debug!("Ignoring unsupported hardware message body: {error:#}");
            return Ok(());
        }
    };
    if !body.is_command() {
        return Ok(());
    }
    let Some(device_id) = envelope.subject.device_id().map(str::to_string) else {
        debug!("Ignoring inbound hardware command without device subject");
        return Ok(());
    };
    let Some(subject_manager_id) = envelope.subject.manager_id() else {
        return Ok(());
    };
    let command = match runtime_command_from_body(body) {
        Ok(command) => command,
        Err(error) => {
            debug!("Ignoring unsupported hardware command: {error:#}");
            return Ok(());
        }
    };
    let sender = {
        let state = shared.lock().await;
        if envelope.recipient_endpoint() != Some(state.endpoint.as_str()) {
            return Ok(());
        }
        if !envelope.is_directly_deliverable_to(
            &EndpointAddress::parse(&state.endpoint)?,
            &state.session_id,
        )? {
            return Ok(());
        }
        if subject_manager_id != state.manager_id {
            return Ok(());
        }
        if !state.devices.contains_key(&device_id) {
            debug!(
                "Dropping command for unknown Saitek device {}/{}",
                subject_manager_id, device_id
            );
            return Ok(());
        }
        if state
            .routing
            .claim_recipient(&device_id)
            .is_none_or(|recipient| {
                recipient.endpoint.as_str() != envelope.sender
                    || recipient.session_id != envelope.sender_session_id
            })
        {
            debug!(
                "Dropping unroutable Saitek command for {}/{} from {}",
                subject_manager_id, device_id, envelope.sender
            );
            return Ok(());
        }
        state.command_map.get(&device_id).cloned()
    };
    if let Some(sender) = sender {
        if sender.send(command).is_err() {
            warn!("Dropping command for disconnected device {device_id}");
        }
    } else {
        debug!(
            "Dropping command for closed Saitek device {}/{}",
            subject_manager_id, device_id
        );
    }
    Ok(())
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
    command_tx: Sender<RuntimeCommand>,
}

#[derive(Debug, Clone)]
struct ActiveWorker {
    worker_id: u64,
    device_id: String,
    command_tx: Sender<RuntimeCommand>,
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
            let (command_tx, command_rx) = mpsc::channel::<RuntimeCommand>();
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
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
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
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
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
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use crate::protocol::{FipControlPacket, REQ_CLEAR_IMAGE, REQ_PROBE, REQ_SET_IMAGE};
    use deckr::beacon::find_candidates;
    use deckr::concord::{ContractHandle, ContractValidityStatus};
    use deckr::hardware::HardwareClaimRoute;
    use deckr::lanes::DeviceRef;
    use deckr::state::MemoryStateStore;

    #[derive(Clone)]
    struct FakeBackend {
        enumerate_rows: Arc<StdMutex<Vec<DeviceCandidate>>>,
        device: Arc<StdMutex<FakeDeviceState>>,
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
            }
        }

        fn sent_frames(&self) -> Vec<Vec<u8>> {
            self.device.lock().unwrap().sent_frames.clone()
        }

        fn commands(&self) -> Vec<u32> {
            self.device.lock().unwrap().commands.clone()
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
            Ok(self.state.lock().unwrap().reports.pop_front())
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

    fn hardware_claim_terms_with_fingerprint(
        manager_id: &str,
        device_id: &str,
        fingerprint: Option<&str>,
    ) -> serde_json::Value {
        let mut device_ref = serde_json::json!({
            "managerId": manager_id,
            "deviceId": device_id,
        });
        if let Some(fingerprint) = fingerprint {
            device_ref
                .as_object_mut()
                .unwrap()
                .insert("fingerprint".to_string(), fingerprint.into());
        }
        serde_json::json!({
            "profile": HARDWARE_CLAIM_PROFILE_ID,
            "claimId": "claim-1",
            "controllerEndpoint": "controller:main",
            "managerEndpoint": hardware_manager_address(manager_id),
            "devices": [{
                "deviceRef": device_ref,
                "instanceCount": 1
            }]
        })
    }

    async fn claim_context(
        manager_id: &str,
        device_id: &str,
        fingerprint: Option<&str>,
    ) -> (
        ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
        ContractHandle,
        EndpointAddress,
        ConcordParticipantManager<MemoryStateStore, MemoryStateStore>,
    ) {
        let contracts = MemoryStateStore::new();
        let tokens = MemoryStateStore::new();
        let concord = ConcordCoordinator::new(contracts, tokens);
        let controller = EndpointAddress::parse("controller:main").unwrap();
        let manager = EndpointAddress::parse(hardware_manager_address(manager_id)).unwrap();
        let lifecycle = ConcordParticipantManager::new(
            concord.clone(),
            manager.clone(),
            "manager-session".into(),
        )
        .unwrap()
        .profile(HARDWARE_CLAIM_PROFILE_ID.to_string());
        let contract = concord
            .create_contract(
                vec![controller.clone(), manager.clone()],
                Some("contract-1".to_string()),
                1,
                Some(HARDWARE_CLAIM_PROFILE_ID.to_string()),
                Some(hardware_claim_terms_with_fingerprint(
                    manager_id,
                    device_id,
                    fingerprint,
                )),
                Some(controller.clone()),
            )
            .await
            .unwrap();
        concord
            .attach(
                &contract,
                &controller,
                "controller-session",
                Some("controller-token".into()),
            )
            .await
            .unwrap();

        (concord, contract, manager, lifecycle)
    }

    async fn claimed_route(
        manager_id: &str,
        device_id: &str,
    ) -> (
        ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
        ContractHandle,
        EndpointAddress,
        ConcordParticipantManager<MemoryStateStore, MemoryStateStore>,
        HardwareClaimRouting,
    ) {
        let (concord, contract, manager, mut lifecycle) =
            claim_context(manager_id, device_id, None).await;
        let managed = lifecycle
            .reconcile(|_, _| -> deckr::Result<bool> { Ok(true) }, None)
            .await
            .unwrap();
        let mut routing = HardwareClaimRouting::default();
        let reconcile =
            routing.reconcile_claims(&managed, &manager, &BTreeSet::from([device_id.to_string()]));

        assert!(reconcile.ignored_claims.is_empty());
        assert!(routing.claim_recipient(device_id).is_some());
        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::Valid
        );

        (concord, contract, manager, lifecycle, routing)
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

    async fn shared_state_with_device(
        device_id: &str,
        fingerprint: &str,
    ) -> Arc<Mutex<ManagerState>> {
        let shared = Arc::new(Mutex::new(ManagerState::new(
            "saitek-main".to_string(),
            "manager-session".to_string(),
        )));
        {
            let mut state = shared.lock().await;
            state.devices.insert(
                device_id.to_string(),
                device_descriptor(&sample_candidate(), device_id, fingerprint),
            );
        }
        shared
    }

    #[test]
    fn supervisor_disconnects_active_worker_when_path_disappears() {
        let (mut supervisor, mut manager_rx) = supervisor_for_tests();
        let path_key = sample_candidate().path_key();
        let device_id = sample_candidate().hardware_id();
        let (command_tx, command_rx) = mpsc::channel::<RuntimeCommand>();
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
        let (command_tx, _command_rx) = mpsc::channel::<RuntimeCommand>();
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
        let (old_command_tx, old_command_rx) = mpsc::channel::<RuntimeCommand>();
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

        let (new_command_tx, _new_command_rx) = mpsc::channel::<RuntimeCommand>();
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

    #[test]
    fn hardware_advertisement_payload_uses_deckr_profile_api() {
        let mut state = ManagerState::new("saitek-main".to_string(), "manager-session".to_string());
        state.devices.insert(
            "fip".to_string(),
            device_descriptor(&sample_candidate(), "fip", "fingerprint:fip"),
        );

        let payload = state.hardware_payload().unwrap();
        let value = payload.to_value().unwrap();
        let parsed = HardwareBeaconPayload::from_value(value).unwrap();

        assert_eq!(parsed.manager_id, "saitek-main");
        assert_eq!(parsed.devices["fip"].device_ref.manager_id, "saitek-main");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn beacon_withdrawal_does_not_release_claim_route() {
        let beacon_state = MemoryStateStore::new();
        let manager = EndpointAddress::parse("hardware_manager:saitek-main").unwrap();
        let advertiser = BeaconAdvertiser::new(
            beacon_state.clone(),
            HARDWARE_FEATURE_ID,
            manager.clone(),
            "manager-session",
        )
        .advertisement_id("hardware-saitek-main-manager-session")
        .payload(serde_json::json!({}));
        let advertisement = advertiser.publish().await.unwrap();
        let (concord, contract, manager, mut lifecycle, mut routing) =
            claimed_route("saitek-main", "fip").await;

        advertiser.withdraw(&advertisement).await.unwrap();

        assert!(find_candidates(&beacon_state, HARDWARE_FEATURE_ID)
            .await
            .unwrap()
            .is_empty());
        let managed = lifecycle
            .reconcile(|_, _| -> deckr::Result<bool> { Ok(true) }, None)
            .await
            .unwrap();
        let reconcile =
            routing.reconcile_claims(&managed, &manager, &BTreeSet::from(["fip".to_string()]));

        assert!(reconcile.reset_devices.is_empty());
        assert!(routing.claim_recipient("fip").is_some());
        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::Valid
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_device_cancels_claim_route_without_withdrawing_advertisement() {
        let beacon_state = MemoryStateStore::new();
        let manager = EndpointAddress::parse("hardware_manager:saitek-main").unwrap();
        let advertiser = BeaconAdvertiser::new(
            beacon_state.clone(),
            HARDWARE_FEATURE_ID,
            manager,
            "manager-session",
        )
        .advertisement_id("hardware-saitek-main-manager-session")
        .payload(serde_json::json!({}));
        let advertisement = advertiser.publish().await.unwrap();
        let (concord, contract, manager, lifecycle, mut routing) =
            claimed_route("saitek-main", "fip").await;

        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let managed_contracts = { claim_manager.lock().await.managed_contracts() };
        cancel_managed_device_claims(claim_manager.clone(), managed_contracts, "fip", "Saitek")
            .await
            .unwrap();

        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::Cancelled
        );
        let managed = {
            let mut lifecycle = claim_manager.lock().await;
            lifecycle
                .reconcile(|_, _| -> deckr::Result<bool> { Ok(true) }, None)
                .await
                .unwrap()
        };
        let reconcile =
            routing.reconcile_claims(&managed, &manager, &BTreeSet::from(["fip".to_string()]));
        let candidates = find_candidates(&beacon_state, HARDWARE_FEATURE_ID)
            .await
            .unwrap();

        assert!(managed.is_empty());
        assert!(reconcile.reset_devices.contains("fip"));
        assert!(routing.claim_recipient("fip").is_none());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].key, advertisement.key);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn current_device_claim_is_accepted_and_attaches_manager_token() {
        let (concord, contract, manager_endpoint, lifecycle) =
            claim_context("saitek-main", "fip", Some("fingerprint:fip")).await;
        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let shared = shared_state_with_device("fip", "fingerprint:fip").await;

        reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "test",
            RoutingReconcileMode::Full,
        )
        .await
        .unwrap();

        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::Valid
        );
        assert!(concord
            .participant_token(&contract, &manager_endpoint)
            .await
            .unwrap()
            .is_some());
        assert!(shared.lock().await.routing.claim_recipient("fip").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_only_reconcile_does_not_discover_new_claims() {
        let (concord, _contract, manager_endpoint, lifecycle) =
            claim_context("saitek-main", "fip", Some("fingerprint:fip")).await;
        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let shared = shared_state_with_device("fip", "fingerprint:fip").await;

        reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "test",
            RoutingReconcileMode::Full,
        )
        .await
        .unwrap();

        let controller = EndpointAddress::parse("controller:main").unwrap();
        let second_contract = concord
            .create_contract(
                vec![controller.clone(), manager_endpoint.clone()],
                Some("contract-2".to_string()),
                1,
                Some(HARDWARE_CLAIM_PROFILE_ID.to_string()),
                Some(hardware_claim_terms_with_fingerprint(
                    "saitek-main",
                    "fip",
                    Some("fingerprint:fip"),
                )),
                Some(controller.clone()),
            )
            .await
            .unwrap();
        concord
            .attach(
                &second_contract,
                &controller,
                "controller-session",
                Some("controller-token-2".into()),
            )
            .await
            .unwrap();

        reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "token watch",
            RoutingReconcileMode::ManagedOnly,
        )
        .await
        .unwrap();

        assert_eq!(claim_manager.lock().await.managed_contracts().len(), 1);
        assert!(concord
            .participant_token(&second_contract, &manager_endpoint)
            .await
            .unwrap()
            .is_none());
        assert!(shared.lock().await.routing.claim_recipient("fip").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_device_claim_is_rejected_before_manager_token_attachment() {
        let (concord, contract, manager_endpoint, lifecycle) =
            claim_context("saitek-main", "missing-fip", None).await;
        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let shared = shared_state_with_device("fip", "fingerprint:fip").await;

        reconcile_routing_current_state(
            shared.clone(),
            claim_manager.clone(),
            "test",
            RoutingReconcileMode::Full,
        )
        .await
        .unwrap();

        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::NotYetFulfilled
        );
        assert!(concord
            .participant_token(&contract, &manager_endpoint)
            .await
            .unwrap()
            .is_none());
        assert!(claim_manager.lock().await.managed_contracts().is_empty());
        assert!(shared
            .lock()
            .await
            .routing
            .claim_recipient("missing-fip")
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fingerprint_mismatch_claim_is_rejected_before_manager_token_attachment() {
        let (concord, contract, manager_endpoint, lifecycle) =
            claim_context("saitek-main", "fip", Some("fingerprint:other")).await;
        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let shared = shared_state_with_device("fip", "fingerprint:fip").await;

        reconcile_routing_current_state(
            shared,
            claim_manager.clone(),
            "test",
            RoutingReconcileMode::Full,
        )
        .await
        .unwrap();

        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::NotYetFulfilled
        );
        assert!(concord
            .participant_token(&contract, &manager_endpoint)
            .await
            .unwrap()
            .is_none());
        assert!(claim_manager.lock().await.managed_contracts().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn omitted_fingerprint_claim_is_accepted_for_present_device() {
        let (concord, contract, manager_endpoint, lifecycle) =
            claim_context("saitek-main", "fip", None).await;
        let claim_manager = Arc::new(Mutex::new(lifecycle));
        let shared = shared_state_with_device("fip", "fingerprint:fip").await;

        reconcile_routing_current_state(
            shared.clone(),
            claim_manager,
            "test",
            RoutingReconcileMode::Full,
        )
        .await
        .unwrap();

        assert_eq!(
            concord.validate(&contract, None).await.status,
            ContractValidityStatus::Valid
        );
        assert!(concord
            .participant_token(&contract, &manager_endpoint)
            .await
            .unwrap()
            .is_some());
        assert!(shared.lock().await.routing.claim_recipient("fip").is_some());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_routing_requires_claiming_controller() {
        let (command_tx, command_rx) = mpsc::channel();
        let shared = Arc::new(Mutex::new(ManagerState::new(
            "saitek-main".to_string(),
            "manager-session".to_string(),
        )));
        {
            let mut state = shared.lock().await;
            state.devices.insert(
                "fip".to_string(),
                device_descriptor(&sample_candidate(), "fip", "fingerprint:fip"),
            );
            state.command_map.insert("fip".to_string(), command_tx);
            state.routing.reconcile_snapshot(
                HashMap::from([(
                    "fip".to_string(),
                    HardwareClaimRoute {
                        controller_endpoint: EndpointAddress::parse("controller:main").unwrap(),
                        controller_session_id: "s1".to_string(),
                        contract_key: "contracts.claim.1.meta".to_string(),
                        claim_id: "claim-1".to_string(),
                    },
                )]),
                HashSet::new(),
            );
        }

        let wrong = DeckrMessage::hardware_command(
            "other",
            "s1",
            "saitek-main",
            "manager-session",
            "fip",
            raster_command("set_frame"),
        )
        .unwrap();
        route_inbound_command(shared.clone(), wrong).await.unwrap();
        assert!(command_rx.try_recv().is_err());

        let stale_manager_session = DeckrMessage::hardware_command(
            "main",
            "s1",
            "saitek-main",
            "stale-manager-session",
            "fip",
            raster_command("set_frame"),
        )
        .unwrap();
        route_inbound_command(shared.clone(), stale_manager_session)
            .await
            .unwrap();
        assert!(command_rx.try_recv().is_err());

        let right = DeckrMessage::hardware_command(
            "main",
            "s1",
            "saitek-main",
            "manager-session",
            "fip",
            raster_command("set_frame"),
        )
        .unwrap();
        route_inbound_command(shared, right).await.unwrap();
        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::SetRasterFrame { .. }
        ));
    }
}
