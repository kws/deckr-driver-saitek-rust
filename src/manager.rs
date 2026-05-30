use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use deckr::beacon::{AdvertisementHandle, BeaconAdvertiser};
use deckr::concord::{
    ConcordCoordinator, ContractHandle, ContractValidityStatus, ParticipantHandle,
};
use deckr::endpoint::{hardware_manager_address, EndpointAddress};
use deckr::keys::concord_contracts_prefix;
use deckr::lanes::{
    DeckrMessage, DeviceRef, HardwareMessageBody, HARDWARE_MESSAGES_LANE as WIRE_HARDWARE_LANE,
};
use deckr::nats::NatsDeckrRuntime;
use deckr::profiles::hardware::{
    HardwareAdvertisementDevice, HardwareBeaconPayload, HardwareClaimTerms, ProfileCapacity,
    HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
};
use deckr::state::{StateStore, DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS};
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
use crate::routing::{ClaimRoute, RoutingState};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const USB_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BACKOFF_SECS: u64 = 10;
const HEARTBEAT_SECONDS: u64 = DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS;
const STATE_RECONCILE_SECONDS: u64 = 1;
const WATCH_RETRY_SECONDS: u64 = 1;

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

pub struct SaitekRemoteManager {
    nats_url: String,
    manager_id: String,
    session_id: String,
    backend: Arc<dyn Backend>,
}

impl SaitekRemoteManager {
    pub fn new(nats_url: String, manager_id: String) -> Result<Self> {
        Ok(Self::with_backend(
            nats_url,
            manager_id,
            Arc::new(UsbBackend),
        ))
    }

    pub fn with_backend(nats_url: String, manager_id: String, backend: Arc<dyn Backend>) -> Self {
        Self {
            nats_url,
            manager_id,
            session_id: Uuid::new_v4().to_string(),
            backend,
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

        let shared = Arc::new(Mutex::new(ManagerState::new(
            self.manager_id.clone(),
            self.session_id.clone(),
        )));
        let (supervisor_event_tx, supervisor_event_rx) =
            tokio_mpsc::unbounded_channel::<WorkerEvent>();
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
        ));
        tasks.spawn(inbound_command_loop(runtime.clone(), shared.clone()));
        tasks.spawn(hardware_advertisement_loop(runtime.clone(), shared.clone()));
        tasks.spawn(concord_contract_watch_loop(runtime.clone(), shared.clone()));
        tasks.spawn(concord_token_watch_loop(runtime.clone(), shared.clone()));
        tasks.spawn(routing_reconciliation_loop(runtime.clone(), shared.clone()));

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
    routing: RoutingState,
    advertisement_handle: Option<AdvertisementHandle>,
    advertisement_dirty: bool,
    concord_tokens: HashMap<String, ConcordLeaseState>,
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
            routing: RoutingState::default(),
            advertisement_handle: None,
            advertisement_dirty: false,
            concord_tokens: HashMap::new(),
        }
    }

    fn hardware_payload(&self) -> Result<HardwareBeaconPayload> {
        Ok(HardwareBeaconPayload {
            profile: deckr::profiles::hardware::HARDWARE_PROFILE_ID.to_string(),
            manager_id: self.manager_id.clone(),
            manager_endpoint: EndpointAddress::parse(&self.endpoint)?,
            session_id: self.session_id.clone(),
            labels: BTreeMap::new(),
            devices: self
                .devices
                .iter()
                .map(|(device_id, descriptor)| {
                    (
                        device_id.clone(),
                        HardwareAdvertisementDevice {
                            capacity: ProfileCapacity {
                                total_instances: Some(1),
                                claimed_instances: 0,
                                available_instances: Some(1),
                            },
                            device_ref: DeviceRef {
                                manager_id: self.manager_id.clone(),
                                device_id: device_id.clone(),
                                fingerprint: Some(descriptor.fingerprint.clone()),
                            },
                            descriptor: descriptor.clone(),
                        },
                    )
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct ConcordLeaseState {
    token: Option<ParticipantHandle>,
    lost_authority: bool,
}

async fn hardware_advertisement_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
        time::sleep(Duration::from_secs(HEARTBEAT_SECONDS)).await;
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

async fn concord_contract_watch_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        match runtime
            .concord_contracts()
            .wait_for_change(concord_contracts_prefix())
            .await
        {
            Ok(()) => {
                reconcile_routing_current_state(runtime.clone(), shared.clone(), "contract watch")
                    .await?
            }
            Err(error) => {
                warn!("Saitek Concord contract watch is unavailable; watch will retry: {error:#}");
                time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
            }
        }
    }
}

async fn concord_token_watch_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        match runtime
            .concord_tokens()
            .wait_for_change(concord_contracts_prefix())
            .await
        {
            Ok(()) => {
                reconcile_routing_current_state(runtime.clone(), shared.clone(), "token watch")
                    .await?
            }
            Err(error) => {
                warn!("Saitek Concord token watch is unavailable; watch will retry: {error:#}");
                time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
            }
        }
    }
}

async fn routing_reconciliation_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        if let Err(error) =
            reconcile_routing_current_state(runtime.clone(), shared.clone(), "broker snapshot")
                .await
        {
            warn!("Saitek routing current state unavailable; reconciliation will retry: {error:#}");
        }
        time::sleep(Duration::from_secs(STATE_RECONCILE_SECONDS)).await;
    }
}

async fn reconcile_routing_current_state(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    reason: &'static str,
) -> Result<()> {
    let concord = ConcordCoordinator::new(
        runtime.concord_contracts().clone(),
        runtime.concord_tokens().clone(),
    );
    let contracts = concord
        .find_contracts(Some(HARDWARE_CLAIM_PROFILE_ID))
        .await?;
    let (manager_endpoint, manager_session, known_devices) = {
        let state = shared.lock().await;
        (
            state.endpoint.clone(),
            state.session_id.clone(),
            state.devices.keys().cloned().collect::<HashSet<_>>(),
        )
    };
    let manager_endpoint = EndpointAddress::parse(&manager_endpoint)?;
    let mut next_claims = HashMap::<String, ClaimRoute>::new();
    let mut invalid_claim_devices = HashSet::<String>::new();

    for contract in contracts {
        if !contract.participants.contains(&manager_endpoint) {
            continue;
        }
        let Some(record) = concord.contract_record(&contract).await? else {
            continue;
        };
        let Some(terms_value) = record.terms.clone() else {
            continue;
        };
        let terms = match HardwareClaimTerms::from_value(terms_value) {
            Ok(terms) => terms,
            Err(error) => {
                warn!(
                    "Ignoring invalid Saitek hardware claim contract {}: {error}",
                    contract.key
                );
                continue;
            }
        };
        if terms.manager_endpoint != manager_endpoint {
            continue;
        }

        let token_state = ensure_manager_concord_token(
            &concord,
            &contract,
            &manager_endpoint,
            &manager_session,
            shared.clone(),
        )
        .await;
        if let Err(error) = token_state {
            warn!(
                "Saitek Concord token maintenance failed for {}: {error:#}",
                contract.key
            );
        }

        let validity = concord.validate(&contract, None).await;
        if validity.status != ContractValidityStatus::Valid {
            for device in &terms.devices {
                if known_devices.contains(&device.device_ref.device_id) {
                    invalid_claim_devices.insert(device.device_ref.device_id.clone());
                }
            }
            continue;
        }

        let controller_endpoint = terms.controller_endpoint.to_string();
        let Some(controller_token) = validity.tokens.get(&controller_endpoint) else {
            continue;
        };
        for device in &terms.devices {
            if !known_devices.contains(&device.device_ref.device_id) {
                continue;
            }
            next_claims.insert(
                device.device_ref.device_id.clone(),
                ClaimRoute {
                    controller_endpoint: controller_endpoint.clone(),
                    controller_session_id: controller_token.session_id.clone(),
                    contract_key: contract.key.clone(),
                    claim_id: terms.claim_id.clone(),
                },
            );
        }
    }

    debug!("Reconciling Saitek routing current state via {reason}");
    let senders_to_reset = {
        let mut state = shared.lock().await;
        invalid_claim_devices.retain(|device_id| !next_claims.contains_key(device_id));
        let devices_to_reset = state
            .routing
            .reconcile_snapshot(next_claims, invalid_claim_devices);
        devices_to_reset
            .into_iter()
            .filter_map(|device_id| state.command_map.get(&device_id).cloned())
            .collect::<Vec<_>>()
    };
    for sender in senders_to_reset {
        let _ = sender.send(RuntimeCommand::ResetDevice);
    }
    Ok(())
}

async fn ensure_manager_concord_token<C, T>(
    concord: &ConcordCoordinator<C, T>,
    contract: &ContractHandle,
    manager_endpoint: &EndpointAddress,
    manager_session: &str,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()>
where
    C: StateStore,
    T: StateStore,
{
    let existing = {
        let state = shared.lock().await;
        state.concord_tokens.get(&contract.key).cloned()
    };
    if existing.as_ref().is_some_and(|state| state.lost_authority) {
        return Ok(());
    }

    if let Some(token) = existing.as_ref().and_then(|state| state.token.clone()) {
        match concord.refresh(&token).await {
            Ok(refreshed) => {
                shared.lock().await.concord_tokens.insert(
                    contract.key.clone(),
                    ConcordLeaseState {
                        token: Some(refreshed),
                        lost_authority: false,
                    },
                );
                return Ok(());
            }
            Err(error) => {
                if is_concord_refresh_race(&error) {
                    return Ok(());
                }
                shared.lock().await.concord_tokens.insert(
                    contract.key.clone(),
                    ConcordLeaseState {
                        token: None,
                        lost_authority: true,
                    },
                );
                return Err(error.into());
            }
        }
    }

    if contract.attached_participants.contains(manager_endpoint) {
        shared.lock().await.concord_tokens.insert(
            contract.key.clone(),
            ConcordLeaseState {
                token: None,
                lost_authority: true,
            },
        );
        return Ok(());
    }

    let token = concord
        .attach(contract, manager_endpoint, manager_session, None)
        .await?;
    shared.lock().await.concord_tokens.insert(
        contract.key.clone(),
        ConcordLeaseState {
            token: Some(token),
            lost_authority: false,
        },
    );
    Ok(())
}

fn is_concord_refresh_race(error: &deckr::Error) -> bool {
    matches!(error, deckr::Error::StateConflict(message) if message.contains("revision changed"))
}

async fn worker_event_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
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
                let descriptor = device.clone();
                let (manager_id, session_id) = {
                    let mut state = shared.lock().await;
                    let manager_id = state.manager_id.clone();
                    let session_id = state.session_id.clone();
                    state.devices.insert(device_id.clone(), device);
                    state.command_map.insert(device_id.clone(), command_tx);
                    (manager_id, session_id)
                };
                publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
                let message = DeckrMessage::hardware_input(
                    &manager_id,
                    &session_id,
                    &device_id,
                    HardwareMessageBody::DeviceAvailable { descriptor },
                )?;
                runtime.publish(&message).await?;
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
                let (manager_id, session_id) = {
                    let mut state = shared.lock().await;
                    let manager_id = state.manager_id.clone();
                    let session_id = state.session_id.clone();
                    state.devices.remove(&device_id);
                    state.command_map.remove(&device_id);
                    state.routing.remove_device(&device_id);
                    (manager_id, session_id)
                };
                publish_hardware_advertisement_safely(runtime.clone(), shared.clone()).await;
                let message = DeckrMessage::hardware_input(
                    &manager_id,
                    &session_id,
                    &device_id,
                    HardwareMessageBody::DeviceUnavailable {
                        device_ref: DeviceRef {
                            manager_id: manager_id.clone(),
                            device_id: device_id.clone(),
                            fingerprint: None,
                        },
                        reason: Some("disconnected".to_string()),
                    },
                )?;
                runtime.publish(&message).await?;
            }
            WorkerEvent::Failed { path_key, error } => {
                warn!("Device worker {path_key} failed: {error}");
            }
        }
    }
    bail!("device worker event stream closed")
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
                recipient.endpoint != envelope.sender
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
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
    manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
}

impl Supervisor {
    fn new(
        manager_id: String,
        backend: Arc<dyn Backend>,
        worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
        worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
        manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    ) -> Self {
        Self {
            manager_id,
            backend,
            worker_tx,
            worker_rx,
            manager_tx,
        }
    }

    async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        let mut discovery = time::interval(DISCOVERY_INTERVAL);
        let mut active_paths = HashSet::<String>::new();
        let mut launched_paths = HashSet::<String>::new();
        let mut worker_senders = Vec::<Sender<RuntimeCommand>>::new();

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                _ = discovery.tick() => {
                    let descriptors = enumerate_canonical(self.backend.clone()).await?;
                    for descriptor in descriptors {
                        let path_key = descriptor.path_key();
                        if active_paths.contains(&path_key) || launched_paths.contains(&path_key) {
                            continue;
                        }
                        launched_paths.insert(path_key.clone());
                        let (command_tx, command_rx) = mpsc::channel::<RuntimeCommand>();
                        worker_senders.push(command_tx.clone());
                        spawn_device_worker(
                            self.manager_id.clone(),
                            self.backend.clone(),
                            descriptor,
                            self.worker_tx.clone(),
                            command_tx,
                            command_rx,
                        );
                    }
                }
                maybe_event = self.worker_rx.recv() => {
                    let Some(event) = maybe_event else { continue; };
                    match &event {
                        WorkerEvent::Connected { path_key, .. } => {
                            launched_paths.remove(path_key.as_str());
                            active_paths.insert(path_key.clone());
                        }
                        WorkerEvent::Disconnected { path_key, .. }
                        | WorkerEvent::Failed { path_key, .. } => {
                            launched_paths.remove(path_key.as_str());
                            active_paths.remove(path_key.as_str());
                        }
                        WorkerEvent::Input { .. } => {}
                    }
                    let _ = self.manager_tx.send(event);
                }
            }
        }

        for sender in worker_senders {
            let _ = sender.send(RuntimeCommand::Stop);
        }
        Ok(())
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
    manager_id: String,
    backend: Arc<dyn Backend>,
    descriptor: DeviceCandidate,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
) {
    let path_key = descriptor.path_key();
    thread::spawn(move || {
        if let Err(error) = device_worker(
            backend,
            manager_id,
            descriptor,
            worker_tx.clone(),
            command_tx,
            command_rx,
        ) {
            let _ = worker_tx.send(WorkerEvent::Failed {
                path_key,
                error: format!("{error:#}"),
            });
        }
    });
}

fn device_worker(
    backend: Arc<dyn Backend>,
    manager_id: String,
    descriptor: DeviceCandidate,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
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
        .send(WorkerEvent::Connected {
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
                let _ = worker_tx.send(WorkerEvent::Disconnected {
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
                    let _ = worker_tx.send(WorkerEvent::Disconnected {
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
                let _ = worker_tx.send(WorkerEvent::Input {
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
                    ClaimRoute {
                        controller_endpoint: "controller:main".to_string(),
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
