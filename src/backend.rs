use std::time::Duration;

use anyhow::{bail, Context, Result};
use rusb::{
    ConfigDescriptor, Device, DeviceDescriptor as UsbDeviceDescriptor,
    DeviceHandle as UsbDeviceHandle, Direction, GlobalContext, TransferType,
};

use crate::protocol::{
    clear_image_packet, probe_packet, set_image_packet, set_led_packet, validate_frame_size,
    validate_probe_reply, validate_set_image_reply, FipControlPacket, CONTROL_PACKET_SIZE,
    HID_REPORT_SIZE, PID_SAITEK_FIP, VID_SAITEK,
};

const USB_CLASS_HID: u8 = 0x03;
const USB_CLASS_VENDOR_SPECIFIC: u8 = 0xFF;

pub trait Backend: Send + Sync + 'static {
    fn enumerate(&self) -> Result<Vec<DeviceCandidate>>;
    fn open(&self, candidate: &DeviceCandidate, timeout: Duration)
        -> Result<Box<dyn DeviceHandle>>;
}

pub trait DeviceHandle {
    fn has_hid_input(&self) -> bool;
    fn probe(&mut self) -> Result<FipControlPacket>;
    fn clear_image(&mut self, page: u32) -> Result<FipControlPacket>;
    fn send_image(&mut self, frame: &[u8], page: u32) -> Result<FipControlPacket>;
    fn set_led(&mut self, page: u32, index: u32, value: bool) -> Result<FipControlPacket>;
    fn read_hid_report(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCandidate {
    pub bus_number: u8,
    pub address: u8,
    pub vendor_id: u16,
    pub product_id: u16,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
    pub serial_number: Option<String>,
    pub vendor_interface: u8,
    pub vendor_bulk_out: u8,
    pub vendor_bulk_in: u8,
    pub vendor_out_packet_size: usize,
    pub hid_interface: Option<u8>,
    pub hid_interrupt_in: Option<u8>,
    pub hid_read_size: usize,
}

impl DeviceCandidate {
    pub fn hardware_id(&self) -> String {
        let identity = self
            .serial_number
            .as_deref()
            .filter(|serial| !serial.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("bus{:03}-addr{:03}", self.bus_number, self.address));
        format!("{:04X}:{:04X}:{identity}", self.vendor_id, self.product_id)
    }

    pub fn path_key(&self) -> String {
        format!("bus{:03}-addr{:03}", self.bus_number, self.address)
    }

    pub fn without_hid_input(&self) -> Self {
        let mut candidate = self.clone();
        candidate.hid_interface = None;
        candidate.hid_interrupt_in = None;
        candidate.hid_read_size = HID_REPORT_SIZE;
        candidate
    }
}

#[derive(Debug, Default)]
pub struct UsbBackend;

impl Backend for UsbBackend {
    fn enumerate(&self) -> Result<Vec<DeviceCandidate>> {
        let devices = rusb::devices().context("enumerating USB devices")?;
        let mut candidates = Vec::new();

        for device in devices.iter() {
            let descriptor = device
                .device_descriptor()
                .context("reading USB device descriptor")?;
            if descriptor.vendor_id() != VID_SAITEK || descriptor.product_id() != PID_SAITEK_FIP {
                continue;
            }
            let config = match active_or_first_config(&device) {
                Ok(config) => config,
                Err(error) => {
                    tracing::warn!(
                        "Skipping Saitek FIP bus={} address={} without readable config: {error:#}",
                        device.bus_number(),
                        device.address()
                    );
                    continue;
                }
            };
            let Some(endpoints) = find_fip_endpoints(&config) else {
                tracing::warn!(
                    "Skipping Saitek FIP bus={} address={} without vendor bulk endpoints",
                    device.bus_number(),
                    device.address()
                );
                continue;
            };
            let strings = read_usb_strings(&device, &descriptor);
            candidates.push(DeviceCandidate {
                bus_number: device.bus_number(),
                address: device.address(),
                vendor_id: descriptor.vendor_id(),
                product_id: descriptor.product_id(),
                manufacturer: strings.manufacturer,
                product: strings.product,
                serial_number: strings.serial_number,
                vendor_interface: endpoints.vendor_interface,
                vendor_bulk_out: endpoints.vendor_bulk_out,
                vendor_bulk_in: endpoints.vendor_bulk_in,
                vendor_out_packet_size: endpoints.vendor_out_packet_size,
                hid_interface: endpoints.hid_interface,
                hid_interrupt_in: endpoints.hid_interrupt_in,
                hid_read_size: endpoints.hid_read_size.unwrap_or(HID_REPORT_SIZE),
            });
        }

        candidates.sort_by_key(|candidate| (candidate.bus_number, candidate.address));
        Ok(candidates)
    }

    fn open(
        &self,
        candidate: &DeviceCandidate,
        timeout: Duration,
    ) -> Result<Box<dyn DeviceHandle>> {
        let device = find_device(candidate)?;
        let mut handle = device.open().with_context(|| {
            format!(
                "opening Saitek FIP bus={} address={}",
                candidate.bus_number, candidate.address
            )
        })?;

        let mut claimed = Vec::new();
        let mut detached = Vec::new();
        claim_interface(
            &mut handle,
            candidate.vendor_interface,
            &mut claimed,
            &mut detached,
        )
        .with_context(|| format!("claiming vendor interface {}", candidate.vendor_interface))?;
        let mut runtime_candidate = candidate.clone();
        if let Some(hid_interface) = candidate.hid_interface {
            if hid_interface != candidate.vendor_interface {
                let claimed_before = claimed.len();
                let detached_before = detached.len();
                if let Err(error) =
                    claim_interface(&mut handle, hid_interface, &mut claimed, &mut detached)
                        .with_context(|| format!("claiming HID interface {hid_interface}"))
                {
                    rollback_interfaces(
                        &mut handle,
                        &mut claimed,
                        claimed_before,
                        &mut detached,
                        detached_before,
                    );
                    tracing::warn!(
                        "Continuing without Saitek FIP HID input for {}: {error:#}",
                        candidate.path_key()
                    );
                    runtime_candidate = runtime_candidate.without_hid_input();
                }
            }
        }

        Ok(Box::new(UsbFipHandle {
            handle,
            candidate: runtime_candidate,
            timeout,
            claimed,
            detached,
        }))
    }
}

struct UsbFipHandle {
    handle: UsbDeviceHandle<GlobalContext>,
    candidate: DeviceCandidate,
    timeout: Duration,
    claimed: Vec<u8>,
    detached: Vec<u8>,
}

impl DeviceHandle for UsbFipHandle {
    fn has_hid_input(&self) -> bool {
        self.candidate.hid_interrupt_in.is_some()
    }

    fn probe(&mut self) -> Result<FipControlPacket> {
        let reply = self.transceive(probe_packet(), None)?;
        validate_probe_reply(reply)?;
        Ok(reply)
    }

    fn clear_image(&mut self, page: u32) -> Result<FipControlPacket> {
        self.transceive(clear_image_packet(page), None)
    }

    fn send_image(&mut self, frame: &[u8], page: u32) -> Result<FipControlPacket> {
        validate_frame_size(frame)?;
        let reply = self.transceive(set_image_packet(page), Some(frame))?;
        validate_set_image_reply(reply, page)?;
        Ok(reply)
    }

    fn set_led(&mut self, page: u32, index: u32, value: bool) -> Result<FipControlPacket> {
        self.transceive(set_led_packet(page, index, value), None)
    }

    fn read_hid_report(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let Some(endpoint) = self.candidate.hid_interrupt_in else {
            return Ok(None);
        };
        let mut buffer = vec![0u8; self.candidate.hid_read_size.max(HID_REPORT_SIZE)];
        match self.handle.read_interrupt(endpoint, &mut buffer, timeout) {
            Ok(size) => {
                buffer.truncate(size);
                Ok(Some(buffer))
            }
            Err(rusb::Error::Timeout) => Ok(None),
            Err(error) => Err(error).context("reading FIP HID interrupt report"),
        }
    }
}

impl UsbFipHandle {
    fn transceive(
        &mut self,
        packet: FipControlPacket,
        payload: Option<&[u8]>,
    ) -> Result<FipControlPacket> {
        self.write_all(&packet.to_bytes())?;
        if let Some(payload) = payload {
            self.write_all(payload)?;
        }
        self.read_reply()
    }

    fn write_all(&mut self, data: &[u8]) -> Result<()> {
        let chunk_size = self.candidate.vendor_out_packet_size.max(1);
        for chunk in data.chunks(chunk_size) {
            let written = self
                .handle
                .write_bulk(self.candidate.vendor_bulk_out, chunk, self.timeout)
                .context("USB bulk OUT transfer failed")?;
            if written != chunk.len() {
                bail!("Short USB write: wrote {written} of {} bytes", chunk.len());
            }
        }
        Ok(())
    }

    fn read_reply(&mut self) -> Result<FipControlPacket> {
        let mut buffer = [0u8; CONTROL_PACKET_SIZE];
        let size = self
            .handle
            .read_bulk(self.candidate.vendor_bulk_in, &mut buffer, self.timeout)
            .context("USB bulk IN reply read failed")?;
        if size != CONTROL_PACKET_SIZE {
            bail!("Short USB reply: expected {CONTROL_PACKET_SIZE} bytes, got {size}");
        }
        FipControlPacket::from_bytes(&buffer)
    }
}

impl Drop for UsbFipHandle {
    fn drop(&mut self) {
        for interface in self.claimed.iter().rev() {
            let _ = self.handle.release_interface(*interface);
        }
        for interface in self.detached.iter().rev() {
            let _ = self.handle.attach_kernel_driver(*interface);
        }
    }
}

#[derive(Debug, Default)]
struct EndpointSet {
    vendor_interface: u8,
    vendor_bulk_out: u8,
    vendor_bulk_in: u8,
    vendor_out_packet_size: usize,
    hid_interface: Option<u8>,
    hid_interrupt_in: Option<u8>,
    hid_read_size: Option<usize>,
}

struct UsbStrings {
    manufacturer: Option<String>,
    product: Option<String>,
    serial_number: Option<String>,
}

fn active_or_first_config(device: &Device<GlobalContext>) -> Result<ConfigDescriptor> {
    device
        .active_config_descriptor()
        .or_else(|_| device.config_descriptor(0))
        .context("reading active or first USB configuration")
}

fn find_fip_endpoints(config: &ConfigDescriptor) -> Option<EndpointSet> {
    let mut endpoints = EndpointSet::default();
    let mut found_vendor = false;

    for interface in config.interfaces() {
        for descriptor in interface.descriptors() {
            if descriptor.class_code() == USB_CLASS_VENDOR_SPECIFIC {
                let mut bulk_out = None;
                let mut bulk_in = None;
                let mut out_packet_size = None;
                for endpoint in descriptor.endpoint_descriptors() {
                    if endpoint.transfer_type() != TransferType::Bulk {
                        continue;
                    }
                    match endpoint.direction() {
                        Direction::Out => {
                            bulk_out = Some(endpoint.address());
                            out_packet_size = Some(endpoint.max_packet_size() as usize);
                        }
                        Direction::In => bulk_in = Some(endpoint.address()),
                    }
                }
                if let (Some(vendor_bulk_out), Some(vendor_bulk_in)) = (bulk_out, bulk_in) {
                    endpoints.vendor_interface = descriptor.interface_number();
                    endpoints.vendor_bulk_out = vendor_bulk_out;
                    endpoints.vendor_bulk_in = vendor_bulk_in;
                    endpoints.vendor_out_packet_size = out_packet_size.unwrap_or(512);
                    found_vendor = true;
                }
            }

            if descriptor.class_code() == USB_CLASS_HID {
                for endpoint in descriptor.endpoint_descriptors() {
                    if endpoint.direction() == Direction::In
                        && endpoint.transfer_type() == TransferType::Interrupt
                    {
                        endpoints.hid_interface = Some(descriptor.interface_number());
                        endpoints.hid_interrupt_in = Some(endpoint.address());
                        endpoints.hid_read_size = Some(endpoint.max_packet_size() as usize);
                        break;
                    }
                }
            }
        }
    }

    found_vendor.then_some(endpoints)
}

fn read_usb_strings(
    device: &Device<GlobalContext>,
    descriptor: &UsbDeviceDescriptor,
) -> UsbStrings {
    let Ok(handle) = device.open() else {
        return UsbStrings {
            manufacturer: None,
            product: None,
            serial_number: None,
        };
    };
    UsbStrings {
        manufacturer: handle.read_manufacturer_string_ascii(descriptor).ok(),
        product: handle.read_product_string_ascii(descriptor).ok(),
        serial_number: handle.read_serial_number_string_ascii(descriptor).ok(),
    }
}

fn find_device(candidate: &DeviceCandidate) -> Result<Device<GlobalContext>> {
    let devices = rusb::devices().context("enumerating USB devices")?;
    for device in devices.iter() {
        if device.bus_number() == candidate.bus_number && device.address() == candidate.address {
            return Ok(device);
        }
    }
    bail!(
        "Saitek FIP bus={} address={} is no longer present",
        candidate.bus_number,
        candidate.address
    )
}

fn claim_interface(
    handle: &mut UsbDeviceHandle<GlobalContext>,
    interface: u8,
    claimed: &mut Vec<u8>,
    detached: &mut Vec<u8>,
) -> Result<()> {
    if matches!(handle.kernel_driver_active(interface), Ok(true)) {
        handle
            .detach_kernel_driver(interface)
            .with_context(|| format!("detaching kernel driver for interface {interface}"))?;
        detached.push(interface);
    }
    handle
        .claim_interface(interface)
        .with_context(|| format!("claiming interface {interface}"))?;
    claimed.push(interface);
    Ok(())
}

fn rollback_interfaces(
    handle: &mut UsbDeviceHandle<GlobalContext>,
    claimed: &mut Vec<u8>,
    claimed_before: usize,
    detached: &mut Vec<u8>,
    detached_before: usize,
) {
    for interface in claimed.drain(claimed_before..).rev() {
        let _ = handle.release_interface(interface);
    }
    for interface in detached.drain(detached_before..).rev() {
        let _ = handle.attach_kernel_driver(interface);
    }
}
