use anyhow::{bail, Result};

pub const VID_SAITEK: u16 = 0x06A3;
pub const PID_SAITEK_FIP: u16 = 0xA2AE;

pub const WIDTH: usize = 320;
pub const HEIGHT: usize = 240;
pub const BYTES_PER_PIXEL: usize = 3;
pub const FRAME_BYTES: usize = WIDTH * HEIGHT * BYTES_PER_PIXEL;

pub const CONTROL_PACKET_WORDS: usize = 11;
pub const CONTROL_PACKET_SIZE: usize = CONTROL_PACKET_WORDS * 4;

pub const REQ_SET_IMAGE: u32 = 0x06;
pub const REQ_PROBE: u32 = 0x0A;
pub const REQ_CLEAR_IMAGE: u32 = 0x13;
pub const REQ_SET_LED: u32 = 0x18;

pub const HID_REPORT_SIZE: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FipControlPacket {
    pub server_id: u32,
    pub page: u32,
    pub data_size: u32,
    pub header_error: u32,
    pub header_info: u32,
    pub request: u32,
    pub param_1: u32,
    pub param_2: u32,
    pub param_3: u32,
    pub request_error: u32,
    pub request_info: u32,
}

impl FipControlPacket {
    pub fn as_words(self) -> [u32; CONTROL_PACKET_WORDS] {
        [
            self.server_id,
            self.page,
            self.data_size,
            self.header_error,
            self.header_info,
            self.request,
            self.param_1,
            self.param_2,
            self.param_3,
            self.request_error,
            self.request_info,
        ]
    }

    pub fn to_bytes(self) -> [u8; CONTROL_PACKET_SIZE] {
        let mut out = [0u8; CONTROL_PACKET_SIZE];
        for (index, word) in self.as_words().iter().enumerate() {
            let start = index * 4;
            out[start..start + 4].copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        if raw.len() != CONTROL_PACKET_SIZE {
            bail!(
                "FIP control packet must be {CONTROL_PACKET_SIZE} bytes, got {}",
                raw.len()
            );
        }
        let mut words = [0u32; CONTROL_PACKET_WORDS];
        for (index, chunk) in raw.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        Ok(Self {
            server_id: words[0],
            page: words[1],
            data_size: words[2],
            header_error: words[3],
            header_info: words[4],
            request: words[5],
            param_1: words[6],
            param_2: words[7],
            param_3: words[8],
            request_error: words[9],
            request_info: words[10],
        })
    }

    pub fn has_error(self) -> bool {
        self.header_error > 0 || self.request_error > 0
    }
}

pub fn fip_packet(
    request: u32,
    page: u32,
    data_size: u32,
    server_id: u32,
    param_1: u32,
    param_2: u32,
    param_3: u32,
) -> FipControlPacket {
    FipControlPacket {
        server_id,
        page,
        data_size,
        request,
        param_1,
        param_2,
        param_3,
        ..FipControlPacket::default()
    }
}

pub fn probe_packet() -> FipControlPacket {
    fip_packet(REQ_PROBE, 0, 0, 0, 0, 0, 0)
}

pub fn set_image_packet(page: u32) -> FipControlPacket {
    fip_packet(REQ_SET_IMAGE, page, FRAME_BYTES as u32, 0, 0, 0, 0)
}

pub fn clear_image_packet(page: u32) -> FipControlPacket {
    fip_packet(REQ_CLEAR_IMAGE, page, 0, 0, 0, 0, 0)
}

pub fn set_led_packet(page: u32, index: u32, value: bool) -> FipControlPacket {
    fip_packet(REQ_SET_LED, 0, 0, 0, page, index, u32::from(value))
}

pub fn validate_frame_size(frame: &[u8]) -> Result<()> {
    if frame.len() != FRAME_BYTES {
        bail!("FIP frame must be {FRAME_BYTES} bytes, got {}", frame.len());
    }
    Ok(())
}

pub fn validate_probe_reply(reply: FipControlPacket) -> Result<()> {
    if reply.request != REQ_PROBE {
        bail!(
            "Probe reply echoed unexpected request 0x{:02x}",
            reply.request
        );
    }
    Ok(())
}

pub fn validate_set_image_reply(reply: FipControlPacket, page: u32) -> Result<()> {
    let mut problems = Vec::new();
    if reply.request != REQ_SET_IMAGE {
        problems.push(format!("request=0x{:02x}", reply.request));
    }
    if reply.page != page {
        problems.push(format!("page={}", reply.page));
    }
    if reply.data_size != 0 {
        problems.push(format!("data_size={}", reply.data_size));
    }
    if reply.header_error != 0 {
        problems.push(format!("header_error=0x{:08x}", reply.header_error));
    }
    if reply.request_error != 0 {
        problems.push(format!("request_error=0x{:08x}", reply.request_error));
    }

    if !problems.is_empty() {
        bail!(
            "FIP SetImage failed or returned unexpected ack: {}",
            problems.join(", ")
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteOrder {
    Big,
    Little,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HidControl {
    pub name: &'static str,
    pub mask: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HidEvent {
    pub control: HidControl,
    pub pressed: bool,
}

pub const HID_CONTROLS: &[HidControl] = &[
    HidControl {
        name: "UP",
        mask: 0x0001,
    },
    HidControl {
        name: "DOWN",
        mask: 0x0002,
    },
    HidControl {
        name: "RIGHT_ANTICLOCKWISE",
        mask: 0x0004,
    },
    HidControl {
        name: "RIGHT_CLOCKWISE",
        mask: 0x0008,
    },
    HidControl {
        name: "S1",
        mask: 0x0100,
    },
    HidControl {
        name: "S2",
        mask: 0x0200,
    },
    HidControl {
        name: "S3",
        mask: 0x0400,
    },
    HidControl {
        name: "S4",
        mask: 0x0800,
    },
    HidControl {
        name: "S5",
        mask: 0x1000,
    },
    HidControl {
        name: "S6",
        mask: 0x2000,
    },
    HidControl {
        name: "LEFT_ANTICLOCKWISE",
        mask: 0x4000,
    },
    HidControl {
        name: "LEFT_CLOCKWISE",
        mask: 0x8000,
    },
];

pub fn decode_hid_mask(report: &[u8], offset: usize, byte_order: ByteOrder) -> Result<u16> {
    let end = offset + HID_REPORT_SIZE;
    if report.len() < end {
        bail!(
            "HID report needs at least {end} bytes for offset {offset}, got {}",
            report.len()
        );
    }
    let bytes = [report[offset], report[offset + 1]];
    Ok(match byte_order {
        ByteOrder::Big => u16::from_be_bytes(bytes),
        ByteOrder::Little => u16::from_le_bytes(bytes),
    })
}

pub fn active_controls(mask: u16) -> Vec<HidControl> {
    HID_CONTROLS
        .iter()
        .copied()
        .filter(|control| mask & control.mask != 0)
        .collect()
}

pub fn changed_events(previous_mask: u16, current_mask: u16) -> Vec<HidEvent> {
    let changed = previous_mask ^ current_mask;
    HID_CONTROLS
        .iter()
        .copied()
        .filter(|control| changed & control.mask != 0)
        .map(|control| HidEvent {
            control,
            pressed: current_mask & control.mask != 0,
        })
        .collect()
}

pub fn unknown_hid_bits(mask: u16) -> u16 {
    let known = HID_CONTROLS
        .iter()
        .fold(0u16, |bits, control| bits | control.mask);
    mask & !known
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_packet_is_expected_big_endian_shape() {
        let raw = probe_packet().to_bytes();

        assert_eq!(raw.len(), CONTROL_PACKET_SIZE);
        assert_eq!(
            raw.map(|byte| format!("{byte:02x}")).join(""),
            "00000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000"
        );
        assert_eq!(
            FipControlPacket::from_bytes(&raw).unwrap().request,
            REQ_PROBE
        );
    }

    #[test]
    fn set_image_packet_has_frame_size_and_page() {
        let packet = set_image_packet(1);

        assert_eq!(packet.request, REQ_SET_IMAGE);
        assert_eq!(packet.page, 1);
        assert_eq!(packet.data_size, FRAME_BYTES as u32);
        assert_eq!(
            &packet.to_bytes()[8..12],
            &(FRAME_BYTES as u32).to_be_bytes()
        );
    }

    #[test]
    fn valid_set_image_reply_is_accepted() {
        let reply = FipControlPacket {
            request: REQ_SET_IMAGE,
            page: 1,
            ..FipControlPacket::default()
        };

        validate_set_image_reply(reply, 1).unwrap();
    }

    #[test]
    fn invalid_set_image_reply_is_rejected() {
        let reply = FipControlPacket {
            request: REQ_SET_IMAGE,
            page: 1,
            request_error: 2,
            ..FipControlPacket::default()
        };

        assert!(validate_set_image_reply(reply, 1).is_err());
    }

    #[test]
    fn frame_size_validation() {
        validate_frame_size(&vec![0; FRAME_BYTES]).unwrap();
        assert!(validate_frame_size(b"too small").is_err());
    }

    #[test]
    fn decode_hid_mask_uses_big_endian_by_default() {
        assert_eq!(
            decode_hid_mask(&[0x01, 0x00], 0, ByteOrder::Big).unwrap(),
            0x0100
        );
    }

    #[test]
    fn decode_hid_mask_can_use_offset_and_little_endian_for_debugging() {
        assert_eq!(
            decode_hid_mask(&[0xaa, 0x00, 0x20], 1, ByteOrder::Little).unwrap(),
            0x2000
        );
    }

    #[test]
    fn decode_hid_mask_rejects_short_reports() {
        assert!(decode_hid_mask(&[0x01], 0, ByteOrder::Big).is_err());
    }

    #[test]
    fn active_controls_maps_known_bits() {
        let controls = active_controls(0x0101 | 0x8000);
        let names = controls
            .iter()
            .map(|control| control.name)
            .collect::<Vec<_>>();

        assert_eq!(names, ["UP", "S1", "LEFT_CLOCKWISE"]);
    }

    #[test]
    fn changed_events_reports_press_and_release_edges() {
        let events = changed_events(0x0101, 0x0201);
        let actual = events
            .iter()
            .map(|event| (event.control.name, event.pressed))
            .collect::<Vec<_>>();

        assert_eq!(actual, [("S1", false), ("S2", true)]);
    }

    #[test]
    fn unknown_hid_bits_returns_unmapped_bits_only() {
        assert_eq!(unknown_hid_bits(0x00f0 | 0x0100), 0x00f0);
    }
}
