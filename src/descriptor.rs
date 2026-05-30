use deckr::lanes::{
    CapabilityConstraint, CapabilityDescriptor, CapabilitySchema, ControlDescriptor,
    ControlGeometry, DeviceDescriptor, DeviceRef, HardwareMessageBody,
};
use serde_json::json;

use crate::backend::DeviceCandidate;
use crate::protocol::{HidEvent, HEIGHT, WIDTH};

pub const SCREEN_CONTROL_ID: &str = "screen";
pub const RASTER_CAPABILITY_ID: &str = "raster.bitmap";
pub const BUTTON_MOMENTARY_CAPABILITY_ID: &str = "button.momentary";
pub const ENCODER_RELATIVE_CAPABILITY_ID: &str = "encoder.relative";
pub const DEFAULT_PAGE_ID: u32 = 1;

const BUTTON_CONTROLS: &[(&str, &str, f64, f64)] = &[
    ("up", "Up", 0.0, 0.0),
    ("down", "Down", 0.0, 1.0),
    ("s1", "S1", 1.0, 0.0),
    ("s2", "S2", 1.0, 1.0),
    ("s3", "S3", 1.0, 2.0),
    ("s4", "S4", 1.0, 3.0),
    ("s5", "S5", 1.0, 4.0),
    ("s6", "S6", 1.0, 5.0),
];

pub fn device_descriptor(
    candidate: &DeviceCandidate,
    device_id: &str,
    fingerprint: &str,
) -> DeviceDescriptor {
    DeviceDescriptor {
        device_id: device_id.to_string(),
        fingerprint: fingerprint.to_string(),
        display_name: candidate
            .product
            .clone()
            .unwrap_or_else(|| "Saitek Flight Instrument Panel".to_string()),
        manufacturer: candidate
            .manufacturer
            .clone()
            .or_else(|| Some("Logitech/Saitek".to_string())),
        model: Some("Flight Instrument Panel".to_string()),
        serial_number: candidate.serial_number.clone(),
        controls: control_descriptors(candidate.hid_interrupt_in.is_some()),
        capabilities: Vec::new(),
    }
}

pub fn control_descriptors(input_enabled: bool) -> Vec<ControlDescriptor> {
    let mut controls = vec![ControlDescriptor {
        control_id: SCREEN_CONTROL_ID.to_string(),
        kind: "screen".to_string(),
        label: Some("Screen".to_string()),
        geometry: Some(ControlGeometry {
            x: 0.0,
            y: 0.0,
            width: Some(4.0),
            height: Some(3.0),
            unit: "grid".to_string(),
        }),
        input_capabilities: Vec::new(),
        output_capabilities: vec![raster_output_capability(WIDTH as u32, HEIGHT as u32)],
    }];

    if input_enabled {
        controls.extend(
            BUTTON_CONTROLS
                .iter()
                .map(|(id, label, x, y)| ControlDescriptor {
                    control_id: (*id).to_string(),
                    kind: "button".to_string(),
                    label: Some((*label).to_string()),
                    geometry: Some(ControlGeometry {
                        x: *x,
                        y: *y,
                        width: Some(1.0),
                        height: Some(1.0),
                        unit: "grid".to_string(),
                    }),
                    input_capabilities: vec![button_momentary_capability()],
                    output_capabilities: Vec::new(),
                }),
        );

        controls.extend([
            ControlDescriptor {
                control_id: "left_encoder".to_string(),
                kind: "dial".to_string(),
                label: Some("Left encoder".to_string()),
                geometry: Some(ControlGeometry {
                    x: 2.0,
                    y: 0.0,
                    width: Some(1.0),
                    height: Some(1.0),
                    unit: "grid".to_string(),
                }),
                input_capabilities: vec![encoder_relative_capability()],
                output_capabilities: Vec::new(),
            },
            ControlDescriptor {
                control_id: "right_encoder".to_string(),
                kind: "dial".to_string(),
                label: Some("Right encoder".to_string()),
                geometry: Some(ControlGeometry {
                    x: 3.0,
                    y: 0.0,
                    width: Some(1.0),
                    height: Some(1.0),
                    unit: "grid".to_string(),
                }),
                input_capabilities: vec![encoder_relative_capability()],
                output_capabilities: Vec::new(),
            },
        ]);
    }

    controls
}

pub fn translate_hid_event(
    event: HidEvent,
    manager_id: &str,
    device_id: &str,
    fingerprint: &str,
) -> Option<HardwareMessageBody> {
    match event.control.name {
        "UP" => button_input(manager_id, device_id, fingerprint, "up", event.pressed),
        "DOWN" => button_input(manager_id, device_id, fingerprint, "down", event.pressed),
        "S1" => button_input(manager_id, device_id, fingerprint, "s1", event.pressed),
        "S2" => button_input(manager_id, device_id, fingerprint, "s2", event.pressed),
        "S3" => button_input(manager_id, device_id, fingerprint, "s3", event.pressed),
        "S4" => button_input(manager_id, device_id, fingerprint, "s4", event.pressed),
        "S5" => button_input(manager_id, device_id, fingerprint, "s5", event.pressed),
        "S6" => button_input(manager_id, device_id, fingerprint, "s6", event.pressed),
        "LEFT_CLOCKWISE" if event.pressed => encoder_input(
            manager_id,
            device_id,
            fingerprint,
            "left_encoder",
            1,
            "clockwise",
        ),
        "LEFT_ANTICLOCKWISE" if event.pressed => encoder_input(
            manager_id,
            device_id,
            fingerprint,
            "left_encoder",
            -1,
            "counterclockwise",
        ),
        "RIGHT_CLOCKWISE" if event.pressed => encoder_input(
            manager_id,
            device_id,
            fingerprint,
            "right_encoder",
            1,
            "clockwise",
        ),
        "RIGHT_ANTICLOCKWISE" if event.pressed => encoder_input(
            manager_id,
            device_id,
            fingerprint,
            "right_encoder",
            -1,
            "counterclockwise",
        ),
        _ => None,
    }
}

fn capability_schema(schema_id: &str, schema: serde_json::Value) -> CapabilitySchema {
    CapabilitySchema {
        schema_id: Some(schema_id.to_string()),
        schema,
    }
}

fn button_momentary_capability() -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: BUTTON_MOMENTARY_CAPABILITY_ID.to_string(),
        family: "dev.deckr.input.button".to_string(),
        capability_type: "momentary".to_string(),
        direction: "input".to_string(),
        access: vec!["emits".to_string()],
        value_schema: Some(capability_schema(
            "dev.deckr.value.input.button.momentary.v1",
            json!({
                "type": "object",
                "required": ["eventType"],
                "properties": {"eventType": {"enum": ["down", "up"]}},
                "additionalProperties": false
            }),
        )),
        command_schema: None,
        constraints: Vec::new(),
        event_types: vec!["down".to_string(), "up".to_string()],
        command_types: Vec::new(),
    }
}

fn encoder_relative_capability() -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: ENCODER_RELATIVE_CAPABILITY_ID.to_string(),
        family: "dev.deckr.input.encoder".to_string(),
        capability_type: "relative".to_string(),
        direction: "input".to_string(),
        access: vec!["emits".to_string()],
        value_schema: Some(capability_schema(
            "dev.deckr.value.input.encoder.relative.v1",
            json!({
                "type": "object",
                "required": ["delta"],
                "properties": {
                    "delta": {"type": "integer", "not": {"const": 0}},
                    "direction": {"enum": ["clockwise", "counterclockwise"]}
                },
                "additionalProperties": false
            }),
        )),
        command_schema: None,
        constraints: Vec::new(),
        event_types: vec!["rotate".to_string()],
        command_types: Vec::new(),
    }
}

fn raster_output_capability(width: u32, height: u32) -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: RASTER_CAPABILITY_ID.to_string(),
        family: "dev.deckr.output.raster".to_string(),
        capability_type: "bitmap".to_string(),
        direction: "output".to_string(),
        access: vec!["settable".to_string()],
        value_schema: None,
        command_schema: Some(capability_schema(
            "dev.deckr.command.output.raster.bitmap.v1",
            json!({
                "oneOf": [
                    {
                        "type": "object",
                        "required": ["image", "encoding"],
                        "properties": {
                            "image": {"type": "string", "contentEncoding": "base64"},
                            "encoding": {"enum": ["jpeg", "png"]},
                            "width": {"const": width},
                            "height": {"const": height}
                        },
                        "additionalProperties": false
                    },
                    {"type": "object", "maxProperties": 0}
                ]
            }),
        )),
        constraints: vec![
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "width".to_string(),
                value: Some(json!(width)),
                ..Default::default()
            },
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "height".to_string(),
                value: Some(json!(height)),
                ..Default::default()
            },
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "channel_order".to_string(),
                value: Some(json!("bgr")),
                ..Default::default()
            },
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "row_order".to_string(),
                value: Some(json!("bottom-up")),
                ..Default::default()
            },
        ],
        event_types: Vec::new(),
        command_types: vec!["set_frame".to_string(), "clear".to_string()],
    }
}

fn button_input(
    manager_id: &str,
    device_id: &str,
    fingerprint: &str,
    control_id: &str,
    pressed: bool,
) -> Option<HardwareMessageBody> {
    let event_type = if pressed { "down" } else { "up" };
    Some(HardwareMessageBody::ControlInput {
        device_ref: device_ref(manager_id, device_id, fingerprint),
        control_id: control_id.to_string(),
        capability_id: BUTTON_MOMENTARY_CAPABILITY_ID.to_string(),
        event_type: event_type.to_string(),
        value: Some(json!({"eventType": event_type})),
    })
}

fn encoder_input(
    manager_id: &str,
    device_id: &str,
    fingerprint: &str,
    control_id: &str,
    delta: i32,
    direction: &str,
) -> Option<HardwareMessageBody> {
    Some(HardwareMessageBody::ControlInput {
        device_ref: device_ref(manager_id, device_id, fingerprint),
        control_id: control_id.to_string(),
        capability_id: ENCODER_RELATIVE_CAPABILITY_ID.to_string(),
        event_type: "rotate".to_string(),
        value: Some(json!({"delta": delta, "direction": direction})),
    })
}

fn device_ref(manager_id: &str, device_id: &str, fingerprint: &str) -> DeviceRef {
    DeviceRef {
        manager_id: manager_id.to_string(),
        device_id: device_id.to_string(),
        fingerprint: Some(fingerprint.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::DeviceCandidate;
    use crate::protocol::{HidControl, HidEvent};

    use super::*;

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

    #[test]
    fn descriptor_exposes_screen_raster_and_inputs() {
        let descriptor = device_descriptor(&sample_candidate(), "fip", "fingerprint");

        let screen = descriptor
            .controls
            .iter()
            .find(|control| control.control_id == SCREEN_CONTROL_ID)
            .expect("screen control should exist");
        let raster = screen
            .output_capabilities
            .iter()
            .find(|capability| capability.capability_id == RASTER_CAPABILITY_ID)
            .expect("screen should expose raster output");

        assert_eq!(raster.command_types, ["set_frame", "clear"]);
        assert!(descriptor
            .controls
            .iter()
            .any(|control| control.control_id == "s1"));
        assert!(descriptor
            .controls
            .iter()
            .any(|control| control.control_id == "left_encoder"));
    }

    #[test]
    fn descriptor_exposes_display_only_when_hid_is_unavailable() {
        let descriptor = device_descriptor(
            &sample_candidate().without_hid_input(),
            "fip",
            "fingerprint",
        );

        assert_eq!(descriptor.controls.len(), 1);
        assert_eq!(descriptor.controls[0].control_id, SCREEN_CONTROL_ID);
        assert!(!descriptor
            .controls
            .iter()
            .any(|control| control.control_id == "s1"));
        assert!(!descriptor
            .controls
            .iter()
            .any(|control| control.control_id == "left_encoder"));
    }

    #[test]
    fn hid_button_event_translates_to_momentary_input() {
        let body = translate_hid_event(
            HidEvent {
                control: HidControl {
                    name: "S1",
                    mask: 0x0100,
                },
                pressed: true,
            },
            "manager",
            "fip",
            "fingerprint",
        )
        .expect("event should translate");

        match body {
            HardwareMessageBody::ControlInput {
                control_id,
                capability_id,
                event_type,
                ..
            } => {
                assert_eq!(control_id, "s1");
                assert_eq!(capability_id, BUTTON_MOMENTARY_CAPABILITY_ID);
                assert_eq!(event_type, "down");
            }
            other => panic!("expected control input, got {other:?}"),
        }
    }

    #[test]
    fn hid_encoder_release_is_ignored() {
        assert!(translate_hid_event(
            HidEvent {
                control: HidControl {
                    name: "LEFT_CLOCKWISE",
                    mask: 0x8000,
                },
                pressed: false,
            },
            "manager",
            "fip",
            "fingerprint",
        )
        .is_none());
    }
}
