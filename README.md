# deckr-driver-saitek-rust

Rust hardware manager for the Logitech/Saitek Flight Instrument Panel over Deckr's
NATS-backed hardware lane.

The target USB device is:

```text
VID: 0x06a3
PID: 0xa2ae
```

The display path uses the reverse-engineered FIP protocol from the Python proof of
concept:

- a vendor-specific USB interface with bulk OUT and IN endpoints
- 44-byte big-endian command packets
- `SetImage` command `0x06`
- raw `320x240x24bpp` framebuffer payloads
- bottom-up BGR framebuffer layout by default

Button and knob input comes from the FIP HID interrupt endpoint as a two-byte
big-endian bitmask.

## Runtime

The manager participates as `hardware_manager:<manager-id>` on the
`hardware_messages` lane. By default it uses `saitek-rust-<hostname>`.
Hardware discovery is advertised through Beacon, and device command/input routing
is fenced by valid Concord hardware-claim contracts and participant tokens.

```sh
deckr-saitek-manager \
  --nats-url nats://127.0.0.1:4222
```

Set `--manager-id` only when you want a stable deployment/location name:

```sh
deckr-saitek-manager \
  --manager-id sim-rig \
  --nats-url nats://127.0.0.1:4222
```

Environment variables:

- `DECKR_MANAGER_ID` (optional; overrides the `saitek-rust-<hostname>` default)
- `DECKR_NATS_URL`

## Linux USB Access

The manager needs permission to claim the Saitek FIP's vendor-specific display
interface and HID input interface. A development udev rule can grant access:

```text
SUBSYSTEM=="usb", ATTR{idVendor}=="06a3", ATTR{idProduct}=="a2ae", MODE="0666"
```

## macOS USB Access

macOS may keep the FIP's HID input interface attached to the system HID stack.
The manager treats HID input as optional: if libusb cannot claim that interface,
it continues as a display-only hardware manager. The vendor-specific display
interface is still required.

## Build

```sh
cargo build
cargo test
```

If you use `just`, the same commands are available as:

```sh
just build
just test
```
