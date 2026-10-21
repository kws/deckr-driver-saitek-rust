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
Hardware discovery candidates are advertised through Beacon. After a controller
claim is negotiated, device command/input routing is fenced only by valid
Concord hardware-claim contracts and participant tokens. Queued display commands
retain the core authorization context and recheck it before execution and after
image conversion, immediately before USB output. Commands whose claim, device
identity, session, or deadline is no longer valid are discarded without
disconnecting the device. LED commands use the same authorization context.
Startup clears all lamps; local reset and stop clear the lamps and screen.

SIGINT and SIGTERM stop the manager during startup, normal operation, or retry
backoff. Shutdown revokes Concord authority and waits for every USB worker to
finish its reset and release its interfaces. A remote cleanup failure is reported
on exit and never starts another session. Enumeration failures also stop and join
existing workers before retrying.

After an unclean exit, the core can hold startup until a previous session's
participant token expires. Recovery negotiates a successor contract; it never
reattaches to the old agreement.

USB output runs in batches of at most four commands before checking HID input,
so a continuous stream of frames cannot starve buttons and encoders. Each USB
command has one five-second deadline covering its header, payload, and reply.
Worker failures retain the underlying USB error in the log. A failed interface
claim restores any kernel driver detached during that attempt.

### Button lamps

Controls `s1`–`s6`, `up`, and `down` expose the manager-owned extension capability
`led.binary`, family `io.github.kws.saitek.output.led`, type `binary`, direction
`output`, access `settable`. Command `set` takes exactly `{"on": true}` or
`{"on": false}`; its inline schema is labelled
`io.github.kws.saitek.command.output.led.binary.v1`. No colour or brightness
control is available. Lamp output remains available when HID input cannot be
claimed. Controllers match the family/type and target the advertised capability ID.

The USB command is `0x18`, with lamp indices 1–6 for S1–S6, 7 for Up and 8 for
Down, and state 0/1. Word 1 carries the running flag (1), word 6 the gauge ID,
word 7 the lamp index, and word 8 the state. This layout and numbering are
cross-checked against the independent
[FIP implementation](https://github.com/JasonCarlyle/saitek-fip-macos/blob/main/fipx/device.py).
Pending outputs coalesce per control, so an old blink sequence cannot build up
behind a slow display transfer. Reset takes priority and drops pending outputs;
dequeued commands still recheck their original Concord authority.

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
- `DECKR_STATE_RECONCILE_SECONDS` (optional; defaults to `300`)
- `DECKR_FRAME_TIMEOUT_SECONDS` (optional; defaults to `30`)

### Frame freshness

`--frame-timeout-seconds` / `DECKR_FRAME_TIMEOUT_SECONDS` sets a positive whole
number of seconds (default `30`) without a successfully authorized and displayed
frame before the manager replaces the screen with **STREAM LOST / WAITING FOR
FRAMES**. The timeout also applies while waiting for the first frame after USB
startup. The notice is drawn once; the next valid frame automatically replaces it.
Lamp commands, rejected frames and claim resets do not extend the deadline.
An explicit authorized raster `clear` intentionally blanks the screen and
disarms this notice until another valid frame is displayed. Shutdown still clears
the device and exits normally.

This is a local display diagnostic, not lifecycle or availability authority:
Concord ownership, cancellation and participant-token rules remain unchanged.
Controllers should send a cached frame heartbeat more often than this timeout;
ATC defaults to `frame_heartbeat_seconds = 15.0`. A heartbeat does not advance the
cached image's timestamp. Configure either value in the service's arguments or
environment / controller TOML, then restart that service; no rebuild is needed.

Concord participant-token renewal timing is managed by the Rust core.

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

This crate is built and tested with Rust 1.88. Check out the Rust core alongside
this repository so its path dependency resolves to `../deckr-core-rust`:

```sh
git clone https://github.com/kws/deckr-core-rust.git ../deckr-core-rust
```

```sh
cargo build
cargo test
```

If you use `just`, the same commands are available as:

```sh
just build
just test
```

## Cross-platform builds

The repo includes a `cross` setup for supported Linux deployment targets:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `armv7-unknown-linux-gnueabihf`
- `arm-unknown-linux-gnueabihf`

The `aarch64-unknown-linux-gnu` target covers 64-bit Raspberry Pi OS on newer
boards, `armv7-unknown-linux-gnueabihf` covers 32-bit ARMv7 boards, and
`arm-unknown-linux-gnueabihf` keeps an ARMv6/Raspberry Pi 1/Zero-compatible
build.

Build the custom `cross` images first:

```sh
just cross-images
```

Then build release binaries for the supported Linux targets:

```sh
just release
```

### OpenWrt on Raspberry Pi 1

The Raspberry Pi 1 OpenWrt build targets the tested device profile:

```text
OpenWrt: 24.10.4
Target:  bcm27xx/bcm2708
Arch:    arm_arm1176jzf-s_vfp
SDK:     openwrt-sdk-24.10.4-bcm27xx-bcm2708_gcc-13.3.0_musl_eabi.Linux-x86_64.tar.zst
Rust:    arm-unknown-linux-musleabihf
```

Build the OpenWrt package and a raw binary tarball with:

```sh
just openwrt-pi1
```

On macOS this command runs the Linux OpenWrt SDK inside Docker. On Linux x86_64
it runs the SDK directly. The command writes:

- `dist/deckr-saitek-manager_<version>-1_arm_arm1176jzf-s_vfp.ipk`
- `dist/deckr-saitek-manager-openwrt-24.10.4-bcm2708-pi1.tar.gz`
- `dist/deckr-saitek-manager-openwrt-24.10.4-bcm2708-pi1.*.txt`

Install the package on the Pi:

```sh
opkg install ./deckr-saitek-manager_<version>-1_arm_arm1176jzf-s_vfp.ipk
```

For a quick raw-binary test instead:

```sh
tar -xzf deckr-saitek-manager-openwrt-24.10.4-bcm2708-pi1.tar.gz
./deckr-saitek-manager-openwrt-24.10.4-bcm2708-pi1/deckr-saitek-manager --help
```

The build stages a static `libusb` archive with the OpenWrt toolchain and fails
if the produced binary references glibc, such as `/lib/ld-linux-armhf.so.3` or
`libc.so.6`. The expected Pi 1 artifact is a statically linked ARM EABI5 binary.
The raw binary also runs on OpenWrt 25.12.4; that release uses `apk`, so the
`opkg` package command above applies to OpenWrt 24.10. For a procd service, set
`procd_set_param term_timeout 60` so USB reset and Concord cleanup can finish
before the supervisor forces termination.

## GitHub Actions

The build workflow runs formatting, clippy, tests, and release builds for the
Linux Intel, Raspberry Pi, macOS, and OpenWrt Raspberry Pi 1 targets. Pushing a
tag that starts with `v`, such as `v0.1.0`, packages the binaries and creates
or updates the matching GitHub Release.

CI checks out `kws/deckr-core-rust` at `.deckr-core-revision` for every build.
Private repository access uses `DECKR_REPO_TOKEN` with read access to that core.

Ordinary alpha development updates Git commits and dependency pins without
changing package versions or creating release tags. Releases require a separate
explicit release request.

## Shared core dependency

Build with the sibling `deckr-core-rust` checkout at the exact Git commit in
`.deckr-core-revision`. CI uses that same commit for every target. After cloning
`kws/deckr-core-rust` alongside this repository, select it with:

```bash
git -C ../deckr-core-rust checkout --detach "$(cat .deckr-core-revision)"
cargo test --locked
```

The core owns Beacon/Concord lifetimes, descriptor validation, standard capability
schemas, and command authorization. Queued output retains `HardwareCommandContext`
and checks its authority before hardware writes. Hardware-specific protocol
encoding, discovery, and safe reset behavior remain in this repository.
