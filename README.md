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
Concord hardware-claim contracts and participant tokens.

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
- `DECKR_CONCORD_TOKEN_REFRESH_SECONDS` (optional; defaults to `15`)
- `DECKR_STATE_RECONCILE_SECONDS` (optional; defaults to `300`)

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

## GitHub Actions

The build workflow runs formatting, clippy, tests, and release builds for the
Linux Intel and Raspberry Pi targets. Pushing a tag that starts with `v`, such
as `v0.1.0`, packages the binaries and creates or updates the matching GitHub
Release.

The workflow checks out the sibling `kws/deckr` repository because this crate
uses the local Deckr Rust core path dependency. If the current branch or tag
exists in `kws/deckr`, the workflow uses it; otherwise it uses Deckr's default
branch. Set the repository variable `DECKR_REF` to force a specific Deckr branch,
tag, or 40-character commit SHA. For private repository access, add a
`DECKR_REPO_TOKEN` secret with read access to `kws/deckr`.

## Release Flow

Driver releases are made from a release commit that pins the workflow to the
exact Deckr commit used for the build. This keeps the release reproducible even
when the driver tag does not exist in the upstream `kws/deckr` repository.

1. Choose the Deckr commit SHA to release against.
2. Update `version` in `Cargo.toml` and `Cargo.lock`.
3. Replace every workflow `DECKR_REF` expression with the chosen 40-character
   Deckr SHA.
4. Run formatting, clippy, and tests.
5. Commit the change as `Release vX.Y.Z`.
6. Tag that commit as `vX.Y.Z` and push the branch and tag.
7. Wait for GitHub Actions to publish the GitHub Release artifacts.
8. Restore every workflow `DECKR_REF` value to
   `${{ vars.DECKR_REF || github.head_ref || github.ref_name }}`.
9. Commit the restore as `Restore unpinned Deckr workflow ref` and push it.
