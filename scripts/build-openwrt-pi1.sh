#!/usr/bin/env bash
set -euo pipefail

if [[ -d /usr/local/cargo/bin ]]; then
  export PATH="/usr/local/cargo/bin:${PATH}"
fi

OPENWRT_VERSION="${OPENWRT_VERSION:-24.10.4}"
OPENWRT_TARGET="${OPENWRT_TARGET:-bcm27xx}"
OPENWRT_SUBTARGET="${OPENWRT_SUBTARGET:-bcm2708}"
OPENWRT_ARCH="${OPENWRT_ARCH:-arm_arm1176jzf-s_vfp}"
OPENWRT_STAGING_ARCH="${OPENWRT_STAGING_ARCH:-${OPENWRT_ARCH/_vfp/+vfp}}"
OPENWRT_GCC_VERSION="${OPENWRT_GCC_VERSION:-13.3.0}"
OPENWRT_LIBC_ABI="${OPENWRT_LIBC_ABI:-musl_eabi}"
OPENWRT_SDK_SHA256="${OPENWRT_SDK_SHA256:-db19c2bee3c62a3f1b820c5ee04afc728b553734317add7dcfdbe158a34a2c96}"
OPENWRT_MAKE_VERBOSE="${OPENWRT_MAKE_VERBOSE:-s}"
LIBUSB_VERSION="${LIBUSB_VERSION:-1.0.27}"
LIBUSB_SHA256="${LIBUSB_SHA256:-ffaa41d741a8a3bee244ac8e54a72ea05bf2879663c098c82fc5757853441575}"

RUST_TARGET="${RUST_TARGET:-arm-unknown-linux-musleabihf}"
BIN_NAME="${BIN_NAME:-deckr-saitek-manager}"
PACKAGE_RELEASE="${PACKAGE_RELEASE:-1}"
PACKAGE_DEPENDS="${PACKAGE_DEPENDS:-libc, libgcc}"
ARCHIVE_NAME="${ARCHIVE_NAME:-deckr-saitek-manager-openwrt-${OPENWRT_VERSION}-${OPENWRT_SUBTARGET}-pi1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORK_ROOT="${OPENWRT_WORK_ROOT:-${REPO_ROOT}/target/openwrt/pi1}"
DOWNLOAD_DIR="${OPENWRT_DOWNLOAD_DIR:-${WORK_ROOT}/downloads}"
SDK_DIR="${WORK_ROOT}/sdk"
DIST_DIR="${REPO_ROOT}/dist"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}"

SDK_ARCHIVE="openwrt-sdk-${OPENWRT_VERSION}-${OPENWRT_TARGET}-${OPENWRT_SUBTARGET}_gcc-${OPENWRT_GCC_VERSION}_${OPENWRT_LIBC_ABI}.Linux-x86_64.tar.zst"
SDK_URL="https://downloads.openwrt.org/releases/${OPENWRT_VERSION}/targets/${OPENWRT_TARGET}/${OPENWRT_SUBTARGET}/${SDK_ARCHIVE}"
LIBUSB_ARCHIVE="libusb-${LIBUSB_VERSION}.tar.bz2"
LIBUSB_URL="https://github.com/libusb/libusb/releases/download/v${LIBUSB_VERSION}/${LIBUSB_ARCHIVE}"

inside_container=0
if [[ "${1:-}" == "--inside-container" ]]; then
  inside_container=1
  shift
fi

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

have() {
  command -v "$1" >/dev/null 2>&1
}

run_in_docker() {
  have docker || die "OpenWrt SDK builds need Linux x86_64; install Docker or run this on Linux/CI"
  docker info >/dev/null 2>&1 || die "Docker is installed but the daemon is not running; start Docker or run this on Linux/CI"

  local image="deckr-saitek-openwrt-pi1-builder:24.10.4"
  local workspace_root
  local driver_dir
  workspace_root="$(dirname "${REPO_ROOT}")"
  driver_dir="$(basename "${REPO_ROOT}")"

  log "Building OpenWrt Pi 1 builder image"
  docker build -f "${REPO_ROOT}/docker/openwrt-pi1/Dockerfile" -t "${image}" "${REPO_ROOT}"

  log "Running OpenWrt build in Linux container"
  docker run --rm --platform linux/amd64 \
    --user "$(id -u):$(id -g)" \
    -e HOME=/tmp/openwrt-home \
    -e CARGO_HOME=/tmp/cargo \
    -e CARGO_TARGET_DIR="/workspace/${driver_dir}/target" \
    -e PATH="/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    -e OPENWRT_DOWNLOAD_DIR="/workspace/${driver_dir}/target/openwrt/pi1/downloads" \
    -e OPENWRT_WORK_ROOT=/tmp/openwrt-pi1 \
    -e OPENWRT_USE_DOCKER=0 \
    -v "${workspace_root}:/workspace" \
    -w "/workspace/${driver_dir}" \
    "${image}" \
    bash -lc 'mkdir -p "${HOME}" "${CARGO_HOME}"; exec ./scripts/build-openwrt-pi1.sh --inside-container "$@"' \
    bash "$@"
}

if [[ "${OPENWRT_USE_DOCKER:-auto}" != "0" && "${inside_container}" -eq 0 && "$(uname -s)" != "Linux" ]]; then
  run_in_docker "$@"
  exit 0
fi

[[ "$(uname -s)" == "Linux" ]] || die "OpenWrt SDK ${SDK_ARCHIVE} runs on Linux x86_64; rerun with Docker enabled"
[[ "$(uname -m)" == "x86_64" ]] || die "OpenWrt SDK ${SDK_ARCHIVE} requires x86_64 Linux"
[[ -f "${REPO_ROOT}/../deckr/rust/deckr/Cargo.toml" ]] || die "missing sibling Deckr checkout at ../deckr/rust/deckr"

jobs() {
  if have nproc; then
    nproc
  else
    printf '2\n'
  fi
}

download_sdk() {
  mkdir -p "${DOWNLOAD_DIR}" "${WORK_ROOT}" "${DIST_DIR}"

  local archive_path="${DOWNLOAD_DIR}/${SDK_ARCHIVE}"
  if [[ ! -f "${archive_path}" ]]; then
    log "Downloading ${SDK_ARCHIVE}"
    curl -fL "${SDK_URL}" -o "${archive_path}.tmp"
    mv "${archive_path}.tmp" "${archive_path}"
  fi

  log "Verifying OpenWrt SDK checksum"
  printf '%s  %s\n' "${OPENWRT_SDK_SHA256}" "${archive_path}" | sha256sum -c -

  if [[ -x "${SDK_DIR}/scripts/feeds" && -f "${SDK_DIR}/.deckr-sdk-${OPENWRT_VERSION}" ]]; then
    return
  fi

  log "Extracting OpenWrt SDK"
  rm -rf "${SDK_DIR}" "${WORK_ROOT}/extract"
  mkdir -p "${WORK_ROOT}/extract"
  tar -C "${WORK_ROOT}/extract" --zstd -xf "${archive_path}"

  local extracted
  extracted="$(find "${WORK_ROOT}/extract" -maxdepth 1 -type d -name 'openwrt-sdk-*' -print -quit)"
  [[ -n "${extracted}" ]] || die "OpenWrt SDK archive did not contain an openwrt-sdk directory"
  mv "${extracted}" "${SDK_DIR}"
  rm -rf "${WORK_ROOT}/extract"
  touch "${SDK_DIR}/.deckr-sdk-${OPENWRT_VERSION}"
}

target_staging_dir() {
  find "${SDK_DIR}/staging_dir" -maxdepth 1 -type d -name "target-${OPENWRT_STAGING_ARCH}_${OPENWRT_LIBC_ABI}" -print -quit
}

toolchain_bin_dir() {
  find "${SDK_DIR}/staging_dir" -maxdepth 2 -type d -path '*/toolchain-*/bin' -print -quit
}

ensure_libusb_staged() {
  local target_staging
  target_staging="$(target_staging_dir)"
  [[ -n "${target_staging}" ]] || die "could not find OpenWrt target staging dir"

  if [[ -f "${target_staging}/usr/lib/pkgconfig/libusb-1.0.pc" && -f "${target_staging}/usr/lib/libusb-1.0.a" ]]; then
    return
  fi

  local archive_path="${DOWNLOAD_DIR}/${LIBUSB_ARCHIVE}"
  if [[ ! -f "${archive_path}" ]]; then
    log "Downloading ${LIBUSB_ARCHIVE}"
    curl -fL "${LIBUSB_URL}" -o "${archive_path}.tmp"
    mv "${archive_path}.tmp" "${archive_path}"
  fi

  log "Verifying libusb checksum"
  printf '%s  %s\n' "${LIBUSB_SHA256}" "${archive_path}" | sha256sum -c -

  local prefix host src_parent src_dir
  prefix="$(tool_prefix)"
  host="$(basename "${prefix}")"
  src_parent="${WORK_ROOT}/libusb-src"
  src_dir="${src_parent}/libusb-${LIBUSB_VERSION}"

  log "Building libusb ${LIBUSB_VERSION} with the OpenWrt toolchain"
  rm -rf "${src_parent}"
  mkdir -p "${src_parent}"
  tar -C "${src_parent}" -xjf "${archive_path}"

  (
    cd "${src_dir}"
    export CC="${prefix}-gcc"
    export CXX="${prefix}-g++"
    export AR="${prefix}-ar"
    export RANLIB="${prefix}-ranlib"
    export STRIP="${prefix}-strip"
    export STAGING_DIR="${SDK_DIR}/staging_dir"
    export CFLAGS="-Os -pipe -march=armv6 -mfpu=vfp -mfloat-abi=hard"
    export LDFLAGS="--sysroot=${target_staging} -Wl,-rpath-link,${target_staging}/usr/lib"
    ./configure \
      --host="${host}" \
      --prefix=/usr \
      --disable-udev \
      --disable-shared \
      --enable-static
    make -j"$(jobs)" "V=${OPENWRT_MAKE_VERBOSE}"
    make DESTDIR="${target_staging}" install
  )

  [[ -f "${target_staging}/usr/lib/pkgconfig/libusb-1.0.pc" ]] || die "libusb-1.0.pc was not staged by the SDK build"
  [[ -f "${target_staging}/usr/lib/libusb-1.0.a" ]] || die "static libusb archive was not staged by the SDK build"
}

ensure_rust_target() {
  have rustup || die "rustup is required to install ${RUST_TARGET}"
  if ! rustup target list --installed | grep -Fxq "${RUST_TARGET}"; then
    log "Installing Rust target ${RUST_TARGET}"
    rustup target add "${RUST_TARGET}"
  fi
}

tool_prefix() {
  local toolchain_bin
  local cc
  toolchain_bin="$(toolchain_bin_dir)"
  [[ -n "${toolchain_bin}" ]] || die "could not find OpenWrt toolchain bin dir"
  cc="$(find "${toolchain_bin}" -maxdepth 1 -type f -name '*openwrt*-gcc' -print -quit)"
  [[ -n "${cc}" ]] || die "could not find OpenWrt target gcc"
  printf '%s\n' "${cc%-gcc}"
}

crate_version() {
  sed -n 's/^version = "\(.*\)"/\1/p' "${REPO_ROOT}/Cargo.toml" | head -n 1
}

build_binary() {
  local target_staging prefix rustflags
  target_staging="$(target_staging_dir)"
  prefix="$(tool_prefix)"

  log "Building ${BIN_NAME} for ${OPENWRT_VERSION} ${OPENWRT_TARGET}/${OPENWRT_SUBTARGET}"
  export CARGO_TARGET_DIR
  export STAGING_DIR="${SDK_DIR}/staging_dir"
  export CC_arm_unknown_linux_musleabihf="${prefix}-gcc"
  export AR_arm_unknown_linux_musleabihf="${prefix}-ar"
  export RANLIB_arm_unknown_linux_musleabihf="${prefix}-ranlib"
  export CARGO_TARGET_ARM_UNKNOWN_LINUX_MUSLEABIHF_LINKER="${prefix}-gcc"
  export CARGO_TARGET_ARM_UNKNOWN_LINUX_MUSLEABIHF_AR="${prefix}-ar"
  export CFLAGS_arm_unknown_linux_musleabihf="-march=armv6 -mfpu=vfp -mfloat-abi=hard"
  export PKG_CONFIG_ALLOW_CROSS=1
  export PKG_CONFIG_ALL_STATIC=1
  export PKG_CONFIG_SYSROOT_DIR="${target_staging}"
  export PKG_CONFIG_LIBDIR="${target_staging}/usr/lib/pkgconfig:${target_staging}/usr/share/pkgconfig"
  export PKG_CONFIG_PATH=
  export LIBUSB_STATIC=1

  rustflags="-C target-cpu=arm1176jzf-s -C link-arg=--sysroot=${target_staging} -C link-arg=-Wl,-rpath-link,${target_staging}/usr/lib"
  export RUSTFLAGS="${RUSTFLAGS:-} ${rustflags}"

  cargo build --release --target "${RUST_TARGET}"
}

verify_binary() {
  local binary="${CARGO_TARGET_DIR}/${RUST_TARGET}/release/${BIN_NAME}"
  local prefix readelf_path file_report interp_report dynamic_report
  prefix="$(tool_prefix)"
  readelf_path="${prefix}-readelf"

  [[ -x "${binary}" ]] || die "expected binary not found: ${binary}"

  file_report="${DIST_DIR}/${ARCHIVE_NAME}.file.txt"
  interp_report="${DIST_DIR}/${ARCHIVE_NAME}.interpreter.txt"
  dynamic_report="${DIST_DIR}/${ARCHIVE_NAME}.needed.txt"

  log "Inspecting OpenWrt binary"
  file "${binary}" | tee "${file_report}"
  "${readelf_path}" -l "${binary}" | tee "${interp_report}" >/dev/null
  "${readelf_path}" -d "${binary}" | tee "${dynamic_report}" >/dev/null || true

  if grep -Eq 'ld-linux|libc\.so\.6' "${interp_report}" "${dynamic_report}"; then
    die "binary appears to be glibc-linked, not OpenWrt musl-linked"
  fi

  if grep -q 'Requesting program interpreter' "${interp_report}" && ! grep -Eq '/lib/ld-musl-arm(hf)?\.so\.1' "${interp_report}"; then
    die "binary has an unexpected dynamic interpreter"
  fi
}

package_binary() {
  local version="${1}"
  local binary="${CARGO_TARGET_DIR}/${RUST_TARGET}/release/${BIN_NAME}"
  local package_root="${WORK_ROOT}/package"
  local data_dir="${package_root}/data"
  local control_dir="${package_root}/control"
  local package_file="${DIST_DIR}/${BIN_NAME}_${version}-${PACKAGE_RELEASE}_${OPENWRT_ARCH}.ipk"
  local raw_root="${WORK_ROOT}/raw"
  local raw_dir="${raw_root}/${ARCHIVE_NAME}"
  local installed_size

  log "Packaging OpenWrt .ipk"
  rm -rf "${package_root}" "${raw_root}"
  mkdir -p "${data_dir}/usr/bin" "${control_dir}" "${raw_dir}" "${DIST_DIR}"
  install -m 0755 "${binary}" "${data_dir}/usr/bin/${BIN_NAME}"
  installed_size="$(du -sk "${data_dir}" | awk '{print $1}')"

  cat >"${control_dir}/control" <<EOF
Package: ${BIN_NAME}
Version: ${version}-${PACKAGE_RELEASE}
Depends: ${PACKAGE_DEPENDS}
Source: deckr-driver-saitek-rust
Architecture: ${OPENWRT_ARCH}
Maintainer: Deckr
Section: utils
Priority: optional
Installed-Size: ${installed_size}
License: MIT
Description: Standalone Rust Saitek Flight Instrument Panel manager for Deckr.
EOF

  (
    cd "${control_dir}"
    tar --sort=name --owner=0 --group=0 --numeric-owner -czf "${package_root}/control.tar.gz" .
  )
  (
    cd "${data_dir}"
    tar --sort=name --owner=0 --group=0 --numeric-owner -czf "${package_root}/data.tar.gz" .
  )
  printf '2.0\n' >"${package_root}/debian-binary"
  rm -f "${package_file}"
  (
    cd "${package_root}"
    tar --owner=0 --group=0 --numeric-owner -czf "${package_file}" ./debian-binary ./data.tar.gz ./control.tar.gz
  )

  log "Packaging raw binary tarball"
  install -m 0755 "${binary}" "${raw_dir}/${BIN_NAME}"
  tar -czf "${DIST_DIR}/${ARCHIVE_NAME}.tar.gz" -C "${raw_root}" "${ARCHIVE_NAME}"

  log "Wrote ${package_file}"
  log "Wrote ${DIST_DIR}/${ARCHIVE_NAME}.tar.gz"
}

main() {
  local version
  version="$(crate_version)"
  [[ -n "${version}" ]] || die "could not read crate version from Cargo.toml"

  download_sdk
  ensure_libusb_staged
  ensure_rust_target
  build_binary
  verify_binary
  package_binary "${version}"
}

main "$@"
