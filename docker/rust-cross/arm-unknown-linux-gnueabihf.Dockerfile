FROM ghcr.io/cross-rs/arm-unknown-linux-gnueabihf:main

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    ca-certificates \
    curl \
    libtool \
    make \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG LIBUSB_VERSION=1.0.27

RUN set -eux; \
    cd /tmp; \
    curl -fsSL "https://github.com/libusb/libusb/releases/download/v${LIBUSB_VERSION}/libusb-${LIBUSB_VERSION}.tar.bz2" -o libusb.tar.bz2; \
    tar -xjf libusb.tar.bz2; \
    cd "libusb-${LIBUSB_VERSION}"; \
    export CC=arm-unknown-linux-gnueabihf-gcc; \
    export CXX=arm-unknown-linux-gnueabihf-g++; \
    export AR=arm-unknown-linux-gnueabihf-ar; \
    export RANLIB=arm-unknown-linux-gnueabihf-ranlib; \
    ./configure \
      --host=arm-unknown-linux-gnueabihf \
      --prefix=/opt/arm-linux-gnueabihf \
      --disable-udev; \
    make -j"$(nproc)"; \
    make install; \
    rm -rf /tmp/libusb.tar.bz2 "/tmp/libusb-${LIBUSB_VERSION}"

ENV PKG_CONFIG_LIBDIR=/opt/arm-linux-gnueabihf/lib/pkgconfig
ENV LD_LIBRARY_PATH=/opt/arm-linux-gnueabihf/lib
