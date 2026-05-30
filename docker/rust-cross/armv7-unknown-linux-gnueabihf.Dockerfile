FROM ghcr.io/cross-rs/armv7-unknown-linux-gnueabihf:main

RUN dpkg --add-architecture armhf

RUN apt-get update && apt-get install -y --no-install-recommends \
    libusb-1.0-0-dev:armhf \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV PKG_CONFIG_LIBDIR=/usr/lib/arm-linux-gnueabihf/pkgconfig
