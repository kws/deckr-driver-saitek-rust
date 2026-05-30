FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:main

RUN apt-get update && apt-get install -y --no-install-recommends \
    libusb-1.0-0-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
