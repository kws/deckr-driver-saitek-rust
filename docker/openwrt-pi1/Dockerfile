FROM rust:1.88-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    bzip2 \
    build-essential \
    ca-certificates \
    curl \
    file \
    gawk \
    gettext \
    git \
    libncurses-dev \
    libssl-dev \
    libtool \
    make \
    perl \
    pkg-config \
    python3 \
    rsync \
    tar \
    unzip \
    wget \
    xz-utils \
    zstd \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add arm-unknown-linux-musleabihf
