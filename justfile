default:
    @just --list

build:
    cargo build

test:
    cargo test

fmt:
    cargo fmt

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

cross-images:
    docker build -f docker/rust-cross/x86_64-unknown-linux-gnu.Dockerfile -t deckr-saitek-cross:x86_64-unknown-linux-gnu .
    docker build -f docker/rust-cross/aarch64-unknown-linux-gnu.Dockerfile -t deckr-saitek-cross:aarch64-unknown-linux-gnu .
    docker build -f docker/rust-cross/armv7-unknown-linux-gnueabihf.Dockerfile -t deckr-saitek-cross:armv7-unknown-linux-gnueabihf .
    docker build -f docker/rust-cross/arm-unknown-linux-gnueabihf.Dockerfile -t deckr-saitek-cross:arm-unknown-linux-gnueabihf .

release:
    CROSS_CONFIG=Cross.toml cross build --release --target x86_64-unknown-linux-gnu
    CROSS_CONFIG=Cross.toml cross build --release --target aarch64-unknown-linux-gnu
    CROSS_CONFIG=Cross.toml cross build --release --target armv7-unknown-linux-gnueabihf
    CROSS_CONFIG=Cross.toml CFLAGS_arm_unknown_linux_gnueabihf="-march=armv6 -mfpu=vfp -mfloat-abi=hard" CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS="-C target-cpu=arm1176jzf-s" cross build --release --target arm-unknown-linux-gnueabihf

deploy:
    just cross-images
    just release
    @echo "Built to $PWD/target/arm-unknown-linux-gnueabihf/release/deckr-saitek-manager"
