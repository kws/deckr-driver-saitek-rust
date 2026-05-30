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
