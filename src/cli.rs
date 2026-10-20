use std::{env, fs};

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

const DEFAULT_MANAGER_PREFIX: &str = "saitek-rust";

#[derive(Debug, Clone, Parser)]
#[command(name = "deckr-saitek-manager")]
#[command(about = "Saitek Flight Instrument Panel hardware manager for Deckr over NATS")]
pub struct Args {
    #[arg(
        long,
        env = "DECKR_NATS_URL",
        default_value = "nats://127.0.0.1:4222",
        help = "NATS server URL"
    )]
    pub nats_url: String,
    #[arg(
        long,
        env = "DECKR_MANAGER_ID",
        default_value_t = default_manager_id(),
        help = "Deckr hardware manager id; defaults to saitek-rust-<hostname>"
    )]
    pub manager_id: String,
    #[arg(long, default_value = "info")]
    pub log_level: String,
    #[arg(
        long,
        env = "DECKR_FRAME_TIMEOUT_SECONDS",
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Show STREAM LOST after this many seconds without a usable frame"
    )]
    pub frame_timeout_seconds: u64,
}

impl Args {
    pub fn parse_and_init() -> Result<Self> {
        let mut args = Self::parse();
        if args.manager_id.trim().is_empty() {
            args.manager_id = default_manager_id();
        }
        let filter = EnvFilter::try_new(&args.log_level)
            .with_context(|| format!("invalid log filter {}", args.log_level))?;
        tracing_subscriber::fmt().with_env_filter(filter).init();
        Ok(args)
    }
}

pub fn default_manager_id() -> String {
    let hostname = runtime_hostname().unwrap_or_else(|| "local".to_string());
    default_manager_id_for_hostname(&hostname)
}

fn default_manager_id_for_hostname(hostname: &str) -> String {
    format!(
        "{DEFAULT_MANAGER_PREFIX}-{}",
        normalize_manager_id_part(hostname)
    )
}

fn runtime_hostname() -> Option<String> {
    env_hostname("HOSTNAME")
        .or_else(|| env_hostname("COMPUTERNAME"))
        .or_else(file_hostname)
}

fn env_hostname(name: &str) -> Option<String> {
    env::var(name).ok().and_then(non_empty)
}

fn file_hostname() -> Option<String> {
    fs::read_to_string("/etc/hostname").ok().and_then(non_empty)
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn normalize_manager_id_part(value: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            normalized.push(ch);
            last_was_dash = false;
        } else if !last_was_dash {
            normalized.push('-');
            last_was_dash = true;
        }
    }
    let normalized = normalized
        .trim_matches(|ch| matches!(ch, '-' | '.' | '_'))
        .to_string();
    if normalized.is_empty() {
        "local".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_timeout_is_configurable_and_positive() {
        assert_eq!(
            Args::try_parse_from(["manager"])
                .unwrap()
                .frame_timeout_seconds,
            30
        );
        assert_eq!(
            Args::try_parse_from(["manager", "--frame-timeout-seconds", "12"])
                .unwrap()
                .frame_timeout_seconds,
            12
        );
        assert!(Args::try_parse_from(["manager", "--frame-timeout-seconds", "0"]).is_err());
        assert!(Args::try_parse_from(["manager", "--frame-timeout-seconds", "-1"]).is_err());
    }

    #[test]
    fn default_manager_id_uses_rust_prefix_and_hostname() {
        assert_eq!(
            default_manager_id_for_hostname("deckr-box.local"),
            "saitek-rust-deckr-box.local"
        );
    }

    #[test]
    fn default_manager_id_normalizes_unfriendly_hostname() {
        assert_eq!(
            default_manager_id_for_hostname(" deckr box!! "),
            "saitek-rust-deckr-box"
        );
        assert_eq!(default_manager_id_for_hostname(":::"), "saitek-rust-local");
    }
}
