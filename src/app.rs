use anyhow::Result;

use crate::cli::Args;
use crate::manager::SaitekRemoteManager;

pub async fn run(args: Args) -> Result<()> {
    let manager = SaitekRemoteManager::new(args.nats_url, args.manager_id)?
        .with_frame_timeout(std::time::Duration::from_secs(args.frame_timeout_seconds));
    manager.run().await
}
