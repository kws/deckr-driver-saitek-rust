use anyhow::Result;

use crate::cli::Args;
use crate::manager::SaitekRemoteManager;

pub async fn run(args: Args) -> Result<()> {
    let manager = SaitekRemoteManager::new(args.nats_url, args.manager_id)?;
    manager.run().await
}
