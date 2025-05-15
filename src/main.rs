mod broker;
mod common;
mod protocol;
mod server;
mod storage;

use anyhow::Result;
use broker::{
    partition::PartitionManager,
    consumer_group::ConsumerGroupManager,
};
use server::Server;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir)?;

    let partition_manager = PartitionManager::new(data_dir.clone())?;
    let consumer_group_manager = ConsumerGroupManager::new(data_dir.join("consumer_groups"))?;
    consumer_group_manager.load_metadata().await?;

    let server = Server::new(partition_manager, consumer_group_manager);

    println!("Starting Bifrost server on 127.0.0.1:9092");
    server.start("127.0.0.1:9092").await?;

    Ok(())
} 