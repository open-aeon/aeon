mod broker;
mod common;
mod protocol;
mod server;
mod storage;
mod config;

use anyhow::Result;
use broker::{
    partition::PartitionManager,
    consumer_group::ConsumerGroupManager,
};
use server::Server;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载配置
    let config = Config::load_from_file("config/default.yaml")?;

    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log.level))
        .init();

    // 创建数据目录
    std::fs::create_dir_all(&config.storage.data_dir)?;
    let consumer_groups_dir = config.storage.data_dir.join("consumer_groups");
    std::fs::create_dir_all(&consumer_groups_dir)?;

    // 初始化管理器
    let partition_manager = PartitionManager::new(config.storage.data_dir.clone())?;
    let consumer_group_manager = ConsumerGroupManager::new(consumer_groups_dir)?;
    consumer_group_manager.load_metadata().await?;

    // 创建并启动服务器
    let server = Server::new(
        partition_manager,
        consumer_group_manager,
        config.server.max_connections,
        config.server.connection_idle_timeout,
    );

    println!("Starting Bifrost server on {}", config.get_server_addr());
    server.start(&config.get_server_addr().to_string()).await?;

    Ok(())
} 