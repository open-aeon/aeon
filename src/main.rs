use anyhow::Result;
use aeon::broker::Broker;
use aeon::config::Config;
use aeon::server::Server;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 加载配置
    let config = Config::load_from_file("config/default.yaml")?;

    // 2. 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log.level))
        .init();

    // 3. 创建并启动 Broker
    // Broker 的 start 方法现在会负责创建所需的数据目录
    let broker = Broker::new(config.broker, config.storage);
    broker.start().await?;
    let broker = Arc::new(broker);

    let addr = format!("{}:{}", config.server.host, config.server.port);
    // 4. 创建 Server
    let server = Server::new(broker.clone(), config.server);

    // 5. 启动 Server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    if let Err(e) = server.start(listener).await {
        eprintln!("Server exited with an error: {}", e);
    }

    Ok(())
} 