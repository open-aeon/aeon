use anyhow::Result;
use aeon::broker::Broker;
use aeon::config::Config;
use aeon::server::Server;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. load config
    let config = Config::load_from_file("config/default.yaml")?;

    // 2. initialize log
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log.level))
        .init();

    // 3. create and start Broker
    // Broker's start method now will be responsible for creating the required data directories
    let broker = Broker::new(config.broker, config.storage);
    broker.start().await?;
    let broker = Arc::new(broker);

    let addr = format!("{}:{}", config.server.host, config.server.port);
    // 4. create Server
    let server = Server::new(broker.clone(), config.server);

    // 5. start Server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    if let Err(e) = server.start(listener).await {
        eprintln!("Server exited with an error: {}", e);
    }

    Ok(())
} 