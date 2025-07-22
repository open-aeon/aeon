use serde::{Deserialize, Serialize};

pub mod server;
pub mod storage;
pub mod log;
pub mod broker;

pub use server::ServerConfig;
pub use storage::StorageConfig;
pub use log::LogConfig;
pub use broker::BrokerConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
    pub broker: BrokerConfig,
}

impl Config {
    pub fn load_from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn get_server_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
} 