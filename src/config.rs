use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::net::SocketAddr;
use std::time::Duration;
use serde_yaml;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub connection_idle_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_size: usize,
    pub retention_hours: u64,
    pub max_segment_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: String,
    pub file: Option<PathBuf>,
    pub max_size: usize,
    pub max_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 9092,
                max_connections: 1000,
                connection_idle_timeout: Duration::from_secs(300),
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("data"),
                segment_size: 1024 * 1024, // 1MB
                retention_hours: 24 * 7,   // 7 days
                max_segment_size: 1024 * 1024 * 1024, // 1GB
            },
            log: LogConfig {
                level: "info".to_string(),
                file: None,
                max_size: 100 * 1024 * 1024, // 100MB
                max_files: 10,
            },
        }
    }
}

impl Config {
    pub fn load_from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn get_server_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.host, self.server.port)
            .parse()
            .expect("Invalid server address")
    }
} 