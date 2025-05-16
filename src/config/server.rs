use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    #[serde(with = "humantime_serde")]
    pub connection_idle_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9092,
            max_connections: 1000,
            connection_idle_timeout: Duration::from_secs(300),
        }
    }
} 