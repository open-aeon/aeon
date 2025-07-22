use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug,Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,

    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: Option<u64>,

    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_ms: Option<u64>,
}

fn default_flush_interval() -> Option<u64> {
    Some(1000)
}

fn default_cleanup_interval() -> Option<u64> {
    Some(300000)
}