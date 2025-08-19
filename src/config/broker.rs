use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug,Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub id: u32,
    pub advertised_host: String,
    pub advertised_port: u16,
    pub data_dir: PathBuf,
    pub raft_listen_addr: String, 
    pub raft_peers: Vec<String>,

    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: Option<u64>,

    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_ms: Option<u64>,

    #[serde(default = "default_internal_topic_name")]
    pub internal_topic_name: String,

    #[serde(default = "default_internal_topic_partitions")]
    pub internal_topic_partitions: u32,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: Option<u64>,
}

fn default_flush_interval() -> Option<u64> {
    Some(1000)
}

fn default_cleanup_interval() -> Option<u64> {
    Some(300000)
}

fn default_internal_topic_name() -> String {
    "__bifrost_offsets".to_string()
}

fn default_internal_topic_partitions() -> u32 {
    1
}

fn default_heartbeat_interval() -> Option<u64> {
    Some(3000)
}