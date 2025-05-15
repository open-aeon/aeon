use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub offset: u64,
}

impl Message {
    pub fn new(key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            key,
            value,
            timestamp,
            offset: 0, // 将在写入时设置
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
} 