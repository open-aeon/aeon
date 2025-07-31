use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 主题分区信息
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: u32,
    pub offset: i64,
}

/// 主题元数据信息
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: HashMap<u32,PartitionMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub leader: u32,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
    pub leader_epoch: i32,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct OffsetCommitMetadata {
    pub group_id: String,
    pub topic: String,
    pub partition: u32,
    pub offset: i64,
    pub timestamp: u64,
}