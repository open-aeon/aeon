use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OffsetCommitMetadata {
    pub group_id: String,
    pub topic: String,
    pub partition: u32,
    pub offset: i64,
    pub timestamp: u64,
}