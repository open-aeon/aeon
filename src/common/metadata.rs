use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// 主题分区信息
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// 主题元数据信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
}

/// 基础消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseMessage {
    /// 消息ID
    pub id: String,
    /// 主题
    pub topic: String,
    /// 分区
    pub partition: i32,
    /// 消息内容
    pub content: Vec<u8>,
    /// 消息时间戳
    pub timestamp: DateTime<Utc>,
} 