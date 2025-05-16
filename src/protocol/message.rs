use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMessage {
    pub id: String,
    pub topic: String,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub offset: i64,
}

impl ProtocolMessage {
    /// 从客户端消息创建协议消息
    pub fn from_client(topic: String, partition: i32, key: Option<Vec<u8>>, value: Vec<u8>, timestamp: i64) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            topic,
            partition,
            key,
            value,
            timestamp,
            offset: 0, // 初始偏移量，由服务器分配
        }
    }

    /// 更新消息偏移量
    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// 获取主题分区
    pub fn topic_partition(&self) -> TopicPartition {
        TopicPartition {
            topic: self.topic.clone(),
            partition: self.partition,
        }
    }
} 