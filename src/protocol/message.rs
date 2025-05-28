use crate::common::metadata::TopicPartition;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 协议消息结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
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
    /// 重试次数
    pub retry_count: u32,
    /// 下次重试时间
    pub next_retry_time: Option<DateTime<Utc>>,
    /// 重试原因
    pub retry_reason: Option<String>,
    /// 消息偏移量
    pub offset: Option<i64>,
}

#[allow(dead_code)]
impl Message {
    /// 从客户端消息创建协议消息
    pub fn from_client(topic: String, partition: i32, content: Vec<u8>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            topic,
            partition,
            content,
            timestamp: Utc::now(),
            retry_count: 0,
            next_retry_time: None,
            retry_reason: None,
            offset: None,
        }
    }

    /// 更新消息偏移量
    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// 获取主题分区
    pub fn topic_partition(&self) -> TopicPartition {
        TopicPartition {
            topic: self.topic.clone(),
            partition: self.partition,
        }
    }

    /// 创建新消息
    pub fn new(id: String, topic: String, partition: i32, content: Vec<u8>) -> Self {
        Self {
            id,
            topic,
            partition,
            content,
            timestamp: Utc::now(),
            retry_count: 0,
            next_retry_time: None,
            retry_reason: None,
            offset: None,
        }
    }

    /// 更新重试信息
    pub fn update_retry_info(&mut self, reason: String, next_retry_time: DateTime<Utc>) {
        self.retry_count += 1;
        self.retry_reason = Some(reason);
        self.next_retry_time = Some(next_retry_time);
    }

    /// 重置重试信息
    pub fn reset_retry_info(&mut self) {
        self.retry_count = 0;
        self.retry_reason = None;
        self.next_retry_time = None;
    }
} 