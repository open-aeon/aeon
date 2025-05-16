use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 生产者 ACK 级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProducerAckLevel {
    /// 不等待确认
    None,
    /// 等待 leader 确认
    Leader,
    /// 等待所有副本确认
    AllReplicas,
}

/// 消费者 ACK 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerAckType {
    /// 自动确认
    Auto,
    /// 手动确认
    Manual,
}

/// ACK 动作
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckAction {
    /// 确认消息
    Commit,
    /// 拒绝消息
    Reject,
    /// 重试消息
    Retry,
}

/// 消息状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageStatus {
    /// 等待确认
    Pending,
    /// 已确认
    Committed,
    /// 已拒绝
    Rejected,
    /// 重试中
    Retrying,
    /// 死信
    DeadLetter,
}

/// 生产者 ACK 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerAckConfig {
    /// ACK 级别
    pub level: ProducerAckLevel,
    /// 超时时间
    pub timeout: Duration,
    /// 重试次数
    pub retry_count: u32,
    /// 批量大小
    pub batch_size: usize,
}

impl Default for ProducerAckConfig {
    fn default() -> Self {
        Self {
            level: ProducerAckLevel::Leader,
            timeout: Duration::from_secs(5),
            retry_count: 3,
            batch_size: 1000,
        }
    }
}

/// 消费者 ACK 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerAckConfig {
    /// ACK 类型
    pub ack_type: ConsumerAckType,
    /// 自动提交间隔
    pub auto_commit_interval: Duration,
    /// 最大重试次数
    pub max_retries: u32,
    /// 重试间隔
    pub retry_interval: Duration,
}

impl Default for ConsumerAckConfig {
    fn default() -> Self {
        Self {
            ack_type: ConsumerAckType::Auto,
            auto_commit_interval: Duration::from_secs(5),
            max_retries: 3,
            retry_interval: Duration::from_secs(1),
        }
    }
} 