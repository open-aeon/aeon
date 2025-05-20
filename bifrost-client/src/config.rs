use std::time::Duration;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerConfig {
    pub bootstrap_servers: Vec<String>,
    pub client_id: String,
    pub retry_config: ProducerRetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    pub bootstrap_servers: Vec<String>,
    pub group_id: String,
    pub client_id: String,
    pub retry_config: ConsumerRetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerRetryConfig {
    pub max_retries: u32,
    pub initial_retry_interval: Duration,
    pub max_retry_interval: Duration,
    pub retry_backoff_factor: f64,
    pub enable_exponential_backoff: bool,
}

impl Default for ProducerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_backoff_factor: 2.0,
            enable_exponential_backoff: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRetryConfig {
    pub max_retries: u32,
    pub retry_interval: Duration,
    pub dead_letter_topic: Option<String>,
    pub retry_topic_prefix: String,
    pub enable_delayed_retry: bool,
}

impl Default for ConsumerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_interval: Duration::from_secs(5),
            dead_letter_topic: Some("dead-letter".to_string()),
            retry_topic_prefix: "retry-".to_string(),
            enable_delayed_retry: true,
        }
    }
}

/// 重试队列策略
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryQueuePolicy {
    /// 参考TCP流量控制，动态调整消费速率
    FlowControl {
        /// 开始降低消费速率的队列容量阈值（百分比）
        slow_down_threshold: f64,
        /// 停止消费的队列容量阈值（百分比）
        stop_threshold: f64,
    },
    /// 将最早的消息移动到死信队列
    MoveToDeadLetter,
    /// 返回错误
    ReturnError,
    /// 直接忽视
    Ignore,
}

impl Default for RetryQueuePolicy {
    fn default() -> Self {
        Self::MoveToDeadLetter
    }
}

/// 重试配置
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 重试队列最大大小
    pub retry_queue_capacity: usize,
    /// 重试队列策略
    pub retry_queue_policy: RetryQueuePolicy,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retry_queue_capacity: 1000,
            retry_queue_policy: RetryQueuePolicy::MoveToDeadLetter,
        }
    }
} 