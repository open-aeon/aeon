mod producer;
mod consumer;
mod builder;
mod error;

use std::time::Duration;
use bifrost::protocol::{Request, Response, codec::ClientCodec};
use bifrost::protocol::message::ProtocolMessage as Message;

/// 生产者 trait
#[async_trait::async_trait]
pub trait Producer: Send + Sync {
    /// 发送消息
    async fn send(&self, message: Message) -> Result<i64>;
    
    /// 批量发送消息
    async fn send_batch(&self, messages: Vec<Message>) -> Result<Vec<i64>>;
    
    /// 等待消息确认
    async fn wait_for_ack(&self, message_id: &str) -> Result<()>;
    
    /// 批量等待消息确认
    async fn wait_for_ack_batch(&self, message_ids: &[String]) -> Result<()>;
    
    /// 关闭生产者
    async fn close(&self) -> Result<()>;
}

/// 消费者 trait
#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    /// 消费消息
    async fn consume(&self, topic: &str, partition: i32, offset: i64) -> Result<Vec<Message>>;
    
    /// 提交偏移量
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<()>;
    
    /// 确认消息
    async fn ack(&self, message_id: &str) -> Result<()>;
    
    /// 批量确认消息
    async fn ack_batch(&self, message_ids: &[String]) -> Result<()>;
    
    /// 拒绝消息
    async fn reject(&self, message_id: &str) -> Result<()>;
    
    /// 批量拒绝消息
    async fn reject_batch(&self, message_ids: &[String]) -> Result<()>;
    
    /// 重试消息
    async fn retry(&self, message_id: &str) -> Result<()>;
    
    /// 批量重试消息
    async fn retry_batch(&self, message_ids: &[String]) -> Result<()>;
    
    /// 关闭消费者
    async fn close(&self) -> Result<()>;
}

/// 客户端配置
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// 服务器地址
    pub server_addr: String,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 请求超时时间
    pub request_timeout: Duration,
    /// 重试次数
    pub retry_count: u32,
    /// 重试间隔
    pub retry_interval: Duration,
    /// 生产者 ACK 级别
    pub producer_ack_level: ProducerAckLevel,
    /// 生产者 ACK 超时时间
    pub producer_ack_timeout: Duration,
    /// 消费者 ACK 类型
    pub consumer_ack_type: ConsumerAckType,
    /// 消费者自动确认间隔
    pub consumer_auto_commit_interval: Duration,
    /// 消费者最大重试次数
    pub consumer_max_retries: u32,
    /// 消费者重试间隔
    pub consumer_retry_interval: Duration,
}

/// 生产者 ACK 级别
#[derive(Debug, Clone, Copy)]
pub enum ProducerAckLevel {
    /// 不等待确认
    None,
    /// 等待 leader 确认
    Leader,
    /// 等待所有副本确认
    AllReplicas,
}

/// 消费者 ACK 类型
#[derive(Debug, Clone, Copy)]
pub enum ConsumerAckType {
    /// 自动确认
    Auto,
    /// 手动确认
    Manual,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:9092".to_string(),
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            retry_count: 3,
            retry_interval: Duration::from_secs(1),
            producer_ack_level: ProducerAckLevel::Leader,
            producer_ack_timeout: Duration::from_secs(5),
            consumer_ack_type: ConsumerAckType::Auto,
            consumer_auto_commit_interval: Duration::from_secs(5),
            consumer_max_retries: 3,
            consumer_retry_interval: Duration::from_secs(1),
        }
    }
}

// 重新导出公共类型
pub use producer::ProducerImpl;
pub use consumer::ConsumerImpl;
pub use builder::ClientBuilder;
pub use error::{ClientError, Result};
pub use bifrost::protocol::{Request, Response, codec::ClientCodec};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_builder() {
        let builder = ClientBuilder::new()
            .server_addr("127.0.0.1:9092")
            .connection_timeout(Duration::from_secs(5))
            .request_timeout(Duration::from_secs(30))
            .retry_count(3)
            .retry_interval(Duration::from_secs(1));

        // TODO: 添加更多测试
    }
}
