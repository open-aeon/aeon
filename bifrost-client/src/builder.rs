use crate::{ClientConfig, Producer, Consumer, Result};
use crate::producer::ProducerImpl;
use crate::consumer::ConsumerImpl;
use std::time::Duration;

/// 客户端构建器
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    /// 创建新的构建器
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
        }
    }

    /// 设置服务器地址
    pub fn server_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.server_addr = addr.into();
        self
    }

    /// 设置连接超时时间
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// 设置请求超时时间
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// 设置重试次数
    pub fn retry_count(mut self, count: u32) -> Self {
        self.config.retry_count = count;
        self
    }

    /// 设置重试间隔
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.config.retry_interval = interval;
        self
    }

    /// 设置生产者 ACK 级别
    pub fn producer_ack_level(mut self, level: ProducerAckLevel) -> Self {
        self.config.producer_ack_level = level;
        self
    }

    /// 设置生产者 ACK 超时时间
    pub fn producer_ack_timeout(mut self, timeout: Duration) -> Self {
        self.config.producer_ack_timeout = timeout;
        self
    }

    /// 设置消费者 ACK 类型
    pub fn consumer_ack_type(mut self, ack_type: ConsumerAckType) -> Self {
        self.config.consumer_ack_type = ack_type;
        self
    }

    /// 设置消费者自动确认间隔
    pub fn consumer_auto_commit_interval(mut self, interval: Duration) -> Self {
        self.config.consumer_auto_commit_interval = interval;
        self
    }

    /// 设置消费者最大重试次数
    pub fn consumer_max_retries(mut self, max_retries: u32) -> Self {
        self.config.consumer_max_retries = max_retries;
        self
    }

    /// 设置消费者重试间隔
    pub fn consumer_retry_interval(mut self, interval: Duration) -> Self {
        self.config.consumer_retry_interval = interval;
        self
    }

    /// 构建生产者
    pub async fn build_producer(self) -> Result<Box<dyn Producer>> {
        let producer = ProducerImpl::new(self.config).await?;
        Ok(Box::new(producer))
    }

    /// 构建消费者
    pub async fn build_consumer(self, group_id: impl Into<String>) -> Result<Box<dyn Consumer>> {
        let consumer = ConsumerImpl::new(self.config, group_id.into()).await?;
        Ok(Box::new(consumer))
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
} 