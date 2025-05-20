use crate::{Producer, Result, ClientConfig, ProducerAckLevel};
use bifrost::protocol::{Request, Response, message::ProtocolMessage};
use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use std::time::Duration;
use std::collections::HashMap;
use tokio::time::timeout;
use anyhow::Result;
use chrono::Utc;
use tokio::time::sleep;

/// 消息状态
#[derive(Debug, Clone, Copy, PartialEq)]
enum MessageStatus {
    /// 等待确认
    Pending,
    /// 已确认
    Acknowledged,
    /// 确认失败
    Failed,
}

/// 消息状态跟踪器
struct MessageTracker {
    /// 消息状态映射
    statuses: HashMap<String, MessageStatus>,
    /// 消息确认通道
    ack_channels: HashMap<String, tokio::sync::oneshot::Sender<Result<()>>>,
}

impl MessageTracker {
    fn new() -> Self {
        Self {
            statuses: HashMap::new(),
            ack_channels: HashMap::new(),
        }
    }

    /// 添加消息到跟踪器
    fn add_message(&mut self, message_id: String) -> tokio::sync::oneshot::Receiver<Result<()>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.statuses.insert(message_id.clone(), MessageStatus::Pending);
        self.ack_channels.insert(message_id, tx);
        rx
    }

    /// 更新消息状态
    fn update_status(&mut self, message_id: &str, status: MessageStatus) -> Option<Result<()>> {
        self.statuses.insert(message_id.to_string(), status);
        if let Some(channel) = self.ack_channels.remove(message_id) {
            match status {
                MessageStatus::Acknowledged => Some(Ok(())),
                MessageStatus::Failed => Some(Err(crate::ClientError::Ack("消息确认失败".to_string()))),
                _ => None,
            }
        } else {
            None
        }
    }

    /// 获取消息状态
    fn get_status(&self, message_id: &str) -> Option<MessageStatus> {
        self.statuses.get(message_id).copied()
    }
}

/// 生产者实现
pub struct ProducerImpl {
    config: Arc<ClientConfig>,
    connection: Mutex<Option<Framed<TcpStream, bifrost::protocol::codec::ClientCodec>>>,
    message_tracker: Arc<RwLock<MessageTracker>>,
}

impl ProducerImpl {
    /// 创建新的生产者
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Ok(Self {
            config: Arc::new(config),
            connection: Mutex::new(None),
            message_tracker: Arc::new(RwLock::new(MessageTracker::new())),
        })
    }

    /// 确保连接已建立
    async fn ensure_connection(&self) -> Result<()> {
        let mut conn = self.connection.lock().await;
        if conn.is_none() {
            let stream = TcpStream::connect(&self.config.server_addr).await?;
            *conn = Some(Framed::new(stream, bifrost::protocol::codec::ClientCodec::default()));
        }
        Ok(())
    }

    /// 发送请求并等待响应
    async fn send_request(&self, request: Request) -> Result<Response> {
        self.ensure_connection().await?;
        
        let mut conn = self.connection.lock().await;
        let framed = conn.as_mut().unwrap();

        // 发送请求
        framed.send(request).await?;

        // 等待响应
        if let Some(response) = framed.next().await {
            Ok(response?)
        } else {
            Err(crate::ClientError::Connection("连接已关闭".to_string()))
        }
    }

    /// 处理消息确认
    async fn handle_ack(&self, message_id: &str, success: bool) {
        let mut tracker = self.message_tracker.write().await;
        let status = if success {
            MessageStatus::Acknowledged
        } else {
            MessageStatus::Failed
        };
        
        if let Some(result) = tracker.update_status(message_id, status) {
            // 通知等待的调用者
            if let Err(e) = result {
                eprintln!("消息确认失败: {}", e);
            }
        }
    }

    /// 尝试发送单条消息
    async fn try_send(&self, message: &ProtocolMessage) -> Result<ProduceResponse> {
        let request = Request::Produce(bifrost::protocol::ProduceRequest {
            topic: message.topic.clone(),
            partition: message.partition,
            messages: vec![message.clone()],
        });

        match self.send_request(request).await? {
            Response::Produce(response) => {
                if let Some(error) = response.error {
                    return Err(crate::ClientError::Server(error));
                }
                Ok(response)
            }
            _ => Err(crate::ClientError::Other("收到意外的响应类型".to_string())),
        }
    }

    /// 尝试批量发送消息
    async fn try_send_batch(&self, messages: &[ProtocolMessage]) -> Result<ProduceResponse> {
        let topic = messages[0].topic.clone();
        let partition = messages[0].partition;

        let request = Request::Produce(bifrost::protocol::ProduceRequest {
            topic,
            partition,
            messages: messages.to_vec(),
        });

        match self.send_request(request).await? {
            Response::Produce(response) => {
                if let Some(error) = response.error {
                    return Err(crate::ClientError::Server(error));
                }
                Ok(response)
            }
            _ => Err(crate::ClientError::Other("收到意外的响应类型".to_string())),
        }
    }

    /// 计算下次重试间隔
    fn calculate_next_interval(&self, current_interval: Duration) -> Duration {
        let next_interval = current_interval.as_secs_f64() * self.config.retry_backoff_factor;
        let next_interval = next_interval.min(self.config.max_retry_interval.as_secs_f64());
        Duration::from_secs_f64(next_interval)
    }
}

#[async_trait]
impl Producer for ProducerImpl {
    async fn send(&self, message: ProtocolMessage) -> Result<i64> {
        let message_id = message.id.clone();
        let ack_receiver = {
            let mut tracker = self.message_tracker.write().await;
            tracker.add_message(message_id.clone())
        };

        let mut retry_count = 0;
        let mut current_interval = self.config.retry_interval;

        loop {
            match self.try_send(&message).await {
                Ok(response) => {
                    // 根据 ACK 级别处理确认
                    match self.config.producer_ack_level {
                        ProducerAckLevel::None => {
                            self.handle_ack(&message_id, true).await;
                            return Ok(response.base_offset);
                        }
                        ProducerAckLevel::Leader | ProducerAckLevel::AllReplicas => {
                            // 等待确认
                            match timeout(self.config.producer_ack_timeout, ack_receiver).await {
                                Ok(Ok(())) => {
                                    return Ok(response.base_offset);
                                }
                                Ok(Err(e)) => {
                                    return Err(e);
                                }
                                Err(_) => {
                                    return Err(crate::ClientError::Ack("等待消息确认超时".to_string()));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if retry_count >= self.config.retry_count {
                        self.handle_ack(&message_id, false).await;
                        return Err(e);
                    }

                    retry_count += 1;
                    if self.config.enable_exponential_backoff {
                        current_interval = self.calculate_next_interval(current_interval);
                    }

                    sleep(current_interval).await;
                }
            }
        }
    }

    async fn send_batch(&self, messages: Vec<ProtocolMessage>) -> Result<Vec<i64>> {
        if messages.is_empty() {
            return Ok(vec![]);
        }

        let message_ids: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
        let ack_receivers: Vec<tokio::sync::oneshot::Receiver<Result<()>>> = {
            let mut tracker = self.message_tracker.write().await;
            message_ids.iter().map(|id| tracker.add_message(id.clone())).collect()
        };

        let mut retry_count = 0;
        let mut current_interval = self.config.retry_interval;

        loop {
            match self.try_send_batch(&messages).await {
                Ok(response) => {
                    // 根据 ACK 级别处理确认
                    match self.config.producer_ack_level {
                        ProducerAckLevel::None => {
                            for id in &message_ids {
                                self.handle_ack(id, true).await;
                            }
                            return Ok(vec![response.base_offset]);
                        }
                        ProducerAckLevel::Leader | ProducerAckLevel::AllReplicas => {
                            // 等待所有消息确认
                            let mut results = Vec::new();
                            for receiver in ack_receivers {
                                match timeout(self.config.producer_ack_timeout, receiver).await {
                                    Ok(Ok(())) => {
                                        results.push(Ok(()));
                                    }
                                    Ok(Err(e)) => {
                                        results.push(Err(e));
                                    }
                                    Err(_) => {
                                        results.push(Err(crate::ClientError::Ack("等待消息确认超时".to_string())));
                                    }
                                }
                            }

                            // 检查是否有任何错误
                            if results.iter().any(|r| r.is_err()) {
                                return Err(crate::ClientError::Ack("批量消息确认失败".to_string()));
                            } else {
                                return Ok(vec![response.base_offset]);
                            }
                        }
                    }
                }
                Err(e) => {
                    if retry_count >= self.config.retry_count {
                        for id in &message_ids {
                            self.handle_ack(id, false).await;
                        }
                        return Err(e);
                    }

                    retry_count += 1;
                    if self.config.enable_exponential_backoff {
                        current_interval = self.calculate_next_interval(current_interval);
                    }

                    sleep(current_interval).await;
                }
            }
        }
    }

    async fn wait_for_ack(&self, message_id: &str) -> Result<()> {
        let receiver = {
            let mut tracker = self.message_tracker.write().await;
            tracker.add_message(message_id.to_string())
        };

        match timeout(self.config.producer_ack_timeout, receiver).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(crate::ClientError::Ack("等待消息确认超时".to_string())),
        }
    }

    async fn wait_for_ack_batch(&self, message_ids: &[String]) -> Result<()> {
        let receivers: Vec<tokio::sync::oneshot::Receiver<Result<()>>> = {
            let mut tracker = self.message_tracker.write().await;
            message_ids.iter().map(|id| tracker.add_message(id.clone())).collect()
        };

        let mut results = Vec::new();
        for receiver in receivers {
            match timeout(self.config.producer_ack_timeout, receiver).await {
                Ok(Ok(())) => {
                    results.push(Ok(()));
                }
                Ok(Err(e)) => {
                    results.push(Err(e));
                }
                Err(_) => {
                    results.push(Err(crate::ClientError::Ack("等待消息确认超时".to_string())));
                }
            }
        }

        // 检查是否有任何错误
        if results.iter().any(|r| r.is_err()) {
            Err(crate::ClientError::Ack("批量消息确认失败".to_string()))
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<()> {
        let mut conn = self.connection.lock().await;
        if let Some(framed) = conn.take() {
            // 关闭连接
            drop(framed);
        }
        Ok(())
    }
} 