use crate::{Consumer, Result, ClientConfig, ConsumerAckType};
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

/// 消息状态
#[derive(Debug, Clone, Copy, PartialEq)]
enum MessageStatus {
    /// 等待确认
    Pending,
    /// 已确认
    Acknowledged,
    /// 已拒绝
    Rejected,
    /// 重试中
    Retrying,
    /// 重试次数超限
    DeadLetter,
}

/// 消息状态跟踪器
struct MessageTracker {
    /// 消息状态映射
    statuses: HashMap<String, MessageStatus>,
    /// 消息重试次数
    retry_counts: HashMap<String, u32>,
    /// 消息确认通道
    ack_channels: HashMap<String, tokio::sync::oneshot::Sender<Result<()>>>,
}

impl MessageTracker {
    fn new() -> Self {
        Self {
            statuses: HashMap::new(),
            retry_counts: HashMap::new(),
            ack_channels: HashMap::new(),
        }
    }

    /// 添加消息到跟踪器
    fn add_message(&mut self, message_id: String) -> tokio::sync::oneshot::Receiver<Result<()>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.statuses.insert(message_id.clone(), MessageStatus::Pending);
        self.retry_counts.insert(message_id.clone(), 0);
        self.ack_channels.insert(message_id, tx);
        rx
    }

    /// 更新消息状态
    fn update_status(&mut self, message_id: &str, status: MessageStatus) -> Option<Result<()>> {
        self.statuses.insert(message_id.to_string(), status);
        if let Some(channel) = self.ack_channels.remove(message_id) {
            match status {
                MessageStatus::Acknowledged => Some(Ok(())),
                MessageStatus::Rejected => Some(Err(crate::ClientError::Ack("消息被拒绝".to_string()))),
                MessageStatus::DeadLetter => Some(Err(crate::ClientError::Ack("消息重试次数超限".to_string()))),
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

    /// 增加重试次数
    fn increment_retry_count(&mut self, message_id: &str) -> u32 {
        let count = self.retry_counts.entry(message_id.to_string()).or_insert(0);
        *count += 1;
        *count
    }

    /// 获取重试次数
    fn get_retry_count(&self, message_id: &str) -> u32 {
        *self.retry_counts.get(message_id).unwrap_or(&0)
    }
}

/// 消费者实现
pub struct ConsumerImpl {
    config: Arc<ClientConfig>,
    connection: Mutex<Option<Framed<TcpStream, bifrost::protocol::codec::ClientCodec>>>,
    group_id: String,
    consumer_id: String,
    message_tracker: Arc<RwLock<MessageTracker>>,
    auto_commit_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ConsumerImpl {
    /// 创建新的消费者
    pub async fn new(config: ClientConfig, group_id: String) -> Result<Self> {
        let consumer_id = format!("consumer-{}", uuid::Uuid::new_v4());
        let consumer = Self {
            config: Arc::new(config),
            connection: Mutex::new(None),
            group_id,
            consumer_id,
            message_tracker: Arc::new(RwLock::new(MessageTracker::new())),
            auto_commit_task: Mutex::new(None),
        };

        // 如果是自动确认模式，启动自动确认任务
        if consumer.config.consumer_ack_type == ConsumerAckType::Auto {
            consumer.start_auto_commit().await?;
        }

        Ok(consumer)
    }

    /// 启动自动确认任务
    async fn start_auto_commit(&self) -> Result<()> {
        let message_tracker = self.message_tracker.clone();
        let config = self.config.clone();
        let group_id = self.group_id.clone();
        let consumer_id = self.consumer_id.clone();
        let connection = self.connection.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.consumer_auto_commit_interval);
            loop {
                interval.tick().await;
                
                // 获取所有待确认的消息
                let pending_messages: Vec<String> = {
                    let tracker = message_tracker.read().await;
                    tracker.statuses
                        .iter()
                        .filter(|(_, status)| **status == MessageStatus::Pending)
                        .map(|(id, _)| id.clone())
                        .collect()
                };

                if !pending_messages.is_empty() {
                    // 批量确认消息
                    if let Err(e) = Self::commit_messages(
                        &connection,
                        &group_id,
                        &consumer_id,
                        &pending_messages,
                    ).await {
                        eprintln!("自动确认消息失败: {}", e);
                    }
                }
            }
        });

        *self.auto_commit_task.lock().await = Some(handle);
        Ok(())
    }

    /// 提交消息
    async fn commit_messages(
        connection: &Mutex<Option<Framed<TcpStream, bifrost::protocol::codec::ClientCodec>>>,
        group_id: &str,
        consumer_id: &str,
        message_ids: &[String],
    ) -> Result<()> {
        let mut conn = connection.lock().await;
        let framed = conn.as_mut().unwrap();

        let request = Request::Ack(bifrost::protocol::AckRequest {
            message_ids: message_ids.to_vec(),
            group_id: group_id.to_string(),
            consumer_id: consumer_id.to_string(),
        });

        framed.send(request).await?;

        if let Some(response) = framed.next().await {
            match response? {
                Response::Ack(response) => {
                    if let Some(error) = response.error {
                        return Err(crate::ClientError::Server(error));
                    }
                    Ok(())
                }
                _ => Err(crate::ClientError::Other("收到意外的响应类型".to_string())),
            }
        } else {
            Err(crate::ClientError::Connection("连接已关闭".to_string()))
        }
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
            MessageStatus::Rejected
        };
        
        if let Some(result) = tracker.update_status(message_id, status) {
            // 通知等待的调用者
            if let Err(e) = result {
                eprintln!("消息确认失败: {}", e);
            }
        }
    }

    /// 处理消息重试
    async fn handle_retry(&self, message_id: &str) -> Result<()> {
        let mut tracker = self.message_tracker.write().await;
        let retry_count = tracker.increment_retry_count(message_id);

        if retry_count > self.config.consumer_max_retries {
            tracker.update_status(message_id, MessageStatus::DeadLetter);
            return Err(crate::ClientError::Ack("消息重试次数超限".to_string()));
        }

        tracker.update_status(message_id, MessageStatus::Retrying);
        Ok(())
    }
}

#[async_trait]
impl Consumer for ConsumerImpl {
    async fn consume(&self, topic: &str, partition: i32, offset: i64) -> Result<Vec<ProtocolMessage>> {
        let request = Request::Fetch(bifrost::protocol::FetchRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_bytes: 1024 * 1024, // 1MB
            group_id: self.group_id.clone(),
            consumer_id: self.consumer_id.clone(),
        });

        let messages = match self.send_request(request).await? {
            Response::Fetch(response) => {
                if let Some(error) = response.error {
                    return Err(crate::ClientError::Server(error));
                }
                response.messages
            }
            _ => return Err(crate::ClientError::Other("收到意外的响应类型".to_string())),
        };

        // 将消息添加到跟踪器
        for message in &messages {
            let mut tracker = self.message_tracker.write().await;
            tracker.add_message(message.id.clone());
        }

        Ok(messages)
    }

    async fn ack(&self, message_id: &str) -> Result<()> {
        let request = Request::Ack(bifrost::protocol::AckRequest {
            message_ids: vec![message_id.to_string()],
            group_id: self.group_id.clone(),
            consumer_id: self.consumer_id.clone(),
        });

        match self.send_request(request).await? {
            Response::Ack(response) => {
                if let Some(error) = response.error {
                    self.handle_ack(message_id, false).await;
                    return Err(crate::ClientError::Server(error));
                }
                self.handle_ack(message_id, true).await;
                Ok(())
            }
            _ => {
                self.handle_ack(message_id, false).await;
                Err(crate::ClientError::Other("收到意外的响应类型".to_string()))
            }
        }
    }

    async fn ack_batch(&self, message_ids: &[String]) -> Result<()> {
        let request = Request::Ack(bifrost::protocol::AckRequest {
            message_ids: message_ids.to_vec(),
            group_id: self.group_id.clone(),
            consumer_id: self.consumer_id.clone(),
        });

        match self.send_request(request).await? {
            Response::Ack(response) => {
                if let Some(error) = response.error {
                    for id in message_ids {
                        self.handle_ack(id, false).await;
                    }
                    return Err(crate::ClientError::Server(error));
                }
                for id in message_ids {
                    self.handle_ack(id, true).await;
                }
                Ok(())
            }
            _ => {
                for id in message_ids {
                    self.handle_ack(id, false).await;
                }
                Err(crate::ClientError::Other("收到意外的响应类型".to_string()))
            }
        }
    }

    async fn reject(&self, message_id: &str) -> Result<()> {
        let mut tracker = self.message_tracker.write().await;
        tracker.update_status(message_id, MessageStatus::Rejected);
        Ok(())
    }

    async fn reject_batch(&self, message_ids: &[String]) -> Result<()> {
        let mut tracker = self.message_tracker.write().await;
        for id in message_ids {
            tracker.update_status(id, MessageStatus::Rejected);
        }
        Ok(())
    }

    async fn retry(&self, message_id: &str) -> Result<()> {
        self.handle_retry(message_id).await
    }

    async fn retry_batch(&self, message_ids: &[String]) -> Result<()> {
        for id in message_ids {
            self.handle_retry(id).await?;
        }
        Ok(())
    }

    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let request = Request::CommitOffset(bifrost::protocol::CommitOffsetRequest {
            topic: topic.to_string(),
            partition,
            offset,
            group_id: self.group_id.clone(),
            consumer_id: self.consumer_id.clone(),
        });

        match self.send_request(request).await? {
            Response::CommitOffset(response) => {
                if let Some(error) = response.error {
                    return Err(crate::ClientError::Server(error));
                }
                Ok(())
            }
            _ => Err(crate::ClientError::Other("收到意外的响应类型".to_string())),
        }
    }

    async fn close(&self) -> Result<()> {
        // 停止自动确认任务
        if let Some(handle) = self.auto_commit_task.lock().await.take() {
            handle.abort();
        }

        let mut conn = self.connection.lock().await;
        if let Some(framed) = conn.take() {
            // 关闭连接
            drop(framed);
        }
        Ok(())
    }
} 