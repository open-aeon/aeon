use crate::{Consumer, Result, ClientConfig, ConsumerAckType, RetryQueuePolicy};
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

/// 重试队列
struct RetryQueue {
    /// 消息队列
    messages: Vec<ProtocolMessage>,
    /// 队列容量
    capacity: usize,
    /// 队列策略
    policy: RetryQueuePolicy,
    /// 当前消费速率（消息/秒）
    current_consume_rate: f64,
    /// 原始消费速率（消息/秒）
    original_consume_rate: f64,
}

impl RetryQueue {
    fn new(capacity: usize, policy: RetryQueuePolicy) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            capacity,
            policy,
            current_consume_rate: 100.0, // 默认值，实际值应该从配置中获取
            original_consume_rate: 100.0,
        }
    }

    /// 添加消息到队列
    fn push(&mut self, message: ProtocolMessage, consume_rate_control: &mut ConsumeRateControl) -> Result<()> {
        if self.messages.len() >= self.capacity {
            match self.policy {
                RetryQueuePolicy::FlowControl { slow_down_threshold, stop_threshold } => {
                    let queue_usage = self.messages.len() as f64 / self.capacity as f64;
                    if queue_usage >= stop_threshold {
                        // 暂停消费
                        consume_rate_control.update_rate(0.0);
                        return Err(crate::ClientError::Other("重试队列已满，暂停消费".to_string()));
                    } else if queue_usage >= slow_down_threshold {
                        // 线性降低消费速率
                        let reduction_factor = 1.0 - (queue_usage - slow_down_threshold) / (stop_threshold - slow_down_threshold);
                        let new_rate = self.original_consume_rate * reduction_factor;
                        self.current_consume_rate = new_rate;
                        consume_rate_control.update_rate(new_rate);
                        eprintln!("重试队列接近容量上限，降低消费速率至: {:.2} 消息/秒", new_rate);
                    }
                }
                RetryQueuePolicy::MoveToDeadLetter => {
                    // 移除最早的消息
                    if let Some(oldest_message) = self.messages.first() {
                        // TODO: 实现死信队列处理
                        eprintln!("重试队列已满，将最早的消息移动到死信队列: {}", oldest_message.id);
                    }
                    self.messages.remove(0);
                }
                RetryQueuePolicy::ReturnError => {
                    return Err(crate::ClientError::Other("重试队列已满，拒绝新的重试消息".to_string()));
                }
                RetryQueuePolicy::Ignore => {
                    eprintln!("重试队列已满，忽略新的重试消息: {}", message.id);
                    return Ok(());
                }
            }
        }
        self.messages.push(message);
        Ok(())
    }

    /// 获取所有消息
    fn drain(&mut self) -> Vec<ProtocolMessage> {
        self.messages.drain(..).collect()
    }

    /// 获取队列大小
    fn len(&self) -> usize {
        self.messages.len()
    }

    /// 获取当前消费速率
    fn get_current_consume_rate(&self) -> f64 {
        self.current_consume_rate
    }

    /// 重置消费速率
    fn reset_consume_rate(&mut self) {
        self.current_consume_rate = self.original_consume_rate;
    }
}

/// 消费速率控制
struct ConsumeRateControl {
    /// 当前消费速率（消息/秒）
    current_rate: f64,
    /// 原始消费速率（消息/秒）
    original_rate: f64,
    /// 上次消费时间
    last_consume_time: Option<chrono::DateTime<Utc>>,
}

impl ConsumeRateControl {
    fn new(rate: f64) -> Self {
        Self {
            current_rate: rate,
            original_rate: rate,
            last_consume_time: None,
        }
    }

    /// 更新消费速率
    fn update_rate(&mut self, new_rate: f64) {
        self.current_rate = new_rate;
    }

    /// 重置消费速率
    fn reset_rate(&mut self) {
        self.current_rate = self.original_rate;
    }

    /// 计算下次消费的等待时间
    fn calculate_wait_time(&mut self) -> Duration {
        let now = Utc::now();
        let wait_time = if let Some(last_time) = self.last_consume_time {
            // 计算应该等待的时间（毫秒）
            let interval = 1000.0 / self.current_rate;
            let elapsed = (now - last_time).num_milliseconds() as f64;
            let wait = (interval - elapsed).max(0.0);
            Duration::from_millis(wait as u64)
        } else {
            Duration::from_millis(0)
        };
        self.last_consume_time = Some(now);
        wait_time
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
    message_handler: Option<Arc<dyn Fn(&ProtocolMessage) -> Result<()> + Send + Sync>>,
    // 本地重试队列
    retry_queue: Arc<RwLock<RetryQueue>>,
    // 重试任务句柄
    retry_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // 消费速率控制
    consume_rate_control: Arc<RwLock<ConsumeRateControl>>,
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
            message_handler: None,
            retry_queue: Arc::new(RwLock::new(RetryQueue::new(
                config.retry_config.retry_queue_capacity,
                config.retry_config.retry_queue_policy,
            ))),
            retry_task: Mutex::new(None),
            consume_rate_control: Arc::new(RwLock::new(ConsumeRateControl::new(100.0))), // 默认100消息/秒
        };

        // 如果是自动确认模式，启动自动确认任务
        if consumer.config.consumer_ack_type == ConsumerAckType::Auto {
            consumer.start_auto_commit().await?;
        }

        // 启动重试任务
        consumer.start_retry_task().await?;

        Ok(consumer)
    }

    /// 设置消息处理函数
    pub fn set_message_handler<F>(&mut self, handler: F)
    where
        F: Fn(&ProtocolMessage) -> Result<()> + Send + Sync + 'static,
    {
        self.message_handler = Some(Arc::new(handler));
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

    pub async fn process_message(&self, message: &mut ProtocolMessage) -> Result<()> {
        match self.try_process(message).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if message.retry_count >= self.config.consumer_max_retries {
                    // 超过最大重试次数，移动到死信队列
                    if let Some(dead_letter_topic) = &self.config.retry_config.dead_letter_topic {
                        // TODO: 实现死信队列处理
                        eprintln!("消息重试次数超限，移动到死信队列: {}", message.id);
                    }
                    return Err(e);
                }

                // 更新重试信息
                message.retry_count += 1;
                message.retry_reason = Some(e.to_string());
                
                // 计算下次重试时间
                let delay = if self.config.retry_config.enable_delayed_retry {
                    self.config.retry_config.retry_interval
                } else {
                    Duration::from_secs(0)
                };
                
                message.next_retry_time = Some(Utc::now() + chrono::Duration::from_std(delay)?);
                
                // 添加到本地重试队列
                let mut queue = self.retry_queue.write().await;
                let mut control = self.consume_rate_control.write().await;
                queue.push(message.clone(), &mut control)?;
                
                Ok(())
            }
        }
    }

    async fn try_process(&self, message: &ProtocolMessage) -> Result<()> {
        if let Some(handler) = &self.message_handler {
            handler(message)
        } else {
            Err(crate::ClientError::Other("未设置消息处理函数".to_string()))
        }
    }

    async fn move_to_dead_letter(&self, message: &ProtocolMessage, topic: &str) -> Result<()> {
        // TODO: 实现死信队列处理逻辑
        Ok(())
    }

    /// 启动重试任务
    async fn start_retry_task(&self) -> Result<()> {
        let retry_queue = self.retry_queue.clone();
        let message_handler = self.message_handler.clone();
        let config = self.config.clone();
        let consume_rate_control = self.consume_rate_control.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                
                // 处理重试队列中的消息
                let mut queue = retry_queue.write().await;
                let mut control = consume_rate_control.write().await;
                let now = Utc::now();
                
                // 检查是否需要调整消费速率
                if let RetryQueuePolicy::FlowControl { slow_down_threshold, .. } = queue.policy {
                    let queue_usage = queue.len() as f64 / config.retry_config.retry_queue_capacity as f64;
                    if queue_usage < slow_down_threshold {
                        queue.reset_consume_rate();
                        control.reset_rate();
                        eprintln!("重试队列使用率降低，恢复消费速率至: {:.2} 消息/秒", control.current_rate);
                    }
                }
                
                // 过滤出需要重试的消息
                let (retry_now, keep): (Vec<_>, Vec<_>) = queue.drain()
                    .partition(|msg| {
                        if let Some(next_retry_time) = msg.next_retry_time {
                            next_retry_time <= now
                        } else {
                            false
                        }
                    });

                // 将不需要立即重试的消息放回队列
                for message in keep {
                    if let Err(e) = queue.push(message, &mut control) {
                        eprintln!("将消息放回重试队列失败: {}", e);
                    }
                }

                // 处理需要重试的消息
                for mut message in retry_now {
                    if let Some(handler) = &message_handler {
                        match handler(&message) {
                            Ok(_) => {
                                // 重试成功，消息处理完成
                                continue;
                            }
                            Err(e) => {
                                // 重试失败，更新重试信息
                                message.retry_count += 1;
                                message.retry_reason = Some(e.to_string());

                                if message.retry_count >= config.consumer_max_retries {
                                    // 超过最大重试次数，移动到死信队列
                                    if let Some(dead_letter_topic) = &config.retry_config.dead_letter_topic {
                                        // TODO: 实现死信队列处理
                                        eprintln!("消息重试次数超限，移动到死信队列: {}", message.id);
                                    }
                                    continue;
                                }

                                // 计算下次重试时间
                                let delay = if config.retry_config.enable_delayed_retry {
                                    config.retry_config.retry_interval
                                } else {
                                    Duration::from_secs(0)
                                };
                                
                                message.next_retry_time = Some(now + chrono::Duration::from_std(delay).unwrap());
                                if let Err(e) = queue.push(message, &mut control) {
                                    eprintln!("将消息放回重试队列失败: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        *self.retry_task.lock().await = Some(handle);
        Ok(())
    }

    /// 消费消息
    async fn consume(&self, topic: &str, partition: i32, offset: i64) -> Result<Vec<ProtocolMessage>> {
        // 计算需要等待的时间
        let wait_time = {
            let mut control = self.consume_rate_control.write().await;
            control.calculate_wait_time()
        };
        
        // 等待指定的时间
        if !wait_time.is_zero() {
            tokio::time::sleep(wait_time).await;
        }

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

        // 处理每条消息
        let mut processed_messages = Vec::new();
        for mut message in messages {
            if let Err(e) = self.process_message(&mut message).await {
                eprintln!("处理消息失败: {}", e);
                continue;
            }
            processed_messages.push(message);
        }

        Ok(processed_messages)
    }
}

#[async_trait]
impl Consumer for ConsumerImpl {
    async fn consume(&self, topic: &str, partition: i32, offset: i64) -> Result<Vec<ProtocolMessage>> {
        self.consume(topic, partition, offset).await
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

        // 停止重试任务
        if let Some(handle) = self.retry_task.lock().await.take() {
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