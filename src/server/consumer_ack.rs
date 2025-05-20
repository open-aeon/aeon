use crate::protocol::ack::{AckAction, ConsumerAckConfig, ConsumerAckType, MessageStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::interval;

/// 消费者 ACK 管理器
pub struct ConsumerAckManager {
    /// 配置
    config: ConsumerAckConfig,
    /// 消息状态
    message_status: Arc<RwLock<HashMap<String, MessageStatus>>>,
    /// 消息重试次数
    retry_count: Arc<RwLock<HashMap<String, u32>>>,
}

impl ConsumerAckManager {
    /// 创建新的消费者 ACK 管理器
    pub fn new(config: ConsumerAckConfig) -> Self {
        Self {
            config,
            message_status: Arc::new(RwLock::new(HashMap::new())),
            retry_count: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 处理消息确认
    pub async fn handle_ack(&self, message_id: &str, action: AckAction) -> Result<(), String> {
        match action {
            AckAction::Commit => self.commit_message(message_id).await,
            AckAction::Reject => self.reject_message(message_id).await,
            AckAction::Retry => self.retry_message(message_id).await,
        }
    }

    /// 提交消息
    async fn commit_message(&self, message_id: &str) -> Result<(), String> {
        let mut status = self.message_status.write().await;
        status.insert(message_id.to_string(), MessageStatus::Committed);
        Ok(())
    }

    /// 拒绝消息
    async fn reject_message(&self, message_id: &str) -> Result<(), String> {
        let mut status = self.message_status.write().await;
        status.insert(message_id.to_string(), MessageStatus::Rejected);
        Ok(())
    }

    /// 重试消息
    async fn retry_message(&self, message_id: &str) -> Result<(), String> {
        let mut retry_count = self.retry_count.write().await;
        let current_retries = retry_count.entry(message_id.to_string()).or_insert(0);
        
        if *current_retries >= self.config.max_retries {
            let mut status = self.message_status.write().await;
            status.insert(message_id.to_string(), MessageStatus::DeadLetter);
            return Err("超过最大重试次数".to_string());
        }

        *current_retries += 1;
        let mut status = self.message_status.write().await;
        status.insert(message_id.to_string(), MessageStatus::Retrying);
        Ok(())
    }

    /// 启动自动确认任务
    pub async fn start_auto_commit(&self) {
        if self.config.ack_type != ConsumerAckType::Auto {
            return;
        }

        let message_status = self.message_status.clone();
        let interval_duration = self.config.auto_commit_interval;
        
        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            loop {
                interval.tick().await;
                let mut status = message_status.write().await;
                // 将所有 Pending 状态的消息标记为已确认
                for (_, status) in status.iter_mut() {
                    if *status == MessageStatus::Pending {
                        *status = MessageStatus::Committed;
                    }
                }
            }
        });
    }

    /// 获取消息状态
    pub async fn get_message_status(&self, message_id: &str) -> Option<MessageStatus> {
        let status = self.message_status.read().await;
        status.get(message_id).copied()
    }

    /// 获取消息重试次数
    pub async fn get_retry_count(&self, message_id: &str) -> u32 {
        let retry_count = self.retry_count.read().await;
        retry_count.get(message_id).copied().unwrap_or(0)
    }
} 