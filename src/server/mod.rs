mod connection;
mod consumer_ack;
mod producer_ack;

use crate::protocol::ack::{ConsumerAckConfig, ProducerAckConfig};
use crate::server::connection::ConnectionManager;
use crate::server::consumer_ack::ConsumerAckManager;
use crate::server::producer_ack::ProducerAckManager;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use std::time::Duration;
use futures::{StreamExt, SinkExt};

/// 服务器结构体
pub struct Server {
    /// 分区管理器
    partition_manager: Arc<RwLock<crate::broker::partition::PartitionManager>>,
    /// 消费者组管理器
    consumer_group_manager: Arc<RwLock<crate::broker::consumer_group::ConsumerGroupManager>>,
    /// 连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 生产者 ACK 管理器
    producer_ack_manager: Arc<ProducerAckManager>,
    /// 消费者 ACK 管理器
    consumer_ack_manager: Arc<ConsumerAckManager>,
}

impl Server {
    /// 创建新的服务器实例
    pub fn new(
        partition_manager: Arc<RwLock<crate::broker::partition::PartitionManager>>,
        consumer_group_manager: Arc<RwLock<crate::broker::consumer_group::ConsumerGroupManager>>,
        max_connections: usize,
        connection_idle_timeout: Duration,
    ) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new(max_connections, connection_idle_timeout));
        let producer_ack_manager = Arc::new(ProducerAckManager::new(ProducerAckConfig::default()));
        let consumer_ack_manager = Arc::new(ConsumerAckManager::new(ConsumerAckConfig::default()));

        Self {
            partition_manager,
            consumer_group_manager,
            connection_manager,
            producer_ack_manager,
            consumer_ack_manager,
        }
    }

    /// 启动服务器
    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("服务器监听地址: {}", addr);

        // 启动消费者自动确认任务
        self.consumer_ack_manager.start_auto_commit().await;

        loop {
            let (_socket, addr) = listener.accept().await?;
            println!("新连接: {}", addr);

            let connection_manager = self.connection_manager.clone();
            let _partition_manager = self.partition_manager.clone();
            let _consumer_group_manager = self.consumer_group_manager.clone();
            let _producer_ack_manager = self.producer_ack_manager.clone();
            let _consumer_ack_manager = self.consumer_ack_manager.clone();

            tokio::spawn(async move {
                if let Err(e) = connection_manager.add_connection(addr).await {
                    eprintln!("处理连接失败: {}", e);
                    return;
                }

                let mut framed = tokio_util::codec::Framed::new(_socket, crate::protocol::codec::ServerCodec::default());
                
                while let Some(request) = framed.next().await {
                    match request {
                        Ok(request) => {
                            let mut consumer_group_manager = _consumer_group_manager.write().await;
                            let partition_manager = _partition_manager.read().await;
                            
                            match crate::protocol::commands::handle_request(
                                request,
                                &partition_manager,
                                &mut consumer_group_manager,
                            ).await {
                                Ok(response) => {
                                    if let Err(e) = framed.send(response).await {
                                        eprintln!("发送响应失败: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("处理请求失败: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("读取请求失败: {}", e);
                            break;
                        }
                    }
                }
            });
        }
    }
} 