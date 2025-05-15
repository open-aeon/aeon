pub mod connection;

pub use connection::ConnectionManager;
use futures::{StreamExt, SinkExt};
use tokio_util::codec::Framed;
use crate::protocol::codec::ServerCodec;

pub struct Server {
    partition_manager: crate::broker::partition::PartitionManager,
    consumer_group_manager: crate::broker::consumer_group::ConsumerGroupManager,
    connection_manager: ConnectionManager,
}

impl Server {
    pub fn new(partition_manager: crate::broker::partition::PartitionManager, consumer_group_manager: crate::broker::consumer_group::ConsumerGroupManager) -> Self {
        let connection_manager = ConnectionManager::new(
            1000, // 最大连接数
            std::time::Duration::from_secs(300), // 空闲超时时间
        );

        Self {
            partition_manager,
            consumer_group_manager,
            connection_manager,
        }
    }

    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        println!("服务器监听在 {}", addr);

        // 启动连接清理任务
        self.connection_manager.start_cleanup_task().await;

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("接受来自 {} 的新连接", addr);

            // 添加到连接管理器
            let socket = match self.connection_manager.add_connection(addr).await {
                Ok(_) => socket,
                Err(e) => {
                    println!("连接管理错误: {}", e);
                    continue;
                }
            };

            let partition_manager = self.partition_manager.clone();
            let consumer_group_manager = self.consumer_group_manager.clone();
            let connection_manager = self.connection_manager.clone();

            tokio::spawn(async move {
                let mut framed = Framed::new(socket, ServerCodec::new());

                while let Some(result) = framed.next().await {
                    match result {
                        Ok(request) => {
                            // 更新连接状态为活跃
                            if let Err(e) = connection_manager.update_connection_status(addr, connection::ConnectionStatus::Active).await {
                                println!("更新连接状态失败: {}", e);
                            }

                            match crate::protocol::commands::handle_request(request, &partition_manager, &consumer_group_manager).await {
                                Ok(response) => {
                                    if let Err(e) = framed.send(response).await {
                                        println!("发送响应失败: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    println!("处理请求失败: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            println!("读取请求失败: {}", e);
                            break;
                        }
                    }
                }

                // 连接关闭，从连接管理器中移除
                if let Err(e) = connection_manager.remove_connection(addr).await {
                    println!("移除连接失败: {}", e);
                }
            });
        }
    }
} 