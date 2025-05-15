use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::RwLock,
    time,
};
use anyhow::Result;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Connection {
    pub addr: SocketAddr,
    pub last_active: Instant,
    pub status: ConnectionStatus,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ConnectionStatus {
    Active,
    Idle,
    Closing,
}

#[derive(Clone)]
pub struct ConnectionManager {
    connections: Arc<RwLock<HashMap<SocketAddr, Connection>>>,
    max_connections: usize,
    idle_timeout: Duration,
}

#[allow(dead_code)]
impl ConnectionManager {
    pub fn new(max_connections: usize, idle_timeout: Duration) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections,
            idle_timeout,
        }
    }

    pub async fn add_connection(&self, addr: SocketAddr) -> Result<()> {
        let mut connections = self.connections.write().await;

        if connections.len() >= self.max_connections {
            return Err(anyhow::anyhow!("达到最大连接数限制"));
        }

        let connection = Connection {
            addr,
            last_active: Instant::now(),
            status: ConnectionStatus::Active,
        };

        connections.insert(addr, connection);
        println!("新连接: {}, 当前连接数: {}", addr, connections.len());

        Ok(())
    }

    pub async fn remove_connection(&self, addr: SocketAddr) -> Result<()> {
        let mut connections = self.connections.write().await;
        if connections.remove(&addr).is_some() {
            println!("移除连接: {}, 当前连接数: {}", addr, connections.len());
        }
        Ok(())
    }

    pub async fn update_connection_status(&self, addr: SocketAddr, status: ConnectionStatus) -> Result<()> {
        let mut connections = self.connections.write().await;
        if let Some(connection) = connections.get_mut(&addr) {
            connection.status = status;
            connection.last_active = Instant::now();
        }
        Ok(())
    }

    pub async fn start_cleanup_task(&self) {
        let connections = self.connections.clone();
        let idle_timeout = self.idle_timeout;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut connections = connections.write().await;
                let now = Instant::now();
                
                // 移除空闲连接
                connections.retain(|addr, conn| {
                    if conn.status == ConnectionStatus::Idle && 
                       now.duration_since(conn.last_active) > idle_timeout {
                        println!("清理空闲连接: {}", addr);
                        false
                    } else {
                        true
                    }
                });
            }
        });
    }

    pub async fn get_connection_count(&self) -> usize {
        self.connections.read().await.len()
    }

    pub async fn get_active_connection_count(&self) -> usize {
        self.connections.read().await
            .values()
            .filter(|conn| conn.status == ConnectionStatus::Active)
            .count()
    }
} 