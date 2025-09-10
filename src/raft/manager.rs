use crate::raft::adapter::RaftRsAdapter;
use crate::raft::generic::{RaftNode, RaftNodeStatus, RaftRequest, RaftResponse, ReadConsistency};
use crate::raft::storage::memory::MemoryStorage;
use crate::raft::types::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as TokioRwLock;

/// 唯一标识一个 Raft Group（Topic + Partition）
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RaftGroupId {
    pub topic: String,
    pub partition: u32,
}

impl RaftGroupId {
    pub fn new(topic: String, partition: u32) -> Self {
        Self { topic, partition }
    }
    
    /// 元数据主题的 Raft Group ID
    pub fn metadata() -> Self {
        Self {
            topic: "__metadata".to_string(),
            partition: 0,
        }
    }
}

impl std::fmt::Display for RaftGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}

/// 多 Raft Group 管理器
/// 
/// 负责管理一个 Broker 节点上的所有 Raft Group，包括：
/// - 元数据 Raft Group（集群配置、Topic 元数据等）
/// - 各个 Topic 分区的 Raft Group
pub struct RaftGroupManager {
    /// 当前 Broker 的节点 ID
    node_id: NodeId,
    /// 所有 Raft Group 的实例
    groups: Arc<TokioRwLock<HashMap<RaftGroupId, Arc<RaftRsAdapter>>>>,
    /// 集群中其他节点的地址映射
    peer_addresses: Arc<RwLock<HashMap<NodeId, String>>>,
}

impl RaftGroupManager {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: Arc::new(TokioRwLock::new(HashMap::new())),
            peer_addresses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 添加集群节点地址
    pub fn add_peer(&self, peer_id: NodeId, address: String) {
        let mut peers = self.peer_addresses.write().unwrap();
        peers.insert(peer_id, address);
    }

    /// 创建一个新的 Raft Group
    pub async fn create_group(
        &self,
        group_id: RaftGroupId,
        replica_nodes: Vec<NodeId>,
    ) -> Result<(), Box<dyn Error>> {
        log::info!("Creating Raft group: {}", group_id);

        // 构建该 Group 的 peer 地址列表
        let peers = {
            let peer_map = self.peer_addresses.read().unwrap();
            replica_nodes
                .iter()
                .filter_map(|&node_id| {
                    if node_id == self.node_id {
                        // 跳过自己
                        None
                    } else {
                        peer_map.get(&node_id).map(|addr| (node_id, addr.clone()))
                    }
                })
                .collect::<Vec<_>>()
        };

        // 创建存储
        let storage = MemoryStorage::new();
        
        // 创建 Raft 适配器
        let adapter = Arc::new(RaftRsAdapter::new(self.node_id, peers, storage)?);
        
        // 注册到管理器
        let mut groups = self.groups.write().await;
        groups.insert(group_id, adapter);

        Ok(())
    }

    /// 获取指定 Raft Group 的引用
    pub async fn get_group(&self, group_id: &RaftGroupId) -> Option<Arc<RaftRsAdapter>> {
        let groups = self.groups.read().await;
        groups.get(group_id).cloned()
    }

    /// 删除 Raft Group
    pub async fn remove_group(&self, group_id: &RaftGroupId) -> Result<(), Box<dyn Error>> {
        log::info!("Removing Raft group: {}", group_id);
        let mut groups = self.groups.write().await;
        groups.remove(group_id);
        Ok(())
    }

    /// 获取所有 Raft Group 的状态
    pub async fn get_all_statuses(&self) -> HashMap<RaftGroupId, RaftNodeStatus> {
        let groups = self.groups.read().await;
        let mut statuses = HashMap::new();
        
        for (group_id, adapter) in groups.iter() {
            let status = adapter.status().await;
            statuses.insert(group_id.clone(), status);
        }
        
        statuses
    }

    /// 获取作为 Leader 的 Raft Group 列表
    pub async fn get_leader_groups(&self) -> Vec<RaftGroupId> {
        let groups = self.groups.read().await;
        let mut leader_groups = Vec::new();
        
        for (group_id, adapter) in groups.iter() {
            let status = adapter.status().await;
            if status.is_leader {
                leader_groups.push(group_id.clone());
            }
        }
        
        leader_groups
    }

    /// 初始化元数据 Raft Group
    /// 
    /// 这是集群启动时的第一步，用于管理集群配置和 Topic 元数据
    pub async fn initialize_metadata_group(
        &self,
        metadata_replicas: Vec<NodeId>,
    ) -> Result<(), Box<dyn Error>> {
        let metadata_group_id = RaftGroupId::metadata();
        self.create_group(metadata_group_id, metadata_replicas).await
    }

    /// 为新 Topic 创建所有分区的 Raft Group
    pub async fn create_topic_groups(
        &self,
        topic: String,
        partition_count: u32,
        replication_factor: u32,
        available_nodes: &[NodeId],
    ) -> Result<(), Box<dyn Error>> {
        log::info!(
            "Creating topic '{}' with {} partitions, replication factor {}",
            topic, partition_count, replication_factor
        );

        for partition in 0..partition_count {
            // 为每个分区选择副本节点
            let replica_nodes = self.select_replica_nodes(
                available_nodes,
                replication_factor as usize,
                partition as usize,
            );

            let group_id = RaftGroupId::new(topic.clone(), partition);
            self.create_group(group_id, replica_nodes).await?;
        }

        Ok(())
    }

    /// 副本节点选择算法（简单轮询）
    fn select_replica_nodes(
        &self,
        available_nodes: &[NodeId],
        replication_factor: usize,
        partition_index: usize,
    ) -> Vec<NodeId> {
        let mut replicas = Vec::new();
        let node_count = available_nodes.len();
        
        for i in 0..replication_factor.min(node_count) {
            let node_index = (partition_index + i) % node_count;
            replicas.push(available_nodes[node_index]);
        }
        
        replicas
    }
}

/// 为上层业务提供统一的 Raft 接口
/// 
/// 这个结构体封装了对特定 Raft Group 的操作，
/// 使得上层业务代码不需要直接处理 Group ID
pub struct RaftGroupHandle {
    group_id: RaftGroupId,
    manager: Arc<RaftGroupManager>,
}

impl RaftGroupHandle {
    pub fn new(group_id: RaftGroupId, manager: Arc<RaftGroupManager>) -> Self {
        Self { group_id, manager }
    }
    
    pub fn group_id(&self) -> &RaftGroupId {
        &self.group_id
    }
}

#[async_trait]
impl RaftNode for RaftGroupHandle {
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, Box<dyn Error>> {
        let group = self
            .manager
            .get_group(&self.group_id)
            .await
            .ok_or_else(|| format!("Raft group {} not found", self.group_id))?;
        
        group.propose(request).await
    }

    async fn query(&self, request: RaftRequest, consistency: ReadConsistency) -> Result<RaftResponse, Box<dyn Error>> {
        let group = self
            .manager
            .get_group(&self.group_id)
            .await
            .ok_or_else(|| format!("Raft group {} not found", self.group_id))?;
        
        group.query(request, consistency).await
    }

    async fn status(&self) -> RaftNodeStatus {
        if let Some(group) = self.manager.get_group(&self.group_id).await {
            group.status().await
        } else {
            RaftNodeStatus {
                node_id: self.manager.node_id,
                is_leader: false,
                term: 0,
                commit_index: 0,
                applied_index: 0,
                last_log_index: 0,
                follower_count: None,
            }
        }
    }

    async fn wait_for_leadership(&self, timeout: Option<std::time::Duration>) -> Result<(), Box<dyn Error>> {
        let group = self
            .manager
            .get_group(&self.group_id)
            .await
            .ok_or_else(|| format!("Raft group {} not found", self.group_id))?;
        
        group.wait_for_leadership(timeout).await
    }

    async fn step_down(&self) -> Result<(), Box<dyn Error>> {
        let group = self
            .manager
            .get_group(&self.group_id)
            .await
            .ok_or_else(|| format!("Raft group {} not found", self.group_id))?;
        
        group.step_down().await
    }

    async fn get_cluster_members(&self) -> Result<Vec<NodeId>, Box<dyn Error>> {
        let group = self
            .manager
            .get_group(&self.group_id)
            .await
            .ok_or_else(|| format!("Raft group {} not found", self.group_id))?;
        
        group.get_cluster_members().await
    }
}
