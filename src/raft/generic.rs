use crate::raft::types::NodeId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;

/// Raft 请求类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftRequest {
    /// 请求的业务数据
    pub data: Vec<u8>,
    /// 请求类型（可选，用于业务层区分不同操作）
    pub request_type: Option<String>,
    /// 请求 ID（用于去重和追踪）
    pub request_id: Option<String>,
}

impl RaftRequest {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            request_type: None,
            request_id: None,
        }
    }

    pub fn with_type(mut self, request_type: String) -> Self {
        self.request_type = Some(request_type);
        self
    }

    pub fn with_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }
}

/// Raft 响应类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftResponse {
    /// 响应的业务数据
    pub data: Vec<u8>,
    /// 是否成功
    pub success: bool,
    /// 错误信息（如果有）
    pub error: Option<String>,
    /// 响应对应的请求 ID
    pub request_id: Option<String>,
}

impl RaftResponse {
    pub fn success(data: Vec<u8>) -> Self {
        Self {
            data,
            success: true,
            error: None,
            request_id: None,
        }
    }

    pub fn error(error: String) -> Self {
        Self {
            data: Vec::new(),
            success: false,
            error: Some(error),
            request_id: None,
        }
    }

    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }
}

/// Raft 节点状态
#[derive(Clone, Debug)]
pub struct RaftNodeStatus {
    /// 节点 ID
    pub node_id: NodeId,
    /// 是否为 Leader
    pub is_leader: bool,
    /// 当前任期
    pub term: u64,
    /// 已提交的日志索引
    pub commit_index: u64,
    /// 已应用的日志索引
    pub applied_index: u64,
    /// 最后日志索引
    pub last_log_index: u64,
    /// Follower 数量（仅 Leader 有效）
    pub follower_count: Option<u32>,
}

/// 读取一致性级别
#[derive(Clone, Debug)]
pub enum ReadConsistency {
    /// 强一致性读取（需要 Leader 确认）
    Linearizable,
    /// 最终一致性读取（可以从任何节点读取）
    Eventual,
    /// 租约读取（Leader 在租约期内可直接响应）
    LeaderLease,
}

/// 核心 Raft 节点接口
///
/// 这个 trait 定义了与单个 Raft Group 交互的核心接口，
/// 上层业务代码通过这个接口与 Raft 层交互，而不需要关心具体的实现细节。
#[async_trait]
pub trait RaftNode: Send + Sync + 'static {
    /// 提交写请求到 Raft 组
    ///
    /// 这个请求会被复制到所有节点，并在达成共识后应用到状态机。
    /// 只有 Leader 节点可以成功处理此请求。
    ///
    /// # 参数
    /// - `request`: 要提交的请求数据
    ///
    /// # 返回
    /// - `Ok(RaftResponse)`: 请求成功提交并应用后的响应
    /// - `Err(_)`: 请求失败（可能是网络错误、不是 Leader 等）
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, Box<dyn Error>>;

    /// 提交读请求到 Raft 组
    ///
    /// 根据一致性级别的不同，可能直接从本地读取或需要经过 Raft 共识。
    ///
    /// # 参数
    /// - `request`: 要查询的请求数据
    /// - `consistency`: 读取一致性级别
    ///
    /// # 返回
    /// - `Ok(RaftResponse)`: 查询结果
    /// - `Err(_)`: 查询失败
    async fn query(
        &self,
        request: RaftRequest,
        consistency: ReadConsistency,
    ) -> Result<RaftResponse, Box<dyn Error>>;

    /// 获取当前节点在 Raft 组中的状态
    async fn status(&self) -> RaftNodeStatus;

    /// 检查当前节点是否为 Leader
    async fn is_leader(&self) -> bool {
        self.status().await.is_leader
    }

    /// 等待成为 Leader（可选超时）
    async fn wait_for_leadership(&self, timeout: Option<std::time::Duration>) -> Result<(), Box<dyn Error>>;

    /// 主动让出 Leadership（如果是 Leader）
    async fn step_down(&self) -> Result<(), Box<dyn Error>>;

    /// 获取集群成员信息
    async fn get_cluster_members(&self) -> Result<Vec<NodeId>, Box<dyn Error>>;
}

/// 扩展的 Raft 管理接口
///
/// 提供集群管理、配置变更等高级功能
#[async_trait]
pub trait RaftAdmin: Send + Sync + 'static {
    /// 添加节点到 Raft 组
    async fn add_node(&self, node_id: NodeId, address: String) -> Result<(), Box<dyn Error>>;

    /// 从 Raft 组中移除节点
    async fn remove_node(&self, node_id: NodeId) -> Result<(), Box<dyn Error>>;

    /// 触发快照创建
    async fn create_snapshot(&self) -> Result<(), Box<dyn Error>>;

    /// 获取 Raft 日志统计信息
    async fn get_log_stats(&self) -> Result<LogStats, Box<dyn Error>>;

    /// 压缩日志（保留到指定索引）
    async fn compact_log(&self, retain_index: u64) -> Result<(), Box<dyn Error>>;
}

/// Raft 日志统计信息
#[derive(Clone, Debug)]
pub struct LogStats {
    /// 第一个日志索引
    pub first_index: u64,
    /// 最后一个日志索引
    pub last_index: u64,
    /// 已提交索引
    pub committed_index: u64,
    /// 已应用索引
    pub applied_index: u64,
    /// 日志条目总数
    pub entry_count: u64,
    /// 日志大小（字节）
    pub log_size_bytes: u64,
}

/// 便利类型别名
pub type RaftResult<T> = Result<T, Box<dyn Error>>;

// 向后兼容的类型别名
pub type Request = RaftRequest;
pub type Response = RaftResponse;
pub type NodeStatus = RaftNodeStatus;
pub type GenericRaftNode = dyn RaftNode + Send + Sync + 'static;