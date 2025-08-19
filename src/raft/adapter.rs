use super::generic::{Request, Response, GenericRaftNode, NodeStatus};
use async_trait::async_trait;
use std::error::Error;

/// OpenRaftAdapter 是 `GenericRaftNode` trait 的具体实现。
/// 它将我们的通用 Raft API 适配到底层的 `openraft` 库。
pub struct OpenRaftAdapter {
    // 在这里，我们将持有 openraft 的核心实例，例如：
    // raft: openraft::Raft<MyTypeConfig>,
    //
    // 以及用于网络通信的客户端等。
}

impl OpenRaftAdapter {
    /// 创建一个新的 OpenRaftAdapter 实例。
    /// 这里会初始化底层的 openraft 节点。
    pub fn new() -> Self {
        // ... 初始化逻辑 ...
        Self {}
    }
}

#[async_trait]
impl GenericRaftNode for OpenRaftAdapter {
    async fn propose(&self, _request: Request) -> Result<Response, Box<dyn Error>> {
        // 实现细节:
        // 1. 将 Request 转换成 openraft 需要的格式。
        // 2. 调用 self.raft.client_write() 方法。
        // 3. 处理来自 openraft 的结果，转换成我们的 Response。
        unimplemented!("将在这里实现 propose 逻辑");
    }

    async fn query(&self, _request: Request) -> Result<Response, Box<dyn Error>> {
        // 实现细节:
        // 1. 确保我们是 Leader。
        // 2. 调用 self.raft.is_leader() 或类似方法，可能需要 `consistent_read`。
        // 3. 如果是 Leader，直接从状态机查询数据。
        unimplemented!("将在这里实现 query 逻辑");
    }

    async fn status(&self) -> NodeStatus {
        // 实现细节:
        // 1. 调用 self.raft.metrics() 获取最新的节点状态。
        // 2. 从 metrics 中提取 is_leader, id 等信息。
        // 3. 组装并返回我们的 NodeStatus 结构体。
        unimplemented!("将在这里实现 status 逻辑");
    }
}