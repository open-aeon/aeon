use async_trait::async_trait;

use crate::raft::types::{GroupId, GroupSpec, LogIndex, NodeId, RaftCommand, RaftResult};

#[async_trait]
pub trait RaftManager: Send + Sync {
    /// Create and bootstrap a raft group.
    async fn create_group(&self, spec: GroupSpec) -> RaftResult<()>;

    /// Remove a raft group from local runtime.
    async fn remove_group(&self, group_id: GroupId) -> RaftResult<()>;

    /// Propose one command and return committed log index.
    async fn propose(&self, group_id: GroupId, cmd: RaftCommand) -> RaftResult<LogIndex>;

    /// Linearizable read barrier.
    async fn read_index(&self, group_id: GroupId) -> RaftResult<LogIndex>;

    /// Current known leader for this group.
    async fn leader(&self, group_id: GroupId) -> RaftResult<Option<NodeId>>;

    /// Current members of this group.
    async fn members(&self, group_id: GroupId) -> RaftResult<Vec<NodeId>>;
}

