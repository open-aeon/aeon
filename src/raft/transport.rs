use async_trait::async_trait;

use crate::raft::rpc::pb;
use crate::raft::types::{NodeId, RaftResult};

#[async_trait]
pub trait RaftTransport: Send + Sync {
    async fn send_append_entries(
        &self,
        target: NodeId,
        req: pb::AppendEntriesRequest,
    ) -> RaftResult<pb::AppendEntriesResponse>;

    async fn send_vote(&self, target: NodeId, req: pb::VoteRequest) -> RaftResult<pb::VoteResponse>;

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: pb::InstallSnapshotRequest,
    ) -> RaftResult<pb::InstallSnapshotResponse>;
}

