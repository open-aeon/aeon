use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::raft::rpc::pb;
use crate::raft::transport::RaftTransport;
use crate::raft::types::{NodeId, RaftError, RaftResult};

#[async_trait]
pub trait RaftRpcHandler: Send + Sync {
    async fn handle_append_entries(
        &self,
        req: pb::AppendEntriesRequest,
    ) -> RaftResult<pb::AppendEntriesResponse>;
    async fn handle_vote(&self, req: pb::VoteRequest) -> RaftResult<pb::VoteResponse>;
    async fn handle_install_snapshot(
        &self,
        req: pb::InstallSnapshotRequest,
    ) -> RaftResult<pb::InstallSnapshotResponse>;
}

#[derive(Default)]
pub struct LocalRaftTransport {
    handlers: DashMap<NodeId, Arc<dyn RaftRpcHandler>>,
}

impl LocalRaftTransport {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_handler(&self, node_id: NodeId, handler: Arc<dyn RaftRpcHandler>) {
        self.handlers.insert(node_id, handler);
    }

    pub fn remove_handler(&self, node_id: NodeId) {
        self.handlers.remove(&node_id);
    }
}

#[async_trait]
impl RaftTransport for LocalRaftTransport {
    async fn send_append_entries(
        &self,
        target: NodeId,
        req: pb::AppendEntriesRequest,
    ) -> RaftResult<pb::AppendEntriesResponse> {
        let Some(h) = self.handlers.get(&target) else {
            return Err(RaftError::Transport(format!("append_entries target not found: {target}")));
        };
        h.handle_append_entries(req).await
    }

    async fn send_vote(&self, target: NodeId, req: pb::VoteRequest) -> RaftResult<pb::VoteResponse> {
        let Some(h) = self.handlers.get(&target) else {
            return Err(RaftError::Transport(format!("vote target not found: {target}")));
        };
        h.handle_vote(req).await
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: pb::InstallSnapshotRequest,
    ) -> RaftResult<pb::InstallSnapshotResponse> {
        let Some(h) = self.handlers.get(&target) else {
            return Err(RaftError::Transport(format!("snapshot target not found: {target}")));
        };
        h.handle_install_snapshot(req).await
    }
}
