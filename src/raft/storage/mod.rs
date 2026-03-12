use async_trait::async_trait;

use crate::raft::types::{GroupId, HardState, LogIndex, RaftEntry, RaftResult, SnapshotData};

pub mod wal;
pub mod hard_state;
pub mod snapshot;

#[async_trait]
pub trait RaftLogStore: Send + Sync {
    async fn append(&self, group_id: GroupId, entries: &[RaftEntry]) -> RaftResult<()>;
    async fn entry(&self, group_id: GroupId, index: LogIndex) -> RaftResult<Option<RaftEntry>>;
    async fn entries(&self, group_id: GroupId, start: LogIndex, end: LogIndex) -> RaftResult<Vec<RaftEntry>>;
    async fn last_index(&self, group_id: GroupId) -> RaftResult<LogIndex>;
    async fn truncate_suffix(&self, group_id: GroupId, from: LogIndex) -> RaftResult<()>;
}

#[async_trait]
pub trait HardStateStore: Send + Sync {
    async fn load(&self, group_id: GroupId) -> RaftResult<Option<HardState>>;
    async fn save(&self, group_id: GroupId, hs: &HardState) -> RaftResult<()>;
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn load_latest(&self, group_id: GroupId) -> RaftResult<Option<SnapshotData>>;
    async fn save_latest(&self, group_id: GroupId, snapshot: &SnapshotData) -> RaftResult<()>;
}

