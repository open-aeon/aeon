use async_trait::async_trait;

use crate::raft::types::{LogIndex, SnapshotData, SnapshotMeta};

pub mod meta_fsm;
pub mod data_fsm;

#[async_trait]
pub trait StateMachine: Send + Sync {
    type Command: Send + Sync + 'static;
    type Output: Send + Sync + 'static;

    /// Apply a committed command at `index`.
    async fn apply(&mut self, index: LogIndex, cmd: Self::Command) -> anyhow::Result<Self::Output>;

    /// Build a point-in-time snapshot of current state.
    async fn snapshot(&self) -> anyhow::Result<SnapshotData>;

    /// Restore state from snapshot.
    async fn restore(&mut self, snap: SnapshotData) -> anyhow::Result<()>;

    /// Last applied index for this FSM.
    fn last_applied(&self) -> LogIndex;
}

pub trait MetaStateMachine: StateMachine {}
pub trait DataStateMachine: StateMachine {}

pub fn empty_snapshot() -> SnapshotData {
    SnapshotData {
        meta: SnapshotMeta {
            last_included_index: 0,
            last_included_term: 0,
        },
        data: Vec::new(),
    }
}

