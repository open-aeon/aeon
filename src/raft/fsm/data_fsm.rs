use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::metadata::TopicPartition;
use crate::raft::fsm::{DataStateMachine, StateMachine};
use crate::raft::types::{LogIndex, SnapshotData, SnapshotMeta};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataCommand {
    AppendBatch {
        tp: TopicPartition,
        payload: Vec<u8>,
        record_count: u32,
    },
    CommitOffset {
        group_id: String,
        tp: TopicPartition,
        offset: i64,
        ts: i64,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataApplyResult {
    pub assigned_offset: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataFsm {
    pub last_applied: LogIndex,
    pub next_offsets: HashMap<TopicPartition, i64>,
}

#[async_trait]
impl StateMachine for DataFsm {
    type Command = DataCommand;
    type Output = DataApplyResult;

    async fn apply(&mut self, index: LogIndex, cmd: Self::Command) -> anyhow::Result<Self::Output> {
        let output = match cmd {
            DataCommand::AppendBatch { tp, record_count, .. } => {
                let next = self.next_offsets.entry(tp).or_insert(0);
                let assigned = *next;
                *next += record_count as i64;
                DataApplyResult {
                    assigned_offset: Some(assigned),
                }
            }
            DataCommand::CommitOffset { .. } => DataApplyResult::default(),
        };

        self.last_applied = index;
        Ok(output)
    }

    async fn snapshot(&self) -> anyhow::Result<SnapshotData> {
        Ok(SnapshotData {
            meta: SnapshotMeta {
                last_included_index: self.last_applied,
                last_included_term: 0,
            },
            data: bincode::serialize(self)?,
        })
    }

    async fn restore(&mut self, snap: SnapshotData) -> anyhow::Result<()> {
        if snap.data.is_empty() {
            *self = Self::default();
            return Ok(());
        }
        let restored: Self = bincode::deserialize(&snap.data)?;
        *self = restored;
        Ok(())
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

impl DataStateMachine for DataFsm {}

