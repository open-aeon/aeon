use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::raft::fsm::{MetaStateMachine, StateMachine, empty_snapshot};
use crate::raft::types::{GroupId, LogIndex, NodeId, SnapshotData};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetaCommand {
    RegisterBroker {
        node_id: NodeId,
        addr: String,
    },
    CreateTopic {
        name: String,
        partitions: u32,
    },
    AssignPartition {
        topic: String,
        partition: u32,
        group_id: GroupId,
        replicas: Vec<NodeId>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaApplyResult;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSpec {
    pub partitions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub group_id: GroupId,
    pub replicas: Vec<NodeId>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MetaFsm {
    pub last_applied: LogIndex,
    pub brokers: HashMap<NodeId, String>,
    pub topics: HashMap<String, TopicSpec>,
    pub assignments: HashMap<(String, u32), PartitionAssignment>,
}

#[async_trait]
impl StateMachine for MetaFsm {
    type Command = MetaCommand;
    type Output = MetaApplyResult;

    async fn apply(&mut self, index: LogIndex, cmd: Self::Command) -> anyhow::Result<Self::Output> {
        match cmd {
            MetaCommand::RegisterBroker { node_id, addr } => {
                self.brokers.insert(node_id, addr);
            }
            MetaCommand::CreateTopic { name, partitions } => {
                self.topics.insert(name, TopicSpec { partitions });
            }
            MetaCommand::AssignPartition {
                topic,
                partition,
                group_id,
                replicas,
            } => {
                self.assignments.insert(
                    (topic, partition),
                    PartitionAssignment { group_id, replicas },
                );
            }
        }
        self.last_applied = index;
        Ok(MetaApplyResult)
    }

    async fn snapshot(&self) -> anyhow::Result<SnapshotData> {
        Ok(SnapshotData {
            meta: crate::raft::types::SnapshotMeta {
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

impl MetaStateMachine for MetaFsm {}

impl MetaFsm {
    pub fn empty_snapshot_data() -> SnapshotData {
        empty_snapshot()
    }
}

