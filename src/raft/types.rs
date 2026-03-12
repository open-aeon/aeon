use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type NodeId = u64;
pub type GroupId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupKind {
    Meta,
    Data,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupSpec {
    pub group_id: GroupId,
    pub kind: GroupKind,
    pub members: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftEntry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotData {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub group_id: GroupId,
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<RaftEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteRequest {
    pub group_id: GroupId,
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub group_id: GroupId,
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: SnapshotData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftCommand {
    Meta(Vec<u8>),
    Data(Vec<u8>),
}

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("raft group not found: {0}")]
    GroupNotFound(GroupId),
    #[error("node is not leader of group: {0}")]
    NotLeader(GroupId),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("fsm error: {0}")]
    Fsm(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;

