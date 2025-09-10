use serde::{Deserialize, Serialize};

// Raft type definitions for raft-rs integration
// These types define the core data structures used throughout our Raft implementation

/// Node ID type - uniquely identifies each node in the cluster
pub type NodeId = u64;

/// Application data type - the payload that gets replicated through Raft
/// We use Vec<u8> for maximum flexibility, allowing upper layers to serialize
/// any data structure they need
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppData {
    pub payload: Vec<u8>,
}

/// Application response type - returned after applying entries to the state machine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppResponse {
    pub result: Vec<u8>,
}

/// Snapshot data structure for raft-rs
/// Contains the serialized state machine data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotData {
    pub data: Vec<u8>,
    pub meta: SnapshotMeta,
}

/// Snapshot metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub nodes: Vec<NodeId>,
}

/// Configuration for a Raft node
#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub id: NodeId,
    pub addr: String,
    pub peers: Vec<(NodeId, String)>,
}


