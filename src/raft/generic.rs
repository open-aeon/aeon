use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct NodeStatus {
    pub id: u64,
    pub is_leader: bool,
}


#[async_trait]
pub trait GenericRaftNode: Send + Sync + 'static {
    /// Submit a write request (Command/Proposal) to the Raft Group.
    ///
    /// This request will be replicated to all nodes and applied to the state machine after consensus is reached.
    /// Only the Leader node can successfully process this request.
    async fn propose(&self, request: Request) -> Result<Response, Box<dyn Error>>;

    /// Submit a read request (Query) to the Raft Group.
    ///
    /// This request is typically handled directly by the Leader and requires linearizable read guarantees.
    /// The underlying implementation may use Leader Lease or `ReadIndex` mechanisms to optimize performance.
    async fn query(&self, request: Request) -> Result<Response, Box<dyn Error>>;

    /// Get the current status of the node in the Raft Group.
    async fn status(&self) -> NodeStatus;

    // TODO: Add more management interfaces in the future.
    //
}