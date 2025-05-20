use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
    CommitOffset(CommitOffsetRequest),
    JoinGroup(JoinGroupRequest),
    LeaveGroup(LeaveGroupRequest),
    Heartbeat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: i32,
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub max_bytes: u32,
    pub group_id: String,
    pub consumer_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataRequest {
    pub topics: Vec<String>,
    pub group_id: Option<String>,
    pub consumer_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOffsetRequest {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub consumer_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub consumer_id: String,
    pub topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub consumer_id: String,
} 