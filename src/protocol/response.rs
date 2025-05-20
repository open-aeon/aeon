use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    Metadata(MetadataResponse),
    CommitOffset(CommitOffsetResponse),
    JoinGroup(JoinGroupResponse),
    LeaveGroup(LeaveGroupResponse),
    Heartbeat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProduceResponse {
    pub topic: String,
    pub partition: i32,
    pub base_offset: u64,
    pub physical_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchResponse {
    pub topic: String,
    pub partition: i32,
    pub messages: Vec<Message>,
    pub next_offset: u64,
    pub group_id: String,
    pub consumer_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOffsetResponse {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    pub group_id: String,
    pub consumer_id: String,
    pub assigned_partitions: Vec<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveGroupResponse {
    pub group_id: String,
    pub consumer_id: String,
    pub error: Option<String>,
} 