use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;
use crate::common::metadata::{TopicPartitionOffset, TopicPartition};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
    CreateTopic(CreateTopicRequest),
    CommitOffset(CommitOffsetRequest),
    FetchOffset(FetchOffsetRequest),
    JoinGroup(JoinGroupRequest),
    LeaveGroup(LeaveGroupRequest),
    Heartbeat(HeartbeatRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u32,
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataRequest {
    pub topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partition_num: u32,
    pub replication_factor: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOffsetRequest {
    pub group_id: String,
    pub topic_partitions: Vec<TopicPartitionOffset>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchOffsetRequest {
    pub group_id: String,
    pub topic_partitions: Vec<TopicPartition>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub member_id: String,
    pub session_timeout_ms: u64,
    pub protocol_type: String,
    pub protocols: Vec<Protocol>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Protocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
}