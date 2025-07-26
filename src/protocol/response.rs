use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    Metadata(MetadataResponse),
    CreateTopic(CreateTopicResponse),
    CommitOffset(CommitOffsetResponse),
    FetchOffset(FetchOffsetResponse),
    JoinGroup(JoinGroupResponse),
    LeaveGroup(LeaveGroupResponse),
    Heartbeat(HeartbeatResponse),
    Error(ErrorResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProduceResponse {
    pub topic: String,
    pub partition: u32,
    pub base_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchResponse {
    pub topic: String,
    pub partition: u32,
    pub messages: Vec<Message>,
    pub next_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub id: u32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub id: u32,
    pub leader: u32,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOffsetResponse {
    pub results: Vec<TopicPartitionResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicPartitionResult {
    pub topic: String,
    pub partition: u32,
    pub error_code: Option<ErrorCode>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct FetchOffsetResponse {
    pub results: Vec<TopicPartitionOffsetResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicPartitionOffsetResult {
    pub topic: String,
    pub partition: u32,
    pub offset: i64,
    pub error_code: Option<ErrorCode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    pub error_code: Option<ErrorCode>,
    pub result: JoinGroupResult,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinGroupResult {
    pub generation_id: u32,
    pub member_id: String,
    pub members: Vec<MemberInfo>,
    pub protocol: Option<String>,
    pub leader_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberInfo {
    pub id: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveGroupResponse {
    pub error_code: Option<ErrorCode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub error_code: Option<ErrorCode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ErrorCode {
    Unknown = 0,
    // Topic Errors
    TopicNotFound = 1,
    TopicAlreadyExists = 2,
    // Partition Errors
    PartitionNotFound = 10,
    // Offset Errors
    OffsetOutOfRange = 20,
    OffsetNotFound = 21,
    // Data Errors
    DataCorruption = 30,
}