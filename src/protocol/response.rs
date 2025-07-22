use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    Metadata(MetadataResponse),
    CreateTopic(CreateTopicResponse),
    Heartbeat,
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
    pub topics: Vec<TopicMetadata>,
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
    // Data Errors
    DataCorruption = 30,
}