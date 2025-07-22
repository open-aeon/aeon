use serde::{Deserialize, Serialize};
use crate::protocol::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
    CreateTopic(CreateTopicRequest),
    Heartbeat,
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

