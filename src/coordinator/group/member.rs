use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ConsumerMember {
    pub id: String,
    pub group_instance_id: Option<String>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration, // todo: 为什么要这个字段？
    pub topics: Vec<String>,
    pub supported_protocols: Vec<(String,Vec<u8>)>,
    pub last_heartbeat: Instant,
    pub assignment: Vec<(String, Vec<u32>)>,
}
