use bytes::{Buf, BufMut};
use std::collections::HashMap;

use crate::kafka::codec::{CompactVec, Encode, Varint};
use crate::error::protocol::Result;

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    // 为不同的API选择正确的header版本
    fn should_use_flexible_header(api_key: i16, api_version: i16) -> bool {
        match api_key {
            18 => false, // ApiVersions - 总是使用v0 header (非flexible)
            3 => api_version >= 9,   // Metadata - v9+使用flexible
            0 => api_version >= 9,   // Produce - v9+使用flexible
            _ => false,
        }
    }
    
    pub fn encode_with_version(&self, buf: &mut impl BufMut, api_key: i16, api_version: i16) -> Result<()> {
        self.correlation_id.encode(buf)?;
        
        // 只有flexible版本才包含tagged fields
        if Self::should_use_flexible_header(api_key, api_version) {
            0u32.encode_varint(buf);
        }
        
        Ok(())
    }
}

impl Encode for ResponseHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        // 默认编码（向后兼容），不包含tagged fields
        self.correlation_id.encode(buf)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResponseType {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
    Produce(ProduceResponse),
}

#[derive(Debug, Clone)]
pub struct Response {
    pub header: ResponseHeader,
    pub response_type: ResponseType,
    pub api_key: i16,
    pub api_version: i16,
}

impl Encode for Response {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        // 使用正确的header版本进行编码
        self.header.encode_with_version(buf, self.api_key, self.api_version)?;
        
        match &self.response_type {
            ResponseType::ApiVersions(response) => {
                // 传递API版本信息给ApiVersionsResponse
                response.encode_with_version(buf, self.api_version)?;
                Ok(())
            },
            ResponseType::Metadata(response) => response.encode(buf),
            ResponseType::Produce(response) => response.encode(buf),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,  // 改为普通Vec，稍后根据版本决定编码方式
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn encode_with_version(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        self.error_code.encode(buf)?;
        
        // 根据API版本选择数组编码格式
        if api_version >= 3 {
            // flexible版本：使用CompactVec编码
            let compact_api_keys = CompactVec(self.api_keys.clone());
            compact_api_keys.encode(buf)?;
        } else {
            // 非flexible版本：使用普通数组编码
            (self.api_keys.len() as i32).encode(buf)?;
            for api_key in &self.api_keys {
                api_key.encode_with_version(buf, api_version)?;
            }
        }
        
        self.throttle_time_ms.encode(buf)?;
        
        // 只有flexible版本才有tagged_fields
        if api_version >= 3 {
            0u32.encode_varint(buf);
        }
        
        Ok(())
    }
}

impl Encode for ApiVersionsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        // 默认使用flexible版本编码（为了向后兼容）
        self.encode_with_version(buf, 3)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiKey {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiKey {
    pub fn encode_with_version(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        self.key.encode(buf)?;
        self.min_version.encode(buf)?;
        self.max_version.encode(buf)?;
        
        // 只有flexible版本才有tagged_fields
        if api_version >= 3 {
            0u32.encode_varint(buf);
        }
        
        Ok(())
    }
}

impl Encode for ApiKey {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        // 默认使用flexible版本编码（为了向后兼容）
        self.encode_with_version(buf, 3)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<Broker>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<Topic>,
}

impl Encode for MetadataResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.throttle_time_ms.encode(buf)?;
        self.brokers.encode(buf)?;
        self.cluster_id.encode(buf)?;
        self.controller_id.encode(buf)?;
        self.topics.encode(buf)?;
        // This response version (e.g. v5) might have tagged fields
        0u32.encode_varint(buf); 
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl Encode for Broker {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.node_id.encode(buf)?;
        self.host.encode(buf)?;
        self.port.encode(buf)?;
        self.rack.encode(buf)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Topic {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
}

impl Encode for Topic {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.name.encode(buf)?;
        self.is_internal.encode(buf)?;
        self.partitions.encode(buf)?;
        // Tagged fields
        0u32.encode_varint(buf);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Partition {
    pub error_code: i16,
    pub partition_index : i16,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl Encode for Partition {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.error_code.encode(buf)?;
        self.partition_index.encode(buf)?;
        self.leader_id.encode(buf)?;
        self.leader_epoch.encode(buf)?;
        self.replica_nodes.encode(buf)?;
        self.isr_nodes.encode(buf)?;
        self.offline_replicas.encode(buf)?;
        // Tagged fields for flexible versions would go here
        0u32.encode_varint(buf);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProduceResponse {
    pub topic_data: HashMap<String, TopicProduceResponse>,
    pub throttle_time_ms: i32,
}

impl Encode for ProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topic_data.encode(buf)?;
        self.throttle_time_ms.encode(buf)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicProduceResponse {
    pub partitions: Vec<PartitionProduceResponse>,
}

impl Encode for TopicProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partitions.encode(buf)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartitionProduceResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

impl Encode for PartitionProduceResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.base_offset.encode(buf)?;
        self.log_append_time_ms.encode(buf)?;
        self.log_start_offset.encode(buf)?;
        // Tagged fields
        0u32.encode_varint(buf);
        Ok(())
    }
}