use bytes::{Buf, BufMut};
use std::collections::HashMap;

use crate::error::protocol::{ProtocolError, Result};
use crate::kafka::{
    codec::{CompactNullableString, CompactString, Decode, Encode, Varint},
    message::RecordBatch,
};

#[derive(Debug, Clone, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    //todo: fill other fields
}

fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
    // This is a simplified check. A complete implementation would have the
    // exact flexible version ranges for each API key.
    match api_key {
        18 => api_version >= 3, // ApiVersions
        3 => api_version >= 9,  // Metadata
        0 => api_version >= 9,  // Produce
        _ => false,
    }
}

// Special handling for header decode, as it doesn't have the version itself.
impl RequestHeader {
    pub fn decode_header(buf: &mut impl Buf) -> Result<Self> {
        let api_key = i16::decode(buf, 0)?;
        let api_version = i16::decode(buf, 0)?;
        let correlation_id = i32::decode(buf, 0)?;

        let client_id = if is_flexible_version(api_key, api_version) {
            let val = CompactNullableString::decode(buf, api_version)?;
            let tagged_fields_count = u32::decode_varint(buf)?;
            if tagged_fields_count > 0 {
                return Err(ProtocolError::InvalidTaggedField);
            }
            val.0
        } else {
            let len = i16::decode(buf, api_version)?;
            if len < 0 {
                None
            } else {
                let len = len as usize;
                if buf.remaining() < len {
                     return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Not enough bytes for client_id string",
                    )));
                }
                let mut bytes = vec![0u8; len];
                buf.copy_to_slice(&mut bytes);
                Some(String::from_utf8(bytes).map_err(|e| {
                    ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?)
            }
        };

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

impl Encode for RequestHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf)?;
        self.api_version.encode(buf)?;
        self.correlation_id.encode(buf)?;
        if is_flexible_version(self.api_key, self.api_version) {
            CompactNullableString(self.client_id.clone()).encode(buf)?;
            0u32.encode_varint(buf);
        } else {
            self.client_id.encode(buf)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum RequestType {
    Produce(ProduceRequest),
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

pub struct Request {
    pub header: RequestHeader,
    pub request_type: RequestType,
}

#[derive(Debug, Clone, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: CompactString,
    pub client_software_version: CompactString,
}

impl Encode for ApiVersionsRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.client_software_name.encode(buf)?;
        self.client_software_version.encode(buf)?;
        // Tagged Fields
        0u32.encode_varint(buf);
        Ok(())
    }
}

impl Decode for ApiVersionsRequest {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        if api_version >= 3 {
            let client_software_name = CompactString::decode(buf, api_version)?;
            let client_software_version = CompactString::decode(buf, api_version)?;
            let tagged_fields_count = u32::decode_varint(buf)?;
            if tagged_fields_count > 0 {
                return Err(ProtocolError::InvalidTaggedField);
            }
            Ok(Self {
                client_software_name,
                client_software_version,
            })
        } else {
            // Versions 0-2 have an empty request body
            Ok(Self::default())
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
}

impl Encode for MetadataRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.topics.encode(buf)
    }
}

impl Decode for MetadataRequest {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        Ok(Self {
            topics: Option::<Vec<String>>::decode(buf, api_version)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: HashMap<String, TopicProduceData>,
}

impl Encode for ProduceRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.acks.encode(buf)?;
        self.timeout_ms.encode(buf)?;
        self.topics.encode(buf)
    }
}

impl Decode for ProduceRequest {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        Ok(Self {
            acks: i16::decode(buf, api_version)?,
            timeout_ms: i32::decode(buf, api_version)?,
            topics: HashMap::decode(buf, api_version)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicProduceData {
    pub partitions: Vec<PartitionProduceData>,
}

impl Encode for TopicProduceData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.partitions.encode(buf)
    }
}

impl Decode for TopicProduceData {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        Ok(Self {
            partitions: Vec::decode(buf, api_version)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartitionProduceData {
    pub index: i32,
    pub records: RecordBatch,
}

impl Encode for PartitionProduceData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.index.encode(buf)?;
        self.records.encode(buf)
    }
}

impl Decode for PartitionProduceData {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        Ok(Self {
            index: i32::decode(buf, api_version)?,
            records: RecordBatch::decode(buf, api_version)?,
        })
    }
}