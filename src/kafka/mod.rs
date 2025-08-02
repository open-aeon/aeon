pub mod codec;
pub mod message;
pub mod protocol;

use bytes::{Buf, BufMut};
use crate::error::protocol::{ProtocolError, Result};
use crate::kafka::codec::{Decode, Encode, Varint, CompactNullableString};
use crate::kafka::protocol::*;

// --- Request ---

#[derive(Debug, Clone, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
    match api_key {
        18 => api_version >= 3, // ApiVersions
        3  => api_version >= 9,  // Metadata
        0  => api_version >= 9,  // Produce
        _ => false, // Default to non-flexible for unknown keys
    }
}

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
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        self.api_key.encode(buf, api_version)?;
        self.api_version.encode(buf, api_version)?;
        self.correlation_id.encode(buf, api_version)?;
        if is_flexible_version(self.api_key, self.api_version) {
            CompactNullableString(self.client_id.clone()).encode(buf, api_version)?;
            0u32.encode_varint(buf);
        } else {
            // Kafka's nullable string encoding: length of -1 for null
            match &self.client_id {
                Some(s) => s.encode(buf, api_version)?,
                None => (-1i16).encode(buf, api_version)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum RequestType {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

#[derive(Debug)]
pub struct Request {
    pub header: RequestHeader,
    pub request_type: RequestType,
}


// --- Response ---

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    fn should_use_flexible_header(api_key: i16, api_version: i16) -> bool {
        match api_key {
            18 => api_version >= 3, // ApiVersions v3+ is flexible
            3  => api_version >= 9,  // Metadata v9+ is flexible
            0  => api_version >= 9,  // Produce v9+ is flexible
            _ => false,
        }
    }
    
    pub fn encode_with_version(&self, buf: &mut impl BufMut, api_key: i16, api_version: i16) -> Result<()> {
        self.correlation_id.encode(buf, api_version)?;
        
        if Self::should_use_flexible_header(api_key, api_version) {
            0u32.encode_varint(buf); // Tagged fields count
        }
        
        Ok(())
    }
}

#[derive(Debug)]
pub enum ResponseType {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
}

#[derive(Debug)]
pub struct Response {
    pub header: ResponseHeader,
    pub response_type: ResponseType,
    pub api_key: i16,
    pub api_version: i16,
}

impl Encode for Response {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        self.header.encode_with_version(buf, self.api_key, api_version)?;
        
        match &self.response_type {
            ResponseType::Produce(response) => response.encode(buf, api_version),
            ResponseType::Fetch(response) => response.encode(buf, api_version),
            ResponseType::ApiVersions(response) => response.encode(buf, self.api_version),
            ResponseType::Metadata(response) => response.encode(buf, api_version),
        }
    }
}
