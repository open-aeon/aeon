//! This module implements the binary encoding for the key and value of records
//! stored in the `__consumer_offsets` topic. It is based on the format used
//! by Apache Kafka.

use bytes::{Buf, BufMut};
use crate::kafka::codec::{Encode, Decode};
use crate::error::protocol::Result;

/// The key of an offset commit message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OffsetKey {
    /// The version of the key schema. We'll use version 1.
    /// v0 is for Zookeeper-based offsets, v1 for Kafka-based.
    pub version: i16,
    /// The consumer group ID.
    pub group: String,
    /// The topic name.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
}

impl Encode for OffsetKey {
    fn encode(&self, dst: &mut impl BufMut, _api_version: i16) -> Result<()> {
        self.version.encode(dst, 0)?;
        self.group.encode(dst, 0)?;
        self.topic.encode(dst, 0)?;
        self.partition.encode(dst, 0)?;
        Ok(())
    }
}

impl Decode for OffsetKey {
    fn decode(src: &mut impl Buf, _api_version: i16) -> Result<Self> {
        let version = i16::decode(src, 0)?;
        let group = String::decode(src, 0)?;
        let topic = String::decode(src, 0)?;
        let partition = i32::decode(src, 0)?;
        Ok(OffsetKey {
            version,
            group,
            topic,
            partition,
        })
    }
}

/// The value of an offset commit message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetValue {
    /// The version of the value schema. We'll use version 3 to include leader epoch.
    pub version: i16,
    /// The offset being committed.
    pub offset: i64,
    /// The leader epoch of the record, used for fencing.
    pub leader_epoch: i32,
    /// Any client-specific metadata.
    pub metadata: String,
    /// The timestamp of the commit.
    pub commit_timestamp: i64,
}

impl Encode for OffsetValue {
    fn encode(&self, dst: &mut impl BufMut, _api_version: i16) -> Result<()> {
        self.version.encode(dst, 0)?;
        self.offset.encode(dst, 0)?;
        if self.version >= 3 {
            self.leader_epoch.encode(dst, 0)?;
        }
        self.metadata.encode(dst, 0)?;
        self.commit_timestamp.encode(dst, 0)?;
        Ok(())
    }
}

impl Decode for OffsetValue {
    fn decode(src: &mut impl Buf, _api_version: i16) -> Result<Self> {
        let version = i16::decode(src, 0)?;
        let offset = i64::decode(src, 0)?;
        let leader_epoch = if version >= 3 { i32::decode(src, 0)? } else { -1 };
        let metadata = String::decode(src, 0)?;
        let commit_timestamp = i64::decode(src, 0)?;

        Ok(OffsetValue {
            version,
            offset,
            leader_epoch,
            metadata,
            commit_timestamp,
        })
    }
}
