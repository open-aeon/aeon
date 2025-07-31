use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::read::GzDecoder;
use std::io::{Cursor, Read, Write};
use varuint::{ReadVarint, WriteVarint};

use crate::error::protocol::{ProtocolError, Result};
use crate::kafka::codec::{Decode, Encode};

// --- Helper functions for Varint encoding/decoding using the `varuint` crate ---
fn encode_varint_i64(val: i64, buf: &mut impl BufMut) {
    let mut writer = buf.writer();
    // Zigzag encoding for signed integers
    let z_val = (val << 1) ^ (val >> 63);
    writer.write_varint(z_val as u64).unwrap();
}

fn decode_varint_i64(buf: &mut impl Buf) -> Result<i64> {
    let mut reader = buf.reader();
    let z_val: u64 = reader.read_varint()?;
    // Zigzag decoding
    Ok((z_val >> 1) as i64 ^ -((z_val & 1) as i64))
}


// --- Record: A single message inside a batch ---
#[derive(Debug, Clone, Default)]
pub struct Record {
    pub length: i64, // Varint
    pub attributes: i8,
    pub timestamp_delta: i64, // Varint
    pub offset_delta: i64,    // Varint
    pub key: Bytes,
    pub value: Bytes,
    // Headers are omitted for simplicity
}

impl Encode for Record {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let mut temp_record_buf = Vec::new();
        self.attributes.encode(&mut temp_record_buf)?;
        encode_varint_i64(self.timestamp_delta, &mut temp_record_buf);
        encode_varint_i64(self.offset_delta, &mut temp_record_buf);

        encode_varint_i64(self.key.len() as i64, &mut temp_record_buf);
        temp_record_buf.put_slice(&self.key);

        encode_varint_i64(self.value.len() as i64, &mut temp_record_buf);
        temp_record_buf.put_slice(&self.value);

        // For now, we assume zero headers.
        encode_varint_i64(0, &mut temp_record_buf);

        encode_varint_i64(temp_record_buf.len() as i64, buf);
        buf.put_slice(&temp_record_buf);
        
        Ok(())
    }
}

impl Decode for Record {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let length = decode_varint_i64(buf)?;
        
        let mut record_buf = buf.copy_to_bytes(length as usize);
        
        let attributes = i8::decode(&mut record_buf, api_version)?;
        let timestamp_delta = decode_varint_i64(&mut record_buf)?;
        let offset_delta = decode_varint_i64(&mut record_buf)?;
        
        let key_len = decode_varint_i64(&mut record_buf)? as usize;
        let key = record_buf.copy_to_bytes(key_len);

        let value_len = decode_varint_i64(&mut record_buf)? as usize;
        let value = record_buf.copy_to_bytes(value_len);

        let headers_count = decode_varint_i64(&mut record_buf)?;
        for _ in 0..headers_count {
            let header_key_len = decode_varint_i64(&mut record_buf)? as usize;
            record_buf.advance(header_key_len); // Skip header key
            let header_value_len = decode_varint_i64(&mut record_buf)? as usize;
            record_buf.advance(header_value_len); // Skip header value
        }

        Ok(Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub leader_epoch: i32,
    pub magic: i8, // Should be 2 for modern Kafka
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Encode for RecordBatch {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let mut temp_buf = Vec::new();

        // Encode everything after batch_length
        self.leader_epoch.encode(&mut temp_buf)?;
        self.magic.encode(&mut temp_buf)?;

        let crc_placeholder_pos = temp_buf.len();
        0u32.encode(&mut temp_buf)?;

        self.attributes.encode(&mut temp_buf)?;
        self.last_offset_delta.encode(&mut temp_buf)?;
        self.first_timestamp.encode(&mut temp_buf)?;
        self.max_timestamp.encode(&mut temp_buf)?;
        self.producer_id.encode(&mut temp_buf)?;
        self.producer_epoch.encode(&mut temp_buf)?;
        self.base_sequence.encode(&mut temp_buf)?;
        (self.records.len() as i32).encode(&mut temp_buf)?;

        for record in &self.records {
            record.encode(&mut temp_buf)?;
        }
        
        let batch_length = temp_buf.len() as i32;
        
        let crc_data_start = crc_placeholder_pos + 4;
        let crc = crc32fast::hash(&temp_buf[crc_data_start..]);

        // Now write everything to the main buffer
        self.base_offset.encode(buf)?;
        batch_length.encode(buf)?;
        buf.put_slice(&temp_buf[..crc_placeholder_pos]);
        crc.encode(buf)?;
        buf.put_slice(&temp_buf[crc_data_start..]);

        Ok(())
    }
}

impl Decode for RecordBatch {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let base_offset = i64::decode(buf, api_version)?;
        let batch_length = i32::decode(buf, api_version)?;

        let mut batch_buf = buf.copy_to_bytes(batch_length as usize);
        let mut batch_cursor = Cursor::new(&batch_buf);

        let leader_epoch = i32::decode(&mut batch_cursor, api_version)?;
        let magic = i8::decode(&mut batch_cursor, api_version)?;

        if magic != 2 {
            return Err(ProtocolError::UnsupportedMagicByte(magic));
        }

        let crc = u32::decode(&mut batch_cursor, api_version)?;
        
        let crc_data = &batch_buf[17..]; // 17 = pos of attributes
        let computed_crc = crc32fast::hash(crc_data);
        if computed_crc != crc {
             return Err(ProtocolError::InvalidCrc);
        }

        let attributes = i16::decode(&mut batch_cursor, api_version)?;
        let last_offset_delta = i32::decode(&mut batch_cursor, api_version)?;
        let first_timestamp = i64::decode(&mut batch_cursor, api_version)?;
        let max_timestamp = i64::decode(&mut batch_cursor, api_version)?;
        let producer_id = i64::decode(&mut batch_cursor, api_version)?;
        let producer_epoch = i16::decode(&mut batch_cursor, api_version)?;
        let base_sequence = i32::decode(&mut batch_cursor, api_version)?;
        let records_count = i32::decode(&mut batch_cursor, api_version)?;

        let records_bytes = batch_cursor.copy_to_bytes(batch_cursor.remaining());
        
        let mut records = Vec::with_capacity(records_count as usize);
        if records_count > 0 {
            let mut records_cursor = Cursor::new(records_bytes);
            for _ in 0..records_count {
                records.push(Record::decode(&mut records_cursor, api_version)?);
            }
        }

        Ok(Self {
            base_offset,
            batch_length,
            leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}