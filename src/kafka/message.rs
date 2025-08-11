use bytes::{Buf, BufMut, Bytes};
use std::io::Cursor;
use varuint::{ReadVarint, WriteVarint};
use crc::{Crc, CRC_32_ISCSI};

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
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        let mut temp_record_buf = Vec::new();
        self.attributes.encode(&mut temp_record_buf, api_version)?;
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
    pub records_count: i32,
    pub records: Vec<Record>,
}

impl Encode for RecordBatch {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        let mut temp_buf = Vec::new();

        // Encode everything after batch_length
        self.leader_epoch.encode(&mut temp_buf, api_version)?;
        self.magic.encode(&mut temp_buf, api_version)?;

        let crc_placeholder_pos = temp_buf.len();
        0u32.encode(&mut temp_buf, api_version)?;

        self.attributes.encode(&mut temp_buf, api_version)?;
        self.last_offset_delta.encode(&mut temp_buf, api_version)?;
        self.first_timestamp.encode(&mut temp_buf, api_version)?;
        self.max_timestamp.encode(&mut temp_buf, api_version)?;
        self.producer_id.encode(&mut temp_buf, api_version)?;
        self.producer_epoch.encode(&mut temp_buf, api_version)?;
        self.base_sequence.encode(&mut temp_buf, api_version)?;
        self.records_count.encode(&mut temp_buf, api_version)?;

        for record in &self.records {
            record.encode(&mut temp_buf, api_version)?;
        }
        
        let batch_length = temp_buf.len() as i32;
        
        // CRC32C (Castagnoli) over everything after CRC field, i.e., starting from attributes
        let crc_data_start = crc_placeholder_pos + 4; // 4 bytes CRC field
        const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
        let crc = CRC32C.checksum(&temp_buf[crc_data_start..]);

        // Now write everything to the main buffer
        self.base_offset.encode(buf, api_version)?;
        batch_length.encode(buf, api_version)?;
        buf.put_slice(&temp_buf[..crc_placeholder_pos]);
        crc.encode(buf, api_version)?;
        buf.put_slice(&temp_buf[crc_data_start..]);

        Ok(())
    }
}

impl Decode for RecordBatch {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let base_offset = i64::decode(buf, api_version)?;
        let batch_length = i32::decode(buf, api_version)?;

        // batch_length不包含前面的 base_offset(8)+batch_length(4)，因此读取这么多字节即可
        let batch_buf = buf.copy_to_bytes(batch_length as usize);
        let mut batch_cursor = Cursor::new(&batch_buf);

        let leader_epoch = i32::decode(&mut batch_cursor, api_version)?;
        let magic = i8::decode(&mut batch_cursor, api_version)?;

        if magic != 2 {
            return Err(ProtocolError::UnsupportedMagicByte(magic));
        }

        let crc = u32::decode(&mut batch_cursor, api_version)?;

        // batch_buf starts at leader_epoch. Attributes start at offset 4 (leader_epoch)
        // + 1 (magic) + 4 (crc) = 9
        let crc_data = &batch_buf[9..];
        const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
        let computed_crc = CRC32C.checksum(crc_data);
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

        // 不再复制/解析 records 正文，避免越界/兼容压缩情况
        let records = Vec::new();

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
            records_count,
            records,
        })
    }
}

impl RecordBatch {

    pub fn parse_header(data: &Bytes) -> Result<Self> {
        if data.len() < 61 {
            return Err(ProtocolError::InvalidRecordBatchFormat);
        }

        // Big-endian (network order)
        let base_offset = i64::from_be_bytes(data[0..8].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("base_offset"))?);
        let batch_length = i32::from_be_bytes(data[8..12].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("batch_length"))?);
        let leader_epoch = i32::from_be_bytes(data[12..16].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("leader_epoch"))?);
        let magic = data[16] as i8;
        let crc = u32::from_be_bytes(data[17..21].try_into().map_err(|_e: std::array::TryFromSliceError| ProtocolError::InvalidRecordBatchCrc)?);
        let attributes = i16::from_be_bytes(data[21..23].try_into().map_err(|_e: std::array::TryFromSliceError| ProtocolError::InvalidRecordBatchField("attributes"))?);
        let last_offset_delta = i32::from_be_bytes(data[23..27].try_into().map_err(|_e: std::array::TryFromSliceError| ProtocolError::InvalidRecordBatchField("last_offset_delta"))?);
        let first_timestamp = i64::from_be_bytes(data[27..35].try_into().map_err(|_e: std::array::TryFromSliceError| ProtocolError::InvalidRecordBatchField("first_timestamp"))?);
        let max_timestamp = i64::from_be_bytes(data[35..43].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("max_timestamp"))?);
        let producer_id = i64::from_be_bytes(data[43..51].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("producer_id"))?);
        let producer_epoch = i16::from_be_bytes(data[51..53].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("producer_epoch"))?);
        let base_sequence = i32::from_be_bytes(data[53..57].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("base_sequence"))?);
        let records_count = i32::from_be_bytes(data[57..61].try_into().map_err(|_e| ProtocolError::InvalidRecordBatchField("records_count"))?);

        Ok(RecordBatch { 
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
            records_count,
            records: Vec::new(),
        })
    }

    pub fn verify_crc(&self, data: &Bytes) -> Result<()> {
        // For the full buffer (starting at base_offset), CRC covers from attributes onward
        let crc_data = &data[21..];
        const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
        let computed_crc = CRC32C.checksum(crc_data);
        if computed_crc != self.crc {
            return Err(ProtocolError::InvalidCrc);
        }
        Ok(())
    }
}