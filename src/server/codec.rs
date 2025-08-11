use bytes::{BytesMut, BufMut};
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::protocol::ProtocolError;
use crate::kafka::codec::{Decode, Encode};
use crate::kafka::{
    protocol::*,
    Request, RequestHeader, RequestType, Response,
};


#[derive(Default)]
pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Request;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[0..4]);
        let len = i32::from_be_bytes(len_bytes) as usize;

        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        let body_buf = src.split_to(4 + len);
        let mut cursor = Cursor::new(&body_buf[4..]);

        let err_convert = |e: ProtocolError| {
            eprintln!("Protocol error: {:?}", e);
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        };

        // println!("Decoding request, length: {}", len);
        // println!("Body data: {:02x?}", &body_buf[4..]);

        // Robust header decode with flexible/legacy fallback
        let data = &body_buf[4..];
        let mut try_flex = || -> Result<(RequestHeader, usize), ProtocolError> {
            use crate::kafka::codec::{Decode, CompactNullableString, Varint};
            let mut cur = Cursor::new(data);
            let api_key = i16::decode(&mut cur, 0)?;
            let api_version = i16::decode(&mut cur, 0)?;
            let correlation_id = i32::decode(&mut cur, 0)?;
            let client_id = CompactNullableString::decode(&mut cur, api_version)?.0;
            // header tagged fields
            let tagged_fields_count = u32::decode_varint(&mut cur)?;
            for _ in 0..tagged_fields_count {
                let _tag = u32::decode_varint(&mut cur)?;
                let size = u32::decode_varint(&mut cur)? as usize;
                if cur.get_ref().len() < (cur.position() as usize + size) {
                    return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes to skip header tagged field")));
                }
                let _ = cur.set_position(cur.position() + size as u64);
            }
            Ok((RequestHeader { api_key, api_version, correlation_id, client_id }, cur.position() as usize))
        };

        let mut try_legacy = || -> Result<(RequestHeader, usize), ProtocolError> {
            use crate::kafka::codec::Decode;
            let mut cur = Cursor::new(data);
            let api_key = i16::decode(&mut cur, 0)?;
            let api_version = i16::decode(&mut cur, 0)?;
            let correlation_id = i32::decode(&mut cur, 0)?;
            let client_id = <Option<String> as Decode>::decode(&mut cur, api_version)?;
            Ok((RequestHeader { api_key, api_version, correlation_id, client_id }, cur.position() as usize))
        };

        // Prefer flexible first, fallback to legacy; if flexible失败或越界则回退
        let (header, header_len) = match try_flex() {
            Ok(h) => h,
            Err(_) => try_legacy().map_err(err_convert)?,
        };
        // reset cursor to after header for body decoding
        let mut cursor = Cursor::new(&data[header_len..]);
        let api_version = header.api_version;

        println!("Decoded header: api_key={}, api_version={}, correlation_id={}, client_id={:?}", 
                 header.api_key, header.api_version, header.correlation_id, header.client_id);

        let request_type = match header.api_key {
            0 => ProduceRequest::decode(&mut cursor, api_version).map(RequestType::Produce),
            1 => FetchRequest::decode(&mut cursor, api_version).map(RequestType::Fetch),
            2 => ListOffsetsRequest::decode(&mut cursor, api_version).map(RequestType::ListOffsets),
            3 => MetadataRequest::decode(&mut cursor, api_version).map(RequestType::Metadata),
            18 => ApiVersionsRequest::decode(&mut cursor, api_version).map(RequestType::ApiVersions),
            _ => return Err(err_convert(ProtocolError::UnknownApiKey(header.api_key))),
        }.map_err(err_convert)?;

        // println!("Successfully decoded request type: {:?}", request_type);

        Ok(Some(Request { header, request_type }))
    }
}

impl Encoder<Response> for ServerCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // 1. Reserve space for the 4-byte length prefix.
        dst.reserve(4);
        // We can't write to it yet, so we advance the buffer's internal cursor.
        // This is safe because we just reserved the space.
        unsafe {
            dst.advance_mut(4);
        }

        // 2. Encode the response payload directly into the destination buffer.
        // `dst` is already a `&mut BytesMut`, which implements `BufMut`.
        item.encode(dst, item.api_version)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // 3. Now, calculate the length of the payload we just wrote.
        // The length of the buffer minus the 4 bytes we skipped at the start.
        let len = dst.len() - 4;

        // 4. Go back to the beginning of the buffer and write the calculated length.
        // We get a mutable slice of the first 4 bytes.
        let mut len_slice = &mut dst[..4];
        // And use the `put_i32` method from `BufMut` to write the length.
        len_slice.put_i32(len as i32);

        Ok(())
    }
}