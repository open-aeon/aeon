use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{Request, Response};

#[derive(Default)]
pub struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = src.get_u32() as usize;
        if src.len() < len {
            return Ok(None);
        }

        let bytes = src.split_to(len);
        match bincode::deserialize(&bytes) {
            Ok(response) => Ok(Some(response)),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize response: {}", e),
            )),
        }
    }
}

impl Encoder<Request> for ClientCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize request: {}", e),
            )
        })?;

        dst.put_u32(bytes.len() as u32);
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

#[derive(Default)]
pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = src.get_u32() as usize;
        if src.len() < len {
            return Ok(None);
        }

        let bytes = src.split_to(len);
        match bincode::deserialize(&bytes) {
            Ok(request) => Ok(Some(request)),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize request: {}", e),
            )),
        }
    }
}

impl Encoder<Response> for ServerCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize response: {}", e),
            )
        })?;

        dst.put_u32(bytes.len() as u32);
        dst.extend_from_slice(&bytes);
        Ok(())
    }
} 