use bytes::{Buf, BufMut};
use std::collections::HashMap;

use crate::error::protocol::{ProtocolError, Result};

#[derive(Debug, Clone, Default)]
pub struct CompactVec<T>(pub Vec<T>);

/// Variable-length integer encoding
pub trait Varint: Sized {
    fn encode_varint(&self, buf: &mut impl BufMut);
    fn decode_varint(buf: &mut impl Buf) -> Result<Self>;
}

pub trait Encode: Sized {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()>;
    fn encode_to_vec(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode(&mut buf)?;
        Ok(buf)
    }
}

pub trait Decode: Sized {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self>;
}

impl Encode for i8 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i8(*self);
        Ok(())
    }
}

impl Decode for i8 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_i8())
    }
}

impl Encode for u8 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(*self);
        Ok(())
    }
}

impl Decode for u8 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_u8())
    }
}

impl Encode for i16 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i16(*self);
        Ok(())
    }
}

impl Decode for i16 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_i16())
    }
}

impl Encode for u16 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u16(*self);
        Ok(())
    }
}

impl Decode for u16 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_u16())
    }
}

impl Encode for i32 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(*self);
        Ok(())
    }
}

impl Decode for i32 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_i32())
    }
}

impl Encode for u32 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u32(*self);
        Ok(())
    }
}

impl Decode for u32 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_u32())
    }
}

impl Encode for i64 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i64(*self);
        Ok(())
    }
}

impl Decode for i64 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_i64())
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u64(*self);
        Ok(())
    }
}

impl Decode for u64 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_u64())
    }
}

impl Encode for f32 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_f32(*self);
        Ok(())
    }
}

impl Decode for f32 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_f32())
    }
}

impl Encode for f64 {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_f64(*self);
        Ok(())
    }
}

impl Decode for f64 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(buf.get_f64())
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = self.len() as i16;
        len.encode(buf)?;
        buf.put_slice(self.as_bytes());
        Ok(())
    }
}

impl Decode for String {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let len = i16::decode(buf, 0)?;
        if len < 0 {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "String length cannot be negative",
            )));
        }
        let len = len as usize;
        if buf.remaining() < len {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for String",
            )));
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).map_err(|e| {
            ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = self.len() as i32;
        len.encode(buf)?;
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len < 0 {
            return Ok(Vec::new());
        }
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            vec.push(T::decode(buf, api_version)?);
        }
        Ok(vec)
    }
}

impl Encode for () {
    fn encode(&self, _: &mut impl BufMut) -> Result<()> {
        Ok(())
    }
}

impl Decode for () {
    fn decode(_: &mut impl Buf, _: i16) -> Result<Self> {
        Ok(())
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Some(value) => value.encode(buf),
            None => (-1i16).encode(buf),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        // This is a simplified implementation for NullableString.
        // A full implementation would need to handle other nullable types.
        let mut temp_buf = buf.chunk();
        let len = i16::from_be_bytes(temp_buf[..2].try_into().unwrap_or([0,0]));

        if len < 0 {
            // Consume the length
            i16::decode(buf, api_version)?;
            Ok(None)
        } else {
            Ok(Some(T::decode(buf, api_version)?))
        }
    }
}


impl<K: Encode, V: Encode> Encode for HashMap<K, V> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.len() as i32).encode(buf)?;
        for (key, value) in self {
            key.encode(buf)?;
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl<K: Decode + Eq + std::hash::Hash, V: Decode> Decode for HashMap<K, V> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len < 0 {
            return Ok(HashMap::new());
        }
        let mut map = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = K::decode(buf, api_version)?;
            let value = V::decode(buf, api_version)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

impl Varint for i64 {
    fn encode_varint(&self, buf: &mut impl BufMut) {
        let mut value = (self << 1) ^ (self >> 63);
        loop {
            if (value & !0x7F) == 0 {
                buf.put_u8(value as u8);
                break;
            } else {
                buf.put_u8(((value & 0x7F) | 0x80) as u8);
                value >>= 7;
            }
        }
    }

    fn decode_varint(buf: &mut impl Buf) -> Result<Self> {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if !buf.has_remaining() {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for varint",
                )));
            }
            let byte = buf.get_u8();
            value |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Varint is too long",
                )));
            }
        }
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }
}

impl Varint for u32 {
    fn encode_varint(&self, buf: &mut impl BufMut) {
        let mut value = *self;
        loop {
            if (value & !0x7F) == 0 {
                buf.put_u8(value as u8);
                break;
            } else {
                buf.put_u8(((value & 0x7F) | 0x80) as u8);
                value >>= 7;
            }
        }
    }

    fn decode_varint(buf: &mut impl Buf) -> Result<Self> {
        let mut value: u32 = 0;
        let mut shift: u32 = 0;
        loop {
            if !buf.has_remaining() {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for varint",
                )));
            }
            let byte = buf.get_u8();
            value |= ((byte & 0x7F) as u32) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;
            if shift >= 32 {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Varint is too long for u32",
                )));
            }
        }
        Ok(value)
    }
}

impl Varint for u16 {
    fn encode_varint(&self, buf: &mut impl BufMut) {
        let mut value = *self;
        loop {
            if (value & !0x7F) == 0 {
                buf.put_u8(value as u8);
                break;
            } else {
                buf.put_u8(((value & 0x7F) | 0x80) as u8);
                value >>= 7;
            }
        }
    }

    fn decode_varint(buf: &mut impl Buf) -> Result<Self> {
        let mut value: u16 = 0;
        let mut shift: u32 = 0;
        loop {
            if !buf.has_remaining() {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Not enough bytes for varint",
                )));
            }
            let byte = buf.get_u8();
            value |= ((byte & 0x7F) as u16) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;
            if shift >= 16 {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Varint is too long for u16",
                )));
            }
        }
        Ok(value)
    }
}

impl<T: Encode> Encode for CompactVec<T> {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = (self.0.len() as u32) + 1;
        len.encode_varint(buf);
        for item in &self.0 {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for CompactVec<T> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = u32::decode_varint(buf)?;
        if len == 0 {
             return Ok(CompactVec(Vec::new()));
        }
        let len = len - 1;
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            vec.push(T::decode(buf, api_version)?);
        }
        Ok(CompactVec(vec))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactString(pub String);

impl Encode for CompactString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = (self.0.len() as u32) + 1;
        len.encode_varint(buf);
        buf.put_slice(self.0.as_bytes());
        Ok(())
    }
}

impl Decode for CompactString {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let len_plus_one = u32::decode_varint(buf)?;
        if len_plus_one == 0 {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CompactString length cannot be 0, as it represents a null string",
            )));
        }
        let len = (len_plus_one - 1) as usize;
        if buf.remaining() < len {
             return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for CompactString",
            )));
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(CompactString(String::from_utf8(bytes).map_err(|e| {
            ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactNullableString(pub Option<String>);

impl Encode for CompactNullableString {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.0 {
            Some(s) => {
                let len = (s.len() as u32) + 1;
                len.encode_varint(buf);
                buf.put_slice(s.as_bytes());
            }
            None => {
                0u32.encode_varint(buf);
            }
        }
        Ok(())
    }
}

impl Decode for CompactNullableString {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let len_plus_one = u32::decode_varint(buf)?;
        if len_plus_one == 0 {
            return Ok(CompactNullableString(None));
        }
        let len = (len_plus_one - 1) as usize;
         if buf.remaining() < len {
             return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for CompactNullableString",
            )));
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(CompactNullableString(Some(String::from_utf8(bytes).map_err(|e| {
            ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?)))
    }
}

impl Encode for bool {
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        (*self as i8).encode(buf)
    }
}