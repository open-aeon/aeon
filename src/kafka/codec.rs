use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::protocol::{ProtocolError, Result};

// --- Core Traits ---

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactVec<T>(pub Vec<T>);

pub trait Varint: Sized {
    fn encode_varint(&self, buf: &mut impl BufMut);
    fn decode_varint(buf: &mut impl Buf) -> Result<Self>;
}

pub trait Encode: Sized {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()>;
    fn encode_to_bytes(&self, api_version: i16) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf, api_version)?;
        Ok(buf.freeze())
    }
}

pub trait Decode: Sized {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self>;
}


// --- Primitive Implementations ---

impl Encode for i8 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_i8(*self); Ok(()) } }
impl Decode for i8 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_i8()) } }
impl Encode for u8 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_u8(*self); Ok(()) } }
impl Decode for u8 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_u8()) } }
impl Encode for i16 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_i16(*self); Ok(()) } }
impl Decode for i16 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_i16()) } }
impl Encode for u16 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_u16(*self); Ok(()) } }
impl Decode for u16 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_u16()) } }
impl Encode for i32 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_i32(*self); Ok(()) } }
impl Decode for i32 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_i32()) } }
impl Encode for u32 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_u32(*self); Ok(()) } }
impl Decode for u32 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_u32()) } }
impl Encode for i64 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_i64(*self); Ok(()) } }
impl Decode for i64 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_i64()) } }
impl Encode for u64 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_u64(*self); Ok(()) } }
impl Decode for u64 { fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> { Ok(buf.get_u64()) } }
impl Encode for u128 { fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> { buf.put_u128(*self); Ok(()) } }
impl Decode for u128 {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        if buf.remaining() < 16 {
            return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for u128/UUID")));
        }
        Ok(buf.get_u128())
    }
}
impl Encode for bool {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> { (*self as i8).encode(buf, api_version) }
}
impl Decode for bool {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let val = i8::decode(buf, api_version)?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid value for bool, must be 0 or 1"))),
        }
    }
}


// --- Complex Type Implementations ---

// String (i16 length prefix)
impl Encode for String {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        if self.len() > i16::MAX as usize {
            return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "String too long for i16 prefix")));
        }
        (self.len() as i16).encode(buf, api_version)?;
        buf.put_slice(self.as_bytes());
        Ok(())
    }
}
impl Decode for String {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i16::decode(buf, api_version)?;
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "String length cannot be negative for non-nullable string"))); }
        let len = len as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for String"))); }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).map_err(|e| ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
    }
}
impl Encode for Option<String> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        match self {
            Some(s) => s.encode(buf, api_version),
            None => (-1i16).encode(buf, api_version),
        }
    }
}
impl Decode for Option<String> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i16::decode(buf, api_version)?;
        if len == -1 { return Ok(None); }
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid negative length for NullableString"))); }
        let len = len as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for NullableString"))); }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(Some(String::from_utf8(bytes).map_err(|e| ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?))
    }
}

// Bytes (i32 length prefix)
impl Encode for Bytes {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        if self.len() > i32::MAX as usize { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Bytes length is too large for i32"))); }
        (self.len() as i32).encode(buf, api_version)?;
        buf.put_slice(self);
        Ok(())
    }
}
impl Decode for Bytes {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Bytes length cannot be negative for non-nullable bytes"))); }
        let len = len as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for Bytes"))); }
        Ok(buf.copy_to_bytes(len))
    }
}
impl Encode for Option<Bytes> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        match self {
            Some(b) => {
                // Legacy (non-flexible) NULLABLE_BYTES encoding: int32 length + data
                b.encode(buf, api_version)
            }
            None => {
                // Legacy (non-flexible) NULLABLE_BYTES null marker: int32 -1
                (-1i32).encode(buf, api_version)
            }
        }
    }
}
impl Decode for Option<Bytes> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len == -1 { return Ok(None); }
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid negative length for NullableBytes"))); }
        let len = len as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for NullableBytes"))); }
        Ok(Some(buf.copy_to_bytes(len)))
    }
}

// Array Types (i32 length prefix)
impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        (self.len() as i32).encode(buf, api_version)?;
        for item in self { item.encode(buf, api_version)?; }
        Ok(())
    }
}
impl<T: Decode> Decode for Vec<T> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Array length cannot be negative"))); }
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len { vec.push(T::decode(buf, api_version)?); }
        Ok(vec)
    }
}
impl<T: Encode> Encode for Option<Vec<T>> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        match self {
            Some(v) => v.encode(buf, api_version),
            None => (-1i32).encode(buf, api_version),
        }
    }
}
impl<T: Decode> Decode for Option<Vec<T>> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let len = i32::decode(buf, api_version)?;
        if len == -1 { return Ok(None); }
        if len < 0 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid negative length for Nullable Array"))); }
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len { vec.push(T::decode(buf, api_version)?); }
        Ok(Some(vec))
    }
}

// Empty Type
impl Encode for () { fn encode(&self, _: &mut impl BufMut, _: i16) -> Result<()> { Ok(()) } }
impl Decode for () { fn decode(_: &mut impl Buf, _: i16) -> Result<Self> { Ok(()) } }

// --- Varint Implementations ---
impl Varint for i32 {
    fn encode_varint(&self, buf: &mut impl BufMut) {
        let mut value = (*self << 1) ^ (*self >> 31);
        loop {
            if (value & !0x7F) == 0 { buf.put_u8(value as u8); break; } 
            else { buf.put_u8(((value & 0x7F) | 0x80) as u8); value >>= 7; }
        }
    }
    fn decode_varint(buf: &mut impl Buf) -> Result<Self> {
        let mut value: u32 = 0;
        let mut shift: u32 = 0;
        loop {
            if !buf.has_remaining() { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for varint"))); }
            let byte = buf.get_u8();
            value |= ((byte & 0x7F) as u32) << shift;
            if (byte & 0x80) == 0 { break; }
            shift += 7;
            if shift >= 32 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Varint is too long"))); }
        }
        Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
    }
}
impl Varint for u32 {
    fn encode_varint(&self, buf: &mut impl BufMut) {
        let mut value = *self;
        loop {
            if (value & !0x7F) == 0 { buf.put_u8(value as u8); break; } 
            else { buf.put_u8(((value & 0x7F) | 0x80) as u8); value >>= 7; }
        }
    }
    fn decode_varint(buf: &mut impl Buf) -> Result<Self> {
        let mut value: u32 = 0;
        let mut shift: u32 = 0;
        loop {
            if !buf.has_remaining() { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for varint"))); }
            let byte = buf.get_u8();
            value |= ((byte & 0x7F) as u32) << shift;
            if (byte & 0x80) == 0 { break; }
            shift += 7;
            if shift >= 32 { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Varint is too long for u32"))); }
        }
        Ok(value)
    }
}


// --- Compact Types ---
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactString(pub String);

impl Encode for CompactString {
    fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> {
        let len = (self.0.len() as u32) + 1;
        len.encode_varint(buf);
        buf.put_slice(self.0.as_bytes());
        Ok(())
    }
}
impl Decode for CompactString {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let len = u32::decode_varint(buf)? - 1;
        let len = len as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for CompactString"))); }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(CompactString(String::from_utf8(bytes).map_err(|e| ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactNullableString(pub Option<String>);

impl Encode for CompactNullableString {
    fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> {
        match &self.0 {
            Some(s) => {
                let len = (s.len() as u32) + 1;
                len.encode_varint(buf);
                buf.put_slice(s.as_bytes());
            }
            None => 0u32.encode_varint(buf),
        }
        Ok(())
    }
}
impl Decode for CompactNullableString {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let len = u32::decode_varint(buf)?;
        if len == 0 { return Ok(CompactNullableString(None)); }
        let len = (len - 1) as usize;
        if buf.remaining() < len { return Err(ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Not enough bytes for CompactNullableString"))); }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(CompactNullableString(Some(String::from_utf8(bytes).map_err(|e| ProtocolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?)))
    }
}

// --- Compact Bytes Types ---
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactBytes(pub Bytes);

impl Encode for CompactBytes {
    fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> {
        use crate::kafka::codec::Varint;
        let len = (self.0.len() as u32) + 1;
        len.encode_varint(buf);
        buf.put_slice(&self.0);
        Ok(())
    }
}

impl Decode for CompactBytes {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let n = u32::decode_varint(buf)?;
        if n == 0 {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CompactBytes cannot be null",
            )));
        }
        let len = (n - 1) as usize;
        if buf.remaining() < len {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for CompactBytes",
            )));
        }
        Ok(CompactBytes(buf.copy_to_bytes(len)))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CompactNullableBytes(pub Option<Bytes>);

impl Encode for CompactNullableBytes {
    fn encode(&self, buf: &mut impl BufMut, _: i16) -> Result<()> {
        use crate::kafka::codec::Varint;
        match &self.0 {
            Some(b) => {
                let len = (b.len() as u32) + 1;
                len.encode_varint(buf);
                buf.put_slice(b);
            }
            None => {
                0u32.encode_varint(buf);
            }
        }
        Ok(())
    }
}

impl Decode for CompactNullableBytes {
    fn decode(buf: &mut impl Buf, _: i16) -> Result<Self> {
        let n = u32::decode_varint(buf)?;
        if n == 0 {
            return Ok(CompactNullableBytes(None));
        }
        let len = (n - 1) as usize;
        if buf.remaining() < len {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for CompactNullableBytes",
            )));
        }
        Ok(CompactNullableBytes(Some(buf.copy_to_bytes(len))))
    }
}

// --- Compact Arrays ---
impl<T: Encode> Encode for CompactVec<T> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        use crate::kafka::codec::Varint;
        let len = (self.0.len() as u32) + 1;
        len.encode_varint(buf);
        for item in &self.0 {
            item.encode(buf, api_version)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for CompactVec<T> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let n = u32::decode_varint(buf)?;
        if n == 0 {
            return Err(ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Compact array cannot be null",
            )));
        }
        let len = (n - 1) as usize;
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(T::decode(buf, api_version)?);
        }
        Ok(CompactVec(v))
    }
}

impl<T: Encode> Encode for Option<CompactVec<T>> {
    fn encode(&self, buf: &mut impl BufMut, api_version: i16) -> Result<()> {
        use crate::kafka::codec::Varint;
        match self {
            Some(cv) => {
                let len = (cv.0.len() as u32) + 1;
                len.encode_varint(buf);
                for item in &cv.0 {
                    item.encode(buf, api_version)?;
                }
                Ok(())
            }
            None => {
                0u32.encode_varint(buf);
                Ok(())
            }
        }
    }
}

impl<T: Decode> Decode for Option<CompactVec<T>> {
    fn decode(buf: &mut impl Buf, api_version: i16) -> Result<Self> {
        let n = u32::decode_varint(buf)?;
        if n == 0 {
            return Ok(None);
        }
        let len = (n - 1) as usize;
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(T::decode(buf, api_version)?);
        }
        Ok(Some(CompactVec(v)))
    }
}
