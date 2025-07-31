use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("IoError: {0}")]
    Io(#[from] io::Error),

    #[error("Utf8Error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Unknown API key: {0}")]
    UnknownApiKey(i16),
    
    #[error("API key mismatch")]
    ApiKeyMismatch,

    #[error("Unsupported compression type: {0}")]
    UnsupportedCompressionType(u8),

    #[error("Invalid CRC32")]
    InvalidCrc,

    #[error("Unsupported magic byte: {0}")]
    UnsupportedMagicByte(i8),

    #[error("Invalid tagged field")]
    InvalidTaggedField,
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
