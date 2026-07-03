use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Segment Full")]
    SegmentFull,
    
    #[error("Segment Not Found")]
    SegmentNotFound,
    
    #[error("Invalid Offset")]
    InvalidOffset,

    #[error("Data Corruption")]
    DataCorruption,
    
    #[error("Other error: {0}")]
    Other(String),
} 