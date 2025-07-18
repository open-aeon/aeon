use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("段已满")]
    SegmentFull,
    
    #[error("段不存在")]
    SegmentNotFound,
    
    #[error("偏移量无效")]
    InvalidOffset,
    
    #[error("消息序列化错误: {0}")]
    Serialization(String),
    
    #[error("消息反序列化错误: {0}")]
    Deserialization(String),
    
    #[error("其他错误: {0}")]
    Other(String),
} 