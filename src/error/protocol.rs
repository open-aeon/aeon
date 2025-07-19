use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("消息序列化错误: {0}")]
    Serialization(String),

    #[error("无效的请求类型")]
    InvalidRequestType,
    
    #[error("无效的响应类型")]
    InvalidResponseType,
    
    #[error("消息编码错误: {0}")]
    Encoding(String),
    
    #[error("消息解码错误: {0}")]
    Decoding(String),
    
    #[error("其他错误: {0}")]
    Other(String),
} 