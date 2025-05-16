use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("连接错误: {0}")]
    Connection(String),
    
    #[error("连接池已满")]
    ConnectionPoolFull,
    
    #[error("连接超时")]
    ConnectionTimeout,
    
    #[error("无效的连接状态")]
    InvalidConnectionState,
    
    #[error("其他错误: {0}")]
    Other(String),
} 