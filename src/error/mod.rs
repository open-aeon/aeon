use thiserror::Error;

pub mod server;
pub mod storage;
pub mod protocol;
pub mod consumer;

pub use server::ServerError;
pub use storage::StorageError;
pub use protocol::ProtocolError;

#[derive(Error, Debug)]
pub enum BifrostError {
    #[error("服务器错误: {0}")]
    Server(#[from] ServerError),
    
    #[error("存储错误: {0}")]
    Storage(#[from] StorageError),
    
    #[error("协议错误: {0}")]
    Protocol(#[from] ProtocolError),
    
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("配置错误: {0}")]
    Config(String),
    
    #[error("其他错误: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BifrostError>; 