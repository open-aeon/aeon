use thiserror::Error;

pub mod server;
pub mod storage;
pub mod protocol;
pub mod consumer;

pub use server::ServerError;
pub use storage::StorageError;
pub use protocol::ProtocolError;

#[derive(Error, Debug)]
pub enum AeonError {
    #[error("Server error: {0}")]
    Server(#[from] ServerError),
    
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, AeonError>; 