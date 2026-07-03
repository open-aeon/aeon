use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Connection Fail: {0}")]
    Connection(String),
    
    #[error("Connection Pool Full")]
    ConnectionPoolFull,
    
    #[error("Connection Timeout")]
    ConnectionTimeout,
    
    #[error("Invalid Connection State")]
    InvalidConnectionState,
    
    #[error("Other error: {0}")]
    Other(String),
} 