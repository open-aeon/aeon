use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("序列化错误: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("连接错误: {0}")]
    Connection(String),

    #[error("超时错误: {0}")]
    Timeout(String),

    #[error("服务器错误: {0}")]
    Server(String),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("ACK 错误: {0}")]
    Ack(String),

    #[error("其他错误: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ClientError>; 