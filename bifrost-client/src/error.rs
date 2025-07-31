use bifrost::kafka::ErrorResponse;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("No bootstrap servers provided")]
    NoBootstrapServers,

    #[error("Failed to connect to any bootstrap server")]
    BootstrapConnectFailed,

    #[error("Broker not found in metadata cache: {0}")]
    BrokerNotFound(u32),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] bincode::Error),

    #[error("Server returned an error: {0:?}")]
    ServerError(ErrorResponse),

    #[error("Unexpected response type from server")]
    UnexpectedResponse,

    #[error("Connection closed by server")]
    ConnectionClosed,
}