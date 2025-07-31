use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use rand::seq::SliceRandom;

use bifrost::kafka::{Request, Response, MetadataRequest};
use bifrost::kafka::codec::ClientCodec;
use bifrost::common::metadata::TopicMetadata;
use crate::error::ClientError;

pub type Connection = Framed<TcpStream, ClientCodec>;

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub bootstrap_servers: Vec<String>,
    pub connect_timeout: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connect_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Default)]
struct Metadata {
    brokers: HashMap<u32, String>,
    topics: HashMap<String, TopicMetadata>,
}

#[derive(Debug, Default)]
struct ConnectionPool {
    connections: HashMap<String, Connection>,
}

pub struct Client {
    options: ClientOptions,
    metadata: Arc<RwLock<Metadata>>,
    connection_pool: Arc<RwLock<ConnectionPool>>,
}

impl Client {
    pub async fn new(options: ClientOptions) -> Result<Self, ClientError> {
        if options.bootstrap_servers.is_empty() {
            return Err(ClientError::NoBootstrapServers);
        }

        let (initial_broker, initial_topics) = Self::fetch_metadata(&options).await?;

        let metadata = Arc::new(RwLock::new(Metadata {
            brokers: initial_broker,
            topics: initial_topics,
        }));
        let connection_pool = Arc::new(RwLock::new(ConnectionPool::default()));

        Ok(Self { options, metadata, connection_pool })
    }

    async fn fetch_metadata(options: &ClientOptions) -> Result<(HashMap<u32, String>, HashMap<String, TopicMetadata>), ClientError> {
        let mut bootstrap_servers = options.bootstrap_servers.clone();
        // Shuffles the bootstrap_servers randomly, which helps with load balancing or prevents always connecting to the same server first.
        bootstrap_servers.shuffle(&mut rand::thread_rng());

        for server in bootstrap_servers {
            let stream = match TcpStream::connect(server).await {
                Ok(stream) => stream,
                Err(_) => continue,
            };
            let mut connection = Framed::new(stream, ClientCodec::default());
            let request = Request::Metadata(MetadataRequest {
                topics: vec![],
            });
            if connection.send(request).await.is_err() {
                continue;
            }

            if let Some(Ok(Response::Metadata(res))) = connection.next().await {
                let broker = res.brokers.into_iter().map(|b| (b.id, format!("{}:{}", b.host, b.port))).collect();
                let topics = res.topics.into_iter().map(|t| (t.name.clone(), t)).collect();
                return Ok((broker, topics));
            }
        }

        Err(ClientError::BootstrapConnectFailed)
    }
}