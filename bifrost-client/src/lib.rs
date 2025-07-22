use anyhow::Result;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use bifrost::protocol::*;
use bifrost::protocol::codec::ClientCodec;

pub struct Client {
    connection: Framed<TcpStream, ClientCodec>,
}

impl Client {
    pub async fn new(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let connection = Framed::new(stream, ClientCodec::default());
        Ok(Self { connection })
    }

    /// 发送一个通用请求并获取响应
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        self.connection.send(request).await?;
        match self.connection.next().await {
            Some(Ok(response)) => Ok(response),
            Some(Err(e)) => Err(e.into()),
            None => Err(anyhow::anyhow!("Connection closed by server")),
        }
    }

    /// 创建 Topic
    pub async fn create_topic(&mut self, request: CreateTopicRequest) -> Result<CreateTopicResponse> {
        match self.send_request(Request::CreateTopic(request)).await? {
            Response::CreateTopic(res) => Ok(res),
            Response::Error(e) => Err(anyhow::anyhow!("Create Topic Error: {:?}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// 生产消息
    pub async fn produce(&mut self, request: ProduceRequest) -> Result<ProduceResponse> {
        match self.send_request(Request::Produce(request)).await? {
            Response::Produce(res) => Ok(res),
            Response::Error(e) => Err(anyhow::anyhow!("Produce Error: {:?}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// 获取消息
    pub async fn fetch(&mut self, request: FetchRequest) -> Result<FetchResponse> {
        match self.send_request(Request::Fetch(request)).await? {
            Response::Fetch(res) => Ok(res),
            Response::Error(e) => Err(anyhow::anyhow!("Fetch Error: {:?}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// 获取元数据
    pub async fn metadata(&mut self, request: MetadataRequest) -> Result<MetadataResponse> {
        match self.send_request(Request::Metadata(request)).await? {
            Response::Metadata(res) => Ok(res),
            Response::Error(e) => Err(anyhow::anyhow!("Metadata Error: {:?}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}
