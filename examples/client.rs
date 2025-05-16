use bifrost::{
    protocol::{MetadataRequest, ProduceRequest, Request, Response},
    protocol::codec::ClientCodec,
    protocol::message::ProtocolMessage,
};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 连接到服务器
    let stream = TcpStream::connect("127.0.0.1:9092").await?;
    let mut framed = Framed::new(stream, ClientCodec::default());

    // 1. 获取元数据
    println!("获取主题元数据...");
    let metadata_request = Request::Metadata(MetadataRequest {
        topics: vec!["test-topic".to_string()],
        group_id: None,
        consumer_id: None,
    });
    framed.send(metadata_request).await?;

    if let Some(response) = framed.next().await {
        match response? {
            Response::Metadata(metadata) => {
                println!("主题元数据: {:?}", metadata);
            }
            _ => println!("收到意外的响应类型"),
        }
    }

    // 2. 发送消息
    println!("\n发送消息...");
    let message = ProtocolMessage {
        id: uuid::Uuid::new_v4().to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        key: Some("test-key".as_bytes().to_vec()),
        value: "Hello, Bifrost!".as_bytes().to_vec(),
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: -1,
    };

    let produce_request = Request::Produce(ProduceRequest {
        topic: "test-topic".to_string(),
        partition: 0,
        messages: vec![message],
    });
    framed.send(produce_request).await?;

    if let Some(response) = framed.next().await {
        match response? {
            Response::Produce(produce) => {
                println!("消息已发送，偏移量: {}", produce.base_offset);
            }
            _ => println!("收到意外的响应类型"),
        }
    }

    Ok(())
} 