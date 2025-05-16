use bifrost::{
    protocol::{
        FetchRequest, Request, Response,
    },
    protocol::codec::ClientCodec,
};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 连接到服务器
    println!("正在连接到服务器...");
    let stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("已连接到服务器");
    let mut framed = Framed::new(stream, ClientCodec::default());

    // 设置消费参数
    let topic = "test-topic";
    let partition = 0;
    let mut offset = 0; // 从开始位置消费
    let group_id = "test-group".to_string();
    let consumer_id = format!("consumer-{}", uuid::Uuid::new_v4());

    println!("开始消费消息...");
    println!("主题: {}, 分区: {}, 起始偏移量: {}, 消费者组: {}, 消费者ID: {}", 
        topic, partition, offset, group_id, consumer_id);

    loop {
        // 发送获取消息请求
        println!("发送获取请求: offset={}", offset);
        let fetch_request = Request::Fetch(FetchRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_bytes: 1024 * 1024, // 1MB
            group_id: group_id.clone(),
            consumer_id: consumer_id.clone(),
        });
        framed.send(fetch_request).await?;
        println!("获取请求已发送");

        // 等待响应
        println!("等待服务器响应...");
        if let Some(response) = framed.next().await {
            match response? {
                Response::Fetch(fetch_response) => {
                    if fetch_response.messages.is_empty() {
                        println!("没有新消息，等待1秒后重试...");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }

                    println!("收到 {} 条消息", fetch_response.messages.len());
                    // 处理接收到的消息
                    for message in fetch_response.messages {
                        println!("收到消息:");
                        println!("  偏移量: {}", offset);
                        if let Some(key) = message.key {
                            println!("  键: {}", String::from_utf8_lossy(&key));
                        }
                        println!("  值: {}", String::from_utf8_lossy(&message.value));
                        println!("  时间戳: {}", message.timestamp);
                        println!("-------------------");
                    }
                    
                    // 使用服务器返回的下一条消息位置
                    offset = fetch_response.next_offset;
                    println!("更新偏移量到: {}", offset);
                }
                _ => println!("收到意外的响应类型"),
            }
        }
    }
} 