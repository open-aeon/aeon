use anyhow::Result;
use bifrost::broker::Broker;
use bifrost::config::Config;
use bifrost::server::Server;
use bifrost::protocol::{CreateTopicRequest, ProduceRequest, FetchRequest, message::Message};
use bifrost_client::Client;
use std::sync::Arc;
use tempfile::TempDir;

struct TestContext {
    pub client: Client,
    _server_handle: tokio::task::JoinHandle<()>,
    _temp_dir: TempDir,
}

async fn setup() -> Result<TestContext> {
    // 1. 创建一个临时目录用于存放数据
    let temp_dir = TempDir::new()?;
    let data_dir = temp_dir.path().to_path_buf();

    // 2. 加载并修改配置以使用临时目录和随机端口
    let mut config = Config::load_from_file("config/default.yaml")?;
    config.broker.data_dir = data_dir;
    config.server.port = 0; // 使用 0 来让操作系统随机选择一个可用端口

    let server_config = config.server;

    // 3. 启动 Broker 和 Server
    let broker = Broker::new(config.broker, config.storage);
    broker.start().await?;
    let broker = Arc::new(broker);

    let server = Server::new(broker.clone(), server_config);
    
    // 获取服务器将要监听的实际地址
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    
    let server_handle = tokio::spawn(async move {
        println!("Test server listening on {}", server_addr);
        server.start(listener).await.unwrap();
    });

    // 等待一小段时间确保服务器已启动
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. 创建并连接客户端
    let client = Client::new(&server_addr.to_string()).await?;

    Ok(TestContext {
        client,
        _server_handle: server_handle,
        _temp_dir: temp_dir,
    })
}


#[tokio::test]
async fn test_create_produce_fetch_single_message() -> Result<()> {
    // 1. 设置环境
    let mut context = setup().await?;

    let topic_name = "single-message-topic".to_string();
    let partition = 0;

    // 2. 创建 Topic
    let create_req = CreateTopicRequest {
        name: topic_name.clone(),
        partition_num: 1,
        replication_factor: 1,
    };
    let create_res = context.client.create_topic(create_req).await?;
    assert!(create_res.error.is_none());

    // 3. 生产消息
    let message_content = b"hello world from test".to_vec();
    let produce_req = ProduceRequest {
        topic: topic_name.clone(),
        partition,
        messages: vec![Message::new(message_content.clone())],
    };
    let produce_res = context.client.produce(produce_req).await?;
    assert_eq!(produce_res.base_offset, 0);

    // 4. 获取消息
    let fetch_req = FetchRequest {
        topic: topic_name,
        partition,
        offset: 0,
        max_bytes: 1024,
    };
    let fetch_res = context.client.fetch(fetch_req).await?;

    // 5. 验证结果
    assert_eq!(fetch_res.messages.len(), 1);
    assert_eq!(fetch_res.messages[0].content, message_content);
    assert_eq!(fetch_res.next_offset, 1);
    
    Ok(())
}