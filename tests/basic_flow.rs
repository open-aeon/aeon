use anyhow::Result;
use bifrost::broker::Broker;
use bifrost::config::Config;
use bifrost::server::Server;
use bifrost::kafka::{CreateTopicRequest, ProduceRequest, FetchRequest, message::Message};
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

// TC-BF-02: Batch message production and consumption
#[tokio::test]
async fn test_produce_fetch_batch_messages() -> Result<()> {
    let mut context = setup().await?;
    let topic_name = "batch-topic".to_string();
    let partition = 0;

    // 1. Create Topic
    context.client.create_topic(CreateTopicRequest {
        name: topic_name.clone(),
        partition_num: 1,
        replication_factor: 1,
    }).await?;

    // 2. Produce a batch of messages
    let messages_to_send = vec![
        Message::new(b"batch message 1".to_vec()),
        Message::new(b"batch message 2".to_vec()),
        Message::new(b"batch message 3".to_vec()),
    ];
    
    let produce_req = ProduceRequest {
        topic: topic_name.clone(),
        partition,
        messages: messages_to_send.clone(),
    };
    let produce_res = context.client.produce(produce_req).await?;
    assert_eq!(produce_res.base_offset, 2); // Offsets 0, 1, 2

    // 3. Fetch the messages back
    let fetch_req = FetchRequest {
        topic: topic_name,
        partition,
        offset: 0,
        max_bytes: 1024,
    };
    let fetch_res = context.client.fetch(fetch_req).await?;

    // 4. Validate the result
    assert_eq!(fetch_res.messages.len(), 3);
    assert_eq!(fetch_res.next_offset, 3);
    for i in 0..3 {
        assert_eq!(fetch_res.messages[i].content, messages_to_send[i].content);
    }

    Ok(())
}

// TC-OM-01: Fetch from a specific offset
#[tokio::test]
async fn test_fetch_from_specific_offset() -> Result<()> {
    let mut context = setup().await?;
    let topic_name = "offset-topic".to_string();
    let partition = 0;

    // 1. Create Topic and produce 5 messages
    context.client.create_topic(CreateTopicRequest { name: topic_name.clone(), partition_num: 1, replication_factor: 1 }).await?;
    let messages_to_send: Vec<Message> = (0..5).map(|i| Message::new(format!("message {}", i).into_bytes())).collect();
    context.client.produce(ProduceRequest { topic: topic_name.clone(), partition, messages: messages_to_send }).await?;

    // 2. Fetch starting from offset 3
    let fetch_req = FetchRequest {
        topic: topic_name,
        partition,
        offset: 3,
        max_bytes: 1024,
    };
    let fetch_res = context.client.fetch(fetch_req).await?;

    // 3. Validate the result
    assert_eq!(fetch_res.messages.len(), 2); // Should get message 3 and 4
    assert_eq!(fetch_res.messages[0].content, b"message 3");
    assert_eq!(fetch_res.messages[1].content, b"message 4");
    assert_eq!(fetch_res.next_offset, 5);

    Ok(())
}

// TC-OM-02: Fetch from an out-of-bounds offset
#[tokio::test]
async fn test_fetch_offset_out_of_bounds() -> Result<()> {
    let mut context = setup().await?;
    let topic_name = "out-of-bounds-topic".to_string();
    let partition = 0;
    
    context.client.create_topic(CreateTopicRequest { name: topic_name.clone(), partition_num: 1, replication_factor: 1 }).await?;
    let messages = vec![Message::new(b"message 0".to_vec())];
    context.client.produce(ProduceRequest { topic: topic_name.clone(), partition, messages }).await?;

    // 2. Fetch from offset 5 (which doesn't exist)
    let fetch_req = FetchRequest {
        topic: topic_name,
        partition,
        offset: 5,
        max_bytes: 1024,
    };
    let fetch_res = context.client.fetch(fetch_req).await?;

    // 3. Validate the result
    assert!(fetch_res.messages.is_empty());
    assert_eq!(fetch_res.next_offset, 5); // next_offset remains the requested offset

    Ok(())
}

// TC-EH-01: Produce to a non-existent topic
#[tokio::test]
async fn test_produce_to_non_existent_topic() -> Result<()> {
    let mut context = setup().await?;
    let topic_name = "i-do-not-exist".to_string();

    let produce_req = ProduceRequest {
        topic: topic_name,
        partition: 0,
        messages: vec![Message::new(b"test".to_vec())],
    };
    
    // The call should fail
    let produce_result = context.client.produce(produce_req).await;
    assert!(produce_result.is_err());
    
    // Optional: Check for a specific error message
    let error_message = produce_result.err().unwrap().to_string();
    assert!(error_message.contains("Topic not found"));

    Ok(())
}