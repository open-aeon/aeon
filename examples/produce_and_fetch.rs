use anyhow::Result;
use bifrost::protocol::{CreateTopicRequest, FetchRequest, message::Message, ProduceRequest};
use bifrost_client::Client;

#[tokio::main]
async fn main() -> Result<()> {
    // --- Step 1: Connect to the Server ---
    // Make sure you have started the server in a separate terminal
    // using `cargo run` before running this example.
    println!("Connecting to the server at 127.0.0.1:8080...");
    let server_addr = "127.0.0.1:8080";
    let mut client = Client::new(server_addr).await?;
    println!("Connection successful!");

    let topic_name = "my-persistent-topic".to_string();

    // --- Step 2: Create a Topic ---
    // This will create a directory in your `data/` folder.
    println!("\nCreating topic '{}'...", topic_name);
    let create_req = CreateTopicRequest {
        name: topic_name.clone(),
        partition_num: 1,
        replication_factor: 1,
    };
    match client.create_topic(create_req).await {
        Ok(res) => {
            if let Some(err) = res.error {
                println!("Topic might already exist: {}", err);
            } else {
                println!("Topic '{}' created successfully.", res.name);
            }
        }
        Err(e) => {
            println!("Failed to create topic: {}", e);
            return Err(e);
        }
    }
    
    // --- Step 3: Produce some messages ---
    // You can now inspect the `data/my-persistent-topic/0/` directory
    // to see the .log and .index files being written.
    println!("\nProducing 3 messages...");
    let messages = vec![
        Message::new(b"This is the first message.".to_vec()),
        Message::new(b"This is the second one.".to_vec()),
        Message::new(b"And a final, third message.".to_vec()),
    ];
    let produce_req = ProduceRequest {
        topic: topic_name.clone(),
        partition: 0,
        messages,
    };
    let produce_res = client.produce(produce_req).await?;
    println!(
        "Successfully produced messages. Last offset: {}",
        produce_res.base_offset
    );

    // --- Step 4: Fetch the messages back ---
    println!("\nFetching messages from offset 0...");
    let fetch_req = FetchRequest {
        topic: topic_name,
        partition: 0,
        offset: 0,
        max_bytes: 4096,
    };
    let fetch_res = client.fetch(fetch_req).await?;
    println!("Fetched {} messages:", fetch_res.messages.len());
    for (i, msg) in fetch_res.messages.iter().enumerate() {
        println!(
            "  Message {}: {}",
            i,
            String::from_utf8_lossy(&msg.content)
        );
    }
    println!("Next offset to fetch from would be: {}", fetch_res.next_offset);

    Ok(())
}