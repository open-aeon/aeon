use log::{info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::message::Message;
use std::time::Duration;

const BROKERS: &str = "127.0.0.1:8080";
const TOPIC_NAME: &str = "my-topic";
const GROUP_ID: &str = "test-group";
const MESSAGE_COUNT: u32 = 10;

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Starting integration test...");

    info!("--- Step 1: Producing {} messages ---", MESSAGE_COUNT);
    produce(MESSAGE_COUNT).await;

    info!("--- Step 2: Consuming {} messages ---", MESSAGE_COUNT);
    consume(MESSAGE_COUNT as usize).await;

    info!("--- Test finished successfully ---");
}

async fn produce(message_count: u32) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let topic = TOPIC_NAME;

    for i in 0..message_count {
        let message = format!("message {}", i);
        info!("Sending message: {}", message);
        let key = format!("key-{}", i);
        let record = BaseRecord::to(topic)
            .key(&key)
            .payload(&message);

        producer.send(record).expect("Failed to send message");
    }
    producer.flush(Duration::from_secs(5)).expect("Flush failed");
    info!("Finished producing messages.");
}

async fn consume(expected_messages: usize) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", GROUP_ID)
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation error");

    consumer.subscribe(&[TOPIC_NAME]).expect("Can't subscribe to specified topic");

    info!("Waiting for messages...");

    let mut messages_consumed = 0;
    while messages_consumed < expected_messages {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(message)) => {
                let payload = match message.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error deserializing message payload: {:?}", e);
                        continue;
                    }
                    None => {
                        warn!("Message with empty payload received");
                        continue;
                    }
                };
                info!("Received message: key: {:?}, payload: '{}', topic: {}, partition: {}, offset: {}",
                      message.key(),
                      payload,
                      message.topic(),
                      message.partition(),
                      message.offset());
                messages_consumed += 1;
            }
            Some(Err(e)) => {
                warn!("An error occurred while consuming message: {:?}", e);
            }
            None => {
                warn!("No message received in the last second, polling again...");
            }
        }
    }
    info!("Successfully consumed {} messages. Consumer is shutting down.", messages_consumed);
}
