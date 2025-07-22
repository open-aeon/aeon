use crate::broker::Broker;
use crate::common::metadata::TopicPartition;
use crate::error::StorageError;
use crate::protocol::message::Message;
use crate::protocol::*;
use anyhow::Result;

pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let response = match request {
        Request::Produce(req) => handle_produce(req, broker).await,
        Request::Fetch(req) => handle_fetch(req, broker).await,
        Request::Metadata(req) => handle_metadata(req, broker).await,
        Request::CreateTopic(req) => handle_create_topic(req, broker).await,
        Request::Heartbeat => Ok(Response::Heartbeat),
    };

    // 统一错误处理
    response.or_else(|e| {
        // 在这里可以根据错误类型映射到不同的 ErrorCode
        let err_resp = ErrorResponse {
            code: ErrorCode::Unknown,
            message: e.to_string(),
        };
        Ok(Response::Error(err_resp))
    })
}

async fn handle_produce(req: ProduceRequest, broker: &Broker) -> Result<Response> {
    let tp = TopicPartition {
        topic: req.topic.clone(),
        partition: req.partition,
    };

   let message_content : Vec<Vec<u8>> = req.messages.iter().map(|m| m.content.clone()).collect();
   if message_content.is_empty() {
        return Ok(Response::Produce(ProduceResponse{
            topic: req.topic,
            partition: req.partition,
            base_offset: 0,
        }));
   }
   let last_offset = broker.append_batch(&tp, &message_content).await?;

    Ok(Response::Produce(ProduceResponse {
        topic: req.topic,
        partition: req.partition,
        base_offset: last_offset,
    }))
}

async fn handle_fetch(req: FetchRequest, broker: &Broker) -> Result<Response> {
    let tp = TopicPartition {
        topic: req.topic.clone(),
        partition: req.partition,
    };

    let mut messages = Vec::new();
    let mut total_bytes = 0;
    let mut current_offset = req.offset;

    while total_bytes < req.max_bytes {
        match broker.read(&tp, current_offset).await {
            Ok(data) => {
                let message = Message::new(data);
                total_bytes += message.content.len() as u32;
                current_offset += 1;
                messages.push(message);
            }
            Err(e) => {
                if let Some(storage_err) = e.downcast_ref::<StorageError>() {
                    if matches!(storage_err, StorageError::InvalidOffset) {
                        break;
                    }
                }
                // 其他错误则向上抛出
                return Err(e);
            }
        }
    }

    let response = FetchResponse {
        topic: req.topic,
        partition: req.partition,
        messages,
        next_offset: current_offset,
    };

    Ok(Response::Fetch(response))
}

async fn handle_metadata(req: MetadataRequest, broker: &Broker) -> Result<Response> {
    let topics_meta = broker.get_topics_meta().await?;
    let requested_topics = if req.topics.is_empty() {
        topics_meta.keys().cloned().collect()
    } else {
        req.topics
    };

    let mut topics = Vec::new();
    for topic_name in requested_topics {
        if let Some(topic_meta) = topics_meta.get(&topic_name) {
            let partitions = (0..topic_meta.partition_count)
                .map(|p_id| PartitionMetadata {
                    id: p_id,
                    leader: 0, // todo: hardcoded leader
                    replicas: vec![0],
                    isr: vec![0],
                })
                .collect();

            topics.push(TopicMetadata {
                name: topic_name.clone(),
                partitions,
            });
        }
    }

    Ok(Response::Metadata(MetadataResponse { topics }))
}

async fn handle_create_topic(req: CreateTopicRequest, broker: &Broker) -> Result<Response> {
    let response = broker.create_topic(req.name.clone(), req.partition_num).await;
    Ok(Response::CreateTopic(CreateTopicResponse {
        name: req.name,
        error: response.err().map(|e| e.to_string()),
    }))
}