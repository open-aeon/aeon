use anyhow::Result;

use crate::broker::Broker;
use crate::common::metadata::TopicPartition;
use crate::error::StorageError;
use crate::protocol::message::Message;
use crate::protocol::*;

pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let response = match request {
        Request::Produce(req) => handle_produce(req, broker).await,
        Request::Fetch(req) => handle_fetch(req, broker).await,
        Request::Metadata(req) => handle_metadata(req, broker).await,
        Request::CreateTopic(req) => handle_create_topic(req, broker).await,
        Request::CommitOffset(req) => handle_commit_offset(req, broker).await,
        Request::FetchOffset(req) => handle_fetch_offset(req, broker).await,
        Request::JoinGroup(req) => handle_join_group(req, broker).await,
        Request::LeaveGroup(req) => handle_leave_group(req, broker).await,
        Request::Heartbeat(req) => handle_heartbeat(req, broker).await,
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

async fn handle_commit_offset(req: CommitOffsetRequest, broker: &Broker) -> Result<Response> {
    let mut results = Vec::with_capacity(req.topic_partitions.len());
    for tpo in req.topic_partitions {
        let tp = TopicPartition {
            topic: tpo.topic,
            partition: tpo.partition,
        };
        let result = broker.commit_offset(req.group_id.clone(), tp.clone(), tpo.offset).await;
        let error_code = match result {
            Ok(_) => None,
            Err(_) => Some(ErrorCode::Unknown),
        };
        results.push(TopicPartitionResult {
            topic: tp.topic,
            partition: tp.partition,
            error_code,
        })
    }
    Ok(Response::CommitOffset(CommitOffsetResponse {
        results,
    }))
}

async fn handle_fetch_offset(req: FetchOffsetRequest, broker: &Broker) -> Result<Response> {
    let mut results = Vec::with_capacity(req.topic_partitions.len());
    for tp in req.topic_partitions {
        let result = broker.fetch_offset(&req.group_id, &tp).await;
        let (offset, error_code) = match result {
            Ok(Some(offset)) => (offset, None),
            Ok(None) => (-1, Some(ErrorCode::OffsetNotFound)),
            Err(_) => (-1, Some(ErrorCode::Unknown) ),
        };
        results.push(TopicPartitionOffsetResult {
            topic: tp.topic,
            partition: tp.partition,
            offset,
            error_code,
        });
    }
    Ok(Response::FetchOffset(FetchOffsetResponse {
        results,
    }))
}

async fn handle_join_group(req: JoinGroupRequest, broker: &Broker) -> Result<Response> {
    let result = broker.join_group(req.group_id, req.member_id, req.session_timeout_ms).await;
    match result {
        Ok((member_id, generation_id)) => {
            Ok(Response::JoinGroup(JoinGroupResponse {
                error_code: None,
                generation_id,
                member_id,
            }))
        }
        Err(_e) => {
            Ok(Response::JoinGroup(JoinGroupResponse {
                error_code: Some(ErrorCode::Unknown),
                generation_id: 0,
                member_id: "".to_string(),
            }))
        }
    }
}

async fn handle_leave_group(req: LeaveGroupRequest, broker: &Broker) -> Result<Response> {
    let result = broker.leave_group(&req.group_id, &req.member_id).await;
    let error_code = match result {
        Ok(_) => None,
        Err(_) => Some(ErrorCode::Unknown),
    };
    Ok(Response::LeaveGroup(LeaveGroupResponse {
        error_code,
    }))
}

async fn handle_heartbeat(req: HeartbeatRequest, broker: &Broker) -> Result<Response> {
    let result = broker.heartbeat(&req.group_id, &req.member_id).await;
    let error_code = match result {
        Ok(_) => None,
        Err(_) => Some(ErrorCode::Unknown),
    };
    Ok(Response::Heartbeat(HeartbeatResponse { error_code }))
}