use anyhow::Result;
use std::collections::HashMap;

use crate::broker::Broker;
use crate::common::metadata::TopicPartition;
use crate::kafka::codec::{CompactVec, Encode};
use crate::kafka::request::{Request, RequestType};
use crate::kafka::response::{
    ApiKey, ApiVersionsResponse, Broker as KafkaBroker, MetadataResponse, Partition as KafkaPartition,
    ProduceResponse, Response, ResponseHeader, ResponseType, Topic as KafkaTopic,
};
use crate::kafka::*;

// todo: broker是否可以换为&Arc<Broker>
pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let correlation_id = request.header.correlation_id;
    let api_key = request.header.api_key;
    let api_version = request.header.api_version;
    
    let response_type = match request.request_type {
        RequestType::ApiVersions(_req) => handle_api_versions(_req, broker).await?,
        RequestType::Metadata(req) => handle_metadata(req, broker).await?,
        RequestType::Produce(req) => handle_produce(req, broker).await?,
    };

    Ok(Response {
        header: ResponseHeader {
            correlation_id,
        },
        response_type,
        api_key,
        api_version,
    })
}

async fn handle_api_versions(_: ApiVersionsRequest, _: &Broker) -> Result<ResponseType> {
    // 返回服务器支持的所有API版本
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            // Produce API
            ApiKey { key: 0, min_version: 0, max_version: 9 },
            // Metadata API  
            ApiKey { key: 3, min_version: 0, max_version: 9 },
            // ApiVersions API
            ApiKey { key: 18, min_version: 2, max_version: 3 },
        ],
        throttle_time_ms: 0,
    };
    Ok(ResponseType::ApiVersions(response))
}

async fn handle_metadata(req: MetadataRequest, broker: &Broker) -> Result<ResponseType> {
    let topics_meta = if let Some(topics) = &req.topics {
        broker.get_topics_metadata(&topics[..]).await?
    } else {
        broker.get_all_topics_metadata().await?
    };
    
    let kafka_topics = topics_meta.into_iter().map(|(topic_name, topic_meta)| {
        let partitions = topic_meta.partitions.into_iter().map(|(partition_id, partition_meta)| {
            KafkaPartition {
                error_code: 0,
                partition_index: partition_id as i16,
                leader_id: partition_meta.leader as i32,
                leader_epoch: partition_meta.leader_epoch,
                replica_nodes: partition_meta.replicas.into_iter().map(|r| r as i32).collect(),
                isr_nodes: partition_meta.isr.into_iter().map(|i| i as i32).collect(),
                offline_replicas: Vec::new(),
            }
        }).collect();

        KafkaTopic {
            error_code: 0,
            name: topic_name,
            is_internal: false,
            partitions,
        }
    }).collect();

    let response = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![KafkaBroker {
            node_id: 0,
            host: "localhost".to_string(),
            port: 8080,
            rack: None,
        }],
        cluster_id: Some("bifrost-cluster".to_string()),
        controller_id: 0,
        topics: kafka_topics,
    };
    Ok(ResponseType::Metadata(response))
}

async fn handle_produce(req: ProduceRequest, broker: &Broker) -> Result<ResponseType> {
    let mut response_topics = HashMap::new();
    for (topic_name, topic_data) in req.topics {
        let mut partition_responses = Vec::new();
        for p_data in topic_data.partitions {
            let tp = TopicPartition {
                topic: topic_name.clone(),
                partition: p_data.index as u32,
            };
            
            let messages : std::result::Result<Vec<Vec<u8>>, _> = p_data.records.records
                .iter().map(|record| record.encode_to_vec(req.api_version)).collect();
            let message_to_store = match messages {
                Ok(messages) => messages,
                Err(e) => {
                    eprintln!("FATAL: Failed to encode record for storage: {}", e);
                    partition_responses.push(PartitionProduceResponse{
                        partition_index: p_data.index,
                        error_code: 1,
                        base_offset: -1,
                        log_append_time_ms: 0,
                        log_start_offset: 0,
                    });
                    continue;
                }
            };
            
            if message_to_store.is_empty() {
                partition_responses.push(PartitionProduceResponse{
                    partition_index: p_data.index,
                    error_code: 1,
                    base_offset: -1,
                    log_append_time_ms: 0,
                    log_start_offset: 0,
                });
                continue;
            }

            match broker.append_batch(&tp, &message_to_store).await {
                Ok(offset) => {
                    let num_messages = message_to_store.len() as u64;
                    let base_offset = if num_messages > 0 {
                        offset - (num_messages - 1)
                    } else {
                        offset
                    };
                    partition_responses.push(PartitionProduceResponse{
                        partition_index: p_data.index,
                        error_code: 0,
                        base_offset: base_offset as i64,
                        log_append_time_ms: 0,
                        log_start_offset: 0,
                    });
                }
                Err(e) => {
                    eprintln!(
                        "Error appending batch for topic {}-{}: {}",
                        topic_name, p_data.index, e
                    );
                    partition_responses.push(PartitionProduceResponse {
                        partition_index: p_data.index,
                        error_code: 3, // 3 = UNKNOWN_TOPIC_OR_PARTITION, a safe default
                        base_offset: -1,
                        log_append_time_ms: 0,
                        log_start_offset: 0,
                    });
                }
            }
        }
        response_topics.insert(topic_name, TopicProduceResponse {
            partitions: partition_responses,
        });
    }
    Ok(ResponseType::Produce(ProduceResponse {
        topic_data: response_topics,
        throttle_time_ms: 0,
    }))
}
/* 
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

// async fn handle_metadata(req: MetadataRequest, broker: &Broker) -> Result<Response> {
//     let topics_meta = broker.get_topics_meta().await?;
//     let requested_topics = if req.topics.is_empty() {
//         topics_meta.keys().cloned().collect()
//     } else {
//         req.topics
//     };

//     let broker_metadata = broker.metadata();
//     let brokers = vec![BrokerMetadata{
//         id: broker_metadata.id,
//         host: broker_metadata.host,
//         port: broker_metadata.port,
//     }];

//     let mut topics = Vec::new();
//     for topic_name in requested_topics {
//         if let Some(topic_meta) = topics_meta.get(&topic_name) {
//             let partitions = (0..topic_meta.partitions.len())
//                 .map(|p_id| PartitionMetadata {
//                     id: p_id as u32,
//                     leader: 0, // todo: hardcoded leader
//                     replicas: vec![0],
//                     isr: vec![0],
//                 })
//                 .collect();

//             topics.push(TopicMetadata {
//                 name: topic_name.clone(),
//                 partitions,
//             });
//         }
//     }

//     Ok(Response::Metadata(MetadataResponse { topics, brokers }))
// }

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
    let result = broker.join_group(
        req.group_id.clone(), 
        req.member_id, 
        req.session_timeout_ms, 
        req.rebalance_timeout_ms, 
        req.topics, 
        req.supported_protocols
    ).await;
    match result {
        Ok(result) => {
            Ok(Response::JoinGroup(JoinGroupResponse {
                error_code: None,
                result,
            }))
        }
        Err(_e) => {
            Ok(Response::JoinGroup(JoinGroupResponse {
                error_code: Some(ErrorCode::Unknown),
                result: JoinGroupResult {
                    generation_id: 0,
                    member_id: "".to_string(),
                    members: vec![],
                    protocol: None,
                    leader_id: "".to_string(),
                },
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
        result: LeaveGroupResult { error_code },
        error_code,
    }))
}

async fn handle_heartbeat(req: HeartbeatRequest, broker: &Broker) -> Result<Response> {
    let result = broker.heartbeat(&req.group_id, &req.member_id).await;
    let error_code = match result {
        Ok(_) => None,
        Err(_) => Some(ErrorCode::Unknown),
    };
    Ok(Response::Heartbeat(HeartbeatResponse {
        result: HeartbeatResult { error_code },
        error_code,
    }))
}

async fn handle_sync_group(req: SyncGroupRequest, broker: &Broker) -> Result<Response> {
    let result = broker.sync_group(req.group_id, req.member_id, req.generation_id, req.assignment).await;
    let error_code = match result {
        Ok(_) => None,
        Err(_) => Some(ErrorCode::Unknown),
    };
    Ok(Response::SyncGroup(SyncGroupResponse {
        result: SyncGroupResult { assignment: vec![] },
        error_code,
    }))
}

    */