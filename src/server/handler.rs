use anyhow::Result;

use crate::broker::Broker;
use crate::error::protocol::ProtocolError;
use crate::error::storage::StorageError;
use crate::common::metadata::TopicPartition;
use crate::kafka::protocol::*;
use crate::kafka::message::RecordBatch;
use crate::kafka::codec::Decode;
use crate::kafka::{Request, RequestType, Response, ResponseHeader, ResponseType};
use bytes::BytesMut;

// todo: broker是否可以换为&Arc<Broker>
pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let correlation_id = request.header.correlation_id;
    let api_key = request.header.api_key;
    let api_version = request.header.api_version;

    let response_type = match request.request_type {
        RequestType::Produce(req) => handle_produce(&req, broker).await?,
        RequestType::Fetch(req) => handle_fetch(&req, broker).await?,
        RequestType::ListOffsets(req) => handle_list_offsets(&req, broker).await?,
        RequestType::ApiVersions(req) => handle_api_versions(&req, broker).await?,
        RequestType::Metadata(req) => handle_metadata(&req, broker).await?,
    };

    Ok(Response {
        header: ResponseHeader { correlation_id },
        response_type,
        api_key,
        api_version,
    })
}

fn map_produce_error(e: &anyhow::Error) -> i16 {
    // 2 = CORRUPT_MESSAGE, 17 = INVALID_RECORD, 3 = UNKNOWN_TOPIC_OR_PARTITION
    if let Some(pe) = e.downcast_ref::<ProtocolError>() {
        match pe {
            ProtocolError::InvalidCrc | ProtocolError::InvalidRecordBatchCrc => 2,
            ProtocolError::UnsupportedMagicByte(_)
            | ProtocolError::InvalidRecordBatchFormat
            | ProtocolError::InvalidRecordBatchField(_) => 17,
            _ => 17,
        }
    } else if let Some(se) = e.downcast_ref::<StorageError>() {
        match se {
            StorageError::DataCorruption => 2,
            _ => 17,
        }
    } else {
        let msg = e.to_string();
        if msg.contains("Topic not found") || msg.contains("Partition not found") {
            3
        } else if msg.contains("Record count mismatch") {
            17
        } else {
            17
        }
    }
}

async fn handle_api_versions(_req: &ApiVersionsRequest, _broker: &Broker) -> Result<ResponseType> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 0, min_version: 0, max_version: 9, ..Default::default() }, // Produce
            ApiVersion { api_key: 1, min_version: 0, max_version: 12, ..Default::default() }, // Fetch
            ApiVersion { api_key: 2, min_version: 1, max_version: 10, ..Default::default() }, // ListOffsets
            ApiVersion { api_key: 3, min_version: 0, max_version: 12, ..Default::default() }, // Metadata
            ApiVersion { api_key: 18, min_version: 0, max_version: 3, ..Default::default() },// ApiVersions
        ],
        throttle_time_ms: 0,
        ..Default::default()
    };
    Ok(ResponseType::ApiVersions(response))
}

async fn handle_metadata(req: &MetadataRequest, broker: &Broker) -> Result<ResponseType> {
    // The new MetadataRequest has a `topics` field of type `Vec<MetadataRequestTopic>`.
    // An empty vector means the client is requesting metadata for all topics.
    let topic_names_to_fetch: Vec<String> = req.topics.as_deref().unwrap_or_default()
        .iter()
        .filter_map(|t| t.name.clone())
        .collect();

    let topics_meta = if topic_names_to_fetch.is_empty() && req.topics.is_some() {
        // Case where client requested specific topics but all were null
        Default::default()
    } else if topic_names_to_fetch.is_empty() {
        broker.get_all_topics_metadata().await?
    } else {
        broker.get_topics_metadata(&topic_names_to_fetch).await?
    };

    let kafka_topics: Vec<MetadataResponseTopic> = topics_meta.into_iter().map(|(topic_name, topic_meta)| {
        let partitions: Vec<MetadataResponsePartition> = topic_meta.partitions.into_iter().map(|(partition_id, partition_meta)| {
            MetadataResponsePartition {
                error_code: 0,
                partition_index: partition_id as i32,
                leader_id: partition_meta.leader as i32,
                leader_epoch: partition_meta.leader_epoch,
                replica_nodes: partition_meta.replicas.into_iter().map(|r| r as i32).collect(),
                isr_nodes: partition_meta.isr.into_iter().map(|i| i as i32).collect(),
                ..Default::default()
            }
        }).collect();

        MetadataResponseTopic {
            error_code: 0,
            name: Some(topic_name),
            partitions,
            ..Default::default()
        }
    }).collect();

    let response = MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: 0,
            host: "localhost".to_string(),
            port: 8080,
            ..Default::default()
        }],
        controller_id: 0, // Hardcoding controller ID for now
        topics: kafka_topics,
        cluster_id: Some("bifrost-cluster".to_string()), // Assuming a fixed cluster_id
        ..Default::default()
    };
    println!("[DEBUG] Sending MetadataResponse: {:?}", response);
    Ok(ResponseType::Metadata(response))
}


async fn handle_produce(req: &ProduceRequest, broker: &Broker) -> Result<ResponseType> {
    let mut response_topics: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());

    for topic_data in &req.topic_data {
        let topic_name = topic_data.name.clone();
        let mut partition_responses: Vec<PartitionProduceResponse> = Vec::with_capacity(topic_data.partition_data.len());

        for p_data in &topic_data.partition_data {
            let tp = TopicPartition {
                topic: topic_name.clone(),
                partition: p_data.index as u32,
            };

            // `p_data.records` is `Option<Bytes>`. This is the raw RecordBatch.
            if let Some(record_batch_bytes) = &p_data.records {
                // The storage engine expects `&[Vec<u8>]`. We have `Bytes`.
                // So we convert `Bytes` to `Vec<u8>` and wrap it in a slice.
                // This is the correct way to pass the raw batch to the storage layer.
                match broker.append_batch(&tp, record_batch_bytes.clone(), 1).await {
                    Ok(offset) => {
                        partition_responses.push(PartitionProduceResponse{
                            index: p_data.index,
                            error_code: 0,
                            base_offset: offset as i64, // The broker returns the last offset of the batch
                            ..Default::default()
                        });
                    }
                    Err(e) => {
                        eprintln!(
                            "Error appending batch for topic {}-{}: {}",
                            topic_name, p_data.index, e
                        );
                        // Map errors to Kafka error codes:
                        // 2 = CORRUPT_MESSAGE, 17 = INVALID_RECORD, 3 = UNKNOWN_TOPIC_OR_PARTITION
                        let code: i16 = map_produce_error(&e);
                        partition_responses.push(PartitionProduceResponse {
                            index: p_data.index,
                            error_code: code,
                            base_offset: -1,
                            ..Default::default()
                        });
                    }
                }
            } else {
                 // No records for this partition, still need to send a response.
                 partition_responses.push(PartitionProduceResponse{
                    index: p_data.index,
                    error_code: 0,
                    base_offset: -1, // No records appended
                    ..Default::default()
                });
            }
        }
        response_topics.push(TopicProduceResponse {
            name: topic_name,
            partition_responses,
            ..Default::default()
        });
    }
    
    Ok(ResponseType::Produce(ProduceResponse {
        responses: response_topics,
        throttle_time_ms: 0,
        ..Default::default()
    }))
}

async fn handle_fetch(req: &FetchRequest, broker: &Broker) -> Result<ResponseType> {
    let mut topic_responses: Vec<FetchableTopicResponse> = Vec::with_capacity(req.topics.len());

    for t in &req.topics {
        let topic_name = t.topic.clone();
        let mut partition_responses: Vec<PartitionData> = Vec::with_capacity(t.partitions.len());

        for p in &t.partitions {
            let tp = TopicPartition {
                topic: topic_name.clone(),
                partition: p.partition as u32,
            };

            if p.fetch_offset < 0 {
                partition_responses.push(PartitionData {
                    partition_index: p.partition,
                    error_code: 1,
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    ..Default::default()
                });
                continue;
            }

            let start_offset = p.fetch_offset as u64;
            let max_bytes = if p.partition_max_bytes > 0 {
                p.partition_max_bytes as usize
            } else {
                req.max_bytes.max(0) as usize
            };

            match broker.read_batch(&tp, start_offset, max_bytes).await {
                Ok(batches) => {
                    if batches.is_empty() {
                        partition_responses.push(PartitionData{
                            partition_index: p.partition,
                            error_code: 0,
                            high_watermark: p.fetch_offset,
                            last_stable_offset: p.fetch_offset,
                            log_start_offset: 0,
                            records: Some(bytes::Bytes::new()),
                            ..Default::default()
                        });
                    } else {
                        let mut last_batch_bytes = batches.last().unwrap().clone();
                        let record_batch = RecordBatch::decode(&mut last_batch_bytes, 0)?;
                        let next_offset = record_batch.base_offset + record_batch.last_offset_delta as i64 + 1;

                        let mut buf = BytesMut::new();
                        for b in batches {
                            buf.extend_from_slice(&b);
                        }
                        let records_bytes = buf.freeze();

                        partition_responses.push(PartitionData {
                            partition_index: p.partition,
                            error_code: 0,
                            high_watermark: next_offset,
                            last_stable_offset: next_offset,
                            log_start_offset: 0,
                            records: Some(records_bytes),
                            ..Default::default()
                        });
                    }
                }
                Err(e) => {
                    let (code, hw, lso) = if let Some(se) = e.downcast_ref::<StorageError>() {
                        match se {
                            StorageError::InvalidOffset => (1i16, -1i64, -1i64), // OFFSET_OUT_OF_RANGE
                            _ => (1i16, -1i64, -1i64),
                        }
                    } else {
                        let msg = e.to_string();
                        if msg.contains("Topic not found") || msg.contains("Partition not found") {
                            (3i16, -1i64, -1i64) // UNKNOWN_TOPIC_OR_PARTITION
                        } else {
                            (1i16, -1i64, -1i64)
                        }
                    };

                    partition_responses.push(PartitionData {
                        partition_index: p.partition,
                        error_code: code,
                        high_watermark: hw,
                        last_stable_offset: lso,
                        log_start_offset: 0,
                        records: Some(bytes::Bytes::new()),
                        ..Default::default()
                    });
                }
            }
        }

        topic_responses.push(FetchableTopicResponse {
            topic: topic_name,
            partitions: partition_responses,
            ..Default::default()
        });
    }

    let response = FetchResponse {
        responses: topic_responses,
        ..Default::default()
    };
    Ok(ResponseType::Fetch(response))
}

async fn handle_list_offsets(req: &ListOffsetsRequest, broker: &Broker) -> Result<ResponseType> {
    // 语义：ListOffsets v1+，每个分区只返回一个 offset
    // Timestamp 约定：-2=earliest, -1=latest，其它时间戳暂返回 OFFSET_NOT_AVAILABLE(103)
    let mut topic_responses: Vec<ListOffsetsTopicResponse> = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let mut part_responses: Vec<ListOffsetsPartitionResponse> = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let tp = TopicPartition { topic: t.name.clone(), partition: p.partition_index as u32 };
            let (error_code, timestamp, offset, leader_epoch) = if p.timestamp == -2 { // earliest
                match broker.earliest_offset(&tp).await {
                    Ok(off) => (0i16, -1i64, off as i64, p.current_leader_epoch),
                    Err(e) => {
                        let msg = e.to_string();
                        let code = if msg.contains("Topic not found") || msg.contains("Partition not found") { 3i16 } else { 1i16 };
                        (code, -1, -1, p.current_leader_epoch)
                    }
                }
            } else if p.timestamp == -1 { // latest
                match broker.latest_offset(&tp).await {
                    Ok(off) => (0i16, -1i64, off as i64, p.current_leader_epoch),
                    Err(e) => {
                        let msg = e.to_string();
                        let code = if msg.contains("Topic not found") || msg.contains("Partition not found") { 3i16 } else { 1i16 };
                        (code, -1, -1, p.current_leader_epoch)
                    }
                }
            } else {
                // 暂不实现按时间戳查找，返回 OFFSET_NOT_AVAILABLE(103)
                (103i16, p.timestamp, -1i64, p.current_leader_epoch)
            };

            part_responses.push(ListOffsetsPartitionResponse{
                partition_index: p.partition_index,
                error_code,
                timestamp,
                offset,
                leader_epoch,
                ..Default::default()
            });
        }
        topic_responses.push(ListOffsetsTopicResponse{ name: t.name.clone(), partitions: part_responses, ..Default::default() });
    }

    let resp = ListOffsetsResponse { topics: topic_responses, ..Default::default() };
    Ok(ResponseType::ListOffsets(resp))
}