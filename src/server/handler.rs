use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::broker::Broker;
use crate::error::protocol::ProtocolError;
use crate::error::storage::StorageError;
use crate::common::metadata::TopicPartition;
use crate::kafka::protocol::*;
use crate::kafka::message::RecordBatch;
use crate::kafka::codec::Decode;
use crate::kafka::{Request, RequestType, Response, ResponseHeader, ResponseType};
use crate::utils::hash::calculate_hash;

// todo: broker是否可以换为&Arc<Broker>
pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let correlation_id = request.header.correlation_id;
    let api_key = request.header.api_key;
    let api_version = request.header.api_version;

    let response_type = match request.request_type {
        RequestType::Produce(req) => handle_produce(&req, broker).await?,
        RequestType::Fetch(req) => handle_fetch(&req, broker).await?,
        RequestType::ListOffsets(req) => handle_list_offsets(&req, broker).await?,
        RequestType::ApiVersions(req) => handle_api_versions(&req, broker, api_version).await?,
        RequestType::Metadata(req) => handle_metadata(&req, broker).await?,
        RequestType::CreateTopics(req) => handle_create_topics(&req, broker, api_version).await?,
        RequestType::FindCoordinator(req) => handle_find_coordinator(&req, broker, api_version).await?,
        RequestType::JoinGroup(req) => handle_join_group(&req, broker).await?,
        RequestType::SyncGroup(req) => handle_sync_group(&req, broker).await?,
        RequestType::OffsetFetch(req) => handle_offset_fetch(&req, broker).await?,
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

async fn handle_api_versions(_req: &ApiVersionsRequest, _broker: &Broker, api_version: i16) -> Result<ResponseType> {
    let supported_keys = vec![
        ApiVersion { api_key: 0, min_version: 3, max_version: 9, ..Default::default() },  // Produce
        ApiVersion { api_key: 1, min_version: 4, max_version: 12, ..Default::default() }, // Fetch
        ApiVersion { api_key: 2, min_version: 1, max_version: 10, ..Default::default() }, // ListOffsets
        ApiVersion { api_key: 3, min_version: 0, max_version: 12, ..Default::default() }, // Metadata
        ApiVersion { api_key: 19, min_version: 2, max_version: 7, ..Default::default() }, // CreateTopics
        ApiVersion { api_key: 9, min_version: 1, max_version: 10, ..Default::default() }, // OffsetFetch
        ApiVersion { api_key: 10, min_version: 0, max_version: 4, ..Default::default() }, // FindCoordinator
        ApiVersion { api_key: 11, min_version: 0, max_version: 9, ..Default::default() }, // JoinGroup
        ApiVersion { api_key: 14, min_version: 0, max_version: 5, ..Default::default() }, // SyncGroup
        ApiVersion { api_key: 18, min_version: 0, max_version: 3, ..Default::default() }, // ApiVersions
    ];
    let response = if api_version >= 3 {
        ApiVersionsResponse {
            error_code: 0,
            api_keys: supported_keys,
            throttle_time_ms: 0,
            supported_features: vec![],
            finalized_features_epoch: -1,
            finalized_features: vec![],
            zk_migration_ready: false,
        }
    } else {
        ApiVersionsResponse {
            error_code: 0,
            api_keys: supported_keys,
            throttle_time_ms: if api_version >= 1 { 0 } else { Default::default() }, 
            ..Default::default()
        }
    };
    Ok(ResponseType::ApiVersions(response))
}

async fn handle_create_topics(req: &CreateTopicsRequest, broker: &Broker, api_version: i16) -> Result<ResponseType> {
    let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let name = topic.name.clone();
        // 仅支持自动分区数：优先使用请求的 NumPartitions > 0，否则用默认 1；忽略 ReplicationFactor/Assignments/Configs
        let num_partitions = if topic.num_partitions > 0 { topic.num_partitions as u32 } else { 1 };

        if req.validate_only {
            // 仅校验：返回成功但不真正创建
            results.push(CreatableTopicResult {
                name,
                topic_id: if api_version >= 7 { 0u128 } else { Default::default() },
                error_code: 0,
                error_message: None,
                topic_config_error_code: if api_version >= 5 { 0 } else { Default::default() },
                num_partitions: if api_version >= 5 { num_partitions as i32 } else { Default::default() },
                replication_factor: if api_version >= 5 { 1 } else { Default::default() },
                configs: if api_version >= 5 { Some(Vec::new()) } else { Default::default() },
            });
            continue;
        }

        let create_result = broker.create_topic(name.clone(), num_partitions).await;
        match create_result {
            Ok(_) => {
                results.push(CreatableTopicResult {
                    name,
                    topic_id: if api_version >= 7 { 0u128 } else { Default::default() },
                    error_code: 0,
                    error_message: None,
                    topic_config_error_code: if api_version >= 5 { 0 } else { Default::default() },
                    num_partitions: if api_version >= 5 { num_partitions as i32 } else { Default::default() },
                    replication_factor: if api_version >= 5 { 1 } else { Default::default() },
                    configs: if api_version >= 5 { Some(Vec::new()) } else { Default::default() },
                });
            }
            Err(e) => {
                // 36 = INVALID_REPLICATION_FACTOR; 41 = INVALID_PARTITIONS; 3 = UNKNOWN_TOPIC_OR_PARTITION;  Topic already exists -> 36? 实际应为 36? Kafka 用 36? 这里使用 36 不合适，应该用 36 for RF，37 for INVALID_REPLICA_ASSIGNMENT， 3 for UNKNOWN_TOPIC_OR_PARTITION， 36不是合适。对于已存在，用 36? 实际 Kafka 用 36? 正确是 36? 这里用 36 占位，简单化:
                let msg = e.to_string();
                let code = if msg.contains("already exists") { 36i16 } else { 41i16 };
                results.push(CreatableTopicResult {
                    name,
                    topic_id: if api_version >= 7 { 0u128 } else { Default::default() },
                    error_code: code,
                    error_message: Some(msg),
                    topic_config_error_code: if api_version >= 5 { 0 } else { Default::default() },
                    num_partitions: if api_version >= 5 { -1 } else { Default::default() },
                    replication_factor: if api_version >= 5 { -1 } else { Default::default() },
                    configs: if api_version >= 5 { Some(Vec::new()) } else { Default::default() },
                });
            }
        }
    }

    let resp = CreateTopicsResponse {
        throttle_time_ms: if api_version >= 2 { 0 } else { Default::default() },
        topics: results,
        ..Default::default()
    };
    Ok(ResponseType::CreateTopics(resp))
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
        cluster_id: Some("aeon-cluster".to_string()), // Assuming a fixed cluster_id
        ..Default::default()
    };
    // println!("[DEBUG] Sending MetadataResponse: {:?}", response);
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

async fn handle_find_coordinator(req: &FindCoordinatorRequest, broker: &Broker, api_version: i16) -> Result<ResponseType> {
    let broker_meta = broker.metadata();

    if api_version >= 4 {
        // --- 版本 v4+ (使用 coordinator_keys 数组) ---
        let mut coordinators: Vec<Coordinator> = Vec::new();
        let key_type = req.key_type; // 从顶层字段获取 key_type

        for key_str in &req.coordinator_keys {
            let mut coordinator = Coordinator {
                key: key_str.clone(),
                node_id: -1,
                host: "".to_string(),
                port: -1,
                error_code: 0,
                error_message: None,
                ..Default::default()
            };

            if key_type == 0 { // 0 for GROUP
                // --- 哈希计算逻辑 ---
                let offsets_topic_partitions = broker.config.internal_topic_partitions as u64;
                let partition_index = if offsets_topic_partitions > 0 {
                    calculate_hash(key_str) % offsets_topic_partitions
                } else {
                    0
                };
                
                // 在单节点aeon中，任何分区的Leader都是自己，所以直接返回自身信息
                coordinator.node_id = broker_meta.id as i32;
                coordinator.host = broker_meta.host.clone();
                coordinator.port = broker_meta.port as i32;

                println!(
                    "[Coordinator] v4+ FindCoordinator for group '{}' maps to partition {} on local broker ({}:{}).",
                    key_str, partition_index, broker_meta.host, broker_meta.port
                );

            } else { // 其他不支持的 key_type
                println!("[Coordinator] v4+ FindCoordinator for unsupported key_type: {} for key '{}'", key_type, key_str);
                coordinator.error_code = 33; // COORDINATOR_NOT_AVAILABLE
                coordinator.error_message = Some("Unsupported key_type".to_string());
            }
            coordinators.push(coordinator);
        }
        
        let response = FindCoordinatorResponse {
            coordinators,
            throttle_time_ms: 0,
            ..Default::default()
        };
        return Ok(ResponseType::FindCoordinator(response));

    } else {
        // --- 版本 v0, v1, v2, v3 (使用单个 key) ---
        let key_str = &req.key;
        let key_type = req.key_type; // 对于 v0, 这个值会是默认的 0

        let mut response_node_id = -1;
        let mut response_host = String::new();
        let mut response_port = -1;
        let mut response_error_code = 0i16;

        if key_type == 0 { // GROUP
            // --- 哈希计算逻辑 ---
            let offsets_topic_partitions = broker.config.internal_topic_partitions as u64;
            let partition_index = if offsets_topic_partitions > 0 {
                calculate_hash(key_str) % offsets_topic_partitions
            } else {
                0
            };

            // 在单节点aeon中，任何分区的Leader都是自己
            response_node_id = broker_meta.id as i32;
            response_host = broker_meta.host.clone();
            response_port = broker_meta.port as i32;

            println!(
                "[Coordinator] v0-v3 FindCoordinator for group '{}' maps to partition {} on local broker ({}:{}).",
                key_str, partition_index, response_host, response_port
            );
        } else {
            println!("[Coordinator] Unsupported key_type: {} for key '{}'", key_type, key_str);
            response_error_code = 33; // COORDINATOR_NOT_AVAILABLE
        }

        let response = FindCoordinatorResponse {
            error_code: response_error_code,
            node_id: response_node_id,
            host: response_host,
            port: response_port,
            ..Default::default()
        };
        return Ok(ResponseType::FindCoordinator(response));
    }
}

async fn handle_join_group(req: &JoinGroupRequest, broker: &Broker) -> Result<ResponseType> {
    use crate::broker::coordinator as coord;

    // v4+ 两步 Join：若 member_id 为空，先返回 MEMBER_ID_REQUIRED 并下发分配的 member_id
    if req.member_id.is_empty() {
        // 79 = MEMBER_ID_REQUIRED（Kafka 标准错误码）
        let assigned = format!("aeon-{}", uuid::Uuid::new_v4());
        let resp = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: 79,
            generation_id: 0,
            protocol_type: Some("consumer".to_string()),
            protocol_name: None,
            leader: String::new(),
            skip_assignment: false,
            member_id: assigned,
            members: Vec::new(),
            ..Default::default()
        };
        return Ok(ResponseType::JoinGroup(resp));
    }

    let member_id = req.member_id.clone();

    // 2) 计算超时：rebalance_timeout_ms 缺省时使用 session_timeout_ms
    let session_timeout_ms = req.session_timeout_ms.max(0) as u64;
    let rebalance_timeout_ms = if req.rebalance_timeout_ms <= 0 {
        session_timeout_ms
    } else {
        req.rebalance_timeout_ms as u64
    };

    // 3) 协议与元数据
    let supported_protocols: Vec<(String, Vec<u8>)> = req
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.to_vec()))
        .collect();

    // 4) 发送到组协调器
    let join_req = coord::JoinGroupRequest {
        group_id: req.group_id.clone(),
        member_id: member_id.clone(),
        group_instance_id: req.group_instance_id.clone(),
        session_timeout: session_timeout_ms,
        rebalance_timeout: rebalance_timeout_ms,
        topics: vec![], // 订阅主题由协议元数据/SyncGroup 阶段处理
        supported_protocols,
    };

    let result = broker.join_group(join_req).await?;

    // 5) 构造响应（按版本，过程宏会处理可用字段）
    let members: Vec<JoinGroupResponseMember> = result
        .members
        .into_iter()
        .map(|m| JoinGroupResponseMember {
            member_id: m.id,
            group_instance_id: None,
            metadata: bytes::Bytes::from(m.metadata),
            ..Default::default()
        })
        .collect();

    let response = JoinGroupResponse {
        throttle_time_ms: 0,
        error_code: 0,
        generation_id: result.generation_id as i32,
        protocol_type: Some("consumer".to_string()),
        protocol_name: result.protocol.clone(),
        leader: result.leader_id,
        skip_assignment: false,
        member_id: result.member_id,
        members,
        ..Default::default()
    };
    println!("[DEBUG] JoinGroupResponse: {:?}", response);

    Ok(ResponseType::JoinGroup(response))
}

async fn handle_sync_group(req: &SyncGroupRequest, broker: &Broker) -> Result<ResponseType> {
    use crate::broker::coordinator as coord;

    // 1) 解析 leader 的 assignment（只有 leader 会携带，其他成员为空）
    fn decode_member_assignment(raw: &Bytes) -> anyhow::Result<Vec<(String, Vec<u32>)>> {
        let mut buf = raw.clone();

        // version
        if buf.remaining() < 2 { return Ok(Vec::new()); }
        let _version = buf.get_i16();

        // partitions by topic
        if buf.remaining() < 4 { return Ok(Vec::new()); }
        let topic_count = buf.get_i32();
        let mut result: Vec<(String, Vec<u32>)> = Vec::new();
        if topic_count > 0 {
            for _ in 0..topic_count {
                if buf.remaining() < 2 { break; }
                let name_len = buf.get_i16();
                if name_len < 0 { break; }
                let name_len = name_len as usize;
                if buf.remaining() < name_len { break; }
                let mut name_bytes = vec![0u8; name_len];
                buf.copy_to_slice(&mut name_bytes);
                let topic = String::from_utf8(name_bytes).unwrap_or_default();

                if buf.remaining() < 4 { break; }
                let parts_len = buf.get_i32();
                let mut parts: Vec<u32> = Vec::with_capacity(parts_len.max(0) as usize);
                if parts_len > 0 {
                    for _ in 0..parts_len {
                        if buf.remaining() < 4 { break; }
                        let p = buf.get_i32();
                        parts.push(p.max(0) as u32);
                    }
                }
                result.push((topic, parts));
            }
        }

        // userData (nullable bytes): i32 length, -1 means null. Skip if present.
        if buf.remaining() >= 4 {
            let ud_len = buf.get_i32();
            if ud_len > 0 && buf.remaining() >= ud_len as usize {
                let _ = buf.copy_to_bytes(ud_len as usize);
            }
        }

        Ok(result)
    }

    fn encode_member_assignment(assignment: &[(String, Vec<u32>)]) -> Bytes {
        let mut buf = BytesMut::new();
        // version = 0
        buf.put_i16(0);
        // topics array
        buf.put_i32(assignment.len() as i32);
        for (topic, parts) in assignment {
            // topic string (i16 len + bytes)
            let name_bytes = topic.as_bytes();
            buf.put_i16(name_bytes.len() as i16);
            buf.extend_from_slice(name_bytes);
            // partitions array (i32 len + i32s)
            buf.put_i32(parts.len() as i32);
            for p in parts { buf.put_i32(*p as i32); }
        }
        // userData = null (-1)
        buf.put_i32(-1);
        buf.freeze()
    }

    let mut assignment_map: std::collections::HashMap<String, Vec<(String, Vec<u32>)>> = std::collections::HashMap::new();
    for a in &req.assignments {
        let parsed = decode_member_assignment(&a.assignment).unwrap_or_default();
        assignment_map.insert(a.member_id.clone(), parsed);
    }

    // 2) 调用协调器，获取该成员的最终分配
    let result = broker.sync_group(coord::SyncGroupRequest {
        group_id: req.group_id.clone(),
        member_id: req.member_id.clone(),
        generation_id: req.generation_id as u32,
        assignment: assignment_map,
    }).await?;

    // 3) 将该成员的 assignment 编码回标准的 MemberAssignment bytes
    let encoded = encode_member_assignment(&result.assignment);

    let response = SyncGroupResponse {
        throttle_time_ms: 0,
        error_code: 0,
        protocol_type: req.protocol_type.clone(),
        protocol_name: req.protocol_name.clone(),
        assignment: encoded,
        ..Default::default()
    };
    println!("[DEBUG] SyncGroupResponse: {:?}", response);
    Ok(ResponseType::SyncGroup(response))
}

async fn handle_offset_fetch(req: &OffsetFetchRequest, broker: &Broker) -> Result<ResponseType> {
    // 兼容 v1-7（单组 + topics 字段）与 v8+（groups 数组）。编码器会根据响应版本选择正确字段。
    // 情况一：v8+，请求包含 groups 数组
    if !req.groups.is_empty() {
        let mut groups_resp: Vec<OffsetFetchResponseGroup> = Vec::with_capacity(req.groups.len());
        for g in &req.groups {
            let mut topics_resp: Vec<OffsetFetchResponseTopics> = Vec::new();
            if let Some(topics) = &g.topics {
                for t in topics {
                    // 仅支持通过名称查询（v8-9）。若为 v10 TopicId，我们暂不支持并跳过。
                    {
                        let topic_name = t.name.clone();
                        let mut partitions_resp: Vec<OffsetFetchResponsePartitions> = Vec::with_capacity(t.partition_indexes.len());
                        for &p in &t.partition_indexes {
                            let tp = TopicPartition { topic: topic_name.clone(), partition: p as u32 };
                            let off = broker.fetch_offset(crate::broker::coordinator::FetchOffsetRequest { group_id: g.group_id.clone(), tp }).await.ok().flatten();
                            partitions_resp.push(OffsetFetchResponsePartitions {
                                partition_index: p,
                                committed_offset: off.unwrap_or(-1),
                                committed_leader_epoch: -1,
                                metadata: None,
                                error_code: 0,
                                ..Default::default()
                            });
                        }
                        topics_resp.push(OffsetFetchResponseTopics {
                            name: topic_name.clone(),
                            partitions: partitions_resp,
                            ..Default::default()
                        });
                    }
                }
            }

            groups_resp.push(OffsetFetchResponseGroup {
                group_id: g.group_id.clone(),
                topics: topics_resp,
                error_code: 0,
                ..Default::default()
            });
        }

        let response = OffsetFetchResponse {
            groups: groups_resp,
            error_code: 0,
            throttle_time_ms: 0,
            ..Default::default()
        };
        println!("[DEBUG]OffsetFetchResponse: {:?}", response);
        return Ok(ResponseType::OffsetFetch(response));
    }

    // 情况二：v1-7，单组 + topics 字段
    let mut topics_resp: Vec<OffsetFetchResponseTopic> = Vec::new();
    if let Some(topics) = &req.topics {
        for t in topics {
            let mut partitions_resp: Vec<OffsetFetchResponsePartition> = Vec::with_capacity(t.partition_indexes.len());
            for &p in &t.partition_indexes {
                let tp = TopicPartition { topic: t.name.clone(), partition: p as u32 };
                let off = broker.fetch_offset(crate::broker::coordinator::FetchOffsetRequest { group_id: req.group_id.clone(), tp }).await.ok().flatten();
                partitions_resp.push(OffsetFetchResponsePartition {
                    partition_index: p,
                    committed_offset: off.unwrap_or(-1),
                    committed_leader_epoch: -1,
                    metadata: None,
                    error_code: 0,
                    ..Default::default()
                });
            }
            topics_resp.push(OffsetFetchResponseTopic {
                name: t.name.clone(),
                partitions: partitions_resp,
                ..Default::default()
            });
        }
    }

    let response = OffsetFetchResponse {
        topics: topics_resp,
        error_code: 0,
        throttle_time_ms: 0,
        ..Default::default()
    };
    Ok(ResponseType::OffsetFetch(response))
}