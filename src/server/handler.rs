use anyhow::Result;

use crate::broker::Broker;
use crate::common::metadata::TopicPartition;
use crate::kafka::protocol::*;
use crate::kafka::{Request, RequestType, Response, ResponseHeader, ResponseType};

// todo: broker是否可以换为&Arc<Broker>
pub async fn handle_request(request: Request, broker: &Broker) -> Result<Response> {
    let correlation_id = request.header.correlation_id;
    let api_key = request.header.api_key;
    let api_version = request.header.api_version;

    let response_type = match request.request_type {
        RequestType::Produce(req) => handle_produce(&req, broker).await?,
        RequestType::Fetch(req) => handle_fetch(&req, broker).await?,
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

async fn handle_api_versions(_req: &ApiVersionsRequest, _broker: &Broker) -> Result<ResponseType> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 0, min_version: 0, max_version: 9, ..Default::default() }, // Produce
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
                        partition_responses.push(PartitionProduceResponse {
                            index: p_data.index,
                            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
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
    let response = FetchResponse {
        responses: vec![],
        ..Default::default()
    };
    Ok(ResponseType::Fetch(response))
}