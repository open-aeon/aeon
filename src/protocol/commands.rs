use crate::{
    broker::{
        partition::PartitionManager,
        consumer_group::ConsumerGroupManager,
    },
    common::metadata::TopicPartition,
    protocol::{
        FetchRequest, FetchResponse, MetadataRequest, MetadataResponse,
        PartitionMetadata, ProduceRequest, ProduceResponse, Request, Response, TopicMetadata,
        CommitOffsetRequest, CommitOffsetResponse,
        JoinGroupRequest, JoinGroupResponse,
        LeaveGroupRequest, LeaveGroupResponse,
    },
    server::{
        producer_ack::ProducerAckManager,
        consumer_ack::ConsumerAckManager,
    },
};
use anyhow::Result;

pub async fn handle_request(
    request: Request,
    partition_manager: &PartitionManager,
    consumer_group_manager: &mut ConsumerGroupManager,
    producer_ack_manager: &ProducerAckManager,
    consumer_ack_manager: &ConsumerAckManager,
) -> Result<Response> {
    match request {
        Request::Produce(req) => handle_produce(req, partition_manager, producer_ack_manager).await,
        Request::Fetch(req) => handle_fetch(req, partition_manager, consumer_group_manager, consumer_ack_manager).await,
        Request::Metadata(req) => handle_metadata(req, partition_manager, consumer_group_manager).await,
        Request::CommitOffset(req) => handle_commit_offset(req, consumer_group_manager).await,
        Request::JoinGroup(req) => handle_join_group(req, consumer_group_manager).await,
        Request::LeaveGroup(req) => handle_leave_group(req, consumer_group_manager).await,
        Request::Heartbeat => {
            Ok(Response::Heartbeat)
        }
    }
}

async fn handle_produce(
    req: ProduceRequest,
    partition_manager: &PartitionManager,
    producer_ack_manager: &ProducerAckManager,
) -> Result<Response> {
    println!("处理生产请求: topic={}, partition={}", req.topic, req.partition);
    let topic_partition = TopicPartition {
        topic: req.topic.clone(),
        partition: req.partition,
    };

    let mut last_logical_offset = 0;
    let mut last_physical_offset = 0;
    for protocol_message in req.messages {
        println!("写入消息: content={:?}", 
            String::from_utf8_lossy(&protocol_message.content));
        let (logical_offset, physical_offset) = partition_manager
            .append_message(&topic_partition, protocol_message.clone())
            .await?;
        println!("消息写入成功，逻辑偏移量: {}, 物理偏移量: {}", logical_offset, physical_offset);
        
        // 添加消息到待确认列表
        producer_ack_manager.add_pending_message(protocol_message).await;
        
        last_logical_offset = logical_offset;
        last_physical_offset = physical_offset;
    }

    Ok(Response::Produce(ProduceResponse {
        topic: req.topic,
        partition: req.partition,
        base_offset: last_logical_offset as u64,
        physical_offset: last_physical_offset as u64,
    }))
}

async fn handle_fetch(
    req: FetchRequest,
    partition_manager: &PartitionManager,
    consumer_group_manager: &ConsumerGroupManager,
    consumer_ack_manager: &ConsumerAckManager,
) -> Result<Response> {
    println!("处理获取请求: topic={}, partition={}, group_id={}, consumer_id={}", 
        req.topic, req.partition, req.group_id, req.consumer_id);
    
    // 检查分区是否分配给当前消费者
    let assigned_partitions = consumer_group_manager
        .get_assigned_partitions(&req.group_id, &req.consumer_id)
        .await?;

    if !assigned_partitions.contains(&req.partition) {
        println!("分区 {} 未分配给消费者 {}", req.partition, req.consumer_id);
        return Ok(Response::Fetch(FetchResponse {
            topic: req.topic,
            partition: req.partition,
            messages: Vec::new(),
            next_offset: req.offset,
            group_id: req.group_id,
            consumer_id: req.consumer_id,
        }));
    }
    
    // 获取消费者组的偏移量
    let offset = consumer_group_manager
        .get_offset(&req.group_id, &req.topic, req.partition)
        .await?;
    
    let topic_partition = TopicPartition {
        topic: req.topic.clone(),
        partition: req.partition,
    };

    let mut messages = Vec::new();
    let mut current_offset = offset as i64;
    let mut total_bytes = 0;
    let mut next_offset = current_offset;

    while total_bytes < req.max_bytes {
        if let Some(message) = partition_manager
            .read_message(&topic_partition, current_offset)
            .await?
        {
            println!("读取到消息: offset={}, content={:?}", 
                current_offset,
                String::from_utf8_lossy(&message.content));
            total_bytes += message.content.len() as u32;
            
            // 记录消息状态
            if let Err(e) = consumer_ack_manager.handle_ack(&message.id, crate::protocol::ack::AckAction::Commit).await {
                return Err(anyhow::anyhow!("处理消息确认失败: {}", e));
            }
            
            messages.push(message);
            next_offset = current_offset + 1;
            current_offset = next_offset;
        } else {
            println!("在偏移量 {} 没有找到消息", current_offset);
            break;
        }
    }

    println!("返回 {} 条消息，下一条消息位置: {}", messages.len(), next_offset);
    Ok(Response::Fetch(FetchResponse {
        topic: req.topic,
        partition: req.partition,
        messages,
        next_offset: next_offset as u64,
        group_id: req.group_id,
        consumer_id: req.consumer_id,
    }))
}

async fn handle_metadata(
    req: MetadataRequest,
    partition_manager: &PartitionManager,
    _consumer_group_manager: &ConsumerGroupManager,
) -> Result<Response> {
    let mut topics = Vec::new();

    for topic_name in req.topics {
        // 从分区管理器获取分区信息
        let partitions: Vec<PartitionMetadata> = partition_manager
            .get_partitions(&topic_name)
            .await?
            .into_iter()
            .map(|p| PartitionMetadata {
                id: p.partition,
                leader: 0,
                replicas: vec![0],
                isr: vec![0],
            })
            .collect();

        topics.push(TopicMetadata {
            name: topic_name,
            partitions,
        });
    }

    Ok(Response::Metadata(MetadataResponse { topics }))
}

async fn handle_commit_offset(
    req: CommitOffsetRequest,
    consumer_group_manager: &mut ConsumerGroupManager,
) -> Result<Response> {
    println!("处理提交偏移量请求: group_id={}, topic={}, partition={}, offset={}, consumer_id={}",
        req.group_id, req.topic, req.partition, req.offset, req.consumer_id);

    match consumer_group_manager
        .commit_offset(
            &req.group_id,
            &req.topic,
            req.partition,
            req.offset,
            &req.consumer_id,
        )
        .await
    {
        Ok(()) => Ok(Response::CommitOffset(CommitOffsetResponse {
            group_id: req.group_id,
            topic: req.topic,
            partition: req.partition,
            error: None,
        })),
        Err(e) => Ok(Response::CommitOffset(CommitOffsetResponse {
            group_id: req.group_id,
            topic: req.topic,
            partition: req.partition,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_join_group(
    req: JoinGroupRequest,
    consumer_group_manager: &ConsumerGroupManager,
) -> Result<Response> {
    println!("消费者 {} 加入组 {}", req.consumer_id, req.group_id);
    
    // 添加消费者到组
    consumer_group_manager.add_consumer(&req.group_id, &req.consumer_id).await?;
    
    // 为每个主题分配分区
    let mut assigned_partitions = Vec::new();
    for topic in &req.topics {
        let partitions = consumer_group_manager
            .assign_partitions(&req.group_id, topic, &req.consumer_id, 4) // 假设每个主题有4个分区
            .await?;
        assigned_partitions.extend(partitions);
    }

    Ok(Response::JoinGroup(JoinGroupResponse {
        group_id: req.group_id,
        consumer_id: req.consumer_id,
        assigned_partitions,
        error: None,
    }))
}

async fn handle_leave_group(
    req: LeaveGroupRequest,
    consumer_group_manager: &ConsumerGroupManager,
) -> Result<Response> {
    println!("消费者 {} 退出组 {}", req.consumer_id, req.group_id);
    
    // 从组中移除消费者
    consumer_group_manager.remove_consumer(&req.group_id, &req.consumer_id).await?;

    Ok(Response::LeaveGroup(LeaveGroupResponse {
        group_id: req.group_id,
        consumer_id: req.consumer_id,
        error: None,
    }))
} 