use anyhow::{Context, Result};
use tokio::sync::watch;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, mpsc, oneshot};
use bytes::Bytes;
use chrono::Utc;

pub mod topic;
pub mod partition;

use crate::coordinator::*;
use crate::broker::topic::Topic;
use crate::common::metadata::{TopicMetadata, TopicPartition};
use crate::config::broker::BrokerConfig;
use crate::config::storage::StorageConfig;
use crate::coordinator::ConsumerGroup;
use crate::common::metadata::OffsetCommitMetadata;
use crate::error::consumer::ConsumerGroupError;
use crate::kafka::codec::Decode;
use crate::kafka::message::Record;
use crate::kafka::message::RecordBatch;
use crate::kafka::offsets::{OffsetKey, OffsetValue};
use crate::kafka::codec::Encode;
use crate::utils::hash::calculate_hash;

pub struct Broker {
    pub config: Arc<BrokerConfig>,
    pub storage_config: Arc<StorageConfig>,
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    metadata: BrokerMetadata,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::watch::Sender<()>>>>,
    coordinators: DashMap<String, mpsc::Sender<CoordinatorCommand>>,
}

#[derive(Clone)]
pub struct BrokerMetadata {
    pub id: u32,
    pub host: String,
    pub port: u16,
}

impl Broker {
    pub fn new(config: BrokerConfig, storage_config: StorageConfig) -> Self {
        let topics = Arc::new(RwLock::new(HashMap::new()));
        let metadata = BrokerMetadata {
            id: config.id,
            host: config.advertised_host.clone(),
            port: config.advertised_port,
        };
        Self { 
            config: Arc::new(config),
            storage_config: Arc::new(storage_config), 
            topics, 
            metadata: metadata,
            shutdown_tx: Arc::new(Mutex::new(None)),
            coordinators: DashMap::new(),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let(tx, _) = watch::channel(());
        *self.shutdown_tx.lock().await = Some(tx);

        let data_dir = &self.config.data_dir;
        std::fs::create_dir_all(data_dir)
        .with_context(|| format!("Failed to create data directory at {:?}", data_dir))?;

        println!("Starting broker, data directory at: {:?}", data_dir);

        let mut local_topics = HashMap::new();

        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(topic_name_os) = path.file_name() {
                    if let Some(topic_name) = topic_name_os.to_str() {
                        println!("Loading topic: {}", topic_name);

                        let engine = self.storage_config.engine;
                        match Topic::load(topic_name.to_string(), path.clone(), engine, &self.storage_config) {
                            Ok(topic) => {
                                local_topics.insert(topic_name.to_string(), topic);
                            }
                            Err(e) => {
                                eprintln!("Failed to load topic {}: {}", topic_name, e);
                            }
                        }
                    }
                }
            }
        }

        if !local_topics.is_empty() {
            self.topics.write().await.extend(local_topics);
        }

        if !self.topics.read().await.contains_key(&self.config.internal_topic_name) {
            self.create_topic(self.config.internal_topic_name.clone(), self.config.internal_topic_partitions).await?;
            println!("Created internal topic: {}", self.config.internal_topic_name);
        }

        println!("Loading consumer offsets...");
        if let Err(e) = self.start_coordinators().await {
            eprintln!("Failed to start coordinators: {}", e);
            return Err(e.into());
        }

        self.start_background_tasks().await?;

        Ok(())
    }

    async fn subscribe_shutdown(&self) -> Result<watch::Receiver<()>> {
        let tx = self.shutdown_tx.lock().await;
        if let Some(tx) = tx.as_ref() {
            Ok(tx.subscribe())
        } else {
            Err(anyhow::anyhow!("Shutdown channel not initialized"))
        }
    }

    pub async fn initiate_shutdown(&self) {
        if self.shutdown_tx.lock().await.take().is_none() {
            eprintln!("[Broker] Shutdown already initiated");
        }
    }

    async fn start_background_tasks(&self) -> Result<()> {
        self.start_flush_task().await?;
        self.start_cleanup_task().await?;
        Ok(())
    }

    async fn start_flush_task(&self) -> Result<()> {
        if let Some(flush_interval) = self.config.flush_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(flush_interval);
            let mut shutdown_rx = self.subscribe_shutdown().await?;

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);

                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let topics = topics.read().await;
                            for topic in topics.values() {
                                if let Err(e) = topic.flush_all().await {
                                    eprintln!("Failed to flush topic {}: {}", topic.name, e);
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            println!("[Broker] Received shutdown signal, exiting...");
                            break;
                        }
                    }

                    
                }
            });
            println!("Spawned periodic flush task with interval: {:?}", interval_duration);
        }
        Ok(())
    }

    async fn start_cleanup_task(&self) -> Result<()> {
        if let Some(cleanup_interval) = self.config.cleanup_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(cleanup_interval);
            let mut shutdown_rx = self.subscribe_shutdown().await?;

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);
                
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let topics = topics.read().await;
                            for topic in topics.values() {
                                if let Err(e) = topic.cleanup().await {
                                    eprintln!("Failed to cleanup topic {}: {}", topic.name, e);
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            println!("[Broker] Received shutdown signal, exiting...");
                            break;
                        }
                    }
                }
            });
            println!("Spawned periodic cleanup task with interval: {:?}", interval_duration);
        }
        Ok(())
    }

    pub fn metadata(&self) -> BrokerMetadata {
        self.metadata.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        println!("Shutting down broker");
        let topics = self.topics.read().await;
        for (name, topic) in topics.iter() {
            println!("Flushing topic: {}", name);
            topic.flush_all().await?
        }
        println!("Broker shutdown complete");
        Ok(())
    }

    pub async fn create_topic(&self, name: String, p_num: u32) -> Result<()> {
        if self.topics.read().await.contains_key(&name) {
            return Err(anyhow::anyhow!("Topic already exists: {}", name));
        }

        let engine = self.storage_config.engine;
        let path = self.config.data_dir.join(&name);
        // todo: potential race condition here, to be fixed
        let topic = Topic::create(name.clone(), path, p_num, engine, &self.storage_config)?;
        self.topics.write().await.insert(name, topic);
        Ok(())
    }

    pub async fn delete_topic(&self, name: String) -> Result<()> {
        let mut topics = self.topics.write().await;
        if let Some(topic) = topics.remove(&name) {
            drop(topics);
            topic.delete().await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", name))
        }
    }

    pub async fn append_batch(&self, tp: &TopicPartition, data: Bytes, record_count: u32) -> Result<u64> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let batch = RecordBatch::parse_header(&data)?;
        batch.verify_crc(&data)?;
        if batch.records_count as u32 != record_count {
            return Err(anyhow::anyhow!("Record count mismatch"));
        }

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            // 让存储层负责回填 base_offset；我们在响应里返回该 base_offset
            let assigned_base = topic.append_batch(p_id, data, record_count).await?;
            Ok(assigned_base)
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn read_batch(&self, tp: &TopicPartition, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.read_batch(p_id, start_offset, max_bytes).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn earliest_offset(&self, tp: &TopicPartition) -> Result<u64> {
        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&tp.topic) {
            topic.earliest_offset(tp.partition).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", tp.topic))
        }
    }

    pub async fn latest_offset(&self, tp: &TopicPartition) -> Result<u64> {
        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&tp.topic) {
            topic.latest_offset(tp.partition).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", tp.topic))
        }
    }

    pub async fn get_topics_metadata(&self, topics: &[String]) -> Result<HashMap<String, TopicMetadata>> {
        let all_topics = self.topics.read().await;
        let mut meta = HashMap::new();
        for topic_name in topics {
            if let Some(topic) = all_topics.get(topic_name) {
                meta.insert(topic_name.clone(), topic.meta().await);
            }
        }
        Ok(meta)
    }

    pub async fn get_all_topics_metadata(&self) -> Result<HashMap<String, TopicMetadata>> {
        let topics = self.topics.read().await;
        let mut meta = HashMap::new();
        for (name, topic) in topics.iter() {
            meta.insert(name.clone(), topic.meta().await);
        }
        Ok(meta)
    }

    pub async fn join_group(
        &self, 
        request: JoinGroupRequest
    ) -> Result<JoinGroupResult> {
        let group_id = request.group_id.clone();
        let coordinator_tx = self.coordinators.entry(group_id.clone()).or_insert_with(|| {
            GroupCoordinator::new(group_id.clone())
        }).clone();

        let (response_tx, response_rx) = oneshot::channel();

        let command = CoordinatorCommand::JoinGroup {
            request,
            response_tx,
        };

        if coordinator_tx.send(command).await.is_err() {
            self.coordinators.remove(&group_id);
            println!("Failed to join group: {}", group_id);
            return Err(anyhow::anyhow!("Failed to join group: {}", group_id));
        }

        match response_rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(anyhow::anyhow!("Failed to join group: {}", e)),
            Err(e) => Err(anyhow::anyhow!("Failed to join group: {}", e)),
        }
    }

    pub async fn leave_group(&self, request: LeaveGroupRequest) -> std::result::Result<(), ConsumerGroupError> {
        let group_id = request.group_id.clone();

        let coordinator_tx = match self.coordinators.get(&group_id) {
            Some(tx) => tx.clone(), 
            None => return Err(ConsumerGroupError::NotCoordinator),
        };


        let (response_tx, response_rx) = oneshot::channel();

        let command = CoordinatorCommand::LeaveGroup {
            request,
            response_tx,
        };

        if coordinator_tx.send(command).await.is_err() {
            self.coordinators.remove(&group_id);
            return Err(ConsumerGroupError::CoordinatorNotAvailable);
        }

        match response_rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_e) => Err(ConsumerGroupError::CoordinatorNotAvailable),
        }
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> std::result::Result<(), ConsumerGroupError> {
        let group_id = request.group_id.clone();
        let coordinator_tx = match self.coordinators.get(&group_id) {
            Some(tx) => tx.clone(),
            None => return Err(ConsumerGroupError::NotCoordinator),
        };

        let (response_tx, response_rx) = oneshot::channel();

        let command = CoordinatorCommand::Heartbeat {
            request,
            response_tx,
        };

        if coordinator_tx.send(command).await.is_err() {
            self.coordinators.remove(&group_id);
            return Err(ConsumerGroupError::CoordinatorNotAvailable);
        }
        
        match response_rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ConsumerGroupError::CoordinatorNotAvailable),
        }
    }

    pub async fn commit_offset(&self, request: CommitOffsetRequest) -> Vec<(TopicPartition, ConsumerGroupError)> {
        let group_id = request.group_id.clone();

        let mut errors: Vec<(TopicPartition, ConsumerGroupError)> = Vec::new();
        let mut success_tps: Vec<CommitOffsetTp> = Vec::new();
        for tp in request.tps.iter() {
            let message = OffsetCommitMetadata {
                group_id: group_id.clone(),
                topic: tp.topic.clone(),
                partition: tp.partition as u32,
                offset: tp.offset,
                timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            };

            let tp_key = TopicPartition {
                topic: tp.topic.clone(),
                partition: tp.partition,
            };

            if self.persist_offset(&message).await.is_err() {
                errors.push((tp_key, ConsumerGroupError::CoordinatorNotAvailable));
            } else {
                success_tps.push(CommitOffsetTp {
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                    offset: tp.offset,
                });
            }
        }

        if success_tps.is_empty() {
            return errors;
        }
    

        let coordinator_tx = self.coordinators.entry(group_id.clone()).or_insert_with(||
            GroupCoordinator::new(group_id.clone())).clone();

        let command_request = CommitOffsetRequest {
            group_id: group_id.clone(),
            tps: success_tps,
        };
        
        let command = CoordinatorCommand::CommitOffset {
            request: command_request,
        };

        if coordinator_tx.send(command).await.is_err() {
            for tp in request.tps.iter() {
                let key = TopicPartition {
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                };
                if !errors.iter().any(|(k, _)| k == &key) {
                    errors.push((key, ConsumerGroupError::CoordinatorNotAvailable));
                }
            }
        }
    
        errors
    }

    pub async fn fetch_offset(&self, request: FetchOffsetRequest) -> Result<Option<i64>> {
        let group_id = request.group_id.clone();
        let coordinator_tx = match self.coordinators.get(&group_id) {
            Some(tx) => tx.clone(), 
            None => return Err(anyhow::anyhow!("Coordinator not found for group: {}", group_id)),
        };

        let (response_tx, response_rx) = oneshot::channel();

        let command = CoordinatorCommand::FetchOffset {
            request,
            response_tx,
        };

        if coordinator_tx.send(command).await.is_err() {
            self.coordinators.remove(&group_id);
            return Err(anyhow::anyhow!("Failed to send fetch offset command to coordinator: {}", group_id));
        }

        match response_rx.await {
            Ok(Ok(offset)) => Ok(offset),
            Ok(Err(e)) => Err(anyhow::anyhow!("Failed to fetch offset: {}", e)),
            Err(e) => Err(anyhow::anyhow!("Failed to fetch offset: {}", e)),
        }
    }

    pub async fn sync_group(
        &self,
        request: SyncGroupRequest
    ) -> Result<SyncGroupResult> {
        let group_id = request.group_id.clone();
        let coordinator_tx = match self.coordinators.get(&group_id) {
            Some(tx) => tx.clone(), 
            None => return Err(anyhow::anyhow!("Coordinator not found for group: {}", group_id)),
        };

        let (response_tx, response_rx) = oneshot::channel();

        let command = CoordinatorCommand::SyncGroup {
            request,
            response_tx,
        };

        if coordinator_tx.send(command).await.is_err() {
            self.coordinators.remove(&group_id);
            return Err(anyhow::anyhow!("Failed to send sync group command to coordinator: {}", group_id));
        }

        match response_rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(anyhow::Error::new(e)),
            Err(e) => Err(anyhow::Error::new(e)),
        }

    }

    async fn persist_offset(&self, metadata: &OffsetCommitMetadata) -> Result<()> {
        let key = OffsetKey {
            version: 1,
            group: metadata.group_id.clone(),
            topic: metadata.topic.clone(),
            partition: metadata.partition as i32,
        };
        let value = OffsetValue {
            version: 3,
            offset: metadata.offset,
            leader_epoch: 0,
            metadata: "".to_string(),
            commit_timestamp: metadata.timestamp as i64,
        };
        let key_bytes = key.encode_to_bytes(0)?;
        let value_bytes = value.encode_to_bytes(0)?;
        let record = Record {
            key: key_bytes,
            value: value_bytes,
            length: 0,
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
        };
        let now = Utc::now().timestamp_millis();
        let record_batch = RecordBatch {
            base_offset: -1, // Broker 写入时会设置
            batch_length: 0, // encode 时会重新计算
            leader_epoch: 0, // 假设
            magic: 2, // Kafka 现代版本 magic byte
            crc: 0, // encode 时会计算
            attributes: 0, // 不使用压缩
            last_offset_delta: 0,
            first_timestamp: now,
            max_timestamp: now,
            producer_id: -1, // -1 表示不是来自事务性或幂等生产者
            producer_epoch: -1,
            base_sequence: -1,
            records_count: 1,
            records: vec![record],
        };

        let record_batch_bytes = record_batch.encode_to_bytes(0)?;

        let p_num = self.config.internal_topic_partitions.max(1);
        let partition = (calculate_hash(&metadata.group_id) % p_num as u64) as u32;
        let tp = TopicPartition {
            topic: self.config.internal_topic_name.clone(),
            partition,
        };

        self.append_batch(&tp, record_batch_bytes, 1).await?;

        Ok(())
    }

    async fn start_coordinators(&self) -> Result<()> {
        let mut groups: HashMap<String, ConsumerGroup> = HashMap::new();

        for p in 0..self.config.internal_topic_partitions {
            let tp = TopicPartition {
                topic: self.config.internal_topic_name.clone(),
                partition: p,
            };

            let mut current_offset = 0;
            loop {
                let batches_data = match self.read_batch(&tp, current_offset, 1024 * 1024).await {
                    Ok(data) => data,
                    Err(e) => {
                        if let Some(err) = e.downcast_ref::<crate::error::StorageError>() {
                            if matches!(err, crate::error::StorageError::InvalidOffset) {
                                break; // reach the end of the partition
                            }
                        }
                        return Err(e.into());
                    }
                };

                if batches_data.is_empty() {
                    break;
                }

                let mut next_offset = current_offset;
                for mut batch_bytes in batches_data.into_iter().map(bytes::Bytes::from) {
                    let record_batch = RecordBatch::decode(&mut batch_bytes, 0)?;

                    for record in &record_batch.records {
                        let key = OffsetKey::decode(&mut record.key.clone(), 0)
                                .with_context(||format!("Found corrupt key in internal topic partition: {}", tp.partition))?;
                        let value = OffsetValue::decode(&mut record.value.clone(), 0)
                                .with_context(||format!("Found corrupt value in internal topic partition: {}", tp.partition))?;

                        let group = groups.entry(key.group.clone()).or_insert_with(|| ConsumerGroup::new(key.group.clone()));
                        let offset_tp = TopicPartition {
                            topic: key.topic,
                            partition: key.partition as u32,
                        };
                        group.commit_offset(offset_tp, value.offset);
                    }
                    next_offset =(record_batch.base_offset + record_batch.last_offset_delta as i64 + 1) as u64;
                }
                current_offset = next_offset;
            }
        }

        println!("Loaded {} consumer groups from internal topic.", groups.len());

        for (group_id, group) in groups {
            println!("Starting coordinator for consumer group: {}", group_id);
            let coordinator_tx = GroupCoordinator::with_state(group);
            self.coordinators.insert(group_id, coordinator_tx);
        }

        Ok(())
    }

}
