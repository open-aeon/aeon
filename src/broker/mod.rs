pub mod partition;
pub mod topic;
pub mod consumer_group;
pub mod coordinator;

use anyhow::{Context, Result};
use tokio::sync::watch;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, mpsc, oneshot};

use crate::broker::coordinator::*;
use crate::broker::consumer_group::*;
use crate::broker::topic::Topic;
use crate::common::metadata::{TopicMetadata, TopicPartition};
use crate::config::broker::BrokerConfig;
use crate::config::storage::StorageConfig;
use crate::broker::consumer_group::ConsumerGroup;
use crate::common::metadata::OffsetCommitMetadata;

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

    pub async fn append(&self, tp: &TopicPartition, data: &[u8]) -> Result<u64> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.append(p_id, data).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn append_batch(&self, tp: &TopicPartition, data: &[Vec<u8>]) -> Result<u64> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.append_batch(p_id, data).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
        }
    }

    pub async fn read(&self, tp: &TopicPartition, offset: u64) -> Result<Vec<u8>> {
        let topic_name = tp.topic.clone();
        let p_id = tp.partition;

        let topics = self.topics.read().await;
        if let Some(topic) = topics.get(&topic_name) {
            topic.read(p_id, offset).await
        } else {
            Err(anyhow::anyhow!("Topic not found: {}", topic_name))
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
            return Err(anyhow::anyhow!("Failed to join group: {}", group_id));
        }

        match response_rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(anyhow::anyhow!("Failed to join group: {}", e)),
            Err(e) => Err(anyhow::anyhow!("Failed to join group: {}", e)),
        }
    }

    pub async fn leave_group(&self, request: LeaveGroupRequest) -> Result<()> {
        let group_id = request.group_id.clone();
        if let Some(coordinator_tx) = self.coordinators.get(&group_id) {
            let (response_tx, response_rx) = oneshot::channel();

            let command = CoordinatorCommand::LeaveGroup {
                request,
                response_tx,
            };

            if coordinator_tx.send(command).await.is_err() {
                self.coordinators.remove(&group_id);
                return Err(anyhow::anyhow!("Failed to leave group: {}", group_id));
            }

            match response_rx.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("Failed to leave group: {}", e)),
                Err(e) => Err(anyhow::anyhow!("Failed to leave group: {}", e)),
            }
        } else {
            Err(anyhow::anyhow!("Coordinator not found for group: {}", group_id))
        }
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        let group_id = request.group_id.clone();
        if let Some(coordinator_tx) = self.coordinators.get(&group_id) {
            let (response_tx, response_rx) = oneshot::channel();

            let command = CoordinatorCommand::Heartbeat {
                request,
                response_tx,
            };

            if coordinator_tx.send(command).await.is_err() {
                self.coordinators.remove(&group_id);
                return Err(anyhow::anyhow!("Failed to send heartbeat command to coordinator: {}", group_id));
            }
            
            match response_rx.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("Failed to send heartbeat command to coordinator: {}", e)),
                Err(e) => Err(anyhow::anyhow!("Failed to send heartbeat command to coordinator: {}", e)),
            }
        } else {
            Err(anyhow::anyhow!("Coordinator not found for group: {}", group_id))
        }
    }

    pub async fn commit_offset(&self, request: CommitOffsetRequest) -> Result<()> {
        let group_id = request.group_id.clone();

        let message = OffsetCommitMetadata {
            group_id: group_id.clone(),
            topic: request.tp.topic.clone(),
            partition: request.tp.partition,
            offset: request.offset,
            timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        };

        self.persist_offset(&message).await.with_context(|| "Failed to persist offset")?;

        let coordinator_tx = self.coordinators.entry(group_id.clone()).or_insert_with(||
            GroupCoordinator::new(group_id.clone())).clone();

        let command = CoordinatorCommand::CommitOffset {
            request,
        };

        if coordinator_tx.send(command).await.is_err() {
           eprintln!("Failed to send commit offset command to coordinator: {}", group_id);
        };

        Ok(())
    }

    pub async fn fetch_offset(&self, request: FetchOffsetRequest) -> Result<Option<i64>> {
        let group_id = request.group_id.clone();
        if let Some(coordinator_tx) = self.coordinators.get(&group_id) {
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
        } else {
            Ok(None)
        }
    }

    pub async fn sync_group(
        &self,
        request: SyncGroupRequest
    ) -> Result<SyncGroupResult> {
        let group_id = request.group_id.clone();
        if let Some(coordinator_tx) = self.coordinators.get(&group_id) {
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
                Ok(Err(e)) => Err(anyhow::anyhow!("Failed to sync group: {}", e)),
                Err(e) => Err(anyhow::anyhow!("Failed to sync group: {}", e)),
            }
        } else {
            Err(anyhow::anyhow!("Coordinator not found for group: {}", group_id))
        }
    }

    async fn persist_offset(&self, metadata: &OffsetCommitMetadata) -> Result<()> {
        let data = bincode::serialize(metadata)?;

        let partition = 0;
        let tp = TopicPartition {
            topic: self.config.internal_topic_name.clone(),
            partition,
        };

        self.append(&tp, &data).await?;

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
                match self.read(&tp, current_offset).await {
                    Ok(data) => {
                        if let Ok(message) = bincode::deserialize::<OffsetCommitMetadata>(&data) {
                            let group = groups
                                .entry(message.group_id.clone())
                                .or_insert_with(|| ConsumerGroup::new(message.group_id.clone()));

                            let offset_tp = TopicPartition {
                                topic: message.topic,
                                partition: message.partition,
                            };
                            group.commit_offset(offset_tp, message.offset);
                        }
                        current_offset += 1;
                    }
                    Err(e) => {
                        if let Some(err) = e.downcast_ref::<crate::error::StorageError>() {
                            if matches!(err, crate::error::StorageError::InvalidOffset) {
                                break; // reach the end of the partition
                            }
                        }
                        return Err(e.into());
                    }
                }
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