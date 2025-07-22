pub mod partition;
pub mod topic;

use crate::broker::topic::Topic;
use crate::common::metadata::{TopicMetadata, TopicPartition};
use crate::config::broker::BrokerConfig;
use crate::config::storage::StorageConfig;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Broker {
    pub config: Arc<BrokerConfig>,
    pub storage_config: Arc<StorageConfig>,
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    data_dir: PathBuf,
}

impl Broker {
    pub fn new(config: BrokerConfig, storage_config: StorageConfig) -> Self {
        let data_dir = config.data_dir.clone();
        let topics = Arc::new(RwLock::new(HashMap::new()));
        Self { config: Arc::new(config),storage_config: Arc::new(storage_config), topics, data_dir }
    }

    pub async fn start(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
        .with_context(|| format!("Failed to create data directory at {:?}", &self.data_dir))?;

        println!("Starting broker, data directory at: {:?}", &self.data_dir);

        let mut local_topics = HashMap::new();

        for entry in std::fs::read_dir(&self.data_dir)? {
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

        self.start_background_tasks();

        Ok(())
    }

    fn start_background_tasks(&self) {
        self.start_flush_task();
        self.start_cleanup_task();
    }

    fn start_flush_task(&self) {
        if let Some(flush_interval) = self.config.flush_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(flush_interval);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);

                loop {
                    interval.tick().await;

                    let topics = topics.read().await;
                    for topic in topics.values() {
                        if let Err(e) = topic.flush_all().await {
                            eprintln!("Failed to flush topic {}: {}", topic.name, e);
                        }
                    }
                }
            });
            println!("Spawned periodic flush task with interval: {:?}", interval_duration);
        }
    }

    fn start_cleanup_task(&self) {
        if let Some(cleanup_interval) = self.config.cleanup_interval_ms {
            let topics = self.topics.clone();
            let interval_duration = tokio::time::Duration::from_millis(cleanup_interval);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval_duration);
                
                loop {
                    interval.tick().await;

                    let topics = topics.read().await;
                    for topic in topics.values() {
                        if let Err(e) = topic.cleanup().await {
                            eprintln!("Failed to cleanup topic {}: {}", topic.name, e);
                        }
                    }
                }
            });
            println!("Spawned periodic cleanup task with interval: {:?}", interval_duration);
        }
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
        let path = self.data_dir.join(&name);
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

    pub async fn get_topics_meta(&self) -> Result<HashMap<String, TopicMetadata>> {
        let topics = self.topics.read().await;
        let mut meta = HashMap::new();
        for (name, topic) in topics.iter() {
            meta.insert(name.clone(), topic.meta().await);
        }
        Ok(meta)
    }
}