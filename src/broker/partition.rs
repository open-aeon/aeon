use crate::{
    common::message::{Message, TopicPartition},
    storage::log::Log,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use std::{collections::HashMap, path::PathBuf};

#[derive(Clone)]
pub struct PartitionManager {
    partitions: Arc<RwLock<HashMap<TopicPartition, Log>>>,
    data_dir: PathBuf,
}

impl PartitionManager {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            partitions: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        })
    }

    pub async fn append_message(&self, topic_partition: &TopicPartition, message: Message) -> Result<(u64, u64)> {
        let mut partitions = self.partitions.write().await;
        let log = partitions
            .entry(topic_partition.clone())
            .or_insert_with(|| {
                let log_dir = self.data_dir
                    .join(&topic_partition.topic)
                    .join(format!("{}", topic_partition.partition));
                Log::new(log_dir.to_string_lossy().to_string()).unwrap()
            });

        Ok(log.append(message).await?)
    }

    pub async fn read_message(&self, topic_partition: &TopicPartition, logical_offset: u64) -> Result<Option<Message>> {
        let partitions = self.partitions.read().await;
        if let Some(log) = partitions.get(topic_partition) {
            log.read(logical_offset).await.map_err(anyhow::Error::from)
        } else {
            Ok(None)
        }
    }

    pub async fn get_partitions(&self, topic: &str) -> Result<Vec<TopicPartition>> {
        let partitions = self.partitions.read().await;
        Ok(partitions
            .keys()
            .filter(|tp| tp.topic == topic)
            .cloned()
            .collect())
    }
} 