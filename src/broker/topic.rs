use crate::broker::partition::Partition;
use crate::common::metadata::TopicPartition;
use crate::config::StorageConfig;
use crate::storage::factory::StorageEngine;
use anyhow::{Context, Result};
use std::{collections::HashMap, path::PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock; 

pub struct Topic {
    pub name: String,
    partitions: Arc<RwLock<HashMap<i32, Partition>>>,
}

impl Topic {
    pub fn create(name: String, path: PathBuf, p_num: i32, engine: StorageEngine, config: &StorageConfig) -> Result<Self> {
        if path.exists() {
            return Err(anyhow::anyhow!("Topic path already exists: {}", path.display()));
        }

        std::fs::create_dir_all(&path).with_context(||format!("Failed to create topic directory: {}", path.display()))?;

        let mut local_partitions = HashMap::new();

        for i in 0..p_num {
            let partition_path = path.join(format!("partition_{}", i));
            let partition = Partition::create(TopicPartition { topic: name.clone(), partition: i }, partition_path, engine, config.clone())?;
            local_partitions.insert(i, partition);
        }

        let partitions = Arc::new(RwLock::new(local_partitions));

        Ok(Self { name, partitions })
    }

    pub fn load(name: String, path: PathBuf, engine: StorageEngine, config: &StorageConfig) -> Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Topic path does not exist: {}", path.display()));
        }

        if !path.is_dir() {
            return Err(anyhow::anyhow!("Topic path is not a directory: {}", path.display()));
        }
        let mut local_partitions = HashMap::new();

        for entry in std::fs::read_dir(&path)? {
            if let Ok(entry) = entry {
                let partition_path = entry.path();
                if partition_path.is_dir() {
                    if let Some(dir_name) = partition_path.file_name() {
                        if let Some(dir_name_str) = dir_name.to_str() {
                            if let Ok(partition_id) = dir_name_str.parse::<i32>() {
                                let partition = Partition::load(TopicPartition { topic: name.clone(), partition: partition_id }, partition_path, engine, config.clone())?;
                                local_partitions.insert(partition_id, partition);
                            }
                        }
                    }
                }
            }
        }

        let partitions = Arc::new(RwLock::new(local_partitions));

        Ok(Self { name, partitions })
    }

    pub async fn append(&self, p_id: i32, data: &[u8]) -> Result<u64> {
        let mut partition = self.partitions.write().await;
        if let Some(partition) = partition.get_mut(&p_id) {
            partition.append(data).await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn read(&self, p_id: i32, offset: u64) -> Result<Vec<u8>> {
        let partition = self.partitions.read().await;
        if let Some(partition) = partition.get(&p_id) {
            partition.read(offset).await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn partition_count(&self) -> usize {
        self.partitions.read().await.len()
    }
}