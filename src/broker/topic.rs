use crate::broker::partition::Partition;
use crate::common::metadata::{PartitionMetadata, TopicMetadata, TopicPartition};
use crate::config::StorageConfig;
use crate::storage::factory::StorageEngine;
use anyhow::{anyhow, Context, Result};
use std::{collections::HashMap, path::PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock; 
use bytes::Bytes;

pub struct Topic {
    pub name: String,
    partitions: Arc<RwLock<HashMap<u32, Partition>>>,
}

impl Topic {
    pub fn create(name: String, path: PathBuf, p_num: u32, engine: StorageEngine, config: &StorageConfig) -> Result<Self> {
        if path.exists() {
            return Err(anyhow::anyhow!("Topic path already exists: {}", path.display()));
        }

        std::fs::create_dir_all(&path).with_context(||format!("Failed to create topic directory: {}", path.display()))?;

        let mut local_partitions = HashMap::new();

        for i in 0..p_num {
            let partition_path = path.join(format!("partition_{}", i));
            let partition = Partition::create(TopicPartition { topic: name.clone(), partition: i as u32 }, partition_path, engine, config.clone())?;
            local_partitions.insert(i as u32, partition);
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
                            if let Ok(partition_id) = dir_name_str.trim_start_matches("partition_").parse::<u32>() {
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

    pub async fn append_batch(&self, p_id: u32, data: Bytes, record_count: u32) -> Result<u64> {
        let partition = self.partitions.read().await;
        if let Some(partition) = partition.get(&p_id) {
            partition.append_batch(data, record_count).await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn read_batch(&self, p_id: u32, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>> {
        let partition = self.partitions.read().await;
        if let Some(partition) = partition.get(&p_id) {
            partition.read_batch(start_offset, max_bytes).await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn earliest_offset(&self, p_id: u32) -> Result<u64> {
        let partition = self.partitions.read().await;
        if let Some(partition) = partition.get(&p_id) {
            partition.earliest_offset().await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn latest_offset(&self, p_id: u32) -> Result<u64> {
        let partition = self.partitions.read().await;
        if let Some(partition) = partition.get(&p_id) {
            partition.latest_offset().await
        } else {
            Err(anyhow::anyhow!("Partition not found: {}", p_id))
        }
    }

    pub async fn partition_count(&self) -> usize {
        self.partitions.read().await.len()
    }

    pub async fn flush_all(&self) -> Result<()> {
        let partitions = self.partitions.read().await;
    
        let mut errors: Vec<anyhow::Error> = Vec::new();
    
        for partition in partitions.values() {
            if let Err(e) = partition.flush().await {
                errors.push(anyhow::anyhow!(
                    "Failed to flush partition {}: {}",
                    partition.tp.partition,
                    e
                ));
            }
        }
    
        if errors.is_empty() {
            Ok(())
        } else {
            let error_messages: String = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join("\n");
            
            Err(anyhow::anyhow!(
                "Encountered errors while flushing partitions for topic '{}':\n{}",
                self.name,
                error_messages
            ))
        }
    }

    pub async fn cleanup(&self) -> Result<()> {
        let partitions = self.partitions.read().await;
        let mut errors: Vec<anyhow::Error> = Vec::new();

        for partition in partitions.values() {
            if let Err(e) = partition.cleanup().await {
                errors.push(anyhow::anyhow!(
                    "Failed to cleanup partition {}: {}",
                    partition.tp.partition,
                    e
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            let error_messages: String = errors.into_iter().map(|e| e.to_string()).collect::<Vec<String>>().join("\n");

            Err(anyhow::anyhow!(
                "Encountered errors while cleaning up partitions for topic '{}':\n{}",
                self.name,
                error_messages
            ))
        }
    }

    pub async fn delete(self) -> Result<()> {
        let partitions_arc = self.partitions;
        
        let partition_rwlock = match Arc::try_unwrap(partitions_arc) {
            Ok(rwlock) => rwlock,
            Err(_) => {
                return Err(anyhow!("Failed to delete topic: partitions still in use"));
            }
        };

        let partitions = partition_rwlock.into_inner();

        let mut errors: Vec<anyhow::Error> = Vec::new();

        for partition in partitions.into_values() {
            let p_id = partition.tp.partition.clone();
            if let Err(e) = partition.delete().await {
                errors.push(anyhow::anyhow!(
                    "Failed to delete partition {}: {}",
                    p_id,
                    e
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            let error_messages: String = errors.into_iter().map(|e| e.to_string()).collect::<Vec<String>>().join("\n");

            Err(anyhow::anyhow!(
                "Encountered errors while deleting partitions for topic '{}':\n{}",
                self.name,
                error_messages
            ))
        }
    }

    pub async fn meta(&self) -> TopicMetadata {
        let partitions_map = self.partitions.read().await;
        let partitions_meta = partitions_map.iter()
            .map(|(id,partition)| {
                let meta = PartitionMetadata {
                    leader: partition.leader(),
                    replicas: partition.replicas(),
                    isr: partition.isr(),
                    leader_epoch: partition.leader_epoch(),
                };
                (*id, meta)
            }).collect();

        TopicMetadata {
            name: self.name.clone(),
            partitions: partitions_meta,
        }
    }

}