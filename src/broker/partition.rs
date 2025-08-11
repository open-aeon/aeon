use crate::{
    common::metadata::TopicPartition,
    storage::{LogStorage, factory::{StorageFactory, StorageEngine}},
    config::storage::StorageConfig,
};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::sync::Mutex;
use bytes::Bytes;

pub struct Partition {
    log: Mutex<Box<dyn LogStorage>>,
    pub tp: TopicPartition,
}

impl Partition {
    pub fn create(tp: TopicPartition, path: PathBuf, engine: StorageEngine, config: StorageConfig) -> Result<Self> {
        if path.exists() {
            return Err(anyhow::anyhow!("Partition path does not exist: {}", path.display()));
        }

        std::fs::create_dir_all(&path).with_context(||format!("Failed to create partition directory: {}", path.display()))?;

        let log_storage = StorageFactory::create(engine, path, config)?;
        Ok(Self { log: Mutex::new(log_storage), tp })
    }

    pub fn load(tp: TopicPartition, path: PathBuf, engine: StorageEngine, config: StorageConfig) -> Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Partition path does not exist: {}", path.display()));
        }

        if !path.is_dir() {
            return Err(anyhow::anyhow!("Partition path is not a directory: {}", path.display()));
        }

        let log = StorageFactory::create(engine, path.clone(), config)?;
        Ok(Self { log: Mutex::new(log), tp })
    }

    pub async fn append_batch(&self, data: Bytes, record_count: u32) -> Result<u64> {
        let mut log = self.log.lock().await;
        log.append_batch(data, record_count).await
    }

    pub async fn read_batch(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>> {
        let log = self.log.lock().await;
        log.read_batch(start_offset, max_bytes).await
    }

    pub async fn earliest_offset(&self) -> Result<u64> {
        let log = self.log.lock().await;
        log.earliest_offset().await
    }

    pub async fn latest_offset(&self) -> Result<u64> {
        let log = self.log.lock().await;
        log.latest_offset().await
    }

    pub async fn flush(&self) -> Result<()> {
        let mut log = self.log.lock().await;
        log.flush().await
    }

    pub async fn cleanup(&self) -> Result<()> {
        let mut log = self.log.lock().await;
        log.cleanup().await
    }

    pub async fn delete(self) -> Result<()> {
        let log = self.log.into_inner();
        log.delete().await
    }

    pub fn leader(&self) -> u32 {
        0
    }

    pub fn replicas(&self) -> Vec<u32> {
        vec![0]
    }

    pub fn isr(&self) -> Vec<u32> {
        vec![0]
    }

    pub fn leader_epoch(&self) -> i32 {
        0
    }
    
}