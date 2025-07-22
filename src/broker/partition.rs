use crate::{
    common::metadata::TopicPartition,
    storage::{LogStorage, factory::{StorageFactory, StorageEngine}},
    config::storage::StorageConfig,
};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::sync::Mutex;

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

    pub async fn append(&self, data: &[u8]) -> Result<u64> {
        let mut log = self.log.lock().await;
        log.append(data).await
    }

    pub async fn append_batch(&self, data: &[Vec<u8>]) -> Result<u64> {
        let mut log = self.log.lock().await;
        log.append_batch(data).await
    }

    pub async fn read(&self, offset: u64) -> Result<Vec<u8>> {
        let log = self.log.lock().await;
        log.read(offset).await
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
}