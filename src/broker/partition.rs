use crate::{
    common::metadata::TopicPartition,
    storage::{LogStorage, factory::{StorageFactory, StorageEngine}},
    config::storage::StorageConfig,
};
use anyhow::{Context, Result};
use std::path::PathBuf;

pub struct Partition {
    log: Box<dyn LogStorage>,
    pub tp: TopicPartition,
}

impl Partition {
    pub fn create(tp: TopicPartition, path: PathBuf, engine: StorageEngine, config: StorageConfig) -> Result<Self> {
        if path.exists() {
            return Err(anyhow::anyhow!("Partition path does not exist: {}", path.display()));
        }

        std::fs::create_dir_all(&path).with_context(||format!("Failed to create partition directory: {}", path.display()))?;

        let log = StorageFactory::create(engine, path, config)?;
        Ok(Self { log, tp })
    }

    pub fn load(tp: TopicPartition, path: PathBuf, engine: StorageEngine, config: StorageConfig) -> Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Partition path does not exist: {}", path.display()));
        }

        if !path.is_dir() {
            return Err(anyhow::anyhow!("Partition path is not a directory: {}", path.display()));
        }

        let log = StorageFactory::create(engine, path.clone(), config)?;
        Ok(Self { log, tp })
    }

    pub async fn append(&mut self, data: &[u8]) -> Result<u64> {
        self.log.append(data).await
    }

    pub async fn read(&self, offset: u64) -> Result<Vec<u8>> {
        self.log.read(offset).await
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.log.flush().await
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        self.log.cleanup().await
    }

    pub async fn delete(self) -> Result<()> {
        self.log.delete().await
    }
}