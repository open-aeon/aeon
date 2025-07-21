use crate::config::storage::StorageConfig;
use crate::storage::api::LogStorage;
use crate::storage::log::MmapLogStorage;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngine {
    Mmap,
}

pub struct StorageFactory;

impl StorageFactory {
    pub fn create(engine: StorageEngine, path: PathBuf, config: StorageConfig) -> Result<Box<dyn LogStorage>> {
        match engine {
            StorageEngine::Mmap => {
                let mmap_storage = MmapLogStorage::new(path, config)?;
                Ok(Box::new(mmap_storage))
            }
        }
    }
}