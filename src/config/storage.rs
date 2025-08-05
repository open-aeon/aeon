use serde::{Deserialize, Serialize};
use crate::storage::factory::StorageEngine;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    pub engine: StorageEngine,
    pub segment_size: usize,
    pub index_interval_bytes: usize,
    pub retention_policy_ms: Option<u128>,
    pub retention_policy_bytes: Option<u64>,
    pub preallocate: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: StorageEngine::Mmap,
            segment_size: 1024 * 1024 * 1024, // 1GB
            index_interval_bytes: 4096, // 4KB
            retention_policy_ms: None,
            retention_policy_bytes: None,
            preallocate: false,
        }
    }
} 