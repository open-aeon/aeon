use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_size: usize,
    pub index_interval_bytes: usize,
    pub max_segments: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            segment_size: 1024 * 1024 * 1024, // 1GB
            index_interval_bytes: 4096, // 4KB
            max_segments: 10,
        }
    }
} 