use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_size: usize,
    pub max_segments: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            segment_size: 1024 * 1024 * 1024, // 1GB
            max_segments: 10,
        }
    }
} 