use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: String,
    pub file: Option<String>,
    pub rotation: Option<LogRotation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRotation {
    pub max_size: usize,
    pub max_files: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: None,
            rotation: Some(LogRotation {
                max_size: 100 * 1024 * 1024, // 100MB
                max_files: 5,
            }),
        }
    }
} 