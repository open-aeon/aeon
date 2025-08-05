use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::time::SystemTime;
use segment::LogSegment;
use anyhow::{anyhow, Result};
use std::fs;
use async_trait::async_trait;
use crate::config::storage::StorageConfig;
use crate::error::StorageError;
use crate::storage::LogStorage;
mod segment;
use bytes::Bytes;

pub struct MmapLogStorage {
    log: Arc<Mutex<MmapLog>>,
}

impl MmapLogStorage {
    pub fn new(path: PathBuf, config: StorageConfig) -> Result<Self> {
        let log = MmapLog::new(path, config)?;
        Ok(Self { log: Arc::new(Mutex::new(log)) })
    }
}

#[async_trait]
impl LogStorage for MmapLogStorage {

    async fn append_batch(&mut self, data: Bytes, record_count: u32) -> Result<u64> {
        let log_clone = self.log.clone();
        let offset = tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Fail to lock log").append_batch(&data, record_count)
        }).await??;

        Ok(offset)
    }

    async fn read_batch(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>> {
        let log_clone = self.log.clone();
        let batches = tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Fail to lock log").read_batch(start_offset, max_bytes)
        }).await??;
        Ok(batches)
    }

    async fn truncate(&mut self, offset: u64) -> Result<()> {
        let log_clone = self.log.clone();
        tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Fail to lock log").truncate(offset)
        }).await?
    }
    
    async fn flush(&mut self) -> Result<()> {
        let log_clone = self.log.clone();

        tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Mutex was poisoned").flush()
        })
        .await?
    }

    async fn cleanup(&mut self) -> Result<()> {
        let log_clone = self.log.clone();

        tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Mutex was poisoned").cleanup()
        })
        .await?
    }

    async fn delete(self: Box<Self>) -> Result<()> {
        let storage = *self;
        let log_arc = storage.log;

        tokio::task::spawn_blocking(move || {
            match Arc::try_unwrap(log_arc) {
                Ok(mutex) => {
                    let mmap_log = mutex.into_inner().expect("Mutex was poisoned");
                    mmap_log.delete()
                }
                Err(_) => {
                    Err(anyhow!(
                        "Cannot delete log: other references to the log still exist."
                    ))
                }
            }
        })
        .await?
    }
}

struct MmapLog {
    path: PathBuf,
    segments: BTreeMap<u64, LogSegment>,
    config: StorageConfig,
    active_segment_offset: u64,
} 

impl MmapLog {
    pub fn new(path: PathBuf, config: StorageConfig) -> Result<Self> {
        fs::create_dir_all(&path)?;

        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(&path)? {
            let file = entry?.path();
            if file.is_file() && file.extension().map_or(false, |s| s == "log") {
                if let Some(stem) = file.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = stem.parse::<u64>() {
                        let segment = LogSegment::open(&path, base_offset, config.index_interval_bytes, config.preallocate)?;
                        segments.insert(base_offset, segment);
                    }
                }
            }
        }
       
       if segments.is_empty() {
            let base_offset = 0;
            let first_segment = LogSegment::create(&path, base_offset, config.index_interval_bytes, config.preallocate)?;
            segments.insert(base_offset, first_segment);
       }

       let active_segment_offset = segments.last_key_value().map(|(&k, _)| k).unwrap_or(0);

       Ok(MmapLog{
        path,
        segments,
        config,
        active_segment_offset,
       })
    }

    pub fn append_batch(&mut self, records: &Bytes, record_count: u32) -> Result<u64> {
        if records.is_empty() {
            let active_segment = self.segments.get(&self.active_segment_offset).unwrap();
            return Ok(active_segment.base_offset() + active_segment.record_count());
        }

        let batch_size: usize = records.len();
        let mut active_segment = self.segments.get_mut(&self.active_segment_offset).unwrap();

        // todo: add chunk and roll logic 
        if active_segment.size() + batch_size > self.config.segment_size as usize {
            if batch_size > self.config.segment_size as usize {
                return Err(anyhow!(
                    "Batch size ({}) is larger than segment size ({})",
                    batch_size,
                    self.config.segment_size
                ));
            }

            let old_base_offset = self.active_segment_offset;
            let new_base_offset = old_base_offset + active_segment.record_count();

            println!(
                "Batch does not fit in segment {}. Creating new segment with base offset {}",
                old_base_offset, new_base_offset
            );

            let new_segment = LogSegment::create(
                &self.path,
                new_base_offset,
                self.config.index_interval_bytes,
                self.config.preallocate,
            )?;
            self.segments.insert(new_base_offset, new_segment);
            self.active_segment_offset = new_base_offset;

            active_segment = self.segments.get_mut(&self.active_segment_offset).unwrap();
        }

        active_segment.append_batch(records, record_count).map_err(|e| e.into())
    }

    pub fn read_batch(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>> {
        let segment = match self.segments.range(..=start_offset).next_back() {
            Some((_, segment)) => segment,
            None => {
                return Err(StorageError::InvalidOffset.into());
            }
        };

        segment.read_batch(start_offset, max_bytes).map_err(|e| e.into())
    }

    pub fn truncate(&mut self, offset: u64) -> Result<()> {
        // 找到第一个base_offset >= offset 的 segment
        let mut found = false;
        let mut to_delete: Vec<u64> = Vec::new();
        let mut to_truncate: Option<u64> = None;

        for (&base_offset, _) in self.segments.range(offset..) {
            // 第一个 >= offset 的 segment
            to_truncate = Some(base_offset);
            found = true;
            break;
        }

        // 收集所有 base_offset > offset 的 segment
        for (&base_offset, _) in self.segments.range((offset + 1)..) {
            to_delete.push(base_offset);
        }

        if !found {
            return Err(StorageError::InvalidOffset.into());
        }

        // truncate 第一个 >= offset 的 segment
        if let Some(base_offset) = to_truncate {
            if let Some(segment) = self.segments.get_mut(&base_offset) {
                segment.truncate(offset)?;
            }
        }

        // 删除所有 base_offset > offset 的 segment
        for base_offset in to_delete {
            if let Some(segment) = self.segments.remove(&base_offset) {
                segment.delete()?;
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let active_segment = self.segments.get_mut(&self.active_segment_offset).expect("Active segment not found");
        active_segment.flush()?;
        Ok(())
    }

    pub fn cleanup(&mut self) -> Result<()> {
        if self.segments.len() <= 1 {
            return Ok(());
        }
        
        let mut total_size: u64 = self.segments.values().map(|s| s.size() as u64).sum();
        let mut removable_offsets: Vec<u64> = Vec::new();

        for (&offset, segment) in self.segments.iter() {
            if offset == self.active_segment_offset {
                continue;
            }

            let mut should_remove = false;

            if let Some(retention_bytes) = self.config.retention_policy_bytes {
                if total_size > retention_bytes {
                    should_remove = true;
                    total_size -= segment.size() as u64;
                }
            }

            if !should_remove {
                if let Some(retention_ms) = self.config.retention_policy_ms {
                    let now = SystemTime::now();
                    let last_modified_time = segment.last_modified()?;
                    if let Ok(duration) = now.duration_since(last_modified_time) {
                        if duration.as_millis() > retention_ms {
                            should_remove = true;
                        }
                    }
                }
            }
           
            if should_remove {
                removable_offsets.push(offset);
            }
        }
        
        for offset in removable_offsets {
            if let Some(segment) = self.segments.remove(&offset) {
                segment.delete()?;
            }
        }

        Ok(())
    }

    pub fn delete(self) -> Result<()> {
        for segment in self.segments.into_values() {
            segment.delete()?;
        }
        fs::remove_dir_all(&self.path)?;
        Ok(())
    }
}