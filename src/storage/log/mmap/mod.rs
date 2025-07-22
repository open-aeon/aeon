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
    async fn append(&mut self, data: &[u8]) -> Result<u64> {
        let log_clone = self.log.clone();
        let data_owned = data.to_vec();
        let offset = tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Failed to lock log").append(&data_owned)
        }).await??;

        Ok(offset)
    }

    async fn append_batch(&mut self, data: &[Vec<u8>]) -> Result<u64> {
        let log_clone = self.log.clone();
        let data_owned = data.to_vec();
        let offset = tokio::task::spawn_blocking(move || {
            log_clone.lock().expect("Fail to lock log").append_batch(&data_owned)
        }).await??;

        Ok(offset)
    }

    async fn read(&self, logical_offset: u64) -> Result<Vec<u8>> {
        let log_clone = self.log.clone();

        tokio::task::spawn_blocking(move || {
            log_clone
                .lock()
                .expect("Mutex was poisoned")
                .read(logical_offset)
        })
        .await?
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
            let path = entry?.path();
            if path.is_file() && path.extension().map_or(false, |s| s == "log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = stem.parse::<u64>() {
                        let segment = LogSegment::open(&path, base_offset, config.index_interval_bytes)?;
                        segments.insert(base_offset, segment);
                    }
                }
            }
        }
       
       if segments.is_empty() {
            let base_offset = 0;
            let first_segment = LogSegment::create(&path, base_offset, config.index_interval_bytes)?;
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

    pub fn append(&mut self, record: &[u8]) -> Result<u64> {

        let needs_roll = {
            let active_segment = self.segments.get_mut(&self.active_segment_offset).unwrap();
            active_segment.size() + record.len() > self.config.segment_size as usize
        };

        if needs_roll {
            let old_base_offset = self.active_segment_offset;
            let old_segment = self.segments.get(&old_base_offset).unwrap();
            let new_base_offset = old_base_offset + old_segment.record_count();

            println!("Log segment {} is full. Creating new segment with base offset {}", old_base_offset, new_base_offset);

            let new_segment = LogSegment::create(&self.path, new_base_offset, self.config.index_interval_bytes)?;
            self.segments.insert(new_base_offset, new_segment);
            self.active_segment_offset = new_base_offset;
        }

        let segment = self.segments.get_mut(&self.active_segment_offset).unwrap();
        let logical_offset = segment.append(&record)?;

        Ok(logical_offset)
    }

    pub fn append_batch(&mut self, records: &[Vec<u8>]) -> Result<u64> {
        if records.is_empty() {
            let active_segment = self.segments.get(&self.active_segment_offset).unwrap();
            return Ok(active_segment.base_offset() + active_segment.record_count());
        }

        let batch_size: usize = records.iter().map(|r| 12 + r.len()).sum();
        let mut active_segment = self.segments.get_mut(&self.active_segment_offset).unwrap();

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
            )?;
            self.segments.insert(new_base_offset, new_segment);
            self.active_segment_offset = new_base_offset;

            active_segment = self.segments.get_mut(&self.active_segment_offset).unwrap();
        }

        active_segment.append_batch(records).map_err(|e| e.into())
    }

    pub fn read(&self, logical_offset: u64) -> Result<Vec<u8>> {
        // Find the segment that contains the logical_offset.
        // The BTreeMap is sorted by base_offset, so we can use range to find the
        // segment with the greatest base_offset that is less than or equal to the
        // logical_offset.
        let segment = match self.segments.range(..=logical_offset).next_back() {
            Some((_, segment)) => segment,
            None => {
                return Err(StorageError::InvalidOffset.into());
            }
        };

        // Calculate the relative offset within the segment.
        let relative_offset = (logical_offset - segment.base_offset()) as u32;

        // Delegate the read operation to the found segment.
        match segment.read(relative_offset) {
            Ok(Some(record)) => Ok(record),
            Ok(None) => Err(StorageError::InvalidOffset.into()),
            Err(e) => Err(e.into()),
        }
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