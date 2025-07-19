use std::collections::BTreeMap;
use segment::LogSegment;
use std::fs;
use crate::config::storage::StorageConfig;
use crate::error::StorageError;
use crate::error::Result;
use crate::protocol::Encodable;
pub mod segment;

pub struct Log {
    segments: BTreeMap<u64, LogSegment>,
    config: StorageConfig,
    active_segment_offset: u64,
} 

impl Log {
    pub fn new(config: StorageConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(&config.data_dir)? {
            let path = entry?.path();
            if path.is_file() && path.extension().map_or(false, |s| s == "log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = stem.parse::<u64>() {
                        let segment = LogSegment::open(&config.data_dir, base_offset, config.index_interval_bytes)?;
                        segments.insert(base_offset, segment);
                    }
                }
            }
        }
       
       if segments.is_empty() {
            let base_offset = 0;
            let first_segment = LogSegment::create(&config.data_dir, base_offset, config.index_interval_bytes)?;
            segments.insert(base_offset, first_segment);
       }

       let active_segment_offset = segments.last_key_value().map(|(&k, _)| k).unwrap_or(0);

       Ok(Log{
        segments,
        config,
        active_segment_offset,
       })
    }

    pub fn append<M: Encodable>(&mut self, message: &M) -> Result<u64> {
        let message_bytes = message.encode_to_vec()?;
        let segment = self.segments.get_mut(&self.active_segment_offset).unwrap();

        match segment.append(&message_bytes) {
            Ok(offset) => {
                Ok(offset)
            }
            Err(StorageError::SegmentFull) => {
                let old_base_offset = self.active_segment_offset;
                let old_segment = self.segments.get(&old_base_offset).unwrap();
                let new_base_offset = old_base_offset + old_segment.record_count();

                println!("Log segment {} is full. Creating new segment with base offset {}", old_base_offset, new_base_offset);
                let segment = LogSegment::create(&self.config.data_dir, new_base_offset, self.config.index_interval_bytes)?;
                
                self.segments.insert(new_base_offset, segment); 
                self.active_segment_offset = new_base_offset;

                let logical_offset = self.segments.get_mut(&self.active_segment_offset).unwrap().append(&message_bytes)?;
                Ok(logical_offset)
            }
            Err(e) => {
                Err(e.into())
            }
        }
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
}