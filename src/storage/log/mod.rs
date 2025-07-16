use std::path::PathBuf;
use std::collections::BTreeMap;
use segment::LogSegment;
use tokio::fs;

pub mod segment;

pub struct Log {
    dir: PathBuf,
    segments: BTreeMap<u64, LogSegment>,
    active_segment_offset: u64,
} 

impl Log {
    pub async fn new(dir: PathBuf) -> std::io::Result<Self> {
        fs::create_dir_all(&dir).await?;

        let mut segments = BTreeMap::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |s| s == "log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = stem.parse::<u64>() {
                        let segment = LogSegment::new(&dir, base_offset).await?;
                        segments.insert(base_offset, segment);
                    }
                }
            }
        }
       
       if segments.is_empty() {
            let base_offset = 0;
            let first_segment = LogSegment::new(&dir, base_offset).await?;
            segments.insert(base_offset, first_segment);
       }

       let active_segment_offset = segments.last_key_value().map(|(k, _)| *k).unwrap();

       Ok(Log{
        dir,
        segments,
        active_segment_offset,
       })
    }

    pub async fn append(&mut self, record: &[u8]) -> std::io::Result<u64> {
        todo!()
    }

    pub async fn read(&self, logical_offset: u64) -> std::io::Result<Vec<u8>> {
        todo!()
    }
}