use crate::common::message::Message;
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Arc,
    collections::HashMap,
};
use tokio::sync::RwLock;

const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB per segment

pub struct LogSegment {
    file: File,
    mmap: MmapMut,
    position: usize,
    #[allow(dead_code)]
    base_offset: u64,
    index: HashMap<u64, usize>, // 逻辑偏移量到物理位置的映射
}

impl LogSegment {
    pub fn new(path: &Path, base_offset: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(SEGMENT_SIZE as u64)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            position: 0,
            base_offset,
            index: HashMap::new(),
        })
    }

    pub fn append(&mut self, message: &Message, logical_offset: u64) -> io::Result<u64> {
        let message_bytes = bincode::serialize(message).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Failed to serialize message: {}", e))
        })?;

        let message_len = message_bytes.len() as u64;
        let len_bytes = message_len.to_le_bytes();
        let total_len = 8 + message_bytes.len();

        if self.position + total_len > SEGMENT_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Segment is full",
            ));
        }

        // 记录消息的物理位置
        let physical_offset = self.position as u64;
        
        // 更新索引
        self.index.insert(logical_offset, self.position);

        // 写入消息长度
        self.mmap[self.position..self.position + 8].copy_from_slice(&len_bytes);
        // 写入消息内容
        self.mmap[self.position + 8..self.position + total_len].copy_from_slice(&message_bytes);
        
        self.position += total_len;
        self.file.flush()?;

        Ok(physical_offset)
    }

    pub fn read(&self, logical_offset: u64) -> io::Result<Option<Message>> {
        // 从索引中获取物理位置
        let position = match self.index.get(&logical_offset) {
            Some(pos) => *pos,
            None => return Ok(None),
        };

        if position + 8 > self.position {
            return Ok(None);
        }

        let len_bytes = &self.mmap[position..position + 8];
        let message_len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        
        if position + 8 + message_len > self.position {
            return Ok(None);
        }

        let message_bytes = &self.mmap[position + 8..position + 8 + message_len];
        
        match bincode::deserialize::<Message>(message_bytes) {
            Ok(message) => Ok(Some(message)),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize message: {}", e)
            ))
        }
    }
}

pub struct Log {
    segments: Vec<Arc<RwLock<LogSegment>>>,
    current_segment: Arc<RwLock<LogSegment>>,
    base_dir: String,
    next_logical_offset: u64,
}

impl Log {
    pub fn new(base_dir: String) -> io::Result<Self> {
        // 确保目录存在
        std::fs::create_dir_all(&base_dir)?;
        
        let current_segment = Arc::new(RwLock::new(LogSegment::new(
            Path::new(&base_dir).join("segment-0").as_ref(),
            0,
        )?));

        Ok(Self {
            segments: vec![current_segment.clone()],
            current_segment,
            base_dir,
            next_logical_offset: 0,
        })
    }

    pub async fn append(&mut self, message: Message) -> io::Result<(u64, u64)> {
        let logical_offset = self.next_logical_offset;
        let result = {
            let mut segment = self.current_segment.write().await;
            segment.append(&message, logical_offset)
        };

        match result {
            Ok(physical_offset) => {
                self.next_logical_offset += 1;
                Ok((logical_offset, physical_offset))
            }
            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                // 创建新的 segment
                let new_segment = Arc::new(RwLock::new(LogSegment::new(
                    Path::new(&self.base_dir)
                        .join(format!("segment-{}", self.segments.len()))
                        .as_ref(),
                    self.segments.len() as u64 * SEGMENT_SIZE as u64,
                )?));
                
                self.segments.push(new_segment.clone());
                self.current_segment = new_segment;
                
                let mut new_segment = self.current_segment.write().await;
                let physical_offset = new_segment.append(&message, logical_offset)?;
                self.next_logical_offset += 1;
                Ok((logical_offset, physical_offset))
            }
            Err(e) => Err(e),
        }
    }

    pub async fn read(&self, logical_offset: u64) -> io::Result<Option<Message>> {
        for segment in &self.segments {
            let segment = segment.read().await;
            if let Some(message) = segment.read(logical_offset)? {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }
} 