use crate::protocol::message::Message;
use memmap2::{MmapMut, MmapOptions};
use std::{
    io::{self},
    path::Path,
    collections::HashMap,
};
use tokio::sync::RwLock;
use anyhow::Result;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    pub async fn new(path: &Path, base_offset: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path).await?;

        file.set_len(SEGMENT_SIZE as u64).await?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            position: 0,
            base_offset,
            index: HashMap::new(),
        })
    }

    pub async fn append(&mut self, message: &Message, logical_offset: u64) -> io::Result<u64> {
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
        self.file.flush().await?;

        Ok(physical_offset)
    }

    pub async fn read(&self, logical_offset: u64) -> io::Result<Option<Message>> {
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
    segments: RwLock<Vec<File>>,
    current_segment: RwLock<File>,
    base_dir: PathBuf,
    next_logical_offset: RwLock<i64>,
}

impl Log {
    pub async fn new(base_dir: String) -> Result<Self> {
        let base_dir = PathBuf::from(base_dir);
        let current_segment = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(base_dir.join("segment-0")).await?;

        Ok(Self {
            segments: RwLock::new(vec![current_segment.try_clone().await?]),
            current_segment: RwLock::new(current_segment),
            base_dir,
            next_logical_offset: RwLock::new(0),
        })
    }

    pub async fn append(&self, message: Message) -> Result<(i64, i64)> {
        let mut current_segment = self.current_segment.write().await;
        let mut next_offset = self.next_logical_offset.write().await;
        
        let offset = *next_offset;
        let bytes = bincode::serialize(&message)?;
        
        // 检查是否需要创建新的段
        if current_segment.metadata().await?.len() >= SEGMENT_SIZE as u64 {
            let new_segment = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(self.base_dir.join(format!("segment-{}", self.segments.read().await.len()))).await?;
            
            self.segments.write().await.push(new_segment.try_clone().await?);
            *current_segment = new_segment;
        }

        current_segment.write_all(&bytes).await?;
        *next_offset += 1;
        
        Ok((offset, *next_offset))
    }

    pub async fn read(&self, offset: i64) -> Result<Option<Message>> {
        if offset >= *self.next_logical_offset.read().await {
            return Ok(None);
        }

        let segment_index = (offset / SEGMENT_SIZE as i64) as usize;
        let segments = self.segments.read().await;
        
        if segment_index >= segments.len() {
            return Ok(None);
        }

        let mut file = segments[segment_index].try_clone().await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
        if let Ok(message) = bincode::deserialize::<Message>(&buffer) {
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
} 