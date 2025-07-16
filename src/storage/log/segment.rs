use crate::protocol::message::Message;
use memmap2::{MmapMut, MmapOptions};
use std::{
    io::{self},
    path::Path,
};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB per segment

pub struct LogSegment {
    log_file: File,
    index_file: File,
    mmap: MmapMut,
    position: usize,
    base_offset: u64,
}

impl LogSegment {
    pub async fn new(path: &Path, base_offset: u64) -> io::Result<Self> {
        let log_file_name = format!("{:020}.log", base_offset);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(log_file_name)).await?;

        let log_file_metadata = log_file.metadata().await?;
        let log_file_size = log_file_metadata.len();
        if log_file_size < SEGMENT_SIZE as u64 {
            log_file.set_len(SEGMENT_SIZE as u64).await?;
        }

        let index_file_name = format!("{:020}.index", base_offset);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(index_file_name)).await?;

        let mmap = unsafe { MmapOptions::new().map_mut(&log_file)? };

        Ok(Self {
            log_file,
            index_file,
            mmap,
            position: log_file_size as usize,
            base_offset,
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

        // 写入消息长度
        self.mmap[self.position..self.position + 8].copy_from_slice(&len_bytes);
        // 写入消息内容
        self.mmap[self.position + 8..self.position + total_len].copy_from_slice(&message_bytes);
        
        self.position += total_len;
        self.log_file.flush().await?;

        let physical_position = physical_offset as u32;
        let physical_position_bytes = physical_position.to_le_bytes();
        let relative_offset = logical_offset - self.base_offset;
        let relative_offset_bytes = (relative_offset as u32).to_le_bytes();
        let mut index_entry_bytes = Vec::with_capacity(4 + 4);
        index_entry_bytes.extend_from_slice(&relative_offset_bytes);
        index_entry_bytes.extend_from_slice(&physical_position_bytes);
        self.index_file.write_all(&index_entry_bytes).await?;
        self.index_file.flush().await?;

        Ok(physical_offset)
    }

    pub async fn read(&mut self, logical_offset: u64) -> io::Result<Option<Message>> {
       let target_relative_offset = (logical_offset - self.base_offset) as u32;

       // 确保我们从文件头开始读
       self.index_file.seek(SeekFrom::Start(0)).await?;
       
       let mut buffer = [0u8; 8];

       loop {
        match self.index_file.read_exact(&mut buffer).await {
            Ok(_) => {
                // 读取成功
                // 把 buffer[0..4] 解析成 relative_offset
                let relative_offset = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
                
                if relative_offset == target_relative_offset {
                    // 找到了！现在解析物理位置
                    let physical_position = u32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize;
                    
                    // --- 从 mmap 读取消息数据 ---
                    if physical_position + 8 > self.mmap.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "Index points beyond log file bounds"));
                    }
                    
                    // 读取消息长度
                    let len_bytes = &self.mmap[physical_position..physical_position + 8];
                    let message_len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    
                    if physical_position + 8 + message_len > self.mmap.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "Message length in log file is invalid"));
                    }
                    
                    // 读取消息内容
                    let message_bytes = &self.mmap[physical_position + 8..physical_position + 8 + message_len];
                    
                    // 反序列化并返回
                    return match bincode::deserialize::<Message>(message_bytes) {
                        Ok(message) => Ok(Some(message)),
                        Err(e) => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Failed to deserialize message: {}", e)
                        ))
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // 正常到达文件末尾，说明没找到
                break;
            }
            Err(e) => {
                // 发生了其他 IO 错误
                return Err(e);
            }
        }
       }
    
       Ok(None)
    }
} 