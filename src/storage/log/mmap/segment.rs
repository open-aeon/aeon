use crate::error::storage::StorageError;
use bytes::Bytes;
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{self,File, OpenOptions}, io::{BufReader, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, time::SystemTime
};

const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB per segment
const INITIAL_SEGMENT_SIZE: usize = 1024 * 1024; // 1MB per segment

pub struct LogSegment {
    log_path: PathBuf,
    index_path: PathBuf,
    log_file: File,
    index_file: File,
    mmap: MmapMut,
    position: usize,
    base_offset: u64,
    record_count: u64,
    bytes_since_last_index: usize,
    index: Vec<(u32, u32)>,
    index_interval_bytes: usize,
    preallocate: bool,
}

impl LogSegment {
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn size(&self) -> usize {
        self.position
    }

    pub fn last_modified(&self) -> Result<SystemTime, StorageError> {
        let metadata = self.log_file.metadata()?;
        let modified_time = metadata.modified()?;
        Ok(modified_time)
    }

    pub fn create(path: &Path, base_offset: u64, index_interval_bytes: usize, preallocate: bool) -> Result<Self, StorageError> {
        let log_path = path.join(format!("{:020}.log", base_offset));
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;

        if preallocate {
            Self::preallocate_file(&log_file, SEGMENT_SIZE)?;
        } else {
            log_file.set_len(0)?;
        }

        let index_path = path.join(format!("{:020}.index", base_offset));
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&index_path)?;
        // index_file.sync_all()?;
            
        let mmap = unsafe { MmapOptions::new().map_mut(&log_file)? };

        Ok(Self {
            log_path,
            index_path,
            log_file,
            index_file,
            mmap,
            position: 0,
            base_offset,
            record_count: 0,
            bytes_since_last_index: 0,
            index: Vec::new(),
            index_interval_bytes,
            preallocate,
        })
    }

    pub fn open(path: &Path, base_offset: u64, index_interval_bytes: usize, preallocate: bool) -> Result<Self, StorageError> {
        let log_path = path.join(format!("{:020}.log", base_offset));
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)?;

        let log_file_metadata = log_file.metadata()?;
        let log_file_size = log_file_metadata.len();
        
        if preallocate {
            if log_file_size < SEGMENT_SIZE as u64 {
                Self::preallocate_file(&log_file, SEGMENT_SIZE)?;
            }
        } else {
            if log_file_size == 0 {
                log_file.set_len(INITIAL_SEGMENT_SIZE as u64)?;
            }
        }

        let index_path = path.join(format!("{:020}.index", base_offset));
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&index_path)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&log_file)? };

        let mut index_reader = BufReader::new(&index_file);
        index_reader.seek(SeekFrom::Start(0))?;
        let mut record_count :u64 = 0;
        let mut position : usize = 0;
        let mut bytes_since_last_index : usize = 0;
        let mut buf = [0u8; 8];
        let mut in_memory_index: Vec<(u32, u32)> = Vec::new();
        while let Ok(()) = index_reader.read_exact(&mut buf) {
            let relative_offset = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let physical_position = u32::from_le_bytes(buf[4..8].try_into().unwrap());
            in_memory_index.push((relative_offset, physical_position));
        }
        
        let clean_index_size: u64 = (in_memory_index.len() * 8) as u64;
        if index_file.metadata()?.len() > clean_index_size {
            index_file.set_len(clean_index_size)?;
        }
        index_file.seek(SeekFrom::End(0))?;

        // 从索引中恢复最后一个 batch 的位置
        if let Some(&(_, last_indexed_position)) = in_memory_index.last() {
            let pos = last_indexed_position as usize;
            if pos + 12 > log_file_metadata.len() as usize {
                position = pos
            } else {
                // 读取 RecordBatch 长度（第 8-12 字节，4字节，小端序）
                let batch_len_bytes: [u8; 4] = mmap[pos + 8..pos + 12].try_into().unwrap();
                let batch_len = u32::from_le_bytes(batch_len_bytes) as usize;
                position = pos + batch_len;
            }
            record_count = in_memory_index.last().map_or(0, |(ro, _)| ro + 1) as u64;
        }

        // 扫描剩余的 RecordBatch，重建索引
        while position < log_file_size as usize {
            if position + 12 > log_file_size as usize {
                break;
            }
            
            let current_batch_physical_pos = position as usize;
            
            // 读取 RecordBatch 长度（第 8-12 字节）
            let batch_len_bytes: [u8; 4] = mmap[position + 8..position + 12].try_into().unwrap();
            let batch_len = u32::from_le_bytes(batch_len_bytes) as usize;
            
            // 验证 batch 完整性
            if batch_len == 0 || position + batch_len > log_file_size as usize {
                break;
            }


            let relative_offset = record_count as u32;
            bytes_since_last_index += batch_len;
            
            // 根据索引间隔决定是否创建索引条目
            if bytes_since_last_index >= index_interval_bytes || record_count == 0 {
                let relative_offset_bytes = relative_offset.to_le_bytes();
                let physical_position_bytes = (current_batch_physical_pos as u32).to_le_bytes();
                index_file.write_all(&relative_offset_bytes)?;
                index_file.write_all(&physical_position_bytes)?;
                in_memory_index.push((relative_offset, current_batch_physical_pos as u32));
                bytes_since_last_index = 0;
            }

            position += batch_len;
            record_count += 1;
        }

        Ok(Self {
            log_path,
            index_path,
            log_file,
            index_file,
            mmap,
            position,
            base_offset,
            record_count,
            bytes_since_last_index,
            index: in_memory_index,
            index_interval_bytes,
            preallocate,
        })
    }

    fn preallocate_file(file: &File, size: usize) -> Result<(), StorageError> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();

            let result = unsafe {
                libc::fallocate(
                    fd,
                    libc::FALLOC_FL_KEEP_SIZE,
                    0,
                    size as i64,
                )
            };

            if result != 0 {
                return Err(StorageError::IOError(format!("fallocate failed: {}", std::io::Error::last_os_error())));
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            file.set_len(size as u64)?;
        }

        Ok(())
    }

    fn extend_file(&mut self, required_size: usize) -> Result<(), StorageError> {
        if self.preallocate {
            return Err(StorageError::SegmentFull);
        }

        let current_size = self.log_file.metadata()?.len() as usize;
        if required_size > current_size {
            let new_size = std::cmp::max(required_size, current_size * 2);
            self.log_file.set_len(new_size as u64)?;
            self.mmap = unsafe {
                MmapOptions::new()
                    .len(new_size)
                    .map_mut(&self.log_file)?
            };
        }

        Ok(())
    }

    pub fn append_batch(&mut self, data: &Bytes, record_count: u32) -> Result<u64, StorageError> {
        if data.is_empty() {
            return Ok(self.base_offset + self.record_count);
        }

        let total_len = data.len();
        
        // 检查是否需要扩展文件
        if self.position + total_len > self.mmap.len() {
            self.extend_file(self.position + total_len)?;
        }

        let physical_position = self.position as u32;
        self.mmap[self.position..self.position + total_len].copy_from_slice(&data);

        self.position += total_len;
        self.bytes_since_last_index += total_len;

        let logical_offset = self.base_offset + self.record_count;
        let relative_offset = self.record_count as u32;
        let last_logical_offset = logical_offset;

        if self.bytes_since_last_index >= self.index_interval_bytes || self.record_count == 0 {
            let physical_position_bytes = physical_position.to_le_bytes();
            let relative_offset_bytes = relative_offset.to_le_bytes();
            self.index_file.write_all(&relative_offset_bytes)?;
            self.index_file.write_all(&physical_position_bytes)?;
            self.index.push((relative_offset, physical_position));
            self.bytes_since_last_index = 0;
        }

        self.record_count += record_count as u64;

        Ok(last_logical_offset)
    }

    pub fn read_batch(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>, StorageError> {
        // 先通过稀疏索引定位到起始物理位置
        let start_index = match self.index.binary_search_by_key(&(start_offset as u32), |&(offset, _)| offset) {
            Ok(index) => index,
            Err(index) => {
                if index == 0 {
                    return Ok(vec![]);
                }
                index - 1
            },
        };

        let (mut current_offset, mut current_pos) = self.index[start_index];
        let mut result = Vec::new();
        let mut total_bytes = 0;

        // 向后遍历，直到找到start_offset对应的batch
        while current_pos < self.position as u32 {
            let pos = current_pos as usize;
            if pos + 12 > self.position {
                break;
            }

            // 读取batch头部，batch长度在第8-12字节（即[8..12]，4字节，le）
            let batch_len_bytes: [u8; 4] = self.mmap[pos + 8..pos + 12].try_into().map_err(|_| StorageError::DataCorruption)?;
            let batch_len = u32::from_le_bytes(batch_len_bytes) as usize;

            let batch_end = pos + batch_len;
            if batch_end > self.position {
                break;
            }

            // 跳过offset小于start_offset的batch
            if current_offset < start_offset as u32 {
                current_offset += 1;
                current_pos += batch_len as u32;
                continue;
            }

            // 读取batch数据
            let batch_data = &self.mmap[pos..batch_end];
            if total_bytes + batch_data.len() > max_bytes && !result.is_empty() {
                break;
            }
            result.push(Bytes::copy_from_slice(batch_data));
            total_bytes += batch_data.len();

            current_offset += 1;
            current_pos += batch_len as u32;
        }

        Ok(result)
    }

    // todo: 预分配与动态增长模式下的不同逻辑
    pub fn truncate(&mut self, offset: u64) -> Result<(), StorageError> {
        let base_offset = self.base_offset as u64;

        if offset < base_offset {
            // offset在本segment之前，返回InvalidOffset错误
            return Err(StorageError::InvalidOffset);
        }

        let rel_offset = (offset - base_offset) as u32;

        // 二分查找index，找到第一个大于等于rel_offset的位置
        let idx = match self.index.binary_search_by_key(&rel_offset, |&(logical_offset, _)| logical_offset) {
            Ok(i) => i, // 精确命中
            Err(i) => i, // 没有精确命中，i为第一个大于rel_offset的位置
        };

        // 如果idx为0，说明所有记录都大于要截断的offset，直接清空
        if idx == 0 {
            self.position = 0;
            self.index.clear();
            self.mmap.flush()?;
            // 实际截断文件到0大小
            self.log_file.set_len(0)?;
            self.log_file.sync_all()?;
            let new_mmap = unsafe { MmapOptions::new().map_mut(&self.log_file)? };
            let _ = std::mem::replace(&mut self.mmap, new_mmap);
            self.index_file.set_len(0)?;
            self.index_file.sync_all()?;
            return Ok(());
        }

        // 截断到idx之前的最后一个batch
        let (_logical_offset, physical_pos) = self.index[idx - 1];
        let pos = physical_pos as usize;

        // 读取batch头部，batch长度在第8-12字节（即[8..12]，4字节，le）
        if pos + 12 > self.position {
            // 数据损坏，无法读取batch长度
            return Err(StorageError::DataCorruption);
        }
        let batch_len_bytes: [u8; 4] = self.mmap[pos + 8..pos + 12]
            .try_into()
            .map_err(|_| StorageError::DataCorruption)?;
        let batch_len = u32::from_le_bytes(batch_len_bytes) as usize;
        let batch_end = pos + batch_len;

        if batch_end > self.position {
            return Err(StorageError::DataCorruption);
        }

        // 截断mmap文件
        self.mmap.flush()?;
        self.log_file.set_len(batch_end as u64)?;
        self.log_file.sync_all()?;
        let new_mmap = unsafe { MmapOptions::new().map_mut(&self.log_file)? };
        let _ = std::mem::replace(&mut self.mmap,new_mmap );
        self.position = batch_end;

        // 截断index
        self.index.truncate(idx);
        self.index_file.set_len((idx * 8) as u64)?;
        self.index_file.sync_all()?;

        Ok(())
    }


    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.mmap.flush()?;
        self.index_file.sync_all()?;
        Ok(())
    }

    pub fn delete(self) -> Result<(), StorageError> {
        match fs::remove_file(self.log_path) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        };
        match fs::remove_file(self.index_path) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        };
        Ok(())
    }


} 