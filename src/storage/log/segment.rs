use crate::error::storage::StorageError;
use memmap2::{MmapMut, MmapOptions};
use crc::{Crc, CRC_32_ISCSI};
use std::{
    io::{self, Read, Write, BufReader, Seek, SeekFrom},
    path::Path,
    fs::{File, OpenOptions},
};

const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB per segment
// const INDEX_INTERVAL_BYTES: usize = 4 * 1024; // 4KB

pub struct LogSegment {
    log_file: File,
    index_file: File,
    mmap: MmapMut,
    position: usize,
    base_offset: u64,
    record_count: u64,
    bytes_since_last_index: usize,
    index: Vec<(u32, u32)>,
    index_interval_bytes: usize,
}

impl LogSegment {
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn create(path: &Path, base_offset: u64, index_interval_bytes: usize) -> Result<Self, StorageError> {
        let log_file_name = format!("{:020}.log", base_offset);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.join(log_file_name))?;

        log_file.set_len(SEGMENT_SIZE as u64)?;

        let index_file_name = format!("{:020}.index", base_offset);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.join(index_file_name))?;
            
        let mmap = unsafe { MmapOptions::new().map_mut(&log_file)? };

        Ok(Self {
            log_file,
            index_file,
            mmap,
            position: 0,
            base_offset,
            record_count: 0,
            bytes_since_last_index: 0,
            index: Vec::new(),
            index_interval_bytes,
        })
    }

    pub fn open(path: &Path, base_offset: u64, index_interval_bytes: usize) -> Result<Self, StorageError> {
        let log_file_name = format!("{:020}.log", base_offset);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(log_file_name))?;

        let log_file_metadata = log_file.metadata()?;
        let log_file_size = log_file_metadata.len();
        if log_file_size < SEGMENT_SIZE as u64 {
                log_file.set_len(SEGMENT_SIZE as u64)?;
        }

        let index_file_name = format!("{:020}.index", base_offset);
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(index_file_name))?;

        let mmap = unsafe { MmapOptions::new().map_mut(&log_file)? };

        let mut index_reader = BufReader::new(&index_file);
        let mut record_count :u64 = 0;
        let mut last_known_position :u64 = 0;
        let mut buf = [0u8; 8];
        let mut in_memory_index: Vec<(u32, u32)> = Vec::new();
        loop {
            match index_reader.read_exact(&mut buf) {
                std::result::Result::Ok(()) => {
                    record_count += 1;
                    let relative_offset = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                    let physical_position = u32::from_le_bytes(buf[4..8].try_into().unwrap());
                    last_known_position = physical_position as u64;
                    in_memory_index.push((relative_offset, physical_position));
                }
                std::result::Result::Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                std::result::Result::Err(e) => {
                    return Err(e.into());
                }
            }
        }
        if index_file.metadata()?.len() > record_count * 8 {
            index_file.set_len(record_count * 8)?;
        }
        index_file.seek(SeekFrom::End(0))?;

        let mut current_physical_size = 0;
        if record_count > 0 {
            let pos = last_known_position as usize;
            if pos + 12 <= log_file_size as usize {
                let len_bytes: [u8; 8] = mmap[pos..pos + 8].try_into().unwrap();
                let len = u64::from_le_bytes(len_bytes);
                current_physical_size = last_known_position + 8 + 4 + len;
            } else {
                current_physical_size = last_known_position;
            }
        }

        while current_physical_size < log_file_size {
            if current_physical_size + 12 > log_file_size {
                break;
            }
            let pos = current_physical_size as usize;
            let len_bytes: [u8; 8] = mmap[pos..pos + 8].try_into().unwrap();
            let record_len = u64::from_le_bytes(len_bytes);

            let crc_bytes: [u8; 4] = mmap[pos + 8..pos + 12].try_into().unwrap();
            let stored_crc = u32::from_le_bytes(crc_bytes);

            LogSegment::verify_record(&mmap, log_file_size as usize, pos, record_len, stored_crc)?;

            if record_len == 0 || current_physical_size + 12 + record_len > log_file_size {
                break;
            }

            current_physical_size += 12 + record_len;
            record_count += 1;
        }

        let bytes_since_last_index = if record_count > 0 {
            current_physical_size - last_known_position
        } else {
            0
        };

        Ok(Self {
            log_file,
            index_file,
            mmap,
            position: current_physical_size as usize,
            base_offset,
            record_count,
            bytes_since_last_index: bytes_since_last_index as usize,
            index: in_memory_index,
            index_interval_bytes,
        })

    }

    pub fn append(&mut self, record: &[u8]) -> Result<u64, StorageError> {
        let record_len = record.len() as u64;
        let len_bytes = record_len.to_le_bytes();

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let crc_bytes = crc.checksum(record);
        let crc_bytes = crc_bytes.to_le_bytes();
        
        let total_len = 8 + 4 + record.len();

        if self.position + total_len > SEGMENT_SIZE {
            return Err(StorageError::SegmentFull);
        }

        let physical_position = self.position as u32;

        // Write message length
        self.mmap[self.position..self.position + 8].copy_from_slice(&len_bytes);
        // Write CRC
        self.mmap[self.position + 8..self.position + 12].copy_from_slice(&crc_bytes);
        // Write message content
        self.mmap[self.position + 12..self.position + total_len].copy_from_slice(&record);

        self.position += total_len;
        self.bytes_since_last_index += total_len;

        let relative_offset = self.base_offset + self.record_count;

        if self.bytes_since_last_index >= self.index_interval_bytes || self.record_count == 0 {
            let physical_position_for_index = physical_position as u32;
            let physical_position_bytes = physical_position_for_index.to_le_bytes();
            let relative_offset_bytes = (relative_offset as u32).to_le_bytes();
            let mut index_entry_bytes = Vec::with_capacity(4 + 4);
            index_entry_bytes.extend_from_slice(&relative_offset_bytes);
            index_entry_bytes.extend_from_slice(&physical_position_bytes);
            self.index_file.write_all(&index_entry_bytes)?;
            self.index.push((relative_offset as u32, physical_position_for_index as u32));
            self.bytes_since_last_index = 0;
        }

        self.record_count += 1;

        Ok(relative_offset)
    }

    // todo: 后续考虑改为返回一个引用
    // pub fn read<'a>(&'a self, ...) -> Result<&'a [u8], StorageError>
    pub fn read(&self, relative_offset: u32) -> Result<Option<Vec<u8>>, StorageError> {
       let start_index = match self.index.binary_search_by_key(&relative_offset, |&(offset, _)| offset) {
        Ok(index) => index,
        Err(index) => {
            if index == 0 {
                return Ok(None);
            }
            index - 1
        },
       };

       let (mut current_relative_offset, mut current_physical_position) = self.index[start_index];
       loop {
        if current_physical_position as usize >= self.position {
            break;
        }
        if current_relative_offset > relative_offset {
            break;
        }
        
        let pos = current_physical_position as usize;

        if pos + 12  > self.position {
            break;
        }

        let len_bytes: [u8; 8] = self.mmap[pos..pos + 8].try_into().unwrap();
        let record_len = u64::from_le_bytes(len_bytes);

        let crc_bytes: [u8; 4] = self.mmap[pos + 8..pos + 12].try_into().unwrap();
        let stored_crc = u32::from_le_bytes(crc_bytes);

        LogSegment::verify_record(&self.mmap, self.position, pos, record_len, stored_crc)?;
        
        if current_relative_offset == relative_offset {
            let data_start = pos + 12;
            let data_end = data_start + record_len as usize;

            if data_end > self.position {
                return Err(StorageError::InvalidOffset);
            }

            let data = &self.mmap[data_start..data_end];
            return Ok(Some(data.to_vec()));
        }
        
        current_relative_offset += 1;
        current_physical_position += (12 + record_len) as u32;
       }
       Ok(None)
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.mmap.flush()?;
        self.index_file.sync_all()?;
        Ok(())
    }

    fn verify_record(mmap: &MmapMut, position: usize, physical_pos: usize, len: u64, stored_crc: u32) -> Result<(), StorageError> {
        let data_start = physical_pos + 12;
        let data_end = data_start + len as usize;
        
        if data_end > position {
            return Err(StorageError::DataCorruption); // 记录不完整
        }

        let data = &mmap[data_start..data_end];
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let calculated_crc = crc.checksum(data);

        if calculated_crc == stored_crc {
            Ok(())
        } else {
            Err(StorageError::DataCorruption)
        }
    }
} 