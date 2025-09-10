use crate::raft::types::{AppData, AppResponse, SnapshotData};
use raft::{
    Storage, StorageError, GetEntriesContext, RaftState,
    eraftpb::{Entry, HardState, Snapshot, ConfState},
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

/// Memory-based storage implementation for raft-rs
/// 
/// This implementation stores all Raft state in memory and is suitable for
/// development and testing. In production, this should be replaced with
/// persistent storage.
#[derive(Clone, Debug)]
pub struct MemoryStorage {
    /// Raft log entries
    entries: Arc<RwLock<Vec<Entry>>>,
    /// Hard state (term, vote, commit)
    hard_state: Arc<RwLock<HardState>>,
    /// Current snapshot
    snapshot: Arc<RwLock<Snapshot>>,
    /// Configuration state
    conf_state: Arc<RwLock<ConfState>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        let mut conf_state = ConfState::default();
        conf_state.voters = vec![1]; // Default single node configuration
        
        Self {
            entries: Arc::new(RwLock::new(vec![Entry::default()])), // Start with empty entry at index 0
            hard_state: Arc::new(RwLock::new(HardState::default())),
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
            conf_state: Arc::new(RwLock::new(conf_state)),
        }
    }

    /// Create a new storage with initial configuration
    pub fn new_with_conf_state(conf_state: ConfState) -> Self {
        Self {
            entries: Arc::new(RwLock::new(vec![Entry::default()])),
            hard_state: Arc::new(RwLock::new(HardState::default())),
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
            conf_state: Arc::new(RwLock::new(conf_state)),
        }
    }

    /// Append new entries to the log
    pub fn append(&self, entries: &[Entry]) -> Result<(), StorageError> {
        let mut log = self.entries.write().unwrap();
        
        if entries.is_empty() {
            return Ok(());
        }

        let first_index = entries[0].index;
        if first_index > log.len() as u64 {
            return Err(StorageError::Unavailable);
        }

        // Remove conflicting entries and append new ones
        if first_index < log.len() as u64 {
            log.truncate(first_index as usize);
        }
        
        log.extend_from_slice(entries);
        Ok(())
    }

    /// Set hard state
    pub fn set_hard_state(&self, hs: HardState) {
        let mut hard_state = self.hard_state.write().unwrap();
        *hard_state = hs;
    }

    /// Apply snapshot
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), StorageError> {
        let mut snap = self.snapshot.write().unwrap();
        *snap = snapshot.clone();

        let mut conf_state = self.conf_state.write().unwrap();
        *conf_state = snapshot.metadata.as_ref().unwrap().conf_state.as_ref().unwrap().clone();

        // Truncate log to snapshot
        let mut entries = self.entries.write().unwrap();
        entries.clear();
        entries.push(Entry::default()); // Keep dummy entry at index 0
        
        Ok(())
    }

    /// Compact log entries up to the given index
    pub fn compact(&self, compact_index: u64) -> Result<(), StorageError> {
        let mut entries = self.entries.write().unwrap();
        
        if compact_index == 0 {
            return Err(StorageError::Compacted);
        }

        if compact_index > entries.len() as u64 - 1 {
            return Err(StorageError::Unavailable);
        }

        // Keep entries from compact_index onwards
        let remaining: Vec<Entry> = entries.drain((compact_index as usize)..).collect();
        entries.clear();
        
        // Add dummy entry at index 0
        let mut dummy = Entry::default();
        if !remaining.is_empty() {
            dummy.index = compact_index - 1;
            dummy.term = remaining[0].term;
        }
        entries.push(dummy);
        entries.extend(remaining);
        
        Ok(())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.hard_state.read().unwrap().clone();
        let conf_state = self.conf_state.read().unwrap().clone();
        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let entries = self.entries.read().unwrap();
        let max_size = max_size.into().unwrap_or(u64::MAX);
        
        if low < 1 {
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        if high > entries.len() as u64 {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }

        let mut size = 0u64;
        let mut result = Vec::new();
        
        for entry in &entries[low as usize..high as usize] {
            size += entry.data.len() as u64 + 24; // Approximate entry overhead
            if size > max_size && !result.is_empty() {
                break;
            }
            result.push(entry.clone());
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        
        if idx == 0 {
            return Ok(0);
        }

        if idx < 1 || idx >= entries.len() as u64 {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }

        Ok(entries[idx as usize].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if entries.len() <= 1 {
            Ok(1)
        } else {
            Ok(entries[1].index)
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if entries.len() <= 1 {
            Ok(0)
        } else {
            Ok(entries.last().unwrap().index)
        }
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let snapshot = self.snapshot.read().unwrap();
        
        if snapshot.metadata.is_some() && snapshot.metadata.as_ref().unwrap().index >= request_index {
            Ok(snapshot.clone())
        } else {
            Err(raft::Error::Store(StorageError::SnapshotTemporarilyUnavailable))
        }
    }
}

/// Memory-based state machine for applying Raft entries
/// 
/// This is a simple key-value store that applies committed Raft entries
/// to maintain application state.
#[derive(Debug)]
pub struct MemoryStateMachine {
    /// Application data storage
    data: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    /// Last applied index
    last_applied: Arc<Mutex<u64>>,
}

impl MemoryStateMachine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            last_applied: Arc::new(Mutex::new(0)),
        }
    }

    /// Apply a single entry to the state machine
    pub fn apply(&self, entry: &Entry) -> Result<AppResponse, Box<dyn std::error::Error>> {
        if entry.data.is_empty() {
            // Empty entry (heartbeat), just update last_applied
            let mut last_applied = self.last_applied.lock().unwrap();
            *last_applied = entry.index;
            return Ok(AppResponse { result: vec![] });
        }

        // Deserialize application data
        let app_data: AppData = bincode::deserialize(&entry.data)?;
        
        // For PoC, we'll store data with a key based on the entry index
        let key = format!("entry_{}", entry.index);
        
        {
            let mut data = self.data.lock().unwrap();
            data.insert(key, app_data.payload.clone());
        }

        // Update last applied index
        {
            let mut last_applied = self.last_applied.lock().unwrap();
            *last_applied = entry.index;
        }

        // Return simple response
        Ok(AppResponse {
            result: format!("Applied entry {}", entry.index).into_bytes(),
        })
    }

    /// Get current state for snapshot creation
    pub fn snapshot(&self) -> Result<SnapshotData, Box<dyn std::error::Error>> {
        let data = self.data.lock().unwrap();
        let last_applied = *self.last_applied.lock().unwrap();
        
        let serialized_data = bincode::serialize(&*data)?;
        
        Ok(SnapshotData {
            data: serialized_data,
            meta: crate::raft::types::SnapshotMeta {
                last_included_index: last_applied,
                last_included_term: 0, // TODO: Get actual term
                nodes: vec![1], // TODO: Get actual node list
            },
        })
    }

    /// Restore state from snapshot
    pub fn restore(&self, snapshot_data: &SnapshotData) -> Result<(), Box<dyn std::error::Error>> {
        let restored_data: BTreeMap<String, Vec<u8>> = bincode::deserialize(&snapshot_data.data)?;
        
        {
            let mut data = self.data.lock().unwrap();
            *data = restored_data;
        }

        {
            let mut last_applied = self.last_applied.lock().unwrap();
            *last_applied = snapshot_data.meta.last_included_index;
        }

        Ok(())
    }

    pub fn last_applied(&self) -> u64 {
        *self.last_applied.lock().unwrap()
    }
}

impl Default for MemoryStateMachine {
    fn default() -> Self {
        Self::new()
    }
}