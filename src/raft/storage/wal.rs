//! Raft WAL storage implementations.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::raft::types::{GroupId, LogIndex, RaftEntry, RaftError, RaftResult};

use super::RaftLogStore;

pub type DynRaftLogStore = Arc<dyn RaftLogStore>;

#[derive(Debug, Default)]
pub struct MemRaftLogStore {
    inner: RwLock<HashMap<GroupId, Vec<RaftEntry>>>,
}

impl MemRaftLogStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl RaftLogStore for MemRaftLogStore {
    async fn append(&self, group_id: GroupId, entries: &[RaftEntry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.write().await;
        let group = inner.entry(group_id).or_default();
        group.extend(entries.iter().cloned());
        Ok(())
    }

    async fn entry(&self, group_id: GroupId, index: LogIndex) -> RaftResult<Option<RaftEntry>> {
        if index == 0 {
            return Ok(None);
        }
        let inner = self.inner.read().await;
        let Some(group) = inner.get(&group_id) else {
            return Ok(None);
        };
        Ok(group.iter().find(|e| e.index == index).cloned())
    }

    async fn entries(&self, group_id: GroupId, start: LogIndex, end: LogIndex) -> RaftResult<Vec<RaftEntry>> {
        let inner = self.inner.read().await;
        let Some(group) = inner.get(&group_id) else {
            return Ok(Vec::new());
        };
        Ok(group
            .iter()
            .filter(|e| e.index >= start && e.index < end)
            .cloned()
            .collect())
    }

    async fn last_index(&self, group_id: GroupId) -> RaftResult<LogIndex> {
        let inner = self.inner.read().await;
        let Some(group) = inner.get(&group_id) else {
            return Ok(0);
        };
        Ok(group.last().map(|e| e.index).unwrap_or(0))
    }

    async fn truncate_suffix(&self, group_id: GroupId, from: LogIndex) -> RaftResult<()> {
        let mut inner = self.inner.write().await;
        if let Some(group) = inner.get_mut(&group_id) {
            group.retain(|e| e.index < from);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileRaftLogStore {
    base_dir: PathBuf,
    index: RwLock<HashMap<GroupId, Vec<RaftEntry>>>,
}

impl FileRaftLogStore {
    pub fn new(base_dir: impl AsRef<Path>) -> RaftResult<Arc<Self>> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)
            .map_err(|e| RaftError::Storage(format!("create wal dir failed: {e}")))?;
        Ok(Arc::new(Self {
            base_dir,
            index: RwLock::new(HashMap::new()),
        }))
    }

    fn file_path(&self, group_id: GroupId) -> PathBuf {
        self.base_dir.join(format!("group_{group_id}.wal"))
    }

    fn read_group_entries(&self, group_id: GroupId) -> RaftResult<Vec<RaftEntry>> {
        let path = self.file_path(group_id);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(path).map_err(|e| RaftError::Storage(format!("read wal failed: {e}")))?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        bincode::deserialize(&bytes).map_err(|e| RaftError::Storage(format!("decode wal failed: {e}")))
    }

    fn write_group_entries(&self, group_id: GroupId, entries: &[RaftEntry]) -> RaftResult<()> {
        let path = self.file_path(group_id);
        let bytes = bincode::serialize(entries)
            .map_err(|e| RaftError::Storage(format!("encode wal failed: {e}")))?;
        fs::write(path, bytes).map_err(|e| RaftError::Storage(format!("write wal failed: {e}")))
    }

    async fn load_or_cached(&self, group_id: GroupId) -> RaftResult<Vec<RaftEntry>> {
        {
            let idx = self.index.read().await;
            if let Some(entries) = idx.get(&group_id) {
                return Ok(entries.clone());
            }
        }
        let entries = self.read_group_entries(group_id)?;
        let mut idx = self.index.write().await;
        idx.insert(group_id, entries.clone());
        Ok(entries)
    }
}

#[async_trait]
impl RaftLogStore for FileRaftLogStore {
    async fn append(&self, group_id: GroupId, entries: &[RaftEntry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut group = self.load_or_cached(group_id).await?;
        group.extend(entries.iter().cloned());
        self.write_group_entries(group_id, &group)?;
        self.index.write().await.insert(group_id, group);
        Ok(())
    }

    async fn entry(&self, group_id: GroupId, index: LogIndex) -> RaftResult<Option<RaftEntry>> {
        if index == 0 {
            return Ok(None);
        }
        let group = self.load_or_cached(group_id).await?;
        Ok(group.into_iter().find(|e| e.index == index))
    }

    async fn entries(&self, group_id: GroupId, start: LogIndex, end: LogIndex) -> RaftResult<Vec<RaftEntry>> {
        let group = self.load_or_cached(group_id).await?;
        Ok(group
            .into_iter()
            .filter(|e| e.index >= start && e.index < end)
            .collect())
    }

    async fn last_index(&self, group_id: GroupId) -> RaftResult<LogIndex> {
        let group = self.load_or_cached(group_id).await?;
        Ok(group.last().map(|e| e.index).unwrap_or(0))
    }

    async fn truncate_suffix(&self, group_id: GroupId, from: LogIndex) -> RaftResult<()> {
        let mut group = self.load_or_cached(group_id).await?;
        group.retain(|e| e.index < from);
        self.write_group_entries(group_id, &group)?;
        self.index.write().await.insert(group_id, group);
        Ok(())
    }
}

