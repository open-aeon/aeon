//! Snapshot storage implementations live here.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::raft::types::{GroupId, RaftError, RaftResult, SnapshotData};

use super::SnapshotStore;

pub type DynSnapshotStore = Arc<dyn SnapshotStore>;

#[derive(Debug, Default)]
pub struct MemSnapshotStore {
    inner: RwLock<HashMap<GroupId, SnapshotData>>,
}

impl MemSnapshotStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl SnapshotStore for MemSnapshotStore {
    async fn load_latest(&self, group_id: GroupId) -> RaftResult<Option<SnapshotData>> {
        Ok(self.inner.read().await.get(&group_id).cloned())
    }

    async fn save_latest(&self, group_id: GroupId, snapshot: &SnapshotData) -> RaftResult<()> {
        self.inner
            .write()
            .await
            .insert(group_id, snapshot.clone());
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileSnapshotStore {
    base_dir: PathBuf,
}

impl FileSnapshotStore {
    pub fn new(base_dir: impl AsRef<Path>) -> RaftResult<Arc<Self>> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)
            .map_err(|e| RaftError::Storage(format!("create snapshot dir failed: {e}")))?;
        Ok(Arc::new(Self { base_dir }))
    }

    fn file_path(&self, group_id: GroupId) -> PathBuf {
        self.base_dir.join(format!("group_{group_id}.snapshot"))
    }
}

#[async_trait]
impl SnapshotStore for FileSnapshotStore {
    async fn load_latest(&self, group_id: GroupId) -> RaftResult<Option<SnapshotData>> {
        let path = self.file_path(group_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)
            .map_err(|e| RaftError::Storage(format!("read snapshot failed: {e}")))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let snap: SnapshotData = bincode::deserialize(&bytes)
            .map_err(|e| RaftError::Storage(format!("decode snapshot failed: {e}")))?;
        Ok(Some(snap))
    }

    async fn save_latest(&self, group_id: GroupId, snapshot: &SnapshotData) -> RaftResult<()> {
        let path = self.file_path(group_id);
        let bytes = bincode::serialize(snapshot)
            .map_err(|e| RaftError::Storage(format!("encode snapshot failed: {e}")))?;
        fs::write(path, bytes)
            .map_err(|e| RaftError::Storage(format!("write snapshot failed: {e}")))?;
        Ok(())
    }
}

