//! HardState storage implementations live here.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::raft::types::{GroupId, HardState, RaftError, RaftResult};

use super::HardStateStore;

pub type DynHardStateStore = Arc<dyn HardStateStore>;

#[derive(Debug, Default)]
pub struct MemHardStateStore {
    inner: RwLock<HashMap<GroupId, HardState>>,
}

impl MemHardStateStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl HardStateStore for MemHardStateStore {
    async fn load(&self, group_id: GroupId) -> RaftResult<Option<HardState>> {
        Ok(self.inner.read().await.get(&group_id).cloned())
    }

    async fn save(&self, group_id: GroupId, hs: &HardState) -> RaftResult<()> {
        self.inner.write().await.insert(group_id, hs.clone());
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileHardStateStore {
    base_dir: PathBuf,
}

impl FileHardStateStore {
    pub fn new(base_dir: impl AsRef<Path>) -> RaftResult<Arc<Self>> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)
            .map_err(|e| RaftError::Storage(format!("create hard_state dir failed: {e}")))?;
        Ok(Arc::new(Self { base_dir }))
    }

    fn file_path(&self, group_id: GroupId) -> PathBuf {
        self.base_dir.join(format!("group_{group_id}.hard_state"))
    }
}

#[async_trait]
impl HardStateStore for FileHardStateStore {
    async fn load(&self, group_id: GroupId) -> RaftResult<Option<HardState>> {
        let path = self.file_path(group_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)
            .map_err(|e| RaftError::Storage(format!("read hard_state failed: {e}")))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let hs: HardState = bincode::deserialize(&bytes)
            .map_err(|e| RaftError::Storage(format!("decode hard_state failed: {e}")))?;
        Ok(Some(hs))
    }

    async fn save(&self, group_id: GroupId, hs: &HardState) -> RaftResult<()> {
        let path = self.file_path(group_id);
        let bytes = bincode::serialize(hs)
            .map_err(|e| RaftError::Storage(format!("encode hard_state failed: {e}")))?;
        fs::write(path, bytes)
            .map_err(|e| RaftError::Storage(format!("write hard_state failed: {e}")))?;
        Ok(())
    }
}

