use anyhow::Result;
use async_trait::async_trait;

/// `LogStorage` is an abstract definition of our storage engine behavior.
///
/// Any struct that implements this trait can be used as the underlying storage engine for Bifrost.
/// This design allows us to easily switch or add new storage implementations in the future (for example, switching from mmap to io_uring)
/// without modifying the upper business logic code (such as `Partition`, `Topic`).
///
/// The `Send + Sync` constraint is required because it allows storage engine instances to be safely shared in a multithreaded environment.
#[async_trait]
pub trait LogStorage: Send + Sync {
    /// Append a record to the log (as a byte slice).
    ///
    /// On success, returns the logical offset assigned to this record.
    async fn append(&mut self, data: &[u8]) -> Result<u64>;

    /// Append a batch of records to the log (as a byte slice).
    ///
    /// On success, returns the logical offset assigned to this record.
    async fn append_batch(&mut self, data: &[Vec<u8>]) -> Result<u64>;

    /// Read a record by logical offset.
    ///
    /// If the record is found, returns a `Vec<u8>` containing its data.
    /// If the offset is invalid or does not exist, should return an error.
    async fn read(&self, logical_offset: u64) -> Result<Vec<u8>>;

    /// Force all buffered in-memory data to be flushed to persistent storage (such as disk).
    ///
    /// This method guarantees that after it returns successfully, data previously appended will not be lost in the event of a system crash.
    async fn flush(&mut self) -> Result<()>;

    /// Clean up old log segments according to the configured retention policy (e.g., data retention time, total log size).
    async fn cleanup(&mut self) -> Result<()>;

    /// Delete all physical files and directories related to this log.
    ///
    /// This is a consuming operation (`self: Box<Self>`) because it will destroy the storage instance itself.
    /// After calling this method, the storage instance can no longer be used.
    async fn delete(self: Box<Self>) -> Result<()>;
}
