use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// `LogStorage` is an abstract definition of Aeon's storage engine behavior.
///
/// It operates on batches of records (`RecordBatch`) as opaque byte blocks,
/// making it a "logically blind" component focused on efficient and reliable persistence.
/// The storage layer does not understand the internal structure of the batches; it trusts
/// the upper layers (e.g., the Broker) to provide validated data and necessary metadata
/// like the number of records in a batch.
///
/// Any struct that implements this trait can be used as the underlying storage engine for Aeon.
#[async_trait]
pub trait LogStorage: Send + Sync {
    /// Append a complete `RecordBatch` to the log.
    ///
    /// This method is the core of the write path. It takes a `RecordBatch` as an
    /// opaque `Bytes` block and persists it. The number of records within the batch
    /// is provided by the caller, as the storage layer itself does not parse the batch.
    ///
    /// # Arguments
    /// * `batch`: The opaque byte block of the `RecordBatch`.
    /// * `record_count`: The number of records in the batch, used to update the logical offset.
    ///
    /// # Returns
    /// On success, returns the logical offset assigned to the first record of this batch.
    async fn append_batch(&mut self, batch: Bytes, record_count: u32) -> Result<u64>;

    /// Read one or more complete `RecordBatch`es from the log, starting at a given offset.
    ///
    /// This method is the core of the read path and is designed to support zero-copy reads.
    /// It returns the raw byte blocks of the batches, which can be sent directly over the network.
    ///
    /// # Arguments
    /// * `start_offset`: The logical offset to start reading from.
    /// * `max_bytes`: The maximum number of bytes to return in a single call. The implementation
    ///                should try to return complete batches without exceeding this limit.
    ///
    /// # Returns
    /// A `Vec<Bytes>` where each `Bytes` element is a complete, raw `RecordBatch`.
    async fn read_batch(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Bytes>>;

    /// Return the earliest logical offset available in this log (log start offset).
    async fn earliest_offset(&self) -> Result<u64>;

    /// Return the latest logical offset (the next offset after the last record in the log).
    async fn latest_offset(&self) -> Result<u64>;

    /// Alias of earliest_offset for clarity with Kafka naming.
    async fn log_start_offset(&self) -> Result<u64> { self.earliest_offset().await }

    /// Truncates the log file to the specified physical byte position.
    ///
    /// This is a pure physical operation, invoked by the Broker's recovery logic
    /// at startup to remove corrupted data resulting from an unexpected shutdown.
    ///
    /// # Arguments
    /// * `last_valid_logical_offset`: The exact byte size to which the file should be truncated.
    async fn truncate(&mut self, last_valid_logical_offset: u64) -> Result<()>;

    /// Force all buffered in-memory data to be flushed to persistent storage (such as disk).
    ///
    /// This method guarantees that after it returns successfully, data previously appended
    /// will not be lost in the event of a system crash.
    async fn flush(&mut self) -> Result<()>;

    /// Clean up old log segments according to the configured retention policy.
    async fn cleanup(&mut self) -> Result<()>;

    /// Delete all physical files and directories related to this log.
    ///
    /// This is a consuming operation (`self: Box<Self>`) because it will destroy
    /// the storage instance itself. After calling this method, the storage instance
    /// can no longer be used.
    async fn delete(self: Box<Self>) -> Result<()>;
}
