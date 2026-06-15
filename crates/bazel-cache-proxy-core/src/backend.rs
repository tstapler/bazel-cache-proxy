use std::sync::Arc;
use async_trait::async_trait;
use tokio::io::AsyncRead;
use crate::{entry_kind::EntryKind, error::CacheError};

/// Pluggable async storage backend for cache entries.
///
/// All methods are idempotent:
/// - `put` with the same hash twice is a no-op (last write wins).
/// - `delete` on a non-existent entry returns `Ok(())`.
/// - `contains` never mutates state.
///
/// `get` and `put` operate on raw bytes; callers are responsible for
/// encoding/decoding higher-level types (e.g., protobuf `ActionResult`).
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Returns `Some(size_bytes)` if the entry exists, `None` if not found.
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<i64>, CacheError>;

    /// Returns a streaming reader for the entry, or `None` if not found.
    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError>;

    /// Writes `data` as a cache entry. Idempotent.
    ///
    /// `logical_size` is the uncompressed content length in bytes.
    /// Implementations that do not verify this value should still accept it
    /// for preflight size checks.
    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError>;

    /// Removes the entry. Returns `Ok(())` if the entry did not exist.
    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError>;
}

/// Blanket impl so `Arc<T>` can be used wherever `T: StorageBackend`.
#[async_trait]
impl<T: StorageBackend> StorageBackend for Arc<T> {
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<i64>, CacheError> {
        (**self).contains(kind, hash, size).await
    }

    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        (**self).get(kind, hash, size).await
    }

    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError> {
        (**self).put(kind, hash, logical_size, data).await
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        (**self).delete(kind, hash).await
    }
}
