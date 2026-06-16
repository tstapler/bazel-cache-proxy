use async_trait::async_trait;
use tokio::io::AsyncRead;
use crate::{backend::StorageBackend, entry_kind::EntryKind, error::CacheError};

/// A storage backend that always reports cache misses and silently discards writes.
///
/// Used when a backend cannot be initialised at startup (e.g. GHA credentials missing
/// outside of a GitHub Actions environment) so the proxy can still serve requests
/// without crashing.
pub struct NoopBackend;

#[async_trait]
impl StorageBackend for NoopBackend {
    async fn contains(&self, _kind: EntryKind, _hash: &str, _size: i64) -> Result<Option<i64>, CacheError> {
        Ok(None)
    }

    async fn get(&self, _kind: EntryKind, _hash: &str, _size: i64) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        Ok(None)
    }

    async fn put(&self, _kind: EntryKind, _hash: &str, _logical_size: i64, _data: Box<dyn AsyncRead + Send + Unpin>) -> Result<(), CacheError> {
        Ok(())
    }

    async fn delete(&self, _kind: EntryKind, _hash: &str) -> Result<(), CacheError> {
        Ok(())
    }
}
