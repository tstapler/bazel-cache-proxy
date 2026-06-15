//! Test helpers for `StorageBackend` implementations.
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]` — not included in
//! production builds unless the `testing` feature is explicitly enabled.

#![cfg(any(test, feature = "testing"))]

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::{
    backend::StorageBackend,
    entry_kind::EntryKind,
    error::CacheError,
};

/// In-memory storage backend for testing. Not suitable for production.
#[derive(Clone, Default)]
pub struct InMemoryBackend {
    data: Arc<Mutex<HashMap<(EntryKind, String), Bytes>>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StorageBackend for InMemoryBackend {
    async fn contains(&self, kind: EntryKind, hash: &str, _size: i64) -> Result<Option<i64>, CacheError> {
        let data = self.data.lock().await;
        Ok(data.get(&(kind, hash.to_string())).map(|b| b.len() as i64))
    }

    async fn get(&self, kind: EntryKind, hash: &str, _size: i64) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        let data = self.data.lock().await;
        Ok(data.get(&(kind, hash.to_string())).map(|b| {
            let cursor = std::io::Cursor::new(b.clone());
            Box::new(cursor) as Box<dyn AsyncRead + Send + Unpin>
        }))
    }

    async fn put(&self, kind: EntryKind, hash: &str, _logical_size: i64, mut data: Box<dyn AsyncRead + Send + Unpin>) -> Result<(), CacheError> {
        let mut buf = Vec::new();
        data.read_to_end(&mut buf).await
            .map_err(|e| CacheError::Io(e.to_string()))?;
        let mut store = self.data.lock().await;
        store.insert((kind, hash.to_string()), Bytes::from(buf));
        Ok(())
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        let mut data = self.data.lock().await;
        data.remove(&(kind, hash.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StorageBackend;

    #[tokio::test]
    async fn in_memory_backend_contract_contains_before_put() {
        let backend = InMemoryBackend::new();
        assert_eq!(backend.contains(EntryKind::CAS, &"a".repeat(64), 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn in_memory_backend_contract_put_then_contains() {
        let backend = InMemoryBackend::new();
        let data = b"hello";
        let reader = std::io::Cursor::new(data.to_vec());
        backend.put(EntryKind::CAS, &"a".repeat(64), 5, Box::new(reader)).await.unwrap();
        let size = backend.contains(EntryKind::CAS, &"a".repeat(64), 5).await.unwrap();
        assert_eq!(size, Some(5));
    }

    #[tokio::test]
    async fn in_memory_backend_contract_put_then_get() {
        let backend = InMemoryBackend::new();
        let data = b"test data";
        let reader = std::io::Cursor::new(data.to_vec());
        backend.put(EntryKind::CAS, &"b".repeat(64), data.len() as i64, Box::new(reader)).await.unwrap();
        let mut r = backend.get(EntryKind::CAS, &"b".repeat(64), data.len() as i64).await.unwrap().unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn in_memory_backend_ac_and_cas_isolated() {
        let backend = InMemoryBackend::new();
        let reader = std::io::Cursor::new(b"data".to_vec());
        backend.put(EntryKind::AC, &"c".repeat(64), 4, Box::new(reader)).await.unwrap();
        // AC put does NOT appear in CAS
        assert_eq!(backend.contains(EntryKind::CAS, &"c".repeat(64), 4).await.unwrap(), None);
    }

    #[tokio::test]
    async fn in_memory_backend_isolated_between_instances() {
        let b1 = InMemoryBackend::new();
        let b2 = InMemoryBackend::new();
        let reader = std::io::Cursor::new(b"test".to_vec());
        b1.put(EntryKind::CAS, &"d".repeat(64), 4, Box::new(reader)).await.unwrap();
        // b2 shares no state with b1
        assert_eq!(b2.contains(EntryKind::CAS, &"d".repeat(64), 4).await.unwrap(), None);
    }
}
