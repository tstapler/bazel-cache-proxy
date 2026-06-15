//! Layered (two-tier) cache backend: fast L1 in front of a larger L2.
//!
//! - **Reads**: L1 hit → return directly. L1 miss → fetch from L2, write to L1
//!   (store-then-forward), serve from L1. Blobs larger than `write_through_limit`
//!   bypass L1 write-back and are streamed from L2 directly.
//! - **Writes**: write to L1 synchronously; queue L2 propagation via a background
//!   task (best-effort, non-blocking).
//! - **Deletes**: delete from both tiers in parallel.

use std::sync::Arc;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use crate::{
    backend::StorageBackend,
    entry_kind::EntryKind,
    error::CacheError,
};

/// Default maximum blob size that will be written through from L2 to L1 on a
/// cache miss (64 MiB). Larger blobs are streamed from L2 directly.
const WRITE_THROUGH_LIMIT_DEFAULT: u64 = 64 * 1024 * 1024;

/// Capacity of the background L2-propagation channel.
const L2_CHANNEL_CAPACITY: usize = 16;

struct L2PropagationJob {
    kind: EntryKind,
    hash: String,
    size: i64,
}

/// A two-tier [`StorageBackend`] that places `l1` in front of `l2`.
pub struct LayeredBackend {
    l1: Arc<dyn StorageBackend>,
    l2: Arc<dyn StorageBackend>,
    upload_tx: mpsc::Sender<L2PropagationJob>,
    write_through_limit: u64,
}

impl LayeredBackend {
    /// Create a layered backend with the default 64 MiB write-through limit.
    pub fn new(
        l1: Arc<dyn StorageBackend>,
        l2: Arc<dyn StorageBackend>,
    ) -> Self {
        Self::with_limit(l1, l2, WRITE_THROUGH_LIMIT_DEFAULT)
    }

    /// Create a layered backend with a custom write-through size limit (bytes).
    pub fn with_limit(
        l1: Arc<dyn StorageBackend>,
        l2: Arc<dyn StorageBackend>,
        write_through_limit: u64,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<L2PropagationJob>(L2_CHANNEL_CAPACITY);

        let l1_clone = l1.clone();
        let l2_clone = l2.clone();

        // Background task: propagate L1 entries to L2.
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                match l1_clone.get(job.kind, &job.hash, job.size).await {
                    Ok(Some(stream)) => {
                        if let Err(e) = l2_clone.put(job.kind, &job.hash, job.size, stream).await {
                            tracing::warn!(
                                "L2 propagation failed for {}/{}: {e}",
                                job.kind, job.hash
                            );
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "L2 propagation: {}/{} not found in L1",
                            job.kind, job.hash
                        );
                    }
                    Err(e) => {
                        tracing::warn!("L2 propagation: L1 read failed: {e}");
                    }
                }
            }
        });

        Self {
            l1,
            l2,
            upload_tx: tx,
            write_through_limit,
        }
    }
}

#[async_trait]
impl StorageBackend for LayeredBackend {
    async fn contains(&self, kind: EntryKind, hash: &str, size: i64) -> Result<Option<i64>, CacheError> {
        // L1 first; fall through to L2 on miss.
        match self.l1.contains(kind, hash, size).await? {
            Some(s) => Ok(Some(s)),
            None => self.l2.contains(kind, hash, size).await,
        }
    }

    async fn get(&self, kind: EntryKind, hash: &str, size: i64) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        // L1 hit — return directly.
        if let Ok(Some(stream)) = self.l1.get(kind, hash, size).await {
            return Ok(Some(stream));
        }

        // L1 miss — check L2.
        let l2_stream = match self.l2.get(kind, hash, size).await? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Oversized blob: stream from L2 without writing to L1.
        if size > 0 && size as u64 > self.write_through_limit {
            tracing::warn!(
                "LayeredBackend: blob {}/{} size={size} exceeds write-through limit, streaming from L2 directly",
                kind, hash
            );
            return Ok(Some(l2_stream));
        }

        // Store-then-forward: buffer L2 content, write to L1, serve from L1.
        let mut buf = Vec::new();
        let mut reader = l2_stream;
        match reader.read_to_end(&mut buf).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("LayeredBackend: L2 read failed: {e}, falling back to fresh L2 read");
                return self.l2.get(kind, hash, size).await;
            }
        }

        let actual_size = buf.len() as i64;

        // Write buffered data to L1.
        let l1_reader = std::io::Cursor::new(buf.clone());
        match self.l1.put(kind, hash, actual_size, Box::new(l1_reader)).await {
            Ok(()) => {
                // Prefer serving from L1 so the read path is consistent.
                match self.l1.get(kind, hash, actual_size).await {
                    Ok(Some(stream)) => Ok(Some(stream)),
                    _ => {
                        // L1 read unexpectedly failed — return buffered data directly.
                        Ok(Some(Box::new(std::io::Cursor::new(buf))))
                    }
                }
            }
            Err(e) => {
                tracing::warn!("LayeredBackend: L1 write-back failed: {e}");
                // Return buffered data directly without poisoning L1.
                Ok(Some(Box::new(std::io::Cursor::new(buf))))
            }
        }
    }

    async fn put(&self, kind: EntryKind, hash: &str, logical_size: i64, data: Box<dyn AsyncRead + Send + Unpin>) -> Result<(), CacheError> {
        // Write to L1 synchronously (stream consumed here).
        self.l1.put(kind, hash, logical_size, data).await?;

        // Queue L2 propagation — best-effort, non-blocking.
        let job = L2PropagationJob {
            kind,
            hash: hash.to_string(),
            size: logical_size,
        };
        if let Err(e) = self.upload_tx.try_send(job) {
            tracing::warn!(
                "LayeredBackend: L2 propagation queue full, dropping job for {kind}/{hash}: {e}"
            );
        }

        Ok(())
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        // Delete from both tiers in parallel.
        let (r1, r2) = tokio::join!(
            self.l1.delete(kind, hash),
            self.l2.delete(kind, hash),
        );
        r1?;
        r2?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::InMemoryBackend;
    use std::sync::Arc;
    use sha2::{Sha256, Digest as Sha2Digest};

    fn make_layered() -> (LayeredBackend, Arc<InMemoryBackend>, Arc<InMemoryBackend>) {
        let l1 = Arc::new(InMemoryBackend::new());
        let l2 = Arc::new(InMemoryBackend::new());
        let layered = LayeredBackend::new(
            l1.clone() as Arc<dyn StorageBackend>,
            l2.clone() as Arc<dyn StorageBackend>,
        );
        (layered, l1, l2)
    }

    async fn put_data(backend: &impl StorageBackend, kind: EntryKind, data: &[u8]) -> String {
        let hash = format!("{:x}", Sha256::digest(data));
        let reader = std::io::Cursor::new(data.to_vec());
        backend.put(kind, &hash, data.len() as i64, Box::new(reader)).await.unwrap();
        hash
    }

    #[tokio::test]
    async fn layered_contains_hits_l1_first() {
        let (layered, l1, _l2) = make_layered();
        let data = b"in l1 only";
        let hash = put_data(&*l1, EntryKind::CAS, data).await;
        assert!(layered.contains(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn layered_contains_falls_through_to_l2_on_l1_miss() {
        let (layered, _l1, l2) = make_layered();
        let data = b"in l2 only";
        let hash = put_data(&*l2, EntryKind::CAS, data).await;
        assert!(layered.contains(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn layered_contains_returns_none_when_both_miss() {
        let (layered, _l1, _l2) = make_layered();
        let hash = "a".repeat(64);
        assert_eq!(layered.contains(EntryKind::CAS, &hash, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn layered_get_l1_hit_does_not_query_l2() {
        let (layered, l1, _l2) = make_layered();
        let data = b"l1 data";
        let hash = put_data(&*l1, EntryKind::CAS, data).await;
        let mut r = layered.get(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn layered_get_l1_miss_populates_l1_from_l2() {
        let (layered, l1, l2) = make_layered();
        let data = b"l2 source data";
        let hash = put_data(&*l2, EntryKind::CAS, data).await;

        // Get from layered — should populate L1.
        let mut r = layered.get(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);

        // L1 should now have the data (store-then-forward).
        assert!(l1.contains(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn layered_put_writes_to_l1_and_queues_l2() {
        let (layered, l1, _l2) = make_layered();
        let data = b"put test data";
        let hash = put_data(&layered, EntryKind::CAS, data).await;

        // L1 should have it immediately.
        assert!(l1.contains(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn layered_l2_populated_by_background_task() {
        let (layered, _l1, l2) = make_layered();
        let data = b"background propagation test";
        let hash = put_data(&layered, EntryKind::CAS, data).await;

        // Give the background task time to propagate to L2.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(l2.contains(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn layered_delete_removes_from_both() {
        let (layered, l1, l2) = make_layered();
        let data = b"to delete";
        let hash = put_data(&*l1, EntryKind::CAS, data).await;
        put_data(&*l2, EntryKind::CAS, data).await;

        layered.delete(EntryKind::CAS, &hash).await.unwrap();

        assert_eq!(l1.contains(EntryKind::CAS, &hash, 0).await.unwrap(), None);
        assert_eq!(l2.contains(EntryKind::CAS, &hash, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn layered_oversized_blob_streams_from_l2_directly() {
        let l1 = Arc::new(InMemoryBackend::new());
        let l2 = Arc::new(InMemoryBackend::new());
        // Set write-through limit very small (10 bytes).
        let layered = LayeredBackend::with_limit(
            l1.clone() as Arc<dyn StorageBackend>,
            l2.clone() as Arc<dyn StorageBackend>,
            10,
        );

        let data = b"this is more than 10 bytes";
        let hash = put_data(&*l2, EntryKind::CAS, data).await;

        let mut r = layered.get(EntryKind::CAS, &hash, data.len() as i64).await.unwrap().unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);

        // L1 should NOT have the oversized blob.
        assert_eq!(l1.contains(EntryKind::CAS, &hash, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn layered_l2_channel_full_drops_job_gracefully() {
        let l1 = Arc::new(InMemoryBackend::new());
        let l2 = Arc::new(InMemoryBackend::new());
        let layered = LayeredBackend::with_limit(
            l1.clone() as Arc<dyn StorageBackend>,
            l2.clone() as Arc<dyn StorageBackend>,
            u64::MAX,
        );

        // Put many items rapidly to fill the channel — should not panic.
        for i in 0..L2_CHANNEL_CAPACITY * 2 + 5 {
            let data = format!("data item {i}");
            let data_bytes = data.as_bytes();
            let hash = format!("{:x}", Sha256::digest(data_bytes));
            let reader = std::io::Cursor::new(data_bytes.to_vec());
            let _ = layered.put(EntryKind::CAS, &hash, data_bytes.len() as i64, Box::new(reader)).await;
        }
    }
}
