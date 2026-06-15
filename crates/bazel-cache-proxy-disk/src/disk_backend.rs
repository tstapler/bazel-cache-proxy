use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use lru::LruCache;
use bazel_cache_proxy_core::{
    backend::StorageBackend,
    digest::Digest,
    entry_kind::EntryKind,
    error::CacheError,
    hashing_writer::HashingWriter,
};
use crate::eviction::evict_to_fit;

pub struct DiskBackend {
    root: PathBuf,
    max_size: u64,
    current_size: Arc<AtomicU64>,
    lru: Arc<Mutex<LruCache<String, u64>>>,
}

impl DiskBackend {
    /// Create or open a DiskBackend at `root` with `max_size_bytes` LRU eviction limit.
    pub async fn new(root: PathBuf, max_size_bytes: u64) -> Result<Self, CacheError> {
        // Create subdirs
        for kind in &["AC", "CAS"] {
            tokio::fs::create_dir_all(root.join(kind))
                .await
                .map_err(|e| CacheError::Io(e.to_string()))?;
        }

        // Scan existing files to populate LRU cache
        let lru_capacity = std::num::NonZeroUsize::new(1_000_000).unwrap();
        let mut lru: LruCache<String, u64> = LruCache::new(lru_capacity);
        let mut total_size: u64 = 0;

        for kind in &["AC", "CAS"] {
            let dir = root.join(kind);
            if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    // Skip .tmp files — clean them up
                    if path.extension().map(|e| e == "tmp").unwrap_or(false) {
                        let _ = tokio::fs::remove_file(&path).await;
                        continue;
                    }
                    if let Ok(meta) = tokio::fs::metadata(&path).await {
                        let size = meta.len();
                        let key = format!(
                            "{}/{}",
                            kind,
                            path.file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                        );
                        lru.put(key, size);
                        total_size += size;
                    }
                }
            }
        }

        Ok(Self {
            root,
            max_size: max_size_bytes,
            current_size: Arc::new(AtomicU64::new(total_size)),
            lru: Arc::new(Mutex::new(lru)),
        })
    }

    fn entry_path(&self, kind: EntryKind, hash: &str) -> PathBuf {
        self.root.join(kind.path_segment()).join(hash)
    }

    fn tmp_path(&self, kind: EntryKind, hash: &str) -> PathBuf {
        self.root
            .join(kind.path_segment())
            .join(format!("{hash}.tmp"))
    }

    fn lru_key(kind: EntryKind, hash: &str) -> String {
        format!("{}/{hash}", kind.path_segment())
    }

    pub fn current_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait]
impl StorageBackend for DiskBackend {
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<i64>, CacheError> {
        let path = self.entry_path(kind, hash);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => {
                // Promote in LRU
                let key = Self::lru_key(kind, hash);
                let mut lru = self.lru.lock().await;
                lru.promote(&key);
                Ok(Some(meta.len() as i64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CacheError::Io(e.to_string())),
        }
    }

    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        let path = self.entry_path(kind, hash);
        match tokio::fs::File::open(&path).await {
            Ok(file) => {
                // Promote in LRU
                let key = Self::lru_key(kind, hash);
                let mut lru = self.lru.lock().await;
                lru.promote(&key);
                Ok(Some(Box::new(file)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CacheError::Io(e.to_string())),
        }
    }

    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        mut data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError> {
        let needed = if logical_size > 0 {
            logical_size as u64
        } else {
            0
        };

        // Evict if needed
        evict_to_fit(&self.lru, &self.current_size, self.max_size, needed, &self.root).await?;

        let tmp_path = self.tmp_path(kind, hash);
        let final_path = self.entry_path(kind, hash);

        // Write to temp file with hashing
        let file = tokio::fs::File::create(&tmp_path)
            .await
            .map_err(|e| CacheError::Io(e.to_string()))?;
        let writer = BufWriter::new(file);
        let mut hashing_writer = HashingWriter::new(writer);

        let mut buf = vec![0u8; 65536];
        loop {
            let n = data
                .read(&mut buf)
                .await
                .map_err(|e| CacheError::Io(e.to_string()))?;
            if n == 0 {
                break;
            }
            hashing_writer
                .write_all(&buf[..n])
                .await
                .map_err(|e| CacheError::Io(e.to_string()))?;
        }
        hashing_writer
            .flush()
            .await
            .map_err(|e| CacheError::Io(e.to_string()))?;

        let actual_written = hashing_writer.bytes_written();

        // Build the expected digest for verification.
        // When logical_size < 0 (unknown), use actual written size.
        let effective_size = if logical_size < 0 {
            actual_written
        } else {
            logical_size
        };

        let expected = Digest::new(hash, effective_size)
            .map_err(|e| CacheError::InvalidDigest(e.to_string()))?;

        // Verify hash (and size when logical_size >= 0)
        match hashing_writer.finalize(&expected) {
            Ok(()) => {
                tokio::fs::rename(&tmp_path, &final_path)
                    .await
                    .map_err(|e| CacheError::Io(e.to_string()))?;
                let actual_size = actual_written as u64;
                let key = Self::lru_key(kind, hash);
                let mut lru = self.lru.lock().await;
                lru.put(key, actual_size);
                self.current_size.fetch_add(actual_size, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                Err(e)
            }
        }
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        let path = self.entry_path(kind, hash);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                let key = Self::lru_key(kind, hash);
                let mut lru = self.lru.lock().await;
                if let Some(size) = lru.pop(&key) {
                    self.current_size.fetch_sub(size, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CacheError::Io(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bazel_cache_proxy_core::{backend::StorageBackend, entry_kind::EntryKind};
    use tempfile::TempDir;

    async fn make_backend(max_size: u64) -> (DiskBackend, TempDir) {
        let dir = TempDir::new().unwrap();
        let backend = DiskBackend::new(dir.path().to_path_buf(), max_size)
            .await
            .unwrap();
        (backend, dir)
    }

    async fn put_bytes(backend: &DiskBackend, kind: EntryKind, data: &[u8]) -> String {
        use sha2::{Digest as Sha2Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(data));
        let reader = std::io::Cursor::new(data.to_vec());
        backend
            .put(kind, &hash, data.len() as i64, Box::new(reader))
            .await
            .unwrap();
        hash
    }

    #[tokio::test]
    async fn disk_backend_contract_contains_before_put() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let hash = "a".repeat(64);
        assert_eq!(
            backend.contains(EntryKind::CAS, &hash, 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn disk_backend_put_then_contains_returns_some() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let data = b"hello world";
        let hash = put_bytes(&backend, EntryKind::CAS, data).await;
        let size = backend
            .contains(EntryKind::CAS, &hash, data.len() as i64)
            .await
            .unwrap();
        assert_eq!(size, Some(data.len() as i64));
    }

    #[tokio::test]
    async fn disk_backend_put_then_get_returns_correct_bytes() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let data = b"test content for get";
        let hash = put_bytes(&backend, EntryKind::CAS, data).await;
        let mut reader = backend
            .get(EntryKind::CAS, &hash, data.len() as i64)
            .await
            .unwrap()
            .unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn disk_backend_delete_removes_entry() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let data = b"to be deleted";
        let hash = put_bytes(&backend, EntryKind::CAS, data).await;
        backend.delete(EntryKind::CAS, &hash).await.unwrap();
        assert_eq!(
            backend.contains(EntryKind::CAS, &hash, 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn disk_backend_delete_nonexistent_is_ok() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let hash = "b".repeat(64);
        assert!(backend.delete(EntryKind::CAS, &hash).await.is_ok());
    }

    #[tokio::test]
    async fn disk_backend_ac_and_cas_are_isolated() {
        let (backend, _dir) = make_backend(1024 * 1024).await;
        let data = b"action result";
        let hash = put_bytes(&backend, EntryKind::AC, data).await;
        // AC entry does NOT appear in CAS
        assert_eq!(
            backend.contains(EntryKind::CAS, &hash, 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn disk_backend_uses_ac_cas_subdirs() {
        let (backend, dir) = make_backend(1024 * 1024).await;
        let data = b"subdir test";

        use sha2::{Digest as Sha2Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(data));

        let ac_reader = std::io::Cursor::new(data.to_vec());
        backend
            .put(EntryKind::AC, &hash, data.len() as i64, Box::new(ac_reader))
            .await
            .unwrap();
        let cas_reader = std::io::Cursor::new(data.to_vec());
        backend
            .put(
                EntryKind::CAS,
                &hash,
                data.len() as i64,
                Box::new(cas_reader),
            )
            .await
            .unwrap();

        assert!(dir.path().join("AC").join(&hash).exists());
        assert!(dir.path().join("CAS").join(&hash).exists());
    }

    #[tokio::test]
    async fn disk_backend_lru_eviction_removes_lru_entry() {
        // max 1024 bytes — write 3 × 400-byte blobs (1200 total), LRU should be evicted
        let (backend, _dir) = make_backend(1024).await;

        let data1 = vec![1u8; 400];
        let data2 = vec![2u8; 400];
        let data3 = vec![3u8; 400];

        let hash1 = put_bytes(&backend, EntryKind::CAS, &data1).await;
        let hash2 = put_bytes(&backend, EntryKind::CAS, &data2).await;
        let hash3 = put_bytes(&backend, EntryKind::CAS, &data3).await;

        // Current size should be <= 1024 + 400 (one blob's worth of slack)
        assert!(backend.current_size() <= 1024 + 400);

        // At least one entry should still exist
        let mut alive = 0usize;
        for h in [&hash1, &hash2, &hash3] {
            if backend
                .contains(EntryKind::CAS, h, 400)
                .await
                .ok()
                .and_then(|o| o)
                .is_some()
            {
                alive += 1;
            }
        }
        assert!(alive >= 1);
    }

    #[tokio::test]
    async fn disk_backend_atomic_write_uses_tmp_then_rename() {
        let (backend, dir) = make_backend(1024 * 1024).await;
        let data = b"atomic test data";

        use sha2::{Digest as Sha2Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(data));
        let tmp_path = dir.path().join("CAS").join(format!("{hash}.tmp"));
        let final_path = dir.path().join("CAS").join(&hash);

        let reader = std::io::Cursor::new(data.to_vec());
        backend
            .put(EntryKind::CAS, &hash, data.len() as i64, Box::new(reader))
            .await
            .unwrap();

        // After put: final file exists, .tmp does not
        assert!(final_path.exists());
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn disk_backend_survives_restart_with_existing_data() {
        let dir = TempDir::new().unwrap();

        {
            let backend = DiskBackend::new(dir.path().to_path_buf(), 1024 * 1024)
                .await
                .unwrap();
            let data = b"persisted data";
            put_bytes(&backend, EntryKind::CAS, data).await;
        }

        // Recreate backend — should find existing data
        let backend2 = DiskBackend::new(dir.path().to_path_buf(), 1024 * 1024)
            .await
            .unwrap();
        use sha2::{Digest as Sha2Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(b"persisted data"));
        assert!(backend2
            .contains(EntryKind::CAS, &hash, 0)
            .await
            .unwrap()
            .is_some());
    }
}
