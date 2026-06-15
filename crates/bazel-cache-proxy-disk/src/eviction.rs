use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use lru::LruCache;
use bazel_cache_proxy_core::error::CacheError;

/// Evict LRU entries until `current_size + needed <= max_size`.
/// Ignores NotFound errors (already evicted by another thread).
pub async fn evict_to_fit(
    lru: &Mutex<LruCache<String, u64>>,
    current_size: &AtomicU64,
    max_size: u64,
    needed: u64,
    root: &Path,
) -> Result<(), CacheError> {
    loop {
        let current = current_size.load(Ordering::Relaxed);
        if max_size == 0 || current + needed <= max_size {
            break;
        }

        let evicted = {
            let mut lru = lru.lock().await;
            lru.pop_lru()
        };

        match evicted {
            None => break, // LRU empty, nothing to evict
            Some((key, size)) => {
                // key format is "KIND/hash"
                let path = root.join(key.replace('/', std::path::MAIN_SEPARATOR_STR));
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {
                        current_size.fetch_sub(size, Ordering::Relaxed);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Already gone — still update accounting
                        let cur = current_size.load(Ordering::Relaxed);
                        current_size.fetch_sub(size.min(cur), Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::warn!("eviction: failed to remove {path:?}: {e}");
                        // Continue evicting
                    }
                }
            }
        }
    }
    Ok(())
}
