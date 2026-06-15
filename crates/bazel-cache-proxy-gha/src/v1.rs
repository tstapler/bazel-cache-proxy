// GHA v1 REST API implementation

use std::time::Duration;

use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use bazel_cache_proxy_core::{entry_kind::EntryKind, error::CacheError};
use bytes::Bytes;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::client::GhaClient;

// ── Serde structs ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CacheEntry {
    #[serde(rename = "archiveLocation")]
    archive_location: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReserveCacheResponse {
    #[serde(rename = "cacheId")]
    cache_id: i64,
}

#[derive(Debug, Serialize)]
struct ReserveRequest {
    key: String,
    version: String,
    #[serde(rename = "cacheSize")]
    cache_size: i64,
}

#[derive(Debug, Serialize)]
struct CommitRequest {
    size: i64,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn cache_key(kind: EntryKind, hash: &str) -> String {
    format!("{}-{}", kind, hash)
}

async fn compress_vec_zstd(data: Vec<u8>) -> Result<Vec<u8>, CacheError> {
    // std::io::Cursor<Vec<u8>> implements tokio::io::AsyncRead directly
    let cursor = std::io::Cursor::new(data);
    let buf_reader = BufReader::new(cursor);
    let mut encoder = ZstdEncoder::new(buf_reader);
    let mut compressed = Vec::new();
    tokio::io::copy(&mut encoder, &mut compressed)
        .await
        .map_err(|e| CacheError::Io(format!("compression failed: {e}")))?;
    Ok(compressed)
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Returns `Ok(Some(-1))` when present (size unknown from this API), `Ok(None)` on miss.
pub async fn contains_v1(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<i64>, CacheError> {
    let key = cache_key(kind, hash);
    let url = client.v1_url(&format!(
        "/_apis/artifactcache/cache?keys={}&api-version=6.0-preview.1",
        key
    ));

    let mut last_err: CacheError = CacheError::BackendUnavailable("no attempts made".into());

    for attempt in 0u32..=3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
        }
        let resp = client
            .http
            .get(&url)
            .header("Authorization", client.auth_header())
            .header("Accept", "application/json;api-version=6.0-preview.1")
            .send()
            .await;

        match resp {
            Err(e) => {
                last_err = CacheError::BackendUnavailable(e.to_string());
                continue;
            }
            Ok(r) if r.status().as_u16() == 204 => {
                // 204 No Content = cache miss
                return Ok(None);
            }
            Ok(r) if r.status().is_server_error() => {
                last_err = CacheError::BackendUnavailable(format!("server error {}", r.status()));
                continue;
            }
            Ok(r) if !r.status().is_success() => {
                return Err(CacheError::Internal(format!(
                    "unexpected status {}",
                    r.status()
                )));
            }
            Ok(r) => {
                let entry: CacheEntry = r.json().await.map_err(|e| {
                    CacheError::Internal(format!("failed to parse cache entry: {e}"))
                })?;
                if entry.archive_location.is_some() {
                    return Ok(Some(-1));
                } else {
                    return Ok(None);
                }
            }
        }
    }

    Err(last_err)
}

/// Returns a streaming reader for the entry, or `None` if not found.
pub async fn get_v1(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
    let key = cache_key(kind, hash);
    let query_url = client.v1_url(&format!(
        "/_apis/artifactcache/cache?keys={}&api-version=6.0-preview.1",
        key
    ));

    // Step 1: resolve the archive location URL
    let mut last_err: CacheError = CacheError::BackendUnavailable("no attempts made".into());
    let mut archive_url: Option<String> = None;

    for attempt in 0u32..=3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
        }
        let resp = client
            .http
            .get(&query_url)
            .header("Authorization", client.auth_header())
            .header("Accept", "application/json;api-version=6.0-preview.1")
            .send()
            .await;

        match resp {
            Err(e) => {
                last_err = CacheError::BackendUnavailable(e.to_string());
                continue;
            }
            Ok(r) if r.status().as_u16() == 204 => return Ok(None),
            Ok(r) if r.status().is_server_error() => {
                last_err =
                    CacheError::BackendUnavailable(format!("server error {}", r.status()));
                continue;
            }
            Ok(r) if !r.status().is_success() => {
                return Err(CacheError::Internal(format!(
                    "unexpected status {}",
                    r.status()
                )));
            }
            Ok(r) => {
                let entry: CacheEntry = r.json().await.map_err(|e| {
                    CacheError::Internal(format!("failed to parse cache entry: {e}"))
                })?;
                match entry.archive_location {
                    None => return Ok(None),
                    Some(loc) => {
                        archive_url = Some(loc);
                        break;
                    }
                }
            }
        }
    }

    let url = match archive_url {
        Some(u) => u,
        None => return Err(last_err),
    };

    // Step 2: download and wrap in ZstdDecoder
    let dl_resp = client
        .http
        .get(&url)
        .send()
        .await
        .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;

    if !dl_resp.status().is_success() {
        return Err(CacheError::Internal(format!(
            "failed to download archive: {}",
            dl_resp.status()
        )));
    }

    let stream = dl_resp
        .bytes_stream()
        .map_err(|e| std::io::Error::other(e.to_string()));
    let stream_reader = StreamReader::new(stream);
    let buf_reader = BufReader::new(stream_reader);
    let decoder = ZstdDecoder::new(buf_reader);

    Ok(Some(Box::new(decoder)))
}

/// Uploads `data` to the GHA cache under the given key.
/// Performs: reserve → compress → upload chunks → commit.
pub async fn put_v1(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
    _logical_size: i64,
    mut data: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<(), CacheError> {
    // Step 1: read all input data into memory
    let mut raw = Vec::new();
    data.read_to_end(&mut raw)
        .await
        .map_err(|e| CacheError::Io(e.to_string()))?;

    // Step 2: compress
    let compressed = compress_vec_zstd(raw).await?;
    let compressed_size = compressed.len() as i64;
    let key = cache_key(kind, hash);

    // Step 3: reserve cache entry
    let reserve_url = client.v1_url("/_apis/artifactcache/caches");
    let reserve_body = ReserveRequest {
        key,
        version: "1".to_string(),
        cache_size: compressed_size,
    };

    let cache_id: i64 = {
        let mut last_err: CacheError = CacheError::BackendUnavailable("no attempts made".into());
        let mut found: Option<i64> = None;
        for attempt in 0u32..=3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
            }
            let resp = client
                .http
                .post(&reserve_url)
                .header("Authorization", client.auth_header())
                .header("Accept", "application/json;api-version=6.0-preview.1")
                .json(&reserve_body)
                .send()
                .await;
            match resp {
                Err(e) => {
                    last_err = CacheError::BackendUnavailable(e.to_string());
                    continue;
                }
                Ok(r) if r.status().as_u16() == 409 => {
                    // Already cached — treat as success
                    return Ok(());
                }
                Ok(r) if r.status().is_server_error() => {
                    last_err = CacheError::BackendUnavailable(format!(
                        "reserve server error {}",
                        r.status()
                    ));
                    continue;
                }
                Ok(r) if !r.status().is_success() => {
                    return Err(CacheError::Internal(format!(
                        "reserve failed with status {}",
                        r.status()
                    )));
                }
                Ok(r) => {
                    let rr: ReserveCacheResponse = r.json().await.map_err(|e| {
                        CacheError::Internal(format!("failed to parse reserve response: {e}"))
                    })?;
                    found = Some(rr.cache_id);
                    break;
                }
            }
        }
        match found {
            Some(id) => id,
            None => return Err(last_err),
        }
    };

    // Step 4: upload compressed data in chunks via PATCH
    const CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32 MiB
    let patch_url = client.v1_url(&format!("/_apis/artifactcache/caches/{}", cache_id));
    let total = compressed.len();
    let compressed_arc = std::sync::Arc::new(compressed);

    let mut offset = 0usize;
    while offset < total {
        let end = (offset + CHUNK_SIZE).min(total);
        let chunk = Bytes::copy_from_slice(&compressed_arc[offset..end]);
        let content_range = format!("bytes {}-{}/{}", offset, end - 1, total);
        let chunk_len = chunk.len();

        let mut last_err: CacheError = CacheError::BackendUnavailable("no attempts made".into());
        let mut ok = false;
        for attempt in 0u32..=3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
            }
            let resp = client
                .http
                .patch(&patch_url)
                .header("Authorization", client.auth_header())
                .header("Accept", "application/json;api-version=6.0-preview.1")
                .header("Content-Type", "application/octet-stream")
                .header("Content-Range", &content_range)
                .header("Content-Length", chunk_len.to_string())
                .body(chunk.clone())
                .send()
                .await;
            match resp {
                Err(e) => {
                    last_err = CacheError::BackendUnavailable(e.to_string());
                    continue;
                }
                Ok(r) if r.status().is_server_error() => {
                    last_err = CacheError::BackendUnavailable(format!(
                        "upload server error {}",
                        r.status()
                    ));
                    continue;
                }
                Ok(r) if !r.status().is_success() => {
                    return Err(CacheError::Internal(format!(
                        "upload failed with status {}",
                        r.status()
                    )));
                }
                Ok(_) => {
                    ok = true;
                    break;
                }
            }
        }
        if !ok {
            return Err(last_err);
        }
        offset = end;
    }

    // Step 5: commit
    let commit_url = client.v1_url(&format!("/_apis/artifactcache/caches/{}", cache_id));
    let commit_body = CommitRequest {
        size: compressed_size,
    };
    let mut last_err: CacheError = CacheError::BackendUnavailable("no attempts made".into());
    for attempt in 0u32..=3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
        }
        let resp = client
            .http
            .post(&commit_url)
            .header("Authorization", client.auth_header())
            .header("Accept", "application/json;api-version=6.0-preview.1")
            .json(&commit_body)
            .send()
            .await;
        match resp {
            Err(e) => {
                last_err = CacheError::BackendUnavailable(e.to_string());
                continue;
            }
            Ok(r) if r.status().is_server_error() => {
                last_err = CacheError::BackendUnavailable(format!(
                    "commit server error {}",
                    r.status()
                ));
                continue;
            }
            Ok(r) if !r.status().is_success() => {
                return Err(CacheError::Internal(format!(
                    "commit failed with status {}",
                    r.status()
                )));
            }
            Ok(_) => return Ok(()),
        }
    }
    Err(last_err)
}
