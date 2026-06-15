// GHA v2 Twirp/JSON API implementation

use std::time::Duration;

use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use bazel_cache_proxy_core::{entry_kind::EntryKind, error::CacheError};
use base64::Engine as _;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::client::GhaClient;

// ── Azure blob upload constants ───────────────────────────────────────────────

const SINGLE_PUT_THRESHOLD: usize = 256 * 1024 * 1024; // 256 MiB
const BLOCK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

// ── Serde structs ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct GetCacheEntryDownloadURLRequest<'a> {
    key: &'a str,
    version: &'a str,
    #[serde(rename = "restoreKeys")]
    restore_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GetCacheEntryDownloadURLResponse {
    #[serde(default)]
    ok: bool,
    #[serde(rename = "signedDownloadUrl")]
    signed_download_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateCacheEntryRequest<'a> {
    key: &'a str,
    version: &'a str,
}

#[derive(Debug, Deserialize)]
struct CreateCacheEntryResponse {
    #[serde(default)]
    ok: bool,
    #[serde(rename = "signedUploadUrl")]
    signed_upload_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct FinalizeCacheEntryUploadRequest<'a> {
    key: &'a str,
    version: &'a str,
    #[serde(rename = "sizeBytes")]
    size_bytes: String, // sizeBytes is a string in the GHA v2 API
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn cache_key(kind: EntryKind, hash: &str) -> String {
    format!("{kind}-{hash}")
}

fn version_ns(client: &GhaClient) -> &str {
    &client.version_ns
}

async fn compress_reader_zstd(
    mut data: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<Vec<u8>, CacheError> {
    let mut raw = Vec::new();
    data.read_to_end(&mut raw)
        .await
        .map_err(|e| CacheError::Io(format!("read failed: {e}")))?;
    let cursor = std::io::Cursor::new(raw);
    let buf_reader = BufReader::new(cursor);
    let mut encoder = ZstdEncoder::new(buf_reader);
    let mut compressed = Vec::new();
    tokio::io::copy(&mut encoder, &mut compressed)
        .await
        .map_err(|e| CacheError::Io(format!("compression failed: {e}")))?;
    Ok(compressed)
}

/// POST to a Twirp JSON-RPC method with retry on 5xx.
async fn twirp_post<Req, Resp>(
    client: &GhaClient,
    method: &str,
    body: &Req,
) -> Result<Resp, CacheError>
where
    Req: Serialize,
    Resp: for<'de> Deserialize<'de>,
{
    let url = client.v2_url(method);
    let mut last_err = CacheError::BackendUnavailable("no attempts made".into());

    for attempt in 0u32..=3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * (1u64 << (attempt - 1)))).await;
        }
        let resp = client
            .http
            .post(&url)
            .header("Authorization", client.auth_header())
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await;

        match resp {
            Err(e) => {
                last_err = CacheError::BackendUnavailable(e.to_string());
                continue;
            }
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
                let parsed: Resp = r.json().await.map_err(|e| {
                    CacheError::Internal(format!("failed to parse response: {e}"))
                })?;
                return Ok(parsed);
            }
        }
    }
    Err(last_err)
}

/// Upload compressed data to Azure Blob Storage via the signed upload URL.
/// Uses single PUT for ≤256 MiB, block upload for larger.
async fn azure_upload(
    client: &GhaClient,
    signed_upload_url: &str,
    data: &[u8],
) -> Result<(), CacheError> {
    let total = data.len();

    if total <= SINGLE_PUT_THRESHOLD {
        // Single-block PUT
        let resp = client
            .http
            .put(signed_upload_url)
            .header("x-ms-blob-type", "BlockBlob")
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", total.to_string())
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(CacheError::Internal(format!(
                "Azure single PUT failed: {}",
                resp.status()
            )));
        }
        return Ok(());
    }

    // Block upload for large blobs
    let mut block_ids: Vec<String> = Vec::new();
    let mut offset = 0usize;
    let mut block_index: u32 = 0;

    while offset < total {
        let end = (offset + BLOCK_SIZE).min(total);
        let chunk = &data[offset..end];

        // blockid is base64 of zero-padded decimal index
        let block_id_bytes = format!("{block_index:032}");
        let block_id = base64::engine::general_purpose::STANDARD.encode(block_id_bytes.as_bytes());
        block_ids.push(block_id.clone());

        let block_url = format!("{signed_upload_url}&comp=block&blockid={block_id}");

        let resp = client
            .http
            .put(&block_url)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", chunk.len().to_string())
            .body(chunk.to_vec())
            .send()
            .await
            .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(CacheError::Internal(format!(
                "Azure block PUT failed at offset {offset}: {}",
                resp.status()
            )));
        }

        offset = end;
        block_index += 1;
    }

    // Commit block list
    let block_list_items: String = block_ids
        .iter()
        .map(|id| format!("<Latest>{id}</Latest>"))
        .collect::<Vec<_>>()
        .join("");
    let xml_body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>{block_list_items}</BlockList>"
    );

    let blocklist_url = format!("{signed_upload_url}&comp=blocklist");
    let resp = client
        .http
        .put(&blocklist_url)
        .header("Content-Type", "application/xml")
        .body(xml_body)
        .send()
        .await
        .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(CacheError::Internal(format!(
            "Azure block list commit failed: {}",
            resp.status()
        )));
    }

    Ok(())
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Returns `Ok(Some(-1))` when present, `Ok(None)` on miss.
pub async fn contains_v2(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<i64>, CacheError> {
    let key = cache_key(kind, hash);
    let ns = version_ns(client).to_owned();
    let req = GetCacheEntryDownloadURLRequest {
        key: &key,
        version: &ns,
        restore_keys: vec![],
    };
    let resp: GetCacheEntryDownloadURLResponse =
        twirp_post(client, "GetCacheEntryDownloadURL", &req).await?;
    if resp.ok && resp.signed_download_url.is_some() {
        Ok(Some(-1))
    } else {
        Ok(None)
    }
}

/// Returns a streaming reader for the entry, or `None` if not found.
pub async fn get_v2(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
    let key = cache_key(kind, hash);
    let ns = version_ns(client).to_owned();
    let req = GetCacheEntryDownloadURLRequest {
        key: &key,
        version: &ns,
        restore_keys: vec![],
    };
    let resp: GetCacheEntryDownloadURLResponse =
        twirp_post(client, "GetCacheEntryDownloadURL", &req).await?;

    let download_url = match resp {
        GetCacheEntryDownloadURLResponse { ok: true, signed_download_url: Some(url) } => url,
        _ => return Ok(None),
    };

    let dl_resp = client
        .http
        .get(&download_url)
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

/// Uploads `data` to the GHA v2 cache under the given key.
/// Performs: CreateCacheEntry → compress → azure upload → FinalizeCacheEntryUpload.
pub async fn put_v2(
    client: &GhaClient,
    kind: EntryKind,
    hash: &str,
    _logical_size: i64,
    data: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<(), CacheError> {
    let key = cache_key(kind, hash);
    let ns = version_ns(client).to_owned();

    // Step 1: reserve a cache entry slot
    let create_req = CreateCacheEntryRequest {
        key: &key,
        version: &ns,
    };
    let create_resp: CreateCacheEntryResponse =
        twirp_post(client, "CreateCacheEntry", &create_req).await?;

    let signed_upload_url = match create_resp {
        CreateCacheEntryResponse { ok: false, .. } => {
            // Conflict / already cached — treat as success
            return Ok(());
        }
        CreateCacheEntryResponse { signed_upload_url: Some(url), .. } => url,
        CreateCacheEntryResponse { signed_upload_url: None, .. } => {
            return Err(CacheError::Internal(
                "CreateCacheEntry returned ok=true but no signedUploadUrl".into(),
            ));
        }
    };

    // Step 2: compress
    let compressed = compress_reader_zstd(data).await?;
    let compressed_size = compressed.len() as i64;

    // Step 3: upload to Azure Blob Storage
    azure_upload(client, &signed_upload_url, &compressed).await?;

    // Step 4: finalize
    let finalize_req = FinalizeCacheEntryUploadRequest {
        key: &key,
        version: &ns,
        size_bytes: compressed_size.to_string(),
    };
    let _: serde_json::Value =
        twirp_post(client, "FinalizeCacheEntryUpload", &finalize_req).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::client::{ApiVersion, GhaClient};

    use super::*;

    fn make_client(base_url: String) -> GhaClient {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        GhaClient {
            http,
            base_url_v1: String::new(),
            base_url_v2: Some(base_url.clone()),
            token: "test-token".into(),
            version: ApiVersion::V2,
            version_ns: "test-ns".into(),
        }
    }

    #[tokio::test]
    async fn contains_v2_hit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "signedDownloadUrl": "https://example.blob.core.windows.net/blob",
                "matchedKey": "cas-abc123"
            })))
            .mount(&server)
            .await;

        let client = make_client(server.uri() + "/");
        let result = contains_v2(&client, EntryKind::CAS, "abc123").await.unwrap();
        assert_eq!(result, Some(-1));
    }

    #[tokio::test]
    async fn contains_v2_miss() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": false
            })))
            .mount(&server)
            .await;

        let client = make_client(server.uri() + "/");
        let result = contains_v2(&client, EntryKind::CAS, "abc123").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn put_v2_conflict_treated_as_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": false
            })))
            .mount(&server)
            .await;

        let client = make_client(server.uri() + "/");
        let data: Box<dyn AsyncRead + Send + Unpin> = Box::new(std::io::Cursor::new(b"hello"));
        let result = put_v2(&client, EntryKind::CAS, "abc123", 5, data).await;
        assert!(result.is_ok());
    }
}
