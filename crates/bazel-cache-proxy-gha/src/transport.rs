//! Whole-entry save/restore against the GitHub Actions cache service.

use std::time::Duration;

use async_trait::async_trait;
use bazel_cache_proxy_core::error::CacheError;
use serde::{Deserialize, Serialize};

use crate::client::GhaClient;

pub struct RestoredEntry {
    pub matched_key: String,
    pub data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SaveOutcome {
    Saved,
    /// The key already exists (entries are immutable); nothing was written.
    AlreadyExists,
}

/// Immutable key/value entries with exact-key-or-newest-prefix lookup, as the GitHub
/// Actions cache offers. Abstracted so the pack logic is testable without the service.
#[async_trait]
pub trait CacheTransport: Send + Sync + 'static {
    /// Looks up `key`, then each prefix in `restore_prefixes` (newest match wins).
    async fn restore(
        &self,
        key: &str,
        restore_prefixes: &[String],
    ) -> Result<Option<RestoredEntry>, CacheError>;

    async fn save(&self, key: &str, data: Vec<u8>) -> Result<SaveOutcome, CacheError>;
}

pub struct GhaTransport {
    client: GhaClient,
}

impl GhaTransport {
    pub fn new(client: GhaClient) -> Self {
        Self { client }
    }
}

#[derive(Serialize)]
struct DownloadReq<'a> {
    key: &'a str,
    version: &'a str,
    #[serde(rename = "restoreKeys")]
    restore_keys: &'a [String],
}

#[derive(Deserialize)]
struct DownloadResp {
    #[serde(default)]
    ok: bool,
    #[serde(rename = "signedDownloadUrl", default)]
    signed_download_url: String,
    #[serde(rename = "matchedKey", default)]
    matched_key: String,
}

#[derive(Serialize)]
struct CreateReq<'a> {
    key: &'a str,
    version: &'a str,
}

#[derive(Deserialize)]
struct CreateResp {
    #[serde(default)]
    ok: bool,
    #[serde(rename = "signedUploadUrl", default)]
    signed_upload_url: String,
    #[serde(default)]
    message: String,
}

#[derive(Serialize)]
struct FinalizeReq<'a> {
    key: &'a str,
    version: &'a str,
    #[serde(rename = "sizeBytes")]
    size_bytes: String,
}

#[derive(Deserialize)]
struct FinalizeResp {
    #[serde(default)]
    ok: bool,
}

/// Signed storage URLs carry SAS signatures; never let them reach logs via reqwest's Display.
fn scrub(e: reqwest::Error) -> String {
    e.without_url().to_string()
}

async fn twirp<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
    client: &GhaClient,
    method: &str,
    body: &Req,
) -> Result<Resp, CacheError> {
    let mut last = CacheError::BackendUnavailable("no attempts made".into());
    for attempt in 0u32..4 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(200 * (1u64 << (attempt - 1)))).await;
        }
        let sent = client
            .http
            .post(client.twirp_url(method))
            .header("Authorization", client.auth_header())
            .json(body)
            .send()
            .await;
        match sent {
            Err(e) => last = CacheError::BackendUnavailable(scrub(e)),
            Ok(r) if r.status().is_server_error() || r.status().as_u16() == 429 => {
                last = CacheError::BackendUnavailable(format!("{method}: HTTP {}", r.status()));
            }
            Ok(r) if !r.status().is_success() => {
                return Err(CacheError::Internal(format!(
                    "{method}: HTTP {}",
                    r.status()
                )));
            }
            Ok(r) => {
                return r.json().await.map_err(|e| {
                    CacheError::Internal(format!("{method}: bad response: {}", scrub(e)))
                });
            }
        }
    }
    Err(last)
}

#[async_trait]
impl CacheTransport for GhaTransport {
    async fn restore(
        &self,
        key: &str,
        restore_prefixes: &[String],
    ) -> Result<Option<RestoredEntry>, CacheError> {
        let resp: DownloadResp = twirp(
            &self.client,
            "GetCacheEntryDownloadURL",
            &DownloadReq {
                key,
                version: &self.client.version,
                restore_keys: restore_prefixes,
            },
        )
        .await?;
        if !resp.ok || resp.signed_download_url.is_empty() {
            return Ok(None);
        }
        let r = self
            .client
            .http
            .get(&resp.signed_download_url)
            .send()
            .await
            .map_err(|e| CacheError::BackendUnavailable(scrub(e)))?;
        if !r.status().is_success() {
            return Err(CacheError::BackendUnavailable(format!(
                "download: HTTP {}",
                r.status()
            )));
        }
        let data = r
            .bytes()
            .await
            .map_err(|e| CacheError::BackendUnavailable(scrub(e)))?;
        let matched_key = if resp.matched_key.is_empty() {
            key.to_string()
        } else {
            resp.matched_key
        };
        Ok(Some(RestoredEntry {
            matched_key,
            data: data.to_vec(),
        }))
    }

    async fn save(&self, key: &str, data: Vec<u8>) -> Result<SaveOutcome, CacheError> {
        let created: CreateResp = twirp(
            &self.client,
            "CreateCacheEntry",
            &CreateReq {
                key,
                version: &self.client.version,
            },
        )
        .await?;
        if !created.ok || created.signed_upload_url.is_empty() {
            tracing::warn!(%key, message = %created.message, ok = created.ok, "CreateCacheEntry declined");
            return Ok(SaveOutcome::AlreadyExists);
        }
        let size = data.len();
        let put = self
            .client
            .http
            .put(&created.signed_upload_url)
            .header("x-ms-blob-type", "BlockBlob")
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await
            .map_err(|e| CacheError::BackendUnavailable(scrub(e)))?;
        if !put.status().is_success() {
            return Err(CacheError::BackendUnavailable(format!(
                "blob upload: HTTP {}",
                put.status()
            )));
        }
        let fin: FinalizeResp = twirp(
            &self.client,
            "FinalizeCacheEntryUpload",
            &FinalizeReq {
                key,
                version: &self.client.version,
                size_bytes: size.to_string(),
            },
        )
        .await?;
        if !fin.ok {
            return Err(CacheError::Internal(
                "FinalizeCacheEntryUpload returned ok=false".into(),
            ));
        }
        Ok(SaveOutcome::Saved)
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    const SVC: &str = "/twirp/github.actions.results.api.v1.CacheService";

    fn transport(server: &MockServer) -> GhaTransport {
        GhaTransport::new(GhaClient::new(server.uri(), "tok".into(), "v").unwrap())
    }

    #[tokio::test]
    async fn restore_miss_returns_none() {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{SVC}/GetCacheEntryDownloadURL")))
            .and(header("authorization", "Bearer tok"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": false})),
            )
            .mount(&s)
            .await;
        assert!(transport(&s)
            .restore("k", &["p-".into()])
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn restore_hit_downloads_and_reports_matched_key() {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{SVC}/GetCacheEntryDownloadURL")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "signedDownloadUrl": format!("{}/blob", s.uri()),
                "matchedKey": "p-123"
            })))
            .mount(&s)
            .await;
        Mock::given(method("GET"))
            .and(path("/blob"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload".to_vec()))
            .mount(&s)
            .await;
        let got = transport(&s)
            .restore("k", &["p-".into()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.matched_key, "p-123");
        assert_eq!(got.data, b"payload");
    }

    #[tokio::test]
    async fn save_runs_create_upload_finalize() {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{SVC}/CreateCacheEntry")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true, "signedUploadUrl": format!("{}/up", s.uri())
            })))
            .expect(1)
            .mount(&s)
            .await;
        Mock::given(method("PUT"))
            .and(path("/up"))
            .and(header("x-ms-blob-type", "BlockBlob"))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&s)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("{SVC}/FinalizeCacheEntryUpload")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .expect(1)
            .mount(&s)
            .await;
        assert_eq!(
            transport(&s).save("k", vec![1, 2, 3]).await.unwrap(),
            SaveOutcome::Saved
        );
    }

    #[tokio::test]
    async fn save_conflict_is_already_exists() {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("{SVC}/CreateCacheEntry")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": false})),
            )
            .mount(&s)
            .await;
        assert_eq!(
            transport(&s).save("k", vec![1]).await.unwrap(),
            SaveOutcome::AlreadyExists
        );
    }

    #[tokio::test]
    async fn auth_failure_is_not_retried_and_not_leaky() {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&s)
            .await;
        let err = transport(&s).restore("k", &[]).await.err().unwrap();
        assert!(!err.to_string().contains("tok"));
    }
}
