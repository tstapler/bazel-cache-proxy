use std::sync::Arc;

use async_trait::async_trait;
use bazel_cache_proxy_core::{backend::StorageBackend, entry_kind::EntryKind, error::CacheError};
use tokio::io::AsyncRead;
use tracing::{debug, warn};

use crate::{
    client::{ApiVersion, GhaClient},
    rate_limiter::TokenBucket,
    v1::{contains_v1, get_v1, put_v1},
    v2::{contains_v2, get_v2, put_v2},
};

pub struct GhaBackend {
    client: GhaClient,
    rate_limiter: Arc<TokenBucket>,
}

impl GhaBackend {
    pub fn from_env() -> Result<Self, CacheError> {
        let client = GhaClient::from_env()?;
        let rate_limiter = TokenBucket::new_default();
        Ok(Self {
            client,
            rate_limiter,
        })
    }

    pub fn new(client: GhaClient, rate_limiter: Arc<TokenBucket>) -> Self {
        Self {
            client,
            rate_limiter,
        }
    }
}

#[async_trait]
impl StorageBackend for GhaBackend {
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<i64>, CacheError> {
        self.rate_limiter.acquire().await;
        let result = match self.client.version {
            ApiVersion::V1 => contains_v1(&self.client, kind, hash).await,
            ApiVersion::V2 => contains_v2(&self.client, kind, hash).await,
        };
        match result {
            Err(CacheError::BackendUnavailable(msg)) => {
                warn!("GHA cache unavailable for contains({kind}, {hash}): {msg}");
                Ok(None)
            }
            other => other,
        }
    }

    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        self.rate_limiter.acquire().await;
        let result = match self.client.version {
            ApiVersion::V1 => get_v1(&self.client, kind, hash).await,
            ApiVersion::V2 => get_v2(&self.client, kind, hash).await,
        };
        match result {
            Err(CacheError::BackendUnavailable(msg)) => {
                warn!("GHA cache unavailable for get({kind}, {hash}): {msg}");
                Ok(None)
            }
            other => other,
        }
    }

    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError> {
        self.rate_limiter.acquire().await;
        let result = match self.client.version {
            ApiVersion::V1 => put_v1(&self.client, kind, hash, logical_size, data).await,
            ApiVersion::V2 => put_v2(&self.client, kind, hash, logical_size, data).await,
        };
        match result {
            Err(CacheError::BackendUnavailable(msg)) => {
                warn!("GHA cache unavailable for put({kind}, {hash}): {msg}; skipping");
                Ok(())
            }
            other => other,
        }
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        // GHA cache API does not support deletion; treat as no-op.
        debug!("GHA cache does not support delete; ignoring delete({kind}, {hash})");
        Ok(())
    }
}
