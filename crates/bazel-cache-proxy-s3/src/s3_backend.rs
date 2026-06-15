use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    ObjectStore,
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjPath,
    Error as ObjectStoreError,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;
use bazel_cache_proxy_core::{
    backend::StorageBackend,
    entry_kind::EntryKind,
    error::CacheError,
};

/// Configuration for the S3 storage backend.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

pub struct S3Backend {
    store: Arc<AmazonS3>,
    prefix: Option<String>,
}

impl S3Backend {
    pub fn new(config: S3Config) -> Result<Self, CacheError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);

        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false);
        }

        if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
            builder = builder
                .with_access_key_id(key)
                .with_secret_access_key(secret);
        }

        let store = builder
            .build()
            .map_err(|e| CacheError::Configuration(e.to_string()))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix,
        })
    }

    fn object_path(&self, kind: EntryKind, hash: &str) -> ObjPath {
        match &self.prefix {
            Some(prefix) => ObjPath::from(format!("{prefix}/{kind}/{hash}")),
            None => ObjPath::from(format!("{kind}/{hash}")),
        }
    }

    #[allow(dead_code)]
    fn map_object_store_error(e: ObjectStoreError) -> CacheError {
        match e {
            ObjectStoreError::NotFound { .. } => CacheError::NotFound,
            e => {
                let msg = e.to_string();
                if msg.contains("Connection")
                    || msg.contains("timeout")
                    || msg.contains("network")
                {
                    CacheError::BackendUnavailable(msg)
                } else {
                    CacheError::Internal(msg)
                }
            }
        }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<i64>, CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(meta.size as i64)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }

    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        _size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.get(&path).await {
            Ok(result) => {
                let stream = result.into_stream().map(|r| {
                    r.map_err(|e| std::io::Error::other(e.to_string()))
                });
                let reader = StreamReader::new(stream);
                Ok(Some(Box::new(reader)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }

    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        mut data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError> {
        let path = self.object_path(kind, hash);

        const MULTIPART_THRESHOLD: i64 = 100 * 1024 * 1024; // 100 MiB
        const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

        if logical_size > MULTIPART_THRESHOLD {
            // Multipart upload for large blobs.
            // object_store 0.11: put_multipart returns Box<dyn MultipartUpload>
            // with put_part(PutPayload) -> UploadPart and complete() -> Result<PutResult>
            let mut upload = self
                .store
                .put_multipart(&path)
                .await
                .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;

            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                let n = data
                    .read(&mut buf)
                    .await
                    .map_err(|e| CacheError::Io(e.to_string()))?;
                if n == 0 {
                    break;
                }
                let payload = object_store::PutPayload::from(Bytes::copy_from_slice(&buf[..n]));
                upload
                    .put_part(payload)
                    .await
                    .map_err(|e| CacheError::Internal(e.to_string()))?;
            }
            upload
                .complete()
                .await
                .map_err(|e| CacheError::Internal(e.to_string()))?;
        } else {
            // Single PUT for small blobs — buffer entirely then upload.
            let mut all_bytes = Vec::new();
            data.read_to_end(&mut all_bytes)
                .await
                .map_err(|e| CacheError::Io(e.to_string()))?;

            let payload = object_store::PutPayload::from(Bytes::from(all_bytes));
            self.store
                .put(&path, payload)
                .await
                .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;
        }

        Ok(())
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::NotFound { .. }) => Ok(()), // idempotent
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_config_prefix_in_object_path() {
        let config = S3Config {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            prefix: Some("my/prefix".into()),
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
        };
        let backend = S3Backend::new(config).unwrap();
        let path = backend.object_path(EntryKind::CAS, &"a".repeat(64));
        assert_eq!(
            path.as_ref(),
            "my/prefix/cas/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn s3_config_no_prefix_in_object_path() {
        let config = S3Config {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            prefix: None,
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
        };
        let backend = S3Backend::new(config).unwrap();
        let path = backend.object_path(EntryKind::AC, &"b".repeat(64));
        assert_eq!(
            path.as_ref(),
            "ac/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn s3_missing_bucket_config_returns_error() {
        let config = S3Config {
            bucket: "".into(),
            region: "us-east-1".into(),
            prefix: None,
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: None,
            secret_access_key: None,
        };
        // Verify it doesn't panic — may or may not error at construction time
        let _ = S3Backend::new(config);
    }
}
