use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    ObjectStore,
    WriteMultipart,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
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

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GcsConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub credentials_path: Option<PathBuf>,
}

pub struct GcsBackend {
    store: Arc<GoogleCloudStorage>,
    prefix: Option<String>,
}

impl GcsBackend {
    pub fn new(config: GcsConfig) -> Result<Self, CacheError> {
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(&config.bucket);

        if let Some(creds) = &config.credentials_path {
            builder = builder.with_service_account_path(
                creds.to_str().ok_or_else(|| CacheError::Configuration("invalid credentials path".into()))?
            );
        }

        let store = builder.build()
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
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn contains(&self, kind: EntryKind, hash: &str, _size: i64) -> Result<Option<i64>, CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(meta.size as i64)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }

    async fn get(&self, kind: EntryKind, hash: &str, _size: i64) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.get(&path).await {
            Ok(result) => {
                let stream = result.into_stream();
                let stream = stream.map(|r| r.map_err(|e| {
                    std::io::Error::other(e.to_string())
                }));
                let reader = StreamReader::new(stream);
                Ok(Some(Box::new(reader)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }

    async fn put(&self, kind: EntryKind, hash: &str, logical_size: i64, mut data: Box<dyn AsyncRead + Send + Unpin>) -> Result<(), CacheError> {
        let path = self.object_path(kind, hash);

        const MULTIPART_THRESHOLD: i64 = 100 * 1024 * 1024; // 100 MiB

        if logical_size > MULTIPART_THRESHOLD {
            let upload = self.store.put_multipart(&path).await
                .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;
            let mut writer = WriteMultipart::new(upload);

            let mut buf = vec![0u8; 8 * 1024 * 1024]; // 8 MiB chunks
            loop {
                let n = data.read(&mut buf).await
                    .map_err(|e| CacheError::Io(e.to_string()))?;
                if n == 0 { break; }
                writer.write(&buf[..n]);
            }
            writer.finish().await
                .map_err(|e| CacheError::Internal(e.to_string()))?;
        } else {
            let mut all_bytes = Vec::new();
            data.read_to_end(&mut all_bytes).await
                .map_err(|e| CacheError::Io(e.to_string()))?;

            let payload = object_store::PutPayload::from(Bytes::from(all_bytes));
            self.store.put(&path, payload).await
                .map_err(|e| CacheError::BackendUnavailable(e.to_string()))?;
        }

        Ok(())
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        let path = self.object_path(kind, hash);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(CacheError::BackendUnavailable(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bazel_cache_proxy_core::entry_kind::EntryKind;

    #[test]
    fn gcs_config_prefix_in_object_path() {
        let prefix = Some("my/prefix".to_string());
        let kind = EntryKind::CAS;
        let hash = "a".repeat(64);
        let path = match &prefix {
            Some(p) => ObjPath::from(format!("{p}/{kind}/{hash}")),
            None => ObjPath::from(format!("{kind}/{hash}")),
        };
        assert_eq!(path.as_ref(), "my/prefix/cas/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn gcs_config_no_prefix_in_object_path() {
        let prefix: Option<String> = None;
        let kind = EntryKind::AC;
        let hash = "b".repeat(64);
        let path = match &prefix {
            Some(p) => ObjPath::from(format!("{p}/{kind}/{hash}")),
            None => ObjPath::from(format!("{kind}/{hash}")),
        };
        assert_eq!(path.as_ref(), "ac/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    }
}
