use std::sync::Arc;
use bazel_cache_proxy_core::{backend::StorageBackend, LayeredBackend, NoopBackend};
use crate::config::BackendConfig;

pub fn build_backend(cfg: &BackendConfig) -> std::pin::Pin<Box<dyn std::future::Future<Output = Arc<dyn StorageBackend>> + Send + '_>> {
    Box::pin(async move {
        match cfg {
            BackendConfig::Disk(c) => {
                use bazel_cache_proxy_disk::DiskBackend;
                let backend = DiskBackend::new(c.root.clone(), c.max_size_bytes.as_u64())
                    .await
                    .expect("failed to create disk backend");
                Arc::new(backend) as Arc<dyn StorageBackend>
            }
            BackendConfig::S3(c) => {
                use bazel_cache_proxy_s3::{S3Backend, S3Config};
                let config = S3Config {
                    bucket: c.bucket.clone(),
                    region: c.region.clone(),
                    prefix: c.prefix.clone(),
                    endpoint: c.endpoint.clone(),
                    access_key_id: c.access_key_id.clone(),
                    secret_access_key: c.secret_access_key.clone(),
                };
                let backend = S3Backend::new(config).expect("failed to create S3 backend");
                Arc::new(backend) as Arc<dyn StorageBackend>
            }
            BackendConfig::Gcs(c) => {
                use bazel_cache_proxy_gcs::{GcsBackend, GcsConfig};
                let config = GcsConfig {
                    bucket: c.bucket.clone(),
                    prefix: c.prefix.clone(),
                    credentials_path: c.credentials_path.clone(),
                };
                let backend = GcsBackend::new(config).expect("failed to create GCS backend");
                Arc::new(backend) as Arc<dyn StorageBackend>
            }
            BackendConfig::Gha(_) => {
                use bazel_cache_proxy_gha::GhaBackend;
                match GhaBackend::from_env() {
                    Ok(backend) => Arc::new(backend) as Arc<dyn StorageBackend>,
                    Err(e) => {
                        tracing::warn!(
                            "GHA backend unavailable: {e}; check that ACTIONS_CACHE_URL and \
                             ACTIONS_RUNTIME_TOKEN env vars are set. Running in no-op mode \
                             (cache misses only — builds will not be accelerated)."
                        );
                        Arc::new(NoopBackend) as Arc<dyn StorageBackend>
                    }
                }
            }
            BackendConfig::Sqlite(c) => {
                use bazel_cache_proxy_sqlite::SqliteBackend;
                let backend = SqliteBackend::new(c.path.clone(), c.max_size_bytes.map(|b| b.as_u64()))
                    .await
                    .expect("failed to create SQLite backend");
                Arc::new(backend) as Arc<dyn StorageBackend>
            }
            BackendConfig::Layered(c) => {
                let l1 = build_backend(&c.primary).await;
                let l2 = build_backend(&c.fallback).await;
                Arc::new(LayeredBackend::new(l1, l2)) as Arc<dyn StorageBackend>
            }
        }
    })
}
