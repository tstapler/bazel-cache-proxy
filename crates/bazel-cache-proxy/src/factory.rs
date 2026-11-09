use crate::config::BackendConfig;
use bazel_cache_proxy_core::{backend::StorageBackend, LayeredBackend};
use std::sync::Arc;

pub fn build_backend(
    cfg: &BackendConfig,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Arc<dyn StorageBackend>> + Send + '_>> {
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
            BackendConfig::Gha(c) => build_gha(c).await,
            BackendConfig::Sqlite(c) => {
                use bazel_cache_proxy_sqlite::SqliteBackend;
                let backend =
                    SqliteBackend::new(c.path.clone(), c.max_size_bytes.map(|b| b.as_u64()))
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

async fn build_gha(c: &crate::config::GhaConfig) -> Arc<dyn StorageBackend> {
    use bazel_cache_proxy_disk::DiskBackend;
    use bazel_cache_proxy_gha::{GhaClient, GhaTransport, PackBackend, PackConfig};

    let root = c.root.clone().unwrap_or_else(|| {
        std::path::PathBuf::from(std::env::var("RUNNER_TEMP").unwrap_or_else(|_| "/tmp".into()))
            .join("bazel-cache-proxy")
    });
    let upload = c.upload
        && !std::env::var("BAZEL_CACHE_PROXY_UPLOAD")
            .is_ok_and(|v| v.eq_ignore_ascii_case("false"));
    let cfg = PackConfig {
        prefix: c.prefix.clone(),
        root: root.clone(),
        max_local_bytes: c.max_local_bytes.as_u64(),
        max_pack_bytes: c.max_pack_bytes.as_u64(),
        max_packs: c.max_packs,
        upload,
        run_id: PackConfig::default_run_id(),
    };
    match GhaClient::from_env("bazel-cache-proxy/v1") {
        Ok(client) => {
            let transport = Arc::new(GhaTransport::new(client));
            let backend = PackBackend::open(cfg, transport)
                .await
                .expect("failed to open GHA pack backend");
            Arc::new(backend)
        }
        Err(e) => {
            tracing::warn!("{e}; serving a local-only disk cache (nothing will be persisted)");
            let backend = DiskBackend::new(root, c.max_local_bytes.as_u64())
                .await
                .expect("failed to create disk backend");
            Arc::new(backend)
        }
    }
}
