// GHA v2 Twirp API implementation — stub

use bazel_cache_proxy_core::{entry_kind::EntryKind, error::CacheError};
use tokio::io::AsyncRead;
use tracing::warn;

use crate::client::GhaClient;

pub async fn contains_v2(
    _client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<i64>, CacheError> {
    warn!("GHA v2 cache not yet implemented; contains({kind}, {hash}) returning miss");
    Ok(None)
}

pub async fn get_v2(
    _client: &GhaClient,
    kind: EntryKind,
    hash: &str,
) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
    warn!("GHA v2 cache not yet implemented; get({kind}, {hash}) returning miss");
    Ok(None)
}

pub async fn put_v2(
    _client: &GhaClient,
    kind: EntryKind,
    hash: &str,
    _logical_size: i64,
    _data: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<(), CacheError> {
    warn!("GHA v2 cache not yet implemented; put({kind}, {hash}) is a no-op");
    Ok(())
}
