//! GitHub Actions cache (v2 Twirp API) backend for bazel-cache-proxy.
//!
//! Blobs are served from a local disk cache. At startup the newest *manifest* entry is
//! restored; its *packs* (tar+zstd bundles of blobs) are unpacked into the disk cache. On
//! flush, blobs written since startup are bundled into one or more new packs plus a new
//! manifest. This keeps the number of GitHub cache entries per run small (the service
//! allows 200 uploads/minute/repository and entries are immutable).

pub mod client;
pub mod pack;
pub mod transport;

pub use client::GhaClient;
pub use pack::{PackBackend, PackConfig};
pub use transport::{CacheTransport, GhaTransport, RestoredEntry, SaveOutcome};
