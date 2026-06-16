pub mod backend;
pub mod digest;
pub mod entry_kind;
pub mod error;
pub mod grpc;
pub mod hashing_writer;
pub mod http;
pub mod layered;
pub mod noop;
pub mod proto;
pub mod testing;

// Re-export `semver` at the crate root so that the prost-generated code inside
// `proto::reapi` can resolve `super::super::super::semver` (three levels up
// from inside the included file lands here at the crate root).
pub use proto::semver;

pub use backend::StorageBackend;
pub use digest::{Digest, EMPTY_SHA256};
pub use entry_kind::EntryKind;
pub use error::CacheError;
pub use hashing_writer::HashingWriter;
pub use layered::LayeredBackend;
pub use noop::NoopBackend;
