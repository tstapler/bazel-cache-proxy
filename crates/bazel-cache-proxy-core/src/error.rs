use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum CacheError {
    #[error("not found")]
    NotFound,

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: i64, actual: i64 },

    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("invalid digest: {0}")]
    InvalidDigest(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl CacheError {
    pub fn is_retriable(&self) -> bool {
        matches!(self, CacheError::BackendUnavailable(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_error_not_found_is_not_retriable() {
        assert!(!CacheError::NotFound.is_retriable());
    }

    #[test]
    fn cache_error_backend_unavailable_is_retriable() {
        assert!(CacheError::BackendUnavailable("test".into()).is_retriable());
    }

    #[test]
    fn cache_error_display_contains_context() {
        let err = CacheError::BackendUnavailable("connection refused".into());
        let s = format!("{err}");
        assert!(s.contains("connection refused"));
    }
}
