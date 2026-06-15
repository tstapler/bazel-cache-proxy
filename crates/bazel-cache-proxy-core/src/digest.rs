use std::fmt;
use std::str::FromStr;
use crate::error::CacheError;

/// SHA-256 digest identifying a cache entry.
/// Hash must be exactly 64 lowercase hex characters; size must be >= 0.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Digest {
    hash: String,
    size: i64,
}

/// SHA-256 of empty input.
pub const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

impl Digest {
    pub fn new(hash: impl Into<String>, size: i64) -> Result<Self, CacheError> {
        let hash = hash.into();
        if hash.len() != 64 {
            return Err(CacheError::InvalidDigest(format!(
                "hash must be 64 hex chars, got {}",
                hash.len()
            )));
        }
        if !hash.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
            return Err(CacheError::InvalidDigest(
                "hash must be lowercase hex".to_string(),
            ));
        }
        if size < 0 {
            return Err(CacheError::InvalidDigest(format!(
                "size must be >= 0, got {size}"
            )));
        }
        Ok(Self { hash, size })
    }

    /// Returns the empty-blob digest (SHA-256 of zero bytes).
    pub fn empty() -> Self {
        Self {
            hash: EMPTY_SHA256.to_string(),
            size: 0,
        }
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn is_empty_blob(&self) -> bool {
        self.hash == EMPTY_SHA256 && self.size == 0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.hash, self.size)
    }
}

impl FromStr for Digest {
    type Err = CacheError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hash, size_str) = s.split_once('/').ok_or_else(|| {
            CacheError::InvalidDigest(format!("expected 'hash/size', got {s:?}"))
        })?;
        let size = size_str.parse::<i64>().map_err(|_| {
            CacheError::InvalidDigest(format!("size is not a valid i64: {size_str:?}"))
        })?;
        Self::new(hash, size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_hash_string() -> String {
        "a".repeat(64)
    }

    #[test]
    fn digest_new_accepts_valid_64_hex_chars() {
        assert!(Digest::new(valid_hash_string(), 0).is_ok());
    }

    #[test]
    fn digest_new_rejects_63_hex_chars() {
        let short = "a".repeat(63);
        assert!(matches!(Digest::new(short, 0), Err(CacheError::InvalidDigest(_))));
    }

    #[test]
    fn digest_new_rejects_65_hex_chars() {
        let long = "a".repeat(65);
        assert!(matches!(Digest::new(long, 0), Err(CacheError::InvalidDigest(_))));
    }

    #[test]
    fn digest_new_rejects_non_hex_chars() {
        let bad = "g".repeat(64);
        assert!(matches!(Digest::new(bad, 0), Err(CacheError::InvalidDigest(_))));
    }

    #[test]
    fn digest_new_rejects_uppercase_hex() {
        let upper = "A".repeat(64);
        assert!(matches!(Digest::new(upper, 0), Err(CacheError::InvalidDigest(_))));
    }

    #[test]
    fn digest_new_accepts_zero_size() {
        assert!(Digest::new(EMPTY_SHA256, 0).is_ok());
    }

    #[test]
    fn digest_display_round_trips() {
        let d = Digest::new(valid_hash_string(), 42).unwrap();
        let s = d.to_string();
        let d2: Digest = s.parse().unwrap();
        assert_eq!(d, d2);
    }

    #[test]
    fn digest_eq_ignores_nothing() {
        let h = valid_hash_string();
        let d1 = Digest::new(h.clone(), 10).unwrap();
        let d2 = Digest::new(h, 20).unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn empty_digest_constant_is_correct() {
        let d = Digest::empty();
        assert_eq!(d.hash(), EMPTY_SHA256);
        assert_eq!(d.size(), 0);
        assert!(d.is_empty_blob());
    }
}
