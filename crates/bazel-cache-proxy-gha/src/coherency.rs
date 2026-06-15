// AC/CAS coherency verification — stub

use bazel_cache_proxy_core::error::CacheError;
use tracing::debug;

/// Verify coherency between AC and CAS entries.
/// Currently a stub that always reports coherent.
pub async fn verify_coherency(_ac_hash: &str, _cas_hash: &str) -> Result<bool, CacheError> {
    debug!("coherency verification is not yet implemented; assuming coherent");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn coherency_stub_returns_true() {
        let result = verify_coherency("abc123", "def456").await;
        assert!(result.unwrap());
    }
}
