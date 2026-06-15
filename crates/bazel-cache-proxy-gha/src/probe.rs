// GHA v2 startup probe

use bazel_cache_proxy_core::error::CacheError;

/// Verify that the GHA v2 results service URL is configured and non-empty.
/// Returns `Ok(())` if available, `Err(CacheError::Configuration(...))` otherwise.
pub fn probe_v2_available() -> Result<(), CacheError> {
    let url = std::env::var("ACTIONS_RESULTS_URL").unwrap_or_default();
    if url.is_empty() {
        return Err(CacheError::Configuration(
            "ACTIONS_RESULTS_URL is not set or empty — GHA v2 cache unavailable".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_fails_when_url_absent() {
        std::env::remove_var("ACTIONS_RESULTS_URL");
        assert!(probe_v2_available().is_err());
    }
}
