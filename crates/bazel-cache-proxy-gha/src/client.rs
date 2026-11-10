use bazel_cache_proxy_core::error::CacheError;
use reqwest::Client;
use sha2::{Digest, Sha256};

/// Credentials and endpoint for the GitHub Actions cache service (v2, Twirp).
///
/// `ACTIONS_RESULTS_URL` and `ACTIONS_RUNTIME_TOKEN` are only injected into JavaScript/Docker
/// actions, not into `run:` steps; see the README for how to export them.
pub struct GhaClient {
    pub(crate) http: Client,
    results_url: String,
    token: String,
    /// Cache "version" sent with every key; entries with another version never match.
    pub(crate) version: String,
}

// Manual Debug so the runtime token can never leak through `{:?}`.
impl std::fmt::Debug for GhaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhaClient")
            .field("results_url", &self.results_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl GhaClient {
    pub fn from_env(version_salt: &str) -> Result<Self, CacheError> {
        let results_url = non_empty_env("ACTIONS_RESULTS_URL")?;
        let token = non_empty_env("ACTIONS_RUNTIME_TOKEN")?;
        Self::new(results_url, token, version_salt)
    }

    pub fn new(results_url: String, token: String, version_salt: &str) -> Result<Self, CacheError> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .map_err(|e| CacheError::Internal(e.to_string()))?;
        let version = format!("{:x}", Sha256::digest(version_salt.as_bytes()));
        Ok(Self {
            http,
            results_url,
            token,
            version,
        })
    }

    pub(crate) fn auth_header(&self) -> String {
        format!("Bearer {}", self.token)
    }

    pub(crate) fn twirp_url(&self, method: &str) -> String {
        format!(
            "{}/twirp/github.actions.results.api.v1.CacheService/{method}",
            self.results_url.trim_end_matches('/')
        )
    }
}

fn non_empty_env(name: &str) -> Result<String, CacheError> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(CacheError::Configuration(format!(
            "{name} is not set; the GHA cache backend only works inside a GitHub Actions \
             job that exports the runtime credentials"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_token() {
        let c = GhaClient::new("https://r.example".into(), "s3cret-token".into(), "v1").unwrap();
        assert!(!format!("{c:?}").contains("s3cret-token"));
    }

    #[test]
    fn twirp_url_trims_trailing_slash() {
        let c = GhaClient::new("https://r.example/".into(), "t".into(), "v1").unwrap();
        assert_eq!(
            c.twirp_url("CreateCacheEntry"),
            "https://r.example/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"
        );
    }

    #[test]
    fn version_is_stable_hex_digest() {
        let a = GhaClient::new("u".into(), "t".into(), "salt").unwrap();
        let b = GhaClient::new("u".into(), "t".into(), "salt").unwrap();
        assert_eq!(a.version, b.version);
        assert_eq!(a.version.len(), 64);
    }
}
