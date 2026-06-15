use reqwest::Client;
use bazel_cache_proxy_core::error::CacheError;

#[derive(Debug, Clone, PartialEq)]
pub enum ApiVersion {
    V1,
    V2,
}

pub struct GhaClient {
    pub http: Client,
    pub base_url_v1: String,
    pub base_url_v2: Option<String>,
    pub token: String,
    pub version: ApiVersion,
}

impl GhaClient {
    pub fn from_env() -> Result<Self, CacheError> {
        let token = std::env::var("ACTIONS_RUNTIME_TOKEN")
            .map_err(|_| CacheError::Configuration("ACTIONS_RUNTIME_TOKEN not set".into()))?;
        let base_url_v1 = std::env::var("ACTIONS_CACHE_URL").unwrap_or_default();
        let base_url_v2 = std::env::var("ACTIONS_RESULTS_URL").ok();
        let version = detect_version(&base_url_v2);
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| CacheError::Internal(e.to_string()))?;
        Ok(Self { http, base_url_v1, base_url_v2, token, version })
    }

    pub fn auth_header(&self) -> String {
        format!("Bearer {}", self.token)
    }

    pub fn v1_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url_v1.trim_end_matches('/'), path)
    }
}

pub fn detect_version(base_url_v2: &Option<String>) -> ApiVersion {
    let has_v2_service = std::env::var("ACTIONS_CACHE_SERVICE_V2")
        .map(|v| v == "true")
        .unwrap_or(false);
    let has_v2_url = base_url_v2.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
    if has_v2_service && has_v2_url {
        ApiVersion::V2
    } else {
        ApiVersion::V1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_version_v1_by_default() {
        let version = detect_version(&None);
        assert_eq!(version, ApiVersion::V1);
    }

    #[test]
    fn detect_version_v1_when_url_absent() {
        let version = detect_version(&None);
        assert_eq!(version, ApiVersion::V1);
    }

    #[test]
    fn detect_version_v2_when_both_present() {
        std::env::set_var("ACTIONS_CACHE_SERVICE_V2", "true");
        let version = detect_version(&Some("https://results.github.com".into()));
        std::env::remove_var("ACTIONS_CACHE_SERVICE_V2");
        assert_eq!(version, ApiVersion::V2);
    }
}
