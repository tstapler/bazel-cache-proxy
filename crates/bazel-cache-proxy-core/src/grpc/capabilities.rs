use tonic::{Request, Response, Status};
use crate::proto::reapi::{
    capabilities_server::Capabilities,
    ActionCacheUpdateCapabilities, CacheCapabilities, GetCapabilitiesRequest, ServerCapabilities,
};

/// SHA256 = 1, matching DigestFunction.Value in the real REAPI proto.
pub const DIGEST_FUNCTION_SHA256: i32 = 1;

pub struct CapabilitiesService;

#[tonic::async_trait]
impl Capabilities for CapabilitiesService {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        use crate::proto::semver::SemVer;
        let caps = ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![DIGEST_FUNCTION_SHA256],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                max_batch_total_size_bytes: 4 * 1024 * 1024,
                symlink_absolute_path_strategy: false,
            }),
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: String::new(),
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: String::new(),
            }),
        };
        Ok(Response::new(caps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Request;

    #[tokio::test]
    async fn capabilities_returns_sha256_and_api_version_2() {
        let svc = CapabilitiesService;
        let resp = svc
            .get_capabilities(Request::new(GetCapabilitiesRequest {
                instance_name: String::new(),
            }))
            .await
            .unwrap();
        let caps = resp.into_inner();
        let cache_caps = caps.cache_capabilities.unwrap();
        assert!(cache_caps.digest_functions.contains(&DIGEST_FUNCTION_SHA256));
        assert_eq!(caps.high_api_version.unwrap().major, 2);
    }
}
