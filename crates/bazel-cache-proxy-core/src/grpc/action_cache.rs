use std::sync::Arc;
use prost::Message;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use crate::{
    backend::StorageBackend,
    entry_kind::EntryKind,
    error::CacheError,
    proto::reapi::{
        action_cache_server::ActionCache, ActionResult, GetActionResultRequest,
        UpdateActionResultRequest,
    },
};

pub struct ActionCacheService {
    backend: Arc<dyn StorageBackend>,
}

impl ActionCacheService {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }
}

#[tonic::async_trait]
impl ActionCache for ActionCacheService {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        let digest = req
            .action_digest
            .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;

        match self.backend.get(EntryKind::AC, &digest.hash, digest.size_bytes).await {
            Ok(Some(mut reader)) => {
                let mut buf = Vec::new();
                reader
                    .read_to_end(&mut buf)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                let result = ActionResult::decode(buf.as_slice())
                    .map_err(|e| Status::internal(format!("decode failed: {e}")))?;
                Ok(Response::new(result))
            }
            Ok(None) => Err(Status::not_found("action result not found")),
            Err(e) => Err(cache_error_to_status(e)),
        }
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        let digest = req
            .action_digest
            .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;
        let result = req
            .action_result
            .ok_or_else(|| Status::invalid_argument("missing action_result"))?;

        let mut buf = Vec::new();
        result.encode(&mut buf).map_err(|e| Status::internal(e.to_string()))?;
        let size = buf.len() as i64;
        let reader = std::io::Cursor::new(buf);

        self.backend
            .put(EntryKind::AC, &digest.hash, size, Box::new(reader))
            .await
            .map_err(cache_error_to_status)?;

        match self.backend.get(EntryKind::AC, &digest.hash, size).await {
            Ok(Some(mut reader)) => {
                let mut buf = Vec::new();
                reader
                    .read_to_end(&mut buf)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                let result = ActionResult::decode(buf.as_slice())
                    .map_err(|e| Status::internal(format!("decode failed: {e}")))?;
                Ok(Response::new(result))
            }
            Ok(None) => Err(Status::internal("stored but not retrievable")),
            Err(e) => Err(cache_error_to_status(e)),
        }
    }
}

pub fn cache_error_to_status(e: CacheError) -> Status {
    match e {
        CacheError::NotFound => Status::not_found("not found"),
        CacheError::BackendUnavailable(msg) => Status::unavailable(msg),
        CacheError::HashMismatch { .. }
        | CacheError::InvalidDigest(_)
        | CacheError::InvalidArgument(_) => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tonic::Code;
    use crate::{
        digest::EMPTY_SHA256,
        proto::reapi::{ActionResult, Digest},
        testing::InMemoryBackend,
    };

    fn make_digest(hash: &str, size: i64) -> Digest {
        Digest { hash: hash.to_string(), size_bytes: size }
    }

    #[tokio::test]
    async fn action_cache_round_trip() {
        let backend = Arc::new(InMemoryBackend::new());
        let svc = ActionCacheService::new(backend);

        let action_digest = make_digest(&"a".repeat(64), 0);
        let result_in = ActionResult { exit_code: 42, ..Default::default() };

        let update_resp = svc
            .update_action_result(Request::new(UpdateActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(action_digest.clone()),
                action_result: Some(result_in.clone()),
            }))
            .await
            .unwrap();
        assert_eq!(update_resp.into_inner().exit_code, 42);

        let get_resp = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(action_digest),
            }))
            .await
            .unwrap();
        assert_eq!(get_resp.into_inner().exit_code, 42);
    }

    #[tokio::test]
    async fn action_cache_miss_returns_not_found() {
        let backend = Arc::new(InMemoryBackend::new());
        let svc = ActionCacheService::new(backend);

        let err = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(make_digest(&"b".repeat(64), 0)),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }
}
