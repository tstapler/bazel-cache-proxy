use std::sync::Arc;
use futures::StreamExt;
use sha2::{Digest as Sha2Digest, Sha256};
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use crate::{
    backend::StorageBackend,
    digest::EMPTY_SHA256,
    entry_kind::EntryKind,
    proto::reapi::{
        batch_read_blobs_response, batch_update_blobs_response,
        content_addressable_storage_server::ContentAddressableStorage,
        BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
        BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse,
        GetTreeRequest, GetTreeResponse,
    },
};
use super::action_cache::cache_error_to_status;

pub const BATCH_READ_CONCURRENCY: usize = 32;

// gRPC canonical status codes
const OK: i32 = 0;
const NOT_FOUND: i32 = 5;
const INVALID_ARGUMENT: i32 = 3;
const INTERNAL: i32 = 13;

pub struct CasService {
    backend: Arc<dyn StorageBackend>,
}

impl CasService {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasService {
    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let req = request.into_inner();
        let mut missing = Vec::new();
        for d in req.blob_digests {
            if d.hash == EMPTY_SHA256 && d.size_bytes == 0 {
                continue;
            }
            match self.backend.contains(EntryKind::CAS, &d.hash, d.size_bytes).await {
                Ok(None) => missing.push(d),
                Ok(Some(_)) => {}
                Err(e) => return Err(cache_error_to_status(e)),
            }
        }
        Ok(Response::new(FindMissingBlobsResponse { missing_blob_digests: missing }))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let req = request.into_inner();
        let backend = self.backend.clone();

        let responses = futures::stream::iter(req.digests)
            .map(|d| {
                let backend = backend.clone();
                async move {
                    if d.hash == EMPTY_SHA256 && d.size_bytes == 0 {
                        return batch_read_blobs_response::Response {
                            digest: Some(d),
                            data: vec![],
                            status_code: OK,
                            status_message: String::new(),
                        };
                    }
                    match backend.get(EntryKind::CAS, &d.hash, d.size_bytes).await {
                        Ok(Some(mut reader)) => {
                            let mut buf = Vec::new();
                            match reader.read_to_end(&mut buf).await {
                                Ok(_) => batch_read_blobs_response::Response {
                                    digest: Some(d),
                                    data: buf,
                                    status_code: OK,
                                    status_message: String::new(),
                                },
                                Err(e) => batch_read_blobs_response::Response {
                                    digest: Some(d),
                                    data: vec![],
                                    status_code: INTERNAL,
                                    status_message: e.to_string(),
                                },
                            }
                        }
                        Ok(None) => batch_read_blobs_response::Response {
                            digest: Some(d),
                            data: vec![],
                            status_code: NOT_FOUND,
                            status_message: "not found".into(),
                        },
                        Err(e) => batch_read_blobs_response::Response {
                            digest: Some(d),
                            data: vec![],
                            status_code: INTERNAL,
                            status_message: e.to_string(),
                        },
                    }
                }
            })
            .buffer_unordered(BATCH_READ_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let req = request.into_inner();
        let mut responses = Vec::new();

        for blob in req.requests {
            let proto_digest = match blob.digest {
                Some(d) => d,
                None => {
                    responses.push(batch_update_blobs_response::Response {
                        digest: None,
                        status_code: INVALID_ARGUMENT,
                        status_message: "missing digest".into(),
                    });
                    continue;
                }
            };

            let data: Vec<u8> = blob.data;
            let actual_hash = format!("{:x}", Sha256::digest(&data));
            if actual_hash != proto_digest.hash {
                responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto_digest.clone()),
                    status_code: INVALID_ARGUMENT,
                    status_message: format!(
                        "hash mismatch: expected {}, got {actual_hash}",
                        proto_digest.hash
                    ),
                });
                continue;
            }

            let size = data.len() as i64;
            let expected_size = proto_digest.size_bytes;
            if size != expected_size {
                responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto_digest),
                    status_code: INVALID_ARGUMENT,
                    status_message: format!(
                        "size mismatch: expected {expected_size}, got {size}",
                    ),
                });
                continue;
            }

            let reader = std::io::Cursor::new(data);
            match self.backend.put(EntryKind::CAS, &proto_digest.hash, size, Box::new(reader)).await {
                Ok(()) => responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto_digest),
                    status_code: OK,
                    status_message: String::new(),
                }),
                Err(e) => responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto_digest),
                    status_code: INTERNAL,
                    status_message: e.to_string(),
                }),
            }
        }

        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    type GetTreeStream = futures::stream::Empty<Result<GetTreeResponse, Status>>;

    async fn get_tree(
        &self,
        _request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented("GetTree not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use sha2::{Digest as Sha2Digest, Sha256};
    use crate::{
        digest::EMPTY_SHA256,
        proto::reapi::{
            batch_update_blobs_request, Digest, FindMissingBlobsRequest,
            BatchUpdateBlobsRequest,
        },
        testing::InMemoryBackend,
    };

    fn make_digest(hash: &str, size: i64) -> Digest {
        Digest { hash: hash.to_string(), size_bytes: size }
    }

    fn hash_bytes(data: &[u8]) -> String {
        format!("{:x}", Sha256::digest(data))
    }

    #[tokio::test]
    async fn find_missing_blobs_present_and_absent() {
        let backend = Arc::new(InMemoryBackend::new());
        let svc = CasService::new(backend);

        let data = b"hello world";
        let hash = hash_bytes(data);
        let size = data.len() as i64;

        // Upload one blob
        svc.batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
            instance_name: String::new(),
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(make_digest(&hash, size)),
                data: data.to_vec().into(),
            }],
        }))
        .await
        .unwrap();

        let absent_hash = "b".repeat(64);
        let resp = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: vec![
                    make_digest(&hash, size),
                    make_digest(&absent_hash, 0),
                ],
            }))
            .await
            .unwrap();

        let missing = resp.into_inner().missing_blob_digests;
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].hash, absent_hash);
    }

    #[tokio::test]
    async fn find_missing_blobs_skips_empty_blob() {
        let backend = Arc::new(InMemoryBackend::new());
        let svc = CasService::new(backend);

        let resp = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: vec![make_digest(EMPTY_SHA256, 0)],
            }))
            .await
            .unwrap();

        // Empty blob is never reported missing
        assert!(resp.into_inner().missing_blob_digests.is_empty());
    }

    #[tokio::test]
    async fn batch_update_rejects_hash_mismatch() {
        let backend = Arc::new(InMemoryBackend::new());
        let svc = CasService::new(backend);

        let data = b"real data";
        let wrong_hash = "a".repeat(64);

        let resp = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(make_digest(&wrong_hash, data.len() as i64)),
                    data: data.to_vec().into(),
                }],
            }))
            .await
            .unwrap();

        let response = &resp.into_inner().responses[0];
        assert_eq!(response.status_code, INVALID_ARGUMENT);
        assert!(response.status_message.contains("hash mismatch"));
    }
}
