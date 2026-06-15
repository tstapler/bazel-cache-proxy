use std::sync::Arc;
use dashmap::DashMap;
use futures::StreamExt;
use sha2::{Digest as Sha2Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use crate::{
    backend::StorageBackend,
    digest::EMPTY_SHA256,
    entry_kind::EntryKind,
    proto::bytestream::{
        byte_stream_server::ByteStream, QueryWriteStatusRequest, QueryWriteStatusResponse,
        ReadRequest, ReadResponse, WriteRequest, WriteResponse,
    },
};
use super::resource_name::{ReadResourceName, WriteResourceName};

const READ_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB

pub struct ByteStreamService {
    backend: Arc<dyn StorageBackend>,
    completed_writes: Arc<DashMap<String, i64>>,
}

impl ByteStreamService {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            completed_writes: Arc::new(DashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamService {
    type ReadStream = tokio_stream::wrappers::ReceiverStream<Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        let parsed = ReadResourceName::parse(&req.resource_name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Empty blob: return empty stream immediately
        if parsed.hash == EMPTY_SHA256 && parsed.size == 0 {
            let (tx, rx) = mpsc::channel(1);
            drop(tx);
            return Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)));
        }

        let mut reader = self
            .backend
            .get(EntryKind::CAS, &parsed.hash, parsed.size)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("blob not found"))?;

        if req.read_offset > 0 {
            let mut skip = vec![0u8; req.read_offset as usize];
            reader
                .read_exact(&mut skip)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let (tx, rx) = mpsc::channel(32);
        let read_limit = req.read_limit;

        tokio::spawn(async move {
            let mut remaining = if read_limit > 0 { read_limit as usize } else { usize::MAX };
            let mut buf = vec![0u8; READ_CHUNK_SIZE];
            loop {
                if remaining == 0 {
                    break;
                }
                let to_read = buf.len().min(remaining);
                match reader.read(&mut buf[..to_read]).await {
                    Ok(0) => break,
                    Ok(n) => {
                        remaining = remaining.saturating_sub(n);
                        let chunk = ReadResponse { data: buf[..n].to_vec() };
                        if tx.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let mut resource_name = String::new();
        let mut all_data: Vec<u8> = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| Status::internal(e.to_string()))?;
            if resource_name.is_empty() && !chunk.resource_name.is_empty() {
                resource_name = chunk.resource_name.clone();
            }
            if !chunk.data.is_empty() {
                all_data.extend_from_slice(&chunk.data);
            }
        }

        if resource_name.is_empty() {
            return Err(Status::invalid_argument("empty resource_name"));
        }

        let parsed = WriteResourceName::parse(&resource_name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let actual_hash = format!("{:x}", Sha256::digest(&all_data));
        if actual_hash != parsed.hash {
            return Err(Status::invalid_argument(format!(
                "hash mismatch: expected {}, got {actual_hash}",
                parsed.hash
            )));
        }

        let committed_size = all_data.len() as i64;
        let reader = std::io::Cursor::new(all_data);

        self.backend
            .put(EntryKind::CAS, &parsed.hash, committed_size, Box::new(reader))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.completed_writes.insert(resource_name, committed_size);

        Ok(Response::new(WriteResponse { committed_size }))
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let req = request.into_inner();
        if let Some(entry) = self.completed_writes.get(&req.resource_name) {
            Ok(Response::new(QueryWriteStatusResponse {
                committed_size: *entry,
                complete: true,
            }))
        } else {
            Ok(Response::new(QueryWriteStatusResponse {
                committed_size: 0,
                complete: false,
            }))
        }
    }
}
