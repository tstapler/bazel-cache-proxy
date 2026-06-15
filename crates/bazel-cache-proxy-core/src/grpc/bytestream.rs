use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use dashmap::DashMap;
use futures::StreamExt;
use sha2::{Digest as Sha2Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;
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
    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

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
            return Ok(Response::new(ReceiverStream::new(rx)));
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

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();

        // Peek first message to get resource_name
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty write stream"))??;
        if first.resource_name.is_empty() {
            return Err(Status::invalid_argument("resource_name missing in first message"));
        }
        let parsed = WriteResourceName::parse(&first.resource_name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Channel pipe: feeder task writes chunks; backend reads from StreamReader
        let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);

        // Shared state for hash verification after put() completes
        let hasher = Arc::new(Mutex::new(Sha256::new()));
        let total_bytes = Arc::new(AtomicI64::new(0));
        let hasher_clone = hasher.clone();
        let total_clone = total_bytes.clone();
        let first_data = first.data.clone();
        let resource_name_for_query = first.resource_name.clone();

        // Spawn feeder: reads gRPC stream, computes SHA-256, sends bytes to channel
        let feed_handle = tokio::spawn(async move {
            // Send first chunk's data
            if !first_data.is_empty() {
                hasher_clone.lock().unwrap().update(&first_data);
                total_clone.fetch_add(first_data.len() as i64, Ordering::Relaxed);
                if tx.send(Ok(bytes::Bytes::from(first_data))).await.is_err() {
                    return;
                }
            }
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(chunk) if !chunk.data.is_empty() => {
                        hasher_clone.lock().unwrap().update(&chunk.data);
                        total_clone.fetch_add(chunk.data.len() as i64, Ordering::Relaxed);
                        if tx.send(Ok(bytes::Bytes::from(chunk.data))).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {} // empty data frame, skip
                    Err(e) => {
                        let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                        break;
                    }
                }
            }
            // dropping tx closes the channel → StreamReader sees EOF
        });

        // Backend reads from the channel (streaming, no full-body buffer)
        let reader = StreamReader::new(ReceiverStream::new(rx));

        self.backend
            .put(EntryKind::CAS, &parsed.hash, parsed.size, Box::new(reader))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Wait for feeder to complete (it may still be running if backend consumed faster)
        let _ = feed_handle.await;

        // Verify hash after put() completes
        let committed_size = total_bytes.load(Ordering::Relaxed);
        let actual_hash = format!("{:x}", hasher.lock().unwrap().clone().finalize());
        if actual_hash != parsed.hash {
            // Delete the corrupted entry
            let _ = self.backend.delete(EntryKind::CAS, &parsed.hash).await;
            return Err(Status::invalid_argument(format!(
                "hash mismatch: expected {}, got {actual_hash}",
                parsed.hash
            )));
        }

        self.completed_writes.insert(resource_name_for_query, committed_size);
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
