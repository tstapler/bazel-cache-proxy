use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use futures::TryStreamExt;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

use crate::{
    digest::EMPTY_SHA256,
    error::CacheError,
};
use super::{
    extractors::{CacheKey, ContentLength},
    server::AppState,
};

pub async fn head_cache(
    State(state): State<AppState>,
    key: CacheKey,
) -> impl IntoResponse {
    // Empty blob short-circuit
    if key.hash == EMPTY_SHA256 {
        return (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, "0")],
        ).into_response();
    }

    match state.backend.contains(key.kind, &key.hash, -1).await {
        Ok(Some(size)) => (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, size.to_string())],
        ).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(CacheError::BackendUnavailable(msg)) => {
            (StatusCode::SERVICE_UNAVAILABLE, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn get_cache(
    State(state): State<AppState>,
    headers: HeaderMap,
    key: CacheKey,
) -> impl IntoResponse {
    // Empty blob short-circuit
    if key.hash == EMPTY_SHA256 {
        return (StatusCode::OK, Body::empty()).into_response();
    }

    match state.backend.get(key.kind, &key.hash, -1).await {
        Ok(Some(reader)) => {
            if accepts_zstd(&headers) {
                use async_compression::tokio::bufread::ZstdEncoder;
                let encoder = ZstdEncoder::new(BufReader::new(reader));
                let stream = ReaderStream::new(encoder);
                (
                    StatusCode::OK,
                    [(header::CONTENT_ENCODING, "zstd")],
                    Body::from_stream(stream),
                ).into_response()
            } else {
                let stream = ReaderStream::new(reader);
                (StatusCode::OK, Body::from_stream(stream)).into_response()
            }
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(CacheError::BackendUnavailable(msg)) => {
            (StatusCode::SERVICE_UNAVAILABLE, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn put_cache(
    State(state): State<AppState>,
    key: CacheKey,
    content_length: ContentLength,
    request: axum::extract::Request,
) -> impl IntoResponse {
    let size = content_length.0;
    let body = request.into_body();
    let stream = body.into_data_stream();

    use tokio_util::io::StreamReader;
    let reader = StreamReader::new(stream.map_err(|e| {
        std::io::Error::other(e.to_string())
    }));

    match state.backend.put(key.kind, &key.hash, size, Box::new(reader)).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(CacheError::HashMismatch { expected, actual }) => {
            (StatusCode::BAD_REQUEST, format!("hash mismatch: expected {expected}, got {actual}"))
                .into_response()
        }
        Err(CacheError::SizeMismatch { expected, actual }) => {
            (StatusCode::BAD_REQUEST, format!("size mismatch: expected {expected}, got {actual}"))
                .into_response()
        }
        Err(CacheError::BackendUnavailable(msg)) => {
            (StatusCode::SERVICE_UNAVAILABLE, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn accepts_zstd(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').any(|enc| enc.trim() == "zstd"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{Request, StatusCode, Method},
    };
    use axum::http::header;
    use tower::ServiceExt;
    use crate::{
        backend::StorageBackend,
        entry_kind::EntryKind,
        error::CacheError,
    };
    use super::super::server::build_router;

    /// Simple in-memory backend for testing.
    #[derive(Clone, Default)]
    struct InMemoryBackend {
        store: Arc<Mutex<HashMap<(EntryKind, String), Vec<u8>>>>,
    }

    #[async_trait]
    impl StorageBackend for InMemoryBackend {
        async fn contains(
            &self,
            kind: EntryKind,
            hash: &str,
            _size: i64,
        ) -> Result<Option<i64>, CacheError> {
            let store = self.store.lock().unwrap();
            Ok(store.get(&(kind, hash.to_string())).map(|v| v.len() as i64))
        }

        async fn get(
            &self,
            kind: EntryKind,
            hash: &str,
            _size: i64,
        ) -> Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>, CacheError> {
            let store = self.store.lock().unwrap();
            match store.get(&(kind, hash.to_string())) {
                Some(data) => {
                    let cursor = std::io::Cursor::new(data.clone());
                    Ok(Some(Box::new(cursor)))
                }
                None => Ok(None),
            }
        }

        async fn put(
            &self,
            kind: EntryKind,
            hash: &str,
            _logical_size: i64,
            mut data: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        ) -> Result<(), CacheError> {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await.map_err(|e| CacheError::Io(e.to_string()))?;
            let mut store = self.store.lock().unwrap();
            store.insert((kind, hash.to_string()), buf);
            Ok(())
        }

        async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
            let mut store = self.store.lock().unwrap();
            store.remove(&(kind, hash.to_string()));
            Ok(())
        }
    }

    fn test_hash() -> &'static str {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }

    fn make_app() -> axum::Router {
        let backend = Arc::new(InMemoryBackend::default());
        build_router(backend, None)
    }

    #[tokio::test]
    async fn get_miss_returns_404() {
        let app = make_app();
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/cas/{}", test_hash()))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_without_content_length_returns_411() {
        let app = make_app();
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/cache/cas/{}", test_hash()))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::LENGTH_REQUIRED);
    }

    #[tokio::test]
    async fn get_empty_blob_returns_200_without_backend_call() {
        let app = make_app();
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/cas/{}", EMPTY_SHA256))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn head_empty_blob_returns_200_without_backend_call() {
        let app = make_app();
        let req = Request::builder()
            .method(Method::HEAD)
            .uri(format!("/cache/cas/{}", EMPTY_SHA256))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_LENGTH).unwrap().to_str().unwrap(),
            "0"
        );
    }

    #[tokio::test]
    async fn cache_key_extraction_works_for_cas_and_ac() {
        let app = make_app();

        // CAS
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/cas/{}", test_hash()))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        // 404 means key was parsed OK, backend just doesn't have it
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // AC
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/ac/{}", test_hash()))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let app = make_app();
        let hash = test_hash();
        let payload = b"hello bazel cache";

        // PUT
        let put_req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/cache/cas/{hash}"))
            .header(header::CONTENT_LENGTH, payload.len().to_string())
            .body(Body::from(payload.as_slice()))
            .unwrap();
        let put_resp = app.clone().oneshot(put_req).await.unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);

        // GET
        let get_req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/cas/{hash}"))
            .body(Body::empty())
            .unwrap();
        let get_resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], payload);
    }

    #[tokio::test]
    async fn invalid_hash_returns_400() {
        let app = make_app();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/cache/cas/not-a-valid-hash")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_kind_returns_404() {
        // Routes only exist for /cache/cas/{hash} and /cache/ac/{hash}.
        // An unknown kind like "blob" matches no route and returns 404 at the router level.
        let app = make_app();
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/cache/blob/{}", test_hash()))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
