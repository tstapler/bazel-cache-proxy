use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
};
use crate::{digest::Digest, entry_kind::EntryKind};

/// Extracted cache key from the URL path segment.
/// URL pattern: /cache/{kind}/{hash}
#[derive(Debug, Clone)]
pub struct CacheKey {
    pub kind: EntryKind,
    pub hash: String,
}

pub struct CacheKeyRejection(String);

impl IntoResponse for CacheKeyRejection {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.0).into_response()
    }
}

impl<S: Send + Sync> FromRequestParts<S> for CacheKey {
    type Rejection = CacheKeyRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract path segments manually
        let path = parts.uri.path();
        // Path format: /cache/{kind}/{hash}
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if segments.len() < 3 || segments[0] != "cache" {
            return Err(CacheKeyRejection("invalid cache path".into()));
        }
        let kind_str = segments[1];
        let hash = segments[2].to_string();

        let kind = kind_str.parse::<EntryKind>().map_err(|_| {
            CacheKeyRejection(format!("unknown entry kind: {kind_str:?}"))
        })?;

        // Validate hash length and hex characters (size 0 is used for validation only)
        Digest::new(&hash, 0).map_err(|e| CacheKeyRejection(format!("{e}")))?;

        Ok(CacheKey { kind, hash })
    }
}

/// Content-Length header extractor — returns 411 if missing on PUT.
pub struct ContentLength(pub i64);

pub struct ContentLengthRejection;

impl IntoResponse for ContentLengthRejection {
    fn into_response(self) -> Response {
        StatusCode::LENGTH_REQUIRED.into_response()
    }
}

impl<S: Send + Sync> FromRequestParts<S> for ContentLength {
    type Rejection = ContentLengthRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let len = parts
            .headers
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .ok_or(ContentLengthRejection)?;
        Ok(ContentLength(len))
    }
}
