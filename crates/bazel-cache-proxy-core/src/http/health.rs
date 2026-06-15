use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;
use crate::{
    digest::EMPTY_SHA256,
    entry_kind::EntryKind,
    error::CacheError,
};
use super::server::AppState;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    match state.backend.contains(EntryKind::CAS, EMPTY_SHA256, 0).await {
        Ok(_) => (StatusCode::OK, Json(HealthResponse { status: "ok" })).into_response(),
        Err(CacheError::BackendUnavailable(_)) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => (StatusCode::OK, Json(HealthResponse { status: "ok" })).into_response(),
    }
}

pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.ready.load(std::sync::atomic::Ordering::Relaxed) {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}
