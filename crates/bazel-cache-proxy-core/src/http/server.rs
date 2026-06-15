use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, head, put},
};
use tokio::net::TcpListener;
use crate::backend::StorageBackend;
use super::handlers;
use super::health;

pub async fn serve(
    backend: Arc<dyn StorageBackend>,
    addr: SocketAddr,
    ready_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router(backend, ready_flag);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

pub fn build_router(
    backend: Arc<dyn StorageBackend>,
    ready_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Router {
    let put_routes = Router::new()
        .route("/cache/cas/{hash}", put(handlers::put_cache))
        .route("/cache/ac/{hash}", put(handlers::put_cache))
        .layer(DefaultBodyLimit::disable());

    Router::new()
        .route("/cache/cas/{hash}", head(handlers::head_cache))
        .route("/cache/cas/{hash}", get(handlers::get_cache))
        .route("/cache/ac/{hash}", head(handlers::head_cache))
        .route("/cache/ac/{hash}", get(handlers::get_cache))
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .merge(put_routes)
        .with_state(AppState {
            backend,
            ready: ready_flag.unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(true))),
        })
}

#[derive(Clone)]
pub struct AppState {
    pub backend: Arc<dyn StorageBackend>,
    pub ready: Arc<std::sync::atomic::AtomicBool>,
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
