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
    serve_with_metrics(backend, addr, ready_flag, None).await
}

/// A metrics renderer: a path and a function that returns the Prometheus scrape output.
pub type MetricsRenderer = (String, Arc<dyn Fn() -> String + Send + Sync + 'static>);

pub async fn serve_with_metrics(
    backend: Arc<dyn StorageBackend>,
    addr: SocketAddr,
    ready_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    metrics: Option<MetricsRenderer>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router_with_metrics(backend, ready_flag, metrics);
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
    build_router_with_metrics(backend, ready_flag, None)
}

pub fn build_router_with_metrics(
    backend: Arc<dyn StorageBackend>,
    ready_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    metrics: Option<MetricsRenderer>,
) -> Router {
    let put_routes = Router::new()
        .route("/cache/cas/{hash}", put(handlers::put_cache))
        .route("/cache/ac/{hash}", put(handlers::put_cache))
        .layer(DefaultBodyLimit::disable());

    let mut router = Router::new()
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
        });

    if let Some((path, renderer)) = metrics {
        router = router.route(
            &path,
            get(move || {
                let r = renderer.clone();
                async move { r() }
            }),
        );
    }

    router
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
