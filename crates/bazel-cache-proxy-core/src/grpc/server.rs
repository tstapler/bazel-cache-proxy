use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use crate::{
    backend::StorageBackend,
    proto::{
        bytestream::byte_stream_server::ByteStreamServer,
        reapi::{
            action_cache_server::ActionCacheServer,
            capabilities_server::CapabilitiesServer,
            content_addressable_storage_server::ContentAddressableStorageServer,
        },
    },
};
use super::{
    action_cache::ActionCacheService, bytestream::ByteStreamService,
    capabilities::CapabilitiesService, cas::CasService,
};

pub async fn serve(
    backend: Arc<dyn StorageBackend>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("gRPC server listening on {addr}");
    Server::builder()
        .add_service(CapabilitiesServer::new(CapabilitiesService))
        .add_service(ActionCacheServer::new(ActionCacheService::new(backend.clone())))
        .add_service(ContentAddressableStorageServer::new(CasService::new(backend.clone())))
        .add_service(ByteStreamServer::new(ByteStreamService::new(backend)))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    Ok(())
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
