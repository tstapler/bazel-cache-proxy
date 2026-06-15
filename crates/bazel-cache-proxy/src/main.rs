use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

mod config;
mod factory;

#[derive(Parser)]
#[command(name = "bazel-cache-proxy", version, about = "Bazel remote cache proxy")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the cache proxy server
    Serve {
        /// Path to TOML config file
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Validate config file
    /// Exit code: 0 = valid, 1 = parse error, 2 = backend unreachable
    ValidateConfig {
        /// Path to TOML config file
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ValidateConfig { config } => {
            match config::Config::from_file(&config) {
                Ok(_cfg) => {
                    println!("Config OK");
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("Config error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Commands::Serve { config } => {
            let cfg = config::Config::from_file(&config).unwrap_or_else(|e| {
                eprintln!("Failed to load config: {e}");
                std::process::exit(1);
            });

            let http_addr: std::net::SocketAddr = cfg.http_addr.parse().unwrap_or_else(|e| {
                eprintln!("Invalid http_addr: {e}");
                std::process::exit(1);
            });
            let grpc_addr: std::net::SocketAddr = cfg.grpc_addr.parse().unwrap_or_else(|e| {
                eprintln!("Invalid grpc_addr: {e}");
                std::process::exit(1);
            });

            let backend = factory::build_backend(&cfg.backend).await;

            // Initialize Prometheus metrics recorder and get a handle for the scrape endpoint
            let prom_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install Prometheus recorder");

            let metrics_path = cfg.metrics_path.clone();
            let metrics_renderer: Arc<dyn Fn() -> String + Send + Sync + 'static> =
                Arc::new(move || prom_handle.render());

            tracing::info!("HTTP server listening on {http_addr}");
            tracing::info!("gRPC server listening on {grpc_addr}");
            tracing::info!("Prometheus metrics at {metrics_path}");

            // Start both servers concurrently
            let ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let http_backend = backend.clone();
            let http_ready = ready.clone();

            let http_fut = bazel_cache_proxy_core::http::server::serve_with_metrics(
                http_backend,
                http_addr,
                Some(http_ready),
                Some((metrics_path, metrics_renderer)),
            );
            let grpc_fut = bazel_cache_proxy_core::grpc::server::serve(
                backend,
                grpc_addr,
            );

            tokio::join!(
                async { if let Err(e) = http_fut.await { tracing::error!("HTTP server error: {e}"); } },
                async { if let Err(e) = grpc_fut.await { tracing::error!("gRPC server error: {e}"); } },
            );
        }
    }
}
