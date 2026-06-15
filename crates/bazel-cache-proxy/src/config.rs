use std::path::PathBuf;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// HTTP listen address (default: "0.0.0.0:9090")
    #[serde(default = "default_http_addr")]
    pub http_addr: String,

    /// gRPC listen address (default: "0.0.0.0:9091")
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,

    /// Prometheus metrics path (default: "/metrics")
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,

    pub backend: BackendConfig,
}

fn default_http_addr() -> String { "0.0.0.0:9090".to_string() }
fn default_grpc_addr() -> String { "0.0.0.0:9091".to_string() }
fn default_metrics_path() -> String { "/metrics".to_string() }

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    Disk(DiskConfig),
    S3(S3Config),
    Gcs(GcsConfig),
    Gha(GhaConfig),
    Layered(LayeredConfig),
}

#[derive(Debug, Deserialize)]
pub struct DiskConfig {
    pub root: PathBuf,
    #[serde(default = "default_max_size")]
    pub max_size_bytes: u64,
}

fn default_max_size() -> u64 { 10 * 1024 * 1024 * 1024 } // 10 GiB

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub credentials_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct GhaConfig {
    // GHA backend reads all config from env vars
}

#[derive(Debug, Deserialize)]
pub struct LayeredConfig {
    pub primary: Box<BackendConfig>,
    pub fallback: Box<BackendConfig>,
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        use figment::{Figment, providers::{Toml, Env, Format}};
        let config: Config = Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("BAZEL_CACHE_"))
            .extract()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_toml_disk_backend_round_trips() {
        let toml = r#"
[backend]
type = "disk"
root = "/tmp/cache"
max_size_bytes = 1073741824
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(matches!(config.backend, BackendConfig::Disk(_)));
        if let BackendConfig::Disk(d) = &config.backend {
            assert_eq!(d.root, std::path::Path::new("/tmp/cache"));
            assert_eq!(d.max_size_bytes, 1073741824);
        }
    }

    #[test]
    fn config_toml_s3_backend_round_trips() {
        let toml = r#"
[backend]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"
prefix = "bazel/cache"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(matches!(config.backend, BackendConfig::S3(_)));
    }

    #[test]
    fn config_toml_gcs_backend_round_trips() {
        let toml = r#"
[backend]
type = "gcs"
bucket = "my-gcs-bucket"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(matches!(config.backend, BackendConfig::Gcs(_)));
    }

    #[test]
    fn config_toml_gha_backend_round_trips() {
        let toml = r#"
[backend]
type = "gha"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(matches!(config.backend, BackendConfig::Gha(_)));
    }

    #[test]
    fn config_toml_layered_backend_round_trips() {
        let toml = r#"
[backend]
type = "layered"

[backend.primary]
type = "disk"
root = "/tmp/l1"

[backend.fallback]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(matches!(config.backend, BackendConfig::Layered(_)));
    }

    #[test]
    fn config_toml_default_ports() {
        let toml = r#"
[backend]
type = "gha"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.http_addr, "0.0.0.0:9090");
        assert_eq!(config.grpc_addr, "0.0.0.0:9091");
    }

    #[test]
    fn config_toml_custom_ports() {
        let toml = r#"
http_addr = "127.0.0.1:8080"
grpc_addr = "127.0.0.1:8081"
[backend]
type = "gha"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.http_addr, "127.0.0.1:8080");
    }

    #[test]
    fn config_missing_backend_returns_error() {
        let toml = r#"
http_addr = "0.0.0.0:9090"
"#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn config_unknown_backend_type_returns_error() {
        let toml = r#"
[backend]
type = "redis"
"#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
    }
}
