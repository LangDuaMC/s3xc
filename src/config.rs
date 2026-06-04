use std::path::PathBuf;
use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug, Clone)]
#[command(author = "LangDuaMC", version, about = "s3xc - high performance S3 caching proxy")]
pub struct Cli {
    /// Path to config file (TOML)
    #[arg(short, long, env = "S3C_CONFIG")]
    pub config: Option<PathBuf>,

    /// Address to bind the server
    #[arg(short, long, env = "S3C_BIND_ADDR", default_value = "127.0.0.1:8080")]
    pub bind_addr: String,

    /// Upstream S3 Endpoint URL
    #[arg(long, env = "S3C_UPSTREAM_ENDPOINT")]
    pub upstream_endpoint: Option<String>,

    /// Backend S3 Endpoint URL (Alternative to upstream_endpoint)
    #[arg(long, env = "S3C_BACKEND_ENDPOINT")]
    pub backend_endpoint: Option<String>,

    /// Upstream S3 Region
    #[arg(long, env = "S3C_UPSTREAM_REGION", default_value = "us-east-1")]
    pub upstream_region: String,

    /// Backend S3 Region (Alternative to upstream_region)
    #[arg(long, env = "S3C_BACKEND_REGION")]
    pub backend_region: Option<String>,

    /// Directory for cache storage
    #[arg(long, env = "S3C_CACHE_DIR", default_value = "./cache")]
    pub cache_dir: PathBuf,

    /// Cache chunk size in bytes (default: 8MB)
    #[arg(long, env = "S3C_CHUNK_SIZE", default_value_t = 8 * 1024 * 1024)]
    pub chunk_size: usize,

    /// Maximum cache size in bytes (default: 50GB)
    #[arg(long, env = "S3C_MAX_CACHE_SIZE", default_value_t = 50 * 1024 * 1024 * 1024)]
    pub max_cache_size: u64,

    /// Frontend client credentials list (format: access:secret;access:secret)
    #[arg(long, env = "S3C_CREDENTIALS")]
    pub credentials: Option<String>,

    /// Backend S3 access key
    #[arg(long, env = "S3C_BACKEND_ACCESS_KEY")]
    pub backend_access_key: Option<String>,

    /// Backend S3 secret key
    #[arg(long, env = "S3C_BACKEND_SECRET_KEY")]
    pub backend_secret_key: Option<String>,

    /// Whether to use path style for backend S3 URL
    #[arg(long, env = "S3C_BACKEND_PATH_SCHEME", default_value = "true")]
    pub backend_path_scheme: String,

    /// Whether to use Signature Version 4 for backend requests
    #[arg(long, env = "S3C_BACKEND_V4_AUTH", default_value = "true")]
    pub backend_v4_auth: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bind_addr: String,
    pub upstream_endpoint: String,
    pub upstream_region: String,
    pub cache_dir: PathBuf,
    pub chunk_size: usize,
    pub max_cache_size: u64,
    pub credentials: Option<String>,
    pub backend_access_key: Option<String>,
    pub backend_secret_key: Option<String>,
    pub backend_path_scheme: bool,
    pub backend_v4_auth: bool,
}

impl Config {
    pub fn from_cli(cli: Cli) -> anyhow::Result<Self> {
        // If a config file is provided, load it; otherwise rely on CLI / Env vars.
        if let Some(config_path) = cli.config {
            let content = std::fs::read_to_string(config_path)?;
            let config: Config = toml::from_str(&content)?;
            Ok(config)
        } else {
            let upstream_endpoint = cli.backend_endpoint
                .or(cli.upstream_endpoint)
                .ok_or_else(|| {
                    anyhow::anyhow!("Backend/Upstream endpoint must be specified via CLI, environment variable, or config file.")
                })?;

            let upstream_region = cli.backend_region
                .unwrap_or(cli.upstream_region);

            let backend_path_scheme = cli.backend_path_scheme.to_lowercase() == "true";
            let backend_v4_auth = cli.backend_v4_auth.to_lowercase() == "true";

            Ok(Config {
                bind_addr: cli.bind_addr,
                upstream_endpoint,
                upstream_region,
                cache_dir: cli.cache_dir,
                chunk_size: cli.chunk_size,
                max_cache_size: cli.max_cache_size,
                credentials: cli.credentials,
                backend_access_key: cli.backend_access_key,
                backend_secret_key: cli.backend_secret_key,
                backend_path_scheme,
                backend_v4_auth,
            })
        }
    }
}
