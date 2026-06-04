use std::sync::Arc;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing::info;

mod config;
mod cache;
mod proxy;
mod server;
mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,s3xc=debug")),
        )
        .init();

    info!("Starting s3xc high-performance S3 caching proxy...");

    // 2. Parse CLI arguments
    let cli = config::Cli::parse();
    let config = config::Config::from_cli(cli)?;

    // 3. Ensure cache directories exist
    let cache_dir = config.cache_dir.clone();
    std::fs::create_dir_all(&cache_dir)?;
    std::fs::create_dir_all(cache_dir.join("data"))?;

    // 4. Initialize Database
    let db_path = cache_dir.join("metadata.db");
    let db = Arc::new(cache::metadata::MetadataDb::new(&db_path)?);

    // 5. Initialize Storage
    let storage = Arc::new(cache::storage::Storage::new(cache_dir.clone()));
    storage.cleanup_temp_files().await?;

    // 6. Initialize Upstream client
    let proxy = proxy::ProxyClient::new(
        &config.upstream_endpoint,
        &config.upstream_region,
        config.backend_access_key.as_deref(),
        config.backend_secret_key.as_deref(),
        config.backend_path_scheme,
        config.backend_v4_auth,
    ).await;

    // 7. Initialize Cache Coordinator
    let cache_coordinator = Arc::new(cache::CacheCoordinator::new(
        db.clone(),
        storage.clone(),
        proxy,
        config.chunk_size,
    ));

    // 8. Start Eviction Manager
    let eviction_mgr = Arc::new(cache::policy::EvictionManager::new(
        db.clone(),
        storage.clone(),
        config.max_cache_size,
    ));
    eviction_mgr.start();

    // 9. Run Axum Server
    server::start_server(
        &config.bind_addr,
        cache_coordinator,
        config.upstream_endpoint,
        config.upstream_region,
        config.credentials.clone(),
    )
    .await?;

    Ok(())
}
