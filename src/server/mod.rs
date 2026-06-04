use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tower_http::trace::TraceLayer;
use tracing::info;

pub mod handlers;
pub mod s3_api;

use crate::cache::CacheCoordinator;
pub use handlers::AppState;
use s3_api::SigV4Verifier;

async fn s3_auth_middleware(
    State(verifier): State<Option<Arc<SigV4Verifier>>>,
    mut req: Request,
    next: Next,
) -> Response {
    if let Some(ref verifier) = verifier {
        match verifier.verify(&req) {
            Ok(creds) => {
                req.extensions_mut().insert(creds);
            }
            Err(err) => {
                return err.into_response();
            }
        }
    }
    next.run(req).await
}

pub async fn start_server(
    bind_addr: &str,
    cache: Arc<CacheCoordinator>,
    upstream_endpoint: String,
    upstream_region: String,
    credentials: Option<String>,
) -> anyhow::Result<()> {
    let state = AppState {
        cache,
        upstream_endpoint,
        upstream_region,
    };

    // Load signature verifier from CLI credentials or environment
    let verifier = credentials
        .and_then(|c| SigV4Verifier::new_from_str(&c))
        .or_else(|| SigV4Verifier::new_from_env())
        .map(Arc::new);

    // Routing design:
    // - Specific GET/HEAD object routes (routed to cache)
    // - Fallback catch-all for any other request (routed directly to upstream S3 proxy)
    // - Middleware applied to check S3 signatures if credentials/access-key is set
    let app = Router::new()
        .route(
            "/:bucket/*key",
            get(handlers::get_object_handler)
                .head(handlers::head_object_handler)
                .put(handlers::put_object_handler)
                .delete(handlers::delete_object_handler),
        )
        .fallback(handlers::fallback_proxy_handler)
        .layer(middleware::from_fn_with_state(verifier, s3_auth_middleware))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("S3 Cache Server listening on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
