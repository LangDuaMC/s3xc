use std::sync::Arc;
use axum::{
    body::Body,
    extract::{Path, State, Request},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info};
use http_body_util::BodyExt;

use crate::cache::CacheCoordinator;
use crate::utils::S3Error;

#[derive(Clone)]
pub struct AppState {
    pub cache: Arc<CacheCoordinator>,
    pub upstream_endpoint: String,
    pub upstream_region: String,
}

/// Parses the HTTP Range header.
/// Format: `bytes=start-end` or `bytes=start-` or `bytes=-num`
/// Returns (start, end) where end is exclusive.
fn parse_range(range_val: &str, file_size: u64) -> Option<(u64, u64)> {
    if !range_val.starts_with("bytes=") {
        return None;
    }
    let range_str = &range_val[6..];
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start_str = parts[0].trim();
    let end_str = parts[1].trim();

    if start_str.is_empty() && end_str.is_empty() {
        return None;
    }

    if start_str.is_empty() {
        // e.g., `-500` -> last 500 bytes
        let num: u64 = end_str.parse().ok()?;
        let start = file_size.saturating_sub(num);
        Some((start, file_size))
    } else if end_str.is_empty() {
        // e.g., `500-` -> from byte 500 to end
        let start: u64 = start_str.parse().ok()?;
        if start >= file_size {
            return Some((file_size, file_size));
        }
        Some((start, file_size))
    } else {
        // e.g., `500-1000` -> bytes 500 to 1000 inclusive
        let start: u64 = start_str.parse().ok()?;
        let end_incl: u64 = end_str.parse().ok()?;
        let end = std::cmp::min(end_incl + 1, file_size);
        if start > end {
            return None;
        }
        Some((start, end))
    }
}

/// Format UNIX timestamp as HTTP date string (RFC 7231 format)
fn format_http_date(secs: i64) -> String {
    let datetime = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

pub async fn head_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, S3Error> {
    debug!("HEAD bucket={} key={}", bucket, key);
    
    let creds = req.extensions().get::<crate::utils::ClientCredentials>().cloned();
    let meta = state.cache.get_object_metadata(&bucket, &key, creds.as_ref()).await
        .map_err(|e| {
            if e.to_string().contains("NoSuchKey") {
                S3Error::NoSuchKey
            } else {
                S3Error::Internal(e)
            }
        })?;

    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    headers.insert(header::CONTENT_LENGTH, meta.content_length.to_string().parse().unwrap());
    
    if let Some(content_type) = meta.content_type {
        if let Ok(h_val) = content_type.parse() {
            headers.insert(header::CONTENT_TYPE, h_val);
        }
    }
    
    if let Some(etag) = meta.etag {
        if let Ok(h_val) = etag.parse() {
            headers.insert(header::ETAG, h_val);
        }
    }

    headers.insert(
        header::LAST_MODIFIED,
        format_http_date(meta.last_modified).parse().unwrap(),
    );

    // Calculate cache status
    let db_key = format!("{}/{}", bucket, key);
    let is_hot = if let Ok(Some(local_meta)) = state.cache.db.get_object(&db_key) {
        if local_meta.content_length == 0 {
            true
        } else if let Ok(Some(chunk_meta)) = state.cache.db.get_chunk(&db_key, 0) {
            chunk_meta.status == crate::cache::metadata::ChunkStatus::Complete
        } else {
            false
        }
    } else {
        false
    };
    let cache_status = if is_hot { "hot" } else { "cold" };
    headers.insert("X-S3XC-CACHED", cache_status.parse().unwrap());

    Ok((StatusCode::OK, headers, ()).into_response())
}

pub async fn get_object_handler(
    State(state): State<AppState>,
    headers_in: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, S3Error> {
    debug!("GET bucket={} key={}", bucket, key);

    let creds = req.extensions().get::<crate::utils::ClientCredentials>().cloned();
    let meta = state.cache.get_object_metadata(&bucket, &key, creds.as_ref()).await
        .map_err(|e| {
            if e.to_string().contains("NoSuchKey") {
                S3Error::NoSuchKey
            } else {
                S3Error::Internal(e)
            }
        })?;

    let file_size = meta.content_length;

    // Parse Range header if present
    let range = headers_in.get(header::RANGE)
        .and_then(|r| r.to_str().ok())
        .and_then(|r_str| parse_range(r_str, file_size));

    let (start, end, status_code) = match range {
        Some((s, e)) => (s, e, StatusCode::PARTIAL_CONTENT),
        None => (0, file_size, StatusCode::OK),
    };

    let stream = state.cache.clone().read_range(&bucket, &key, start, end, creds).await
        .map_err(|e| S3Error::Internal(e))?;

    // Map stream errors to std::io::Error
    let body = Body::from_stream(stream);

    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    
    if let Some(content_type) = meta.content_type {
        if let Ok(h_val) = content_type.parse() {
            headers.insert(header::CONTENT_TYPE, h_val);
        }
    }
    
    if let Some(etag) = meta.etag {
        if let Ok(h_val) = etag.parse() {
            headers.insert(header::ETAG, h_val);
        }
    }

    headers.insert(
        header::LAST_MODIFIED,
        format_http_date(meta.last_modified).parse().unwrap(),
    );

    let content_len = end.saturating_sub(start);
    headers.insert(header::CONTENT_LENGTH, content_len.to_string().parse().unwrap());

    if status_code == StatusCode::PARTIAL_CONTENT {
        let content_range = format!("bytes {}-{}/{}", start, end.saturating_sub(1), file_size);
        headers.insert(header::CONTENT_RANGE, content_range.parse().unwrap());
    }

    // Calculate cache status
    let db_key = format!("{}/{}", bucket, key);
    let is_hot = if let Ok(Some(local_meta)) = state.cache.db.get_object(&db_key) {
        if local_meta.content_length == 0 {
            true
        } else if let Ok(Some(chunk_meta)) = state.cache.db.get_chunk(&db_key, 0) {
            chunk_meta.status == crate::cache::metadata::ChunkStatus::Complete
        } else {
            false
        }
    } else {
        false
    };
    let cache_status = if is_hot { "hot" } else { "cold" };
    headers.insert("X-S3XC-CACHED", cache_status.parse().unwrap());

    Ok((status_code, headers, body).into_response())
}

pub async fn put_object_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, S3Error> {
    debug!("PUT bucket={} key={}", bucket, key);

    let creds = req.extensions().get::<crate::utils::ClientCredentials>().cloned();

    // Check if it's a CopyObject request (x-amz-copy-source header present)
    if let Some(copy_source) = headers.get("x-amz-copy-source")
        .and_then(|h| h.to_str().ok())
    {
        let copy_source_decoded = crate::utils::percent_decode(copy_source);
        let path = copy_source_decoded.trim_start_matches('/');
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() == 2 {
            let src_bucket = parts[0];
            let mut src_key = parts[1];
            if let Some(idx) = src_key.find('?') {
                src_key = &src_key[..idx];
            }

            // Forward CopyObject call to upstream
            let etag = state.cache.proxy.copy_object(src_bucket, src_key, &bucket, &key).await
                .map_err(|e| S3Error::Internal(e))?;

            // Copy local cache files and metadata from src to dest
            state.cache.copy_cache_object(src_bucket, src_key, &bucket, &key).await
                .map_err(|e| S3Error::Internal(e))?;

            let xml_body = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
   <LastModified>{}</LastModified>
   <ETag>{}</ETag>
</CopyObjectResult>"#,
                format_http_date(chrono::Utc::now().timestamp()),
                etag
            );

            let mut response_headers = HeaderMap::new();
            response_headers.insert(header::CONTENT_TYPE, "application/xml".parse().unwrap());
            response_headers.insert(header::CONTENT_LENGTH, xml_body.len().to_string().parse().unwrap());

            return Ok((StatusCode::OK, response_headers, Body::from(xml_body)).into_response());
        }
    }

    // Extract Content-Length
    let content_len = headers.get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Extract Content-Type
    let content_type = headers.get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .map(String::from);

    // Get Body Stream from client request
    let body_stream = req.into_body().into_data_stream();

    // Write chunk data locally on write and proxy it to upstream
    let etag = state.cache.write_and_cache_object(&bucket, &key, body_stream, content_len, content_type, creds).await
        .map_err(|e| S3Error::Internal(e))?;

    let mut response_headers = HeaderMap::new();
    if let Ok(h_val) = etag.parse() {
        response_headers.insert(header::ETAG, h_val);
    }
    response_headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());

    Ok((StatusCode::OK, response_headers, ()).into_response())
}

pub async fn delete_object_handler(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, S3Error> {
    debug!("DELETE bucket={} key={}", bucket, key);

    let _creds = req.extensions().get::<crate::utils::ClientCredentials>().cloned();

    // 1. Forward the DELETE to upstream first
    state.cache.proxy.delete_object(&bucket, &key).await
        .map_err(|e| {
            if e.to_string().contains("NoSuchKey") {
                S3Error::NoSuchKey
            } else {
                S3Error::Internal(e)
            }
        })?;

    // 2. If successful, drop local cache chunks and DB entries
    state.cache.drop_cache_for_object(&bucket, &key).await
        .map_err(|e| S3Error::Internal(e))?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());

    Ok((StatusCode::NO_CONTENT, response_headers, ()).into_response())
}

/// Catch-all fallback handler to proxy mutating operations, listing, bucket ops, etc.
/// directly to upstream S3.
pub async fn fallback_proxy_handler(
    State(state): State<AppState>,
    req: Request,
) -> Response {
    let method = req.method().as_str().to_string();
    let path_and_query = req.uri().path_and_query().map(|pq| pq.as_str().to_string()).unwrap_or_default();
    let headers = req.headers().clone();

    debug!("Proxying request: {} {}", method, path_and_query);

    let _creds = req.extensions().get::<crate::utils::ClientCredentials>().cloned();

    // Read body bytes if request size is reasonable
    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(bytes) => Some(bytes),
        Err(_) => None,
    };

    // Forward the call to proxy client
    let proxy_res = match state.cache.proxy.proxy_request(&method, &path_and_query, headers, body_bytes).await {
        Ok(res) => res,
        Err(e) => {
            error!("Failed generic proxy request to upstream: {:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let status = StatusCode::from_u16(proxy_res.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response_builder = Response::builder().status(status);

    // Copy headers back to client
    for (name, val) in proxy_res.headers().iter() {
        response_builder = response_builder.header(name.as_str(), val.as_bytes());
    }

    // Stream the body response
    let stream = proxy_res.bytes_stream().map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let body = Body::from_stream(stream);

    response_builder.body(body).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
