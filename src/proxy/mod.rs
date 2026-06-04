use reqwest::header::HeaderMap;
use tracing::debug;
use hmac::Mac;
use sha2::{Digest, Sha256};
use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathScheme {
    PathStyle,
    VirtualHostStyle,
}

#[derive(Debug, Clone)]
pub enum AuthSigner {
    Anonymous,
    SigV4 {
        access_key: String,
        secret_key: String,
        region: String,
    },
}

#[derive(Clone)]
pub struct ProxyClient {
    http_client: reqwest::Client,
    signer: AuthSigner,
    path_scheme: PathScheme,
    upstream_endpoint: String,
    upstream_region: String,
}

impl ProxyClient {
    pub async fn new(
        endpoint: &str,
        region: &str,
        backend_access_key: Option<&str>,
        backend_secret_key: Option<&str>,
        backend_path_scheme: bool,
        backend_v4_auth: bool,
    ) -> Self {
        debug!(
            "Initializing upstream S3 Client for endpoint: {}, region: {}, path_scheme: {}, v4_auth: {}",
            endpoint, region, backend_path_scheme, backend_v4_auth
        );

        let path_scheme = if backend_path_scheme {
            PathScheme::PathStyle
        } else {
            PathScheme::VirtualHostStyle
        };

        // Determine AuthSigner
        let signer = if !backend_v4_auth {
            AuthSigner::Anonymous
        } else if let (Some(ak), Some(sk)) = (backend_access_key, backend_secret_key) {
            AuthSigner::SigV4 {
                access_key: ak.to_string(),
                secret_key: sk.to_string(),
                region: region.to_string(),
            }
        } else if let (Ok(ak), Ok(sk)) = (std::env::var("AWS_ACCESS_KEY_ID"), std::env::var("AWS_SECRET_ACCESS_KEY")) {
            AuthSigner::SigV4 {
                access_key: ak,
                secret_key: sk,
                region: region.to_string(),
            }
        } else {
            AuthSigner::Anonymous
        };

        let http_client = reqwest::Client::builder()
            .build()
            .unwrap_or_default();

        Self {
            http_client,
            signer,
            path_scheme,
            upstream_endpoint: endpoint.to_string(),
            upstream_region: region.to_string(),
        }
    }

    pub fn build_url(&self, bucket: Option<&str>, key_and_query: &str) -> anyhow::Result<url::Url> {
        let mut endpoint = url::Url::parse(&self.upstream_endpoint)?;
        
        let (path_part, query_part) = match key_and_query.find('?') {
            Some(idx) => (&key_and_query[..idx], Some(&key_and_query[idx + 1..])),
            None => (key_and_query, None),
        };

        match (bucket, self.path_scheme) {
            (Some(b), PathScheme::VirtualHostStyle) => {
                let host = endpoint.host_str().unwrap_or("");
                let new_host = format!("{}.{}", b, host);
                endpoint.set_host(Some(&new_host))?;
                
                let clean_path = format!("/{}", path_part.trim_start_matches('/'));
                endpoint.set_path(&clean_path);
            }
            (Some(b), PathScheme::PathStyle) => {
                let clean_path = format!("/{}/{}", b, path_part.trim_start_matches('/'));
                endpoint.set_path(&clean_path);
            }
            (None, _) => {
                let clean_path = format!("/{}", path_part.trim_start_matches('/'));
                endpoint.set_path(&clean_path);
            }
        }
        
        if let Some(qp) = query_part {
            endpoint.set_query(Some(qp));
        } else {
            endpoint.set_query(None);
        }
        
        Ok(endpoint)
    }

    async fn prepare_request(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        key_and_query: &str,
        mut headers: HeaderMap,
        body_bytes: Option<Bytes>,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let url = self.build_url(bucket, key_and_query)?;
        
        // Ensure Host header matches the constructed URL host + port
        let host_val = if let Some(port) = url.port() {
            format!("{}:{}", url.host_str().unwrap_or(""), port)
        } else {
            url.host_str().unwrap_or("").to_string()
        };
        headers.insert(reqwest::header::HOST, host_val.parse()?);

        // Check if we are signing using SigV4
        if let AuthSigner::SigV4 { ref access_key, ref secret_key, ref region } = self.signer {
            let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            let date = datetime[0..8].to_string();
            
            headers.insert("x-amz-date", datetime.parse()?);

            let payload_hash = if let Some(val) = headers.get("x-amz-content-sha256").and_then(|h| h.to_str().ok()) {
                val.to_string()
            } else if let Some(ref body) = body_bytes {
                let mut hasher = Sha256::new();
                hasher.update(body);
                hex::encode(hasher.finalize())
            } else {
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string()
            };
            headers.insert("x-amz-content-sha256", payload_hash.parse()?);

            // Reconstruct Canonical Request
            let canonical_uri = url.path().to_string();
            
            let mut query_params: Vec<(String, String)> = url.query()
                .map(|q| url::form_urlencoded::parse(q.as_bytes()).into_owned().collect())
                .unwrap_or_default();
            query_params.sort_by(|a, b| a.0.cmp(&b.0));
            
            let canonical_query = query_params.iter()
                .map(|(k, v)| format!("{}={}", 
                    crate::utils::s3_percent_encode(k), 
                    crate::utils::s3_percent_encode(v)
                ))
                .collect::<Vec<String>>()
                .join("&");

            // List of signed headers: we will sign host, x-amz-content-sha256, x-amz-date, range/x-amz-copy-source if present
            let mut signed_headers = vec![
                "host".to_string(),
                "x-amz-content-sha256".to_string(),
                "x-amz-date".to_string(),
            ];
            
            if headers.contains_key("range") {
                signed_headers.push("range".to_string());
            }
            if headers.contains_key("x-amz-copy-source") {
                signed_headers.push("x-amz-copy-source".to_string());
            }
            
            signed_headers.sort();

            let mut canonical_headers = String::new();
            for header_name in &signed_headers {
                let val = headers.get(header_name)
                    .map(|v| v.to_str().unwrap_or("").trim())
                    .unwrap_or("");
                canonical_headers.push_str(&format!("{}:{}\n", header_name, val));
            }

            let signed_headers_list = signed_headers.join(";");
            let canonical_req = format!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                method.as_str(),
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers_list,
                payload_hash
            );

            let mut hasher = Sha256::new();
            hasher.update(canonical_req.as_bytes());
            let canonical_hash = hex::encode(hasher.finalize());

            let credential_scope = format!("{}/{}/s3/aws4_request", date, region);
            let string_to_sign = format!(
                "AWS4-HMAC-SHA256\n{}\n{}\n{}",
                datetime, credential_scope, canonical_hash
            );

            let secret = format!("AWS4{}", secret_key);
            let k_date = hmac_sha256(secret.as_bytes(), date.as_bytes());
            let k_region = hmac_sha256(&k_date, region.as_bytes());
            let k_service = hmac_sha256(&k_region, b"s3");
            let k_signing = hmac_sha256(&k_service, b"aws4_request");

            let signature = {
                let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&k_signing).unwrap();
                mac.update(string_to_sign.as_bytes());
                hex::encode(mac.finalize().into_bytes())
            };

            let auth_header = format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                access_key, credential_scope, signed_headers_list, signature
            );
            headers.insert("authorization", auth_header.parse()?);
        }

        let mut req_builder = self.http_client.request(method, url).headers(headers);
        if let Some(ref body) = body_bytes {
            req_builder = req_builder.body(body.clone());
        }
        Ok(req_builder)
    }

    /// Retrieve object metadata (HeadObject) from upstream.
    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<crate::cache::metadata::ObjectMetadata> {
        debug!("Upstream HeadObject via reqwest: bucket={} key={}", bucket, key);

        let headers = HeaderMap::new();
        let req_builder = self.prepare_request(
            reqwest::Method::HEAD,
            Some(bucket),
            key,
            headers,
            None,
        ).await?;

        let res = req_builder.send().await?;
        if !res.status().is_success() {
            let status = res.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                anyhow::bail!("NoSuchKey: Object not found");
            } else {
                anyhow::bail!("Upstream HEAD failed with status {}", status);
            }
        }

        let content_length = res.headers().get("content-length")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let content_type = res.headers().get("content-type")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let etag = res.headers().get("etag")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        let last_modified = res.headers().get("last-modified")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.timestamp())
            .unwrap_or_else(|| chrono::Utc::now().timestamp());

        Ok(crate::cache::metadata::ObjectMetadata {
            content_length,
            content_type,
            etag,
            last_modified,
        })
    }

    /// Retrieve a range of bytes from upstream.
    pub async fn get_object_range_stream(
        &self,
        bucket: &str,
        key: &str,
        range_header: &str,
    ) -> anyhow::Result<impl Stream<Item = Result<Bytes, std::io::Error>>> {
        debug!("Upstream GetObject via reqwest: bucket={} key={} range={}", bucket, key, range_header);

        let mut headers = HeaderMap::new();
        headers.insert("range", range_header.parse().unwrap());

        let req_builder = self.prepare_request(
            reqwest::Method::GET,
            Some(bucket),
            key,
            headers,
            None,
        ).await?;

        let res = req_builder.send().await?;
        if !res.status().is_success() {
            let status = res.status();
            anyhow::bail!("Upstream GET failed with status {}", status);
        }

        let stream = res.bytes_stream().map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
        Ok(stream)
    }

    /// Upload a stream of bytes to upstream.
    pub async fn put_object_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_length: u64,
        content_type: Option<String>,
    ) -> anyhow::Result<String>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        debug!("Upstream PutObject via reqwest stream: bucket={} key={}", bucket, key);

        let mut headers = HeaderMap::new();
        headers.insert("content-length", content_length.to_string().parse().unwrap());
        if let Some(ct) = content_type {
            headers.insert("content-type", ct.parse().unwrap());
        }
        headers.insert("x-amz-content-sha256", "UNSIGNED-PAYLOAD".parse().unwrap());

        let req_builder = self.prepare_request(
            reqwest::Method::PUT,
            Some(bucket),
            key,
            headers,
            None,
        ).await?;

        // Wrap stream in reqwest body and send
        let body = reqwest::Body::wrap_stream(stream);
        let res = req_builder.body(body).send().await?;
        
        if !res.status().is_success() {
            let status = res.status();
            let body_text = res.text().await.unwrap_or_default();
            anyhow::bail!("Upstream upload failed with status {}: {}", status, body_text);
        }

        // Parse ETag from response headers to return it
        let etag = res.headers().get("etag")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(etag)
    }

    /// Delete an object from upstream.
    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<()> {
        let url = self.build_url(Some(bucket), key)?;
        debug!("Upstream DeleteObject via reqwest: bucket={} key={} url={}", bucket, key, url);

        let headers = HeaderMap::new();
        let req_builder = self.prepare_request(
            reqwest::Method::DELETE,
            Some(bucket),
            key,
            headers,
            None,
        ).await?;

        let res = req_builder.send().await?;
        debug!("Upstream DeleteObject response: status={}", res.status());
        if !res.status().is_success() {
            let status = res.status();
            anyhow::bail!("Upstream DELETE failed with status {}", status);
        }
        Ok(())
    }

    /// Copy an object upstream (CopyObject).
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dest_bucket: &str,
        dest_key: &str,
    ) -> anyhow::Result<String> {
        debug!("Upstream CopyObject via reqwest: src_bucket={} src_key={} dest_bucket={} dest_key={}", src_bucket, src_key, dest_bucket, dest_key);

        let mut headers = HeaderMap::new();
        let copy_source_val = format!("/{}/{}", src_bucket, crate::utils::s3_percent_encode(src_key));
        headers.insert("x-amz-copy-source", copy_source_val.parse().unwrap());

        let req_builder = self.prepare_request(
            reqwest::Method::PUT,
            Some(dest_bucket),
            dest_key,
            headers,
            None,
        ).await?;

        let res = req_builder.send().await?;
        if !res.status().is_success() {
            let status = res.status();
            let err_body = res.text().await.unwrap_or_default();
            anyhow::bail!("Upstream CopyObject failed with status {}: {}", status, err_body);
        }

        // Parse ETag from response headers to return it
        let etag = res.headers().get("etag")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(etag)
    }

    /// Catch-all generic S3 proxy method.
    pub async fn proxy_request(
        &self,
        method: &str,
        path_and_query: &str,
        headers: HeaderMap,
        body_bytes: Option<Bytes>,
    ) -> anyhow::Result<reqwest::Response> {
        debug!("Proxying generic request: {} {}", method, path_and_query);

        // Copy incoming headers, omitting signature/host/content-length/etc headers
        let mut out_headers = HeaderMap::new();
        for (name, val) in headers.iter() {
            let name_str = name.as_str();
            if name_str != "host" 
                && name_str != "authorization" 
                && !name_str.starts_with("x-amz-")
                && name_str != "content-length"
            {
                out_headers.insert(name.clone(), val.clone());
            }
        }

        // Since path_and_query starts with /bucket/key?query, we need to extract bucket and the rest.
        // Split query string off first to avoid it being included in bucket/key segments.
        let (path_part, query_part) = match path_and_query.find('?') {
            Some(idx) => (&path_and_query[..idx], Some(&path_and_query[idx + 1..])),
            None => (path_and_query, None),
        };

        let (bucket, key_and_query) = {
            let clean_path = path_part.trim_start_matches('/');
            let parts: Vec<&str> = clean_path.splitn(2, '/').collect();
            if parts.is_empty() || parts[0].is_empty() {
                (None, String::new())
            } else if parts.len() == 1 {
                // Bucket-level operation (e.g. POST /bucket?delete)
                let mut kq = String::new();
                if let Some(q) = query_part {
                    kq = format!("?{}", q);
                }
                (Some(parts[0]), kq)
            } else {
                // Bucket + key operation
                let mut kq = parts[1].to_string();
                if let Some(q) = query_part {
                    kq = format!("{}?{}", kq, q);
                }
                (Some(parts[0]), kq)
            }
        };

        // Prepare and sign request using helper
        let req_builder = self.prepare_request(
            reqwest::Method::from_bytes(method.as_bytes())?,
            bucket,
            &key_and_query,
            out_headers,
            body_bytes,
        ).await?;

        let res = req_builder.send().await?;
        Ok(res)
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
