use axum::http::{header, HeaderMap, Request};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{debug, warn};

use crate::utils::S3Error;

type HmacSha256 = Hmac<Sha256>;

pub struct SigV4Verifier {
    pub credentials: HashMap<String, String>,
}

#[derive(Debug)]
struct SigParams {
    access_key: String,
    date: String, // YYYYMMDD
    datetime: String, // YYYYMMDDTHHMMSSZ
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

impl SigV4Verifier {
    pub fn new_from_env() -> Option<Self> {
        if let Ok(creds_str) = std::env::var("S3C_CREDENTIALS") {
            if let Some(verifier) = Self::new_from_str(&creds_str) {
                return Some(verifier);
            }
        }
        let access_key = std::env::var("S3C_ACCESS_KEY").ok()?;
        let secret_key = std::env::var("S3C_SECRET_KEY").ok()?;
        if access_key.is_empty() || secret_key.is_empty() {
            return None;
        }
        let mut credentials = HashMap::new();
        credentials.insert(access_key, secret_key);
        Some(Self { credentials })
    }

    pub fn new_from_str(creds_str: &str) -> Option<Self> {
        if creds_str.trim().is_empty() {
            return None;
        }
        let mut credentials = HashMap::new();
        for cred in creds_str.split(';') {
            let parts: Vec<&str> = cred.splitn(2, ':').collect();
            if parts.len() == 2 {
                let access = parts[0].trim().to_string();
                let secret = parts[1].trim().to_string();
                if !access.is_empty() && !secret.is_empty() {
                    credentials.insert(access, secret);
                }
            }
        }
        if credentials.is_empty() {
            None
        } else {
            Some(Self { credentials })
        }
    }

    /// Verifies the signature of the incoming request.
    /// Supports both Header and Query parameters authentication.
    pub fn verify<B>(&self, req: &Request<B>) -> Result<crate::utils::ClientCredentials, S3Error> {
        // 1. Extract signature parameters from Header or Query
        let params = if let Some(header_params) = self.extract_from_headers(req.headers()) {
            header_params
        } else if let Some(query_params) = self.extract_from_query(req.uri().query().unwrap_or("")) {
            query_params
        } else {
            warn!("Authentication parameters missing from request");
            return Err(S3Error::InvalidAccessKeyId);
        };

        // 2. Validate Access Key
        let secret_key = self.credentials.get(&params.access_key)
            .ok_or_else(|| {
                warn!("Access Key mismatch or not found: {}", params.access_key);
                S3Error::InvalidAccessKeyId
            })?;

        // 3. Reconstruct Canonical Request
        let canonical_req = self.build_canonical_request(req, &params);
        let canonical_hash = {
            let mut hasher = Sha256::new();
            hasher.update(canonical_req.as_bytes());
            hex::encode(hasher.finalize())
        };

        // 4. Reconstruct String to Sign
        let credential_scope = format!("{}/{}/{}/aws4_request", params.date, params.region, params.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            params.datetime, credential_scope, canonical_hash
        );

        // 5. Calculate Signing Key
        let signing_key = self.derive_signing_key(secret_key, &params.date, &params.region, &params.service);

        // 6. Compute expected Signature
        let expected_signature = {
            let mut mac = HmacSha256::new_from_slice(&signing_key)
                .map_err(|e| S3Error::Internal(anyhow::anyhow!("HMAC init failed: {:?}", e)))?;
            mac.update(string_to_sign.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        };

        // 7. Verify Signature matches
        if expected_signature != params.signature {
            warn!("Signature mismatch. Computed: {}, Client provided: {}", expected_signature, params.signature);
            return Err(S3Error::SignatureDoesNotMatch);
        }

        debug!("Signature verified successfully for access key: {}", params.access_key);

        let session_token = req.headers().get("x-amz-security-token")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| {
                req.uri().query()
                    .and_then(|q| url::form_urlencoded::parse(q.as_bytes())
                        .find(|(k, _)| k == "X-Amz-Security-Token")
                        .map(|(_, v)| v.into_owned()))
            });

        Ok(crate::utils::ClientCredentials {
            access_key: params.access_key.clone(),
            secret_key: secret_key.clone(),
            session_token,
        })
    }

    fn extract_from_headers(&self, headers: &HeaderMap) -> Option<SigParams> {
        let auth_val = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
        if !auth_val.starts_with("AWS4-HMAC-SHA256 ") {
            return None;
        }

        let parts = auth_val["AWS4-HMAC-SHA256 ".len()..].trim();
        let mut credential = "";
        let mut signed_headers_str = "";
        let mut signature = "";

        for part in parts.split(',') {
            let kv: Vec<&str> = part.trim().splitn(2, '=').collect();
            if kv.len() == 2 {
                match kv[0] {
                    "Credential" => credential = kv[1],
                    "SignedHeaders" => signed_headers_str = kv[1],
                    "Signature" => signature = kv[1],
                    _ => {}
                }
            }
        }

        if credential.is_empty() || signed_headers_str.is_empty() || signature.is_empty() {
            return None;
        }

        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() < 5 {
            return None;
        }

        let access_key = cred_parts[0].to_string();
        let date = cred_parts[1].to_string();
        let region = cred_parts[2].to_string();
        let service = cred_parts[3].to_string();
        
        let datetime = headers.get("x-amz-date")
            .and_then(|d| d.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| {
                headers.get(header::DATE)
                    .and_then(|d| d.to_str().ok())
                    .map(|s| s.to_string())
            })?;

        let signed_headers = signed_headers_str.split(';').map(|s| s.to_string()).collect();

        Some(SigParams {
            access_key,
            date,
            datetime,
            region,
            service,
            signed_headers,
            signature: signature.to_string(),
        })
    }

    fn extract_from_query(&self, query: &str) -> Option<SigParams> {
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();

        let algorithm = params.get("X-Amz-Algorithm")?;
        if algorithm != "AWS4-HMAC-SHA256" {
            return None;
        }

        let credential = params.get("X-Amz-Credential")?;
        let datetime = params.get("X-Amz-Date")?;
        let signed_headers_str = params.get("X-Amz-SignedHeaders")?;
        let signature = params.get("X-Amz-Signature")?;

        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() < 5 {
            return None;
        }

        let access_key = cred_parts[0].to_string();
        let date = cred_parts[1].to_string();
        let region = cred_parts[2].to_string();
        let service = cred_parts[3].to_string();

        let signed_headers = signed_headers_str.split(';').map(|s| s.to_string()).collect();

        Some(SigParams {
            access_key,
            date,
            datetime: datetime.to_string(),
            region,
            service,
            signed_headers,
            signature: signature.to_string(),
        })
    }

    fn derive_signing_key(&self, secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
        let secret = format!("AWS4{}", secret_key);
        let k_date = hmac_sha256(secret.as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        hmac_sha256(&k_service, b"aws4_request")
    }

    fn build_canonical_request<B>(&self, req: &Request<B>, params: &SigParams) -> String {
        let method = req.method().as_str();
        
        // Canonical URI
        let canonical_uri = req.uri().path().to_string();

        // Canonical Query String (sorted keys)
        let query_str = req.uri().query().unwrap_or("");
        let mut query_params: Vec<(String, String)> = url::form_urlencoded::parse(query_str.as_bytes())
            .into_owned()
            .collect();
        
        // Exclude X-Amz-Signature from the query parameters to sign
        query_params.retain(|(k, _)| k != "X-Amz-Signature");
        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_query = query_params.iter()
            .map(|(k, v)| format!("{}={}", 
                crate::utils::s3_percent_encode(k), 
                crate::utils::s3_percent_encode(v)
            ))
            .collect::<Vec<String>>()
            .join("&");

        // Canonical Headers
        let mut canonical_headers = String::new();
        for header_name in &params.signed_headers {
            let val = req.headers().get(header_name)
                .map(|v| v.to_str().unwrap_or("").trim())
                .unwrap_or("");
            canonical_headers.push_str(&format!("{}:{}\n", header_name.to_lowercase(), val));
        }

        // Signed Headers list
        let signed_headers_list = params.signed_headers.join(";");

        // Payload Hash
        let hashed_payload = req.headers().get("x-amz-content-sha256")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();

        format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, canonical_uri, canonical_query, canonical_headers, signed_headers_list, hashed_payload
        )
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 should support any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
