use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use quick_xml::se::to_string;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ClientCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl ClientCredentials {
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
}

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("NoSuchKey: The specified key does not exist.")]
    NoSuchKey,

    #[error("NoSuchBucket: The specified bucket does not exist.")]
    NoSuchBucket,

    #[error("InvalidAccessKeyId: The AWS Access Key Id you provided does not exist in our records.")]
    InvalidAccessKeyId,

    #[error("SignatureDoesNotMatch: The request signature we calculated does not match the signature you provided.")]
    SignatureDoesNotMatch,

    #[error("InternalError: We encountered an internal error. Please try again.")]
    Internal(#[from] anyhow::Error),
}

#[derive(Serialize, Debug)]
#[serde(rename = "Error")]
struct S3ErrorXml {
    #[serde(rename = "Code")]
    code: &'static str,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Resource")]
    resource: Option<String>,
    #[serde(rename = "RequestId")]
    request_id: &'static str,
}

impl S3Error {
    pub fn to_xml(&self) -> (StatusCode, String) {
        let (status, code, msg) = match self {
            S3Error::NoSuchKey => (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist."),
            S3Error::NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist."),
            S3Error::InvalidAccessKeyId => (StatusCode::FORBIDDEN, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records."),
            S3Error::SignatureDoesNotMatch => (StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided."),
            S3Error::Internal(e) => {
                tracing::error!("Internal error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError", "We encountered an internal error. Please try again.")
            }
        };

        let xml_struct = S3ErrorXml {
            code,
            message: msg.to_string(),
            resource: None,
            request_id: "tx000000000000000000001",
        };

        let xml_str = match to_string(&xml_struct) {
            Ok(s) => format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}", s),
            Err(_) => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InternalError</Code><Message>Failed to serialize error XML</Message></Error>".to_string(),
        };

        (status, xml_str)
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let (status, xml_body) = self.to_xml();
        Response::builder()
            .status(status)
            .header("Content-Type", "application/xml")
            .body(axum::body::Body::from(xml_body))
            .unwrap()
    }
}

pub fn s3_percent_encode(input: &str) -> String {
    let mut encoded = String::new();
    for byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(*byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

pub fn percent_decode(input: &str) -> String {
    let mut result = Vec::new();
    let mut bytes = input.as_bytes().iter();
    while let Some(&b) = bytes.next() {
        if b == b'%' {
            if let (Some(&h), Some(&l)) = (bytes.next(), bytes.next()) {
                if let Ok(hex_byte) = u8::from_str_radix(&format!("{}{}", h as char, l as char), 16) {
                    result.push(hex_byte);
                    continue;
                }
            }
        }
        result.push(b);
    }
    String::from_utf8_lossy(&result).into_owned()
}
