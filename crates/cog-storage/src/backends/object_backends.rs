//! Persistent object-backend implementations.

use async_trait::async_trait;
use cog_core::{HttpRequest, ObjectBackend, SFError, SFResult};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// File-based object backend for local development and single-node deployment.
#[derive(Debug, Clone)]
pub struct FileObjectBackend {
    base_dir: std::path::PathBuf,
}

impl FileObjectBackend {
    pub fn new(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    fn object_path(&self, key: &str) -> std::path::PathBuf {
        self.base_dir.join(key)
    }
}

#[async_trait]
impl ObjectBackend for FileObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        let path = self.object_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SFError::IO(e.to_string()))?;
        }
        tokio::fs::write(&path, data)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(format!("file://{}", path.display()))
    }

    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        let path = self.object_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn delete(&self, key: &str) -> SFResult<()> {
        let path = self.object_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
        let path = self.object_path(key);
        Ok(format!("file://{}", path.display()))
    }

    async fn exists(&self, key: &str) -> SFResult<bool> {
        let path = self.object_path(key);
        Ok(tokio::fs::metadata(&path).await.is_ok())
    }

    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let mut keys = Vec::new();
        let dir = match prefix {
            Some(p) => self.base_dir.join(p),
            None => self.base_dir.clone(),
        };
        if tokio::fs::metadata(&dir).await.is_err() {
            return Ok(keys);
        }
        self.list_recursive(&dir, &self.base_dir, &mut keys).await?;
        keys.sort();
        Ok(keys)
    }
}

impl FileObjectBackend {
    async fn list_recursive(
        &self,
        start_dir: &std::path::Path,
        base: &std::path::Path,
        keys: &mut Vec<String>,
    ) -> SFResult<()> {
        let mut dirs = vec![start_dir.to_path_buf()];

        while let Some(dir) = dirs.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| SFError::IO(e.to_string()))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
            {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.is_file() {
                    let rel = path.strip_prefix(base).unwrap_or(&path);
                    let key = rel.to_string_lossy().replace('\\', "/");
                    keys.push(key);
                }
            }
        }
        Ok(())
    }
}

// ─── S3-compatible object backend ───

/// S3-compatible object backend for production deployments.
/// Works with AWS S3, MinIO (self-hosted), and Tencent Cloud COS
/// (all expose an S3-compatible REST API).
/// Uses AWS Signature Version 4 for authentication and presigned URLs.
#[derive(Debug, Clone)]
pub struct S3ObjectBackend {
    endpoint: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    client: Option<Arc<dyn cog_core::HttpClient>>,
}

impl S3ObjectBackend {
    /// Create a new S3-compatible backend.
    /// `endpoint` should include the protocol, e.g.:
    /// - `"https://s3.amazonaws.com"` (AWS S3)
    /// - `"http://localhost:9000"` (MinIO)
    /// - `"https://cos.ap-guangzhou.myqcloud.com"` (Tencent COS)
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            client: None,
        }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn client(&self) -> SFResult<&Arc<dyn cog_core::HttpClient>> {
        self.client.as_ref().ok_or_else(|| SFError::Adapter {
            provider: "s3".to_string(),
            message: "no HttpClient configured".to_string(),
        })
    }

    /// Full URL for an object key (path-style: `{endpoint}/{bucket}/{key}`).
    fn object_url(&self, key: &str) -> String {
        let ep = self.endpoint.trim_end_matches('/');
        format!("{}/{}/{}", ep, self.bucket, key)
    }

    /// Extract the host portion from the endpoint URL.
    fn host(&self) -> String {
        let ep = self.endpoint.trim_end_matches('/');
        ep.strip_prefix("https://")
            .or_else(|| ep.strip_prefix("http://"))
            .unwrap_or(ep)
            .to_string()
    }

    /// Build the canonical URI for a key.
    fn canonical_uri(&self, key: &str) -> String {
        let encoded = percent_encode(key);
        format!("/{}/{}", self.bucket, encoded)
    }

    /// Compute HMAC-SHA256.
    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Compute SHA-256 hex digest.
    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Derive the V4 signing key.
    fn signing_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.secret_key);
        let k_date = Self::hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = Self::hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = Self::hmac_sha256(&k_region, b"s3");
        Self::hmac_sha256(&k_service, b"aws4_request")
    }

    /// Sign a request and return the headers that must be added.
    fn sign_request(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        payload_hash: &str,
        extra_headers: &[(String, String)],
    ) -> Vec<(String, String)> {
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // Build header list: host + extras + x-amz-date + x-amz-content-sha256
        let mut headers: Vec<(String, String)> = Vec::with_capacity(extra_headers.len() + 3);
        headers.push(("host".to_string(), self.host()));
        for (k, v) in extra_headers {
            headers.push((k.to_lowercase(), v.clone()));
        }
        headers.push(("x-amz-date".to_string(), amz_date.clone()));
        headers.push(("x-amz-content-sha256".to_string(), payload_hash.to_string()));

        // Sort by lowercase key name
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        // Canonical headers
        let mut canonical_headers = String::new();
        let mut signed_header_names: Vec<String> = Vec::with_capacity(headers.len());
        for (k, v) in &headers {
            canonical_headers.push_str(&format!("{}:{}", k, v.trim()));
            canonical_headers.push('\n');
            signed_header_names.push(k.clone());
        }
        let signed_headers = signed_header_names.join(";");

        // Canonical request
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        // String to sign
        let credential_scope = format!(
            "{date_stamp}/{region}/s3/aws4_request",
            region = self.region
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}",
            hashed_canonical_request = Self::sha256_hex(canonical_request.as_bytes())
        );

        // Signature
        let sig_key = self.signing_key(&date_stamp);
        let signature = Self::hmac_sha256(&sig_key, string_to_sign.as_bytes());
        let signature_hex = hex::encode(signature);

        // Authorization header
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature_hex}",
            access_key = self.access_key
        );

        vec![
            ("Authorization".to_string(), auth),
            ("x-amz-date".to_string(), amz_date),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ]
    }

    /// Generate a presigned URL using V4 signature in query parameters.
    fn presign_url_v4(&self, key: &str, expiry_secs: u64) -> String {
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();
        let canonical_uri = self.canonical_uri(key);
        let host = self.host();

        // Build sorted query parameters
        let credential = format!(
            "{}/{}/{}/s3/aws4_request",
            self.access_key, date_stamp, self.region
        );
        let mut params: Vec<(&str, String)> = vec![
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
            ("X-Amz-Credential", credential),
            ("X-Amz-Date", amz_date.clone()),
            ("X-Amz-Expires", expiry_secs.to_string()),
            ("X-Amz-SignedHeaders", "host".to_string()),
        ];
        params.sort_by(|a, b| a.0.cmp(b.0));

        let canonical_query = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        // Canonical request for presigned URL uses UNSIGNED-PAYLOAD
        let canonical_request = format!(
            "GET\n{canonical_uri}\n{canonical_query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD"
        );

        let credential_scope = format!(
            "{date_stamp}/{region}/s3/aws4_request",
            region = self.region
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed}",
            hashed = Self::sha256_hex(canonical_request.as_bytes())
        );

        let sig_key = self.signing_key(&date_stamp);
        let signature = Self::hmac_sha256(&sig_key, string_to_sign.as_bytes());
        let signature_hex = hex::encode(signature);

        let url = self.object_url(key);
        format!("{url}?{canonical_query}&X-Amz-Signature={signature_hex}")
    }

    /// Apply a 30-second timeout to an HTTP request.
    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = SFResult<T>>,
    ) -> SFResult<T> {
        match tokio::time::timeout(std::time::Duration::from_secs(30), fut).await {
            Ok(v) => v,
            Err(_) => Err(SFError::Timeout),
        }
    }

    /// Retry an S3 operation with exponential backoff (3 attempts).
    async fn with_retry<T, F, Fut>(&self, mut op: F) -> SFResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = SFResult<T>>,
    {
        let mut last_err = None;
        for attempt in 0..3 {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            200 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(last_err.unwrap())
    }
}

/// Minimal percent-encoder for URI path components (RFC 3986 unreserved chars + `/`).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{:02X}", byte));
            }
        }
    }
    out
}

/// Parse S3 ListObjectsV2 XML response and extract object keys.
fn parse_list_keys(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for block in xml.split("<Contents>").skip(1) {
        if let Some(start) = block.find("<Key>") {
            let text_start = start + 5; // len("<Key>")
            if let Some(end) = block[text_start..].find("</Key>") {
                keys.push(block[text_start..text_start + end].to_string());
            }
        }
    }
    keys
}

#[async_trait]
impl ObjectBackend for S3ObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        self.with_retry(|| async {
            let payload_hash = S3ObjectBackend::sha256_hex(data);
            let canonical_uri = self.canonical_uri(key);
            let extra = vec![("content-length".to_string(), data.len().to_string())];
            let signed_headers =
                self.sign_request("PUT", &canonical_uri, "", &payload_hash, &extra);

            let url = self.object_url(key);
            let mut req = HttpRequest::new("PUT", &url).body(data.to_vec());
            for (k, v) in signed_headers {
                req = req.header(k, v);
            }

            let resp = self.with_timeout(self.client()?.execute(req)).await?;
            if resp.is_success() {
                Ok(format!("s3://{}/{}", self.bucket, key))
            } else {
                let status = resp.status;
                let body = resp
                    .text()
                    .unwrap_or_else(|_| "<unable to read body>".to_string());
                Err(SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("PUT {} failed: {} - {}", key, status, body),
                })
            }
        })
        .await
    }

    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        self.with_retry(|| async {
            let empty_hash = S3ObjectBackend::sha256_hex(b"");
            let canonical_uri = self.canonical_uri(key);
            let signed_headers = self.sign_request("GET", &canonical_uri, "", &empty_hash, &[]);

            let url = self.object_url(key);
            let mut req = HttpRequest::get(&url);
            for (k, v) in signed_headers {
                req = req.header(k, v);
            }

            let resp = self.with_timeout(self.client()?.execute(req)).await?;
            match resp.status {
                200 => Ok(Some(resp.body)),
                404 => Ok(None),
                _ => {
                    let status = resp.status;
                    let body = resp
                        .text()
                        .unwrap_or_else(|_| "<unable to read body>".to_string());
                    Err(SFError::Adapter {
                        provider: "s3".to_string(),
                        message: format!("GET {} failed: {} - {}", key, status, body),
                    })
                }
            }
        })
        .await
    }

    async fn delete(&self, key: &str) -> SFResult<()> {
        self.with_retry(|| async {
            let empty_hash = S3ObjectBackend::sha256_hex(b"");
            let canonical_uri = self.canonical_uri(key);
            let signed_headers = self.sign_request("DELETE", &canonical_uri, "", &empty_hash, &[]);

            let url = self.object_url(key);
            let mut req = HttpRequest::delete(&url);
            for (k, v) in signed_headers {
                req = req.header(k, v);
            }

            let resp = self.with_timeout(self.client()?.execute(req)).await?;
            if resp.is_success() || resp.status == 404 {
                Ok(())
            } else {
                let status = resp.status;
                let body = resp
                    .text()
                    .unwrap_or_else(|_| "<unable to read body>".to_string());
                Err(SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("DELETE {} failed: {} - {}", key, status, body),
                })
            }
        })
        .await
    }

    async fn presign_url(&self, key: &str, expiry_secs: u64) -> SFResult<String> {
        Ok(self.presign_url_v4(key, expiry_secs))
    }

    async fn exists(&self, key: &str) -> SFResult<bool> {
        self.with_retry(|| async {
            let empty_hash = S3ObjectBackend::sha256_hex(b"");
            let canonical_uri = self.canonical_uri(key);
            let signed_headers = self.sign_request("HEAD", &canonical_uri, "", &empty_hash, &[]);

            let url = self.object_url(key);
            let mut req = HttpRequest::head(&url);
            for (k, v) in signed_headers {
                req = req.header(k, v);
            }

            let resp = self.with_timeout(self.client()?.execute(req)).await?;
            match resp.status {
                200 => Ok(true),
                404 => Ok(false),
                _ => {
                    let status = resp.status;
                    let body = resp
                        .text()
                        .unwrap_or_else(|_| "<unable to read body>".to_string());
                    Err(SFError::Adapter {
                        provider: "s3".to_string(),
                        message: format!("HEAD {} failed: {} - {}", key, status, body),
                    })
                }
            }
        })
        .await
    }

    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        self.with_retry(|| async {
            let empty_hash = S3ObjectBackend::sha256_hex(b"");
            let canonical_uri = format!("/{}/", self.bucket);

            // Build query string for ListObjectsV2
            let mut query_parts = vec!["list-type=2".to_string(), "max-keys=1000".to_string()];
            if let Some(p) = prefix {
                query_parts.push(format!("prefix={}", percent_encode(p)));
            }
            query_parts.sort();
            let canonical_query = query_parts.join("&");

            let signed_headers =
                self.sign_request("GET", &canonical_uri, &canonical_query, &empty_hash, &[]);

            let ep = self.endpoint.trim_end_matches('/');
            let url = format!("{}/{}?{}", ep, self.bucket, canonical_query);
            let mut req = HttpRequest::get(&url);
            for (k, v) in signed_headers {
                req = req.header(k, v);
            }

            let resp = self.with_timeout(self.client()?.execute(req)).await?;
            if resp.is_success() {
                let body = resp.text().map_err(|e| SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("read list body failed: {}", e),
                })?;
                Ok(parse_list_keys(&body))
            } else {
                let status = resp.status;
                let body = resp
                    .text()
                    .unwrap_or_else(|_| "<unable to read body>".to_string());
                Err(SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("LIST failed: {} - {}", status, body),
                })
            }
        })
        .await
    }
}
