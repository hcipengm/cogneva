//! Persistent object-backend implementations.

use async_trait::async_trait;
use cog_core::{HttpRequest, ObjectBackend, SFError, SFResult};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// 一次对象存储调用的墙：30s。
///
/// 这些调用都是单对象读/写（元数据、小文件），慢在远端而不是本机；30s 之后
/// 重试还有意义，再等只是把调用方一起挂住。
// cap: bound — 这些是单对象读写的远端延迟；30s 之后重试还有意义，再等只是把调用方一起挂住
const BACKEND_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// File-based object backend for local development and single-node deployment.
#[derive(Debug, Clone)]
pub struct FileObjectBackend {
    base_dir: std::path::PathBuf,
}

/// Whether the filesystem said the path cannot hold the object: nothing is
/// there, or an ancestor is a regular file. That is a determinate "no".
/// Anything else — a symlink cycle, a directory that cannot be searched — is a
/// store that could not be looked at, and folding it into "absent" reports an
/// unknown as a zero.
fn cannot_hold_object(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

impl FileObjectBackend {
    pub fn new(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Resolve an object key to a path inside the object-store root.
    ///
    /// A key is a plain relative path — no empty key, no NUL, no root, no `.` or
    /// `..`. The gateway builds keys out of URL segments, so a key that reaches
    /// the filesystem unchecked decides where the file lands; refusing the shape
    /// here is what makes every caller safe, present and future.
    fn object_path(&self, key: &str) -> SFResult<std::path::PathBuf> {
        let path = self.base_dir.join(key);
        let plain = std::path::Path::new(key)
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)));
        if key.is_empty() || key.contains('\0') || !plain || !path.starts_with(&self.base_dir) {
            return Err(SFError::IO(format!(
                "object key {:?} refused: bad key shape",
                key
            )));
        }
        Ok(path)
    }
}

#[async_trait]
impl ObjectBackend for FileObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        let path = self.object_path(key)?;
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
        let path = self.object_path(key)?;
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn delete(&self, key: &str) -> SFResult<()> {
        let path = self.object_path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
        let path = self.object_path(key)?;
        Ok(format!("file://{}", path.display()))
    }

    async fn exists(&self, key: &str) -> SFResult<bool> {
        let path = self.object_path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if cannot_hold_object(&e) => Ok(false),
            Err(e) => Err(SFError::IO(format!("checking {}: {}", path.display(), e))),
        }
    }

    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let mut keys = Vec::new();
        let dir = match prefix {
            Some(p) => self.object_path(p)?,
            None => self.base_dir.clone(),
        };
        match tokio::fs::metadata(&dir).await {
            Ok(_) => {}
            Err(e) if cannot_hold_object(&e) => return Ok(keys),
            Err(e) => return Err(SFError::IO(format!("listing {}: {}", dir.display(), e))),
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
                .map_err(|e| SFError::IO(format!("reading {}: {}", dir.display(), e)))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| SFError::IO(format!("reading {}: {}", dir.display(), e)))?
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

// ─── What a deployment that was on `local-fs` leaves behind ───

/// The path a deployment that once ran `local-fs` wrote its objects to, when
/// the active provider reads somewhere else.
///
/// `None` means there is nothing to look for: either the local backend is the
/// one running (it reads these files, so none of them is orphaned), or the
/// deployment declares no `base_path` — in that case the local backend shared
/// `data_dir` with everything else the process writes, and a walk there would
/// count databases, backups and workspaces as objects to move.
///
/// This is the D7 switch: the document's `base_path` is where the objects are
/// until someone copies them into the bucket, and counting them at startup is
/// what turns "the switch happened, the data did not" from a thing to
/// rediscover into a line in the log.
pub fn local_object_base(storage: &cog_core::ProviderConfig) -> Option<std::path::PathBuf> {
    if storage.provider == "local-fs" {
        return None;
    }
    storage
        .options
        .get("base_path")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
}

/// How many objects sit under `base`, counted by walking it without loading any.
///
/// A missing directory is zero, not an error: it is the end state the caller is
/// looking for — the local tree is gone. Every other IO failure is returned, so
/// the caller can say it did not manage to look; a count of zero and a failed
/// look must never read the same, or a full volume under an unreadable path
/// looks migrated.
///
/// The entries counted are the ones [`FileObjectBackend::list`] would return:
/// regular files, symlinks followed, hidden files included. Sockets and other
/// special entries are not objects and are not counted.
pub async fn objects_on_disk(base: &std::path::Path) -> std::io::Result<usize> {
    let mut objects = 0usize;
    let mut dirs = vec![base.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            match tokio::fs::metadata(&path).await {
                Ok(meta) if meta.is_dir() => dirs.push(path),
                Ok(meta) if meta.is_file() => objects += 1,
                Ok(_) => {}
                // The entry vanished between readdir and stat; it is not there
                // to migrate, which is the same reading as an empty one.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(objects)
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
        match tokio::time::timeout(BACKEND_CALL_TIMEOUT, fut).await {
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

/// Encode a query-string value the way AWS SigV4 canonicalization requires:
/// every character except the unreserved set is percent-encoded — including
/// `/`, which the path encoder deliberately leaves alone. A continuation token
/// is base64 and routinely contains `/`, `+` and `=`, so reusing the path
/// encoder here would sign a canonical query the server never reproduces.
fn percent_encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
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

/// Extract the text of the first `<tag>...</tag>` in an XML document.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)?;
    Some(xml[start..start + end].to_string())
}

/// Whether a ListObjectsV2 response reports more keys beyond this page.
fn parse_is_truncated(xml: &str) -> bool {
    matches!(xml_text(xml, "IsTruncated").as_deref(), Some("true"))
}

/// The opaque cursor for the next page, undecoded as returned by the server.
fn parse_next_token(xml: &str) -> Option<String> {
    xml_text(xml, "NextContinuationToken")
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
            let ep = self.endpoint.trim_end_matches('/');

            let mut all_keys = Vec::new();
            let mut continuation: Option<String> = None;
            // A bucket listing is unbounded; cap the walk so a server that keeps
            // reporting truncation without advancing the cursor cannot spin forever.
            // cap: bound — a page-count ceiling so a cursor that keeps truncating cannot spin forever
            const MAX_PAGES: usize = 10_000;

            for _ in 0..MAX_PAGES {
                // Query parts are sorted before signing; SigV4 canonicalization
                // requires the same ordering the URL carries.
                let mut query_parts = vec!["list-type=2".to_string(), "max-keys=1000".to_string()];
                if let Some(p) = prefix {
                    query_parts.push(format!("prefix={}", percent_encode_query(p)));
                }
                if let Some(token) = &continuation {
                    query_parts.push(format!(
                        "continuation-token={}",
                        percent_encode_query(token)
                    ));
                }
                query_parts.sort();
                let canonical_query = query_parts.join("&");

                let signed_headers =
                    self.sign_request("GET", &canonical_uri, &canonical_query, &empty_hash, &[]);

                let url = format!("{}/{}?{}", ep, self.bucket, canonical_query);
                let mut req = HttpRequest::get(&url);
                for (k, v) in signed_headers {
                    req = req.header(k, v);
                }

                let resp = self.with_timeout(self.client()?.execute(req)).await?;
                if !resp.is_success() {
                    let status = resp.status;
                    let body = resp
                        .text()
                        .unwrap_or_else(|_| "<unable to read body>".to_string());
                    return Err(SFError::Adapter {
                        provider: "s3".to_string(),
                        message: format!("LIST failed: {} - {}", status, body),
                    });
                }
                let body = resp.text().map_err(|e| SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("read list body failed: {}", e),
                })?;

                all_keys.extend(parse_list_keys(&body));

                if !parse_is_truncated(&body) {
                    return Ok(all_keys);
                }
                continuation = parse_next_token(&body);
                if continuation.is_none() {
                    // Truncated but no cursor: the rest is unreachable. Returning
                    // what we have is wrong silently, so fail loudly.
                    return Err(SFError::Adapter {
                        provider: "s3".to_string(),
                        message: "LIST response truncated without NextContinuationToken"
                            .to_string(),
                    });
                }
            }

            Err(SFError::Adapter {
                provider: "s3".to_string(),
                message: format!(
                    "LIST exceeded {} pages; refusing to loop further",
                    MAX_PAGES
                ),
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serves a scripted sequence of ListObjectsV2 pages and records the URL of
    /// every request so a test can assert the continuation cursor was forwarded.
    #[derive(Debug)]
    struct PagingHttp {
        pages: Vec<String>,
        seen: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for PagingHttp {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            let index = {
                let mut seen = self.seen.lock().unwrap();
                let index = seen.len();
                seen.push(req.url.clone());
                index
            };
            let body = self
                .pages
                .get(index)
                .cloned()
                .ok_or_else(|| SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("unexpected extra LIST request #{}", index + 1),
                })?;
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: body.into_bytes(),
            })
        }
    }

    fn page(keys: &[&str], truncated: bool, token: Option<&str>) -> String {
        let contents: String = keys
            .iter()
            .map(|k| format!("<Contents><Key>{}</Key></Contents>", k))
            .collect();
        let next = token
            .map(|t| format!("<NextContinuationToken>{}</NextContinuationToken>", t))
            .unwrap_or_default();
        format!(
            "<ListBucketResult>{}{}<IsTruncated>{}</IsTruncated>{}</ListBucketResult>",
            contents, next, truncated, ""
        )
    }

    fn backend(pages: Vec<String>) -> (S3ObjectBackend, Arc<PagingHttp>) {
        let http = Arc::new(PagingHttp {
            pages,
            seen: Mutex::new(Vec::new()),
        });
        let backend =
            S3ObjectBackend::new("http://seaweed-s3:8333", "us-east-1", "cogneva", "ak", "sk")
                .with_client(http.clone());
        (backend, http)
    }

    #[tokio::test]
    async fn list_walks_every_page_via_continuation_token() {
        let (backend, http) = backend(vec![
            page(&["a/1", "a/2"], true, Some("tok en/+1=")),
            page(&["a/3"], false, None),
        ]);

        let keys = backend.list(Some("a/")).await.unwrap();

        assert_eq!(keys, vec!["a/1", "a/2", "a/3"]);
        let seen = http.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "expected exactly two LIST requests");
        assert!(seen[0].contains("prefix=a%2F"), "got: {}", seen[0]);
        assert!(
            seen[1].contains("continuation-token=tok%20en%2F%2B1%3D"),
            "second page must carry the encoded cursor, got: {}",
            seen[1]
        );
    }

    #[tokio::test]
    async fn list_stops_after_a_single_untruncated_page() {
        let (backend, http) = backend(vec![page(&["only"], false, None)]);

        let keys = backend.list(None).await.unwrap();

        assert_eq!(keys, vec!["only"]);
        assert_eq!(http.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_fails_loudly_when_truncated_without_a_cursor() {
        // The retry wrapper re-runs the whole listing, so the scripted server
        // must answer each attempt the same way.
        let truncated = page(&["a/1"], true, None);
        let (backend, _) = backend(vec![truncated.clone(), truncated.clone(), truncated]);

        let err = backend.list(None).await.unwrap_err().to_string();

        assert!(
            err.contains("truncated without NextContinuationToken"),
            "got: {}",
            err
        );
    }

    /// A file backend rooted in a directory nobody else in this test run uses.
    fn file_backend(label: &str) -> (FileObjectBackend, std::path::PathBuf) {
        let unique = format!("cogneva-object-keys-{}-{}", label, std::process::id());
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        (FileObjectBackend::new(dir.clone()), dir)
    }

    #[tokio::test]
    async fn file_backend_refuses_keys_that_escape_its_root() {
        let (objects, dir) = file_backend("escape");
        for key in ["../x", "files/../../x", "/etc/passwd", "", "a/../../b"] {
            assert!(objects.put(key, b"x").await.is_err());
            assert!(objects.get(key).await.is_err());
            assert!(objects.exists(key).await.is_err());
            assert!(objects.delete(key).await.is_err());
            assert!(objects.presign_url(key, 60).await.is_err());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_backend_round_trips_a_nested_key() {
        let (objects, dir) = file_backend("round-trip");
        objects.put("files/abc", b"payload").await.unwrap();
        let stored = objects.get("files/abc").await.unwrap();
        assert_eq!(stored, Some(b"payload".to_vec()));
        assert!(objects.exists("files/abc").await.unwrap());
        let listed = objects.list(Some("files")).await.unwrap();
        assert_eq!(listed, vec!["files/abc".to_string()]);
        objects.delete("files/abc").await.unwrap();
        assert!(objects.get("files/abc").await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The count a deployment reads at startup is the same population `list`
    /// returns, so "nothing left behind" can never mean "the walk missed them".
    #[tokio::test]
    async fn objects_on_disk_counts_what_list_would_return() {
        let (objects, dir) = file_backend("left-behind");
        assert_eq!(objects_on_disk(&dir).await.unwrap(), 0);
        objects.put("files/a", b"1").await.unwrap();
        objects.put("files/deep/b", b"2").await.unwrap();
        objects.put("c", b"3").await.unwrap();
        assert_eq!(objects_on_disk(&dir).await.unwrap(), 3);
        assert_eq!(objects.list(None).await.unwrap().len(), 3);
        objects.delete("files/deep/b").await.unwrap();
        assert_eq!(objects_on_disk(&dir).await.unwrap(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A tree that is gone reads as zero; a tree that cannot be looked at reads
    /// as an error. Folding the two together would let an unreadable volume
    /// report itself as already migrated.
    #[tokio::test]
    async fn objects_on_disk_tells_zero_apart_from_could_not_look() {
        let missing = std::env::temp_dir().join(format!(
            "cogneva-object-keys-missing-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&missing);
        assert_eq!(objects_on_disk(&missing).await.unwrap(), 0);

        // A path that is a file, not a directory: something is there that
        // cannot be enumerated, which is not the same fact as an empty tree.
        let (objects, dir) = file_backend("not-a-dir");
        objects.put("file", b"x").await.unwrap();
        assert!(objects_on_disk(&dir.join("file")).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A store that cannot be looked at is not an empty store. `exists` used to
    /// answer `false` and `list` used to answer `[]` for every metadata error,
    /// so a symlink cycle read as "nothing was ever written here".
    #[tokio::test]
    async fn file_backend_tells_absent_apart_from_could_not_look() {
        let (objects, dir) = file_backend("could-not-look");
        // A self-referential symlink: every walk through it fails with ELOOP,
        // which is not the filesystem saying the object is absent.
        let looped = dir.join("loop");
        std::os::unix::fs::symlink("loop", &looped).unwrap();

        let err = objects
            .exists("loop/obj")
            .await
            .expect_err("a store that cannot be looked at must not answer `absent`");
        assert!(err.to_string().contains("loop/obj"), "got: {err}");

        let err = objects
            .list(Some("loop/obj"))
            .await
            .expect_err("a store that cannot be looked at must not answer with an empty list");
        assert!(err.to_string().contains("loop/obj"), "got: {err}");

        // A missing object is still a determinate absence, and so is one under
        // an ancestor that is a regular file.
        assert!(!objects.exists("never-written").await.unwrap());
        objects.put("plain", b"x").await.unwrap();
        assert!(!objects.exists("plain/under-a-file").await.unwrap());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Only a remote provider with a declared `base_path` has an orphaned tree
    /// to look for: `local-fs` is reading it, and without `base_path` the local
    /// backend shared `data_dir` with everything else this process writes.
    #[test]
    fn local_object_base_names_a_tree_only_a_remote_provider_orphans() {
        let provider = |name: &str, options: serde_json::Value| cog_core::ProviderConfig {
            provider: name.to_string(),
            enabled: true,
            options: serde_json::from_value(options).unwrap(),
        };
        assert_eq!(
            local_object_base(&provider(
                "local-fs",
                serde_json::json!({ "base_path": "/var/lib/cogneva-data/storage" })
            )),
            None
        );
        assert_eq!(
            local_object_base(&provider(
                "seaweedfs",
                serde_json::json!({ "base_path": "/var/lib/cogneva-data/storage" })
            )),
            Some(std::path::PathBuf::from("/var/lib/cogneva-data/storage"))
        );
        assert_eq!(
            local_object_base(&provider("seaweedfs", serde_json::json!({}))),
            None
        );
    }
}
