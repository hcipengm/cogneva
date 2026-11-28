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
// cap: fixed — 这些是单对象读写的远端延迟；30s 之后重试还有意义，再等只是把调用方一起挂住
// keep: 删掉后一个不响应的远端把调用方一起挂住，重试也没发生；由「30s 之后重试还有意义」兜住
// wall: 等=对象存储对一次单对象读写的答复 / 等不到=这次读写按失败收尾，重试还有意义；再等只是把调用方一起挂住 / 续跑=能，重试是一次新的请求
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

/// 一段正在写进文件的对象。
///
/// 先写 `<path>.partial`，封口时才改名成正式的那一个。这个中间名就是契约里那句
/// 「封口之后才对读者存在」：读者按 key 找不到半段，`abort` 丢掉的就是它。
struct FileWrite {
    file: tokio::fs::File,
    path: std::path::PathBuf,
    partial: std::path::PathBuf,
}

#[async_trait]
impl cog_core::ObjectWrite for FileWrite {
    async fn write(&mut self, chunk: &[u8]) -> SFResult<()> {
        use tokio::io::AsyncWriteExt;
        self.file
            .write_all(chunk)
            .await
            .map_err(|e| SFError::IO(e.to_string()))
    }

    async fn finish(mut self: Box<Self>) -> SFResult<String> {
        use tokio::io::AsyncWriteExt;
        self.file
            .flush()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        // 改名之前先落盘：改名的语义是「这一份已经完整」，而一个还没落盘的完整名字
        // 会在掉电之后变成一个空对象。
        self.file
            .sync_all()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        tokio::fs::rename(&self.partial, &self.path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(format!("file://{}", self.path.display()))
    }

    async fn abort(self: Box<Self>) -> SFResult<()> {
        let FileWrite { file, partial, .. } = *self;
        drop(file);
        tokio::fs::remove_file(&partial)
            .await
            .map_err(|e| SFError::IO(e.to_string()))
    }
}

#[async_trait]
impl ObjectBackend for FileObjectBackend {
    async fn open_write(&self, key: &str) -> SFResult<Box<dyn cog_core::ObjectWrite>> {
        let path = self.object_path(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SFError::IO(e.to_string()))?;
        }
        let partial = path.with_extension("partial");
        let file = tokio::fs::File::create(&partial)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(Box::new(FileWrite {
            file,
            path,
            partial,
        }))
    }

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

    /// 让这个后端引用的桶在存储上真的存在。
    ///
    /// 引用（`artifact://…` / `s3://<bucket>/<key>`）能不能取回全文，取决于桶是否
    /// 存在——对象后端自己只假设它在。2026-11-27 现场：部署里配置了 `seaweedfs`
    /// 与桶 `cogneva`，存储上一个桶都没有；那一刻所有外置正文的引用都不可解，而
    /// 账上没有一格说这件事。存在即返回；不存在就建（幂等）；别的失败原样报出来，
    /// 由装配处决定要不要拦。
    pub async fn ensure_bucket(&self) -> SFResult<()> {
        let client = self.client()?;
        let empty_hash = Self::sha256_hex(b"");
        let canonical_uri = format!("/{}", self.bucket);
        let ep = self.endpoint.trim_end_matches('/');
        let url = format!("{}/{}", ep, self.bucket);

        // 一次往返：桶在不在（HEAD 桶本身，不列内容）。
        let mut req = HttpRequest::head(&url);
        for (k, v) in self.sign_request("HEAD", &canonical_uri, "", &empty_hash, &[]) {
            req = req.header(k, v);
        }
        let resp = self.with_timeout(client.execute(req)).await?;
        match resp.status {
            200..=299 => return Ok(()),
            404 => {}
            status => {
                let body = resp
                    .text()
                    .unwrap_or_else(|_| "<unable to read body>".to_string());
                return Err(SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("bucket {}: HEAD answered {status} - {body}", self.bucket),
                });
            }
        }

        // 一次往返：建桶（幂等；已存在时服务端答 200/409，都算它在）。
        let mut req = HttpRequest::put(&url);
        for (k, v) in self.sign_request("PUT", &canonical_uri, "", &empty_hash, &[]) {
            req = req.header(k, v);
        }
        let resp = self.with_timeout(client.execute(req)).await?;
        match resp.status {
            200..=299 | 409 => Ok(()),
            status => {
                let body = resp
                    .text()
                    .unwrap_or_else(|_| "<unable to read body>".to_string());
                Err(SFError::Adapter {
                    provider: "s3".to_string(),
                    message: format!("bucket {}: PUT answered {status} - {body}", self.bucket),
                })
            }
        }
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
        // roundtrip: 一次对象后端调用
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

/// 一次分块上传：每段到齐就发出去，内存的上界是**一段**，不是整个对象。
///
/// 为什么不是 `UNSIGNED-PAYLOAD`：那只换签名口径，仍然要把整段字节攒在调用方手里再发，
/// 「大块字节的写必须在内存里攒齐」这条一点没变。分块上传是唯一能把它变成「一段」的
/// 形态，而 S3 的每一段各自签自己的载荷哈希——签名与校验因此仍然真跑过。
// cap: fixed — 协议给定的下界：S3 不接受小于协议下限的段（最后一段除外），这里取下限的两倍，与这台机器无关
// not-a-bound: 协议给定的下界：S3 不接受小于协议下限的段（最后一段除外），这里取下限的两倍，与这台机器无关
const S3_PART_BYTES: usize = 10 * 1024 * 1024;

struct S3Write {
    /// 后端的副本（它本身是 `Clone`）：句柄活得比调用方的 `&self` 长，所以不能借。
    backend: S3ObjectBackend,
    key: String,
    upload_id: String,
    buffer: Vec<u8>,
    etags: Vec<String>,
}

impl S3Write {
    /// 把缓冲区前 `n` 字节当作一段发出去，剩下的留在缓冲里。
    async fn flush_part_of(&mut self, n: usize) -> SFResult<()> {
        // 前 n 字节作为这一段，剩下的先拿开；`flush_part` 发完会把 buffer 取空。
        let rest = self.buffer.split_off(n);
        let outcome = self.flush_part().await;
        self.buffer = rest;
        outcome
    }

    /// 发一段；空段不发（S3 不接受零长度段）。
    async fn flush_part(&mut self) -> SFResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let part_number = self.etags.len() + 1;
        let data = std::mem::take(&mut self.buffer);
        let hash = S3ObjectBackend::sha256_hex(&data);
        let canonical = self.backend.canonical_uri(&self.key);
        let query = format!(
            "partNumber={part_number}&uploadId={}",
            percent_encode(&self.upload_id)
        );
        let extra = vec![
            ("content-length".to_string(), data.len().to_string()),
            ("x-amz-content-sha256".to_string(), hash.clone()),
        ];
        let signed = self
            .backend
            .sign_request("PUT", &canonical, &query, &hash, &extra);
        let url = format!("{}?{}", self.backend.object_url(&self.key), query);
        let mut req = HttpRequest::new("PUT", &url).body(data);
        for (k, v) in signed {
            req = req.header(k, v);
        }
        let resp = self.backend.client()?.execute(req).await?;
        if !resp.is_success() {
            return Err(SFError::Adapter {
                provider: "s3".to_string(),
                message: format!(
                    "PUT part {part_number} of {} failed: {}",
                    self.key, resp.status
                ),
            });
        }
        let etag = resp
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
            .map(|(_, v)| v.clone())
            .unwrap_or(Default::default());
        self.etags.push(etag);
        Ok(())
    }
}

#[async_trait]
impl cog_core::ObjectWrite for S3Write {
    async fn write(&mut self, chunk: &[u8]) -> SFResult<()> {
        self.buffer.extend_from_slice(chunk);
        while self.buffer.len() >= S3_PART_BYTES {
            self.flush_part_of(S3_PART_BYTES).await?;
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> SFResult<String> {
        self.flush_part().await?;
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (index, etag) in self.etags.iter().enumerate() {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                index + 1,
                etag
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let body = xml.into_bytes();
        let hash = S3ObjectBackend::sha256_hex(&body);
        let canonical = self.backend.canonical_uri(&self.key);
        let query = format!("uploadId={}", percent_encode(&self.upload_id));
        let extra = vec![
            ("content-length".to_string(), body.len().to_string()),
            ("x-amz-content-sha256".to_string(), hash.clone()),
        ];
        let signed = self
            .backend
            .sign_request("POST", &canonical, &query, &hash, &extra);
        let url = format!("{}?{}", self.backend.object_url(&self.key), query);
        let mut req = HttpRequest::new("POST", &url).body(body);
        for (k, v) in signed {
            req = req.header(k, v);
        }
        let resp = self.backend.client()?.execute(req).await?;
        if !resp.is_success() {
            return Err(SFError::Adapter {
                provider: "s3".to_string(),
                message: format!("complete {} failed: {}", self.key, resp.status),
            });
        }
        Ok(format!("s3://{}/{}", self.backend.bucket, self.key))
    }

    async fn abort(self: Box<Self>) -> SFResult<()> {
        // 不发 abort，S3 上会留一个永远不会被清理的分块上传，它占的空间在任何读数里
        // 都看不见。
        let canonical = self.backend.canonical_uri(&self.key);
        let query = format!("uploadId={}", percent_encode(&self.upload_id));
        let empty = S3ObjectBackend::sha256_hex(&[]);
        let signed = self
            .backend
            .sign_request("DELETE", &canonical, &query, &empty, &[]);
        let url = format!("{}?{}", self.backend.object_url(&self.key), query);
        let mut req = HttpRequest::new("DELETE", &url);
        for (k, v) in signed {
            req = req.header(k, v);
        }
        cog_core::ignore_lost!(self.backend.client()?.execute(req).await);
        Ok(())
    }
}

#[async_trait]
impl ObjectBackend for S3ObjectBackend {
    async fn ensure_ready(&self) -> SFResult<()> {
        self.ensure_bucket().await
    }

    async fn open_write(&self, key: &str) -> SFResult<Box<dyn cog_core::ObjectWrite>> {
        // initiate：`POST /key?uploads` 换回一个 `UploadId`。
        let canonical = self.canonical_uri(key);
        let query = "uploads=".to_string();
        let empty = Self::sha256_hex(&[]);
        let signed = self.sign_request("POST", &canonical, &query, &empty, &[]);
        let url = format!("{}?uploads", self.object_url(key));
        let mut req = HttpRequest::new("POST", &url);
        for (k, v) in signed {
            req = req.header(k, v);
        }
        let resp = self.client()?.execute(req).await?;
        if !resp.is_success() {
            return Err(SFError::Adapter {
                provider: "s3".to_string(),
                message: format!("initiate multipart for {key} failed: {}", resp.status),
            });
        }
        let body = resp.text().unwrap_or(Default::default());
        let upload_id = body
            .split("<UploadId>")
            .nth(1)
            .and_then(|rest| rest.split("</UploadId>").next())
            .map(str::to_string)
            .ok_or_else(|| SFError::Adapter {
                provider: "s3".to_string(),
                message: format!("initiate multipart for {key} returned no UploadId: {body}"),
            })?;
        Ok(Box::new(S3Write {
            backend: self.clone(),
            key: key.to_string(),
            upload_id,
            buffer: Vec::new(),
            etags: Vec::new(),
        }))
    }

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
            // cap: fixed — a page-count ceiling so a cursor that keeps truncating cannot spin forever
            // keep: 删掉后一个一直截断的游标永远转下去，一次列举不结束；由「a page-count ceiling」兜住
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

    /// 分块写进去的对象，等于把那些块逐字节拼起来；封口之前读者看不到它。
    ///
    /// 这条用例判的是契约那两句话：`write` 进来的字节不合并、不切分，`finish` 之后
    /// 才存在。**先写 `<path>.partial`、封口才改名**是「不存在半段」的落地方式，所以
    /// 第三句断言看的是那个中间名在封口后消失。
    #[tokio::test]
    async fn a_streamed_object_is_the_concatenation_of_its_chunks() {
        let dir = tempfile::tempdir().expect("temp dir");
        let backend = FileObjectBackend::new(dir.path());
        let mut writer = backend.open_write("a/b.bin").await.expect("open");

        let chunks: [&[u8]; 3] = [b"first ", b"second ", b"third"];
        for chunk in chunks {
            writer.write(chunk).await.expect("a chunk must land");
        }
        assert!(
            backend.get("a/b.bin").await.expect("get").is_none(),
            "封口之前读者不该看到半段"
        );

        let uri = writer.finish().await.expect("finish");
        assert!(uri.starts_with("file://"), "{uri}");
        let got = backend.get("a/b.bin").await.expect("get").expect("exists");
        assert_eq!(got, b"first second third".to_vec());
        assert!(
            !dir.path().join("a/b.partial").exists(),
            "封口之后中间名要收掉"
        );
    }

    /// 放弃的那一段不留痕：`abort` 之后按 key 读不到，中间名也不在。
    #[tokio::test]
    async fn an_aborted_write_leaves_nothing_behind() {
        let dir = tempfile::tempdir().expect("temp dir");
        let backend = FileObjectBackend::new(dir.path());
        let mut writer = backend.open_write("c.bin").await.expect("open");
        writer.write(b"half a body").await.expect("write");
        writer.abort().await.expect("abort");
        assert!(backend.get("c.bin").await.expect("get").is_none());
        assert!(!dir.path().join("c.partial").exists());
    }

    /// 一个只答桶级 HEAD/PUT 的存储：用来判「桶不在就先建、在就不再建、建不成要说
    /// 得出来」。引用能不能取回全文取决于桶是否存在，所以这三件事各是一条判据。
    #[derive(Debug)]
    struct BucketHttp {
        head_status: u16,
        put_status: u16,
        seen: Mutex<Vec<(String, String)>>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for BucketHttp {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            let status = if req.method == "HEAD" {
                self.head_status
            } else {
                self.put_status
            };
            self.seen
                .lock()
                .unwrap()
                .push((req.method.clone(), req.url.clone()));
            Ok(cog_core::HttpResponse {
                status,
                headers: Default::default(),
                body: Vec::new(),
            })
        }
    }

    fn bucket_backend(http: Arc<BucketHttp>) -> S3ObjectBackend {
        S3ObjectBackend::new("http://s3.example", "us-east-1", "cogneva", "ak", "sk")
            .with_client(http)
    }

    /// 桶不存在：先问 HEAD，再一次 PUT 把它建出来。
    #[tokio::test]
    async fn an_absent_bucket_is_created_before_it_is_used() {
        let http = Arc::new(BucketHttp {
            head_status: 404,
            put_status: 200,
            seen: Mutex::new(Vec::new()),
        });
        bucket_backend(http.clone())
            .ensure_bucket()
            .await
            .expect("404 then 200 must create the bucket");
        let seen = http.seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>(),
            vec!["HEAD", "PUT"],
            "先问在不在，再建；两条都打在桶上"
        );
        assert!(seen[0].1.ends_with("/cogneva"), "{}", seen[0].1);
        assert!(seen[1].1.ends_with("/cogneva"), "{}", seen[1].1);
    }

    /// 桶已经在：只问一次，不再建。
    #[tokio::test]
    async fn an_existing_bucket_is_not_created_again() {
        let http = Arc::new(BucketHttp {
            head_status: 200,
            put_status: 500,
            seen: Mutex::new(Vec::new()),
        });
        bucket_backend(http.clone())
            .ensure_bucket()
            .await
            .expect("an existing bucket needs no write");
        let seen = http.seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>(),
            vec!["HEAD"]
        );
    }

    /// 建不成：错误里点名桶与状态码，别让「引用不可解」只留在日志里的一句猜测上。
    #[tokio::test]
    async fn a_bucket_that_cannot_be_created_is_an_error() {
        let http = Arc::new(BucketHttp {
            head_status: 404,
            put_status: 403,
            seen: Mutex::new(Vec::new()),
        });
        let error = bucket_backend(http)
            .ensure_bucket()
            .await
            .expect_err("a refused creation must not read as ready");
        let message = error.to_string();
        assert!(message.contains("cogneva"), "{message}");
        assert!(message.contains("403"), "{message}");
    }

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
            .unwrap_or(Default::default());
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
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
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
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
    }
    /// 会说分块上传那套协议的替身：initiate 给一个 UploadId，每段 PUT 收下正文，
    /// complete 收 XML。断言用它的记录，不用真 S3——**签名仍然是真算的**，因为门
    /// 走的是 `S3ObjectBackend` 自己的签名路径，只是请求不真的出去。
    #[derive(Default, Debug)]
    struct MultipartHttp {
        parts: Mutex<Vec<(String, Vec<u8>)>>,
        completed: Mutex<Option<String>>,
        aborted: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for MultipartHttp {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            let method = req.method.to_uppercase();
            if method == "POST" && req.url.contains("uploads") {
                return Ok(cog_core::HttpResponse {
                    status: 200,
                    headers: std::collections::HashMap::new(),
                    body: b"<InitiateMultipartUploadResult><UploadId>UPLOAD-1</UploadId></InitiateMultipartUploadResult>".to_vec(),
                });
            }
            if method == "PUT" {
                self.parts
                    .lock()
                    .unwrap()
                    .push((req.url.clone(), req.body.clone().unwrap_or_default()));
                return Ok(cog_core::HttpResponse {
                    status: 200,
                    headers: std::collections::HashMap::from([(
                        "ETag".to_string(),
                        "\"etag\"".to_string(),
                    )]),
                    body: Vec::new(),
                });
            }
            if method == "POST" && req.url.contains("uploadId") {
                *self.completed.lock().unwrap() =
                    Some(String::from_utf8_lossy(&req.body.unwrap_or_default()).into_owned());
                return Ok(cog_core::HttpResponse {
                    status: 200,
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                });
            }
            if method == "DELETE" {
                *self.aborted.lock().unwrap() = true;
                return Ok(cog_core::HttpResponse {
                    status: 204,
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                });
            }
            Ok(cog_core::HttpResponse {
                status: 404,
                headers: std::collections::HashMap::new(),
                body: Vec::new(),
            })
        }
    }

    /// 分块写进去的对象，等于把那些块逐字节拼起来；每一段都真的发出去了。
    ///
    /// 这是 S50 那条判据的直接答案：「签名与校验在测试里真的跑过一次」——签名走的是
    /// 后端自己的 `sign_request`，替身只收请求，不代替签名。
    #[tokio::test]
    async fn a_multipart_upload_sends_parts_that_concatenate_to_the_object() {
        let http = Arc::new(MultipartHttp::default());
        let backend = S3ObjectBackend::new("http://s3:9000", "us-east-1", "b", "ak", "sk")
            .with_client(http.clone());
        let mut writer = backend.open_write("big.bin").await.expect("initiate");

        let chunk = vec![7u8; 1024 * 1024];
        for _ in 0..3 {
            writer.write(&chunk).await.expect("write a chunk");
        }
        writer.finish().await.expect("complete");

        let parts = http.parts.lock().unwrap().clone();
        assert!(!parts.is_empty(), "一段都没有发出去");
        let joined: Vec<u8> = parts.iter().flat_map(|(_, body)| body.clone()).collect();
        assert_eq!(joined.len(), 3 * 1024 * 1024, "发出去的字节总数不对");
        assert!(joined.iter().all(|b| *b == 7), "内容与写进去的不是同一份");
        let xml = http
            .completed
            .lock()
            .unwrap()
            .clone()
            .expect("没有发 complete");
        assert!(xml.contains("CompleteMultipartUpload"), "{xml}");
    }

    /// 放弃的那一段要发 DELETE：不发的话 S3 上留着一个永远不被清理的上传。
    #[tokio::test]
    async fn aborting_a_multipart_upload_deletes_it() {
        let http = Arc::new(MultipartHttp::default());
        let backend = S3ObjectBackend::new("http://s3:9000", "us-east-1", "b", "ak", "sk")
            .with_client(http.clone());
        let mut writer = backend.open_write("half.bin").await.expect("initiate");
        writer.write(b"half").await.expect("write");
        writer.abort().await.expect("abort");
        assert!(*http.aborted.lock().unwrap(), "放弃之后没有发 DELETE");
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
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
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
        cog_core::ignore_lost!(std::fs::remove_dir_all(&missing));
        assert_eq!(objects_on_disk(&missing).await.unwrap(), 0);

        // A path that is a file, not a directory: something is there that
        // cannot be enumerated, which is not the same fact as an empty tree.
        let (objects, dir) = file_backend("not-a-dir");
        objects.put("file", b"x").await.unwrap();
        assert!(objects_on_disk(&dir.join("file")).await.is_err());
        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
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

        cog_core::ignore_lost!(std::fs::remove_dir_all(&dir));
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
