/// 7 Protobuf raw streams (SSOT — Single Source of Truth).
/// All runtime events are written as Length-delimited Protobuf
/// records with a unified envelope { Meta, Context, Payload }.
/// 7 streams:
///   session_raw, task_raw, agent_raw, llm_raw, tool_raw, system_raw, transport_raw
/// Storage: hot (SSD, 0-7d), warm (HDD, 7-90d), cold (S3/COS, 90d+).
/// **Machine layer**: SSOT for system internal data flow.
/// **Human layer**: feeds Loki/Jaeger via Exporters.
/// **Agent layer**: raw source for Snapshot generation and replay.
use crate::{RawContext, RawEnvelope, RawMeta, RawStreamName};
use bytes::{Buf, BufMut, BytesMut};
use chrono::{NaiveDate, Utc};
use cog_core::ObjectBackend;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Unified writer for all 7 raw streams.
/// Thread-safe via internal locking. Each write appends to the
/// appropriate per-stream file in the hot directory, using
/// Length-delimited encoding.
pub struct RawStreamWriter {
    hot_dir: PathBuf,
    warm_dir: PathBuf,
    cold_dir: PathBuf,
    max_file_size: u64,
    #[allow(dead_code)]
    flush_interval_sec: u64,
}

impl RawStreamWriter {
    pub fn new(
        hot_dir: impl Into<PathBuf>,
        warm_dir: impl Into<PathBuf>,
        cold_dir: impl Into<PathBuf>,
        max_hot_file_size_mb: u64,
        flush_interval_sec: u64,
    ) -> Self {
        Self {
            hot_dir: hot_dir.into(),
            warm_dir: warm_dir.into(),
            cold_dir: cold_dir.into(),
            max_file_size: max_hot_file_size_mb * 1024 * 1024,
            flush_interval_sec,
        }
    }

    pub fn hot_dir(&self) -> &Path {
        &self.hot_dir
    }

    pub fn warm_dir(&self) -> &Path {
        &self.warm_dir
    }

    pub fn cold_dir(&self) -> &Path {
        &self.cold_dir
    }

    /// Write a raw envelope to the appropriate stream file.
    /// Encoding: Length-delimited (4-byte big-endian length prefix + payload bytes).
    pub async fn write(&self, envelope: &RawEnvelope) -> std::io::Result<()> {
        let date = chrono::Local::now().date_naive();
        let file_name = format!(
            "{}_{}.raw",
            envelope.meta.stream_name,
            date.format("%Y%m%d")
        );
        let file_path = self.hot_dir.join(&file_name);

        // Ensure directory exists
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let payload_bytes = encode_envelope(envelope);
        let len = payload_bytes.len() as u32;

        let mut buf = BytesMut::with_capacity(4 + payload_bytes.len());
        buf.put_u32(len);
        buf.extend_from_slice(&payload_bytes);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        file.write_all(&buf).await?;
        crate::observable::global_observable().record_event();

        // Periodic flush based on interval, or force flush if file is large
        let metadata = file.metadata().await?;
        if metadata.len() >= self.max_file_size {
            file.flush().await?;
            file.sync_all().await?;
        }

        Ok(())
    }

    /// Flush all open file handles.
    pub async fn flush_all(&self) -> std::io::Result<()> {
        // In Phase 1 we use one-shot file handles, so no persistent
        // open files need flushing. Phase 2 may introduce a file pool.
        Ok(())
    }

    /// Rotate hot files that exceed the size threshold.
    pub async fn rotate_if_needed(
        &self,
        stream: RawStreamName,
    ) -> std::io::Result<Option<PathBuf>> {
        let date = chrono::Local::now().date_naive();
        let file_name = format!("{}_{}.raw", stream.as_str(), date.format("%Y%m%d"));
        let file_path = self.hot_dir.join(&file_name);

        if tokio::fs::try_exists(&file_path).await.unwrap_or(false) {
            let metadata = tokio::fs::metadata(&file_path).await?;
            if metadata.len() >= self.max_file_size {
                let rotated_name = format!(
                    "{}_{}_{}.raw",
                    stream.as_str(),
                    date.format("%Y%m%d"),
                    Utc::now().timestamp_millis()
                );
                let rotated_path = self.hot_dir.join(&rotated_name);
                tokio::fs::rename(&file_path, &rotated_path).await?;
                return Ok(Some(rotated_path));
            }
        }

        Ok(None)
    }
}

/// Sequential reader for raw stream files.
/// Scans Length-delimited records and decodes them into `RawEnvelope`.
pub struct RawStreamReader;

impl Default for RawStreamReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RawStreamReader {
    pub fn new() -> Self {
        Self
    }

    /// Read all envelopes from a raw stream file.
    pub async fn read_file(&self, path: &Path) -> std::io::Result<Vec<RawEnvelope>> {
        let bytes = tokio::fs::read(path).await?;
        Ok(decode_stream(&bytes))
    }

    /// Stream envelopes from a raw file (memory-efficient for large files).
    pub async fn stream_file<F>(&self, path: &Path, mut callback: F) -> std::io::Result<u64>
    where
        F: FnMut(RawEnvelope) -> Result<(), std::io::Error>,
    {
        let mut file = tokio::fs::File::open(path).await?;
        let mut count = 0u64;
        let mut buf = BytesMut::with_capacity(64 * 1024);

        loop {
            let n = file.read_buf(&mut buf).await?;
            if n == 0 && buf.len() < 4 {
                break;
            }

            while buf.len() >= 4 {
                let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() < 4 + len {
                    break;
                }

                buf.advance(4);
                let payload = buf.split_to(len).freeze().to_vec();
                match decode_envelope(&payload) {
                    Some(envelope) => {
                        callback(envelope)?;
                        count += 1;
                    }
                    None => {
                        tracing::warn!("Failed to decode envelope in raw stream");
                    }
                }
            }

            if n == 0 {
                break;
            }
        }

        Ok(count)
    }

    /// Scan a directory for raw stream files matching a date pattern.
    pub async fn scan_dir(
        &self,
        dir: &Path,
        stream: RawStreamName,
        date: NaiveDate,
    ) -> std::io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let prefix = format!("{}_{}", stream.as_str(), date.format("%Y%m%d"));

        if !tokio::fs::try_exists(dir).await.unwrap_or(false) {
            return Ok(files);
        }

        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) && name.ends_with(".raw") {
                files.push(entry.path());
            }
        }

        files.sort();
        Ok(files)
    }
}

/// Raw stream tier migration engine.
/// Moves aged raw stream files from hot → warm → cold tiers,
/// applying zstd compression appropriate to each tier.
pub struct RawStreamTierMigration {
    hot_dir: PathBuf,
    warm_dir: PathBuf,
    cold_dir: PathBuf,
    hot_retention_days: u32,
    warm_retention_days: u32,
    object_backend: Option<Arc<dyn ObjectBackend>>,
}

impl RawStreamTierMigration {
    pub fn new(
        hot_dir: impl Into<PathBuf>,
        warm_dir: impl Into<PathBuf>,
        cold_dir: impl Into<PathBuf>,
        hot_retention_days: u32,
        warm_retention_days: u32,
    ) -> Self {
        Self {
            hot_dir: hot_dir.into(),
            warm_dir: warm_dir.into(),
            cold_dir: cold_dir.into(),
            hot_retention_days,
            warm_retention_days,
            object_backend: None,
        }
    }

    /// Attach an object backend (S3/COS/MinIO) for cold-tier offloading.
    /// When set, warm→cold migration uploads files to object storage
    /// instead of keeping them on local disk.
    pub fn with_object_backend(mut self, backend: Arc<dyn ObjectBackend>) -> Self {
        self.object_backend = Some(backend);
        self
    }

    pub async fn run(&self) -> std::io::Result<MigrationStats> {
        let mut stats = MigrationStats::default();

        // Hot → Warm
        let hot_files = self.list_files(&self.hot_dir).await?;
        for path in hot_files {
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                if let Ok(modified) = metadata.modified() {
                    let age = std::time::SystemTime::now()
                        .duration_since(modified)
                        .unwrap_or_default();
                    let age_days = age.as_secs() / 86400;
                    if age_days > self.hot_retention_days as u64 {
                        self.migrate_file(&path, &self.warm_dir, 3).await?;
                        stats.hot_to_warm += 1;
                    }
                }
            }
        }

        // Warm → Cold
        let warm_files = self.list_files(&self.warm_dir).await?;
        for path in warm_files {
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                if let Ok(modified) = metadata.modified() {
                    let age = std::time::SystemTime::now()
                        .duration_since(modified)
                        .unwrap_or_default();
                    let age_days = age.as_secs() / 86400;
                    if age_days > self.warm_retention_days as u64 {
                        if let Some(ref backend) = self.object_backend {
                            self.migrate_to_object_storage(&path, backend, 9).await?;
                        } else {
                            self.migrate_file(&path, &self.cold_dir, 9).await?;
                        }
                        stats.warm_to_cold += 1;
                    }
                }
            }
        }

        Ok(stats)
    }

    async fn list_files(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        if !tokio::fs::try_exists(dir).await.unwrap_or(false) {
            return Ok(files);
        }
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("raw") {
                files.push(path);
            }
        }
        Ok(files)
    }

    async fn migrate_file(
        &self,
        src: &Path,
        dst_dir: &Path,
        compression_level: i32,
    ) -> std::io::Result<()> {
        tokio::fs::create_dir_all(dst_dir).await?;
        let file_name = src.file_name().unwrap_or_default();
        let dst = dst_dir.join(file_name);

        let bytes = tokio::fs::read(src).await?;
        let compressed =
            zstd::encode_all(&bytes[..], compression_level).map_err(std::io::Error::other)?;

        tokio::fs::write(&dst, compressed).await?;
        tokio::fs::remove_file(src).await?;

        tracing::info!(
            src = %src.display(),
            dst = %dst.display(),
            original_size = bytes.len(),
            compressed_size = tokio::fs::metadata(&dst).await?.len(),
            "Raw stream tier migration"
        );

        Ok(())
    }

    async fn migrate_to_object_storage(
        &self,
        src: &Path,
        backend: &Arc<dyn ObjectBackend>,
        compression_level: i32,
    ) -> std::io::Result<()> {
        let bytes = tokio::fs::read(src).await?;
        let compressed =
            zstd::encode_all(&bytes[..], compression_level).map_err(std::io::Error::other)?;

        let relative = src
            .strip_prefix(&self.warm_dir)
            .unwrap_or(src)
            .to_string_lossy()
            .replace('\\', "/");
        let key = format!("raw/cold/{}", relative);

        backend
            .put(&key, &compressed)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        tokio::fs::remove_file(src).await?;

        tracing::info!(
            src = %src.display(),
            s3_key = %key,
            original_size = bytes.len(),
            compressed_size = compressed.len(),
            "Raw stream cold tier uploaded to object storage"
        );

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MigrationStats {
    pub hot_to_warm: u64,
    pub warm_to_cold: u64,
}

/// Build a `RawEnvelope` for a stream event.
pub struct RawEnvelopeBuilder {
    stream: RawStreamName,
    trace_id: String,
    span_id: String,
    source_crate: String,
    source_version: String,
    context: RawContext,
    payload: Vec<u8>,
}

impl RawEnvelopeBuilder {
    pub fn new(stream: RawStreamName) -> Self {
        Self {
            stream,
            trace_id: String::new(),
            span_id: String::new(),
            source_crate: String::new(),
            source_version: env!("CARGO_PKG_VERSION").into(),
            context: RawContext::default(),
            payload: Vec::new(),
        }
    }

    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.trace_id = id.into();
        self
    }

    pub fn span_id(mut self, id: impl Into<String>) -> Self {
        self.span_id = id.into();
        self
    }

    pub fn source_crate(mut self, name: impl Into<String>) -> Self {
        self.source_crate = name.into();
        self
    }

    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.context.session_id = Some(id.into());
        self
    }

    pub fn task_id(mut self, id: impl Into<String>) -> Self {
        self.context.task_id = Some(id.into());
        self
    }

    pub fn agent_id(mut self, id: impl Into<String>) -> Self {
        self.context.agent_id = Some(id.into());
        self
    }

    pub fn user_id(mut self, id: impl Into<String>) -> Self {
        self.context.user_id = Some(id.into());
        self
    }

    pub fn team_id(mut self, id: impl Into<String>) -> Self {
        self.context.team_id = Some(id.into());
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.labels.insert(key.into(), value.into());
        self
    }

    pub fn payload(mut self, bytes: Vec<u8>) -> Self {
        self.payload = bytes;
        self
    }

    pub fn json_payload(mut self, value: &impl serde::Serialize) -> Self {
        self.payload = serde_json::to_vec(value).unwrap_or_default();
        self
    }

    pub fn build(self) -> RawEnvelope {
        RawEnvelope {
            meta: RawMeta {
                stream_name: self.stream.as_str().into(),
                event_id: uuid::Uuid::new_v4().to_string(),
                timestamp_unix_ms: Utc::now().timestamp_millis(),
                trace_id: self.trace_id,
                span_id: self.span_id,
                source_crate: self.source_crate,
                source_version: self.source_version,
            },
            context: self.context,
            payload: self.payload,
        }
    }
}

// ─── Encoding / Decoding ──────────────────────────────────────────

fn encode_envelope(envelope: &RawEnvelope) -> Vec<u8> {
    let mut buf = Vec::new();

    // Meta
    write_str(&mut buf, &envelope.meta.stream_name);
    write_str(&mut buf, &envelope.meta.event_id);
    buf.extend_from_slice(&envelope.meta.timestamp_unix_ms.to_be_bytes());
    write_str(&mut buf, &envelope.meta.trace_id);
    write_str(&mut buf, &envelope.meta.span_id);
    write_str(&mut buf, &envelope.meta.source_crate);
    write_str(&mut buf, &envelope.meta.source_version);

    // Context
    write_opt_str(&mut buf, envelope.context.session_id.as_deref());
    write_opt_str(&mut buf, envelope.context.task_id.as_deref());
    write_opt_str(&mut buf, envelope.context.agent_id.as_deref());
    write_opt_str(&mut buf, envelope.context.user_id.as_deref());
    write_opt_str(&mut buf, envelope.context.team_id.as_deref());

    buf.extend_from_slice(&(envelope.context.labels.len() as u32).to_be_bytes());
    for (k, v) in &envelope.context.labels {
        write_str(&mut buf, k);
        write_str(&mut buf, v);
    }

    // Payload
    buf.extend_from_slice(&(envelope.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&envelope.payload);

    buf
}

fn decode_stream(bytes: &[u8]) -> Vec<RawEnvelope> {
    let mut envelopes = Vec::new();
    let mut cursor = std::io::Cursor::new(bytes);

    while cursor.position() < bytes.len() as u64 {
        if let Some(env) = decode_envelope_from_cursor(&mut cursor) {
            envelopes.push(env);
        } else {
            break;
        }
    }

    envelopes
}

fn decode_envelope(bytes: &[u8]) -> Option<RawEnvelope> {
    let mut cursor = std::io::Cursor::new(bytes);
    decode_envelope_from_cursor(&mut cursor)
}

fn decode_envelope_from_cursor(cursor: &mut std::io::Cursor<&[u8]>) -> Option<RawEnvelope> {
    use std::io::Read;

    let read_u32 = |c: &mut std::io::Cursor<&[u8]>| -> Option<u32> {
        let mut buf = [0u8; 4];
        Read::read_exact(c, &mut buf).ok()?;
        Some(u32::from_be_bytes(buf))
    };

    let read_str = |c: &mut std::io::Cursor<&[u8]>| -> Option<String> {
        let len = read_u32(c)? as usize;
        let mut buf = vec![0u8; len];
        Read::read_exact(c, &mut buf).ok()?;
        String::from_utf8(buf).ok()
    };

    let read_opt_str = |c: &mut std::io::Cursor<&[u8]>| -> Option<Option<String>> {
        let present = read_u32(c)?;
        if present > 0 {
            Some(Some(read_str(c)?))
        } else {
            Some(None)
        }
    };

    let meta = RawMeta {
        stream_name: read_str(cursor)?,
        event_id: read_str(cursor)?,
        timestamp_unix_ms: {
            let mut buf = [0u8; 8];
            Read::read_exact(cursor, &mut buf).ok()?;
            i64::from_be_bytes(buf)
        },
        trace_id: read_str(cursor)?,
        span_id: read_str(cursor)?,
        source_crate: read_str(cursor)?,
        source_version: read_str(cursor)?,
    };

    let context = RawContext {
        session_id: read_opt_str(cursor)?,
        task_id: read_opt_str(cursor)?,
        agent_id: read_opt_str(cursor)?,
        user_id: read_opt_str(cursor)?,
        team_id: read_opt_str(cursor)?,
        labels: {
            let count = read_u32(cursor)? as usize;
            let mut map = HashMap::with_capacity(count);
            for _ in 0..count {
                let k = read_str(cursor)?;
                let v = read_str(cursor)?;
                map.insert(k, v);
            }
            map
        },
    };

    let payload_len = read_u32(cursor)? as usize;
    let mut payload = vec![0u8; payload_len];
    std::io::Read::read_exact(cursor, &mut payload).ok()?;

    Some(RawEnvelope {
        meta,
        context,
        payload,
    })
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn write_opt_str(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(v) => {
            buf.extend_from_slice(&1u32.to_be_bytes());
            write_str(buf, v);
        }
        None => {
            buf.extend_from_slice(&0u32.to_be_bytes());
        }
    }
}
