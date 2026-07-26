use async_trait::async_trait;
use cog_core::{WalBackend, WalCodec, WalError, WalRecord};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;

/// File-based WAL backend.
/// Stores records as length-delimited protobuf (`.wal.bin`) per session.
/// Each session has its own file: `{base_dir}/{session_id}.wal.bin`.
/// **Backward compatibility**: reads also support legacy `.wal.jsonl` files.
/// When a `.wal.bin` file exists it takes precedence; otherwise the backend
/// falls back to `.wal.jsonl`.  Writes always go to `.wal.bin`.
/// On-disk format (protobuf):
///   <varint length><WalRecord proto bytes><varint length><WalRecord proto bytes>...
/// Legacy format (JSONL):
///   {json}\n{json}\n...
#[derive(Debug)]
pub struct FileWalBackend {
    base_dir: PathBuf,
    codec: Arc<dyn WalCodec>,
    /// Maximum file size in bytes before rotating to a new segment.
    /// `None` disables rotation (default).
    max_file_size_bytes: Option<u64>,
}

impl FileWalBackend {
    pub fn new(base_dir: impl Into<PathBuf>, codec: Arc<dyn WalCodec>) -> Self {
        Self {
            base_dir: base_dir.into(),
            codec,
            max_file_size_bytes: None,
        }
    }

    /// Enable file rotation when the current WAL file exceeds `bytes`.
    pub fn with_max_file_size(mut self, bytes: u64) -> Self {
        self.max_file_size_bytes = Some(bytes);
        self
    }

    fn session_path_bin(&self, session_id: &str) -> PathBuf {
        self.base_dir.join(format!("{}.wal.bin", session_id))
    }

    fn session_path_jsonl(&self, session_id: &str) -> PathBuf {
        self.base_dir.join(format!("{}.wal.jsonl", session_id))
    }

    fn archive_path(&self, session_id: &str, idx: u32) -> PathBuf {
        self.base_dir
            .join(format!("{}.wal.bin.{:03}", session_id, idx))
    }

    /// Detect which file exists: prefer `.wal.bin`, fall back to `.wal.jsonl`.
    fn resolve_path(&self, session_id: &str) -> Option<PathBuf> {
        let bin = self.session_path_bin(session_id);
        if bin.exists() {
            return Some(bin);
        }
        let jsonl = self.session_path_jsonl(session_id);
        if jsonl.exists() {
            return Some(jsonl);
        }
        None
    }

    /// List all WAL files for a session, ordered from oldest to newest.
    /// Returns `(archive_paths, current_path_option)`.
    fn list_session_files(&self, session_id: &str) -> (Vec<PathBuf>, Option<PathBuf>) {
        let mut archives = Vec::new();
        let mut idx = 1u32;
        loop {
            let path = self.archive_path(session_id, idx);
            if path.exists() {
                archives.push(path);
                idx += 1;
            } else {
                break;
            }
        }
        let current = self.resolve_path(session_id);
        (archives, current)
    }

    /// Rotate the current WAL file to an archive segment.
    async fn rotate_file(&self, session_id: &str) -> Result<(), WalError> {
        let current = self.session_path_bin(session_id);
        if !current.exists() {
            return Ok(());
        }
        let mut idx = 1u32;
        while self.archive_path(session_id, idx).exists() {
            idx += 1;
        }
        let target = self.archive_path(session_id, idx);
        tokio::fs::rename(&current, &target)
            .await
            .map_err(WalError::Io)?;
        Ok(())
    }

    /// Read records from a single file (protobuf or legacy jsonl).
    async fn read_file_records(
        &self,
        path: &PathBuf,
        min_seq: u64,
    ) -> Result<Vec<WalRecord>, WalError> {
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            let content = tokio::fs::read_to_string(path)
                .await
                .map_err(WalError::Io)?;
            let mut records = Vec::new();
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let record = WalRecord::decode_from_json_line(line)?;
                if record.seq >= min_seq {
                    records.push(record);
                }
            }
            Ok(records)
        } else {
            let bytes = tokio::fs::read(path).await.map_err(WalError::Io)?;
            let mut records = Vec::new();
            let mut offset = 0usize;
            while offset < bytes.len() {
                let (record, consumed) = self.codec.decode_length_delimited(&bytes[offset..])?;
                if record.seq >= min_seq {
                    records.push(record);
                }
                offset += consumed;
            }
            Ok(records)
        }
    }

    /// Write records to a single file (always protobuf length-delimited).
    async fn write_file_records(
        &self,
        path: &PathBuf,
        records: &[WalRecord],
    ) -> Result<(), WalError> {
        let mut buf = Vec::new();
        for record in records {
            let encoded = self.codec.encode_length_delimited(record)?;
            buf.extend_from_slice(&encoded);
        }
        tokio::fs::write(path, buf).await.map_err(WalError::Io)?;
        Ok(())
    }
}

#[async_trait]
impl WalBackend for FileWalBackend {
    async fn append(&self, record: WalRecord) -> Result<u64, WalError> {
        let path = self.session_path_bin(&record.session_id);

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(WalError::Io)?;
        }

        // Rotate if the current file exceeds the size threshold
        if let Some(max_size) = self.max_file_size_bytes {
            if let Ok(meta) = tokio::fs::metadata(&path).await {
                if meta.len() >= max_size {
                    self.rotate_file(&record.session_id).await?;
                }
            }
        }

        let buf = self.codec.encode_length_delimited(&record)?;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(WalError::Io)?;

        file.write_all(&buf).await.map_err(WalError::Io)?;
        file.sync_data().await.map_err(WalError::Io)?;

        Ok(record.seq)
    }

    async fn read_since(&self, session_id: &str, seq: u64) -> Result<Vec<WalRecord>, WalError> {
        let (archives, current) = self.list_session_files(session_id);
        let mut records = Vec::new();
        for path in archives {
            records.extend(self.read_file_records(&path, seq).await?);
        }
        if let Some(path) = current {
            records.extend(self.read_file_records(&path, seq).await?);
        }
        Ok(records)
    }

    async fn read_latest(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<WalRecord>, WalError> {
        let mut records = self.read_since(session_id, 0).await?;
        if records.len() > limit {
            records.drain(0..records.len() - limit);
        }
        Ok(records)
    }

    async fn truncate_before(&self, session_id: &str, seq: u64) -> Result<(), WalError> {
        let (archives, current) = self.list_session_files(session_id);

        for path in archives {
            let all_records = self.read_file_records(&path, 0).await?;
            if let Some(last) = all_records.last() {
                if last.seq < seq {
                    // Entire archive is before seq — delete it
                    tokio::fs::remove_file(&path).await.map_err(WalError::Io)?;
                    continue;
                }
            }
            let kept: Vec<_> = all_records.into_iter().filter(|r| r.seq >= seq).collect();
            self.write_file_records(&path, &kept).await?;
        }

        if let Some(path) = current {
            let kept = self.read_file_records(&path, seq).await?;
            self.write_file_records(&path, &kept).await?;
        }

        Ok(())
    }

    async fn next_seq(&self, session_id: &str) -> Result<u64, WalError> {
        let records = self.read_since(session_id, 0).await?;
        Ok(records.last().map(|r| r.seq + 1).unwrap_or(0))
    }
}

/// In-memory WAL backend for testing.
/// Stores records in a per-session Vec. Not durable — use only in tests.
#[derive(Debug, Default)]
pub struct MemoryWalBackend {
    sessions: Mutex<HashMap<String, Vec<WalRecord>>>,
}

impl MemoryWalBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl WalBackend for MemoryWalBackend {
    async fn append(&self, record: WalRecord) -> Result<u64, WalError> {
        let mut sessions = self.sessions.lock().unwrap();
        let entries = sessions.entry(record.session_id.clone()).or_default();
        entries.push(record.clone());
        Ok(record.seq)
    }

    async fn read_since(&self, session_id: &str, seq: u64) -> Result<Vec<WalRecord>, WalError> {
        let sessions = self.sessions.lock().unwrap();
        let records = sessions
            .get(session_id)
            .map(|v| v.iter().filter(|r| r.seq >= seq).cloned().collect())
            .unwrap_or_default();
        Ok(records)
    }

    async fn read_latest(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<WalRecord>, WalError> {
        let sessions = self.sessions.lock().unwrap();
        let records = sessions.get(session_id).cloned().unwrap_or_default();
        let start = records.len().saturating_sub(limit);
        Ok(records[start..].to_vec())
    }

    async fn truncate_before(&self, session_id: &str, seq: u64) -> Result<(), WalError> {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(entries) = sessions.get_mut(session_id) {
            entries.retain(|r| r.seq >= seq);
        }
        Ok(())
    }

    async fn next_seq(&self, session_id: &str) -> Result<u64, WalError> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions
            .get(session_id)
            .and_then(|v| v.last().map(|r| r.seq + 1))
            .unwrap_or(0))
    }
}

/// Redis-based WAL backend.
/// Uses Redis Lists for ordered storage of WAL records per session.
/// Each session maps to a Redis List key: `wal:{session_id}`.
#[derive(Debug, Clone)]
pub struct RedisWalBackend {
    client: redis::aio::MultiplexedConnection,
    /// Maximum number of entries per session list.
    /// When exceeded, older entries are trimmed via LTRIM.
    /// `None` disables auto-trim (default).
    max_entries: Option<usize>,
}

impl RedisWalBackend {
    pub async fn new(redis_url: &str) -> Result<Self, WalError> {
        let client =
            redis::Client::open(redis_url).map_err(|e| WalError::Backend(e.to_string()))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| WalError::Backend(e.to_string()))?;
        Ok(Self {
            client: conn,
            max_entries: None,
        })
    }

    /// Set the maximum number of entries per session list.
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = Some(max);
        self
    }

    fn key(session_id: &str) -> String {
        format!("wal:{}", session_id)
    }
}

#[async_trait]
impl WalBackend for RedisWalBackend {
    async fn append(&self, record: WalRecord) -> Result<u64, WalError> {
        let key = Self::key(&record.session_id);
        let payload = record.encode_to_json_line()?;
        let mut conn = self.client.clone();
        redis::cmd("RPUSH")
            .arg(&key)
            .arg(&payload)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis RPUSH failed: {}", e)))?;

        // Auto-trim if max_entries is configured
        if let Some(max) = self.max_entries {
            let len: usize = redis::cmd("LLEN")
                .arg(&key)
                .query_async::<usize>(&mut conn)
                .await
                .map_err(|e| WalError::Backend(format!("Redis LLEN failed: {}", e)))?;
            if len > max {
                let trim_start = len - max;
                redis::cmd("LTRIM")
                    .arg(&key)
                    .arg(trim_start as isize)
                    .arg(-1)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| WalError::Backend(format!("Redis LTRIM failed: {}", e)))?;
            }
        }

        Ok(record.seq)
    }

    async fn read_since(&self, session_id: &str, seq: u64) -> Result<Vec<WalRecord>, WalError> {
        let key = Self::key(session_id);
        let mut conn = self.client.clone();
        let items: Vec<String> = redis::cmd("LRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .query_async::<Vec<String>>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LRANGE failed: {}", e)))?;

        let mut records = Vec::new();
        for (idx, line) in items.into_iter().enumerate() {
            let record = WalRecord::decode_from_json_line(&line)?;
            // seq 对应 list index，从 0 开始
            if idx as u64 >= seq {
                records.push(record);
            }
        }
        Ok(records)
    }

    async fn read_latest(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<WalRecord>, WalError> {
        let key = Self::key(session_id);
        let mut conn = self.client.clone();
        let items: Vec<String> = redis::cmd("LRANGE")
            .arg(&key)
            .arg(-(limit as isize))
            .arg(-1)
            .query_async::<Vec<String>>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LRANGE failed: {}", e)))?;

        let mut records = Vec::new();
        for line in items {
            records.push(WalRecord::decode_from_json_line(&line)?);
        }
        Ok(records)
    }

    async fn truncate_before(&self, session_id: &str, seq: u64) -> Result<(), WalError> {
        let key = Self::key(session_id);
        let mut conn = self.client.clone();
        // LTRIM keeps elements from start to stop (inclusive)
        // We want to keep elements from seq onwards, so start = seq, stop = -1
        redis::cmd("LTRIM")
            .arg(&key)
            .arg(seq as isize)
            .arg(-1)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LTRIM failed: {}", e)))?;
        Ok(())
    }

    async fn next_seq(&self, session_id: &str) -> Result<u64, WalError> {
        let key = Self::key(session_id);
        let mut conn = self.client.clone();
        let len: usize = redis::cmd("LLEN")
            .arg(&key)
            .query_async::<usize>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LLEN failed: {}", e)))?;
        Ok(len as u64)
    }
}
