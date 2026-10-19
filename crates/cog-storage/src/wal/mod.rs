use async_trait::async_trait;
use cog_core::{WalBackend, WalCodec, WalError, WalRecord};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// File-based WAL backend.
/// Stores records as length-delimited protobuf (`.wal.bin`) per session.
/// Each session has its own file: `{base_dir}/{session_id}.wal.bin`.
/// **Backward compatibility**: reads also support legacy `.wal.jsonl` files.
/// When a `.wal.bin` file exists it takes precedence; otherwise the backend
/// falls back to `.wal.jsonl`.  Writes always go to `.wal.bin`.
/// On-disk format (protobuf):
///   `<varint length><WalRecord proto bytes>` repeated end-to-end.
/// Legacy format (JSONL):
///   `{json}\n{json}\n...`
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

/// How many records one session's log may keep before the oldest are dropped.
///
/// The store has no other bound: `wal:{id}` is appended to for as long as an
/// agent lives and is never deleted, so without a cap the list grows with the
/// agent's lifetime. Measured 2026-10-19 on the live deployment: 3,592 session
/// logs holding 1,549,847 records (733 MB against a 2 GiB container limit whose
/// `maxmemory` is 0), with a distribution of p50=28, p90=135, p95=2,637,
/// p99=9,145, max=35,389 records. The tail is what carries the bytes.
///
/// 5,000 sits above p95, so a log that is behaving like the population is never
/// touched, while the 1% of logs past 9,000 records are cut back to about 2.4 MB
/// each (records average 473 B). Trimming on this bound removes 28% of the
/// records held today.
///
/// What it costs: a replay whose checkpoint offset predates the retained head
/// loses the events before it. That is a stated bound rather than a silent one
/// — `read_since` reports the gap when the requested sequence is older than the
/// oldest record still held. Raising this number keeps more history; lowering
/// it trades history for memory. Neither choice removes the logs themselves;
/// that is the retirement path, which is a separate change.
pub const WAL_MAX_ENTRIES_PER_SESSION: usize = 5_000;

/// How long the sweep waits between passes over the store.
///
/// Ten minutes, not a control period: the write path already caps every log
/// that is still being appended to, so a pass only has work to do for logs
/// whose session ended while over the cap, and that population changes when
/// sessions end rather than on any clock. What the period really buys is a
/// bounded delay between a session ending and its log giving the memory back.
pub const WAL_CAP_SWEEP_PERIOD: Duration = Duration::from_secs(600);

/// The name this loop registers under in the process's loop readings.
pub const WAL_CAP_SWEEP_LOOP: &str = "storage_wal_cap_sweep";

/// What one pass over the store did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrimOutcome {
    /// Logs the pass brought down to the cap.
    pub trimmed: u64,
    /// Records those logs gave up.
    pub released: u64,
    /// The cap the pass applied; `None` when this backend has none.
    pub cap: Option<usize>,
}

/// Redis-based WAL backend.
/// Uses Redis Lists for ordered storage of WAL records per session.
/// Each session maps to a Redis List key: `wal:{session_id}`.
///
/// The sequence number is the record's own `seq` field, not its position in the
/// list. The two agreed only while nothing was ever removed from the head; the
/// moment a trim runs they part ways, and a reader that counts positions would
/// then replay from the wrong record — or, once `next_seq` regressed to the new
/// length, hand out numbers that are already in use. Every read below filters
/// on the field for that reason, and `next_seq` asks the newest record rather
/// than the list.
#[derive(Clone)]
pub struct RedisWalBackend {
    client: redis::aio::ConnectionManager,
    /// Maximum number of entries per session list.
    /// When exceeded, older entries are trimmed via LTRIM.
    /// `None` disables auto-trim (default).
    max_entries: Option<usize>,
}

impl std::fmt::Debug for RedisWalBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The connection manager has no `Debug` of its own: whether one is held
        // is what a reader needs from this type, its socket internals are not.
        f.debug_struct("RedisWalBackend")
            .field("client", &"redis connection")
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

impl RedisWalBackend {
    /// Build over an already-open connection.
    ///
    /// A process keeps one shared Redis manager, so a caller that has already
    /// opened one hands it here rather than opening a second. Same shape as the
    /// dead-letter queue and the agent registry, which share the connection the
    /// plugin opened once.
    pub fn from_connection(client: redis::aio::ConnectionManager) -> Self {
        Self {
            client,
            max_entries: None,
        }
    }

    pub async fn new(redis_url: &str) -> Result<Self, WalError> {
        let client =
            redis::Client::open(redis_url).map_err(|e| WalError::Backend(e.to_string()))?;
        let conn = cog_redis::connect(&client)
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

    /// Bring every log in the store down to the cap, including the ones
    /// nothing will append to again.
    ///
    /// `append` applies the cap on the way in. That bounds a log that is still
    /// being written and leaves every other one exactly as it was, because the
    /// trim runs on the next append and a session that has ended never makes
    /// one. Measured on the live deployment when the cap landed: 96 of 3,637
    /// logs were above it, and those 96 held 925,579 of the 1,593,304 records
    /// in the store — 58% of the records, all of them in logs the write path
    /// would never touch again.
    ///
    /// The cut is the one `append` makes and costs the same: a replay whose
    /// checkpoint predates the retained head is told so by `read_since` rather
    /// than reading into a gap. One EVAL rather than a round trip per key, for
    /// the same reason the census beside this store walks the keyspace that
    /// way — the pass is a few thousand LLENs, and paying for them one at a
    /// time costs more in latency than the walk costs in blocking.
    pub async fn trim_over_cap(&self) -> Result<TrimOutcome, WalError> {
        let Some(cap) = self.max_entries else {
            return Ok(TrimOutcome::default());
        };
        let keep = cap.max(1);
        let mut conn = self.client.clone();
        let (trimmed, released): (u64, u64) = redis::cmd("EVAL")
            .arg(
                // The keys are the ones `append` writes under; the pass does
                // not need to know which sessions they belong to, only how
                // long they are, so it never decodes a record.
                r#"
                local cur = "0"
                local trimmed = 0
                local released = 0
                local keep = tonumber(ARGV[1])
                repeat
                  local r = redis.call("SCAN", cur, "MATCH", "wal:*", "COUNT", 1000)
                  cur = r[1]
                  for i = 1, #r[2] do
                    local key = r[2][i]
                    local len = redis.call("LLEN", key)
                    if len > keep then
                      redis.call("LTRIM", key, -keep, -1)
                      trimmed = trimmed + 1
                      released = released + (len - keep)
                    end
                  end
                until cur == "0"
                return {trimmed, released}
                "#,
            )
            .arg(0)
            .arg(keep)
            .query_async::<(u64, u64)>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis WAL cap sweep failed: {}", e)))?;
        Ok(TrimOutcome {
            trimmed,
            released,
            cap: Some(cap),
        })
    }

    /// Run the cap sweep on its own period until shutdown.
    ///
    /// Registered from here so the name in the loop readings and the name this
    /// module declares cannot drift into two spellings of the same loop.
    pub fn spawn(
        self: Arc<Self>,
        shutdown: cog_core::ShutdownSignal,
    ) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            WAL_CAP_SWEEP_LOOP,
            cog_core::loop_health::Cadence::Periodic(WAL_CAP_SWEEP_PERIOD),
            shutdown.clone(),
            move |beat| {
                let backend = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move {
                    loop {
                        beat.beat();
                        match backend.trim_over_cap().await {
                            Ok(outcome) if outcome.trimmed > 0 => tracing::info!(
                                logs = outcome.trimmed,
                                records = outcome.released,
                                cap = outcome.cap,
                                "Brought agent logs that had stopped being appended to back under the cap"
                            ),
                            Ok(_) => {}
                            Err(e) => tracing::warn!(
                                error = %e,
                                "Agent log cap sweep failed"
                            ),
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(WAL_CAP_SWEEP_PERIOD) => {}
                            _ = shutdown.wait() => break,
                        }
                    }
                }
            },
        )
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

        // Auto-trim if max_entries is configured. One command, not LLEN then
        // LTRIM: Redis clamps a start before the head, so a list shorter than
        // the cap is left untouched by `LTRIM -max -1` and the common case
        // costs no extra round trip. A cap of zero is read as one — a log that
        // keeps nothing cannot be replayed at all, and silently emptying the
        // list is a worse answer than keeping the newest record.
        if let Some(max) = self.max_entries {
            let keep = max.max(1) as isize;
            redis::cmd("LTRIM")
                .arg(&key)
                .arg(-keep)
                .arg(-1)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| WalError::Backend(format!("Redis LTRIM failed: {}", e)))?;
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
        for line in items {
            let record = WalRecord::decode_from_json_line(&line)?;
            // The record's own sequence, not the list position: after a trim
            // the two differ by however many records were dropped, and reading
            // positions here would silently skip that many records ahead.
            if record.seq >= seq {
                records.push(record);
            }
        }
        // A caller resuming from a sequence the log no longer holds has lost
        // the records between its offset and the retained head. Saying so is
        // the difference between a bounded gap and a silent one.
        if let Some(first) = records.first() {
            if first.seq > seq {
                tracing::warn!(
                    session_id,
                    requested_seq = seq,
                    retained_from_seq = first.seq,
                    "WAL records before the retained head were trimmed; the replay starts later than asked"
                );
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
        // The cut point is the first record whose own sequence reaches `seq`,
        // which is not `seq` itself unless nothing was ever trimmed. Finding it
        // costs one full read; the alternative — treating the sequence as an
        // index — keeps the oldest records of a trimmed log forever, because
        // the index they sit at is below the number the caller asked to keep.
        let items: Vec<String> = redis::cmd("LRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .query_async::<Vec<String>>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LRANGE failed: {}", e)))?;
        let mut cut = None;
        for (idx, line) in items.iter().enumerate() {
            if WalRecord::decode_from_json_line(line)?.seq >= seq {
                cut = Some(idx as isize);
                break;
            }
        }
        match cut {
            // LTRIM keeps elements from start to stop (inclusive).
            Some(start) => {
                redis::cmd("LTRIM")
                    .arg(&key)
                    .arg(start)
                    .arg(-1)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| WalError::Backend(format!("Redis LTRIM failed: {}", e)))?;
            }
            // Every record held is older than the cut: the caller asked to keep
            // nothing, so the key goes rather than staying behind empty.
            None => {
                redis::cmd("DEL")
                    .arg(&key)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| WalError::Backend(format!("Redis DEL failed: {}", e)))?;
            }
        }
        Ok(())
    }

    async fn next_seq(&self, session_id: &str) -> Result<u64, WalError> {
        let key = Self::key(session_id);
        let mut conn = self.client.clone();
        // The newest record's own sequence plus one. The list length answered
        // this only while nothing was trimmed; after one it would hand out
        // numbers already written, and the two records would share a position
        // that replay filters by.
        let last: Option<String> = redis::cmd("LINDEX")
            .arg(&key)
            .arg(-1)
            .query_async::<Option<String>>(&mut conn)
            .await
            .map_err(|e| WalError::Backend(format!("Redis LINDEX failed: {}", e)))?;
        match last {
            Some(line) => Ok(WalRecord::decode_from_json_line(&line)?.seq + 1),
            None => Ok(0),
        }
    }
}
