use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot};

use cog_core::raw_logger::{RawLoggerConfig, RawLoggerFormat, RawRecord};
use cog_core::{RawRecordCodec, SFError, SFResult};
use std::sync::Arc;

const DEFAULT_ZSTD_LEVEL: i32 = 3;

fn zstd_compress(bytes: &[u8], level: i32) -> SFResult<Vec<u8>> {
    zstd::stream::encode_all(bytes, level)
        .map_err(|e| SFError::Agent(format!("zstd compress failed: {}", e)))
}

fn zstd_decompress(bytes: &[u8]) -> SFResult<Vec<u8>> {
    zstd::stream::decode_all(bytes)
        .map_err(|e| SFError::Agent(format!("zstd decompress failed: {}", e)))
}

// ─── No-op implementation ───

/// Zero-overhead logger used when raw logging is disabled.
#[derive(Debug, Default)]
pub struct NoopRawLogger;

impl NoopRawLogger {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl cog_core::raw_logger::RawLogger for NoopRawLogger {
    async fn write(&self, _record: RawRecord) -> SFResult<()> {
        Ok(())
    }

    async fn flush(&self) -> SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> SFResult<()> {
        Ok(())
    }

    async fn write_proto(&self, _encoded: Bytes) -> SFResult<()> {
        Ok(())
    }
}

// ─── In-memory implementation ───

/// In-memory raw logger for testing and local development.
#[derive(Debug)]
pub struct MemoryRawLogger {
    records: RwLock<Vec<RawRecord>>,
    codec: Option<Arc<dyn RawRecordCodec>>,
}

impl Default for MemoryRawLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryRawLogger {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(Vec::new()),
            codec: None,
        }
    }

    pub fn with_codec(mut self, codec: Arc<dyn RawRecordCodec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Return a snapshot of all stored records.
    pub fn all_records(&self) -> SFResult<Vec<RawRecord>> {
        let records = self
            .records
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(records.clone())
    }

    /// Return only records belonging to a specific stream.
    pub fn records_by_stream(&self, stream: &str) -> SFResult<Vec<RawRecord>> {
        let records = self
            .records
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(records
            .iter()
            .filter(|r| r.meta.stream == stream)
            .cloned()
            .collect())
    }

    /// Clear all stored records.
    pub fn clear(&self) -> SFResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        records.clear();
        Ok(())
    }
}

#[async_trait]
impl cog_core::raw_logger::RawLogger for MemoryRawLogger {
    async fn write(&self, record: RawRecord) -> SFResult<()> {
        let mut records = self
            .records
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        records.push(record);
        Ok(())
    }

    async fn flush(&self) -> SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> SFResult<()> {
        Ok(())
    }

    async fn write_proto(&self, encoded: Bytes) -> SFResult<()> {
        match &self.codec {
            Some(codec) => {
                let record = codec.decode_record(&encoded)?;
                self.write(record).await
            }
            None => Err(SFError::Agent(
                "MemoryRawLogger has no codec configured".into(),
            )),
        }
    }
}

// ─── File-backed implementation ───

enum WorkerMsg {
    Record(Box<RawRecord>),
    Flush(oneshot::Sender<SFResult<()>>),
}

/// Production-grade raw logger that appends to rotated daily files.
/// Files are organised as `{base_dir}/{stream}/{YYYY-MM-DD}.{ext}` where
/// `ext` is one of `jsonl`, `proto.bin`, or `proto.bin.zst` — controlled by
/// [`RawLoggerConfig::format`]. A background tokio task receives records
/// over a channel and batches writes to disk.
#[derive(Debug)]
pub struct FileRawLogger {
    tx: mpsc::UnboundedSender<WorkerMsg>,
    base_dir: String,
    zstd_level: i32,
    codec: Arc<dyn RawRecordCodec>,
}

impl FileRawLogger {
    pub async fn new(config: RawLoggerConfig, codec: Arc<dyn RawRecordCodec>) -> SFResult<Self> {
        fs::create_dir_all(&config.base_dir).await.map_err(|e| {
            SFError::Agent(format!(
                "failed to create raw log dir {}: {}",
                config.base_dir, e
            ))
        })?;

        let (tx, mut rx) = mpsc::unbounded_channel::<WorkerMsg>();
        let base_dir = config.base_dir.clone();
        let zstd_level = config.zstd_level.unwrap_or(DEFAULT_ZSTD_LEVEL);
        let format = config.format;
        let max_buffer_size = config.max_buffer_size;
        let flush_interval_secs = config.flush_interval_secs;
        let codec_bg = codec.clone();

        tokio::spawn(async move {
            let mut buffers: HashMap<String, Vec<RawRecord>> = HashMap::new();
            let mut writers: HashMap<String, BufWriter<tokio::fs::File>> = HashMap::new();
            let mut current_dates: HashMap<String, String> = HashMap::new();

            let mut ticker = (flush_interval_secs > 0).then(|| {
                let mut t = tokio::time::interval(Duration::from_secs(flush_interval_secs));
                // A slow flush must not be answered by a burst of catch-up
                // ticks; the next one is due a full interval after it returns.
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                t
            });
            if let Some(t) = ticker.as_mut() {
                // `interval`'s first tick is immediate; consume it so the first
                // periodic flush lands one interval from now, not at startup.
                t.tick().await;
            }

            loop {
                let msg = match ticker.as_mut() {
                    Some(t) => tokio::select! {
                        msg = rx.recv() => msg,
                        _ = t.tick() => {
                            if let Err(e) = flush_all(
                                &base_dir,
                                format,
                                zstd_level,
                                &mut buffers,
                                &mut writers,
                                &mut current_dates,
                                &*codec_bg,
                            )
                            .await
                            {
                                tracing::error!("FileRawLogger periodic flush failed: {}", e);
                            }
                            continue;
                        }
                    },
                    None => rx.recv().await,
                };
                let Some(msg) = msg else { break };
                match msg {
                    WorkerMsg::Record(record) => {
                        let stream = record.meta.stream.clone();
                        let entry = buffers.entry(stream.clone()).or_default();
                        entry.push(*record);

                        if entry.len() >= max_buffer_size {
                            if let Err(e) = flush_stream(
                                &base_dir,
                                &stream,
                                format,
                                zstd_level,
                                &mut buffers,
                                &mut writers,
                                &mut current_dates,
                                &*codec_bg,
                            )
                            .await
                            {
                                tracing::error!("FileRawLogger flush failed: {}", e);
                            }
                        }
                    }
                    WorkerMsg::Flush(reply) => {
                        let overall = flush_all(
                            &base_dir,
                            format,
                            zstd_level,
                            &mut buffers,
                            &mut writers,
                            &mut current_dates,
                            &*codec_bg,
                        )
                        .await;
                        let _ = reply.send(overall);
                    }
                }
            }

            // Drain remaining buffers on shutdown
            let _ = flush_all(
                &base_dir,
                format,
                zstd_level,
                &mut buffers,
                &mut writers,
                &mut current_dates,
                &*codec_bg,
            )
            .await;
            for mut writer in writers.into_values() {
                let _ = writer.flush().await;
            }
        });

        Ok(Self {
            tx,
            base_dir: config.base_dir,
            zstd_level,
            codec,
        })
    }

    /// Test helper: zstd compression level resolved at construction time.
    #[doc(hidden)]
    pub fn zstd_level(&self) -> i32 {
        self.zstd_level
    }
}

#[async_trait]
impl cog_core::raw_logger::RawLogger for FileRawLogger {
    async fn write(&self, record: RawRecord) -> SFResult<()> {
        self.tx
            .send(WorkerMsg::Record(Box::new(record)))
            .map_err(|_| SFError::Agent("raw logger worker dropped".into()))?;
        Ok(())
    }

    async fn flush(&self) -> SFResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WorkerMsg::Flush(tx))
            .map_err(|_| SFError::Agent("raw logger worker dropped".into()))?;
        rx.await
            .map_err(|_| SFError::Agent("raw logger flush channel closed".into()))?
    }

    async fn shutdown(&self) -> SFResult<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WorkerMsg::Flush(tx));
        let _ = rx.await;
        Ok(())
    }

    async fn write_proto(&self, encoded: Bytes) -> SFResult<()> {
        let record = self.codec.decode_record(&encoded)?;
        self.write(record).await
    }

    async fn read_proto(&self, stream: &str) -> SFResult<Vec<RawRecord>> {
        // Make sure any in-memory buffer is on disk first; otherwise a caller
        // who writes-then-reads in the same test would observe a partial view.
        self.flush().await?;
        read_stream_files(&self.base_dir, stream, &*self.codec).await
    }
}

/// Flush every stream's buffer, then every writer. Returns the first error
/// rather than short-circuiting, so one failing stream cannot leave the
/// remaining streams' records stranded in memory.
#[allow(clippy::too_many_arguments)]
async fn flush_all(
    base_dir: &str,
    format: RawLoggerFormat,
    zstd_level: i32,
    buffers: &mut HashMap<String, Vec<RawRecord>>,
    writers: &mut HashMap<String, BufWriter<tokio::fs::File>>,
    current_dates: &mut HashMap<String, String>,
    codec: &dyn RawRecordCodec,
) -> SFResult<()> {
    let mut overall = Ok(());
    for stream in buffers.keys().cloned().collect::<Vec<_>>() {
        if let Err(e) = flush_stream(
            base_dir,
            &stream,
            format,
            zstd_level,
            buffers,
            writers,
            current_dates,
            codec,
        )
        .await
        {
            overall = Err(e);
        }
    }
    for writer in writers.values_mut() {
        if let Err(e) = writer.flush().await {
            overall = Err(SFError::Agent(format!("flush error: {}", e)));
        }
    }
    overall
}

#[allow(clippy::too_many_arguments)]
async fn flush_stream(
    base_dir: &str,
    stream: &str,
    format: RawLoggerFormat,
    zstd_level: i32,
    buffers: &mut HashMap<String, Vec<RawRecord>>,
    writers: &mut HashMap<String, BufWriter<tokio::fs::File>>,
    current_dates: &mut HashMap<String, String>,
    codec: &dyn RawRecordCodec,
) -> SFResult<()> {
    let records = match buffers.get_mut(stream) {
        Some(v) if !v.is_empty() => {
            let mut drained = Vec::new();
            std::mem::swap(v, &mut drained);
            drained
        }
        _ => return Ok(()),
    };

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let needs_new_file = current_dates
        .get(stream)
        .map(|d| d != &today)
        .unwrap_or(true);

    if needs_new_file {
        // The writer for the previous day still holds bytes that `write_all`
        // accepted but never pushed to the file. Dropping it here — which is
        // what inserting a replacement would do — discards them with no error
        // anywhere, so flush it first.
        if let Some(mut previous) = writers.remove(stream) {
            previous
                .flush()
                .await
                .map_err(|e| SFError::Agent(format!("flush error on rotation: {}", e)))?;
        }

        let stream_dir = std::path::Path::new(base_dir).join(stream);
        fs::create_dir_all(&stream_dir).await.map_err(|e| {
            SFError::Agent(format!(
                "failed to create stream dir {}: {}",
                stream_dir.display(),
                e
            ))
        })?;

        let file_path = stream_dir.join(format!("{}.{}", today, format.extension()));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await
            .map_err(|e| {
                SFError::Agent(format!(
                    "failed to open raw log file {}: {}",
                    file_path.display(),
                    e
                ))
            })?;

        writers.insert(stream.to_string(), BufWriter::new(file));
        current_dates.insert(stream.to_string(), today);
    }

    let writer = writers
        .get_mut(stream)
        .ok_or_else(|| SFError::Agent(format!("no writer for stream {}", stream)))?;

    match format {
        RawLoggerFormat::Jsonl => {
            for record in records {
                let line = serde_json::to_string(&record).map_err(|e| {
                    SFError::Agent(format!("failed to serialize raw record: {}", e))
                })?;
                writer
                    .write_all(line.as_bytes())
                    .await
                    .map_err(|e| SFError::Agent(format!("write error: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .await
                    .map_err(|e| SFError::Agent(format!("write error: {}", e)))?;
            }
        }
        RawLoggerFormat::Proto => {
            let mut batch = Vec::new();
            for record in &records {
                codec.append_delimited(&mut batch, record)?;
            }
            writer
                .write_all(&batch)
                .await
                .map_err(|e| SFError::Agent(format!("write error: {}", e)))?;
        }
        RawLoggerFormat::ProtoZstd => {
            // Encode the whole batch as a length-delimited stream, then wrap
            // it in a single zstd frame. Each flush appends one frame; zstd
            // decoders concatenate frames transparently on read.
            let mut batch = Vec::new();
            for record in &records {
                codec.append_delimited(&mut batch, record)?;
            }
            let compressed = zstd_compress(&batch, zstd_level)?;
            writer
                .write_all(&compressed)
                .await
                .map_err(|e| SFError::Agent(format!("write error: {}", e)))?;
        }
    }

    Ok(())
}

/// Scan `base_dir/{stream}/` and decode every persisted record, regardless of
/// the on-disk format. This is the read-side counterpart to `flush_stream` —
/// it transparently handles legacy `.jsonl` files and the binary
/// `.proto.bin` / `.proto.bin.zst` formats so callers can migrate without
/// touching consumer code.
async fn read_stream_files(
    base_dir: &str,
    stream: &str,
    codec: &dyn RawRecordCodec,
) -> SFResult<Vec<RawRecord>> {
    let stream_dir = Path::new(base_dir).join(stream);
    if !stream_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(&stream_dir)
        .await
        .map_err(|e| SFError::Agent(format!("read dir error: {}", e)))?;

    // Sort by file name so the playback order is deterministic (YYYY-MM-DD).
    let mut paths: Vec<std::path::PathBuf> = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| SFError::Agent(format!("dir entry error: {}", e)))?
    {
        if entry
            .file_type()
            .await
            .map(|t| t.is_file())
            .unwrap_or(false)
        {
            paths.push(entry.path());
        }
    }
    paths.sort();

    let mut out = Vec::new();
    for path in paths {
        out.extend(read_one_file(&path, codec).await?);
    }
    Ok(out)
}

async fn read_one_file(path: &Path, codec: &dyn RawRecordCodec) -> SFResult<Vec<RawRecord>> {
    let bytes = fs::read(path)
        .await
        .map_err(|e| SFError::Agent(format!("failed to read {}: {}", path.display(), e)))?;
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    // Warm-tier migration compresses a file in place and keeps the extension it
    // found (`2026-09-14.jsonl` -> `2026-09-14.jsonl.zst`), so the frame has to
    // come off before the extension can pick the decoder. Without this a
    // warm-tier file matched no branch and was skipped as an unknown extension,
    // i.e. archived data silently read back as empty.
    let (name, bytes) = match file_name.strip_suffix(".zst") {
        Some(inner) => (inner.to_string(), zstd_decompress(&bytes)?),
        None => (file_name.to_string(), bytes),
    };

    if name.ends_with(".proto.bin") {
        codec.decode_all_delimited(&bytes)
    } else if name.ends_with(".jsonl") {
        let mut out = Vec::new();
        for line in bytes.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let r: RawRecord = serde_json::from_slice(line).map_err(|e| {
                SFError::Agent(format!("invalid jsonl in {}: {}", path.display(), e))
            })?;
            out.push(r);
        }
        Ok(out)
    } else {
        // Unknown extension — skip silently to avoid breaking on stray files
        // (e.g. .tmp or .meta files written by adjacent components).
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod file_logger_tests {
    use super::*;
    use cog_core::RawLogger as _;
    use cog_protocol::convert::ProtoCodec;

    fn record(stream: &str, sequence: u64) -> RawRecord {
        RawRecord {
            meta: cog_core::raw_logger::RawMeta {
                version: "1.0".into(),
                stream: stream.into(),
                recorded_at: Utc::now(),
                recorded_by: "logger-test".into(),
                sequence,
                trace_id: format!("trace-{sequence}"),
                span_id: None,
            },
            context: cog_core::raw_logger::RawContext::default(),
            payload: cog_core::raw_logger::RawPayload {
                direction: "inbound".into(),
                transport: "test".into(),
                format: None,
                raw: serde_json::json!({ "sequence": sequence }),
            },
        }
    }

    fn config(base_dir: &str, flush_interval_secs: u64) -> RawLoggerConfig {
        RawLoggerConfig {
            enabled: true,
            base_dir: base_dir.into(),
            max_buffer_size: 1000,
            format: RawLoggerFormat::Proto,
            zstd_level: None,
            flush_interval_secs,
        }
    }

    async fn records_on_disk(base_dir: &str, stream: &str) -> Vec<RawRecord> {
        read_stream_files(base_dir, stream, &ProtoCodec)
            .await
            .unwrap()
    }

    /// 记录数阈值在低流量下永远够不到：一个 stream 在 max_buffer_size 以下可以静坐
    /// 几小时，期间写下的记录全在内存里，进程一挂即失。所以盘上的存在不能只由计数
    /// 触发——这条断言在没有任何显式 flush 的前提下验证定时器自己把记录推了下去。
    #[tokio::test]
    async fn timer_writes_a_buffered_record_with_no_explicit_flush() {
        let dir = tempfile::tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap().to_string();
        let logger = FileRawLogger::new(config(&base_dir, 1), Arc::new(ProtoCodec))
            .await
            .unwrap();

        logger.write(record("transport_raw", 1)).await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !records_on_disk(&base_dir, "transport_raw").await.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "record stayed in memory with no explicit flush"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let on_disk = records_on_disk(&base_dir, "transport_raw").await;
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].meta.sequence, 1);
    }

    /// 关掉定时器（0）时计数阈值仍是唯一触发：这条锁住"配 0 就等于回到旧行为"，
    /// 免得有人以为 0 是个安全的默认值。
    #[tokio::test]
    async fn a_zero_interval_leaves_only_the_count_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap().to_string();
        let logger = FileRawLogger::new(config(&base_dir, 0), Arc::new(ProtoCodec))
            .await
            .unwrap();

        logger.write(record("transport_raw", 1)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(records_on_disk(&base_dir, "transport_raw").await.is_empty());
    }

    /// warm 迁移把热层文件就地压缩、保留原扩展名（`2026-09-14.jsonl` →
    /// `2026-09-14.jsonl.zst`）。读取侧不先脱掉这层 zstd 就会把它归进"未知扩展名"
    /// 而静默返回空——归档就成了读不回来的东西，且不报错。
    #[tokio::test]
    async fn a_warm_tier_compressed_jsonl_file_still_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap().to_string();
        let stream_dir = std::path::Path::new(&base_dir).join("transport_raw");
        tokio::fs::create_dir_all(&stream_dir).await.unwrap();

        let jsonl = format!(
            "{}\n",
            serde_json::to_string(&record("transport_raw", 7)).unwrap()
        );
        let compressed = zstd_compress(jsonl.as_bytes(), 3).unwrap();
        tokio::fs::write(stream_dir.join("2026-09-14.jsonl.zst"), &compressed)
            .await
            .unwrap();

        let on_disk = records_on_disk(&base_dir, "transport_raw").await;
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].meta.sequence, 7);
    }

    /// 同一个 warm 迁移路径也压缩 `.proto.bin`，压缩帧里是长度分隔的记录流。
    #[tokio::test]
    async fn a_warm_tier_compressed_proto_file_still_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap().to_string();
        let stream_dir = std::path::Path::new(&base_dir).join("system_raw");
        tokio::fs::create_dir_all(&stream_dir).await.unwrap();

        let mut framed = Vec::new();
        ProtoCodec
            .append_delimited(&mut framed, &record("system_raw", 9))
            .unwrap();
        let compressed = zstd_compress(&framed, 3).unwrap();
        tokio::fs::write(stream_dir.join("2026-09-14.proto.bin.zst"), &compressed)
            .await
            .unwrap();

        let on_disk = records_on_disk(&base_dir, "system_raw").await;
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].meta.sequence, 9);
    }
}
