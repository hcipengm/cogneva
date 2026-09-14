use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use cog_core::MemoryExtractor;
use cog_core::{AgentEvent, SFResult};
use cog_core::{MemoryBackend, RawSource};

use crate::IngestConfig;

/// 归档 id 里来源 slug 的长度上限。与时间戳/随机段合计仍远低于文件系统
/// NAME_MAX(255 字节)，同时保留足够前缀让人能从对象键认出来源。
const RAW_ID_SLUG_MAX: usize = 64;

/// 把任意来源 id 收敛成对象键安全的有界形式。自进化系统里 agent_id 可以是
/// 整段 issue 标题（数百字节 CJK，含 `:`、`#`、空格甚至 `/`）：直接拼进
/// 对象键会让 local-fs 后端把整键当超长单段路径写（ENAMETOOLONG），每次
/// 归档都失败；同一 agent 的多次会话还会互相覆盖同一个对象。规则：只保留
/// ASCII 字母数字与 `.`/`-`，其余折成 `_`；截到 [`RAW_ID_SLUG_MAX`]；附
/// 毫秒时间戳与 8 位随机段，保证每次归档唯一。原始 id 由调用方放进 tags
/// 保留可追溯性。
fn bounded_raw_id(prefix: &str, source: &str, now: chrono::DateTime<chrono::Utc>) -> String {
    let slug: String = source
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(RAW_ID_SLUG_MAX)
        .collect();
    let random = &Uuid::new_v4().simple().to_string()[..8];
    format!("{prefix}-{slug}-{}-{random}", now.timestamp_millis())
}

/// 从 [`bounded_raw_id`] 生成的 id 尾部取回毫秒时间戳（`-{millis}-{rand8}`
/// 收尾，slug 里允许出现 `-`，所以从右往左取）。解析失败返回 None，调用方
/// 按"无时间信息"处理（宁可多扫不漏扫）。
fn raw_id_timestamp(id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis: i64 = id.rsplit('-').nth(1)?.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(millis)
}

/// Runtime knobs for [`MemoryIngestor`]. Values come from the `memory.ingest`
/// section of the config file; the [`Default`] impl is only a fallback.
#[derive(Debug, Clone)]
pub struct MemoryIngestorConfig {
    /// Maximum number of retry attempts before giving up on an event.
    pub max_retries: u32,
    /// Base delay in milliseconds for exponential backoff (1s, 2s, 4s, ...).
    pub retry_base_delay_ms: u64,
    /// Whether to write failed events to the DLQ namespace.
    pub enable_dlq: bool,
    /// Namespace used for dead-letter queue entries.
    pub dlq_namespace: String,
    /// 抽取 worker 的并发上限。抽取是 LLM 时延主导的 I/O 任务，并发度决定
    /// 积压的排空速率；爆发期队列允许变长，靠它追平。
    pub extraction_concurrency: usize,
    /// 启动时是否对账扫描：把已归档但没有 summary 的 raw 重新入队。覆盖
    /// 崩溃/重启丢掉的在途抽取，以及任何"归档成功但抽取缺席"的残留。
    pub startup_reconcile: bool,
    /// 对账扫描只回看最近这么多个小时的 raw——更早的缺口随时间失去修复
    /// 价值，全量扫老数据只会拖慢启动。
    pub reconcile_lookback_hours: u64,
    /// 积压深度告警起点：深度首次达到该值及之后每翻倍一次打一条 WARN，
    /// 让吞不下的事件洪峰在日志里可见而不是静默排队。
    pub backlog_warn_at: usize,
}

impl Default for MemoryIngestorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay_ms: 1000,
            enable_dlq: true,
            dlq_namespace: "dlq".into(),
            extraction_concurrency: 4,
            startup_reconcile: true,
            reconcile_lookback_hours: 24,
            backlog_warn_at: 64,
        }
    }
}

impl From<&IngestConfig> for MemoryIngestorConfig {
    fn from(c: &IngestConfig) -> Self {
        Self {
            max_retries: c.max_retries,
            retry_base_delay_ms: c.retry_base_delay_ms,
            enable_dlq: c.enable_dlq,
            dlq_namespace: c.dlq_namespace.clone(),
            extraction_concurrency: c.extraction_concurrency,
            startup_reconcile: c.startup_reconcile,
            reconcile_lookback_hours: c.reconcile_lookback_hours,
            backlog_warn_at: c.backlog_warn_at,
        }
    }
}

/// 排队等待处理的 raw。`archived` 标记是否已落对象存储：worker 先补归档
/// （幂等重放）再做抽取，重启后由对账扫描兜底。
struct QueuedRaw {
    raw: RawSource,
}

/// Background service that listens to the AgentEvent broadcast stream and
/// automatically archives + extracts memories when conversations end.
/// Spawn this with [`MemoryIngestor::spawn`] and drop the returned handle
/// to stop listening.
pub struct MemoryIngestor {
    backend: Arc<dyn MemoryBackend>,
    extractor: Arc<dyn MemoryExtractor>,
    config: MemoryIngestorConfig,
}

impl MemoryIngestor {
    pub fn new(backend: Arc<dyn MemoryBackend>, extractor: Arc<dyn MemoryExtractor>) -> Self {
        Self {
            backend,
            extractor,
            config: MemoryIngestorConfig::default(),
        }
    }

    pub fn with_config(mut self, config: MemoryIngestorConfig) -> Self {
        self.config = config;
        self
    }

    /// Start the ingest pipeline and return its stop handle.
    ///
    /// 结构上分两段：接收循环只做"事件 → RawSource → 入队"，零 I/O、永不
    /// 阻塞，所以广播通道不会因消费慢而 lag 丢事件；归档（对象存储 PUT）与
    /// 抽取（两次 LLM 调用，分钟级）由 worker 池按配置并发在队列后做。爆发
    /// 期积压变长、按 LLM 吞吐自然排空，事件本身不丢。进程崩溃只丢"已入队
    /// 未归档"的一小段，启动对账扫描会把已归档未抽取的 raw 补回来。
    pub fn spawn(self, mut event_rx: broadcast::Receiver<AgentEvent>) -> mpsc::Sender<()> {
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let (job_tx, job_rx) = mpsc::unbounded_channel::<QueuedRaw>();
        let backlog = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inner = Arc::new(self);

        // 派发循环：认领一个任务就拿一个信号量许可 spawn 出去，绝不在循环
        // 位置 await 整条处理；许可数即并发上限。
        {
            let inner = inner.clone();
            let backlog = backlog.clone();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(
                inner.config.extraction_concurrency,
            ));
            tokio::spawn(async move {
                let mut job_rx = job_rx;
                while let Some(job) = job_rx.recv().await {
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break, // 信号量关闭只发生在派发任务自身消亡时
                    };
                    let inner = inner.clone();
                    let backlog = backlog.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        inner.process(job).await;
                        backlog.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    });
                }
            });
        }

        tokio::spawn(async move {
            info!("MemoryIngestor started");
            if inner.config.startup_reconcile {
                inner.reconcile(&job_tx, &backlog).await;
            }
            loop {
                tokio::select! {
                    result = event_rx.recv() => {
                        match result {
                            Ok(AgentEvent::AgentEnd { agent_id, messages, .. }) => {
                                let raw = build_raw(&agent_id, &messages);
                                enqueue(&job_tx, &backlog, QueuedRaw { raw }, inner.config.backlog_warn_at);
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                // 接收循环零 I/O 之后这里理论上到不了；真出现
                                // 说明上游事件速率高过单条 select 的分发能力，
                                // 必须响亮可见。
                                warn!("MemoryIngestor lagged, skipped {} events", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("AgentEvent broadcast closed; MemoryIngestor stopping");
                                break;
                            }
                        }
                    }
                    _ = stop_rx.recv() => {
                        info!("MemoryIngestor stopping");
                        break;
                    }
                }
            }
            // 关掉入口：派发循环收完残余任务后自然退出，在途抽取跑完。
            drop(job_tx);
        });

        stop_tx
    }

    /// 单条 raw 的完整处理：先确保归档（幂等），再补齐缺失的层。层检查让
    /// 崩溃重放不会重复写已存在的 schema/summary。
    async fn process(&self, job: QueuedRaw) {
        let raw = job.raw;
        let label = format!("archive {}", raw.id);
        if let Err(e) = self
            .retry_with_backoff(&label, || async {
                self.backend.archive_raw(&raw).await.map(|_| ())
            })
            .await
        {
            // 归档都失败时 DLQ（本身也是一次归档）大概率同样写不进，只能
            // 响亮报错；事件体仍在广播上游的日志链路里可查。
            error!("Memory archive failed for {} after retries: {}", raw.id, e);
            return;
        }
        debug!("Archived raw source: {}", raw.id);

        let label = format!("ingest {}", raw.id);
        if let Err(e) = self
            .retry_with_backoff(&label, || self.ingest_missing(&raw))
            .await
        {
            error!(
                "Memory ingestion failed for {} after {} retries: {}",
                raw.id, self.config.max_retries, e
            );
            if self.config.enable_dlq {
                if let Err(dlq_err) = self.write_dlq(&raw, &e.to_string()).await {
                    warn!("Failed to write DLQ entry: {}", dlq_err);
                }
            }
        }
    }

    /// 补齐 raw 缺失的层：schema 或 summary 已存在就跳过对应抽取。对账重放
    /// 与正常路径共用这一段，靠层存在性保证幂等。
    async fn ingest_missing(&self, raw: &RawSource) -> SFResult<()> {
        let schema_done = !self
            .backend
            .schema_for_raw(&raw.namespace, &raw.id)
            .await?
            .is_empty();
        if !schema_done {
            let schema_entries = self.extractor.extract_schema(raw).await?;
            for entry in &schema_entries {
                self.backend.store_schema(&raw.namespace, entry).await?;
            }
            debug!("Stored {} schema entries", schema_entries.len());
        }

        let summary_done = !self
            .backend
            .summary_for_raw(&raw.namespace, &raw.id)
            .await?
            .is_empty();
        if !summary_done {
            let summary = self.extractor.generate_summary(raw).await?;
            self.backend.store_summary(&raw.namespace, &summary).await?;
            debug!("Stored summary {}", summary.id);
        }

        Ok(())
    }

    /// 启动对账：扫最近窗口内的会话 raw，把没有 summary 的重新入队。
    /// 失败不阻塞主循环——对账是自愈增强，不是启动前置。
    async fn reconcile(
        &self,
        job_tx: &mpsc::UnboundedSender<QueuedRaw>,
        backlog: &std::sync::atomic::AtomicUsize,
    ) {
        match self.collect_unextracted().await {
            Ok(raws) => {
                if raws.is_empty() {
                    return;
                }
                info!(
                    "Memory ingest reconcile re-driving {} unextracted raw sources",
                    raws.len()
                );
                for raw in raws {
                    enqueue(
                        job_tx,
                        backlog,
                        QueuedRaw { raw },
                        self.config.backlog_warn_at,
                    );
                }
            }
            Err(e) => warn!("Memory ingest reconcile scan failed: {}", e),
        }
    }

    async fn collect_unextracted(&self) -> SFResult<Vec<RawSource>> {
        let ids = self
            .backend
            .list_raw("default", Some("conversation/transcript"))
            .await?;
        let cutoff = chrono::Utc::now()
            - chrono::Duration::hours(self.config.reconcile_lookback_hours as i64);
        let mut out = Vec::new();
        for id in ids {
            // 无时间信息的 id 不跳过：宁可多查一次 summary。
            if let Some(ts) = raw_id_timestamp(&id) {
                if ts < cutoff {
                    continue;
                }
            }
            if self
                .backend
                .summary_for_raw("default", &id)
                .await?
                .is_empty()
            {
                if let Some(raw) = self.backend.get_raw("default", &id).await? {
                    out.push(raw);
                }
            }
        }
        Ok(out)
    }

    async fn retry_with_backoff<F, Fut>(&self, label: &str, mut op: F) -> SFResult<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        let mut last_error = None;
        for attempt in 0..=self.config.max_retries {
            match op().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        let delay_ms = self.config.retry_base_delay_ms * 2_u64.pow(attempt);
                        warn!(
                            "{} attempt {}/{} failed, retrying in {}ms",
                            label,
                            attempt + 1,
                            self.config.max_retries + 1,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| cog_core::SFError::Agent(format!("{label}: unknown error"))))
    }

    async fn write_dlq(&self, raw: &RawSource, error_msg: &str) -> SFResult<()> {
        let dlq_payload = serde_json::json!({
            "original_namespace": raw.namespace,
            "original_id": raw.id,
            "error": error_msg,
            "content_type": raw.content_type,
            "payload_preview": String::from_utf8_lossy(&raw.payload).chars().take(500).collect::<String>(),
            "dlq_timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let dlq_raw = RawSource::new(
            format!("dlq-{}-{}", raw.id, chrono::Utc::now().timestamp_millis()),
            &self.config.dlq_namespace,
            "ingestion/failed",
            serde_json::to_vec(&dlq_payload).unwrap_or_default(),
        );

        let uri = self.backend.archive_raw(&dlq_raw).await?;
        info!("Wrote failed ingestion to DLQ: {}", uri);
        Ok(())
    }
}

/// 事件 → 持久层记录。agent_id 在自进化系统里可以是整段 issue 标题（数百
/// 字节 CJK，含 `:`/`#`/空格），收敛成对象键安全的有界形式；原始 id 放
/// tags 保留可追溯性。
fn build_raw(agent_id: &str, messages: &[cog_core::Message]) -> RawSource {
    let payload = serde_json::to_vec(messages).unwrap_or_else(|_| b"[]".to_vec());
    RawSource::new(
        bounded_raw_id("agent", agent_id, chrono::Utc::now()),
        "default",
        "conversation/transcript",
        payload,
    )
    .with_tags(vec![format!("agent_id:{}", agent_id)])
}

/// 入队并维护积压深度。深度达到告警起点及其后每个 2 的幂打一条 WARN：
/// 洪峰期积压会翻倍式增长，按幂打点既不掉点也不刷屏。
fn enqueue(
    job_tx: &mpsc::UnboundedSender<QueuedRaw>,
    backlog: &std::sync::atomic::AtomicUsize,
    job: QueuedRaw,
    warn_at: usize,
) {
    if job_tx.send(job).is_err() {
        // 接收端只在停止后关闭，此时事件随进程退出一起结束，无需补救。
        return;
    }
    let depth = backlog.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if depth >= warn_at && depth.is_power_of_two() {
        warn!(
            "Memory ingest backlog depth {} — extraction is trailing the event rate",
            depth
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CompositeMemoryBackend, MemoryMemoryBackend, NoopVectorBackend, RuleBasedExtractor,
    };
    use chrono::Utc;
    use cog_core::{Message, SchemaEntry, SummaryEntry};

    /// The agent_id shape that motivated [`bounded_raw_id`]: a self-evolving
    /// system routes whole issue titles through as agent ids — hundreds of
    /// CJK bytes plus `:`, `#`, spaces.
    const LIVE_LONG_AGENT_ID: &str = "squad:decompose-Fix gitee issue #34295240: 刷新接口未区分访问令牌和刷新令牌，访问令牌可换取新令牌";

    fn agent_end(agent_id: &str) -> AgentEvent {
        AgentEvent::AgentEnd {
            agent_id: agent_id.into(),
            messages: vec![Message::user(
                "the deploy key lives in the security gateway",
            )],
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    async fn archived_count(backend: &MemoryMemoryBackend) -> usize {
        backend
            .list_raw("default", None)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
    }

    async fn summary_count(backend: &MemoryMemoryBackend) -> usize {
        backend
            .list_summary("default")
            .await
            .map(|v| v.len())
            .unwrap_or(0)
    }

    async fn wait_for(
        cond: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
    ) -> bool {
        for _ in 0..200 {
            if cond().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Regression: the stop handle is the ingestor's lifetime anchor. A caller
    /// that discards it (`let _ = spawn(...)`) silently kills auto-ingest
    /// within milliseconds — the plugin used to do exactly that. While the
    /// handle is held, the task must keep consuming events across many rounds.
    #[tokio::test]
    async fn keeps_consuming_while_handle_is_held() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let _handle = ingestor.spawn(event_tx.subscribe());

        for round in 0..3 {
            event_tx.send(agent_end(&format!("a-{round}"))).unwrap();
            let backend = backend.clone();
            assert!(
                wait_for(move || {
                    let backend = backend.clone();
                    Box::pin({
                        let backend = backend.clone();
                        async move { archived_count(&backend).await > round }
                    })
                })
                .await,
                "event {round} must be archived while the handle is held"
            );
        }
    }

    /// Contract pin: dropping the handle stops the background task.
    #[tokio::test]
    async fn dropping_handle_stops_task() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let handle = ingestor.spawn(event_tx.subscribe());
        drop(handle);

        let mut exited = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            // Once the task exits, its broadcast receiver is gone and send fails.
            if event_tx.send(agent_end("a-x")).is_err() {
                exited = true;
                break;
            }
        }
        assert!(exited, "task must exit after its stop handle is dropped");
    }

    /// Contract pin: an explicit stop signal through the handle ends the task.
    #[tokio::test]
    async fn explicit_stop_signal_stops_task() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let handle = ingestor.spawn(event_tx.subscribe());

        handle.send(()).await.unwrap();
        let mut exited = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if event_tx.send(agent_end("a-x")).is_err() {
                exited = true;
                break;
            }
        }
        assert!(exited, "task must exit after an explicit stop signal");
    }

    /// Contract pin for [`bounded_raw_id`]: a whole-issue-title agent_id must
    /// collapse to a path-safe, NAME_MAX-bounded object key that stays unique
    /// per archive even within the same millisecond.
    #[test]
    fn bounded_raw_id_bounds_pathological_agent_id() {
        let now = Utc::now();
        let id = bounded_raw_id("agent", LIVE_LONG_AGENT_ID, now);

        // Worst case: prefix(5+1) + slug(64) + '-' + 13-digit millis + '-' +
        // 8 hex — comfortably below the 255-byte NAME_MAX per path component.
        assert!(id.len() < 128, "id not bounded: {} bytes", id.len());
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')),
            "id contains path-unsafe characters: {id}"
        );
        // Same input at the same instant must still produce distinct ids,
        // otherwise one agent's sessions overwrite each other's archives.
        let id2 = bounded_raw_id("agent", LIVE_LONG_AGENT_ID, now);
        assert_ne!(id, id2, "ids must stay unique per archive");
    }

    /// The id timestamp round trip the reconcile lookback depends on.
    #[test]
    fn raw_id_timestamp_round_trips() {
        let now = Utc::now();
        let id = bounded_raw_id("agent", "some-source", now);
        let parsed = raw_id_timestamp(&id).expect("timestamp must parse back");
        assert_eq!(parsed.timestamp_millis(), now.timestamp_millis());
        assert!(raw_id_timestamp("no-timestamp-here").is_none());
        assert!(raw_id_timestamp("agent-x-1234567890123").is_none());
    }

    /// Regression for the ENAMETOOLONG archive failure: with the raw layer on
    /// a real filesystem object store (the production wiring), a full issue
    /// title as agent_id must still archive, and the verbatim agent_id must
    /// survive as a tag for traceability.
    #[tokio::test]
    async fn long_agent_id_archives_on_filesystem_backend() {
        let dir = tempfile::tempdir().unwrap();
        let composite = CompositeMemoryBackend::new(
            Arc::new(cog_storage::FileObjectBackend::new(dir.path())),
            Arc::new(NoopVectorBackend::new()),
            8,
        );
        let backend: Arc<dyn MemoryBackend> = Arc::new(composite);
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let _handle = ingestor.spawn(event_tx.subscribe());

        event_tx.send(agent_end(LIVE_LONG_AGENT_ID)).unwrap();

        let mut archived_id = None;
        for _ in 0..100 {
            if let Ok(list) = backend.list_raw("default", None).await {
                if let Some(id) = list.into_iter().next() {
                    archived_id = Some(id);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let id = archived_id.expect("long agent_id must archive on the filesystem backend");
        assert!(id.len() < 128, "archived id not bounded: {id}");

        let raw = backend
            .get_raw("default", &id)
            .await
            .expect("raw readable")
            .expect("raw present");
        assert!(
            raw.tags
                .iter()
                .any(|t| t == &format!("agent_id:{LIVE_LONG_AGENT_ID}")),
            "verbatim agent_id must be retained in tags, got {:?}",
            raw.tags
        );
    }

    /// 比广播容量还慢得多的抽取器：复刻线上形态（两次 LLM 调用分钟级 vs
    /// 爆发期事件速率），让回归测试真站在会 lag 的悬崖边上。
    struct SlowExtractor {
        inner: RuleBasedExtractor,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl MemoryExtractor for SlowExtractor {
        async fn extract_schema(&self, source: &RawSource) -> SFResult<Vec<SchemaEntry>> {
            tokio::time::sleep(self.delay).await;
            self.inner.extract_schema(source).await
        }

        async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry> {
            tokio::time::sleep(self.delay).await;
            self.inner.generate_summary(source).await
        }
    }

    /// D40 回归：事件洪峰下一条都不许丢。广播容量 8、单条抽取 100ms、异步
    /// 灌 32 条（生产里事件来自异步上下文，每条之间运行时会调度到接收循环；
    /// 同步死循环灌事件连"瞬收"的接收循环都排不上，那不是生产形态）——
    /// 串行消费在第二条处理完之前就会 Lagged 掉大半；队列化 + 并发 worker
    /// 之后必须全量归档、全量出 summary。
    #[tokio::test]
    async fn burst_events_are_all_archived_and_extracted() {
        const EVENTS: usize = 32;
        let backend = Arc::new(MemoryMemoryBackend::new());
        let extractor = Arc::new(SlowExtractor {
            inner: RuleBasedExtractor::new(),
            delay: Duration::from_millis(100),
        });
        let ingestor = MemoryIngestor::new(backend.clone(), extractor);
        let (event_tx, _) = broadcast::channel::<AgentEvent>(8);
        let _handle = ingestor.spawn(event_tx.subscribe());

        for i in 0..EVENTS {
            event_tx.send(agent_end(&format!("burst-{i}"))).unwrap();
            tokio::task::yield_now().await;
        }

        let backend2 = backend.clone();
        assert!(
            wait_for(move || {
                let backend = backend2.clone();
                Box::pin(async move { summary_count(&backend).await >= EVENTS })
            })
            .await,
            "all {EVENTS} burst events must be extracted, none dropped by lag"
        );
        assert_eq!(
            archived_count(&backend).await,
            EVENTS,
            "all {EVENTS} burst events must be archived"
        );
    }

    /// 崩溃窗口自愈：raw 已归档但抽取没跑（进程在两者之间死掉），下次启动
    /// 的对账扫描必须把它补回来。
    #[tokio::test]
    async fn startup_reconcile_redrives_unextracted_raws() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let raw = RawSource::new(
            bounded_raw_id("agent", "crashed-mid-flight", Utc::now()),
            "default",
            "conversation/transcript",
            b"[]".to_vec(),
        );
        backend.archive_raw(&raw).await.unwrap();
        assert_eq!(summary_count(&backend).await, 0);

        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let _handle = ingestor.spawn(event_tx.subscribe());

        let backend2 = backend.clone();
        assert!(
            wait_for(move || {
                let backend = backend2.clone();
                Box::pin(async move { summary_count(&backend).await >= 1 })
            })
            .await,
            "reconcile must re-drive archived-but-unextracted raws"
        );
    }

    /// 幂等重放：schema 已落库、summary 缺失（抽取进行到一半崩溃）的 raw
    /// 被对账重放时，只补 summary，不许重复写 schema。
    #[tokio::test]
    async fn reconcile_redrives_only_missing_layers() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let raw = RawSource::new(
            bounded_raw_id("agent", "half-extracted", Utc::now()),
            "default",
            "conversation/transcript",
            serde_json::to_vec(&vec![Message::user("deploy key in gateway")]).unwrap(),
        );
        backend.archive_raw(&raw).await.unwrap();
        // 模拟"schema 抽完、summary 还没生成进程就死了"的半成品现场：
        // schema_for_raw 以 source_ref.raw_uri 匹配，挂上同一 raw id。
        let entry = SchemaEntry::new(
            "schema-entity-0",
            "default",
            cog_core::SchemaKind::Entity,
            "security gateway",
            "security_gateway",
            cog_core::SourceRef::new(format!("memory://{}", raw.id), "rule_based/v1"),
        );
        backend.store_schema("default", &entry).await.unwrap();
        let schema_before = backend.list_schema("default").await.unwrap().len();
        assert!(schema_before > 0);

        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = broadcast::channel::<AgentEvent>(16);
        let _handle = ingestor.spawn(event_tx.subscribe());

        let backend2 = backend.clone();
        assert!(
            wait_for(move || {
                let backend = backend2.clone();
                Box::pin(async move { summary_count(&backend).await >= 1 })
            })
            .await,
            "missing summary must be re-driven"
        );
        assert_eq!(
            backend.list_schema("default").await.unwrap().len(),
            schema_before,
            "existing schema entries must not be duplicated by the re-drive"
        );
    }
}
