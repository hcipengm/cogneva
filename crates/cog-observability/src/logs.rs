/// Structured JSON logging — three-layer consumer support.
/// **Human layer**: pretty-print console output for developers/SREs.
/// **Machine layer**: JSON Lines with standardized fields for Loki ingestion.
/// **Agent layer**: span-attached log events that flow into Snapshot replay.
///   timestamp, level, service, version, hostname, trace_id, span_id,
///   task_id, agent_id, context
use std::collections::HashMap;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use cog_core::{HttpClient, HttpRequest};
use std::sync::Arc;

use std::collections::BTreeMap;
use std::sync::OnceLock;

pub type LogFilterHandle =
    tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>;

/// An owned, type-erased output layer stack that can sit on subscriber `S`.
pub(crate) type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync + 'static>;

/// The subscriber stack installed by [`install_early_subscriber`]: a
/// reloadable `EnvFilter` directly on the registry (so the returned
/// [`LogFilterHandle`] keeps its canonical type) with a swappable output
/// stack on top.
pub(crate) type EarlyRegistry = tracing_subscriber::layer::Layered<
    tracing_subscriber::reload::Layer<EnvFilter, tracing_subscriber::Registry>,
    tracing_subscriber::Registry,
>;

/// Handles captured by the early subscriber so the observability plugin can
/// upgrade the output stack in place instead of installing a second global
/// subscriber (which would silently fail and drop the configured format).
pub(crate) struct EarlyHandles {
    pub(crate) filter: LogFilterHandle,
    pub(crate) output: tracing_subscriber::reload::Handle<BoxedLayer<EarlyRegistry>, EarlyRegistry>,
}

static EARLY: OnceLock<EarlyHandles> = OnceLock::new();

/// Install a minimal global subscriber as the very first step of process
/// startup, before config loading and plugin initialization.
///
/// The full log stack (JSON/pretty format, Loki mirror, context fields) is
/// installed by the observability plugin, which initialises only after its
/// dependencies (net, storage). Without an early subscriber every `tracing`
/// event emitted before that point — config load, identity seeding, all
/// layer-0 plugin init logs — falls into tracing's no-op default subscriber
/// and is silently lost, which is exactly the window where startup failures
/// happen. The early subscriber prints to stdout at `RUST_LOG` (default
/// `info`); when [`init_subscriber_with_pusher`] or the Jaeger initializer
/// runs later it hot-swaps the output stack and reloads the filter, so no
/// event is dropped on either side of the swap.
///
/// Idempotent in effect: if a global subscriber already exists the install
/// fails, a note goes to stderr, and later initializers fall back to their
/// direct-install path.
pub fn install_early_subscriber() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (reloadable_filter, filter_handle) = tracing_subscriber::reload::Layer::new(filter);
    let early_output = tracing_subscriber::fmt::layer()
        .compact()
        .with_target(true)
        .with_line_number(true)
        .with_writer(std::io::stdout);
    let (reloadable_output, output_handle) =
        tracing_subscriber::reload::Layer::new(Box::new(early_output) as BoxedLayer<EarlyRegistry>);
    match tracing_subscriber::registry()
        .with(reloadable_filter)
        .with(reloadable_output)
        .try_init()
    {
        Ok(()) => {
            let _ = EARLY.set(EarlyHandles {
                filter: filter_handle,
                output: output_handle,
            });
        }
        Err(e) => eprintln!(
            "early log subscriber not installed ({e}); \
             the observability plugin will install the full stack directly"
        ),
    }
}

pub(crate) fn early_handles() -> Option<&'static EarlyHandles> {
    EARLY.get()
}

/// Replace the early subscriber's output stack and filter level in place.
/// Events keep flowing across the swap; failures are reported on stderr
/// because by this point stderr is the only guaranteed log sink.
pub(crate) fn upgrade_early(
    handles: &EarlyHandles,
    output: BoxedLayer<EarlyRegistry>,
    filter: EnvFilter,
) {
    if let Err(e) = handles.output.modify(|slot| *slot = output) {
        eprintln!("failed to swap early log output stack: {e}");
    }
    if let Err(e) = handles.filter.reload(filter) {
        eprintln!("failed to reload early log filter: {e}");
    }
}

/// Build the configured console/mirror output stack: format layer +
/// standardized context fields + optional Loki mirror. Generic over the
/// subscriber it will sit on (plain registry for direct installs, the early
/// registry stack for in-place upgrades).
fn output_stack<S>(
    format: crate::LogFormat,
    pusher: Option<Arc<LokiBackgroundPusher>>,
    service: &str,
) -> BoxedLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a> + 'static,
{
    let loki_layer = pusher.map(|p| LokiLayer::new(p, service));
    match format {
        crate::LogFormat::Json => {
            let json_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::CLOSE)
                .with_current_span(true)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true)
                .flatten_event(true);
            Box::new(json_layer.and_then(SfContextLayer).and_then(loki_layer))
        }
        crate::LogFormat::Pretty => {
            let pretty_layer = tracing_subscriber::fmt::layer()
                .pretty()
                .with_span_events(FmtSpan::CLOSE)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true);
            Box::new(pretty_layer.and_then(SfContextLayer).and_then(loki_layer))
        }
    }
}

/// Initialize the global subscriber with the configured format and level.
/// Returns a [`LogFilterHandle`] so callers can hot-reload the `EnvFilter`
/// without restarting the process.
pub fn init_subscriber(log_level: &str, format: crate::LogFormat) -> LogFilterHandle {
    init_subscriber_with_pusher(log_level, format, None, "cogneva")
}

/// Same as [`init_subscriber`] but additionally mirrors every event into Loki
/// through `pusher`. Pass `None` to skip the mirror (console-only logging).
///
/// When [`install_early_subscriber`] ran earlier in this process the full
/// stack is swapped into the existing global subscriber (the only way to
/// keep both the boot logs and the configured format — a global subscriber
/// can be installed just once); otherwise it is installed directly.
pub fn init_subscriber_with_pusher(
    log_level: &str,
    format: crate::LogFormat,
    pusher: Option<Arc<LokiBackgroundPusher>>,
    service: &str,
) -> LogFilterHandle {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(handles) = early_handles() {
        upgrade_early(handles, output_stack(format, pusher, service), filter);
        return handles.filter.clone();
    }

    let (reloadable_filter, handle) = tracing_subscriber::reload::Layer::new(filter);
    if let Err(e) = tracing_subscriber::registry()
        .with(reloadable_filter)
        .with(output_stack(format, pusher, service))
        .try_init()
    {
        eprintln!("log subscriber not installed: {e}");
    }

    handle
}

/// `tracing` layer that mirrors events to a [`LokiBackgroundPusher`].
///
/// Without this the Loki client has nothing feeding it — the structured logs
/// land in Loki only if something converts tracing events into [`LogEntry`].
pub struct LokiLayer {
    pusher: Arc<LokiBackgroundPusher>,
    service: String,
}

impl LokiLayer {
    pub fn new(pusher: Arc<LokiBackgroundPusher>, service: impl Into<String>) -> Self {
        Self {
            pusher,
            service: service.into(),
        }
    }
}

/// Collects an event's fields into a [`LogEntry`] shape.
#[derive(Default)]
struct LokiFieldVisitor {
    message: String,
    event: Option<String>,
    fields: HashMap<String, serde_json::Value>,
}

impl LokiFieldVisitor {
    fn record(&mut self, name: &str, value: serde_json::Value) {
        match name {
            "message" => {
                if let serde_json::Value::String(s) = &value {
                    self.message = s.clone();
                } else {
                    self.message = value.to_string();
                }
            }
            "event" => {
                self.event = Some(match value {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                });
            }
            other => {
                self.fields.insert(other.to_string(), value);
            }
        }
    }
}

impl tracing::field::Visit for LokiFieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        // The fmt macros render `%x` through record_debug as a quoted string;
        // LogEntry carries plain text, so unwrap the debug quoting for strings.
        let text = format!("{value:?}");
        let plain = text
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .map(|t| t.to_string())
            .unwrap_or(text);
        self.record(name, serde_json::Value::String(plain));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field.name(), serde_json::Value::String(value.to_string()));
    }
}

impl<S> Layer<S> for LokiLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = LokiFieldVisitor::default();
        event.record(&mut visitor);
        let meta = event.metadata();
        visitor.fields.insert(
            "service".into(),
            serde_json::Value::String(self.service.clone()),
        );
        visitor.fields.insert(
            "target".into(),
            serde_json::Value::String(meta.target().to_string()),
        );

        let message = if visitor.message.is_empty() {
            meta.target().to_string()
        } else {
            visitor.message
        };
        self.pusher.enqueue(LogEntry {
            timestamp: chrono::Utc::now(),
            level: meta.level().to_string().to_lowercase(),
            event: visitor.event.unwrap_or_else(|| meta.target().to_string()),
            trace_id: None,
            span_id: None,
            task_id: None,
            agent_id: None,
            message,
            context: visitor.fields,
        });
    }
}

/// Custom tracing layer that injects Cogneva standardized context fields.
///   service, version, hostname, trace_id, span_id, task_id, agent_id
pub(crate) struct SfContextLayer;

impl<S> Layer<S> for SfContextLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, _event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        // The JSON formatter already captures span context via
        // `with_current_span` and `with_span_events`.  Additional
        // SF-specific fields (service, version, hostname) are injected
        // via the `tracing::info!` macros below.
    }
}

/// Write a structured business log entry.
/// # Example
/// ```rust,ignore
/// use cog_observability::log_event;
/// log_event!("task_started", task_id = "t-123", agent_id = "a-456",
///            context = { "input_size": 1024 });
/// ```
#[macro_export]
macro_rules! log_event {
    ($event:expr, $($key:ident = $value:expr),* $(,)?) => {
        {
            let mut ctx = ::std::collections::HashMap::new();
            $(
                ctx.insert(stringify!($key), json!($value));
            )*
            info!(
                event = %$event,
                service = "cogneva",
                version = env!("CARGO_PKG_VERSION"),
                context = %json!(ctx),
                ""
            );
        }
    };
}

/// In-memory log buffer for short-term querying (last N entries).
/// Used by the Agent layer for quick context retrieval during replay.
pub struct LogBuffer {
    capacity: usize,
    entries: std::sync::Mutex<Vec<LogEntry>>,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub level: String,
    pub event: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub message: String,
    pub context: HashMap<String, serde_json::Value>,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: std::sync::Mutex::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn push(&self, entry: LogEntry) {
        let mut buf = self.entries.lock().unwrap();
        if buf.len() >= self.capacity {
            buf.remove(0);
        }
        buf.push(entry);
    }

    pub fn query_by_task(&self, task_id: &str, limit: usize) -> Vec<LogEntry> {
        let buf = self.entries.lock().unwrap();
        buf.iter()
            .rev()
            .filter(|e| e.task_id.as_deref() == Some(task_id))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn query_by_agent(&self, agent_id: &str, limit: usize) -> Vec<LogEntry> {
        let buf = self.entries.lock().unwrap();
        buf.iter()
            .rev()
            .filter(|e| e.agent_id.as_deref() == Some(agent_id))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn recent(&self, limit: usize) -> Vec<LogEntry> {
        let buf = self.entries.lock().unwrap();
        buf.iter().rev().take(limit).cloned().collect()
    }
}

/// Loki push client.
/// Sends structured log entries to Loki via HTTP POST to
/// `/loki/api/v1/push`.  Groups entries by level and service labels.
pub struct LokiPushClient {
    endpoint: String,
    base_labels: HashMap<String, String>,
    client: Option<Arc<dyn HttpClient>>,
    max_retries: u32,
    timeout_secs: u64,
}

impl LokiPushClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        let mut base_labels = HashMap::new();
        base_labels.insert("job".into(), "cogneva".into());
        base_labels.insert("service".into(), "cogneva".into());
        Self {
            endpoint: endpoint.into(),
            base_labels,
            client: None,
            max_retries: 3,
            timeout_secs: 10,
        }
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.base_labels.insert(key.into(), value.into());
        self
    }

    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn with_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Push log entries to Loki.
    /// Entries are grouped by `level` label into separate streams.
    /// Each stream carries the base labels plus per-entry metadata.
    pub async fn push(&self, entries: Vec<LogEntry>) -> Result<(), anyhow::Error> {
        if entries.is_empty() {
            return Ok(());
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("LokiPushClient has no HttpClient configured"))?;

        let payload = self.build_payload(entries);
        let url = format!("{}/loki/api/v1/push", self.endpoint.trim_end_matches('/'));

        let mut attempt = 0u32;
        loop {
            let req = HttpRequest::post(&url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                .timeout(self.timeout_secs);

            match client.execute(req).await {
                Ok(resp) if resp.is_success() => return Ok(()),
                Ok(resp) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow::anyhow!(
                            "Loki push failed with status {} after {} retries",
                            resp.status,
                            self.max_retries
                        ));
                    }
                    tracing::warn!(
                        status = resp.status,
                        attempt,
                        "Loki push failed, retrying..."
                    );
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(anyhow::anyhow!(
                            "Loki push request failed after {} retries: {}",
                            self.max_retries,
                            e
                        ));
                    }
                    tracing::warn!(error = %e, attempt, "Loki push request failed, retrying...");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100 * (1u64 << attempt))).await;
        }
    }

    fn build_payload(&self, entries: Vec<LogEntry>) -> serde_json::Value {
        // 流标签必须是对象（`{"job":"…"}`），不能是标签集字面量字符串：Loki 的
        // loghttp 解码器对字符串形式一律 400（`Value looks like object`），
        // 只有对象形式收得下。按标签集分组，键用 BTreeMap 保证流顺序确定。
        let mut streams: BTreeMap<BTreeMap<String, String>, Vec<(String, String)>> =
            BTreeMap::new();
        for entry in entries {
            let level = entry.level.clone();
            let mut labels: BTreeMap<String, String> = BTreeMap::new();
            labels.insert(
                "job".into(),
                self.base_labels
                    .get("job")
                    .cloned()
                    .unwrap_or_else(|| "cogneva".to_string()),
            );
            labels.insert(
                "service".into(),
                self.base_labels
                    .get("service")
                    .cloned()
                    .unwrap_or_else(|| "cogneva".to_string()),
            );
            labels.insert("level".into(), level.clone());
            let timestamp_ns = format!("{}", entry.timestamp.timestamp_nanos_opt().unwrap_or(0));
            let line = format!(
                "[{}] {} {}",
                entry.timestamp.to_rfc3339(),
                level.to_uppercase(),
                entry.message
            );
            streams
                .entry(labels)
                .or_default()
                .push((timestamp_ns, line));
        }

        let streams_json: Vec<serde_json::Value> = streams
            .into_iter()
            .map(|(stream, values)| {
                serde_json::json!({
                    "stream": stream,
                    "values": values.into_iter().map(|(ts, line)| {
                        serde_json::json!([ts, line])
                    }).collect::<Vec<_>>(),
                })
            })
            .collect();

        serde_json::json!({ "streams": streams_json })
    }
}

/// Background task that periodically flushes buffered log entries to Loki.
pub struct LokiBackgroundPusher {
    client: Arc<LokiPushClient>,
    interval: std::time::Duration,
    max_batch_size: usize,
    buffer: std::sync::Mutex<Vec<LogEntry>>,
}

impl LokiBackgroundPusher {
    pub fn new(
        client: Arc<LokiPushClient>,
        interval: std::time::Duration,
        max_batch_size: usize,
    ) -> Self {
        Self {
            client,
            interval,
            max_batch_size,
            buffer: std::sync::Mutex::new(Vec::with_capacity(max_batch_size)),
        }
    }

    pub fn enqueue(&self, entry: LogEntry) {
        let mut buf = self.buffer.lock().unwrap();
        buf.push(entry);
        if buf.len() >= self.max_batch_size {
            let batch = std::mem::replace(&mut *buf, Vec::with_capacity(self.max_batch_size));
            drop(buf);
            let client = self.client.clone();
            tokio::spawn(async move {
                if let Err(e) = client.push(batch).await {
                    tracing::warn!("Loki background push failed: {}", e);
                }
            });
        }
    }

    pub async fn run_loop(&self) {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            interval.tick().await;
            let batch = {
                let mut buf = self.buffer.lock().unwrap();
                if buf.is_empty() {
                    continue;
                }
                std::mem::replace(&mut *buf, Vec::with_capacity(self.max_batch_size))
            };
            if let Err(e) = self.client.push(batch).await {
                tracing::warn!("Loki background push failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(level: &str, message: &str) -> LogEntry {
        LogEntry {
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            level: level.into(),
            event: "probe".into(),
            trace_id: None,
            span_id: None,
            task_id: None,
            agent_id: None,
            message: message.into(),
            context: HashMap::new(),
        }
    }

    /// 流标签必须是对象：Loki 的 loghttp 解码器拒收标签集字面量字符串
    /// （`map[level:…]` 形式的 `"stream"` 会 400 `Value looks like object`）。
    #[test]
    fn payload_stream_is_object_not_label_literal() {
        let client = LokiPushClient::new("http://loki:3100").with_label("service", "probe");
        let payload = client.build_payload(vec![entry("info", "hello")]);

        let streams = payload
            .get("streams")
            .and_then(|v| v.as_array())
            .expect("streams array");
        assert_eq!(streams.len(), 1);
        let stream = streams[0].get("stream").expect("stream field");
        assert!(
            stream.is_object(),
            "stream must be a JSON object, got {stream}"
        );
        assert_eq!(
            stream.get("service").and_then(|v| v.as_str()),
            Some("probe")
        );
        assert_eq!(stream.get("level").and_then(|v| v.as_str()), Some("info"));
        assert_eq!(stream.get("job").and_then(|v| v.as_str()), Some("cogneva"));

        let values = streams[0]
            .get("values")
            .and_then(|v| v.as_array())
            .expect("values array");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].as_array().expect("entry pair").len(), 2);
    }

    /// 同一标签集合并成一条流，级别不同则分属不同流。
    #[test]
    fn payload_groups_entries_by_label_set() {
        let client = LokiPushClient::new("http://loki:3100").with_label("service", "probe");
        let payload = client.build_payload(vec![
            entry("info", "a"),
            entry("info", "b"),
            entry("warn", "c"),
        ]);
        let streams = payload.get("streams").and_then(|v| v.as_array()).unwrap();
        assert_eq!(streams.len(), 2, "info 两条合一，warn 单独一条");
    }

    /// 数事件的假输出层：模拟早期兜底栈与升级后的正式栈。
    #[derive(Clone)]
    struct Capture(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl Capture {
        fn new() -> Self {
            Self(std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)))
        }

        fn count(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, _event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 早期订阅者的热替换契约：升级前的事件不丢（这正是 init 日志丢失的
    /// 修复点）、升级后旧输出栈彻底让位、过滤器可就地重载生效。
    /// 用 with_default 走线程本地分发，不碰进程级 EARLY OnceLock。
    #[test]
    fn early_subscriber_captures_then_hot_swaps_output_and_filter() {
        let early = Capture::new();
        let late = Capture::new();

        let (reloadable_filter, filter_handle) =
            tracing_subscriber::reload::Layer::new(EnvFilter::new("info"));
        let (reloadable_output, output_handle) = tracing_subscriber::reload::Layer::new(Box::new(
            early.clone(),
        )
            as BoxedLayer<EarlyRegistry>);
        let subscriber = tracing_subscriber::registry()
            .with(reloadable_filter)
            .with(reloadable_output);
        let handles = EarlyHandles {
            filter: filter_handle,
            output: output_handle,
        };

        tracing::subscriber::with_default(subscriber, || {
            // 升级前：兜底栈收得到事件（修复前这些全部落进 no-op 订阅者）。
            tracing::info!(target: "early-subscriber-test", "boot event before upgrade");
            assert_eq!(early.count(), 1, "early stack must capture boot events");
            assert_eq!(late.count(), 0);

            // 热替换输出栈：旧栈让位，新栈接管，事件流不中断。
            upgrade_early(&handles, Box::new(late.clone()), EnvFilter::new("info"));
            tracing::info!(target: "early-subscriber-test", "event after upgrade");
            assert_eq!(early.count(), 1, "swapped-out stack must stop receiving");
            assert_eq!(late.count(), 1, "upgraded stack must receive events");

            // 过滤器就地重载到 warn：info 被挡、warn 通过。
            upgrade_early(&handles, Box::new(late.clone()), EnvFilter::new("warn"));
            tracing::info!(target: "early-subscriber-test", "should be filtered out");
            assert_eq!(late.count(), 1, "reloaded filter must block info");
            tracing::warn!(target: "early-subscriber-test", "should pass");
            assert_eq!(late.count(), 2, "reloaded filter must pass warn");
        });
    }
}
