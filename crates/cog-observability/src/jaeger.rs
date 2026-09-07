/// Lightweight Jaeger HTTP exporter for distributed tracing.
/// - Sends finished spans to Jaeger Collector via HTTP JSON
/// - Captures `tracing` spans created by `task_span!`, `llm_span!`, etc.
/// - No OpenTelemetry dependency — pure tracing + serde_json.
///   Jaeger Collector accepts POST /api/v2/spans with JSON Thrift-like format.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use cog_core::{HttpClient, HttpRequest};

/// A span in Jaeger's JSON format.
#[derive(Debug, serde::Serialize)]
struct JaegerSpan {
    #[serde(rename = "traceID")]
    trace_id: String,
    #[serde(rename = "spanID")]
    span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "parentSpanID")]
    parent_span_id: Option<String>,
    #[serde(rename = "operationName")]
    operation_name: String,
    references: Vec<serde_json::Value>,
    #[serde(rename = "startTime")]
    start_time: u64,
    duration: u64,
    tags: Vec<JaegerTag>,
    logs: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process: Option<JaegerProcess>,
}

#[derive(Debug, serde::Serialize, Clone)]
struct JaegerTag {
    key: String,
    #[serde(rename = "type")]
    tag_type: String,
    value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
struct JaegerProcess {
    #[serde(rename = "serviceName")]
    service_name: String,
    tags: Vec<JaegerTag>,
}

/// Batch of spans ready for export.
#[derive(Debug, serde::Serialize)]
struct JaegerBatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    process: Option<JaegerProcess>,
    spans: Vec<JaegerSpan>,
}

/// HTTP exporter that pushes spans to Jaeger Collector.
pub struct JaegerExporter {
    endpoint: String,
    service_name: String,
    client: Option<Arc<dyn HttpClient>>,
    buffer: Mutex<Vec<JaegerSpan>>,
    max_batch_size: usize,
    timeout_secs: u64,
}

impl JaegerExporter {
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
            client: None,
            buffer: Mutex::new(Vec::with_capacity(100)),
            max_batch_size: 100,
            timeout_secs: 10,
        }
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn with_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn enqueue(&self, span: JaegerSpan) {
        let mut buf = self.buffer.lock().unwrap();
        buf.push(span);
        if buf.len() >= self.max_batch_size {
            let batch = std::mem::replace(&mut *buf, Vec::with_capacity(self.max_batch_size));
            drop(buf);
            let _ = self.flush(batch);
        }
    }

    pub fn flush_all(&self) -> Result<(), anyhow::Error> {
        let batch = {
            let mut buf = self.buffer.lock().unwrap();
            if buf.is_empty() {
                return Ok(());
            }
            std::mem::replace(&mut *buf, Vec::with_capacity(self.max_batch_size))
        };
        self.flush(batch)
    }

    fn flush(&self, spans: Vec<JaegerSpan>) -> Result<(), anyhow::Error> {
        if spans.is_empty() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("JaegerExporter has no HttpClient configured"))?
            .clone();
        let process = JaegerProcess {
            service_name: self.service_name.clone(),
            tags: vec![],
        };
        let batch = JaegerBatch {
            process: Some(process),
            spans,
        };
        let url = format!("{}/api/v2/spans", self.endpoint.trim_end_matches('/'));
        let timeout_secs = self.timeout_secs;
        // Fire-and-forget: flush runs on the tracing hot path (span close),
        // where blocking the runtime panics. A failed export costs one batch.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("No Tokio runtime available for Jaeger flush — spans dropped");
            return Ok(());
        };
        handle.spawn(async move {
            let result = async {
                let req = HttpRequest::post(&url)
                    .header("Content-Type", "application/json")
                    .json(&batch)
                    .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                    .timeout(timeout_secs);
                let resp = client
                    .execute(req)
                    .await
                    .map_err(|e| anyhow::anyhow!("Jaeger flush failed: {}", e))?;
                if !resp.is_success() {
                    return Err(anyhow::anyhow!("Jaeger returned {}", resp.status));
                }
                Ok(())
            }
            .await;
            if let Err(e) = result {
                tracing::debug!("Jaeger span export failed: {}", e);
            }
        });
        Ok(())
    }
}

/// `tracing_subscriber::Layer` that exports spans to Jaeger.
pub struct JaegerLayer {
    exporter: std::sync::Arc<JaegerExporter>,
}

impl JaegerLayer {
    pub fn new(exporter: std::sync::Arc<JaegerExporter>) -> Self {
        Self { exporter }
    }
}

impl<S> Layer<S> for JaegerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span not found");
        let mut exts = span.extensions_mut();
        if exts.get_mut::<JaegerSpanState>().is_none() {
            let state = JaegerSpanState::from_attrs(attrs);
            exts.insert(state);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = match ctx.span(&id) {
            Some(s) => s,
            None => return,
        };
        let name = span.name().to_string();
        let start = span
            .extensions()
            .get::<JaegerSpanState>()
            .map(|s| s.started_at)
            .unwrap_or_else(std::time::Instant::now);
        let duration_us = start.elapsed().as_micros() as u64;
        let start_time_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
            - duration_us;

        let mut trace_id = String::new();
        let mut span_id = String::new();
        let mut parent_span_id: Option<String> = None;
        let mut tags = Vec::new();

        if let Some(state) = span.extensions().get::<JaegerSpanState>() {
            trace_id.clone_from(&state.trace_id);
            span_id.clone_from(&state.span_id);
            parent_span_id.clone_from(&state.parent_span_id);
            for (k, v) in &state.fields {
                tags.push(JaegerTag {
                    key: k.clone(),
                    tag_type: "string".into(),
                    value: serde_json::Value::String(v.clone()),
                });
            }
        }

        let jaeger_span = JaegerSpan {
            trace_id,
            span_id,
            parent_span_id,
            operation_name: name,
            references: vec![],
            start_time: start_time_us,
            duration: duration_us,
            tags,
            logs: vec![],
            process: None,
        };

        self.exporter.enqueue(jaeger_span);
    }
}

/// Per-span state captured from tracing attributes.
struct JaegerSpanState {
    started_at: std::time::Instant,
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    fields: HashMap<String, String>,
}

impl JaegerSpanState {
    fn from_attrs(attrs: &span::Attributes<'_>) -> Self {
        let mut state = Self {
            started_at: std::time::Instant::now(),
            trace_id: String::new(),
            span_id: String::new(),
            parent_span_id: None,
            fields: HashMap::new(),
        };
        attrs.record(&mut FieldVisitor(&mut state));
        state
    }
}

struct FieldVisitor<'a>(&'a mut JaegerSpanState);

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "trace_id" => self.0.trace_id = value.to_string(),
            "span_id" => self.0.span_id = value.to_string(),
            "parent_span_id" => self.0.parent_span_id = Some(value.to_string()),
            _ => {
                self.0
                    .fields
                    .insert(field.name().to_string(), value.to_string());
            }
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{:?}", value);
        match field.name() {
            "trace_id" => self.0.trace_id = s,
            "span_id" => self.0.span_id = s,
            "parent_span_id" => self.0.parent_span_id = Some(s),
            _ => {
                self.0.fields.insert(field.name().to_string(), s);
            }
        }
    }
}

/// Initialize the global subscriber with a Jaeger export layer.
/// Returns the exporter handle so callers can call `flush_all()` on shutdown.
/// This installs a new `tracing_subscriber::Registry` with both the
/// configured log format layer *and* the Jaeger layer.
pub fn init_jaeger_subscriber(
    endpoint: &str,
    service_name: &str,
    log_level: &str,
    log_format: crate::LogFormat,
    http_client: Option<Arc<dyn HttpClient>>,
) -> std::sync::Arc<JaegerExporter> {
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let mut exporter = JaegerExporter::new(endpoint, service_name);
    if let Some(client) = http_client {
        exporter = exporter.with_client(client);
    }
    let exporter = std::sync::Arc::new(exporter);
    let jaeger_layer = JaegerLayer::new(exporter.clone());

    match log_format {
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

            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(json_layer)
                .with(crate::logs::SfContextLayer)
                .with(jaeger_layer)
                .try_init();
        }
        crate::LogFormat::Pretty => {
            let pretty_layer = tracing_subscriber::fmt::layer()
                .pretty()
                .with_span_events(FmtSpan::CLOSE)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true);

            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(pretty_layer)
                .with(crate::logs::SfContextLayer)
                .with(jaeger_layer)
                .try_init();
        }
    }

    exporter
}
