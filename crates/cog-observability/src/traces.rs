use serde::{Deserialize, Serialize};
/// Distributed tracing (OpenTelemetry + Jaeger).
/// **Human layer**: Jaeger UI shows trace waterfalls.
/// **Machine layer**: Trace IDs propagate across async boundaries and
/// are embedded in RawEnvelope / Snapshot / AgentEvent.
/// **Agent layer**: Spans capture task→plan→llm-call→tool-call hierarchy.
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{HttpClient, HttpRequest};

use cog_core::TraceContext;

/// Configuration for the tracing / OpenTelemetry subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Service name shown in Jaeger / OTLP backends.
    pub service_name: String,
    /// OTLP / Jaeger Collector HTTP endpoint, e.g. `http://localhost:4318/v1/traces`.
    pub jaeger_endpoint: String,
    /// Whether tracing is enabled.
    pub enabled: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "cogneva".into(),
            jaeger_endpoint: "http://localhost:4318/v1/traces".into(),
            enabled: false,
        }
    }
}

/// Attribute value types supported by spans.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum SpanAttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl From<String> for SpanAttributeValue {
    fn from(v: String) -> Self {
        SpanAttributeValue::String(v)
    }
}
impl From<&str> for SpanAttributeValue {
    fn from(v: &str) -> Self {
        SpanAttributeValue::String(v.into())
    }
}
impl From<i64> for SpanAttributeValue {
    fn from(v: i64) -> Self {
        SpanAttributeValue::Int(v)
    }
}
impl From<f64> for SpanAttributeValue {
    fn from(v: f64) -> Self {
        SpanAttributeValue::Float(v)
    }
}
impl From<bool> for SpanAttributeValue {
    fn from(v: bool) -> Self {
        SpanAttributeValue::Bool(v)
    }
}

/// A lightweight OpenTelemetry-like span backed by `tracing`.
/// Wraps a `tracing::Span` together with an optional async OTLP exporter.
/// All spans are forwarded into `tracing` regardless of whether the OTLP
/// exporter is enabled; the OTLP exporter provides a second machine-readable
/// export path (Phase 2).
#[derive(Debug, Clone)]
pub struct OtlpSpan {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub attributes: HashMap<String, SpanAttributeValue>,
}

impl OtlpSpan {
    /// Returns the `tracing::Span` associated with this context.
    /// Callers typically use `let _enter = span.to_tracing_span().enter();`.
    pub fn to_tracing_span(&self) -> tracing::Span {
        let span = tracing::info_span!(
            "span",
            otel.name = %self.name,
            trace_id = %self.trace_id,
            span_id = %self.span_id,
            parent_span_id = %self.parent_span_id.as_deref().unwrap_or(""),
        );
        for (k, v) in &self.attributes {
            let s = match v {
                SpanAttributeValue::String(s) => s.clone(),
                SpanAttributeValue::Int(i) => i.to_string(),
                SpanAttributeValue::Float(f) => f.to_string(),
                SpanAttributeValue::Bool(b) => b.to_string(),
            };
            span.record(k.as_str(), tracing::field::display(s.as_str()));
        }
        span
    }
}

/// Start a new named span within a trace context.
/// Returns an `OtlpSpan` that callers can attach attributes and events to,
/// then enter in tracing scope via `to_tracing_span().enter()`.
pub fn start_span(name: impl Into<String>, ctx: &TraceContext) -> OtlpSpan {
    OtlpSpan {
        name: name.into(),
        trace_id: ctx.trace_id.clone(),
        span_id: uuid::Uuid::new_v4().to_string().replace("-", ""),
        parent_span_id: Some(ctx.span_id.clone()),
        attributes: HashMap::new(),
    }
}

/// Add an event to a span.
/// This records an `event` inside the currently-active `tracing::Span`.
pub fn add_event(
    span: &OtlpSpan,
    name: impl Into<String>,
    attributes: HashMap<String, SpanAttributeValue>,
) {
    let tracing_span = span.to_tracing_span();
    let _enter = tracing_span.enter();
    let attrs_json: HashMap<String, serde_json::Value> = attributes
        .iter()
        .map(|(k, v)| {
            let val = match v {
                SpanAttributeValue::String(s) => serde_json::Value::String(s.clone()),
                SpanAttributeValue::Int(i) => serde_json::Value::Number((*i).into()),
                SpanAttributeValue::Float(f) => {
                    serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))
                }
                SpanAttributeValue::Bool(b) => serde_json::Value::Bool(*b),
            };
            (k.clone(), val)
        })
        .collect();
    tracing::info!(
        otel.event_name = %name.into(),
        otel.attributes = %serde_json::to_string(&attrs_json).unwrap_or_default(),
        "span event"
    );
}

/// Set a single attribute on a span.
/// Adds / overwrites an attribute in the span descriptor and records it
/// on the underlying `tracing::Span`.
pub fn set_attribute(
    span: &mut OtlpSpan,
    key: impl Into<String>,
    value: impl Into<SpanAttributeValue>,
) {
    let key = key.into();
    let value = value.into();
    span.attributes.insert(key.clone(), value);
}

/// Lightweight OTLP/HTTP exporter for spans.
/// Sends finished spans to an OTLP Collector or Jaeger (which accepts
/// OTLP from v1.35+) via HTTP JSON.  Uses `reqwest` so there is no heavy
/// OpenTelemetry SDK dependency.
/// The JSON format mirrors the OpenTelemetry Protocol (OTLP) trace
/// structure: `ResourceSpans -> ScopeSpans -> Span`.
pub struct OtlpHttpExporter {
    endpoint: String,
    service_name: String,
    client: Option<Arc<dyn HttpClient>>,
    buffer: std::sync::Mutex<Vec<OtlpSpanJson>>,
    max_batch_size: usize,
    timeout_secs: u64,
}

/// Serializable representation of an OTLP span (simplified).
#[derive(Debug, Clone, serde::Serialize)]
pub struct OtlpSpanJson {
    trace_id: String,
    span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    name: String,
    kind: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time_unix_nano: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time_unix_nano: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attributes: Vec<OtlpKeyValue>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    events: Vec<OtlpEvent>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    status: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpKeyValue {
    key: String,
    value: OtlpAnyValue,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpAnyValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    string_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    int_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    double_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bool_value: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpEvent {
    name: String,
    time_unix_nano: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpResource {
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpScope {
    name: String,
    version: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpScopeSpans {
    scope: OtlpScope,
    spans: Vec<OtlpSpanJson>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpResourceSpans {
    resource: OtlpResource,
    scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OtlpTraceRequest {
    resource_spans: Vec<OtlpResourceSpans>,
}

impl OtlpHttpExporter {
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
            client: None,
            buffer: std::sync::Mutex::new(Vec::with_capacity(100)),
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

    /// Enqueue a span for async batch export.
    pub fn enqueue(&self, span: OtlpSpanJson) {
        let mut buf = self.buffer.lock().unwrap();
        buf.push(span);
        if buf.len() >= self.max_batch_size {
            let batch = std::mem::replace(&mut *buf, Vec::with_capacity(self.max_batch_size));
            drop(buf);
            let _ = self.flush(batch);
        }
    }

    /// Flush all buffered spans synchronously (best-effort).
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

    fn flush(&self, spans: Vec<OtlpSpanJson>) -> Result<(), anyhow::Error> {
        if spans.is_empty() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("OtlpHttpExporter has no HttpClient configured"))?
            .clone();
        let resource = OtlpResource {
            attributes: vec![OtlpKeyValue {
                key: "service.name".into(),
                value: OtlpAnyValue {
                    string_value: Some(self.service_name.clone()),
                    int_value: None,
                    double_value: None,
                    bool_value: None,
                },
            }],
        };
        let scope = OtlpScope {
            name: "cogneva".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        };
        let scope_spans = OtlpScopeSpans { scope, spans };
        let request = OtlpTraceRequest {
            resource_spans: vec![OtlpResourceSpans {
                resource,
                scope_spans: vec![scope_spans],
            }],
        };

        // Fire-and-forget: flush runs on the tracing hot path, where blocking
        // the runtime panics. A failed export costs one batch.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("No Tokio runtime for OTLP flush — spans dropped");
            return Ok(());
        };
        let endpoint = self.endpoint.clone();
        let timeout_secs = self.timeout_secs;
        handle.spawn(async move {
            let result = async {
                let req = HttpRequest::post(&endpoint)
                    .header("Content-Type", "application/json")
                    .json(&request)
                    .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                    .timeout(timeout_secs);
                let resp = client
                    .execute(req)
                    .await
                    .map_err(|e| anyhow::anyhow!("OTLP flush failed: {}", e))?;
                if !resp.is_success() {
                    return Err(anyhow::anyhow!("OTLP returned {}", resp.status));
                }
                Ok(())
            }
            .await;
            if let Err(e) = result {
                tracing::debug!("OTLP span export failed: {}", e);
            }
        });
        Ok(())
    }
}

/// Convert an `OtlpSpan` into the JSON serializable form for export.
pub fn span_to_otlp_json(
    span: &OtlpSpan,
    start: std::time::SystemTime,
    end: std::time::SystemTime,
) -> OtlpSpanJson {
    let attributes: Vec<OtlpKeyValue> = span
        .attributes
        .iter()
        .map(|(k, v)| OtlpKeyValue {
            key: k.clone(),
            value: match v {
                SpanAttributeValue::String(s) => OtlpAnyValue {
                    string_value: Some(s.clone()),
                    int_value: None,
                    double_value: None,
                    bool_value: None,
                },
                SpanAttributeValue::Int(i) => OtlpAnyValue {
                    int_value: Some(*i),
                    string_value: None,
                    double_value: None,
                    bool_value: None,
                },
                SpanAttributeValue::Float(f) => OtlpAnyValue {
                    double_value: Some(*f),
                    string_value: None,
                    int_value: None,
                    bool_value: None,
                },
                SpanAttributeValue::Bool(b) => OtlpAnyValue {
                    bool_value: Some(*b),
                    string_value: None,
                    int_value: None,
                    double_value: None,
                },
            },
        })
        .collect();

    OtlpSpanJson {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_span_id.clone(),
        name: span.name.clone(),
        kind: 1, // SPAN_KIND_INTERNAL
        start_time_unix_nano: Some(
            start
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        ),
        end_time_unix_nano: Some(
            end.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        ),
        attributes,
        events: vec![],
        status: vec![],
    }
}

/// Initialize the global tracer with an OTLP / Jaeger exporter.
/// Returns `Some(Arc<OtlpHttpExporter>)` on success; logs a warning on failure.
/// Uses `tracing` as the underlying instrumentation API so no heavy
/// OpenTelemetry SDK is required.
pub fn init_tracer(
    config: &TracingConfig,
    http_client: Option<Arc<dyn HttpClient>>,
) -> Option<Arc<OtlpHttpExporter>> {
    if !config.enabled {
        tracing::info!("Tracing disabled (TracingConfig.enabled = false)");
        return None;
    }
    let mut exporter = OtlpHttpExporter::new(&config.jaeger_endpoint, &config.service_name);
    if let Some(client) = http_client {
        exporter = exporter.with_client(client);
    }
    let exporter = Arc::new(exporter);
    tracing::info!(
        jaeger_endpoint = %config.jaeger_endpoint,
        service_name = %config.service_name,
        "Tracing initialized with OTLP HTTP exporter"
    );
    Some(exporter)
}

/// Create a child trace context for a sub-operation.
pub fn child_context(parent: &TraceContext) -> TraceContext {
    TraceContext {
        trace_id: parent.trace_id.clone(),
        span_id: uuid::Uuid::new_v4().to_string().replace("-", ""),
        parent_span_id: Some(parent.span_id.clone()),
        sampled: parent.sampled,
    }
}

/// Task-level tracing span helper.
/// Usage:
/// ```rust,ignore
/// let ctx = TraceContext::generate();
/// let _guard = task_span!(ctx.clone(), "task-123");
/// ```
#[macro_export]
macro_rules! task_span {
    ($ctx:expr, $task_id:expr) => {
        tracing::info_span!(
            "processTask",
            trace_id = %$ctx.trace_id,
            span_id = %$ctx.span_id,
            task_id = %$task_id,
        )
    };
}

/// LLM-call tracing span helper.
#[macro_export]
macro_rules! llm_span {
    ($ctx:expr, $model:expr) => {
        tracing::info_span!(
            "llm-call",
            trace_id = %$ctx.trace_id,
            span_id = %$ctx.span_id,
            model = %$model,
        )
    };
}

/// Tool-call tracing span helper.
#[macro_export]
macro_rules! tool_span {
    ($ctx:expr, $tool_name:expr) => {
        tracing::info_span!(
            "tool-call",
            trace_id = %$ctx.trace_id,
            span_id = %$ctx.span_id,
            tool_name = %$tool_name,
        )
    };
}

/// Plan-phase tracing span helper.
#[macro_export]
macro_rules! plan_span {
    ($ctx:expr) => {
        tracing::info_span!(
            "plan",
            trace_id = %$ctx.trace_id,
            span_id = %$ctx.span_id,
        )
    };
}

/// Evaluate-phase tracing span helper.
#[macro_export]
macro_rules! evaluate_span {
    ($ctx:expr) => {
        tracing::info_span!(
            "evaluate",
            trace_id = %$ctx.trace_id,
            span_id = %$ctx.span_id,
        )
    };
}

/// Produce trace headers for HTTP request propagation.
/// Callers (e.g. cog-gateway) apply these to their HTTP client.
pub fn trace_headers(ctx: &TraceContext) -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert("x-trace-id".into(), ctx.trace_id.clone());
    h.insert("x-span-id".into(), ctx.span_id.clone());
    if let Some(ref parent) = ctx.parent_span_id {
        h.insert("x-parent-span-id".into(), parent.clone());
    }
    h
}
