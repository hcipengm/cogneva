use cog_core::metric_names::TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL;
use cog_core::{
    CommandEvent, HttpClient, HttpRequest, MetricsBackend, SFError, SFResult, SandboxBackend,
    SandboxPayload, SandboxRequest, DEFAULT_MEMORY_NAMESPACE, MEMORY_API_BASE_ENV,
    MEMORY_RAW_CONTENT_PATH, MEMORY_RAW_ITEM_PATH, MEMORY_RAW_PATH,
};
use cog_core::{Tool, ToolImplementation};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// How long one memory-read call may take before it is abandoned. The call sits
/// on an agent turn, so its budget is a share of a turn rather than a background
/// job's: long enough for a healthy API to return a bounded payload, short
/// enough that an API which stopped answering delays the turn instead of
/// wedging it.
const MEMORY_READ_TIMEOUT_SECS: u64 = 30;

/// The payload size a read moves without being told otherwise. A quarter
/// megabyte is what a tool result that already fit an agent's context roughly
/// holds; a caller that wants a larger source names its own budget.
const DEFAULT_RAW_FETCH_BUDGET_BYTES: u64 = 262_144;

/// Real execution identity of one agent run. Carried explicitly from the
/// actor (which knows the DAG task) through the runtime into sandbox requests,
/// so the executor pod can attribute and route work per task.
#[derive(Debug, Clone)]
pub struct ToolScope {
    pub task_id: String,
    pub agent_id: String,
}

#[derive(Clone)]
pub struct ToolRegistry {
    tools: Arc<std::sync::RwLock<HashMap<String, Tool>>>,
    sandbox_backend: Option<Arc<dyn SandboxBackend>>,
    guardrail: Option<Arc<dyn cog_core::Guardrail>>,
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
    wasm_timeout: Duration,
    /// Budget for shell commands. Kept apart from `wasm_timeout`: a snippet is
    /// bounded work, a command may be a compiler run.
    shell_timeout: Duration,
    /// When true, shell-class tools reject calls without a [`ToolScope`].
    /// Evolution pods turn this on so no command can reach the executor under
    /// a synthetic identity; default off keeps embedded/main-app behavior.
    require_identity: bool,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: Arc::new(std::sync::RwLock::new(HashMap::new())),
            sandbox_backend: None,
            guardrail: None,
            plugin_registry: None,
            wasm_timeout: Duration::from_secs(30),
            shell_timeout: Duration::from_secs(600),
            require_identity: false,
        }
    }

    pub fn with_require_identity(mut self, require: bool) -> Self {
        self.require_identity = require;
        self
    }

    pub fn set_require_identity(&mut self, require: bool) {
        self.require_identity = require;
    }

    pub fn with_wasm_timeout(mut self, secs: u64) -> Self {
        self.wasm_timeout = Duration::from_secs(secs);
        self
    }

    pub fn with_shell_timeout(mut self, secs: u64) -> Self {
        self.shell_timeout = Duration::from_secs(secs);
        self
    }

    pub fn with_sandbox_backend(mut self, backend: Arc<dyn SandboxBackend>) -> Self {
        self.sandbox_backend = Some(backend);
        self
    }

    pub fn set_sandbox_backend(&mut self, backend: Arc<dyn SandboxBackend>) {
        self.sandbox_backend = Some(backend);
    }

    pub fn with_guardrail(mut self, guardrail: Arc<dyn cog_core::Guardrail>) -> Self {
        self.guardrail = Some(guardrail);
        self
    }

    pub fn set_guardrail(&mut self, guardrail: Arc<dyn cog_core::Guardrail>) {
        self.guardrail = Some(guardrail);
    }

    pub fn with_plugin_registry(mut self, registry: Arc<dyn cog_core::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }

    pub fn set_plugin_registry(&mut self, registry: Arc<dyn cog_core::PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    pub fn get(&self, name: &str) -> Option<Tool> {
        self.tools.read().unwrap().get(name).cloned()
    }

    /// The tool list in name order.
    ///
    /// The definitions live in a HashMap, whose iteration order is arbitrary:
    /// stable within one process, reshuffled by the next. This list is serialized
    /// into every LLM request, so a reshuffle re-sends a block that did not
    /// change and loses the upstream prefix cache for everything that follows it.
    /// Order is not part of what the tool set *is*, so it comes from the content
    /// (the names) rather than from the container.
    pub fn list(&self) -> Vec<Tool> {
        let mut tools: Vec<Tool> = self.tools.read().unwrap().values().cloned().collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tools.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    pub async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        self.execute_scoped(name, arguments, None).await
    }

    /// Execute a tool with the real run identity. `None` scope means the
    /// caller never established task identity (legacy/embedded path); the
    /// synthetic id gets an `unscoped-` prefix so logs can tell the two apart,
    /// and when `require_identity` is on shell tools are rejected outright.
    pub async fn execute_scoped(
        &self,
        name: &str,
        arguments: serde_json::Value,
        scope: Option<&ToolScope>,
    ) -> SFResult<serde_json::Value> {
        let tool = self
            .tools
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| cog_core::SFError::Agent(format!("Tool not found: {}", name)))?;

        // Guardrail check before tool execution
        if let Some(ref guardrail) = self.guardrail {
            let tool_call = cog_core::ToolCall {
                id: uuid::Uuid::new_v4().to_string(),
                name: name.into(),
                arguments: arguments.clone(),
            };
            match guardrail.check_tool_call(&tool_call).await {
                cog_core::GuardResult::Pass => {}
                cog_core::GuardResult::Block { reason, rule } => {
                    return Err(cog_core::SFError::Agent(format!(
                        "Guardrail blocked tool '{}': {} (rule: {})",
                        name, reason, rule
                    )));
                }
                cog_core::GuardResult::Warn { reason, rule } => {
                    tracing::warn!(
                        "Guardrail warned on tool '{}': {} (rule: {})",
                        name,
                        reason,
                        rule
                    );
                }
            }
        }

        match &tool.implementation {
            ToolImplementation::Native(handler) => (handler)(arguments).await,
            ToolImplementation::Wasm {
                plugin_id,
                export_name,
            } => {
                let backend = self.sandbox_backend.as_ref().ok_or_else(|| {
                    cog_core::SFError::Agent("SandboxBackend not configured for WASM tool".into())
                })?;
                let bytes = if let Some(ref registry) = self.plugin_registry {
                    match registry.fetch_by_id(plugin_id).await {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(cog_core::SFError::Agent(format!(
                                "Failed to fetch plugin '{}': {}",
                                plugin_id, e
                            )));
                        }
                    }
                } else {
                    return Err(cog_core::SFError::Agent(
                        "PluginRegistry not configured for WASM tool".into(),
                    ));
                };
                let req = SandboxRequest {
                    task_id: scope
                        .map(|s| s.task_id.clone())
                        .unwrap_or_else(|| format!("unscoped-tool-{}", name)),
                    agent_id: scope
                        .map(|s| s.agent_id.clone())
                        .unwrap_or_else(|| plugin_id.clone()),
                    payload: SandboxPayload::Wasm {
                        bytes,
                        entry: export_name.clone(),
                    },
                    input: arguments,
                    timeout: self.wasm_timeout,
                    limits: Default::default(),
                };
                let result = backend.execute(&req).await?;
                Ok(result.into_json())
            }
            ToolImplementation::Shell(op) => {
                if scope.is_none() && self.require_identity {
                    return Err(cog_core::SFError::Agent(format!(
                        "tool '{name}' requires task identity (require_tool_identity=true); \
                         call through prompt_for_task"
                    )));
                }
                let backend = self.sandbox_backend.as_ref().ok_or_else(|| {
                    cog_core::SFError::Agent("SandboxBackend not configured for shell tool".into())
                })?;
                let payload =
                    match op {
                        cog_core::ShellOp::Command => {
                            let command = arguments
                                .get("command")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    cog_core::SFError::Validation("command required".into())
                                })?;
                            if command.trim().is_empty() {
                                return Err(cog_core::SFError::Validation("empty command".into()));
                            }
                            SandboxPayload::Command {
                                command: command.to_string(),
                            }
                        }
                        cog_core::ShellOp::ReadFile => {
                            let path = arguments.get("path").and_then(|v| v.as_str()).ok_or_else(
                                || cog_core::SFError::Validation("path required".into()),
                            )?;
                            SandboxPayload::ReadFile {
                                path: path.to_string(),
                            }
                        }
                        cog_core::ShellOp::WriteFile => {
                            let path = arguments.get("path").and_then(|v| v.as_str()).ok_or_else(
                                || cog_core::SFError::Validation("path required".into()),
                            )?;
                            let content = arguments
                                .get("content")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    cog_core::SFError::Validation("content required".into())
                                })?;
                            SandboxPayload::WriteFile {
                                path: path.to_string(),
                                content: content.to_string(),
                            }
                        }
                    };
                let req = SandboxRequest {
                    task_id: scope
                        .map(|s| s.task_id.clone())
                        .unwrap_or_else(|| format!("unscoped-shell-{}", uuid::Uuid::new_v4())),
                    agent_id: scope
                        .map(|s| s.agent_id.clone())
                        .unwrap_or_else(|| format!("unscoped-tool-{}", name)),
                    payload,
                    input: arguments,
                    timeout: self.shell_timeout,
                    limits: Default::default(),
                };
                let mut stream = backend.execute_stream(&req).await?;
                let mut stdout = String::new();
                let mut stderr = String::new();
                let mut code = 0;
                while let Some(event) = stream.next().await {
                    match event {
                        CommandEvent::Stdout { data } => stdout.push_str(&data),
                        CommandEvent::Stderr { data } => stderr.push_str(&data),
                        CommandEvent::Exit { code: c } => code = c,
                    }
                }
                match op {
                    cog_core::ShellOp::Command => Ok(serde_json::json!({
                        "stdout": stdout,
                        "stderr": stderr,
                        "code": code,
                    })),
                    cog_core::ShellOp::ReadFile => {
                        if code == 0 {
                            Ok(serde_json::json!({ "content": stdout }))
                        } else {
                            Err(cog_core::SFError::IO(stderr))
                        }
                    }
                    cog_core::ShellOp::WriteFile => {
                        if code == 0 {
                            Ok(serde_json::json!({ "success": true }))
                        } else {
                            Err(cog_core::SFError::IO(stderr))
                        }
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tools.read().unwrap().is_empty()
    }

    /// The names in `allowed` this registry has no tool behind, in the order
    /// they were given, each once.
    ///
    /// Narrowing drops these without a word; naming them is what turns "this
    /// role has fewer tools than its skill says" from an absence into a reading.
    pub fn missing_tools(&self, allowed: &[String]) -> Vec<String> {
        let source = self.tools.read().unwrap();
        let mut missing: Vec<String> = Vec::new();
        for name in allowed {
            if !source.contains_key(name) && !missing.contains(name) {
                missing.push(name.clone());
            }
        }
        missing
    }

    /// A registry holding only the named tools, over the same execution
    /// machinery — sandbox backend, guardrail, plugin registry, timeouts — so
    /// a narrowed registry runs a tool exactly as the full one would.
    ///
    /// This is how a role's tool boundary is enforced, and it is enforced in
    /// both directions: the model is offered only these definitions, and a call
    /// it invents for anything else has nothing behind it to reach. A boundary
    /// that only hid the definitions would be a suggestion.
    ///
    /// A name with no tool behind it is dropped rather than refused. The list
    /// states what the role is allowed to do; a name that matches nothing is a
    /// stale entry in that statement, and failing to build the registry over it
    /// would turn a tidying omission into an outage. What keeps the statement
    /// honest is a gate over the shipped lists, not a runtime panic.
    ///
    /// Which is exactly why the drops have to be reportable: a gate over the
    /// lists in the repository says nothing about which lists a running
    /// deployment loaded. [`Self::missing_tools`] is that report.
    pub fn restricted_to(&self, allowed: &[String]) -> Self {
        let mut kept = HashMap::new();
        {
            let source = self.tools.read().unwrap();
            for name in allowed {
                if let Some(tool) = source.get(name) {
                    kept.insert(name.clone(), tool.clone());
                }
            }
        }
        Self {
            tools: Arc::new(std::sync::RwLock::new(kept)),
            sandbox_backend: self.sandbox_backend.clone(),
            guardrail: self.guardrail.clone(),
            plugin_registry: self.plugin_registry.clone(),
            wasm_timeout: self.wasm_timeout,
            shell_timeout: self.shell_timeout,
            require_identity: self.require_identity,
        }
    }
}

#[async_trait::async_trait]
impl cog_core::ToolExecutor for ToolRegistry {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> cog_core::SFResult<serde_json::Value> {
        self.execute(name, arguments).await
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    #[derive(Debug)]
    struct StubHttp;

    #[async_trait::async_trait]
    impl cog_core::HttpClient for StubHttp {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: format!("{} {}", req.method, req.url).into_bytes(),
            })
        }
    }

    #[tokio::test]
    async fn http_request_tool_executes_via_client() {
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(
            &registry,
            builtins::http_request(Arc::new(StubHttp), None, 8192),
        );
        let out = registry
            .execute(
                "http_request",
                serde_json::json!({"url": "http://example/x", "method": "post"}),
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["body"], "POST http://example/x");
    }

    /// Answers the fetch with a body of a chosen size, and the memory API's
    /// ingest endpoint with a chosen status, so both the store-accepted and the
    /// store-refused branch of `http_request` can be exercised off one double.
    #[derive(Debug)]
    struct FetchStub {
        body_chars: usize,
        ingest_status: u16,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for FetchStub {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            if req.url.contains(cog_core::MEMORY_INGEST_PATH) {
                return Ok(cog_core::HttpResponse {
                    status: self.ingest_status,
                    headers: Default::default(),
                    body: b"{}".to_vec(),
                });
            }
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: "x".repeat(self.body_chars).into_bytes(),
            })
        }
    }

    fn fetch_registry(stub: FetchStub, threshold: usize) -> ToolRegistry {
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(
            &registry,
            builtins::http_request(Arc::new(stub), Some("http://mem:8080".into()), threshold),
        );
        registry
    }

    #[tokio::test]
    async fn http_request_keeps_a_small_body_inline() {
        let registry = fetch_registry(
            FetchStub {
                body_chars: 64,
                ingest_status: 200,
            },
            8192,
        );
        let out = registry
            .execute("http_request", serde_json::json!({"url": "http://api/x"}))
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["body"].as_str().unwrap().chars().count(), 64);
        assert!(out.get("artifact").is_none());
    }

    #[tokio::test]
    async fn http_request_stores_a_large_body_and_references_it() {
        let registry = fetch_registry(
            FetchStub {
                body_chars: 10_000,
                ingest_status: 200,
            },
            100,
        );
        let out = registry
            .execute(
                "http_request",
                serde_json::json!({"url": "http://host.example/page"}),
            )
            .await
            .unwrap();
        assert_eq!(out["status"], 200);
        assert_eq!(out["body_chars"], 10_000);
        // The body is gone from the turn; only the head of it survives as a
        // preview, and the reference is what the rest is fetched back through.
        assert!(out.get("body").is_none());
        assert_eq!(out["preview"].as_str().unwrap().chars().count(), 100);
        let artifact = out["artifact"].as_str().unwrap();
        assert!(
            artifact.starts_with("artifact://default/external-host.example-"),
            "{artifact}"
        );
    }

    #[tokio::test]
    async fn http_request_falls_back_to_inline_when_the_store_refuses() {
        let registry = fetch_registry(
            FetchStub {
                body_chars: 10_000,
                ingest_status: 500,
            },
            100,
        );
        let out = registry
            .execute(
                "http_request",
                serde_json::json!({"url": "http://host.example/page"}),
            )
            .await
            .unwrap();
        // Refused store, no cut: the document comes back whole, which is the
        // behaviour this tool had before there was a threshold at all.
        assert_eq!(out["status"], 200);
        assert_eq!(out["body"].as_str().unwrap().chars().count(), 10_000);
        assert!(out.get("artifact").is_none());
    }

    #[test]
    fn url_host_drops_scheme_userinfo_port_and_path() {
        assert_eq!(
            super::builtins::url_host("http://example.com/a?b#c"),
            "example.com"
        );
        assert_eq!(
            super::builtins::url_host("https://user:pw@host.example:8443/x"),
            "host.example"
        );
        assert_eq!(super::builtins::url_host("example.com"), "example.com");
        assert_eq!(
            super::builtins::url_host("http://[2001:db8::1]:80/x"),
            "2001:db8::1"
        );
        assert_eq!(super::builtins::url_host(""), "unknown");
    }

    fn raw_source(
        id: &str,
        namespace: &str,
        content_type: &str,
        payload: &[u8],
    ) -> cog_core::RawSource {
        let now = chrono::Utc::now();
        cog_core::RawSource {
            id: id.into(),
            namespace: namespace.into(),
            content_type: content_type.into(),
            payload: payload.to_vec(),
            tags: Vec::new(),
            created_at: now,
            archived_at: now,
        }
    }

    /// Stands in for the platform memory API's raw routes: the listing, the
    /// metadata read, and the content read, answered off the path the tool
    /// asked for. The content read encodes the payload the same way the API
    /// does, so the tool's pass-through is exercised end to end.
    #[derive(Debug)]
    struct MemoryApiStub {
        raws: Vec<cog_core::RawSource>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for MemoryApiStub {
        async fn execute(&self, req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            let not_found = || cog_core::HttpResponse {
                status: 404,
                headers: Default::default(),
                body: b"{\"error\":\"not found\"}".to_vec(),
            };
            // Keep only the API-relative path, dropping any scheme/host and query.
            let path = req
                .url
                .split_once("/api/v1/memory")
                .map(|(_, rest)| format!("/api/v1/memory{rest}"))
                .unwrap_or_default();
            let path_only = path.split_once('?').map(|(p, _)| p).unwrap_or(&path);
            let find = |id: &str| self.raws.iter().find(|r| r.id == id);

            let body = if path_only == "/api/v1/memory/raw" {
                let items: Vec<serde_json::Value> = self
                    .raws
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "content_type": r.content_type,
                            "payload_length": r.payload.len(),
                            "created_at": r.created_at,
                        })
                    })
                    .collect();
                serde_json::json!({ "items": items })
            } else if let Some(id) = path_only
                .strip_prefix("/api/v1/memory/raw/")
                .and_then(|rest| rest.strip_suffix("/content"))
            {
                let Some(r) = find(id) else {
                    return Ok(not_found());
                };
                let (encoding, payload) = match std::str::from_utf8(&r.payload) {
                    Ok(text) => ("utf8", text.to_string()),
                    Err(_) => (
                        "base64",
                        base64::engine::general_purpose::STANDARD.encode(&r.payload),
                    ),
                };
                serde_json::json!({
                    "id": r.id,
                    "namespace": r.namespace,
                    "content_type": r.content_type,
                    "payload_length": r.payload.len(),
                    "encoding": encoding,
                    "payload": payload,
                    "created_at": r.created_at,
                })
            } else if let Some(id) = path_only.strip_prefix("/api/v1/memory/raw/") {
                let Some(r) = find(id) else {
                    return Ok(not_found());
                };
                serde_json::json!({
                    "id": r.id,
                    "content_type": r.content_type,
                    "payload_length": r.payload.len(),
                    "created_at": r.created_at,
                })
            } else {
                return Ok(not_found());
            };
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: serde_json::to_vec(&body).unwrap(),
            })
        }
    }

    #[derive(Debug, Default)]
    struct Counters(std::sync::Mutex<Vec<(cog_core::MetricName, HashMap<String, String>)>>);

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for Counters {
        async fn record_gauge(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            _value: f64,
            labels: HashMap<String, String>,
        ) -> SFResult<()> {
            self.0.lock().unwrap().push((name, labels));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> SFResult<()> {
            Ok(())
        }
    }

    fn memory_registry(
        base: Option<&str>,
        raws: Vec<cog_core::RawSource>,
        metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    ) -> ToolRegistry {
        let client: Arc<dyn cog_core::HttpClient> = Arc::new(MemoryApiStub { raws });
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(
            &registry,
            builtins::raw_list(Some(client.clone()), base.map(str::to_string)),
        );
        cog_core::ToolRegistry::register(
            &registry,
            builtins::raw_fetch(Some(client), base.map(str::to_string), metrics),
        );
        registry
    }

    /// The read path the whole raw layer had no caller for: a tool that asks for
    /// an id gets the archived bytes back.
    #[tokio::test]
    async fn raw_fetch_returns_the_archived_bytes() {
        let registry = memory_registry(
            Some("http://mem"),
            vec![
                raw_source("notes", "default", "text/plain", b"hello raw"),
                raw_source(
                    "blob",
                    "default",
                    "application/octet-stream",
                    &[0xff, 0x00, 0xfe],
                ),
            ],
            None,
        );

        let listed = registry
            .execute("raw_list", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(listed["namespace"], cog_core::DEFAULT_MEMORY_NAMESPACE);
        assert_eq!(listed["count"], 2);
        // The listing carries each source's shape, so a caller can pick what to
        // read without first paying for a read per candidate.
        assert_eq!(listed["items"][0]["id"], "notes");
        assert_eq!(listed["items"][0]["content_type"], "text/plain");
        assert_eq!(listed["items"][0]["payload_length"], 9);

        let fetched = registry
            .execute("raw_fetch", serde_json::json!({"id": "notes"}))
            .await
            .unwrap();
        assert_eq!(fetched["encoding"], "utf8");
        assert_eq!(fetched["payload"], "hello raw");
        assert_eq!(fetched["content_type"], "text/plain");
        assert_eq!(fetched["payload_length"], 9);

        // A payload that is not text still comes back whole, with the encoding
        // named so the caller never has to guess which form it holds.
        let blob = registry
            .execute("raw_fetch", serde_json::json!({"id": "blob"}))
            .await
            .unwrap();
        assert_eq!(blob["encoding"], "base64");
        assert_eq!(blob["payload"], "/wD+");
        assert_eq!(blob["payload_length"], 3);
    }

    /// An unconfigured memory API must fail the call, not return an empty
    /// listing. "Nowhere to ask" and "this namespace holds nothing" are
    /// different facts, and a caller that cannot tell them apart reads the first
    /// as the second.
    #[tokio::test]
    async fn raw_tools_report_an_unconfigured_api_instead_of_an_empty_one() {
        let registry = memory_registry(None, Vec::new(), None);
        let list_err = registry
            .execute("raw_list", serde_json::json!({}))
            .await
            .expect_err("an unconfigured API must not answer with an empty listing");
        let fetch_err = registry
            .execute("raw_fetch", serde_json::json!({"id": "x"}))
            .await
            .expect_err("an unconfigured API must not answer with a not-found");
        for err in [list_err, fetch_err] {
            let text = err.to_string();
            assert!(
                text.contains(MEMORY_API_BASE_ENV),
                "the error must name the unset base, got: {text}"
            );
        }
    }

    /// The id is checked before it reaches the URL: an id carrying a separator
    /// becomes a subtree at the store and the object quietly stops being listed,
    /// so the caller is told at the call that caused it.
    #[tokio::test]
    async fn raw_fetch_rejects_an_id_that_cannot_be_a_key() {
        let registry = memory_registry(Some("http://mem"), Vec::new(), None);
        let err = registry
            .execute("raw_fetch", serde_json::json!({"id": "a/b"}))
            .await
            .expect_err("a separator in the id must be rejected");
        assert!(err.to_string().contains("not addressable"), "got: {err}");
    }

    /// A payload over budget is an explicit outcome, not a silent truncation
    /// and not an error: the caller set the budget, so it is told the source is
    /// larger and no bytes are moved -- and the event is counted, so a bounded
    /// read is visible outside the tool result.
    #[tokio::test]
    async fn raw_fetch_reports_budget_exhaustion_rather_than_truncating() {
        let counters = Arc::new(Counters::default());
        let registry = memory_registry(
            Some("http://mem"),
            vec![raw_source("big", "default", "text/plain", b"0123456789")],
            Some(counters.clone()),
        );

        let out = registry
            .execute("raw_fetch", serde_json::json!({"id": "big", "budget": 4}))
            .await
            .unwrap();
        assert_eq!(out["budget_exhausted"], true);
        assert_eq!(out["payload_length"], 10);
        assert_eq!(out["budget"], 4);
        assert!(
            out.get("payload").is_none(),
            "a bounded read must not hand back bytes it did not move: {out}"
        );

        let recorded = counters.0.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL);
        assert_eq!(
            recorded[0].1.get("tool").map(String::as_str),
            Some("raw_fetch")
        );
    }

    /// A budget large enough for the payload moves the bytes: the bound is a
    /// ceiling, not a filter that drops everything.
    #[tokio::test]
    async fn raw_fetch_moves_the_bytes_when_the_budget_allows() {
        let registry = memory_registry(
            Some("http://mem"),
            vec![raw_source("small", "default", "text/plain", b"hello")],
            None,
        );
        let out = registry
            .execute(
                "raw_fetch",
                serde_json::json!({"id": "small", "budget": 16}),
            )
            .await
            .unwrap();
        assert_eq!(out["payload"], "hello");
        assert!(out.get("budget_exhausted").is_none());
    }

    #[tokio::test]
    async fn file_tools_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cog-tools-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.txt");
        let path = path.to_str().unwrap();

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());
        cog_core::ToolRegistry::register(&registry, builtins::write_file());

        registry
            .execute(
                "write_file",
                serde_json::json!({"path": path, "content": "hello"}),
            )
            .await
            .unwrap();
        let out = registry
            .execute("read_file", serde_json::json!({"path": path}))
            .await
            .unwrap();
        assert_eq!(out["content"], "hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The tool list a request carries must come out in name order, whatever
    /// order the definitions were registered in.
    ///
    /// The definitions live in a HashMap, so the order it yields is arbitrary and
    /// changes with the process. This list is serialized into every LLM request:
    /// a reshuffle re-sends a block that did not change and costs the upstream
    /// prefix cache for everything after it. A small set of names can hash into
    /// alphabetical order by luck, which would make this test pass without the
    /// ordering; enough names makes that coincidence negligible.
    #[test]
    fn the_tool_list_is_in_name_order_whatever_the_registration_order() {
        const COUNT: usize = 32;
        let names = |i: usize| format!("tool_{i:02}");
        let tool = |i: usize| {
            let mut tool = builtins::read_file();
            tool.name = names(i);
            tool.description = format!("synthetic tool {i}");
            tool
        };

        let registry = ToolRegistry::new();
        // Registered in a scrambled order, so the result cannot be right by
        // matching the insertion order either.
        for step in 0..COUNT {
            cog_core::ToolRegistry::register(&registry, tool((step * 7) % COUNT));
        }

        let listed: Vec<String> = registry.list().iter().map(|t| t.name.clone()).collect();
        let expected: Vec<String> = (0..COUNT).map(names).collect();
        assert_eq!(
            listed, expected,
            "the serialized tool definitions must be in name order"
        );
    }

    /// In-process backend for tests: mirrors the production executor for all
    /// environment payloads (commands via `sh -c`, file IO via tokio::fs).
    struct LocalShellBackend;

    #[async_trait::async_trait]
    impl SandboxBackend for LocalShellBackend {
        async fn execute(&self, req: &SandboxRequest) -> SFResult<cog_core::SandboxResult> {
            let result = match &req.payload {
                SandboxPayload::Command { command } => {
                    let output = tokio::process::Command::new("sh")
                        .arg("-c")
                        .arg(command)
                        .output()
                        .await
                        .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
                    cog_core::SandboxResult {
                        stdout: String::from_utf8_lossy(&output.stdout).into(),
                        stderr: String::from_utf8_lossy(&output.stderr).into(),
                        exit_code: output.status.code().unwrap_or(-1),
                        output: None,
                        duration_ms: 0,
                        resource_usage: Default::default(),
                    }
                }
                SandboxPayload::ReadFile { path } => match tokio::fs::read_to_string(path).await {
                    Ok(content) => cog_core::SandboxResult {
                        stdout: content,
                        exit_code: 0,
                        ..Default::default()
                    },
                    Err(e) => cog_core::SandboxResult {
                        stderr: e.to_string(),
                        exit_code: 1,
                        ..Default::default()
                    },
                },
                SandboxPayload::WriteFile { path, content } => {
                    match tokio::fs::write(path, content).await {
                        Ok(()) => cog_core::SandboxResult {
                            exit_code: 0,
                            ..Default::default()
                        },
                        Err(e) => cog_core::SandboxResult {
                            stderr: e.to_string(),
                            exit_code: 1,
                            ..Default::default()
                        },
                    }
                }
                SandboxPayload::Wasm { .. } => {
                    return Err(cog_core::SFError::Agent("unsupported payload".into()));
                }
            };
            Ok(result)
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Err(cog_core::SFError::Agent("unsupported".into()))
        }
    }

    #[tokio::test]
    async fn run_command_supports_full_shell_syntax() {
        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::run_command());
        let out = registry
            .execute(
                "run_command",
                serde_json::json!({"command": "echo hello world | tr 'a-z' 'A-Z'"}),
            )
            .await
            .unwrap();
        assert_eq!(out["code"], 0);
        assert_eq!(out["stdout"].as_str().unwrap().trim(), "HELLO WORLD");

        let dir = std::env::temp_dir().join(format!("cog-tool-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("task.py");
        std::fs::write(&script, "print(sum(range(10)))").unwrap();
        let out = registry
            .execute(
                "run_command",
                serde_json::json!({"command": format!("python3 {}", script.display())}),
            )
            .await
            .unwrap();
        assert_eq!(out["stdout"].as_str().unwrap().trim(), "45");
        let _ = std::fs::remove_dir_all(&dir);

        assert!(registry
            .execute("run_command", serde_json::json!({"command": "   "}))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn run_command_requires_sandbox_backend() {
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(&registry, builtins::run_command());
        let err = registry
            .execute("run_command", serde_json::json!({"command": "echo hi"}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("SandboxBackend not configured"));
    }

    /// Records the identity and budget stamped on every request so tests can
    /// assert what the sandbox plane actually sees.
    #[derive(Default)]
    struct RecordingBackend {
        seen: tokio::sync::Mutex<Vec<(String, String, Duration)>>,
    }

    #[async_trait::async_trait]
    impl SandboxBackend for RecordingBackend {
        async fn execute(&self, req: &SandboxRequest) -> SFResult<cog_core::SandboxResult> {
            self.seen
                .lock()
                .await
                .push((req.task_id.clone(), req.agent_id.clone(), req.timeout));
            Ok(cog_core::SandboxResult {
                exit_code: 0,
                ..Default::default()
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Err(cog_core::SFError::Agent("unsupported".into()))
        }
    }

    struct StubPluginRegistry;

    #[async_trait::async_trait]
    impl cog_core::PluginRegistry for StubPluginRegistry {
        async fn discover(&self, _source: &str) -> SFResult<Vec<cog_core::PluginManifest>> {
            Ok(vec![])
        }
        async fn fetch(&self, _manifest: &cog_core::PluginManifest) -> SFResult<Vec<u8>> {
            Ok(vec![])
        }
        async fn load(
            &self,
            _bytes: &[u8],
            _manifest: &cog_core::PluginManifest,
        ) -> SFResult<cog_core::PluginHandle> {
            Err(cog_core::SFError::Agent("not loadable".into()))
        }
        async fn unload(&self, _handle: &cog_core::PluginHandle) -> SFResult<()> {
            Ok(())
        }
        async fn fetch_by_id(&self, _plugin_id: &str) -> SFResult<Vec<u8>> {
            Ok(vec![])
        }
    }

    fn scope(task_id: &str, agent_id: &str) -> ToolScope {
        ToolScope {
            task_id: task_id.into(),
            agent_id: agent_id.into(),
        }
    }

    #[tokio::test]
    async fn scoped_shell_carries_real_task_identity() {
        let backend = Arc::new(RecordingBackend::default());
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        registry
            .execute_scoped(
                "run_command",
                serde_json::json!({"command": "echo hi"}),
                Some(&scope("dag-task-7", "worker-3")),
            )
            .await
            .unwrap();

        let seen = backend.seen.lock().await.clone();
        assert_eq!(
            seen,
            vec![(
                "dag-task-7".into(),
                "worker-3".into(),
                Duration::from_secs(600)
            )]
        );
    }

    #[tokio::test]
    async fn shell_commands_get_their_own_budget_not_the_wasm_one() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_wasm_timeout(30)
            .with_shell_timeout(600)
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        registry
            .execute("run_command", serde_json::json!({"command": "cargo check"}))
            .await
            .unwrap();

        let seen = backend.seen.lock().await.clone();
        assert_eq!(seen.len(), 1);
        // A command may be a compiler run; the WASM snippet budget cannot serve it.
        assert_eq!(seen[0].2, Duration::from_secs(600));
    }

    #[tokio::test]
    async fn unscoped_shell_gets_distinct_synthetic_identity() {
        let backend = Arc::new(RecordingBackend::default());
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        registry
            .execute("run_command", serde_json::json!({"command": "echo hi"}))
            .await
            .unwrap();

        let seen = backend.seen.lock().await.clone();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].0.starts_with("unscoped-shell-"));
        assert_eq!(seen[0].1, "unscoped-tool-run_command");
    }

    #[tokio::test]
    async fn require_identity_rejects_unscoped_shell_but_allows_scoped() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>)
            .with_require_identity(true);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        let err = registry
            .execute("run_command", serde_json::json!({"command": "echo hi"}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires task identity"));
        assert!(backend.seen.lock().await.is_empty());

        registry
            .execute_scoped(
                "run_command",
                serde_json::json!({"command": "echo hi"}),
                Some(&scope("dag-task-9", "worker-1")),
            )
            .await
            .unwrap();
        let seen = backend.seen.lock().await.clone();
        assert_eq!(
            seen,
            vec![(
                "dag-task-9".into(),
                "worker-1".into(),
                Duration::from_secs(600)
            )]
        );
    }

    #[tokio::test]
    async fn scope_does_not_leak_between_runs() {
        let backend = Arc::new(RecordingBackend::default());
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        for (task, agent) in [("dag-a", "w-1"), ("dag-b", "w-2")] {
            registry
                .execute_scoped(
                    "run_command",
                    serde_json::json!({"command": "echo hi"}),
                    Some(&scope(task, agent)),
                )
                .await
                .unwrap();
        }

        let seen = backend.seen.lock().await.clone();
        assert_eq!(
            seen,
            vec![
                ("dag-a".into(), "w-1".into(), Duration::from_secs(600)),
                ("dag-b".into(), "w-2".into(), Duration::from_secs(600))
            ]
        );
    }

    #[tokio::test]
    async fn require_identity_does_not_reject_unscoped_wasm() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>)
            .with_plugin_registry(Arc::new(StubPluginRegistry))
            .with_require_identity(true);
        cog_core::ToolRegistry::register(
            &registry,
            Tool {
                name: "wasm_echo".into(),
                description: "test wasm tool".into(),
                parameters: serde_json::json!({"type": "object"}),
                implementation: ToolImplementation::Wasm {
                    plugin_id: "plugin-42".into(),
                    export_name: "run".into(),
                },
            },
        );

        registry
            .execute("wasm_echo", serde_json::json!({}))
            .await
            .unwrap();

        let seen = backend.seen.lock().await.clone();
        // A WASM snippet keeps the tight snippet budget.
        assert_eq!(
            seen,
            vec![(
                "unscoped-tool-wasm_echo".into(),
                "plugin-42".into(),
                Duration::from_secs(30)
            )]
        );
    }
}

impl cog_core::ToolRegistry for ToolRegistry {
    fn register(&self, tool: cog_core::Tool) {
        self.tools.write().unwrap().insert(tool.name.clone(), tool);
    }
}

/// 内置工具工厂。
pub mod builtins {
    use super::*;

    pub fn read_file() -> Tool {
        Tool {
            name: "read_file".into(),
            description: "Read the contents of a file. Relative paths resolve against \
                          the task's working directory, which is the repository root \
                          when a checkout is provisioned."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path" }
                },
                "required": ["path"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::ReadFile),
        }
    }

    pub fn write_file() -> Tool {
        Tool {
            name: "write_file".into(),
            description: "Write content to a file. Relative paths resolve against the \
                          task's working directory."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::WriteFile),
        }
    }

    pub fn run_command() -> Tool {
        Tool {
            name: "run_command".into(),
            description: "Run a shell command with full shell syntax: pipes, redirects, \
                          variable expansion, and scripts written via write_file are all \
                          supported. The command runs in the task's working directory; when \
                          a repository checkout is provisioned it is already rooted there, \
                          so work on the repo in place instead of cloning it again."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Command to run" }
                },
                "required": ["command"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::Command),
        }
    }

    pub fn search_code() -> Tool {
        Tool {
            name: "search_code".into(),
            description: "Search for code patterns in the codebase".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query" }
                },
                "required": ["query"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let query = args["query"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("query required".into()))?;
                    // Placeholder - actual implementation would use ripgrep or similar
                    Ok(serde_json::json!({ "results": [], "query": query }))
                })
            })),
        }
    }

    /// Percent-encode one path or query segment so an id or prefix reaches the
    /// API byte-for-byte as the caller wrote it. The unreserved set passes
    /// through; anything that could end the segment or start a new query
    /// parameter is escaped, which is enough for both a path segment and a
    /// query value.
    fn encode_url_component(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for b in value.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// The platform memory API base URL, or an error naming what is missing.
    ///
    /// The tools read over HTTP rather than resolving the backend in-process:
    /// the process that runs the loops holds no memory dataset on purpose, so an
    /// in-process handle is absent exactly where a squad runs, while the API's
    /// internal face is reachable with no credentials. An unconfigured base is a
    /// loud error: "nowhere to ask" and "this source does not exist" are
    /// different facts, and a caller that folds them reads an unwired layer as
    /// an empty one.
    fn memory_base(base: Option<&str>) -> SFResult<String> {
        base.filter(|b| !b.trim().is_empty())
            .map(|b| b.trim_end_matches('/').to_string())
            .ok_or_else(|| {
                SFError::Config(format!(
                    "{MEMORY_API_BASE_ENV} is not set: there is no memory API to read from"
                ))
            })
    }

    /// The HTTP client the tools read through, or an error naming what is
    /// missing. The tools are registered whether or not this process holds one:
    /// a tool that resolves to a configuration error tells a squad "no layer
    /// here", while a tool that was never registered at all would look like
    /// "this namespace is empty".
    fn memory_client(client: Option<Arc<dyn HttpClient>>) -> SFResult<Arc<dyn HttpClient>> {
        client.ok_or_else(|| {
            SFError::Config("no HTTP client is available to reach the memory API".into())
        })
    }

    /// GET `url` and decode the JSON body. A non-success status is an error
    /// carrying the status and the API's own body, so a caller sees the refusal
    /// rather than an empty object.
    async fn get_json(client: &Arc<dyn HttpClient>, url: &str) -> SFResult<serde_json::Value> {
        let mut req = HttpRequest::new("GET", url);
        req.timeout_secs = Some(MEMORY_READ_TIMEOUT_SECS);
        let resp = client.execute(req).await?;
        if !resp.is_success() {
            return Err(SFError::Agent(format!(
                "memory API answered {} for {url}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        serde_json::from_slice(&resp.body)
            .map_err(|e| SFError::Agent(format!("memory API returned unreadable JSON: {e}")))
    }

    /// List archived raw sources with the metadata a caller needs to choose
    /// which to read.
    ///
    /// Reads over HTTP from the platform memory API. The in-process backend this
    /// used before is switched off in exactly the process that runs squads, so
    /// it was absent where the tool was needed; the API's internal face carries
    /// no token and is reachable by a credential-free pod. An unconfigured base
    /// fails loudly rather than returning an empty list, so a deployment with no
    /// memory API to ask is not read as a namespace that holds nothing.
    pub fn raw_list(client: Option<Arc<dyn HttpClient>>, base: Option<String>) -> Tool {
        Tool {
            name: "raw_list".into(),
            description: "List archived raw sources in the shared memory namespace, \
                          optionally filtered by content-type prefix. Each item carries its \
                          content type, payload length and creation time so a caller can \
                          choose what to read within its budget; call raw_fetch for the \
                          bytes. Fails with an error when no memory API base is configured."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "content_type_prefix": {
                        "type": "string",
                        "description": "Only ids whose content type starts with this"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Cap on how many ids to return"
                    }
                }
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let client = client.clone();
                let base = base.clone();
                Box::pin(async move {
                    let base = memory_base(base.as_deref())?;
                    let client = memory_client(client)?;
                    let mut url = format!("{base}{MEMORY_RAW_PATH}");
                    let mut query: Vec<String> = Vec::new();
                    if let Some(prefix) = args["content_type_prefix"].as_str() {
                        query.push(format!("prefix={}", encode_url_component(prefix)));
                    }
                    if let Some(limit) = args["limit"].as_u64() {
                        query.push(format!("limit={limit}"));
                    }
                    if !query.is_empty() {
                        url.push('?');
                        url.push_str(&query.join("&"));
                    }
                    let body = get_json(&client, &url).await?;
                    let items = body["items"].as_array().cloned().unwrap_or_default();
                    let mut out = serde_json::json!({
                        "namespace": DEFAULT_MEMORY_NAMESPACE,
                        "count": items.len(),
                        "items": items,
                    });
                    // The store's full size, when the API trimmed the listing to
                    // `limit`: without it a capped read is indistinguishable
                    // from a complete one.
                    if let Some(total) = body["total"].as_u64() {
                        out["total"] = serde_json::json!(total);
                    }
                    Ok(out)
                })
            })),
        }
    }

    /// Fetch one archived raw source, bytes included, bounded by a byte budget.
    ///
    /// Two calls: the metadata route first for `payload_length`, then the
    /// content route only when the payload fits the budget. The order is the
    /// point -- a route that returned the bytes first would make the budget
    /// meaningless, because the transfer is spent by the time the size is known.
    /// A payload over budget is not an unexpected error: it is the budget
    /// working, so it comes back as an explicit outcome with its own reading,
    /// not as a silent truncation -- which a caller could not tell from a
    /// complete read.
    ///
    /// The id is checked against the contract's key rules before it reaches the
    /// URL: an id carrying a separator does not fail at the store, it becomes a
    /// subtree and the object disappears from every listing that would have
    /// counted it. Reporting that here turns a silently missing key into an
    /// error at the call that caused it.
    pub fn raw_fetch(
        client: Option<Arc<dyn HttpClient>>,
        base: Option<String>,
        metrics: Option<Arc<dyn MetricsBackend>>,
    ) -> Tool {
        Tool {
            name: "raw_fetch".into(),
            description: "Fetch one archived raw source by id, returning its bytes when \
                          they fit the byte budget. The payload is UTF-8 text when the \
                          source is text and base64 otherwise; `encoding` says which. Over \
                          budget, the result carries `budget_exhausted` and no bytes. Fails \
                          with an error when no memory API base is configured."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Raw source id" },
                    "budget": {
                        "type": "integer",
                        "description": "Maximum payload bytes to move (default 262144)"
                    }
                },
                "required": ["id"]
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let client = client.clone();
                let base = base.clone();
                let metrics = metrics.clone();
                Box::pin(async move {
                    let id = args["id"]
                        .as_str()
                        .ok_or_else(|| SFError::Validation("id required".into()))?;
                    if let Some(why) = cog_core::raw_id_key_error(id) {
                        return Err(SFError::Validation(format!(
                            "raw id {:?} is not addressable: {}",
                            id, why
                        )));
                    }
                    let base = memory_base(base.as_deref())?;
                    let client = memory_client(client)?;
                    let segment = encode_url_component(id);
                    let budget = args["budget"]
                        .as_u64()
                        .unwrap_or(DEFAULT_RAW_FETCH_BUDGET_BYTES);

                    let item_url =
                        format!("{base}{}", MEMORY_RAW_ITEM_PATH.replace("{id}", &segment));
                    let meta = get_json(&client, &item_url).await?;
                    let payload_length = meta["payload_length"].as_u64().unwrap_or(0);
                    if payload_length > budget {
                        // The budget did its job: hand back an outcome a caller
                        // cannot mistake for the bytes, and count it so the
                        // bounded read is visible outside the tool result.
                        if let Some(metrics) = metrics.as_ref() {
                            let mut labels = HashMap::new();
                            labels.insert("tool".to_string(), "raw_fetch".to_string());
                            if let Err(e) = metrics
                                .record_counter(
                                    TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL,
                                    1.0,
                                    labels,
                                )
                                .await
                            {
                                tracing::warn!(
                                    error = %e,
                                    "raw_fetch: could not record budget-exhausted outcome"
                                );
                            }
                        }
                        return Ok(serde_json::json!({
                            "id": id,
                            "content_type": meta["content_type"],
                            "payload_length": payload_length,
                            "budget": budget,
                            "budget_exhausted": true,
                        }));
                    }

                    let content_url = format!(
                        "{base}{}",
                        MEMORY_RAW_CONTENT_PATH.replace("{id}", &segment)
                    );
                    get_json(&client, &content_url).await
                })
            })),
        }
    }

    /// HTTP request tool. Goes through the system's [`cog_core::HttpClient`]
    /// so proxy/TLS policy is applied uniformly; pods hold no credentials, so
    /// there is nothing in the environment for a request to exfiltrate.
    ///
    /// A response at or below `archive_threshold_chars` comes back inline, the
    /// way it always has. A larger one is stored as its own raw source under
    /// `external/<host>` and the turn carries a reference to it: material
    /// fetched from outside is not a turn of the conversation, and leaving it
    /// inside one is what made it unlistable and what let a single page push a
    /// conversation past its window. The reference is fetchable with
    /// `raw_fetch`, so nothing is lost by shortening the turn.
    ///
    /// When the store refuses the write the body comes back inline instead. The
    /// document was never cut, so that path loses nothing: it is the behaviour
    /// this tool had before there was a threshold at all. There is therefore no
    /// reading of its own for the failed store — the caller sees it in the
    /// result shape and the pod's log, which is what a fate that loses no bytes
    /// warrants.
    pub fn http_request(
        client: Arc<dyn cog_core::HttpClient>,
        memory_api_base: Option<String>,
        archive_threshold_chars: usize,
    ) -> Tool {
        Tool {
            name: "http_request".into(),
            description: "Make an HTTP request and return the status and body; a body too \
                          large to be a turn is stored instead, and the result names it"
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "Request URL" },
                    "method": { "type": "string", "description": "HTTP method (default GET)" },
                    "headers": { "type": "object", "description": "Optional header map" },
                    "body": { "type": "string", "description": "Optional request body" }
                },
                "required": ["url"]
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let client = client.clone();
                let memory_api_base = memory_api_base.clone();
                Box::pin(async move {
                    let url = args["url"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("url required".into()))?;
                    let method = args["method"]
                        .as_str()
                        .unwrap_or("GET")
                        .to_ascii_uppercase();
                    let mut req = cog_core::HttpRequest::new(method, url);
                    req.timeout_secs = Some(30);
                    if let Some(headers) = args["headers"].as_object() {
                        for (k, v) in headers {
                            if let Some(v) = v.as_str() {
                                req.headers.insert(k.clone(), v.to_string());
                            }
                        }
                    }
                    if let Some(body) = args["body"].as_str() {
                        req.body = Some(body.as_bytes().to_vec());
                    }
                    let resp = client.execute(req).await?;
                    let body = String::from_utf8_lossy(&resp.body).to_string();
                    // The count is what decides, not the byte length: the two
                    // are the same number for ASCII and diverge by up to four
                    // for the CJK a fetched page is likely to be full of, and
                    // the budget this is kept under is a character count.
                    let body_chars = body.chars().count();
                    if body_chars <= archive_threshold_chars {
                        return Ok(serde_json::json!({
                            "status": resp.status,
                            "body": body,
                        }));
                    }
                    let host = url_host(url);
                    let id = cog_core::bounded_raw_id("external", &host, chrono::Utc::now());
                    let content_type = format!("external/{host}");
                    match crate::archive::post_raw(
                        client.as_ref(),
                        memory_api_base.as_deref(),
                        &id,
                        &content_type,
                        &body,
                    )
                    .await
                    {
                        Ok(artifact) => Ok(serde_json::json!({
                            "status": resp.status,
                            "artifact": artifact,
                            "body_chars": body_chars,
                            "preview": body.chars().take(archive_threshold_chars).collect::<String>(),
                        })),
                        Err(failure) => {
                            tracing::warn!(
                                url,
                                id = %id,
                                error = %failure.into_error(),
                                "fetched document could not be stored; returning it inline"
                            );
                            Ok(serde_json::json!({
                                "status": resp.status,
                                "body": body,
                            }))
                        }
                    }
                })
            })),
        }
    }

    /// The host of a URL, folded for use in a raw id and a content type.
    ///
    /// Everything that can appear between the scheme and the first path
    /// separator is dropped except the host itself: userinfo would put a
    /// credential where a listing shows, and the port is a property of this
    /// deployment's route rather than of the material. IPv6 literals keep their
    /// brackets, because a host is not the place to re-open the question of
    /// which colons belong to the address.
    pub(super) fn url_host(url: &str) -> String {
        let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
        let authority = after_scheme
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        let host_port = authority.rsplit('@').next().unwrap_or_default();
        let host = if let Some(rest) = host_port.strip_prefix('[') {
            rest.split(']').next().unwrap_or_default().to_string()
        } else {
            host_port.split(':').next().unwrap_or_default().to_string()
        };
        if host.is_empty() {
            "unknown".to_string()
        } else {
            host.to_ascii_lowercase()
        }
    }
}
