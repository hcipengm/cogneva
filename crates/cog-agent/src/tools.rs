use base64::Engine as _;
use cog_core::{CommandEvent, SFResult, SandboxBackend, SandboxPayload, SandboxRequest};
use cog_core::{Tool, ToolImplementation};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The memory backend as the raw-source tools see it: a handle that resolves
/// the backend on first call instead of during plugin `init`.
///
/// The tools cannot consume the backend while the plugin is initialising: the
/// memory and agent plugins share an init layer, and a layer initialises
/// concurrently, so that read would return the backend only when agent's init
/// happens to finish second. Resolving on first call moves the read past every
/// plugin's `init`, where the registry has stopped changing.
pub type LateMemoryBackend = cog_core::LateService<dyn cog_core::MemoryBackend>;

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
        cog_core::ToolRegistry::register(&registry, builtins::http_request(Arc::new(StubHttp)));
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

    /// Minimal raw store: enough of [`cog_core::MemoryBackend`] for the two
    /// tools. Everything they do not call is `unimplemented!()`.
    struct StubMemory {
        raws: Vec<cog_core::RawSource>,
    }

    #[async_trait::async_trait]
    impl cog_core::MemoryBackend for StubMemory {
        async fn archive_raw(&self, _s: &cog_core::RawSource) -> SFResult<String> {
            unimplemented!()
        }
        async fn get_raw(&self, ns: &str, id: &str) -> SFResult<Option<cog_core::RawSource>> {
            Ok(self
                .raws
                .iter()
                .find(|r| r.namespace == ns && r.id == id)
                .cloned())
        }
        async fn list_raw(&self, ns: &str, prefix: Option<&str>) -> SFResult<Vec<String>> {
            Ok(self
                .raws
                .iter()
                .filter(|r| {
                    r.namespace == ns && prefix.is_none_or(|p| r.content_type.starts_with(p))
                })
                .map(|r| r.id.clone())
                .collect())
        }
        async fn delete_raw(&self, _n: &str, _i: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn store_schema(&self, _n: &str, _e: &cog_core::SchemaEntry) -> SFResult<()> {
            unimplemented!()
        }
        async fn get_schema(&self, _n: &str, _i: &str) -> SFResult<Option<cog_core::SchemaEntry>> {
            unimplemented!()
        }
        async fn search_schema(
            &self,
            _n: &str,
            _q: &str,
            _l: usize,
        ) -> SFResult<Vec<cog_core::SchemaSearchResult>> {
            unimplemented!()
        }
        async fn schema_for_raw(&self, _n: &str, _r: &str) -> SFResult<Vec<cog_core::SchemaEntry>> {
            unimplemented!()
        }
        async fn list_schema(&self, _n: &str) -> SFResult<Vec<cog_core::SchemaEntry>> {
            unimplemented!()
        }
        async fn delete_schema(&self, _n: &str, _i: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn query_relations(
            &self,
            _n: &str,
            _e: &str,
            _d: cog_core::RelationDirection,
            _t: Option<&str>,
        ) -> SFResult<Vec<cog_core::SchemaEntry>> {
            unimplemented!()
        }
        async fn update_schema(&self, _n: &str, _e: &cog_core::SchemaEntry) -> SFResult<()> {
            unimplemented!()
        }
        async fn store_summary(&self, _n: &str, _e: &cog_core::SummaryEntry) -> SFResult<()> {
            unimplemented!()
        }
        async fn get_summary(
            &self,
            _n: &str,
            _i: &str,
        ) -> SFResult<Option<cog_core::SummaryEntry>> {
            unimplemented!()
        }
        async fn search_summary(
            &self,
            _n: &str,
            _q: &[f32],
            _k: usize,
            _t: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
        ) -> SFResult<Vec<cog_core::SummarySearchResult>> {
            unimplemented!()
        }
        async fn summary_for_raw(
            &self,
            _n: &str,
            _r: &str,
        ) -> SFResult<Vec<cog_core::SummaryEntry>> {
            unimplemented!()
        }
        async fn list_summary(&self, _n: &str) -> SFResult<Vec<cog_core::SummaryEntry>> {
            unimplemented!()
        }
        async fn delete_summary(&self, _n: &str, _i: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn update_summary(&self, _n: &str, _e: &cog_core::SummaryEntry) -> SFResult<()> {
            unimplemented!()
        }
        fn metrics(&self) -> cog_core::MemoryMetrics {
            cog_core::MemoryMetrics::default()
        }
        async fn health_check(&self) -> SFResult<()> {
            Ok(())
        }
        async fn search_all(
            &self,
            _n: &str,
            _q: &str,
            _e: Option<&[f32]>,
            _k: usize,
            _t: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
        ) -> SFResult<Vec<cog_core::UnifiedSearchResult>> {
            unimplemented!()
        }
        async fn ingest_explicit(
            &self,
            _n: &str,
            _t: &str,
            _i: f32,
            _g: Vec<String>,
        ) -> SFResult<()> {
            unimplemented!()
        }
        async fn forget(&self, _n: &str, _i: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn decay(&self, _n: &str, _a: u64, _i: f32) -> SFResult<cog_core::DecayReport> {
            unimplemented!()
        }
    }

    fn memory_registry(backend: Option<Arc<dyn cog_core::MemoryBackend>>) -> ToolRegistry {
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        if let Some(backend) = backend {
            ctx.publish_service(backend);
        }
        let backend: LateMemoryBackend = cog_core::LateService::new(ctx.as_owner("test"));
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(&registry, builtins::raw_list(backend.clone()));
        cog_core::ToolRegistry::register(&registry, builtins::raw_fetch(backend));
        registry
    }

    /// The read path the whole raw layer had no caller for: a tool that asks for
    /// an id gets the archived bytes back.
    #[tokio::test]
    async fn raw_fetch_returns_the_archived_bytes() {
        let stub = Arc::new(StubMemory {
            raws: vec![
                raw_source("notes", "default", "text/plain", b"hello raw"),
                raw_source(
                    "blob",
                    "default",
                    "application/octet-stream",
                    &[0xff, 0x00, 0xfe],
                ),
            ],
        });
        let registry = memory_registry(Some(stub));

        let listed = registry
            .execute("raw_list", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(listed["namespace"], cog_core::DEFAULT_MEMORY_NAMESPACE);
        assert_eq!(listed["count"], 2);

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

    /// An absent memory layer must fail the call, not return an empty listing.
    /// "No layer here" and "this namespace holds nothing" are different facts,
    /// and a caller that cannot tell them apart reads the first as the second.
    #[tokio::test]
    async fn raw_tools_report_an_absent_layer_instead_of_an_empty_one() {
        let registry = memory_registry(None);
        let list_err = registry
            .execute("raw_list", serde_json::json!({}))
            .await
            .expect_err("an absent backend must not answer with an empty listing");
        let fetch_err = registry
            .execute("raw_fetch", serde_json::json!({"id": "x"}))
            .await
            .expect_err("an absent backend must not answer with a not-found");
        for err in [list_err, fetch_err] {
            let text = err.to_string();
            assert!(
                text.contains("memory backend is not available"),
                "the error must name the absent layer, got: {text}"
            );
        }
    }

    /// The id is checked before the store sees it: an id carrying a separator
    /// becomes a subtree there and the object quietly stops being listed, so
    /// the caller is told at the call that caused it.
    #[tokio::test]
    async fn raw_fetch_rejects_an_id_that_cannot_be_a_key() {
        let stub = Arc::new(StubMemory { raws: Vec::new() });
        let registry = memory_registry(Some(stub));
        let err = registry
            .execute("raw_fetch", serde_json::json!({"id": "a/b"}))
            .await
            .expect_err("a separator in the id must be rejected");
        assert!(err.to_string().contains("not addressable"), "got: {err}");
    }

    #[test]
    fn payload_encoding_names_which_form_it_returns() {
        assert_eq!(
            builtins::encode_raw_payload(b"plain"),
            ("utf8", "plain".into())
        );
        assert_eq!(
            builtins::encode_raw_payload(&[0xff, 0xfe]),
            ("base64", "//4=".into())
        );
        assert_eq!(builtins::encode_raw_payload(b""), ("utf8", String::new()));
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

    /// Namespace a memory read lands in when the call carries none.
    fn ns_arg(args: &serde_json::Value) -> String {
        args["namespace"]
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(cog_core::DEFAULT_MEMORY_NAMESPACE)
            .to_string()
    }

    /// Render a raw payload for a JSON tool result.
    ///
    /// Returns the encoding name alongside the string so the caller never has
    /// to guess which one it got: text is passed through, anything else is
    /// base64 so no byte is lost. A single field rather than both forms -- the
    /// payload can be large, and shipping it twice would double the cost of
    /// every read for the benefit of a caller that can decode one of them.
    pub fn encode_raw_payload(payload: &[u8]) -> (&'static str, String) {
        match std::str::from_utf8(payload) {
            Ok(text) => ("utf8", text.to_string()),
            Err(_) => (
                "base64",
                base64::engine::general_purpose::STANDARD.encode(payload),
            ),
        }
    }

    /// List the ids of archived raw sources.
    ///
    /// This reads the in-process memory backend instead of calling the
    /// gateway's raw route. That route is behind an operator token and pods
    /// hold no credentials, so a tool that had to authenticate would be a tool
    /// no squad could ever call; the backend is the same store the route serves
    /// from, so this reaches the same entries without crossing a credential
    /// boundary. When nothing published a backend -- the memory layer is off in
    /// this process -- the call fails loudly rather than returning an empty
    /// list: an empty list is what "this namespace holds nothing" looks like,
    /// and a caller that cannot tell an absent layer from an empty one
    /// concludes the memory is empty rather than that it was never there.
    pub fn raw_list(backend: super::LateMemoryBackend) -> Tool {
        Tool {
            name: "raw_list".into(),
            description: "List the ids of archived raw sources in a memory namespace, \
                          optionally filtered by content-type prefix. Returns ids only; \
                          call raw_fetch for the bytes. Fails with an error when this \
                          process has no memory backend."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "namespace": {
                        "type": "string",
                        "description": "Memory namespace (default: the shared default namespace)"
                    },
                    "content_type_prefix": {
                        "type": "string",
                        "description": "Only ids whose content type starts with this"
                    }
                }
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let backend = backend.clone();
                Box::pin(async move {
                    let backend = backend.get().ok_or_else(|| {
                        cog_core::SFError::Config(
                            "memory backend is not available in this process: the memory \
                             layer is disabled here, so raw sources cannot be listed"
                                .into(),
                        )
                    })?;
                    let ns = ns_arg(&args);
                    let prefix = args["content_type_prefix"].as_str();
                    let ids = backend.list_raw(&ns, prefix).await?;
                    Ok(serde_json::json!({
                        "namespace": ns,
                        "count": ids.len(),
                        "ids": ids,
                    }))
                })
            })),
        }
    }

    /// Fetch one archived raw source, bytes included.
    ///
    /// The id is checked against the contract's key rules before the store is
    /// asked: an id carrying a separator does not fail at the store, it becomes
    /// a subtree and the object disappears from every listing that would have
    /// counted it. Reporting that here turns a silently missing key into an
    /// error at the call that caused it.
    pub fn raw_fetch(backend: super::LateMemoryBackend) -> Tool {
        Tool {
            name: "raw_fetch".into(),
            description: "Fetch one archived raw source by id, returning its bytes. The \
                          payload is UTF-8 text when the source is text and base64 \
                          otherwise; `encoding` says which. Fails with an error when this \
                          process has no memory backend."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Raw source id" },
                    "namespace": {
                        "type": "string",
                        "description": "Memory namespace (default: the shared default namespace)"
                    }
                },
                "required": ["id"]
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let backend = backend.clone();
                Box::pin(async move {
                    let id = args["id"]
                        .as_str()
                        .ok_or_else(|| cog_core::SFError::Validation("id required".into()))?;
                    if let Some(why) = cog_core::raw_id_key_error(id) {
                        return Err(cog_core::SFError::Validation(format!(
                            "raw id {:?} is not addressable: {}",
                            id, why
                        )));
                    }
                    let backend = backend.get().ok_or_else(|| {
                        cog_core::SFError::Config(
                            "memory backend is not available in this process: the memory \
                             layer is disabled here, so raw sources cannot be read"
                                .into(),
                        )
                    })?;
                    let ns = ns_arg(&args);
                    let raw = backend.get_raw(&ns, id).await?.ok_or_else(|| {
                        cog_core::SFError::Validation(format!(
                            "raw source {:?} not found in namespace {:?}",
                            id, ns
                        ))
                    })?;
                    let (encoding, payload) = encode_raw_payload(&raw.payload);
                    Ok(serde_json::json!({
                        "id": raw.id,
                        "namespace": raw.namespace,
                        "content_type": raw.content_type,
                        "payload_length": raw.payload.len(),
                        "encoding": encoding,
                        "payload": payload,
                        "created_at": raw.created_at,
                    }))
                })
            })),
        }
    }

    /// HTTP request tool. Goes through the system's [`cog_core::HttpClient`]
    /// so proxy/TLS policy is applied uniformly; pods hold no credentials, so
    /// there is nothing in the environment for a request to exfiltrate.
    pub fn http_request(client: Arc<dyn cog_core::HttpClient>) -> Tool {
        Tool {
            name: "http_request".into(),
            description: "Make an HTTP request and return the status and body".into(),
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
                    Ok(serde_json::json!({
                        "status": resp.status,
                        "body": String::from_utf8_lossy(&resp.body),
                    }))
                })
            })),
        }
    }
}
