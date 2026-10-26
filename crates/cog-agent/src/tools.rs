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

/// How long an ask waits for its answer when the caller does not say, and the
/// longest a caller may ask for.
///
/// The wait happens inside the caller's turn, holding it: long enough for a
/// peer's run to finish, short enough that an unanswering peer costs one turn
/// rather than the run. The cap exists because the bound is the only thing
/// standing between "ask a peer" and "wait for it forever".
const ASK_DEFAULT_TIMEOUT_MS: u64 = 120_000;
const ASK_MAX_TIMEOUT_MS: u64 = 600_000;

/// Default wall for one WASM tool call. The guest is somebody else's code: a
/// module that never returns has to come back to the caller as a failure, so
/// the bound is short — a compute-shaped tool that legitimately needs longer
/// asks for it instead of raising this.
const DEFAULT_WASM_TIMEOUT_SECS: u64 = 30;

/// Default wall for one shell tool call. An order of magnitude above the WASM
/// bound because the thing being timed is a build or a test run, which is
/// expected to take minutes; what this catches is a command that wedged, not a
/// command that works.
const DEFAULT_SHELL_TIMEOUT_SECS: u64 = 600;

/// Real execution identity of one agent run. Carried explicitly from the
/// actor (which knows the DAG task) through the runtime into sandbox requests,
/// so the executor pod can attribute and route work per task.
#[derive(Debug, Clone)]
pub struct ToolScope {
    pub task_id: String,
    pub agent_id: String,
}

tokio::task_local! {
    /// The agent whose run is making the tool call on this task.
    ///
    /// A tool is registered once per process and shared by every agent in it,
    /// so nothing in the handler's own state can say who is calling; the run
    /// loop is the only party that knows, and it is also the one that
    /// dispatches the call. It announces the id around the await, and a handler
    /// that needs an identity reads it back here rather than inventing one.
    ///
    /// Absent when the dispatcher never established one — a runtime created
    /// without an agent config, a handler called directly by a test. Tools that
    /// report an identity say `null` in that case: "nobody said who is calling"
    /// and "the caller has no id" are different facts, and a made-up name would
    /// read as the second when the truth is the first.
    pub static CALLER_AGENT_ID: String;
}

/// The id of the agent whose run is making this call, when the dispatcher
/// announced one. See [`CALLER_AGENT_ID`].
pub fn caller_agent_id() -> Option<String> {
    CALLER_AGENT_ID.try_with(|id| id.clone()).ok()
}

/// Why a tool call produced no value.
///
/// A guardrail refusal is kept apart from a tool failure on purpose. A refusal is
/// a decision: the identical call cannot come out differently, so a loop that
/// retries it is not exploring, it is paying for the same answer again — each
/// retry a full round trip with the whole context. A tool failure may be worth
/// another attempt. Flattening both into one error made the refusal reach the
/// model as an ordinary tool failure, and ordinary failures are what an agent
/// loop is built to retry.
#[derive(Debug)]
pub enum ToolRunError {
    Blocked {
        tool: String,
        reason: String,
        rule: String,
    },
    Failed(SFError),
}

impl ToolRunError {
    /// The text this error carries when it is reported to a caller that has no
    /// way to tell the two cases apart. Worded exactly as before the split, so
    /// existing readers of the message (logs, traces) keep working.
    pub fn message(&self) -> String {
        match self {
            Self::Blocked { tool, reason, rule } => {
                format!(
                    "Guardrail blocked tool '{}': {} (rule: {})",
                    tool, reason, rule
                )
            }
            Self::Failed(e) => e.to_string(),
        }
    }
}

impl From<SFError> for ToolRunError {
    fn from(e: SFError) -> Self {
        Self::Failed(e)
    }
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
            wasm_timeout: Duration::from_secs(DEFAULT_WASM_TIMEOUT_SECS),
            shell_timeout: Duration::from_secs(DEFAULT_SHELL_TIMEOUT_SECS),
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

    /// Judge a finished reply with the same guardrail the tool calls meet.
    ///
    /// The registry is where the guardrail is attached, so this is the one
    /// place both entry points of the gate can reach: the call before it runs,
    /// and the answer before it leaves. Absent is a pass rather than a refusal:
    /// a run with no guardrail configured has no rule to apply, and inventing
    /// one here would refuse every reply in embedded runs.
    pub async fn check_output(&self, reply: &str) -> cog_core::GuardResult {
        match self.guardrail.as_ref() {
            Some(guardrail) => guardrail.check_output(reply).await,
            None => cog_core::GuardResult::Pass,
        }
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
        self.execute_scoped_checked(name, arguments, scope)
            .await
            .map_err(|e| match e {
                // The typed variant, not a string: callers outside this crate
                // decide whether to re-plan or stop, and they can only do that
                // by matching on the kind rather than on the wording.
                ToolRunError::Blocked { tool, reason, rule } => {
                    SFError::Blocked { tool, reason, rule }
                }
                ToolRunError::Failed(e) => e,
            })
    }

    /// The same execution, with a guardrail refusal kept distinguishable from a
    /// tool failure. Callers that decide whether to try again need the
    /// difference; callers that only report an error keep using
    /// [`Self::execute_scoped`], whose message is unchanged.
    pub async fn execute_scoped_checked(
        &self,
        name: &str,
        arguments: serde_json::Value,
        scope: Option<&ToolScope>,
    ) -> Result<serde_json::Value, ToolRunError> {
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
                    return Err(ToolRunError::Blocked {
                        tool: name.to_string(),
                        reason,
                        rule,
                    });
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
            ToolImplementation::Native(handler) => (handler)(arguments).await.map_err(Into::into),
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
                            return Err(ToolRunError::Failed(cog_core::SFError::Agent(format!(
                                "Failed to fetch plugin '{}': {}",
                                plugin_id, e
                            ))));
                        }
                    }
                } else {
                    return Err(ToolRunError::Failed(cog_core::SFError::Agent(
                        "PluginRegistry not configured for WASM tool".into(),
                    )));
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
                    return Err(ToolRunError::Failed(cog_core::SFError::Agent(format!(
                        "tool '{name}' requires task identity (require_tool_identity=true); \
                         call through prompt_for_task"
                    ))));
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
                                return Err(ToolRunError::Failed(cog_core::SFError::Validation(
                                    "empty command".into(),
                                )));
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
                            Err(cog_core::SFError::IO(stderr).into())
                        }
                    }
                    cog_core::ShellOp::WriteFile => {
                        if code == 0 {
                            Ok(serde_json::json!({ "success": true }))
                        } else {
                            Err(cog_core::SFError::IO(stderr).into())
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

    /// Refuses every tool call under one rule, so the refusal path can be
    /// exercised without a real policy.
    #[derive(Debug)]
    struct BlockAll;

    #[async_trait::async_trait]
    impl cog_core::Guardrail for BlockAll {
        async fn check_input(&self, _messages: &[cog_core::Message]) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_output(&self, _response: &str) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_tool_call(&self, tool: &cog_core::ToolCall) -> cog_core::GuardResult {
            cog_core::GuardResult::Block {
                reason: format!("{} is on the list", tool.name),
                rule: "test-rule".into(),
            }
        }
    }

    /// 网页抓取这条工具必须接上那条后备出口。
    ///
    /// 判据在**注册表**这一层，不在 `web_fetch_with_fallback` 里：函数自己再
    /// 正确，注册时传的是 `None` 就等于这个部署没有抓取后备，而症状是「本地的
    /// crawl4ai 一挂，抓取就整个不可用」——那看起来像 crawl4ai 的问题，不像一行
    /// 漏传的参数。所以这里从一个真的注册表里把工具取出来调用。
    #[tokio::test]
    async fn the_registered_fetch_tool_consults_the_fallback_outlet() {
        #[derive(Debug, Default)]
        struct Scripted {
            asked: std::sync::Mutex<Vec<(String, String)>>,
        }

        #[async_trait::async_trait]
        impl cog_core::HttpClient for Scripted {
            async fn execute(
                &self,
                req: cog_core::HttpRequest,
            ) -> SFResult<cog_core::HttpResponse> {
                self.asked
                    .lock()
                    .unwrap()
                    .push((req.method.clone(), req.url.clone()));
                // 第一次（本地渲染后端）失败，第二次（后备出口）给出正文。
                if req.url.contains("/search-fallback/") {
                    Ok(cog_core::HttpResponse {
                        status: 200,
                        headers: Default::default(),
                        body: br#"{"url":"https://example.com/a","title":"A","content":"the fallback text"}"#
                            .to_vec(),
                    })
                } else {
                    Ok(cog_core::HttpResponse {
                        status: 502,
                        headers: Default::default(),
                        body: b"renderer down".to_vec(),
                    })
                }
            }
        }

        let scripted = Arc::new(Scripted::default());
        let registry = ToolRegistry::new();
        builtins::register_builtins(
            &registry,
            builtins::BuiltinInputs {
                http_client: Some(scripted.clone() as Arc<dyn cog_core::HttpClient>),
                metrics: None,
                memory_api_base: None,
                external_archive_threshold_chars: 4096,
                message_backend: None,
                agent_registry: None,
                web_search_base: None,
                web_search_fallback_base: Some("http://gateway:8081/search-fallback".into()),
                web_fetch_base: Some("http://render:11235".into()),
            },
        );
        let tool = registry.get("web_fetch").expect("web_fetch is a built-in");
        let cog_core::ToolImplementation::Native(run) = &tool.implementation else {
            panic!("web_fetch is native");
        };
        let out = run(serde_json::json!({"url": "https://example.com/a"}))
            .await
            .expect("the fallback outlet answered");
        assert_eq!(out["text"], "the fallback text");
        assert_eq!(out["extraction"], "fallback");
        assert!(
            out["fallback_because"]
                .as_str()
                .unwrap_or_default()
                .contains("502"),
            "答案里要留下本地那层为什么没答：{out}"
        );
        let asked = scripted.asked.lock().unwrap().clone();
        // 本地那层自己会重试，次数不是这条测试的判据；判据是最后那一次问的是谁。
        let (last_method, last_url) = asked.last().cloned().expect("至少问过一次");
        assert_eq!(last_url, "http://gateway:8081/search-fallback/fetch");
        assert_eq!(last_method, "POST");
        assert!(asked.len() > 1, "能走到后备，说明本地那层先问过：{asked:?}");
    }

    /// A refusal must be readable as a refusal. Flattened into an ordinary
    /// error it is indistinguishable from a tool that failed, and an agent
    /// loop retries failures — the retries are what cost money for nothing.
    #[tokio::test]
    async fn a_guardrail_refusal_is_not_a_tool_failure() {
        let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hit = ran.clone();
        let mut registry = ToolRegistry::new();
        registry.set_guardrail(Arc::new(BlockAll));
        cog_core::ToolRegistry::register(
            &registry,
            Tool {
                name: "counted".into(),
                description: "counts its own executions".into(),
                parameters: serde_json::json!({"type": "object"}),
                implementation: ToolImplementation::Native(Arc::new(move |_args| {
                    let hit = hit.clone();
                    Box::pin(async move {
                        hit.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(serde_json::json!({ "ran": true }))
                    })
                })),
            },
        );

        match registry
            .execute_scoped_checked("counted", serde_json::json!({}), None)
            .await
        {
            Err(ToolRunError::Blocked { tool, rule, .. }) => {
                assert_eq!(tool, "counted");
                assert_eq!(rule, "test-rule");
            }
            other => panic!("expected a Blocked refusal, got {other:?}"),
        }
        assert_eq!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the tool body must not run behind a refusal"
        );

        // Callers that only report an error keep the message they had before
        // the split, so existing log and trace readers are unaffected.
        let flat = registry
            .execute("counted", serde_json::json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            flat.contains("Guardrail blocked tool 'counted': counted is on the list"),
            "unexpected message: {flat}"
        );
        assert!(flat.contains("test-rule"), "unexpected message: {flat}");
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

    /// The contract face of the inherent `names()` above — one listing, not a
    /// second one that could drift from it. Spelled with the type path so the
    /// call cannot be read as an alias for this method: an inherent method
    /// outranks a trait method of the same name, so `self.names()` here means
    /// the one above, but only a reader who already knows that rule can tell.
    fn names(&self) -> Vec<String> {
        ToolRegistry::names(self)
    }
}

/// 内置工具工厂。
pub mod builtins {
    // 网页检索与抓取实现放在 `crate::web`（自带测试与地址守卫），这里只把它们
    // 并进同一张注册表：工具的**清单**仍然只有 `register_builtins` 一处。
    use super::*;
    pub use crate::web::{
        web_fetch, web_fetch_with_fallback, web_search, web_search_with_fallback,
    };

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

    /// What the built-in set is built from when the two outside-world inputs
    /// are available.
    ///
    /// Every field is optional, and none of them is required to keep the list
    /// whole: a process with no HTTP client registers the same tools, and the
    /// ones that need a client then fail with a message naming what is
    /// missing. A caller that could tell "this layer is absent" from "this
    /// namespace is empty" through the tool result loses that ability the
    /// moment the tool is left out of the list instead.
    #[derive(Clone, Default)]
    pub struct BuiltinInputs {
        pub http_client: Option<Arc<dyn HttpClient>>,
        pub metrics: Option<Arc<dyn MetricsBackend>>,
        pub memory_api_base: Option<String>,
        pub external_archive_threshold_chars: usize,
        /// The queue an agent's commands travel on, and the roster that says
        /// which names are live. Both are needed to hand one agent the ability
        /// to address another: the first carries the message, the second is
        /// what keeps it from being written into a stream no consumer will ever
        /// read.
        pub message_backend: Option<Arc<dyn cog_core::MessageBackend>>,
        pub agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
        /// Where the free web search backend lives. `None` means the
        /// deployment wired none: `web_search` is registered anyway and names
        /// the missing setting when called, because a caller has to be able to
        /// tell "this deployment has no web access" from "this tool does not
        /// exist".
        pub web_search_base: Option<String>,
        /// A second search backend, consulted only when the first put the query
        /// to nobody. `None` means this deployment keeps every query inside its
        /// own tree, which is the default and not a degradation: the fallback
        /// only ever answers a query that was never asked of anything here.
        pub web_search_fallback_base: Option<String>,
        /// Where the page-rendering backend lives. `None` means the fetch reads
        /// the page in process; `Some` means every fetch is rendered in a
        /// browser, and a backend that does not answer is reported as such
        /// rather than quietly replaced by the path that cannot run scripts.
        pub web_fetch_base: Option<String>,
    }

    /// Register every built-in tool into `registry`.
    ///
    /// One list, in one place. The same built-ins are served to two callers --
    /// the agent loops in this process, and an external MCP client over stdio
    /// -- and both mean the same thing by "this platform's tools". A list laid
    /// out a second time at the second caller is a second answer to which
    /// tools exist, and the two answers drift in the direction that is hard to
    /// see: the shorter list looks fine, works, and is quietly weaker than
    /// what the process claims to offer. So the factories above have exactly
    /// one caller and it is this one.
    pub fn register_builtins(registry: &dyn cog_core::ToolRegistry, inputs: BuiltinInputs) {
        cog_core::ToolRegistry::register(registry, read_file());
        cog_core::ToolRegistry::register(registry, write_file());
        cog_core::ToolRegistry::register(registry, run_command());
        match inputs.http_client.clone() {
            Some(client) => cog_core::ToolRegistry::register(
                registry,
                http_request(
                    client,
                    inputs.memory_api_base.clone(),
                    inputs.external_archive_threshold_chars,
                ),
            ),
            // Not a silent omission: the caller that wanted it can read why it
            // is absent off this line, and the rest of the list is unaffected.
            None => tracing::warn!(
                "no HttpClient available; the http_request built-in is not registered"
            ),
        }
        cog_core::ToolRegistry::register(
            registry,
            raw_list(inputs.http_client.clone(), inputs.memory_api_base.clone()),
        );
        cog_core::ToolRegistry::register(
            registry,
            raw_fetch(
                inputs.http_client.clone(),
                inputs.memory_api_base.clone(),
                inputs.metrics.clone(),
            ),
        );
        // 网页检索与抓取：都用同一个 HTTP 客户端。搜索后端地址缺失时工具照常注册，
        // 调用时报出缺的那个设置——这样「这个部署没接网页检索」与「没有这个工具」
        // 在调用者眼里不是同一个答案。
        cog_core::ToolRegistry::register(
            registry,
            web_search_with_fallback(
                inputs.http_client.clone(),
                inputs.web_search_base.clone(),
                inputs.web_search_fallback_base.clone(),
            ),
        );
        // 抓取与检索走同一条后备：本地那层答不上时，问的是同一个出口（第三方池，
        // 池也答不上才轮到现网在用的 LLM）。`web_search_fallback_base` 这个地址
        // 同时服务两个动作，路由自己分辨 `/search` 与 `/fetch`——一个部署里
        // 「允许查询出门」只有一次决定，不该被拆成两次。
        cog_core::ToolRegistry::register(
            registry,
            web_fetch_with_fallback(
                inputs.http_client.clone(),
                inputs.web_fetch_base.clone(),
                inputs.web_search_fallback_base.clone(),
            ),
        );
        match (
            inputs.message_backend.clone(),
            inputs.agent_registry.clone(),
        ) {
            (Some(backend), Some(registry_handle)) => {
                cog_core::ToolRegistry::register(registry, send_to_agent(backend, registry_handle))
            }
            // Both halves are needed, so the tool is absent when either is: a
            // message with no roster check goes into a stream nobody reads, and
            // a roster with no queue has nowhere to put it. Registered but
            // failing would look the same from the model's side, and this way
            // the process says which half is missing.
            _ => tracing::warn!(
                "no message backend or agent registry; the send_to_agent built-in is not registered"
            ),
        }
        match inputs.agent_registry.clone() {
            // Listing needs only the roster, so it stays when the queue is
            // absent: discovery is still true, and the caller finds out which
            // half is missing when it tries to send rather than by finding the
            // directory gone.
            Some(registry_handle) => {
                cog_core::ToolRegistry::register(registry, list_agents(registry_handle))
            }
            None => tracing::warn!("no agent registry; the list_agents built-in is not registered"),
        }
    }

    /// Names the live agents, so a caller has something to address.
    ///
    /// Without this the send tool has no address to send to: an agent id is
    /// derived from the task and the role, and a squad member knows neither for
    /// its peers. Reading it off the roster is the same lookup the send path
    /// performs before publishing, so the two cannot disagree about who exists.
    ///
    /// The entries are the ones the roster calls live — a registration whose
    /// expiry has passed is not returned, which is what makes "listed here" and
    /// "will be delivered to" the same set.
    pub fn list_agents(registry: Arc<dyn cog_core::AgentRegistry>) -> Tool {
        Tool {
            name: "list_agents".into(),
            description: "List the live agents this platform has registered, optionally \
                          filtered by role or capability. Use it to find the exact \
                          agent_id to pass to send_to_agent. Only live agents appear: a \
                          registration that has expired is not listed. When the run \
                          established an identity, your own entry carries `is_me: true` \
                          and the result repeats your id as `caller`."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "role": {
                        "type": "string",
                        "description": "Only agents playing this role"
                    },
                    "capability": {
                        "type": "string",
                        "description": "Only agents declaring this capability"
                    }
                }
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let registry = registry.clone();
                Box::pin(async move {
                    let role = args["role"].as_str();
                    let capability = args["capability"].as_str();
                    // The caller's own row is marked, because an id derived
                    // from a task and a role is not something an agent can
                    // reconstruct from its prompt: without the mark the only
                    // way to find yourself in the directory is to guess which
                    // of several peers you are.
                    let me = caller_agent_id();
                    let live = registry.list().await?;
                    let agents: Vec<serde_json::Value> = live
                        .into_iter()
                        .filter(|r| role.is_none_or(|role| r.role == role))
                        .filter(|r| {
                            capability.is_none_or(|cap| r.capabilities.iter().any(|c| c == cap))
                        })
                        .map(|r| {
                            let is_me = me.as_deref() == Some(r.agent_id.as_str());
                            serde_json::json!({
                                "agent_id": r.agent_id,
                                "role": r.role,
                                "workspace_id": r.workspace_id,
                                "capabilities": r.capabilities,
                                "last_heartbeat": r.last_heartbeat.to_rfc3339(),
                                "is_me": is_me,
                            })
                        })
                        .collect();
                    Ok(serde_json::json!({
                        "count": agents.len(),
                        "caller": me,
                        "agents": agents,
                    }))
                })
            })),
        }
    }

    /// Hand one agent's instruction to another agent's inbox.
    ///
    /// This is the only route between agents. The HTTP face of the same
    /// operation sits behind the gateway's credentials, and the processes that
    /// run agent loops hold none on purpose, so a tool that dialed the API
    /// would be a tool that never works in the deployment it ships in. The
    /// queue is already in this process, and the wire form is the one the
    /// inbox consumer reads.
    ///
    /// The roster check is part of the delivery, not a courtesy: publishing to
    /// an agent id nobody holds writes a stream no consumer group will ever
    /// read, which is indistinguishable at this call site from a successful
    /// delivery and leaves a key behind for good. A miss is therefore an error
    /// naming the id, and nothing is published. Both miss forms are counted
    /// under their own reason: the caller gets the error, but a producer that
    /// keeps addressing agents nobody holds publishes nothing, so no queue
    /// depth or lag reading anywhere would show it.
    ///
    /// Delivery is at-least-once — the target acks after running the command,
    /// and a target that dies mid-run sees it again — so the instruction has to
    /// be one that tolerates being applied twice. There is no reply channel:
    /// the run's events reach the event plane as any other run's do, but a
    /// caller waiting on an answer waits forever. `reply_stream` exists on the
    /// wire for callers that can read a stream; a tool call cannot.
    ///
    /// The caller's own id comes from the run loop, which is the only party
    /// that knows it: the handler is shared by every agent in the process and
    /// its signature carries arguments only. It rides into the log line and
    /// into the result, so a message can be traced to its sender and an agent
    /// that was never told its own id can read it off the receipt.
    pub fn send_to_agent(
        backend: Arc<dyn cog_core::MessageBackend>,
        registry: Arc<dyn cog_core::AgentRegistry>,
    ) -> Tool {
        Tool {
            name: "send_to_agent".into(),
            description: "Send an instruction to another live agent's inbox. Delivery is \
                          at-least-once and the target acks only after it has run the \
                          command, so write instructions that survive being applied twice. \
                          With expect_reply the call waits for the target's answer and \
                          returns it under `reply`; without it the call returns as soon as \
                          the instruction is queued and nothing comes back. The result \
                          repeats your own agent id as `from` when the run established \
                          one, and `null` when it did not. kind=prompt starts a run on the \
                          target, kind=steering injects into a run already in progress, \
                          kind=continue appends to the target's conversation; only \
                          kind=prompt can carry expect_reply, because only a run produces \
                          an answer."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {
                        "type": "string",
                        "description": "Exact agent id as it appears in the roster"
                    },
                    "message": {
                        "type": "string",
                        "description": "The instruction the target agent receives"
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["prompt", "steering", "continue"],
                        "description": "How the target should take it (default prompt)"
                    },
                    "expect_reply": {
                        "type": "boolean",
                        "description": "Wait for the target's answer and return it. Blocks \
                                        this turn for up to timeout_ms, so ask only for an \
                                        answer you need in this turn (default false)"
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "How long to wait when expect_reply is set (default \
                                        120000, capped at 600000)"
                    }
                },
                "required": ["agent_id", "message"]
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let backend = backend.clone();
                let registry = registry.clone();
                Box::pin(async move {
                    let agent_id = args["agent_id"]
                        .as_str()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| SFError::Validation("agent_id required".into()))?;
                    let message = args["message"]
                        .as_str()
                        .filter(|s| !s.trim().is_empty())
                        .ok_or_else(|| SFError::Validation("message required".into()))?;
                    let kind = args["kind"].as_str().unwrap_or("prompt");
                    let expect_reply = args["expect_reply"].as_bool().unwrap_or(false);
                    // Refused before anything is written: the other two commands
                    // do not start a run, so there is no answer to wait for and
                    // a wait would spend the whole timeout learning that.
                    if expect_reply && kind != "prompt" {
                        return Err(SFError::Validation(format!(
                            "expect_reply needs kind=prompt; a {kind} command runs nothing, \
                             so no answer can come back"
                        )));
                    }
                    // The stream this call will read, named before the request
                    // exists so the target can be told where to answer.
                    let ask = if expect_reply {
                        Some(ask_reply_stream())
                    } else {
                        None
                    };

                    let inbox = match kind {
                        "prompt" => cog_core::InboxMessage::Prompt {
                            goal: serde_json::Value::String(message.to_string()),
                            reply_stream: ask.clone(),
                        },
                        "steering" => cog_core::InboxMessage::Steering {
                            instruction: message.to_string(),
                        },
                        "continue" => cog_core::InboxMessage::Continue {
                            instruction: message.to_string(),
                        },
                        other => {
                            return Err(SFError::Validation(format!(
                                "kind {other:?} is not one of prompt, steering, continue"
                            )))
                        }
                    };

                    // Asked before the write, because the two answers a missing
                    // name produces are not the same: an error here, or a key
                    // that outlives every process and never delivers anything.
                    match registry.get(agent_id).await {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            // Counted, not only returned: the caller sees the
                            // error, but a producer that keeps naming agents
                            // nobody holds looks like a working one from every
                            // other reading -- it publishes nothing, so there
                            // is no queue depth anywhere to notice either.
                            crate::observable::global_observable()
                                .record_inbox_refused("no_live_agent");
                            return Err(SFError::Validation(format!(
                                "no live agent named {agent_id:?}; a message to it would be \
                                 written to an inbox no process reads"
                            )));
                        }
                        Err(e) => {
                            crate::observable::global_observable()
                                .record_inbox_refused("roster_unreadable");
                            return Err(SFError::Agent(format!(
                                "cannot read the agent roster, so whether {agent_id:?} is live \
                                 is unknown: {e}"
                            )));
                        }
                    }

                    let inbox_stream = cog_core::agent_inbox_stream(agent_id);
                    let payload = serde_json::to_vec(&inbox).map_err(SFError::Serialization)?;

                    // Opened *before* the request goes out, because a consumer
                    // group starts at the stream's current end: an answer
                    // published between the two would sit behind the reader's
                    // start position and never be delivered, and the wait would
                    // run to its timeout with the answer already on the wire.
                    //
                    // The group is created first, and it is what creates the
                    // key: every `?` between here and the wait's own cleanup
                    // can return with the stream already in the backend. The
                    // guard is armed at that moment, so a failed publish, a
                    // dropped future or a cancelled turn all end the same way
                    // as an answer does -- with the stream deleted and the ask
                    // counted under the outcome it actually met.
                    let mut reply_guard = match ask.as_deref() {
                        Some(stream) => {
                            backend.create_consumer_group(stream, ASK_GROUP).await?;
                            Some(ReplyStreamGuard::new(backend.clone(), stream))
                        }
                        None => None,
                    };
                    let mut reply_reader = match ask.as_deref() {
                        Some(stream) => Some(backend.subscribe(stream, ASK_GROUP).await?),
                        None => None,
                    };

                    backend.publish(&inbox_stream, &payload).await?;
                    // One line per message, naming both ends: the queue keeps
                    // no sender anywhere — the wire form carries the command
                    // and nothing about who wrote it — so without this the only
                    // party who could ever say who asked is the caller's own
                    // transcript. What the line says is what happened at this
                    // point: published, not delivered, because delivery is the
                    // target's ack and this process does not see it.
                    let from = caller_agent_id();
                    tracing::info!(
                        from = from.as_deref().unwrap_or("-"),
                        to = agent_id,
                        kind,
                        stream = %inbox_stream,
                        awaiting_reply = ask.is_some(),
                        "inter-agent message published to an inbox"
                    );

                    let mut receipt = serde_json::json!({
                        "delivered": true,
                        "from": from,
                        "agent_id": agent_id,
                        "kind": kind,
                        "stream": inbox_stream,
                    });

                    if let (Some(stream), Some(reader)) = (ask.as_deref(), reply_reader.as_mut()) {
                        if let Some(guard) = reply_guard.as_mut() {
                            guard.begin_wait();
                        }
                        let waited = wait_for_reply(backend.as_ref(), stream, reader, &args).await;
                        // The wait deletes the stream on every path it reaches,
                        // so from here on the guard has nothing left to clean.
                        if let Some(guard) = reply_guard.as_mut() {
                            guard.disarm();
                        }
                        let observable = crate::observable::global_observable();
                        observable.record_inbox_ask(waited.outcome);
                        let waited_ms = waited.waited.as_millis() as u64;
                        match waited.reply {
                            Some(reply) => {
                                receipt["replied"] = serde_json::json!(true);
                                receipt["reply"] = reply;
                            }
                            None => {
                                receipt["replied"] = serde_json::json!(false);
                                receipt["reason"] = serde_json::json!(waited.outcome);
                                receipt["hint"] = serde_json::json!(match waited.outcome {
                                    "timeout" =>
                                        "the target did not answer in time; it may \
                                                  still be running the instruction, or its \
                                                  handler failed and left it pending",
                                    _ => "the reply channel closed before an answer arrived",
                                });
                            }
                        }
                        receipt["waited_ms"] = serde_json::json!(waited_ms);
                    }

                    Ok(receipt)
                })
            })),
        }
    }

    /// The consumer group a waiting ask reads its reply with.
    ///
    /// One name for every ask, because each ask owns its own stream: the group
    /// is the reader, and a stream with exactly one reader in the whole
    /// deployment does not need a second identity.
    const ASK_GROUP: &str = "asker";

    /// The stream one ask waits on, named by the ask itself.
    ///
    /// The name *is* the correlation. A reply carries nothing that says which
    /// question it answers, so two questions sharing a stream would leave the
    /// reader unable to tell the answers apart. Giving each ask its own stream
    /// makes the correlation a property of the address instead of a field the
    /// reply must carry and every producer must remember to fill in.
    ///
    /// Process id, wall clock and a counter together, because no one of them
    /// is unique alone: the counter restarts at zero with the process, the
    /// clock collides inside a tick, and the process id is reused.
    fn ask_reply_stream() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!(
            "orchestrator:reply:{}:{:x}:{}",
            std::process::id(),
            nanos,
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Ends a reply stream the wait never got to end, and counts the ask.
    ///
    /// `wait_for_reply` closes the stream on every path it *reaches*. A
    /// cancelled turn reaches none of them: the future is dropped at whatever
    /// `.await` it was parked on, so the `close_reply_stream` at the end never
    /// runs and the key stays in the backend with its consumer group and
    /// nothing left to read it. The same is true of the `?` returns between
    /// the group's creation and the wait — a publish that fails leaves the
    /// stream behind just as surely.
    ///
    /// A guard is the only construct that runs on both. It is armed when the
    /// key starts existing and disarmed when the wait has done its own
    /// cleanup, so its `Drop` fires exactly on the paths that skip that
    /// cleanup. The delete is spawned rather than awaited because `Drop`
    /// cannot wait: the runtime is still up (a drop only happens inside one),
    /// and a delete that fails is the same orphaned key the wait itself
    /// tolerates and logs.
    ///
    /// The ask is counted as `abandoned` here, not by the wait: the wait's
    /// outcomes all mean the caller got an answer or ran out of patience, and
    /// neither is what happened. Only a drop *after* the wait began is booked
    /// that way: a publish that failed never became an ask, so its caller has
    /// an error in hand and there is no question to count -- the stream it had
    /// already opened is cleaned up all the same.
    struct ReplyStreamGuard {
        backend: std::sync::Arc<dyn cog_core::MessageBackend>,
        stream: String,
        armed: bool,
        waiting: bool,
    }

    impl ReplyStreamGuard {
        fn new(backend: std::sync::Arc<dyn cog_core::MessageBackend>, stream: &str) -> Self {
            Self {
                backend,
                stream: stream.to_string(),
                armed: true,
                waiting: false,
            }
        }

        /// The question is on the wire and the caller is now waiting for it.
        fn begin_wait(&mut self) {
            self.waiting = true;
        }

        /// The wait has already deleted the stream; do not delete it twice.
        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for ReplyStreamGuard {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            if self.waiting {
                crate::observable::global_observable().record_inbox_ask("abandoned");
            }
            let backend = self.backend.clone();
            let stream = self.stream.clone();
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        backend.stop_stream(&stream, ASK_GROUP).await;
                        if let Err(e) = backend.delete_stream(&stream).await {
                            tracing::warn!(
                                stream = %stream,
                                error = %e,
                                "an abandoned ask left its reply stream behind: this \
                                 backend cannot delete streams"
                            );
                        }
                    });
                }
                Err(_) => tracing::warn!(
                    stream = %self.stream,
                    "an abandoned ask could not be reaped: no runtime to close its reply stream"
                ),
            }
        }
    }

    /// What one wait produced: how it ended, what came back, and how long it
    /// held the caller's turn.
    struct Waited {
        outcome: &'static str,
        reply: Option<serde_json::Value>,
        waited: Duration,
    }

    /// Wait for the first frame on a reply stream opened for this ask, then
    /// give the stream back.
    ///
    /// The stream is deleted on every path — answered, timed out, closed — so
    /// a request-scoped stream is bounded by the asks in flight rather than by
    /// every ask ever made. Nothing else in the backend contract removes an
    /// entry, which is what makes this the only cleanup there is.
    ///
    /// A timeout is reported, not raised. The instruction was published and
    /// the target may still be running it, so turning the wait into a tool
    /// error would tell the caller its send failed when what ran out was only
    /// the waiting.
    async fn wait_for_reply(
        backend: &dyn cog_core::MessageBackend,
        stream: &str,
        reader: &mut cog_core::MessageStream,
        args: &serde_json::Value,
    ) -> Waited {
        let started = std::time::Instant::now();
        let timeout = Duration::from_millis(
            args["timeout_ms"]
                .as_u64()
                .unwrap_or(ASK_DEFAULT_TIMEOUT_MS)
                .clamp(1, ASK_MAX_TIMEOUT_MS),
        );
        let first = tokio::time::timeout(timeout, reader.next()).await;
        let waited = started.elapsed();

        let mut outcome = "timeout";
        let mut reply = None;
        match first {
            Ok(Some(Ok((id, payload)))) => {
                if let Err(e) = backend
                    .ack(stream, ASK_GROUP, std::slice::from_ref(&id))
                    .await
                {
                    tracing::warn!(stream = %stream, error = %e, "reply acked with an error");
                }
                // The target's answer is the result of its run, which is JSON
                // by construction; a backend or build that published something
                // else still answered, so the bytes are returned rather than
                // the whole ask being called a failure.
                reply = Some(serde_json::from_slice(&payload).unwrap_or_else(|_| {
                    serde_json::Value::String(String::from_utf8_lossy(&payload).into_owned())
                }));
                outcome = "replied";
            }
            Ok(Some(Err(e))) => {
                outcome = "stream_error";
                tracing::warn!(stream = %stream, error = %e, "reply stream failed while waiting");
            }
            Ok(None) => {
                // Closed before an answer arrived. Not a timeout: the wait did
                // not run out, the channel went away under it.
                outcome = "stream_error";
            }
            Err(_elapsed) => {}
        }

        close_reply_stream(backend, stream).await;
        Waited {
            outcome,
            reply,
            waited,
        }
    }

    /// End the read loop and drop the stream, best effort.
    ///
    /// The delete error is logged and swallowed: the caller already has its
    /// answer or its timeout, and cleanup that failed is a key left behind,
    /// not a reason to turn a completed ask into an error.
    async fn close_reply_stream(backend: &dyn cog_core::MessageBackend, stream: &str) {
        backend.stop_stream(stream, ASK_GROUP).await;
        if let Err(e) = backend.delete_stream(stream).await {
            tracing::warn!(
                stream = %stream,
                error = %e,
                "reply stream left behind: this backend cannot delete streams"
            );
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
