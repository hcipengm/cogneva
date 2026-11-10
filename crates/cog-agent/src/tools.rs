use cog_core::metric_names::TOOL_OUTPUT_FETCH_BUDGET_EXHAUSTED_TOTAL;
use cog_core::{
    CommandEvent, HttpClient, HttpRequest, MetricsBackend, SFError, SFResult, SandboxBackend,
    SandboxPayload, SandboxRequest, DEFAULT_MEMORY_NAMESPACE, MEMORY_API_BASE_ENV,
    MEMORY_RAW_CONTENT_PATH, MEMORY_RAW_ITEM_PATH, MEMORY_RAW_PATH,
};
use cog_core::{Tool, ToolImplementation};
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

/// How long one memory-read call may take before it is abandoned. The call sits
/// on an agent turn, so its budget is a share of a turn rather than a background
/// job's: long enough for a healthy API to return a bounded payload, short
/// enough that an API which stopped answering delays the turn instead of
/// wedging it.
// cap: bound — 短到让一个不再应答的 API 拖慢这一回合而不是把它卡死
const MEMORY_READ_TIMEOUT_SECS: u64 = 30;

/// The payload size a read moves without being told otherwise. A quarter
/// megabyte is what a tool result that already fit an agent's context roughly
/// holds; a caller that wants a larger source names its own budget.
// cap: bound — 一兆是「本来就装得进 agent 上下文的一条工具结果」的量级；要更大来源的调用方自己报名额
const DEFAULT_RAW_FETCH_BUDGET_BYTES: u64 = 262_144;

/// How long an ask waits for its answer when the caller does not say, and the
/// longest a caller may ask for.
///
/// The wait happens inside the caller's turn, holding it: long enough for a
/// peer's run to finish, short enough that an unanswering peer costs one turn
/// rather than the run. The cap exists because the bound is the only thing
/// standing between "ask a peer" and "wait for it forever".
// cap: bound — 这个界是「问一个同伴」与「永远等下去」之间唯一拦着的东西
const ASK_DEFAULT_TIMEOUT_MS: u64 = 120_000;
// cap: bound — the cap that keeps a peer's silence from becoming an unbounded wait
const ASK_MAX_TIMEOUT_MS: u64 = 600_000;

/// How long a filed work item may stay open when its submitter names no budget.
///
/// The row default is sized for one dispatched round; a work item is carried by
/// an employee whose turn includes thinking, tool calls and possibly a peer's
/// answer, and the over-budget sweep fails the row by age rather than by
/// liveness. A caller that knows better states its own number.
// cap: bound — 一件活能挂多久；不设的话接活的人活得比行久，行会先被判死
const DEFAULT_WORK_ITEM_TIMEOUT_SECS: u64 = 3600;

/// Default wall for one WASM tool call. The guest is somebody else's code: a
/// module that never returns has to come back to the caller as a failure, so
/// the bound is short — a compute-shaped tool that legitimately needs longer
/// asks for it instead of raising this.
// cap: bound — 计算型工具确实需要更久的话该自己报，而不是把这里调大
const DEFAULT_WASM_TIMEOUT_SECS: u64 = 30;

/// Default wall for one shell tool call. An order of magnitude above the WASM
/// bound because the thing being timed is a build or a test run, which is
/// expected to take minutes; what this catches is a command that wedged, not a
/// command that works.
// cap: derived — 与构建侧的墙同源：这些命令本来就以分钟计，这里要抓的是卡住的命令而不是能跑的命令
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

/// The task id a tool call carries: the scoped task when there is one, and
/// otherwise the caller's own lane.
///
/// A call with no task scope used to get a fresh random id every time, and the
/// executor routes by it — so every call in a conversation was handed a
/// different checkout. Measured on the deployed chat path (2026-11-06): `pwd`
/// answered `/opt/cogneva/sandbox/workspaces/unscoped-shell-f294ca21-…`, and the
/// next call in the same conversation would have named a different directory.
/// That makes a conversation unable to finish any job that takes two calls —
/// `write_file` then `run_command` tests nothing, because the file was written
/// into a tree the next call never sees — which is exactly the "one call at a
/// time" ceiling the tools were meant to lift.
///
/// The id is attribution, not routing: the tree a request lands in is chosen by
/// the agent id (see [`cog_core::worktree_lane`]), so a call that names its
/// caller lands in that caller's checkout whichever task it says it is for. A
/// caller nobody announced still gets the random id: two unknown callers
/// sharing one directory would be worse than the churn.
fn sandbox_task_id(scope: Option<&ToolScope>) -> String {
    if let Some(scope) = scope {
        return scope.task_id.clone();
    }
    match caller_agent_id() {
        Some(caller) => cog_core::worktree_lane(&caller),
        None => format!("unscoped-shell-{}", uuid::Uuid::new_v4()),
    }
}

/// `read_file` 在调用方没说要看多少行时给出的一页。
///
/// 一页按行数计，不按字节：行数读起来能对上编辑器，也能直接当 `offset`
/// 用。取值让常见源码文件一次读完，而整文件倾倒型的读（几千行、几十万
/// 字符）自动变成可分页的几段。
// cap: bound — 让常见源码文件一次读完，而整文件倾倒型（几千行、几十万字符）自动变成可分页的几段
const READ_FILE_DEFAULT_WINDOW_LINES: usize = 600;

/// 一页最多带多少个字符，行数没到也停下来。
///
/// 行数不是字节：一份压缩过的 JSON、一张宽表、一行几十万字符的日志，600 行
/// 可以轻松超过一次对话里任何一段的体量。这一页之后每一轮都要重发，所以
/// 两道界取更紧的那道；被这道界停下来的页面照样带 `has_more` 与
/// `next_offset`，翻页不会漏行。
// cap: bound — 两道界取更紧的那道；被它停下的页面照样带 has_more 与 next_offset，翻页不漏行
const READ_FILE_PAGE_MAX_CHARS: usize = 32 * 1024;

/// `read_file` 一次批量读最多带几个文件。
///
/// 批量读省的是往返次数，不是正文：读进来的字节与逐个读一样多。但一次带回
/// 几十个文件就是把这几十页一起钉进上下文，之后每一轮都要重发，省下的往返
/// 又按轮数赔回去。八份是一屏常见的「这一次改动要碰的文件」，再多的调用方
/// 应当先缩小范围再读。
// cap: bound — 一次带回几十份会把它们一起钉进上下文，之后每轮重发，省下的往返按轮数赔回去
const READ_FILE_MAX_BATCH: usize = 8;

/// `read_file` 调用方给的是哪几个文件。
///
/// `paths` 与 `path` 都认：前者一次读多份，后者是原有的一次一份。两个一起给
/// 是调用方没说清要读什么，报错比猜一个更能省一轮——猜错的那一轮要付整份
/// 上下文的重发钱。`paths` 为空同样报错：空批量回一个空数组，读的人会把它
/// 当成「这些文件都不存在」。
fn batch_paths(arguments: &serde_json::Value) -> Result<Option<Vec<String>>, cog_core::SFError> {
    let Some(raw) = arguments.get("paths") else {
        return Ok(None);
    };
    if arguments.get("path").is_some() {
        return Err(cog_core::SFError::Validation(
            "give either path (one file) or paths (several), not both".into(),
        ));
    }
    let list = raw
        .as_array()
        .ok_or_else(|| cog_core::SFError::Validation("paths must be an array of strings".into()))?;
    if list.is_empty() {
        return Err(cog_core::SFError::Validation("paths is empty".into()));
    }
    if list.len() > READ_FILE_MAX_BATCH {
        return Err(cog_core::SFError::Validation(format!(
            "paths has {} entries; at most {} files per call, read the rest in another call",
            list.len(),
            READ_FILE_MAX_BATCH
        )));
    }
    list.iter()
        .map(|p| {
            p.as_str().map(str::to_string).ok_or_else(|| {
                cog_core::SFError::Validation("paths must be an array of strings".into())
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// 按行切出 `read_file` 调用方要的那一段。
///
/// 整份读回来再整份塞进 tool_result 的代价不在磁盘上：这份正文进了上下文，
/// 之后每一轮都要重发一遍，还要再被上游缓存或不缓存各算一次钱。调用方大多
/// 只想要一段（某个函数、报错所在的那几行）。`offset` 与 `limit` 都不给时
/// 原样返回整份，老调用方与老断言不受影响；给了就带上文件的总行数与「后面
/// 还有没有」，只回切片不回总行数，读的人会以为文件到那里就结束了。
fn read_window(text: &str, offset: Option<u64>, limit: Option<u64>) -> serde_json::Value {
    let lines: Vec<&str> = text.lines().collect();
    let total = lines.len();
    // 调用方没说要看多少行，而这份文件又比一页长：给一页。
    // 整份塞进上下文的代价不在这一次调用上——它之后每一轮都要重发，
    // 缓存命中率的分母就是每一轮新追加的字节，一次整文件读能把接下来
    // 整段对话的命中去掉一大块。`has_more` 与 `next_offset` 让读的人
    // 知道后面还有、从哪继续，而不是把「到这儿就结束了」当成结论。
    let limit = limit.or_else(|| {
        let too_long = total > READ_FILE_DEFAULT_WINDOW_LINES
            || text.chars().count() > READ_FILE_PAGE_MAX_CHARS;
        too_long.then_some(READ_FILE_DEFAULT_WINDOW_LINES as u64)
    });
    if offset.is_none() && limit.is_none() {
        return serde_json::json!({ "content": text });
    }
    let start = offset.unwrap_or(1).max(1) as usize;
    let wanted = match limit {
        Some(want) => start
            .saturating_sub(1)
            .saturating_add(want as usize)
            .min(total),
        None => total,
    };
    // 行数与字符两道界取更紧的那道。至少给一行：一行就超预算时如果不给，
    // `next_offset` 永远停在同一个位置，翻页就成了死循环。
    let mut end = start.saturating_sub(1).min(total);
    let mut chars = 0usize;
    while end < wanted {
        let line = lines[end].len();
        if end > start.saturating_sub(1) && chars + line + 1 > READ_FILE_PAGE_MAX_CHARS {
            break;
        }
        chars += line + 1;
        end += 1;
    }
    let slice: Vec<&str> = if start > total {
        Vec::new()
    } else {
        lines[start - 1..end].to_vec()
    };
    let has_more = end < total;
    serde_json::json!({
        "content": slice.join("\n"),
        "start_line": start,
        "total_lines": total,
        "has_more": has_more,
        "next_offset": has_more.then_some(end + 1),
    })
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
    /// When true, shell-class tools reject calls that carry no identity at all
    /// -- neither a [`ToolScope`] nor an announced caller. Evolution pods turn
    /// this on so no command can reach the executor under a synthetic identity;
    /// default off keeps embedded/main-app behavior.
    require_identity: bool,
    /// 每个工具在本进程里被点过几次。
    ///
    /// `fork()` 共用同一个 `Arc`，不各拿一份：这个数答的是
    /// 「这个平台在用这个工具吗」，那是关于进程的事实，不是关于某一轮工具表的事
    /// 实。每一轮各记一份的话，读的人只会看到最后一轮点过什么，而一个只在启动时
    /// 派过活的工具会读成「没人用」。
    calls: Arc<std::sync::Mutex<HashMap<String, u64>>>,
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
            calls: Arc::new(std::sync::Mutex::new(HashMap::new())),
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

    /// 同一套跑工具的能力，另起一张工具表。
    ///
    /// 注册表装的是两样东西：哪些工具存在，和一只工具**怎么跑**——沙箱后端、
    /// 护栏、插件源、两个超时、是否需要身份。把 `list()` 复制进一个空的
    /// `ToolRegistry::new()` 只搬走了前一样，后一样留在原处；搬过去的是一张
    /// 装得上却跑不动的名单：它报得出 `run_command`，每次调用却回
    /// `SandboxBackend not configured for shell tool`——智能体读到「这个进程有
    /// shell」，运行时说它没有。界面上的员工曾经就是这样：同一份工具表，
    /// 一只也执行不了。
    ///
    /// 工具表本身是复制的而不是共用的：调用方要往自己这张表上装只属于这一轮
    /// 的工具，那些工具不该跟着出现在每一条别的运行里。
    pub fn fork(&self) -> Self {
        let tools = self.tools.read().unwrap().clone();
        Self {
            tools: Arc::new(std::sync::RwLock::new(tools)),
            sandbox_backend: self.sandbox_backend.clone(),
            guardrail: self.guardrail.clone(),
            plugin_registry: self.plugin_registry.clone(),
            wasm_timeout: self.wasm_timeout,
            shell_timeout: self.shell_timeout,
            require_identity: self.require_identity,
            calls: self.calls.clone(),
        }
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

    /// Execute a tool with the real run identity. `None` scope means the caller
    /// never established task identity (legacy/embedded path); the synthetic id
    /// gets an `unscoped-` prefix so logs can tell the two apart. With
    /// `require_identity` on, shell tools are rejected when there is neither a
    /// task scope nor an announced caller ([`CALLER_AGENT_ID`]): a task-less
    /// run of a named employee or squad member has the second, and refusing it
    /// would be refusing an identity it has.
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

        // Counted here, before the guardrail and before the implementation: this
        // funnel is the only place that sees every call, and the reading answers
        // "does anyone dial this action", not "how often does it succeed". A
        // tool that is called and always refused is still being used — and the
        // refusal is then the thing to fix, which a count that only recorded
        // successes would hide.
        *self
            .calls
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(0) += 1;

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
                // 「这个调用是谁发的」有两个来源，不是一个：任务作用域，和派发器
                // 在调用前公布的本轮执行者（`CALLER_AGENT_ID`）。名册上的员工与
                // 小队成员跑的是没有任务的 prompt，它只有后者——只认前者会把它们
                // 的每一条 shell 都拒掉，而它们的身份并不缺。缺的是两者都没有：
                // 那种调用才无名可归，`sandbox_task_id` 也只会给它一个一次性目录。
                if scope.is_none() && self.require_identity && caller_agent_id().is_none() {
                    return Err(ToolRunError::Failed(cog_core::SFError::Agent(format!(
                        "tool '{name}' requires task identity (require_tool_identity=true); \
                         call through prompt_for_task"
                    ))));
                }
                let backend = self.sandbox_backend.as_ref().ok_or_else(|| {
                    cog_core::SFError::Agent("SandboxBackend not configured for shell tool".into())
                })?;
                // 一次调用读多个文件。
                //
                // 规划器每份计划要读近二十个文件，而每一次 `read_file` 是一整轮
                // 往返：集群实测（2026-11-06，24 小时）里规划器的 5,765 次往返
                // 有 4,721 次的工具调用就是 `read_file`，`run_command` 一次也没
                // 出现过——模型不会自己把多次读塞进一条 shell，它照着工具表一个
                // 文件一个文件地读。每多一轮，之前读到的全部正文都要重发一遍，
                // 这份代价不在磁盘上，在上下文的重发次数上。
                //
                // 批量读减少的是往返，不是正文：每份文件仍然只给一页，读进来的
                // 字节与逐个读一样多，只是被重发的次数从「每份一次」变成「每批
                // 一次」。单个文件读失败不拖垮整批——只写进它自己那一项。
                if matches!(op, cog_core::ShellOp::ReadFile) {
                    if let Some(paths) = batch_paths(&arguments).map_err(ToolRunError::Failed)? {
                        let offset = arguments.get("offset").and_then(|v| v.as_u64());
                        let limit = arguments.get("limit").and_then(|v| v.as_u64());
                        let task_id = sandbox_task_id(scope);
                        let agent_id = scope
                            .map(|s| s.agent_id.clone())
                            .or_else(caller_agent_id)
                            .unwrap_or_else(|| format!("unscoped-tool-{}", name));
                        let mut files = Vec::with_capacity(paths.len());
                        for path in &paths {
                            let req = SandboxRequest {
                                task_id: task_id.clone(),
                                agent_id: agent_id.clone(),
                                payload: SandboxPayload::ReadFile { path: path.clone() },
                                input: serde_json::json!({ "path": path }),
                                timeout: self.shell_timeout,
                                limits: Default::default(),
                            };
                            let mut stream = match backend.execute_stream(&req).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    files.push(serde_json::json!({
                                        "path": path,
                                        "error": e.to_string(),
                                    }));
                                    continue;
                                }
                            };
                            let mut stdout = String::new();
                            let mut stderr = String::new();
                            let mut code = 0;
                            while let Some(event) = stream.next().await {
                                match event {
                                    CommandEvent::Stdout { data } => stdout.push_str(&data),
                                    CommandEvent::Stderr { data } => stderr.push_str(&data),
                                    CommandEvent::Exit { code: c } => code = c,
                                    // 这一批的每一份文件都在同一棵树上；条目里不带
                                    // 路径，模型已经用相对路径问的
                                    CommandEvent::Workdir { .. } => {}
                                }
                            }
                            if code == 0 {
                                let window = read_window(&stdout, offset, limit);
                                let mut entry = serde_json::Map::new();
                                entry.insert("path".into(), serde_json::json!(path));
                                if let Some(fields) = window.as_object() {
                                    for (k, v) in fields {
                                        entry.insert(k.clone(), v.clone());
                                    }
                                }
                                files.push(serde_json::Value::Object(entry));
                            } else {
                                files.push(serde_json::json!({
                                    "path": path,
                                    "error": stderr,
                                }));
                            }
                        }
                        return Ok(serde_json::json!({ "files": files }));
                    }
                }
                let payload =
                    match op {
                        cog_core::ShellOp::Command | cog_core::ShellOp::CommandBackground => {
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
                        cog_core::ShellOp::EditFile => {
                            let path = arguments.get("path").and_then(|v| v.as_str()).ok_or_else(
                                || cog_core::SFError::Validation("path required".into()),
                            )?;
                            let old_string = arguments
                                .get("old_string")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                cog_core::SFError::Validation("old_string required".into())
                            })?;
                            let new_string = arguments
                                .get("new_string")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                cog_core::SFError::Validation("new_string required".into())
                            })?;
                            SandboxPayload::EditFile {
                                path: path.to_string(),
                                old_string: old_string.to_string(),
                                new_string: new_string.to_string(),
                                replace_all: arguments
                                    .get("replace_all")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false),
                            }
                        }
                    };
                // 行范围在参数被交给沙箱之前取出来：`input` 会把整份参数搬走，
                // 而读回结果时还要用它切出调用方要的那一段。
                let read_offset = arguments.get("offset").and_then(|v| v.as_u64());
                let read_limit = arguments.get("limit").and_then(|v| v.as_u64());
                // 单条命令可以自己定一堵墙；不写就用配置那一格。要的比配置大时当场
                // 拒绝并说出上限：静默改小，调用方会以为它拿到的是自己定的那个数，
                // 而它拿到的是一次在它没料到的时刻发生的超时。
                let timeout = match arguments.get("timeout_seconds") {
                    None => self.shell_timeout,
                    Some(value) => {
                        let secs = value.as_u64().filter(|n| *n > 0).ok_or_else(|| {
                            cog_core::SFError::Validation(
                                "`timeout_seconds` has to be a positive whole number of seconds"
                                    .into(),
                            )
                        })?;
                        let asked = std::time::Duration::from_secs(secs);
                        if asked > self.shell_timeout {
                            return Err(ToolRunError::Failed(cog_core::SFError::Validation(
                                format!(
                                    "timeout_seconds {secs} is longer than the {}s wall this \
                                     process allows (system.shell_timeout_secs); raise that \
                                     setting for a longer run -- the value is not shortened \
                                     silently",
                                    self.shell_timeout.as_secs()
                                ),
                            )));
                        }
                        asked
                    }
                };
                let req = SandboxRequest {
                    task_id: sandbox_task_id(scope),
                    // 没有任务作用域时，执行者就是派发器公布的那一个：沙箱里的
                    // 归属应当写谁发的这条命令，而 `unscoped-tool-<工具名>` 写的
                    // 是工具本身——同一台机器上两个人的 `run_command` 会共用这
                    // 个名字。
                    agent_id: scope
                        .map(|s| s.agent_id.clone())
                        .or_else(caller_agent_id)
                        .unwrap_or_else(|| format!("unscoped-tool-{}", name)),
                    payload,
                    input: arguments,
                    timeout,
                    limits: Default::default(),
                };
                // 背景命令：把句柄先交出去，再把流接进那本账里。这一刻起读得到
                // 的只有「已开、还没输出」——这正是对的，读者要能知道命令已经
                // 发出去了，而不是等它说完第一句话才看见它存在。
                if matches!(op, cog_core::ShellOp::CommandBackground) {
                    let backend = backend.clone();
                    let command = crate::background::open();
                    let handle = command.handle().to_string();
                    tokio::spawn(async move {
                        match backend.execute_stream(&req).await {
                            Ok(mut stream) => {
                                let mut code = None;
                                while let Some(event) = stream.next().await {
                                    if let Some(status) = command.push(event) {
                                        code = Some(status);
                                    }
                                }
                                command.finish(match code {
                                    Some(code) => crate::background::Ending::Exited(code),
                                    // A stream that ends without a status says
                                    // nothing about how the command ended, and
                                    // reporting it as success would hide that.
                                    None => crate::background::Ending::Failed(
                                        "the executor's stream ended without an exit status".into(),
                                    ),
                                });
                            }
                            Err(e) => {
                                command.finish(crate::background::Ending::Failed(e.to_string()))
                            }
                        }
                    });
                    return Ok(serde_json::json!({
                        "handle": handle,
                        "state": "running",
                    }));
                }
                let mut stream = backend.execute_stream(&req).await?;
                let mut stdout = String::new();
                let mut stderr = String::new();
                let mut code = 0;
                // 执行器最先说的那句话是「这条命令落在哪个目录」的绝对路径。
                // 它不在提示词里，因为只有执行器那台机器知道车道名对应哪条路径；
                // 不回给模型，模型就得再跑一条 `pwd` 才知道自己在哪。
                let mut workdir = None;
                while let Some(event) = stream.next().await {
                    match event {
                        CommandEvent::Workdir { path } => workdir = Some(path),
                        CommandEvent::Stdout { data } => stdout.push_str(&data),
                        CommandEvent::Stderr { data } => stderr.push_str(&data),
                        CommandEvent::Exit { code: c } => code = c,
                    }
                }
                match op {
                    cog_core::ShellOp::Command => {
                        let mut reply = serde_json::json!({
                            "stdout": stdout,
                            "stderr": stderr,
                            "code": code,
                        });
                        if let Some(workdir) = workdir {
                            reply["workdir"] = serde_json::Value::String(workdir);
                        }
                        Ok(reply)
                    }
                    cog_core::ShellOp::ReadFile => {
                        if code == 0 {
                            Ok(read_window(&stdout, read_offset, read_limit))
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
                    // The executor's sentence -- how many sites moved and at
                    // which lines -- is the whole result; repeating it in a
                    // field of our own would only give the model two spellings
                    // of the same fact to choose between.
                    cog_core::ShellOp::EditFile => {
                        if code == 0 {
                            Ok(serde_json::json!({ "success": true, "report": stdout }))
                        } else {
                            Err(cog_core::SFError::IO(stderr).into())
                        }
                    }
                    // Answered with a handle before this point; if that ever
                    // stops being true, the caller must hear that the tool is
                    // miswired rather than get an empty-looking success.
                    cog_core::ShellOp::CommandBackground => Err(cog_core::SFError::Agent(
                        "a background command is answered with a handle, not by waiting for its \
                         output"
                            .into(),
                    )
                    .into()),
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tools.read().unwrap().is_empty()
    }

    /// 每个工具在本进程里被点过几次，按名字排。
    ///
    /// 表里的每一个名字都在，没人点过的记 0：「这项没人用」与「这项不在这张表
    /// 上」是两件事，缺一个键会让人去猜自己读到的是哪一件。换一张新表（`fork`）
    /// 不会把这些数清零——见字段上的说明。
    pub fn call_counts(&self) -> BTreeMap<String, u64> {
        let counts = self.calls.lock().unwrap();
        let mut out: BTreeMap<String, u64> = self
            .tools
            .read()
            .unwrap()
            .keys()
            .map(|name| (name.clone(), 0))
            .collect();
        for (name, n) in counts.iter() {
            // 只报这张表此刻有的名字：一次 fork 里装过的临时工具不算这个平台会
            // 什么，把它列成「用过 N 次」会给出一个点不开的动作。
            if let Some(slot) = out.get_mut(name) {
                *slot = *n;
            }
        }
        out
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
            recorded[0].1.get(crate::LABEL_TOOL).map(String::as_str),
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

    /// 一次调用读多个文件。
    ///
    /// 迁移前每个文件一次往返：集群实测（2026-11-06，24 小时）规划器 5,765
    /// 次往返里 4,721 次就是逐个 `read_file`，而每次往返都要把之前读到的全部
    /// 正文重发一遍。批量读必须按调用方给的顺序回，否则调用方要自己按 path
    /// 去对，等于把省下的那几轮又花回去找。
    #[tokio::test]
    async fn read_file_reads_several_paths_in_one_call_and_keeps_their_order() {
        let dir = std::env::temp_dir().join(format!("cog-tools-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let b = dir.join("b.txt");
        let a = dir.join("a.txt");
        std::fs::write(&a, "alpha").unwrap();
        std::fs::write(&b, "beta").unwrap();
        let missing = dir.join("missing.txt");
        let (a, b, missing) = (
            a.to_str().unwrap(),
            b.to_str().unwrap(),
            missing.to_str().unwrap(),
        );

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());

        let out = registry
            .execute("read_file", serde_json::json!({"paths": [b, a, missing]}))
            .await
            .unwrap();
        let files = out["files"].as_array().expect("files array");
        assert_eq!(files.len(), 3);
        assert_eq!(files[0]["path"], b);
        assert_eq!(files[0]["content"], "beta");
        assert_eq!(files[1]["path"], a);
        assert_eq!(files[1]["content"], "alpha");
        assert_eq!(files[2]["path"], missing);
        assert!(
            files[2]["error"].is_string(),
            "one unreadable path must be reported on its own entry, not sink the batch: {out}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 批量的两道界都报错而不是猜：`path` 与 `paths` 同时给是没说清要读什么，
    /// 超过八份则是一次把太多页钉进上下文，之后每轮都要重发。猜一个都能省
    /// 一轮往返，但猜错了那一轮要付整份上下文的重发钱。
    #[tokio::test]
    async fn read_file_refuses_an_ambiguous_or_oversized_batch() {
        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());

        let both = registry
            .execute(
                "read_file",
                serde_json::json!({"path": "a.txt", "paths": ["b.txt"]}),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(both.contains("not both"), "unexpected message: {both}");

        let too_many: Vec<String> = (0..READ_FILE_MAX_BATCH + 1)
            .map(|i| format!("f{i}.txt"))
            .collect();
        let over = registry
            .execute("read_file", serde_json::json!({ "paths": too_many }))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            over.contains(&READ_FILE_MAX_BATCH.to_string()) && over.contains("at most"),
            "unexpected message: {over}"
        );
    }

    /// 带行范围读时只回那一段，但必须同时说清文件有多长、后面还有没有：
    /// 只回切片，读到的人会以为文件到那里就结束了；不回这一段，整份正文就
    /// 要进上下文，并且之后每一轮都跟着重发。
    #[tokio::test]
    async fn read_file_returns_the_requested_line_window_and_says_what_it_left() {
        let dir = std::env::temp_dir().join(format!("cog-tools-window-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lines.txt");
        let path = path.to_str().unwrap();
        let body: String = (1..=10).map(|i| format!("line {i}\n")).collect();

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());
        cog_core::ToolRegistry::register(&registry, builtins::write_file());
        registry
            .execute(
                "write_file",
                serde_json::json!({"path": path, "content": body}),
            )
            .await
            .unwrap();

        let part = registry
            .execute(
                "read_file",
                serde_json::json!({"path": path, "offset": 3, "limit": 4}),
            )
            .await
            .unwrap();
        assert_eq!(part["content"], "line 3\nline 4\nline 5\nline 6");
        assert_eq!(part["total_lines"], 10);
        assert_eq!(part["has_more"], true);

        let tail = registry
            .execute("read_file", serde_json::json!({"path": path, "offset": 9}))
            .await
            .unwrap();
        assert_eq!(tail["content"], "line 9\nline 10");
        assert_eq!(tail["has_more"], false);

        let whole = registry
            .execute("read_file", serde_json::json!({"path": path}))
            .await
            .unwrap();
        assert_eq!(whole["content"], body.as_str());
        assert!(
            whole.get("total_lines").is_none(),
            "不带范围的老调用形状不变"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 不带范围地读一份长文件，拿到的是一页，不是整份。
    ///
    /// 这份正文要在之后每一轮重发：一轮读出几千行，会把这一整段对话的输入
    /// 全部推高，而上游前缀缓存只认「与上一轮相同的字节」——新追加的那一坨
    /// 越大，命中率越低。所以默认给一页，并让读的人从 `next_offset` 接着读；
    /// 文件本来就短时行为不变，老调用方看到的一直是 `content`。
    #[tokio::test]
    async fn a_long_file_comes_back_one_page_at_a_time() {
        let dir = std::env::temp_dir().join(format!("cog-tools-page-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("long.txt");
        let path = path.to_str().unwrap();
        let body: String = (1..=2000).map(|i| format!("line {i}\n")).collect();

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());
        cog_core::ToolRegistry::register(&registry, builtins::write_file());
        registry
            .execute(
                "write_file",
                serde_json::json!({"path": path, "content": body}),
            )
            .await
            .unwrap();

        let first = registry
            .execute("read_file", serde_json::json!({"path": path}))
            .await
            .unwrap();
        assert_eq!(first["total_lines"], 2000);
        assert_eq!(first["start_line"], 1);
        assert_eq!(first["has_more"], true);
        assert_eq!(
            first["content"].as_str().unwrap().lines().count(),
            READ_FILE_DEFAULT_WINDOW_LINES
        );
        assert!(
            first["content"]
                .as_str()
                .unwrap()
                .starts_with("line 1\nline 2\n"),
            "一页从文件头开始"
        );

        let next = first["next_offset"].as_u64().unwrap();
        let second = registry
            .execute(
                "read_file",
                serde_json::json!({"path": path, "offset": next}),
            )
            .await
            .unwrap();
        assert_eq!(second["start_line"], next);
        assert!(
            second["content"]
                .as_str()
                .unwrap()
                .starts_with(&format!("line {next}")),
            "续读必须从上一页的下一行开始，不重不漏"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 行数没到也不是无限给：超长行照样要分页。
    ///
    /// 一页是给上下文的一笔预算，行数只是它的一半口径。压缩过的 JSON、一行
    /// 几十万字符的日志，600 行能有几兆——那样一页和整份读没有区别，翻页
    /// 也就白加了。
    #[tokio::test]
    async fn a_page_stops_on_characters_even_when_the_line_count_is_small() {
        let dir = std::env::temp_dir().join(format!("cog-tools-chars-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wide.txt");
        let path = path.to_str().unwrap();
        // 8 行、每行 10000 字符：行数远没到 600，字节数是 80000。
        let body: String = (1..=8)
            .map(|i| format!("{}{i}\n", "x".repeat(9999)))
            .collect();

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());
        cog_core::ToolRegistry::register(&registry, builtins::write_file());
        registry
            .execute(
                "write_file",
                serde_json::json!({"path": path, "content": body}),
            )
            .await
            .unwrap();

        let first = registry
            .execute("read_file", serde_json::json!({"path": path}))
            .await
            .unwrap();
        assert_eq!(first["total_lines"], 8);
        assert_eq!(first["has_more"], true);
        assert_eq!(
            first["content"].as_str().unwrap().lines().count(),
            3,
            "三万字符一页：三行就够，第四行会把它推到 40000"
        );
        assert_eq!(first["next_offset"], 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 被字符界截断的一页不能读成「文件到这儿就完了」。
    ///
    /// 一条读接上 `head` 之后再从里面数行，数出来的是这一页，不是文件：截断处
    /// 之后的那些，与「本来就不存在」在盘面上同形。所以被字符界停下的那一页
    /// 必须自己说清它停在哪——`total_lines` 是整份文件的行数，`has_more` 与
    /// `next_offset` 让读的人走到最后一行，而不许把余下的部分当成不存在就此收
    /// 尾。这里顺着 `next_offset` 一直翻到 `has_more: false`，把每一页的正文按
    /// 序接起来，要求它一字不差地还原整份文件：少一行、重一行、或者把某一页
    /// 的余下部分截掉，都会在这里对不上。
    ///
    /// principle: feedback_a_read_cut_with_head_reads_as_if_the_rest_were_absent
    #[tokio::test]
    async fn read_file_pages_cut_by_the_character_bound_walk_to_the_end() {
        let dir = std::env::temp_dir().join(format!("cog-tools-cut-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wide.txt");
        let path = path.to_str().unwrap();
        // 六行，每行都比一页的字符预算还长：一页装不下一行，翻页只能靠「至少给
        // 一行」这条兜底往前走——走不动是死循环，走快了是漏行。
        const LINES: usize = 6;
        let body: String = (1..=LINES)
            .map(|i| {
                format!(
                    "line {i} {} {i}\n",
                    "x".repeat(READ_FILE_PAGE_MAX_CHARS + 1024)
                )
            })
            .collect();
        let expected: Vec<String> = body.lines().map(str::to_string).collect();
        assert_eq!(expected.len(), LINES);

        let registry = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&registry, builtins::read_file());
        cog_core::ToolRegistry::register(&registry, builtins::write_file());
        registry
            .execute(
                "write_file",
                serde_json::json!({ "path": path, "content": body }),
            )
            .await
            .unwrap();

        let mut offset: Option<u64> = None;
        let mut collected: Vec<String> = Vec::new();
        let mut pages = 0usize;
        loop {
            let arguments = match offset {
                Some(from) => serde_json::json!({ "path": path, "offset": from }),
                None => serde_json::json!({ "path": path }),
            };
            let page = registry.execute("read_file", arguments).await.unwrap();
            pages += 1;
            assert!(
                pages <= LINES,
                "翻了 {pages} 页还没读到末尾，offset 停在 {offset:?}：这一页没有往前走"
            );
            assert_eq!(
                page["total_lines"], LINES as u64,
                "第 {pages} 页只说自己这一页有多长，读的人会把截断处当成文件末尾: {page}"
            );
            let content = page["content"].as_str().expect("content 是一段字符串");
            assert!(
                !content.is_empty(),
                "第 {pages} 页是空的：翻页停在了文件中间"
            );
            collected.extend(content.lines().map(str::to_string));
            assert_eq!(
                page["has_more"].as_bool(),
                Some(page["next_offset"].is_u64()),
                "has_more 与 next_offset 必须说的是同一件事: {page}"
            );
            match page["next_offset"].as_u64() {
                Some(next) => {
                    assert_eq!(
                        next as usize,
                        collected.len() + 1,
                        "next_offset 不是上一页之后的第一行: {page}"
                    );
                    offset = Some(next);
                }
                None => break,
            }
        }
        assert!(pages > 1, "六行超长行必须分页读，一页读完说明字符界没生效");
        assert_eq!(
            collected, expected,
            "把所有页接起来必须还原整份文件：少一行、重一行，或者把一页的余下部分当成不存在，都会对不上"
        );

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
                        workdir: None,
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
                // Editing happens where the file is; this double has no story
                // for it, and says so rather than answering as if it worked.
                // The replacement itself is covered by `cog_extension::edit`.
                SandboxPayload::EditFile { .. } => {
                    return Err(cog_core::SFError::Agent(
                        "LocalShellBackend does not edit files".into(),
                    ));
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

    /// 复制一张工具表，搬过去的是能跑的工具，不只是名单。
    ///
    /// 判据是「调用真的执行了」：`fork()` 之后 `run_command` 回的是命令的读数，
    /// 不是 `SandboxBackend not configured`。只比名单不会红——那份名单本来就
    /// 一样，差别全在跑不跑得动。界面上的员工曾经就是拿着这样一张名单：它看得见
    /// shell，一调就被告知这个进程没有沙箱。
    #[tokio::test]
    async fn a_forked_registry_carries_the_ability_to_run_its_tools() {
        let shared = ToolRegistry::new().with_sandbox_backend(Arc::new(LocalShellBackend));
        cog_core::ToolRegistry::register(&shared, builtins::run_command());

        let mine = shared.fork();
        assert_eq!(mine.names(), shared.names(), "复制表时工具本身要跟着过去");
        let out = mine
            .execute("run_command", serde_json::json!({"command": "echo forked"}))
            .await
            .expect("复制出来的表要跑得动它自己报出来的工具");
        assert_eq!(out["code"], 0);
        assert_eq!(out["stdout"].as_str().unwrap().trim(), "forked");

        // 另一张表：装进去的工具只属于这一张。
        cog_core::ToolRegistry::register(&mine, builtins::read_file());
        assert!(
            !shared.names().iter().any(|name| name == "read_file"),
            "装在这张表上的工具漏进了共用的那张：一条运行的工具会变成每个 agent 的工具"
        );
    }

    /// 执行器报的目录要落到模型的回复里，而不是只落在事件流里。
    ///
    /// 车道名是调用方拼的，目录只有执行器知道：它按名字把树解出来，语言里
    /// 没有第二条路能让模型拿到那个绝对路径。所以回复里带一格 `workdir`；
    /// 落在任何树之外的调用（本机执行、无名无任务）反过来不许凭空报出一条
    /// 路径——那会让模型以为自己在某棵树里。
    #[tokio::test]
    async fn a_command_reply_names_the_checkout_it_ran_in() {
        let (backend, tx) = OpenStream::pair();
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        tx.send(CommandEvent::Workdir {
            path: "/srv/workspaces/employee-kai-a9ab11fc".into(),
        })
        .unwrap();
        tx.send(CommandEvent::Stdout {
            data: "/srv/workspaces/employee-kai-a9ab11fc\n".into(),
        })
        .unwrap();
        tx.send(CommandEvent::Exit { code: 0 }).unwrap();
        drop(tx);

        let out = registry
            .execute("run_command", serde_json::json!({"command": "pwd"}))
            .await
            .unwrap();
        assert_eq!(
            out["workdir"], "/srv/workspaces/employee-kai-a9ab11fc",
            "执行器报出的目录没有回给模型：它只能再跑一条 pwd 去问一个回复里本就有的答案"
        );

        let (backend, tx) = OpenStream::pair();
        let bare = ToolRegistry::new().with_sandbox_backend(backend as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&bare, builtins::run_command());
        tx.send(CommandEvent::Stdout {
            data: "hi\n".into(),
        })
        .unwrap();
        tx.send(CommandEvent::Exit { code: 0 }).unwrap();
        drop(tx);
        let out = bare
            .execute("run_command", serde_json::json!({"command": "echo hi"}))
            .await
            .unwrap();
        assert!(
            out.get("workdir").is_none(),
            "没有车道的调用凭空报了一个工作目录：{out}"
        );
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

    /// 一条命令可以自己把墙立短一点：调用方写多少，沙箱就收到多少。
    #[tokio::test]
    async fn a_command_can_name_its_own_shorter_wall() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_shell_timeout(600)
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        registry
            .execute(
                "run_command",
                serde_json::json!({"command": "echo hi", "timeout_seconds": 45}),
            )
            .await
            .unwrap();

        let seen = backend.seen.lock().await;
        assert_eq!(seen[0].2, Duration::from_secs(45), "沙箱收到的是 45 秒");
    }

    /// 比配置那一格更大时当场拒绝，并把它自己报出来。
    ///
    /// 静默截成配置值，调用方会以为它按自己的数在跑，然后在它没料到的时刻看到
    /// 超时——两个数里真正生效的那个必须出现在它读得到的地方。
    #[tokio::test]
    async fn a_command_that_asks_for_longer_than_the_ceiling_is_refused_by_name() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_shell_timeout(300)
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        let err = registry
            .execute(
                "run_command",
                serde_json::json!({"command": "cargo build", "timeout_seconds": 900}),
            )
            .await
            .expect_err("900 秒超过 300 秒的上限，应当拒绝");
        let text = err.to_string();
        assert!(text.contains("900"), "要说出调用方要的那个数：{text}");
        assert!(text.contains("300"), "要说出真正生效的上限：{text}");
        assert!(
            backend.seen.lock().await.is_empty(),
            "拒绝的命令不该已经交给沙箱"
        );
    }

    /// Records the payload an edit hands the executor and answers with what the
    /// test scripted. What the agent side has to get right is the shape it
    /// sends and how it carries the answer back; the replacement itself runs
    /// next to the file and is covered by `cog_extension::edit`.
    #[derive(Default)]
    struct EditBackend {
        seen: tokio::sync::Mutex<Vec<SandboxPayload>>,
        /// exit code, stdout, stderr.
        reply: std::sync::Mutex<(i32, String, String)>,
    }

    #[async_trait::async_trait]
    impl SandboxBackend for EditBackend {
        async fn execute(&self, req: &SandboxRequest) -> SFResult<cog_core::SandboxResult> {
            self.seen.lock().await.push(req.payload.clone());
            let (exit_code, stdout, stderr) = self.reply.lock().unwrap().clone();
            Ok(cog_core::SandboxResult {
                stdout,
                stderr,
                exit_code,
                ..Default::default()
            })
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Err(cog_core::SFError::Agent("unsupported".into()))
        }
    }

    fn edit_registry(backend: Arc<EditBackend>) -> ToolRegistry {
        let registry = ToolRegistry::new().with_sandbox_backend(backend as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::edit_file());
        registry
    }

    /// 一次编辑只送两段文本：要换掉的、换成的。整份文件不在这条路上走。
    #[tokio::test]
    async fn an_edit_names_the_text_to_replace_and_where_it_is() {
        let backend = Arc::new(EditBackend::default());
        *backend.reply.lock().unwrap() = (
            0,
            "edited src/lib.rs: 1 replacement at line 12".into(),
            String::new(),
        );
        let registry = edit_registry(backend.clone());

        let out = registry
            .execute(
                "edit_file",
                serde_json::json!({
                    "path": "src/lib.rs",
                    "old_string": "let a = 1;",
                    "new_string": "let a = 2;",
                }),
            )
            .await
            .unwrap();

        assert_eq!(out["success"], true);
        assert_eq!(out["report"], "edited src/lib.rs: 1 replacement at line 12");
        let seen = backend.seen.lock().await;
        assert_eq!(seen.len(), 1);
        match &seen[0] {
            SandboxPayload::EditFile {
                path,
                old_string,
                new_string,
                replace_all,
            } => {
                assert_eq!(path, "src/lib.rs");
                assert_eq!(old_string, "let a = 1;");
                assert_eq!(new_string, "let a = 2;");
                assert!(!*replace_all, "没写 replace_all 就是只改一处");
            }
            other => panic!("edit_file 应当发 EditFile 载荷，实际是 {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_edit_can_ask_to_change_every_site() {
        let backend = Arc::new(EditBackend::default());
        let registry = edit_registry(backend.clone());

        registry
            .execute(
                "edit_file",
                serde_json::json!({
                    "path": "f.rs",
                    "old_string": "x = 1;",
                    "new_string": "x = 2;",
                    "replace_all": true,
                }),
            )
            .await
            .unwrap();

        let seen = backend.seen.lock().await;
        match &seen[0] {
            SandboxPayload::EditFile { replace_all, .. } => assert!(*replace_all),
            other => panic!("应当发 EditFile 载荷，实际是 {other:?}"),
        }
    }

    /// 执行器为什么拒绝，原话要原样到调用方手上：它要拿这句话里的行号决定补多少下文。
    #[tokio::test]
    async fn an_edit_the_executor_refuses_reaches_the_caller_with_its_reason() {
        let backend = Arc::new(EditBackend::default());
        *backend.reply.lock().unwrap() = (
            1,
            String::new(),
            "`old_string` appears 2 times in f.rs (at lines 2, 4)".into(),
        );
        let registry = edit_registry(backend.clone());

        let err = registry
            .execute(
                "edit_file",
                serde_json::json!({
                    "path": "f.rs",
                    "old_string": "x = 1;",
                    "new_string": "x = 2;",
                }),
            )
            .await
            .expect_err("执行器返回非零退出码时工具应当是失败，不是空成功");
        let text = err.to_string();
        assert!(text.contains("appears 2 times"), "{text}");
        assert!(text.contains("at lines 2, 4"), "{text}");
    }

    /// 缺了要换的那段文本，在交给执行器之前就该拒绝：跑一趟只知道文件没变，
    /// 而调用方会以为是锚点没对上。
    #[tokio::test]
    async fn an_edit_without_the_text_to_replace_is_refused_before_the_executor_sees_it() {
        let backend = Arc::new(EditBackend::default());
        let registry = edit_registry(backend.clone());

        let err = registry
            .execute(
                "edit_file",
                serde_json::json!({ "path": "f.rs", "new_string": "y" }),
            )
            .await
            .expect_err("缺 old_string 应当拒绝");
        assert!(err.to_string().contains("old_string"), "{err}");
        assert!(
            backend.seen.lock().await.is_empty(),
            "被拒绝的调用不该已经交给执行器"
        );
    }

    /// A backend whose stream stays open until the test closes it.
    ///
    /// The point of the background path is what the caller gets while the
    /// command is still running, and a stream that ends immediately cannot
    /// show that: the answer and the end arrive together in either case.
    struct OpenStream {
        seen: tokio::sync::Mutex<Vec<SandboxPayload>>,
        rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<CommandEvent>>>,
    }

    impl OpenStream {
        fn pair() -> (Arc<Self>, tokio::sync::mpsc::UnboundedSender<CommandEvent>) {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    seen: tokio::sync::Mutex::new(Vec::new()),
                    rx: tokio::sync::Mutex::new(Some(rx)),
                }),
                tx,
            )
        }
    }

    #[async_trait::async_trait]
    impl SandboxBackend for OpenStream {
        async fn execute(&self, _req: &SandboxRequest) -> SFResult<cog_core::SandboxResult> {
            Err(SFError::Agent(
                "this backend answers as a stream; buffering it would hide the difference \
                 the background path is about"
                    .into(),
            ))
        }

        async fn execute_stream(
            &self,
            req: &SandboxRequest,
        ) -> SFResult<cog_core::CommandEventStream> {
            self.seen.lock().await.push(req.payload.clone());
            let rx = self.rx.lock().await.take().expect("one stream per backend");
            Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (event, rx))
            })))
        }

        async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
            Err(SFError::Agent("unsupported".into()))
        }
    }

    fn background_registry() -> (
        ToolRegistry,
        Arc<OpenStream>,
        tokio::sync::mpsc::UnboundedSender<CommandEvent>,
    ) {
        let (backend, tx) = OpenStream::pair();
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::run_command_background());
        cog_core::ToolRegistry::register(&registry, builtins::command_output());
        (registry, backend, tx)
    }

    /// 发出去的长命令当场回句柄，然后边跑边读得到。
    #[tokio::test]
    async fn a_background_command_answers_with_a_handle_while_the_stream_is_still_open() {
        let (registry, backend, tx) = background_registry();

        let out = tokio::time::timeout(
            Duration::from_secs(5),
            registry.execute(
                "run_command_background",
                serde_json::json!({"command": "cargo test --all"}),
            ),
        )
        .await
        .expect("流还开着时这次调用也必须已经回来：它要交的是句柄，不是输出")
        .unwrap();

        let handle = out["handle"].as_str().expect("回答里要带句柄").to_string();
        assert_eq!(out["state"], "running");

        // 命令还在跑，已经写出来的那一截读得到。
        tx.send(CommandEvent::Workdir {
            path: "/srv/worktrees/employee-kai-a9ab11fc".into(),
        })
        .unwrap();
        tx.send(CommandEvent::Stdout {
            data: "compiling crate A\n".into(),
        })
        .unwrap();
        let command = crate::background::get(&handle).expect("句柄一交出去就在账上");
        tokio::time::timeout(Duration::from_secs(5), async {
            while command.snapshot(0)["stdout"] == "" {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("执行器写出来的东西要接进账里");

        // 句柄先交出去，命令随后才真的送到执行器：账上有字的时候那条载荷
        // 一定已经发过了。所以这条断言放在这里而不是上面。
        {
            let seen = backend.seen.lock().await;
            assert!(
                matches!(seen[0], SandboxPayload::Command { .. }),
                "背景命令发的仍是 Command 载荷，差别只在调用方这侧"
            );
        }

        let mid = registry
            .execute("command_output", serde_json::json!({ "handle": handle }))
            .await
            .unwrap();
        assert_eq!(mid["state"], "running");
        assert_eq!(mid["stdout"], "compiling crate A\n");

        // 结束之后取回退出码，游标之后没有新东西。
        tx.send(CommandEvent::Stderr {
            data: "warning: unused import\n".into(),
        })
        .unwrap();
        tx.send(CommandEvent::Exit { code: 3 }).unwrap();
        drop(tx);

        let done = registry
            .execute(
                "command_output",
                serde_json::json!({ "handle": handle, "wait_seconds": 5 }),
            )
            .await
            .unwrap();
        assert_eq!(done["state"], "exited");
        assert_eq!(done["exit_code"], 3);
        assert_eq!(done["stderr"], "warning: unused import\n");
        assert_eq!(
            done["workdir"], "/srv/worktrees/employee-kai-a9ab11fc",
            "执行器报出的工作目录要跟着输出一起回给模型：否则长命令跑完，读者仍不知道自己刚才在哪编译"
        );

        let cursor = done["next_offset"].as_u64().unwrap();
        let again = registry
            .execute(
                "command_output",
                serde_json::json!({ "handle": handle, "offset": cursor }),
            )
            .await
            .unwrap();
        assert_eq!(again["stdout"], "", "游标之后没有新输出");
    }

    /// 没人开过的句柄按名字拒绝：空输出读起来像「命令什么都没说」。
    #[tokio::test]
    async fn a_read_of_a_handle_nobody_opened_is_refused_by_name() {
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(&registry, builtins::command_output());

        let err = registry
            .execute(
                "command_output",
                serde_json::json!({ "handle": "cmd-doesnotexist" }),
            )
            .await
            .expect_err("没有这条命令，读它应当是失败而不是空输出");
        assert!(err.to_string().contains("cmd-doesnotexist"), "{err}");
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

    /// Every call an announced agent makes lands in one workspace.
    ///
    /// A conversation has no DAG task, so its calls reach the executor
    /// unscoped — and each one used to carry a fresh random id, which the
    /// executor routes to a separate checkout. Measured on the deployed chat
    /// path (2026-11-06): `pwd` answered
    /// `/opt/cogneva/sandbox/workspaces/unscoped-shell-f294ca21-…`, and the
    /// next call in the same conversation would have been given another
    /// directory. So a write and a read two calls apart could never see the
    /// same file, and the only work a conversation could finish was work that
    /// fits in one call. The caller's id is stable for the whole conversation,
    /// which is what makes it the workspace's name.
    ///
    /// Without an announced caller the random id stays: two unknown callers
    /// sharing one directory would be a worse failure than the churn.
    #[tokio::test]
    async fn calls_from_one_announced_agent_share_one_workspace() {
        let backend = Arc::new(RecordingBackend::default());
        let registry =
            ToolRegistry::new().with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>);
        cog_core::ToolRegistry::register(&registry, builtins::write_file());
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        CALLER_AGENT_ID
            .scope("ralph-3be244e0".to_string(), async {
                registry
                    .execute(
                        "write_file",
                        serde_json::json!({"path": "a.txt", "content": "x"}),
                    )
                    .await
                    .unwrap();
                registry
                    .execute("run_command", serde_json::json!({"command": "cat a.txt"}))
                    .await
                    .unwrap();
                registry
                    .execute("run_command", serde_json::json!({"command": "ls"}))
                    .await
                    .unwrap();
            })
            .await;

        let seen = backend.seen.lock().await.clone();
        assert_eq!(seen.len(), 3, "{seen:?}");
        assert_eq!(
            seen[0].0,
            cog_core::worktree_lane("ralph-3be244e0"),
            "an unscoped call lands in the calling agent's own checkout: {seen:?}"
        );
        assert!(
            seen.iter().all(|call| call.0 == seen[0].0),
            "every call one agent makes has to land in one workspace, or a write \
             is invisible to the read after it: {seen:?}"
        );
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

    /// An announced caller is an identity, and it is the only one a task-less
    /// run has.
    ///
    /// The roster's employees and squad members run prompts with no task; the
    /// dispatcher announces who they are through [`CALLER_AGENT_ID`]. Judging
    /// "has a task scope" as "has an identity" refused every shell call they
    /// made as `requires task identity` even though the answer was right there
    /// -- measured 2026-11-07 on the deployed chat path. The second half is the
    /// other reason the caller matters: the workspace a task-less call lands in
    /// is keyed by the caller, so two calls from one agent share it, and two
    /// agents do not.
    #[tokio::test]
    async fn require_identity_accepts_an_announced_caller_and_keeps_one_workspace() {
        let backend = Arc::new(RecordingBackend::default());
        let registry = ToolRegistry::new()
            .with_sandbox_backend(backend.clone() as Arc<dyn SandboxBackend>)
            .with_require_identity(true);
        cog_core::ToolRegistry::register(&registry, builtins::run_command());

        for _ in 0..2 {
            CALLER_AGENT_ID
                .scope("squad:DSC-1A2B-generator".to_string(), async {
                    registry
                        .execute("run_command", serde_json::json!({"command": "pwd"}))
                        .await
                        .unwrap();
                })
                .await;
        }

        let seen = backend.seen.lock().await.clone();
        assert_eq!(
            seen.len(),
            2,
            "an announced caller is not refused: {seen:?}"
        );
        assert_eq!(
            seen[0].0,
            cog_core::worktree_lane("squad:DSC-1A2B-generator")
        );
        assert_eq!(
            seen[1].0, seen[0].0,
            "the second call landed in another workspace: {seen:?}"
        );
        assert_eq!(
            seen[0].1, "squad:DSC-1A2B-generator",
            "the sandbox records the tool's name as the agent, not the caller: {seen:?}"
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

    /// The same listing as the inherent `list()`, not a second one: a caller
    /// that reads the boundary of a tool and a caller that serializes the tool
    /// table into an LLM request must see one description, or the platform
    /// advertises one shape and runs another.
    fn definitions(&self) -> Vec<Tool> {
        ToolRegistry::list(self)
    }

    fn calls(&self) -> BTreeMap<String, u64> {
        ToolRegistry::call_counts(self)
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
                          when a checkout is provisioned. Pass offset and/or limit \
                          to read one line range of a long file instead of the whole \
                          thing; the result then also reports the file's total line \
                          count and whether more remains. To read several files, \
                          pass paths instead of path: one call returns them all, \
                          which costs far less than one call per file."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path" },
                    "paths": {
                        "type": "array",
                        "items": { "type": "string" },
                        "maxItems": READ_FILE_MAX_BATCH,
                        "description": "Several file paths to read in this one call; the result is an array of entries in the same order, each with path and either content or error. Give either path or paths, not both. At most 8 files per call"
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "1-based line to start at; omit to start at the first line"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "How many lines to return; omit to read one page (600 lines) or the rest of the file if shorter. The reply carries total_lines, has_more and next_offset, so a long file is read page by page instead of in one piece"
                    }
                },
                "anyOf": [{ "required": ["path"] }, { "required": ["paths"] }]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::ReadFile),
        }
    }

    pub fn write_file() -> Tool {
        Tool {
            name: "write_file".into(),
            description: "Write content to a file, replacing whatever it held. Relative paths \
                          resolve against the task's working directory. To change part of an \
                          existing file, use edit_file: rewriting the whole thing to change a \
                          line carries every other line again."
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

    pub fn edit_file() -> Tool {
        Tool {
            name: "edit_file".into(),
            description: "Replace an exact string in a file with new text, without rewriting \
                          the whole file. `old_string` is the text to find, copied exactly \
                          from the file including indentation; `new_string` replaces it. When \
                          the text appears more than once the call is refused and the \
                          occurrences' line numbers are named, so include enough surrounding \
                          text to pick one site -- or set `replace_all` to change all of \
                          them. The reply says how many replacements were made and at which \
                          lines. Read the file first: an anchor that does not match the bytes \
                          on disk is refused, never guessed at."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path" },
                    "old_string": {
                        "type": "string",
                        "description": "The exact text to replace, copied from the file"
                    },
                    "new_string": {
                        "type": "string",
                        "description": "The text to put in its place"
                    },
                    "replace_all": {
                        "type": "boolean",
                        "description": "Replace every occurrence instead of insisting on exactly one; defaults to false"
                    }
                },
                "required": ["path", "old_string", "new_string"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::EditFile),
        }
    }

    pub fn run_command() -> Tool {
        Tool {
            name: "run_command".into(),
            description: "Run a shell command with full shell syntax: pipes, redirects, \
                          variable expansion, and scripts written via write_file are all \
                          supported. The command runs in the task's working directory; when \
                          a repository checkout is provisioned it is already rooted there, \
                          so work on the repo in place instead of cloning it again. For a \
                          build or a test suite that takes minutes, use \
                          run_command_background instead: it answers with a handle at once \
                          and the turn goes on while the command runs."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Command to run" },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "How long this one command may run, in seconds. \
                                        Leave it out for the configured wall \
                                        (`system.shell_timeout_secs`). A value above that \
                                        wall is refused with the limit named, never \
                                        silently shortened, so build and test runs that \
                                        need longer say so in the call"
                    }
                },
                "required": ["command"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::Command),
        }
    }

    pub fn run_command_background() -> Tool {
        Tool {
            name: "run_command_background".into(),
            description: "Start a shell command and answer at once with a handle, instead of \
                          waiting for it to finish. Use it for builds and test suites that take \
                          minutes: a foreground run holds the whole turn until it ends, while \
                          this one keeps running in the task's working directory and the turn \
                          goes on. Read its output and exit status with command_output."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Command to run" },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "How long this command may run in total, in seconds. \
                                        Leave it out for the configured wall; a value above \
                                        the wall is refused with the limit named"
                    }
                },
                "required": ["command"]
            }),
            implementation: ToolImplementation::Shell(cog_core::ShellOp::CommandBackground),
        }
    }

    pub fn command_output() -> Tool {
        Tool {
            name: "command_output".into(),
            description: "Read what a background command started by run_command_background has \
                          written, and how it ended. Leave `offset` out on the first read; after \
                          that pass back the `next_offset` from the previous answer and you get \
                          only what is new. `wait_seconds` waits that long for the command to \
                          finish before answering -- without it the call answers immediately."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "handle": {
                        "type": "string",
                        "description": "The handle run_command_background returned"
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Byte cursor from a previous answer's next_offset; omit to read from the start"
                    },
                    "wait_seconds": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "How long to wait for the command to finish before answering; omit or 0 to answer now"
                    }
                },
                "required": ["handle"]
            }),
            implementation: ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move {
                    let handle = args
                        .get("handle")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| cog_core::SFError::Validation("handle required".into()))?;
                    let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                    let wait = args
                        .get("wait_seconds")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let command = crate::background::get(handle).ok_or_else(|| {
                        cog_core::SFError::Validation(format!(
                            "no background command named {handle}: it was never started, or it \
                             finished long enough ago that its output was let go"
                        ))
                    })?;
                    command.wait(wait).await;
                    Ok(command.snapshot(offset))
                })
            })),
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
        cog_core::ToolRegistry::register(registry, edit_file());
        cog_core::ToolRegistry::register(registry, run_command());
        cog_core::ToolRegistry::register(registry, run_command_background());
        cog_core::ToolRegistry::register(registry, command_output());
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
        // Filing work needs the same two halves the memory reads need -- a
        // client and an API base -- and the roster only to check a named
        // assignee. Registered whether or not they are present, so a caller
        // gets "this process cannot file work" rather than "there is no such
        // tool"; a missing roster still files, it just cannot check the name
        // first, and says so by not being able to promise delivery to it.
        cog_core::ToolRegistry::register(
            registry,
            start_task(
                inputs.http_client.clone(),
                inputs.memory_api_base.clone(),
                inputs.agent_registry.clone(),
            ),
        );
        // 读账与派活是同一面的一对。只给派活、不给读账，智能体就只能在心里数
        // 自己派过几件——而它看不见的正是别人派的那几千件，也就是「现在还有
        // 什么可做」这个问题的全部答案。
        cog_core::ToolRegistry::register(
            registry,
            read_task_ledger(inputs.http_client.clone(), inputs.memory_api_base.clone()),
        );
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
                          registration that has expired is not listed. Every entry \
                          carries the agent's `display_name` -- the name the workbench \
                          shows, derived from the id -- so a colleague is addressed by \
                          the same name a person sees. When the run established an \
                          identity, your own entry carries `is_me: true` and the result \
                          repeats your id as `caller`."
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
                    // 名字来自名册行（工号＋姓名）。读不到名册时留下的是一句
                    // 「待编」而不是退回机器码：目录里出现一串十六进制，读的
                    // 人只会把它当成名字，于是没人去补那一次编号。
                    let roster: std::collections::HashMap<String, cog_core::EmployeeRecord> =
                        match registry.employees().await {
                            Ok(rows) => rows
                                .into_iter()
                                .map(|row| (row.agent_id.clone(), row))
                                .collect(),
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "the employee roster could not be read; the directory \
                                     lists ids without names this pass"
                                );
                                std::collections::HashMap::new()
                            }
                        };
                    let agents: Vec<serde_json::Value> = live
                        .into_iter()
                        .filter(|r| role.is_none_or(|role| r.role == role))
                        .filter(|r| {
                            capability.is_none_or(|cap| r.capabilities.iter().any(|c| c == cap))
                        })
                        .map(|r| {
                            let is_me = me.as_deref() == Some(r.agent_id.as_str());
                            // 与员工页同一个来源：名册行。目录和名单读到的是
                            // 同一个名字，没有第二条拼法。
                            let entry = roster.get(&r.agent_id);
                            let display_name = match entry {
                                Some(row) => row.label(),
                                None => cog_core::unregistered_label(&r.agent_id),
                            };
                            let number = entry
                                .and_then(|row| row.number)
                                .map(|n| format!("{}{n:04}", cog_core::EMPLOYEE_NUMBER_PREFIX));
                            serde_json::json!({
                                "agent_id": r.agent_id,
                                "display_name": display_name,
                                "number": number,
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

    /// Put a piece of work on the shared graph, and name who carries it.
    ///
    /// This is how an agent starts something the platform owns rather than
    /// something its own turn owns. The row is a work item — the same kind the
    /// scenes file — so the answer is produced by an employee on the roster,
    /// judged against the row's acceptance criteria, and read back from the
    /// task's result by whoever asked for it. Two agents coordinating over the
    /// inbox reach each other, but nothing they say survives the turn; a row
    /// does, and it is what the workbench shows.
    ///
    /// The filing goes through the platform's own submission door rather than
    /// an in-process graph handle, because the door is where the rules live:
    /// credential-shaped input is refused, a verified row without acceptance
    /// criteria is refused, the workspace's quota is evaluated, and the venue
    /// is recorded on the row. A second write path here would be a second
    /// answer to all four, and the deployment's business processes hold no
    /// credentials precisely so they reach this face instead.
    ///
    /// A name nobody holds is refused before anything is filed: the row would
    /// otherwise be handed to the rotation, and the caller would read a queued
    /// submission as "my colleague is on it".
    pub fn start_task(
        client: Option<Arc<dyn HttpClient>>,
        api_base: Option<String>,
        registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    ) -> Tool {
        Tool {
            name: "start_task".into(),
            description: "File a piece of work on the shared task graph, optionally naming the \
                          person who should carry it. The row appears on the workbench under the \
                          venue you give it, and its acceptance criteria are what the answer is \
                          judged by. Use it when the work outlives this turn, needs someone \
                          else, or should be on the board where the owner can see it. The call \
                          returns as soon as the row is queued; the answer comes back in the \
                          task's result, not in this turn. Call it again with the returned \
                          goal_id to add more items to the same goal — that is how a squad is \
                          formed around one goal."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What to do, stated so whoever picks it up can act on it. \
                                        The first line reads as the title."
                    },
                    "acceptance": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "What done means: one falsifiable sentence each. A filing \
                                        without one is refused, because nothing downstream \
                                        invents a criterion and the row would sit on the board \
                                        reading like work in progress."
                    },
                    "venue": {
                        "type": "string",
                        "description": "Which column of the board this work belongs in — memory, \
                                        wiki, eval … Omit for work that belongs to no venue."
                    },
                    "assign_to": {
                        "type": "string",
                        "description": "Agent id of the person who should carry it, as it appears \
                                        in list_agents. Omit to leave it to rotation over the \
                                        standing employees."
                    },
                    "goal_id": {
                        "type": "string",
                        "description": "Returned by an earlier call: add this item to that goal \
                                        instead of starting a new one."
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "How long the row may stay open before the graph fails it \
                                        (default 3600)."
                    }
                },
                "required": ["goal", "acceptance"]
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let client = client.clone();
                let api_base = api_base.clone();
                let registry = registry.clone();
                Box::pin(async move {
                    let base = memory_base(api_base.as_deref())?;
                    let client = memory_client(client)?;
                    let filed = start_task_filing(&args, caller_agent_id().as_deref())?;

                    // Asked before the write, for the same reason the send tool
                    // asks: a name nobody holds would be handed to the rotation
                    // instead, and the caller would read "queued" as "my
                    // colleague has it".
                    if let (Some(assignee), Some(registry)) = (filed.assignee(), registry.as_ref())
                    {
                        match registry.get(assignee).await {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                return Err(SFError::Validation(format!(
                                    "no live agent named {assignee:?}, so the work would fall \
                                     to rotation rather than to the person you named"
                                )))
                            }
                            Err(e) => {
                                return Err(SFError::Agent(format!(
                                    "cannot read the agent roster, so whether {assignee:?} is \
                                     live is unknown: {e}"
                                )))
                            }
                        }
                    }

                    let url = format!("{base}/api/v1/tasks");
                    let mut request = cog_core::HttpRequest::new("POST", &url);
                    request.timeout_secs = Some(MEMORY_READ_TIMEOUT_SECS);
                    request.headers.insert(
                        "content-type".into(),
                        cog_core::contract::net::CONTENT_TYPE_JSON.into(),
                    );
                    request.body =
                        Some(serde_json::to_vec(&filed.body).map_err(SFError::Serialization)?);
                    let response = client.execute(request).await?;
                    let text = String::from_utf8_lossy(&response.body).to_string();
                    if !response.is_success() {
                        return Err(SFError::Validation(format!(
                            "the task door refused this filing (HTTP {}): {}",
                            response.status,
                            text.trim()
                        )));
                    }
                    tracing::info!(
                        caller = filed.filed_by.as_deref().unwrap_or("-"),
                        goal_id = %filed.goal_id,
                        task_id = %filed.task_id,
                        assignee = filed.assignee().unwrap_or("-"),
                        venue = filed.venue.as_deref().unwrap_or("-"),
                        "work item filed on the shared graph"
                    );
                    let mut receipt = serde_json::json!({
                        "filed": true,
                        "task_id": filed.task_id,
                        "goal_id": filed.goal_id,
                        "venue": filed.venue,
                        "assigned_to": filed.assignee(),
                        "filed_by": filed.filed_by,
                        "door_response": serde_json::from_str::<serde_json::Value>(&text)
                            .unwrap_or(serde_json::Value::String(text)),
                    });
                    // The door answers 202: the row is queued and the graph
                    // decomposes or dispatches it from here. Saying so keeps a
                    // caller from reading "filed" as "done".
                    receipt["queued"] = serde_json::json!(true);
                    Ok(receipt)
                })
            })),
        }
    }

    /// Read the shared task ledger: what is on the board, folded into readings.
    ///
    /// `start_task` puts work on the graph; without this, an agent that filed
    /// work has no way to find out what is already queued for it, what came
    /// back, or which of its own rows nobody has picked up. Nothing else on
    /// this face answers that: the operator face's `/api/v1/tasks/list` returns
    /// the whole table in one body -- nine thousand rows and thirty-eight
    /// megabytes on 2026-11-06 -- which reads as noise rather than as a state.
    ///
    /// The answer is a folded reading, not an enumeration: counts by status, by
    /// kind crossed with status, by venue and by owner; the oldest open rows;
    /// and how many of those a worker has already taken. Reading it is how an
    /// agent decides what to do next without guessing, and how it notices that
    /// the row it filed hours ago is still sitting there.
    pub fn read_task_ledger(client: Option<Arc<dyn HttpClient>>, api_base: Option<String>) -> Tool {
        Tool {
            name: "read_task_ledger".into(),
            description: "Read the shared task ledger: how much work is on the board, in what \
                          states, in which venues, and on whose hands. Use it before starting \
                          something new, to find out whether it is already queued or already \
                          done, and after filing work, to find out whether anyone picked it up. \
                          The reading is folded: counts plus the oldest open rows, never the \
                          whole table. Narrow it with any of the filters; leave them out to read \
                          the whole board. `open` rows that were never taken are queue depth, \
                          rows with a `started_at` are work in flight, and `truncated` says the \
                          row list was cut to `limit` rather than being complete."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "venue": {
                        "type": "string",
                        "description": "Only work whose row carries this venue label (memory, \
                                        wiki, eval …), matched exactly."
                    },
                    "owner": {
                        "type": "string",
                        "description": "Only work on this person's hands: rows assigned to them, \
                                        or rows whose result they submitted."
                    },
                    "status": {
                        "type": "string",
                        "description": "Only rows in this state: pending, scheduled, running, \
                                        completed, failed, cancelled."
                    },
                    "kind": {
                        "type": "string",
                        "description": "Only rows of this task kind, as the ledger spells it in \
                                        by_kind_status."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "How many of the oldest open rows to bring back (default \
                                        20, capped at 50 regardless of what is asked for)."
                    }
                }
            }),
            implementation: ToolImplementation::Native(Arc::new(move |args| {
                let client = client.clone();
                let api_base = api_base.clone();
                Box::pin(async move {
                    let base = memory_base(api_base.as_deref())?;
                    let client = memory_client(client)?;
                    // 条件名原样传给服务端，由那一条读自己解释：在这里先把筛选做一遍
                    // 就成了第二个定义，两处迟早对不上。
                    let mut query: Vec<String> = Vec::new();
                    for key in ["venue", "owner", "status", "kind"] {
                        if let Some(value) = optional_label(&args, key) {
                            query.push(format!("{key}={}", encode_url_component(&value)));
                        }
                    }
                    if let Some(limit) = args.get("limit").and_then(serde_json::Value::as_u64) {
                        query.push(format!("limit={limit}"));
                    }
                    let mut url = format!("{base}/api/v1/tasks/ledger");
                    if !query.is_empty() {
                        url.push('?');
                        url.push_str(&query.join("&"));
                    }
                    get_json(&client, &url).await
                })
            })),
        }
    }

    /// What a `start_task` call turns into, decided without an HTTP client.
    ///
    /// Kept apart from the tool body so the shape of a filing is assertable:
    /// one row, its type, the acceptance copied into the input at the key the
    /// door reads, and the venue when the caller named one. These are the
    /// properties that decide whether the row can run at all, and they should
    /// be testable before anything is on a wire.
    struct StartTaskFiling {
        body: serde_json::Value,
        goal_id: String,
        task_id: String,
        venue: Option<String>,
        filed_by: Option<String>,
        assignee: Option<String>,
    }

    impl StartTaskFiling {
        fn assignee(&self) -> Option<&str> {
            self.assignee.as_deref()
        }
    }

    /// Read one optional label off the arguments: trimmed, and a blank one is
    /// the absence it looks like rather than a value made of spaces.
    fn optional_label(args: &serde_json::Value, key: &str) -> Option<String> {
        args.get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }

    fn start_task_filing(
        args: &serde_json::Value,
        caller: Option<&str>,
    ) -> SFResult<StartTaskFiling> {
        let goal = args
            .get("goal")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|goal| !goal.is_empty())
            .ok_or_else(|| {
                SFError::Validation(
                    "start_task needs a `goal`: whoever takes the row reads it as the work".into(),
                )
            })?;
        let acceptance: Vec<String> = args
            .get("acceptance")
            .and_then(|v| v.as_array())
            .map(|lines| {
                lines
                    .iter()
                    .filter_map(|line| line.as_str())
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        // Refused here as well as at the door, and with the reason a submitter
        // can act on: the row is filed as already decided, so no planner ever
        // runs on it to state what done means. The door would refuse it too;
        // catching it here means the caller does not have to read a refusal
        // out of an HTTP body to learn why.
        if acceptance.is_empty() {
            return Err(SFError::Validation(
                "start_task needs at least one `acceptance` line: the row is filed as already \
                 decided, so nothing downstream will state what done means, and the door \
                 refuses a filing without one"
                    .into(),
            ));
        }
        let venue = optional_label(args, "venue");
        let assignee = optional_label(args, "assign_to");
        // A goal id is either the one the caller is continuing or a fresh one.
        // Continuing is how several rows become one goal -- a squad formed
        // around work, not around a role.
        let goal_id = optional_label(args, "goal_id")
            .unwrap_or_else(|| format!("agent-goal-{}", uuid::Uuid::new_v4()));
        // Zero is refused rather than read as "no limit": the over-budget sweep
        // compares the row's age against this number, so a zero would fail the
        // row before it ever ran while the caller read a success.
        let timeout_seconds = match args.get("timeout_seconds") {
            Some(value) => value.as_u64().filter(|secs| *secs > 0).ok_or_else(|| {
                SFError::Validation(
                    "`timeout_seconds` has to be a positive whole number of seconds".into(),
                )
            })?,
            None => DEFAULT_WORK_ITEM_TIMEOUT_SECS,
        };
        // The row has to land in the workspace the platform reads, and the
        // loops run in a process that is not told it by any config section it
        // owns. The deployment sets nothing, which is why the fallback is the
        // same name the memory namespace defaults to rather than an empty
        // string: an empty workspace is a row no listing matches.
        let workspace = std::env::var(cog_core::env::WORKSPACE_ID)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_MEMORY_NAMESPACE.to_string());

        let task_id = format!("work-{}", uuid::Uuid::new_v4());
        let mut input = serde_json::json!({
            "task": goal,
            "acceptance": acceptance,
        });
        if let Some(assignee) = assignee.as_deref() {
            input["agent_id"] = serde_json::json!(assignee);
        }
        let mut row = serde_json::json!({
            "id": task_id,
            "task_type": cog_core::contract::work_item::PLATFORM_AGENT_TASK,
            "input": input,
            "timeout_seconds": timeout_seconds,
        });
        // The caller is stating the work as already decided. Without this the
        // row goes through decomposition, which would spend a planning call to
        // restate what the caller already said. The field is read under the
        // planner role's own name: a second spelling here would be a field the
        // door never looks at, and the row would be planned anyway.
        row[cog_core::roles::PLANNER] = serde_json::json!({
            "verified": true,
            "source": "user_provided",
        });
        if let Some(assignee) = assignee.as_deref() {
            row["agent_id"] = serde_json::json!(assignee);
        }
        if let Some(venue) = venue.as_deref() {
            row["venue"] = serde_json::json!(venue);
        }
        let body = serde_json::json!({
            "goal_id": goal_id.clone(),
            "goal": goal,
            "workspace_id": workspace,
            "intent_kind": "task",
            "tasks": [row],
        });
        Ok(StartTaskFiling {
            body,
            goal_id,
            task_id,
            venue,
            filed_by: caller.map(str::to_string),
            assignee,
        })
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
                            labels.insert(crate::LABEL_TOOL.to_string(), "raw_fetch".to_string());
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

    #[cfg(test)]
    mod tests {
        use super::*;

        fn filing(args: serde_json::Value) -> SFResult<StartTaskFiling> {
            start_task_filing(&args, Some("agent-caller"))
        }

        /// The shape the door reads: one row, filed as already decided, with the
        /// acceptance at the key the handler copies from and the venue on the
        /// row. Each of these is what keeps the row from being planned again,
        /// from being held for want of a criterion, or from landing outside the
        /// board column its submitter named.
        #[test]
        fn a_filing_carries_one_verified_row_with_its_acceptance_and_venue() {
            let filed = filing(serde_json::json!({
                "goal": "  read the last week of memory writes and rate their importance  ",
                "acceptance": ["every item carries a 1-10 rating", "  ", "the reply is one JSON object"],
                "venue": "  memory  ",
                "assign_to": "agent-abc",
            }))
            .expect("a complete filing is accepted");
            let body = &filed.body;
            assert_eq!(body["intent_kind"], "task");
            assert_eq!(
                body["tasks"].as_array().map(Vec::len),
                Some(1),
                "one goal gets one row; more rows would need the goal decomposition this \
                 submission exists to skip"
            );
            let row = &body["tasks"][0];
            assert_eq!(
                row["task_type"],
                cog_core::contract::work_item::PLATFORM_AGENT_TASK
            );
            assert_eq!(
                row["input"]["acceptance"],
                serde_json::json!([
                    "every item carries a 1-10 rating",
                    "the reply is one JSON object"
                ]),
                "blank lines are dropped, and what remains is what the door copies onto the row"
            );
            assert_eq!(row["venue"], "memory", "the venue travels trimmed");
            assert_eq!(row["agent_id"], "agent-abc");
            assert_eq!(row["input"]["agent_id"], "agent-abc");
            assert_eq!(row["planner"]["verified"], true);
            assert_eq!(row["timeout_seconds"], DEFAULT_WORK_ITEM_TIMEOUT_SECS);
            assert_eq!(body["goal_id"], filed.goal_id.as_str());
            assert!(
                body["workspace_id"].as_str().is_some_and(|w| !w.is_empty()),
                "a row filed into an empty workspace is a row no listing matches"
            );
        }

        /// The refusal happens before the write, and both the absent and the
        /// blank-only cases are refusals: a criterion made of spaces states
        /// nothing, and the door would refuse it after the round trip.
        #[test]
        fn a_filing_without_acceptance_is_refused_before_anything_is_sent() {
            for acceptance in [serde_json::json!(null), serde_json::json!(["  ", ""])] {
                let err = filing(serde_json::json!({
                    "goal": "do the thing",
                    "acceptance": acceptance,
                }))
                .err()
                .expect("a filing that states no criterion is refused");
                assert!(
                    matches!(err, SFError::Validation(_)),
                    "the caller has to be told this is its input, not a transport failure: {err:?}"
                );
            }
        }

        /// Continuing a goal is how several rows become one goal -- a squad
        /// formed around the work rather than around a role.
        #[test]
        fn a_filing_continues_the_goal_it_is_given() {
            let filed = filing(serde_json::json!({
                "goal": "second slice",
                "acceptance": ["the second slice is on the board under the first"],
                "goal_id": "  7c1f0d  ",
            }))
            .expect("a continuation is accepted");
            assert_eq!(filed.goal_id, "7c1f0d");
            assert_eq!(filed.body["goal_id"], "7c1f0d");
        }

        /// Zero would fail the row before it ran, so it is refused where the
        /// caller can still fix it rather than queued as a success.
        #[test]
        fn a_zero_budget_is_refused_rather_than_read_as_no_limit() {
            let err = filing(serde_json::json!({
                "goal": "do the thing",
                "acceptance": ["the thing is done"],
                "timeout_seconds": 0,
            }))
            .err()
            .expect("a zero budget is refused");
            assert!(matches!(err, SFError::Validation(_)));
        }
    }
}
