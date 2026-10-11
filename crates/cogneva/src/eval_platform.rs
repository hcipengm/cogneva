//! 评测台与平台之间那一段：端口在评测台那一侧声明，实现在这里落地。
//!
//! 只有组合根同时看得见平台类型（任务、执行器、事件流）与评测台类型（`AgentOutput`、
//! `Budget`），所以「投一题进平台、等它跑完、把带 trace 的答案取回来」这条链路只能
//! 写在这里。评测台不认识平台，平台也不认识评测台。
//!
//! 这个文件里同时住着**缺席**的表示：没有配置骨干上游时的 [`NoBackbone`]、没有接上
//! 平台时的 [`NoPlatform`]。它们不是替身——替身会交回一份看着像已接通的假结果，而这两
//! 个一律报错。少了它们，缺席会被读成「这一格答错了」，而「上游没配」与「答错」在表里
//! 都得是 0 分却完全是两件事。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::observability::TaskMetrics;
use cog_core::{
    AgentEvent, AgentTrace, AssistantMessageEventStream, ChatOptions, ChatResponse,
    CompleteOptions, HttpClient, HttpRequest, LlmClient, Message, SFError, SFResult, Usage,
};
use cog_eval::metric::StepRecord;
use cog_eval::scaffold::{AgentOutput, FinishReason};
use cog_eval::scaffolds::nql::PlatformReply;
use cog_eval::scaffolds::{PlatformRequest, PlatformRunner};

/// 没有骨干上游时挂在 `LlmClient` 位置上的东西：每次调用都是一条明确的「没配上」。
///
/// 它存在的理由是评测台需要一个 `LlmClient` 才能建起来，而「没有上游」必须是**报错**
/// 而不是一个空答案——空答案会走进判分器，变成一格 0 分。
pub struct NoBackbone {
    reason: String,
}

impl NoBackbone {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    fn refuse(&self) -> SFError {
        SFError::Config(format!("no backbone upstream is wired: {}", self.reason))
    }
}

#[async_trait]
impl LlmClient for NoBackbone {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(self.refuse())
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(self.refuse())
    }

    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Err(self.refuse())
    }

    async fn health_check(&self) -> bool {
        false
    }
}

/// 没有接上平台时的 [`PlatformRunner`]：投一题就报一次错，绝不交回一份假答案。
///
/// 表里 NQL 那一行若拿到的是替身返回的 `AgentOutput`，会显示成「跑通了」而其实是别的东西
/// 在答——这正是最需要防的形状。缺平台只能表现为这一行跑不起来。
pub struct NoPlatform {
    reason: String,
}

impl NoPlatform {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[async_trait]
impl PlatformRunner for NoPlatform {
    async fn run(&self, _request: PlatformRequest) -> anyhow::Result<PlatformReply> {
        anyhow::bail!("no platform endpoint is wired: {}", self.reason)
    }
}

// ─── 平台桥：把一题投进跑着的平台 ─────────────────────────────────────
//
// 这一段的形状是平台的实况给的，不是设计出来的：
//
// - 平台没有「收一题、当场交答案」的同步入口。运营面的任务路由是**异步受理**：
//   提交只回 202 与一个 goal id，任务被喂进 DAG 后由执行器跑。所以这里必须轮询到
//   终态，而不是等一个响应体。
// - 平台的 step 面按 task_id 是取不出来的：事件过滤对 `task_id` 只认
//   `TaskStatusChange`，其余变体一律不匹配。每一步的原文在**执行轨迹**
//   （`AgentTrace`，带 task_id 与完整事件数组）里，所以步数从轨迹取。
// - 计数也是另一套：平台按任务记账（`TaskMetrics`），评测台只认
//   `cog_core::Usage`。
//
// 三处对应关系都写在这个文件里，别处不再有一份。
//
// 还有一条前提，它不在这个文件里、也不由这个文件决定：**平台必须把投进去的那道题当一道
// 题跑，并且说清楚答案落在哪一行上**。平台受理的是「意图」，默认会让规划器把意图拆成一堆
// 子任务，投进去的那个 id 变成一个不可执行的父占位符。所以平台侧有两条约定，缺一不可：
//
//   1. 占位行上带一条声明（`sink_task_id`），指出这道目标的答案落在分解出的哪一条上；
//   2. 那条走到终态时，占位行的终态跟着它走——否则这里轮询的那一行永远停在 Pending。
//
// 分解若没有给出恰好一条收口任务，平台整份判退（占位行落 Failed 并带上原因），这里就把
// 平台的失败原因报出去：不挑一条顶上，也不把某一片的中间产物算成这一格的答案。对上一个
// 既不声明也不推导的平台，这里同样不猜——轮询到时限，报出平台自己的状态。

/// 投给平台的一题在平台侧叫什么种类。
///
/// 未知种类在平台里就是普通原子任务，走它自己的 DAG 与执行器——这一行量的是「题目
/// 进平台之后平台跑成什么样」，所以外壳不替平台挑执行路径，只把题交出去。
const PLATFORM_TASK_KIND: &str = "benchmark_case";
/// 平台 API 的基址，例如 `http://127.0.0.1:8080`。未设置＝这个部署没有可投递的平台。
pub const PLATFORM_BASE_ENV: &str = "COG_EVAL_PLATFORM_BASE";
/// 平台 API 的 bearer token。平台的运营路由按角色判，多数部署要带一枚；未设置时
/// 按匿名发，收不收由服务侧的鉴权层决定。
pub const PLATFORM_TOKEN_ENV: &str = "COG_EVAL_PLATFORM_TOKEN";
/// 一题在平台侧最多等多久（秒）。
pub const PLATFORM_DEADLINE_ENV: &str = "COG_EVAL_PLATFORM_DEADLINE_SECS";
const DEFAULT_DEADLINE_SECS: u64 = 3600;
const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// 轨迹列表一次取多少条。按最近的排，刚跑完的那条在最前面。
const TRACE_LIST_LIMIT: usize = 200;

/// 桥的接入点。三样东西各有独立的判据：没有基址就是没有平台；令牌与时限有默认值。
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeSettings {
    pub base: String,
    pub token: Option<String>,
    pub deadline_secs: u64,
}

impl BridgeSettings {
    /// 从三个输入值算接入点。纯函数，测试不用去改进程环境。
    pub fn from_parts(
        base: Option<&str>,
        token: Option<&str>,
        deadline: Option<&str>,
    ) -> Option<Self> {
        let base = base?.trim().trim_end_matches('/');
        if base.is_empty() {
            return None;
        }
        Some(Self {
            base: base.to_string(),
            token: token
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string),
            deadline_secs: deadline
                .and_then(|d| d.trim().parse().ok())
                .unwrap_or(DEFAULT_DEADLINE_SECS),
        })
    }

    pub fn from_env() -> Option<Self> {
        Self::from_parts(
            std::env::var(PLATFORM_BASE_ENV).ok().as_deref(),
            std::env::var(PLATFORM_TOKEN_ENV).ok().as_deref(),
            std::env::var(PLATFORM_DEADLINE_ENV).ok().as_deref(),
        )
    }
}

/// 平台的任务视图里这一行要看的那几个字段。
///
/// 平台的 `TaskView` 只实现 `Serialize`（它是给客户端看的 DTO），收的一侧便要有自己的
/// 形状；两者的一致性由测试钉住——测试把**平台自己的** `TaskView` 序列化出来喂给这里，
/// 平台改了字段名，测试就红。
#[derive(Debug, serde::Deserialize)]
struct TaskFacts {
    status: String,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<String>,
    /// 平台在占位行上写的声明：这道目标的答案落在哪一条上。平台没拆解投递时缺席，
    /// 那时自报的那一行就是干活的那一行。
    #[serde(default)]
    sink_task_id: Option<String>,
}

/// 平台侧跑完一题之后留下的东西，投影成评测台的 [`AgentOutput`]。
///
/// 投影不是搬运：平台的 step 是 ReAct 循环，评测台的 step 是一次动作；平台的账是
/// `TaskMetrics`，评测台的计数是 `Usage`。两边怎么对应写死在这一个函数里。
fn project(
    case_id: &str,
    facts: &TaskFacts,
    trace: &AgentTrace,
    metrics: &TaskMetrics,
) -> anyhow::Result<AgentOutput> {
    // 答案就是平台执行器交回的那份输出（平台存任务结果时存的是 `TaskResult.output`，
    // 成败则落在任务的终态上）。它是什么形状由平台定，这里不猜字段名：是字符串就用
    // 原文，是结构就原样序列化——猜错字段名会静默交出一个空答案。
    let result = facts.result.as_ref().ok_or_else(|| {
        anyhow::anyhow!("the platform completed task {case_id} and stored no result at all")
    })?;
    let final_answer = match result {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    let steps = project_steps(&trace.events);
    if steps.is_empty() {
        anyhow::bail!(
            "the platform's trace for task {case_id} holds no step ({} events, none of them a \
             step): a row scored from this would read as the model failing the task rather than \
             as a task that never ran",
            trace.events.len()
        );
    }

    Ok(AgentOutput {
        final_answer,
        trace: steps,
        tokens: Usage {
            input: u32::try_from(metrics.prompt_tokens).unwrap_or(u32::MAX),
            output: u32::try_from(metrics.completion_tokens).unwrap_or(u32::MAX),
            total_tokens: u32::try_from(metrics.total_tokens).unwrap_or(u32::MAX),
            ..Default::default()
        },
        // 平台不通过这条路由报「为什么停下」。撞预算与答完在平台侧都是 Completed，
        // 所以这里只能报 Answered；真撞了预算的那种跑，平台会以失败终态出现，
        // 而那一路是报错，不是低分。
        finish: FinishReason::Answered,
    })
}

/// 把平台的事件流收成步序列。
///
/// 平台对一题自报的步是 ReAct 循环（一次 think→act→observe），所以有它就用它：
/// 工具事件是这些步的零件，两样都收会把同一次动作数两遍。没有 ReAct 步的跑（例如
/// 直接答一句）退到工具事件，再退到消息事件。
///
/// 步里的 `duration_ms` 一律留 0：它只装墙上时钟，而这一份东西会随表一起被比对。
fn project_steps(events: &[AgentEvent]) -> Vec<StepRecord> {
    let react: Vec<StepRecord> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ReActStepEnd {
                iteration,
                thought,
                tool_calls,
                observations,
                ..
            } => Some(StepRecord {
                step_index: *iteration as usize,
                action_type: "react_step".into(),
                action_params: serde_json::json!({ "observations": observations.len() }),
                thought: Some(thought.clone()),
                duration_ms: 0,
                success: true,
                tool_calls: tool_calls.iter().map(|c| c.name.clone()).collect(),
            }),
            _ => None,
        })
        .collect();
    if !react.is_empty() {
        return react;
    }

    // 工具的名字与成败落在两个事件上：`ToolExecutionStart` 带名字，`ToolExecutionEnd`
    // 带成败，两边只共用 `tool_call_id`。按 id 配起来读——只看其中一半会得到要么没有
    // 名字、要么没有成败的步。
    let mut names: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for event in events {
        if let AgentEvent::ToolExecutionStart {
            tool_call_id,
            tool_name,
            ..
        } = event
        {
            names.insert(tool_call_id.as_str(), tool_name.as_str());
        }
    }
    let tools: Vec<StepRecord> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                is_error,
                ..
            } => {
                // 没有配对的 start（轨迹截断、事件来自别的生产者）时用调用 id 顶上：
                // 名字缺失是可惜，凭空造一个名字是错。
                let name = names
                    .get(tool_call_id.as_str())
                    .copied()
                    .unwrap_or(tool_call_id.as_str());
                Some(StepRecord {
                    step_index: 0,
                    action_type: name.to_string(),
                    action_params: serde_json::Value::Null,
                    thought: None,
                    duration_ms: 0,
                    success: !is_error,
                    tool_calls: vec![name.to_string()],
                })
            }
            _ => None,
        })
        .collect();
    if !tools.is_empty() {
        return tools
            .into_iter()
            .enumerate()
            .map(|(i, mut step)| {
                step.step_index = i;
                step
            })
            .collect();
    }

    events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::MessageEnd { .. } => Some(StepRecord {
                step_index: 0,
                action_type: "message".into(),
                action_params: serde_json::Value::Null,
                thought: None,
                duration_ms: 0,
                success: true,
                tool_calls: vec![],
            }),
            _ => None,
        })
        .enumerate()
        .map(|(i, mut step)| {
            step.step_index = i;
            step
        })
        .collect()
}

/// 组合根那一侧的真平台：投一题进跑着的平台，把它的产物取回来。
///
/// 它不做任何「像已接通」的事——平台的每一种缺席（没基址、不答、票据不对、跑不完、
/// 没留轨迹、留了失败）都从这里变成 `Err`，由 NQL 那一行记成「跑不起来」。
pub struct PlatformApiRunner {
    http: Arc<dyn HttpClient>,
    settings: BridgeSettings,
}

impl PlatformApiRunner {
    pub fn new(http: Arc<dyn HttpClient>, settings: BridgeSettings) -> Self {
        Self { http, settings }
    }

    /// 从环境读接入点。没有基址返回 `None`——调用方据此挂上 [`NoPlatform`]。
    pub fn from_env(http: Arc<dyn HttpClient>) -> Option<Self> {
        BridgeSettings::from_env().map(|settings| Self::new(http, settings))
    }

    /// 这次跑投给了哪个平台。表里要写出来：一份不写平台的表，别人复现不到同一套。
    pub fn base(&self) -> &str {
        &self.settings.base
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.settings.base, path);
        let mut request = HttpRequest::new(method, &url);
        if let Some(token) = &self.settings.token {
            request
                .headers
                .insert("authorization".into(), format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request
                .headers
                .insert("content-type".into(), "application/json".into());
            request.body = Some(serde_json::to_vec(&body)?);
        }
        let response = self.http.execute(request).await.map_err(|e| {
            anyhow::anyhow!(
                "the platform at {} did not answer {method} {path}: {e}",
                self.settings.base
            )
        })?;
        if !(200..300).contains(&response.status) {
            let detail = String::from_utf8_lossy(&response.body).trim().to_string();
            anyhow::bail!(
                "the platform answered {} for {method} {path}: {detail}",
                response.status
            );
        }
        if response.body.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_slice(&response.body).map_err(|e| {
            anyhow::anyhow!("the platform's answer to {method} {path} is not JSON: {e}")
        })
    }

    /// 投一题。题面、种子与预算一起放进任务的 `input`：种子必须真的到达平台，
    /// 否则同一格的三个种子在平台侧是同一道题，那三个数就是一份拷贝。
    ///
    /// `input.goal` 是题面本身而不是一句说明：平台执行一条原子任务时，取的是任务
    /// `input` 里的 `goal` 作为要达成的东西，顶层那个 `goal` 只在平台决定怎么拆解时
    /// 被读。写成说明句，平台拿到的是「去解 eval-table 那个 case」，不是那道题。
    async fn submit(&self, case_id: &str, request: &PlatformRequest) -> anyhow::Result<()> {
        let body = serde_json::json!({
            "goal": request.task,
            "workspace_id": "",
            "tasks": [{
                "id": case_id,
                "task_type": PLATFORM_TASK_KIND,
                "input": {
                    "goal": request.task,
                    "seed": request.seed,
                    "max_steps": request.budget.max_steps,
                    "max_context_tokens": request.budget.max_context_tokens,
                    "temperature": request.budget.temperature,
                },
                "blocked_by": [],
                "priority": 0,
            }],
        });
        self.call("POST", "/api/v1/tasks", Some(body)).await?;
        Ok(())
    }

    /// 取一条任务的视图。
    async fn fetch_facts(&self, task_id: &str) -> anyhow::Result<TaskFacts> {
        let raw = self
            .call("GET", &format!("/api/v1/tasks/{task_id}"), None)
            .await?;
        serde_json::from_value(raw)
            .map_err(|e| anyhow::anyhow!("the platform's task view changed shape: {e}"))
    }

    /// 轮询到自己投的那道题走到终态。平台上别的题走得快走得慢都不影响这里。
    ///
    /// 轮询的是**投进去的那一行**：平台把这道题拆了的话，它会是一条不可执行的占位行，
    /// 终态由它点出的那条任务带过来（见本段开头的两条约定）。返回时占位行上带着那条
    /// 声明，调用方据此去读真正干活的那一行。
    async fn await_terminal(&self, case_id: &str) -> anyhow::Result<TaskFacts> {
        let deadline = Duration::from_secs(self.settings.deadline_secs);
        let started = std::time::Instant::now();
        loop {
            let facts = self.fetch_facts(case_id).await?;
            match facts.status.as_str() {
                "Completed" | "Failed" | "Cancelled" => return Ok(facts),
                _ => {}
            }
            if started.elapsed() >= deadline {
                anyhow::bail!(
                    "the platform left task {case_id} in {} for the whole {}s budget: a case that \
                     cannot finish must not be scored as an answer",
                    facts.status,
                    self.settings.deadline_secs
                );
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// 取这道题的执行轨迹。列表按最近排，刚跑完的那条在最前面。
    async fn fetch_trace(&self, case_id: &str) -> anyhow::Result<AgentTrace> {
        let list = self
            .call(
                "GET",
                &format!("/api/v1/traces?limit={TRACE_LIST_LIMIT}"),
                None,
            )
            .await?;
        let trace_id = list
            .get("traces")
            .and_then(serde_json::Value::as_array)
            .and_then(|traces| {
                traces.iter().find_map(|t| {
                    (t.get("task_id").and_then(serde_json::Value::as_str) == Some(case_id))
                        .then(|| t.get("trace_id").and_then(serde_json::Value::as_str))
                        .flatten()
                })
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the platform kept no execution trace for task {case_id}: a row without its \
                     process cannot be checked against anything"
                )
            })?;
        let raw = self
            .call("GET", &format!("/api/v1/traces/{trace_id}"), None)
            .await?;
        serde_json::from_value(raw)
            .map_err(|e| anyhow::anyhow!("the platform's trace changed shape: {e}"))
    }

    async fn fetch_metrics(&self, case_id: &str) -> anyhow::Result<TaskMetrics> {
        let raw = self
            .call("GET", &format!("/api/v1/tasks/{case_id}/metrics"), None)
            .await?;
        serde_json::from_value(raw)
            .map_err(|e| anyhow::anyhow!("the platform's per-task accounting changed shape: {e}"))
    }
}

#[async_trait]
impl PlatformRunner for PlatformApiRunner {
    async fn run(&self, request: PlatformRequest) -> anyhow::Result<PlatformReply> {
        // 同一道题＋同一颗种子＝平台侧同一个任务。三次种子是不同的任务，因此不会
        // 命中同一份缓存；同一次跑重来会命中平台自己的「同 id 幂等跳过」，那是平台
        // 对「同一个任务」的定义，不是缓存穿透。
        let case_id = format!("eval-{}-{}", request.seed, digest(&request.task));
        self.submit(&case_id, &request).await?;
        let facts = self.await_terminal(&case_id).await?;
        // 答案行 = 那道题被拆解时占位行点出的那条；没被拆解时两者是同一个 id。判成与计
        // 都读它**自己**那一份事实，不读占位行上抄来的那份：抄写漏掉一个字段时，读抄件
        // 会把「没存结果」报成平台的错，而真相是那一步没做完。
        let answer_row = facts
            .sink_task_id
            .clone()
            .unwrap_or_else(|| case_id.clone());
        let answer = if answer_row == case_id {
            facts
        } else {
            self.fetch_facts(&answer_row).await?
        };
        match answer.status.as_str() {
            "Completed" => {}
            other => anyhow::bail!(
                "the platform ended task {answer_row} as {other}: {}",
                answer.error.as_deref().unwrap_or("it recorded no reason")
            ),
        }
        let trace = self.fetch_trace(&answer_row).await?;
        let metrics = self.fetch_metrics(&answer_row).await?;
        let output = project(&answer_row, &answer, &trace, &metrics)?;
        // 报出去的 id 是干活的那一行的：表里某一行对不上时要能回去看那一次跑，而轨迹与
        // 计数都挂在那一行上。投进去的那一个 id 是题面与种子的纯函数，随时能重算。
        Ok(PlatformReply {
            task_id: answer_row,
            output,
        })
    }
}

/// 题面的短指纹，用来拼任务 id。它只是让同一道题认得出是同一道题，不是安全边界。
fn digest(text: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(text.as_bytes()))[..12].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_eval::scaffold::Budget;

    #[tokio::test]
    async fn the_missing_backbone_refuses_every_call() {
        let llm = NoBackbone::new("no routing config");
        let err = llm
            .chat(&[], &ChatOptions::default())
            .await
            .expect_err("a missing backbone must not answer");
        assert!(err.to_string().contains("no routing config"), "{err}");
        assert!(!llm.health_check().await);
    }

    #[tokio::test]
    async fn the_missing_platform_refuses_the_task_instead_of_answering_it() {
        let platform = NoPlatform::new("nothing implemented the port");
        let err = platform
            .run(PlatformRequest {
                task: "a question".into(),
                seed: 0,
                budget: Budget::default(),
            })
            .await
            .expect_err("a missing platform must not answer");
        assert!(
            err.to_string().contains("nothing implemented the port"),
            "{err}"
        );
    }

    // ─── 平台桥 ──────────────────────────────────────────────────────────
    //
    // 下面这个假平台不是「返回一份写死的 JSON」的替身：响应体一律由**平台自己的类型**
    // 序列化而来（`TaskView` 走它自己的 `From<Task>`、`AgentTrace`、`TaskMetrics`），
    // 收的一侧（这一侧）若与平台对不上，测试就红。列表那条路由是唯一手搭的，因为平台
    // 的处理函数也是手搭的 json；它的键按那个处理函数里的名字写，改了就一起改。

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;

    use axum::extract::{Path as AxumPath, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get as axum_get, post as axum_post};
    use axum::{Json, Router};

    #[derive(Clone)]
    struct FakePlatform {
        view: serde_json::Value,
        /// 投进去那一行之外的那条任务的视图。平台把这道题拆了时，答案落在这一行上：
        /// 占位行上抄来的那份可能不全（这里就故意让它不全），权威的是这一行。
        answer_view: Option<serde_json::Value>,
        trace_ids: Vec<String>,
        /// 轨迹列表里挂的那条 task_id。缺省是提交时的那一个；平台把题拆了时是干活
        /// 那一行的。
        trace_task_id: Option<String>,
        trace: cog_core::AgentTrace,
        metrics: cog_core::TaskMetrics,
        submitted: Arc<StdMutex<Vec<serde_json::Value>>>,
        ticks: Arc<AtomicU64>,
    }

    impl FakePlatform {
        fn new(view: serde_json::Value, trace: cog_core::AgentTrace) -> Self {
            let task_id = trace.task_id.clone();
            Self {
                view,
                answer_view: None,
                trace_ids: vec![trace.trace_id.clone()],
                trace_task_id: None,
                trace,
                metrics: cog_core::TaskMetrics {
                    task_id,
                    total_tokens: 30,
                    prompt_tokens: 20,
                    completion_tokens: 10,
                    tool_calls: 1,
                    iterations: 2,
                    duration_ms: 0,
                    timestamp: chrono::Utc::now(),
                },
                submitted: Arc::new(StdMutex::new(Vec::new())),
                ticks: Arc::new(AtomicU64::new(0)),
            }
        }

        /// 每次应答都推进一格计数器与时间戳：平台侧的墙钟若被这一侧偷偷带进
        /// `AgentOutput`，两次同样的跑就不再逐字节相同。
        fn tick(&self) -> u64 {
            self.ticks.fetch_add(1, Ordering::SeqCst) + 1
        }
    }

    async fn handle_submit(State(f): State<FakePlatform>, body: String) -> impl IntoResponse {
        f.submitted
            .lock()
            .unwrap()
            .push(serde_json::from_str(&body).unwrap_or(serde_json::Value::Null));
        f.tick();
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "goal": "", "task_count": 1, "task_ids": [], "message_id": "goal-1",
            })),
        )
    }

    async fn handle_task(
        State(f): State<FakePlatform>,
        AxumPath(id): AxumPath<String>,
    ) -> impl IntoResponse {
        f.tick();
        // 投进去的那一行与它点出的那一行各答各的：平台真把题拆了时，两条是不同的行。
        let submitted_id = f.submitted.lock().unwrap().first().and_then(|b| {
            b.pointer("/tasks/0/id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });
        match (&f.answer_view, submitted_id) {
            (Some(answer), Some(submitted)) if id != submitted => Json(answer.clone()),
            _ => Json(f.view.clone()),
        }
    }

    async fn handle_metrics(
        State(f): State<FakePlatform>,
        AxumPath(_id): AxumPath<String>,
    ) -> impl IntoResponse {
        let mut metrics = f.metrics.clone();
        metrics.duration_ms = f.tick();
        metrics.timestamp = chrono::Utc::now();
        Json(serde_json::to_value(metrics).unwrap())
    }

    async fn handle_trace_list(State(f): State<FakePlatform>) -> impl IntoResponse {
        let task_id = f
            .submitted
            .lock()
            .unwrap()
            .first()
            .and_then(|b| {
                b.pointer("/tasks/0/id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let task_id = f.trace_task_id.clone().unwrap_or(task_id);
        let items: Vec<serde_json::Value> = f
            .trace_ids
            .iter()
            .map(|trace_id| {
                serde_json::json!({
                    "trace_id": trace_id,
                    "agent_id": "agent-1",
                    "task_id": task_id,
                    "event_count": 2,
                    "created_at": chrono::Utc::now().to_rfc3339(),
                    "tier": "hot",
                })
            })
            .collect();
        f.tick();
        Json(serde_json::json!({ "traces": items }))
    }

    async fn handle_trace(
        State(f): State<FakePlatform>,
        AxumPath(_id): AxumPath<String>,
    ) -> impl IntoResponse {
        let mut trace = f.trace.clone();
        trace.created_at = chrono::Utc::now();
        f.tick();
        Json(serde_json::to_value(trace).unwrap())
    }

    /// 起一个只在本进程里听着的平台，跑完随测试结束。
    struct Served {
        base: String,
        http: Arc<dyn HttpClient>,
        handle: tokio::task::JoinHandle<()>,
    }

    impl Served {
        async fn start(fake: FakePlatform) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = Router::new()
                .route("/api/v1/tasks", axum_post(handle_submit))
                .route("/api/v1/tasks/{id}", axum_get(handle_task))
                .route("/api/v1/tasks/{id}/metrics", axum_get(handle_metrics))
                .route("/api/v1/traces", axum_get(handle_trace_list))
                .route("/api/v1/traces/{id}", axum_get(handle_trace))
                .with_state(fake);
            let handle = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self {
                base: format!("http://{addr}"),
                http: Arc::new(cog_net::factory::ReqwestHttpClient::new(
                    reqwest::Client::new(),
                )),
                handle,
            }
        }

        fn runner(&self, deadline_secs: u64) -> PlatformApiRunner {
            PlatformApiRunner::new(
                self.http.clone(),
                BridgeSettings {
                    base: self.base.clone(),
                    token: None,
                    deadline_secs,
                },
            )
        }
    }

    impl Drop for Served {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    fn task_of(id: &str, kind: &str) -> cog_core::Task {
        cog_core::Task::new(
            id,
            cog_core::TaskType::Custom(kind.into()),
            serde_json::json!({}),
        )
    }

    fn view_of(task: cog_core::Task) -> serde_json::Value {
        serde_json::to_value(cog_gateway::tasks::TaskView::from(task)).unwrap()
    }

    fn task_view(
        id: &str,
        status: cog_core::TaskStatus,
        result: Option<serde_json::Value>,
        error: Option<String>,
    ) -> serde_json::Value {
        let mut task = task_of(id, PLATFORM_TASK_KIND);
        task.status = status;
        task.result = result;
        task.error = error;
        view_of(task)
    }

    fn trace_for(id: &str) -> cog_core::AgentTrace {
        let step =
            |iteration: u32, thought: &str, tools: &[&str]| cog_core::AgentEvent::ReActStepEnd {
                agent_id: "agent-1".into(),
                iteration,
                thought: thought.into(),
                tool_calls: tools
                    .iter()
                    .map(|name| cog_core::ToolCall {
                        id: format!("call-{name}"),
                        name: (*name).into(),
                        arguments: serde_json::Value::Null,
                    })
                    .collect(),
                observations: vec![serde_json::json!({ "ok": true })],
                timestamp: chrono::Utc::now(),
            };
        cog_core::AgentTrace {
            trace_id: format!("trace-{id}"),
            session_id: None,
            task_id: id.into(),
            agent_id: "agent-1".into(),
            created_at: chrono::Utc::now(),
            event_count: 2,
            byte_size: 0,
            version: "1".into(),
            tier: cog_core::StorageTier::Hot,
            compression: 0,
            checksum: String::new(),
            events: vec![
                step(0, "look the question up", &["web_search"]),
                step(1, "answer it", &[]),
            ],
            llm_requests: vec![],
            llm_responses: vec![],
            tool_calls: vec![],
        }
    }

    fn request(task: &str, seed: u64) -> PlatformRequest {
        PlatformRequest {
            task: task.into(),
            seed,
            budget: Budget {
                max_steps: 7,
                max_context_tokens: 2048,
                temperature: 0.5,
            },
        }
    }

    /// 平台的接入点解析是纯函数：没有基址就是没有平台，其余各有默认。
    #[test]
    fn the_bridge_endpoint_parses_without_touching_the_environment() {
        assert_eq!(BridgeSettings::from_parts(None, None, None), None);
        assert_eq!(BridgeSettings::from_parts(Some("   "), None, None), None);
        let settings = BridgeSettings::from_parts(
            Some("http://platform:8080/"),
            Some("  "),
            Some("not-a-number"),
        )
        .expect("a base is a platform");
        assert_eq!(settings.base, "http://platform:8080");
        assert_eq!(settings.token, None);
        assert_eq!(settings.deadline_secs, DEFAULT_DEADLINE_SECS);
        let settings =
            BridgeSettings::from_parts(Some("http://p"), Some(" t "), Some("30")).unwrap();
        assert_eq!(settings.token.as_deref(), Some("t"));
        assert_eq!(settings.deadline_secs, 30);
    }

    #[tokio::test]
    async fn a_case_goes_in_as_a_task_and_comes_back_as_an_agent_output() {
        // 先按 id 探一次，才能把响应体造成「这一次提交」的样子。
        let id_for_view = format!("eval-{}-{}", 3, digest("2 + 2?"));
        let fake = FakePlatform::new(
            task_view(
                &id_for_view,
                cog_core::TaskStatus::Completed,
                Some(serde_json::json!("4")),
                None,
            ),
            trace_for(&id_for_view),
        );
        let served = Served::start(fake.clone()).await;
        let reply = served
            .runner(60)
            .run(request("2 + 2?", 3))
            .await
            .expect("a completing platform answers");

        assert_eq!(reply.task_id, id_for_view);
        assert_eq!(reply.output.final_answer, "4");
        assert_eq!(reply.output.finish, FinishReason::Answered);
        assert_eq!(reply.output.trace.len(), 2);
        assert_eq!(reply.output.trace[0].tool_calls, vec!["web_search"]);
        assert!(reply.output.trace[0]
            .thought
            .as_deref()
            .is_some_and(|t| t.contains("look the question up")));
        // 平台的账是 `TaskMetrics`，评测台认的是 `Usage`：这一格两边要对得上。
        assert_eq!(reply.output.tokens.input, 20);
        assert_eq!(reply.output.tokens.output, 10);
        assert_eq!(reply.output.tokens.total_tokens, 30);
        // 平台的墙钟不许进表：步里的时长一律为 0。
        assert!(reply.output.trace.iter().all(|s| s.duration_ms == 0));

        let submitted = fake.submitted.lock().unwrap().clone();
        let sent = submitted.first().expect("the case was submitted").clone();
        assert_eq!(sent["tasks"][0]["id"], id_for_view);
        assert_eq!(sent["tasks"][0]["task_type"], PLATFORM_TASK_KIND);
        // 题面要落在 `input.goal` 上，也落在顶层 `goal` 上：平台执行一条原子任务读
        // 前者，决定怎么拆解读后者。种子与预算必须一起到。
        assert_eq!(sent["tasks"][0]["input"]["goal"], "2 + 2?");
        assert_eq!(sent["goal"], "2 + 2?");
        assert_eq!(sent["tasks"][0]["input"]["seed"], 3);
        assert_eq!(sent["tasks"][0]["input"]["max_steps"], 7);
        assert_eq!(sent["tasks"][0]["input"]["max_context_tokens"], 2048);
    }

    /// 平台把投进去的题拆了：自报终态的那一行是不可执行的占位，答案落在它点出的那条上。
    /// 这道桥必须跟着那条声明走——判成、轨迹、计数都读干活的那一行。
    ///
    /// 占位行上抄来的那份结果在这里**故意是空的**（平台只抄了终态没抄结果），所以这条
    /// 测试只有在桥读那条被声明出来的行时才会绿：读抄件会得到「complete 了却没存结果」。
    #[tokio::test]
    async fn a_decomposed_case_is_read_through_the_declaration_the_platform_wrote() {
        let case_id = format!("eval-{}-{}", 4, digest("a question the platform splits"));
        let answer_id = format!("{case_id}-answer");

        let mut goal_row = task_of(&case_id, PLATFORM_TASK_KIND);
        goal_row.is_executable = false;
        goal_row.status = cog_core::TaskStatus::Completed;
        goal_row.sink_task_id = Some(answer_id.clone());

        let mut answer_row = task_of(&answer_id, PLATFORM_TASK_KIND);
        answer_row.parent_task_id = Some(case_id.clone());
        answer_row.status = cog_core::TaskStatus::Completed;
        answer_row.result = Some(serde_json::json!("42"));

        let mut fake = FakePlatform::new(view_of(goal_row), trace_for(&answer_id));
        fake.answer_view = Some(view_of(answer_row));
        fake.trace_task_id = Some(answer_id.clone());
        let served = Served::start(fake.clone()).await;
        let reply = served
            .runner(60)
            .run(request("a question the platform splits", 4))
            .await
            .expect("a decomposed case still answers through the row it declares");

        assert_eq!(reply.task_id, answer_id);
        assert_eq!(reply.output.final_answer, "42");
        assert_eq!(reply.output.trace.len(), 2);
        assert_eq!(reply.output.tokens.total_tokens, 30);
    }

    #[tokio::test]
    async fn two_runs_of_one_case_come_back_identical() {
        let id = format!("eval-{}-{}", 1, digest("same question"));
        let fake = FakePlatform::new(
            task_view(
                &id,
                cog_core::TaskStatus::Completed,
                Some(serde_json::json!({ "answer": "same" })),
                None,
            ),
            trace_for(&id),
        );
        let served = Served::start(fake).await;
        let runner = served.runner(60);
        let first = runner.run(request("same question", 1)).await.unwrap();
        let second = runner.run(request("same question", 1)).await.unwrap();
        assert_eq!(
            serde_json::to_vec(&first.output).unwrap(),
            serde_json::to_vec(&second.output).unwrap(),
            "the platform's own clock must not reach the projection"
        );
    }

    #[tokio::test]
    async fn a_case_that_never_finishes_is_an_error_not_a_zero() {
        let id = format!("eval-{}-{}", 0, digest("slow question"));
        let fake = FakePlatform::new(
            task_view(&id, cog_core::TaskStatus::Pending, None, None),
            trace_for(&id),
        );
        let served = Served::start(fake).await;
        let err = served
            .runner(0)
            .run(request("slow question", 0))
            .await
            .expect_err("a case that cannot finish must not be scored");
        let text = err.to_string();
        assert!(text.contains("Pending"), "{text}");
        assert!(text.contains("cannot finish"), "{text}");
    }

    #[tokio::test]
    async fn a_failed_case_carries_the_platforms_own_reason() {
        let id = format!("eval-{}-{}", 0, digest("doomed question"));
        let fake = FakePlatform::new(
            task_view(
                &id,
                cog_core::TaskStatus::Failed,
                None,
                Some("the squad refused the goal".into()),
            ),
            trace_for(&id),
        );
        let served = Served::start(fake).await;
        let err = served
            .runner(60)
            .run(request("doomed question", 0))
            .await
            .expect_err("a failed case is not an answer");
        assert!(
            err.to_string().contains("the squad refused the goal"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_score_without_a_process_is_an_error() {
        let id = format!("eval-{}-{}", 0, digest("untraceable question"));
        let mut fake = FakePlatform::new(
            task_view(
                &id,
                cog_core::TaskStatus::Completed,
                Some(serde_json::json!("42")),
                None,
            ),
            trace_for(&id),
        );
        fake.trace_ids.clear();
        let served = Served::start(fake).await;
        let err = served
            .runner(60)
            .run(request("untraceable question", 0))
            .await
            .expect_err("an answer with no trace cannot be checked against anything");
        assert!(err.to_string().contains("no execution trace"), "{err}");
    }

    #[tokio::test]
    async fn a_platform_that_does_not_answer_is_an_error() {
        // 没有平台在听的那个端口：连接失败必须变成一条错误，而不是一份空答案。
        let runner = PlatformApiRunner::new(
            Arc::new(cog_net::factory::ReqwestHttpClient::new(
                reqwest::Client::new(),
            )),
            BridgeSettings {
                base: "http://127.0.0.1:1".into(),
                token: None,
                deadline_secs: 1,
            },
        );
        let err = runner
            .run(request("anything", 0))
            .await
            .expect_err("a platform that is not there cannot answer");
        assert!(err.to_string().contains("did not answer"), "{err}");
    }
}
