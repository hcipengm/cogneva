//! 评估执行器 — 批量运行 agent + 结果收集 + metric 计算。

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::{EvalMetric, EvalResult, MetricValue, StepRecord};
use cog_core::{cosine_similarity, AgentRuntime, EmbeddingProvider};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Runner 配置。
///
/// **没有逐例超时这一项**（2026-11-10 删掉 `timeout_seconds`）。原来一例跑到 120 秒
/// 就被掐断、记成 `Timeout`、按 `retry_count` 重来——那条判据读的是钟，不是这一例
/// 有没有在动：一个一直在产出、只是慢的例与一个第一秒就卡死的例在它眼里是同一个读数，
/// 而它更贵的代价在后面：被它掐掉的那次尝试花掉的 token 不进入任何样本，于是「这个
/// 界合不合适」永远量不出来。现在一例的结局只由它自己的结果决定——跑完、报错、或
/// 一直没回来。没回来不是没人知道：见 [`in_flight_cases`]，那里读得到谁还在飞、飞了
/// 多久，这个读数不掐任何东西。
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub max_concurrency: usize,
    pub retry_count: u32,
    /// 是否启用 LLM-as-a-Judge 对主观指标进行评分。
    pub judge_enabled: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            retry_count: 1,
            judge_enabled: false,
        }
    }
}

/// 一个正在飞的评测例：它是谁、第几次尝试、从几时起。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightCase {
    pub case_id: String,
    pub attempt: u32,
    pub running: std::time::Duration,
}

fn in_flight() -> &'static Mutex<std::collections::HashMap<String, (u32, std::time::Instant)>> {
    static IN_FLIGHT: std::sync::OnceLock<
        Mutex<std::collections::HashMap<String, (u32, std::time::Instant)>>,
    > = std::sync::OnceLock::new();
    IN_FLIGHT.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// 现在还有哪些例在飞、各自从几时起。
///
/// 这是删掉逐例超时之后，「慢」与「挂住」之间唯一的读数。它**不判**任何一例：
/// 一份读数被判成「挂住」再拿去掐，就是又造了一个钟——只是把这个钟藏进了读者里。
/// 要不要处理一个跑了很久的例，由读它的人决定，而他至少能看到「跑在第几次尝试、
/// 从几时起」，而不是从一条平的进度线上去猜。
pub async fn in_flight_cases() -> Vec<InFlightCase> {
    let now = std::time::Instant::now();
    in_flight()
        .lock()
        .await
        .iter()
        .map(|(case_id, (attempt, started))| InFlightCase {
            case_id: case_id.clone(),
            attempt: *attempt,
            running: now.saturating_duration_since(*started),
        })
        .collect()
}

/// 评估执行器。
pub struct EvalRunner {
    agent: Arc<Mutex<dyn AgentRuntime>>,
    llm: Arc<dyn cog_core::LlmClient>,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
    config: RunnerConfig,
}

impl EvalRunner {
    pub fn new(
        agent: Arc<Mutex<dyn AgentRuntime>>,
        llm: Arc<dyn cog_core::LlmClient>,
        config: RunnerConfig,
    ) -> Self {
        Self {
            agent,
            llm,
            embedder: None,
            config,
        }
    }

    /// 注入 EmbeddingProvider，用于语义相似度评估（替换 Jaccard）。
    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// 运行整个数据集。
    pub async fn run_dataset(&self, dataset: &EvalDataset) -> Vec<EvalResult> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));
        let mut handles = vec![];

        for case in &dataset.cases {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let agent = self.agent.clone();
            let llm = self.llm.clone();
            let embedder = self.embedder.clone();
            let case = case.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                let _permit = permit;
                run_single_case(agent, llm, embedder, case, config).await
            });
            handles.push(handle);
        }

        let mut results = vec![];
        for h in handles {
            if let Ok(r) = h.await {
                results.push(r);
            }
        }
        results
    }
}

async fn run_single_case(
    agent: Arc<Mutex<dyn AgentRuntime>>,
    llm: Arc<dyn cog_core::LlmClient>,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
    case: EvalCase,
    config: RunnerConfig,
) -> EvalResult {
    let start = std::time::Instant::now();

    let mut last_error = None;
    let mut output = serde_json::Value::Null;
    let mut tool_calls: Vec<String> = vec![];

    for attempt in 0..=config.retry_count {
        // 从进这一轮到出这一轮，这一例都登记着：它是谁、第几次。删掉钟之后这是
        // 「它还在」的唯一证据——一个永远不返回的例不会留下别的痕迹。
        in_flight()
            .lock()
            .await
            .insert(case.id.clone(), (attempt, std::time::Instant::now()));
        // 这一例的结局由它自己决定，不由钟决定：跑完就是跑完，报错就是报错。
        let outcome = {
            let mut agent = agent.lock().await;
            agent.run(case.input.clone(), &*llm).await
        };
        match outcome {
            Ok(result) => {
                output = serde_json::to_value(&result).unwrap_or_default();
                // Extract tool calls from result if present
                if let Some(tools) = result.get("tool_calls").and_then(|v| v.as_array()) {
                    tool_calls = tools
                        .iter()
                        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
                        .collect();
                }
                break;
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }
        // 失败之后不许立刻再发：紧循环重试会把一个上游拒绝放大成一场风暴
        // （2026-10-19T01:23Z 实测一分钟 455 条，客户端没有任何间隔）。
        // 读得到「下次几点再来」就按它等；读不到也有一个有界的下限，
        // 「没说等多久」不等于「可以立刻再试」。
        if attempt < config.retry_count {
            tokio::time::sleep(retry_wait(last_error.as_deref())).await;
        }
    }

    in_flight().lock().await.remove(&case.id);

    let duration_ms = start.elapsed().as_millis() as u64;

    // 构建 StepRecord 列表（当前 agent.run 不返回 steps，使用占位）
    let steps = vec![StepRecord {
        step_index: 0,
        action_type: "run".into(),
        action_params: output.clone(),
        thought: None,
        duration_ms,
        success: last_error.is_none(),
        tool_calls: tool_calls.clone(),
    }];

    let mut result = EvalResult {
        case_id: case.id.clone(),
        passed: last_error.is_none(),
        metrics: vec![],
        agent_output: output,
        duration_ms,
        // `AgentRuntime::run` hands back the agent's own value, not the turns'
        // usage frames, so no reading of what this run spent reaches the
        // harness. Leaving these empty is the whole point: an answer's length
        // is not a token count, and a cost derived from it would pass a
        // cost-per-task gate by construction while telling nobody what was
        // actually paid.
        token_usage: None,
        cost: None,
        error: last_error,
        steps,
        trace_json: None,
    };

    // Compute metrics based on case configuration
    let mut metrics = vec![];
    let mut passed = result.passed;

    for metric in &case.metrics {
        let metric_name = metric.name();
        let Some((value, metric_passed)) = compute_metric_value(
            metric,
            &case,
            &result,
            &embedder,
            llm.clone(),
            config.judge_enabled,
        )
        .await
        else {
            // 量不出来的指标不出行。写成 0 会被下游读成「花得很少、通过」，
            // 而它真正的意思是「这一轮没人量」。两者在表上必须长得不一样。
            continue;
        };

        if !metric_passed {
            passed = false;
        }

        metrics.push(MetricValue {
            metric: metric_name,
            value,
            passed: metric_passed,
            threshold: metric.default_threshold(),
        });
    }

    result.passed = passed;
    result.metrics = metrics;
    result
}

// ---------------------------------------------------------------------------
// Metric implementations
// ---------------------------------------------------------------------------

/// 精确匹配：JSON 结构完全相等（忽略数组顺序和空白）。
fn exact_match_score(expected: &serde_json::Value, actual: &serde_json::Value) -> f64 {
    if normalize_json(expected) == normalize_json(actual) {
        1.0
    } else {
        0.0
    }
}

fn normalize_json(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut sorted: Vec<_> = map.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(b.0));
            let normalized: serde_json::Map<String, serde_json::Value> = sorted
                .into_iter()
                .map(|(k, v)| (k.clone(), normalize_json(v)))
                .collect();
            serde_json::Value::Object(normalized)
        }
        serde_json::Value::Array(arr) => {
            let mut normalized: Vec<_> = arr.iter().map(normalize_json).collect();
            // Sort arrays of primitives for stable comparison
            normalized.sort_by(|a, b| {
                serde_json::to_string(a)
                    .unwrap_or_default()
                    .cmp(&serde_json::to_string(b).unwrap_or_default())
            });
            serde_json::Value::Array(normalized)
        }
        other => other.clone(),
    }
}

/// 语义相似度：Embedding + 余弦相似度（替换 Jaccard）。
/// 使用 BGE-M3 dense embedding 计算 expected 和 actual 的语义相似度。
/// 当 embedding 失败时回退到 Jaccard。
async fn semantic_similarity_embedding(
    expected: &serde_json::Value,
    actual: &serde_json::Value,
    embedder: &dyn EmbeddingProvider,
) -> f64 {
    let expected_str = json_to_searchable_text(expected);
    let actual_str = json_to_searchable_text(actual);

    if expected_str.is_empty() && actual_str.is_empty() {
        return 1.0;
    }
    if expected_str.is_empty() || actual_str.is_empty() {
        return 0.0;
    }

    match embedder.embed(vec![expected_str, actual_str]).await {
        Ok(vecs) if vecs.len() == 2 => cosine_similarity(&vecs[0], &vecs[1]),
        _ => {
            tracing::warn!("embedding failed, falling back to jaccard");
            semantic_similarity_jaccard(expected, actual)
        }
    }
}

/// 语义相似度：Jaccard 系数（回退方案）。
fn semantic_similarity_jaccard(expected: &serde_json::Value, actual: &serde_json::Value) -> f64 {
    let expected_str = json_to_searchable_text(expected);
    let actual_str = json_to_searchable_text(actual);

    let expected_tokens: std::collections::HashSet<String> =
        tokenize(&expected_str).into_iter().collect();
    let actual_tokens: std::collections::HashSet<String> =
        tokenize(&actual_str).into_iter().collect();

    if expected_tokens.is_empty() && actual_tokens.is_empty() {
        return 1.0;
    }
    if expected_tokens.is_empty() || actual_tokens.is_empty() {
        return 0.0;
    }

    let intersection: std::collections::HashSet<_> = expected_tokens
        .intersection(&actual_tokens)
        .cloned()
        .collect();
    let union: std::collections::HashSet<_> =
        expected_tokens.union(&actual_tokens).cloned().collect();

    intersection.len() as f64 / union.len() as f64
}

fn json_to_searchable_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Object(map) => map
            .values()
            .map(json_to_searchable_text)
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(json_to_searchable_text)
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// 工具调用准确率：期望工具名集合 vs 实际工具名集合的精确匹配。
fn tool_call_accuracy(expected: &[String], actual: &[String]) -> f64 {
    if expected.is_empty() && actual.is_empty() {
        return 1.0;
    }
    if expected.is_empty() || actual.is_empty() {
        return 0.0;
    }
    let expected_set: std::collections::HashSet<_> = expected.iter().cloned().collect();
    let actual_set: std::collections::HashSet<_> = actual.iter().cloned().collect();

    if expected_set == actual_set {
        1.0
    } else {
        let intersection: std::collections::HashSet<_> =
            expected_set.intersection(&actual_set).cloned().collect();
        intersection.len() as f64 / expected_set.len().max(actual_set.len()) as f64
    }
}

/// 计算单个指标的值与通过状态。
/// 对于客观指标（ExactMatch、SemanticSimilarity 等）使用确定性算法；
/// 对于主观指标（GoalFulfillment、PlanQuality 等）在 `judge_enabled` 时调用 LLM-as-a-Judge，
/// 否则回退到 dimensions 模块的启发式计算。
async fn compute_metric_value(
    metric: &EvalMetric,
    case: &EvalCase,
    result: &EvalResult,
    embedder: &Option<Arc<dyn EmbeddingProvider>>,
    llm: Arc<dyn cog_core::LlmClient>,
    judge_enabled: bool,
) -> Option<(f64, bool)> {
    // 先问「这一指标要的量，这一轮到底量到了没有」。花销与用量都只有上游的
    // usage 帧能给，给不出时 `cost` / `token_usage` 是 `None`；再往下算就会把
    // 「没量到」折成 0 并判通过——那不是通过，那是没测。
    if measures_cost(metric) && result.cost.is_none() {
        return None;
    }
    if measures_tokens(metric) && result.token_usage.is_none() {
        return None;
    }
    Some(match metric {
        EvalMetric::ExactMatch => {
            let expected = case
                .expected_output
                .clone()
                .unwrap_or(serde_json::Value::Null);
            let score = exact_match_score(&expected, &result.agent_output);
            (score, score >= 1.0)
        }
        EvalMetric::SemanticSimilarity { threshold } => {
            let expected = case
                .expected_output
                .clone()
                .unwrap_or(serde_json::Value::Null);
            let score = if let Some(ref embedder) = embedder {
                semantic_similarity_embedding(&expected, &result.agent_output, embedder.as_ref())
                    .await
            } else {
                semantic_similarity_jaccard(&expected, &result.agent_output)
            };
            let t = *threshold as f64;
            (score, score >= t)
        }
        EvalMetric::ToolCallAccuracy => {
            let expected_tools = case.expected_tools.clone().unwrap_or_default();
            let actual_tools: Vec<String> = result
                .steps
                .iter()
                .flat_map(|s| s.tool_calls.clone())
                .collect();
            let score = tool_call_accuracy(&expected_tools, &actual_tools);
            (score, score >= 1.0)
        }
        EvalMetric::LatencyP50 => (result.duration_ms as f64, result.duration_ms <= 5000),
        EvalMetric::LatencyP99 => (result.duration_ms as f64, result.duration_ms <= 30000),
        EvalMetric::TokenEfficiency => {
            // 前面已经按 `measures_tokens` 挡掉了 `None`。
            let tokens = result.token_usage.unwrap_or(0) as f64;
            let output_len = result.agent_output.to_string().len().max(1) as f64;
            let efficiency = tokens / output_len;
            (efficiency, efficiency <= 0.5)
        }
        EvalMetric::CostPerTask => {
            // 同上：没有真实花销读数时这一行根本不出。
            let spent = result.cost.unwrap_or(0.0);
            (spent, spent <= 0.01)
        }
        EvalMetric::Custom { .. } => (0.0, true),
        // 主观指标：优先使用 LLM-as-a-Judge
        _ if judge_enabled => match judge_subjective(metric, case, result, llm).await {
            Some(score) => {
                let threshold = metric.default_threshold().unwrap_or(0.5);
                (score, score >= threshold)
            }
            None => crate::dimensions::compute(metric, case, result),
        },
        // D1-D9 扩展指标统一路由到 dimensions 模块
        other => crate::dimensions::compute(other, case, result),
    })
}

/// 这一指标要不要钱的读数？
///
/// 与 [`measures_tokens`] 分开列，因为两者可能一个有一个没有：上游报了
/// usage 帧就可以有 token 数，而金额还要另有一份真实价目。
fn measures_cost(metric: &EvalMetric) -> bool {
    matches!(
        metric,
        EvalMetric::CostPerTask | EvalMetric::CostPerStep { .. }
    )
}

/// 这一指标要不要本轮用量的读数？
fn measures_tokens(metric: &EvalMetric) -> bool {
    matches!(
        metric,
        EvalMetric::TokenEfficiency | EvalMetric::TokenPerStep { .. }
    )
}

/// 对主观指标调用 LLM-as-a-Judge。
/// 返回 `Some(0.0-1.0 标准化分数)`，若该指标无需 Judge 则返回 `None`。
async fn judge_subjective(
    metric: &EvalMetric,
    case: &EvalCase,
    result: &EvalResult,
    llm: Arc<dyn cog_core::LlmClient>,
) -> Option<f64> {
    use crate::judge::{
        execution_efficiency_judge, goal_judge, logic_judge, plan_adherence_judge, plan_judge,
        safety_judge, tool_calling_judge, tool_selection_judge, Judge,
    };

    let judge: Box<dyn Judge> = match metric {
        EvalMetric::GoalFulfillment { .. } => Box::new(goal_judge(llm)),
        EvalMetric::PlanQuality { .. } => Box::new(plan_judge(llm)),
        EvalMetric::PlanAdherence { .. } => Box::new(plan_adherence_judge(llm)),
        EvalMetric::LogicalConsistency { .. } => Box::new(logic_judge(llm)),
        EvalMetric::ToolSelection { .. } => Box::new(tool_selection_judge(llm)),
        EvalMetric::ToolCalling { .. } => Box::new(tool_calling_judge(llm)),
        EvalMetric::ExecutionEfficiency { .. } => Box::new(execution_efficiency_judge(llm)),
        // D6 安全类指标统一使用 safety_judge
        EvalMetric::HarmfulOutputRate { .. }
        | EvalMetric::CorrectRefusalRate { .. }
        | EvalMetric::OverRefusalRate { .. }
        | EvalMetric::PolicyComplianceRate { .. }
        | EvalMetric::AdversarialSuccessRate { .. }
        | EvalMetric::InstructionFollowingRate { .. }
        | EvalMetric::FairnessBiasScore { .. }
        | EvalMetric::TransparencyScore { .. } => Box::new(safety_judge(llm)),
        _ => return None,
    };

    match judge.judge(case, result).await {
        Ok(score) => Some(score.clamp(0.0, 1.0)),
        Err(e) => {
            tracing::warn!(metric = %metric.name(), error = %e, "LLM Judge failed, falling back to heuristic");
            None
        }
    }
}

/// 一次失败之后到下一次尝试之间等多久。
///
/// 错误在这里已经被压成字符串（它离开了产生它的那一层），所以提示要从渲染里读回来；
/// 读不到就用一个有界下限——超时、`Timeout`、没有任何原因的错误都不是
/// 「可以立刻再试」，它们只是没说等多久。
fn retry_wait(last_error: Option<&str>) -> std::time::Duration {
    last_error
        .and_then(cog_core::contract::llm::retry_after_hint_in)
        .map(std::time::Duration::from_secs)
        .unwrap_or(crate::backoff::DEFAULT_REFUSAL_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stated_wait_is_honoured_and_an_unstated_one_is_bounded() {
        let stated = format!(
            "503 refused {}",
            cog_core::contract::llm::render_retry_after(42)
        );
        assert_eq!(
            retry_wait(Some(&stated)),
            std::time::Duration::from_secs(42)
        );
        assert_eq!(
            retry_wait(Some("Timeout")),
            crate::backoff::DEFAULT_REFUSAL_BACKOFF
        );
        assert_eq!(retry_wait(None), crate::backoff::DEFAULT_REFUSAL_BACKOFF);
        assert_ne!(
            retry_wait(Some("Timeout")),
            std::time::Duration::ZERO,
            "an unstated wait must not be a zero-length one"
        );
    }
}

// ---------------------------------------------------------------------------
// 一例的结局由它自己决定，不由钟决定
// ---------------------------------------------------------------------------

#[cfg(test)]
mod no_clock_wall_tests {
    use super::*;
    use crate::dataset::EvalCase;
    use async_trait::async_trait;
    use cog_core::{ChatOptions, CompleteOptions, Message, SFResult};

    /// 这条路不碰上游；这一组判的是「这一例跑多久」，不是模型答得如何。
    struct NeverCalled;

    #[async_trait]
    impl cog_core::LlmClient for NeverCalled {
        async fn chat_stream(
            &self,
            _m: &[Message],
            _o: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn complete_stream(
            &self,
            _p: &str,
            _o: &CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<cog_core::ChatResponse> {
            unreachable!()
        }
        async fn health_check(&self) -> bool {
            false
        }
    }

    fn case(id: &str) -> EvalCase {
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!({}),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    /// 一个慢例：它一直在同一个调用里等，但等的是自己的事，不是挂死。
    struct SlowAgent {
        secs: u64,
    }

    #[async_trait]
    impl cog_core::AgentRuntime for SlowAgent {
        async fn run(
            &mut self,
            _input: serde_json::Value,
            _llm: &dyn cog_core::LlmClient,
        ) -> SFResult<serde_json::Value> {
            tokio::time::sleep(std::time::Duration::from_secs(self.secs)).await;
            Ok(serde_json::json!({"answer": "done"}))
        }
        fn agent_id(&self) -> &str {
            "slow"
        }
        fn role(&self) -> &str {
            "eval"
        }
    }

    /// 这一格在没有改动时会红：旧的 `tokio::time::timeout(120s, ...)` 判的是钟，
    /// 300 秒的例在 120 秒那一刻被判成 `Timeout`、记成没过。新判据只问它有没有
    /// 跑完——所以这一格是「慢而不停的例不再被掐」的那条测试。
    ///
    /// 虚拟时钟：`start_paused` 让 300 秒在一次测试里瞬间走完，而两个定时器
    /// 的先后关系与真实时间上完全一样（120 早于 300）。
    #[tokio::test(start_paused = true)]
    async fn a_case_that_runs_past_the_old_wall_judges_itself_instead_of_timing_out() {
        let agent: Arc<Mutex<dyn cog_core::AgentRuntime>> =
            Arc::new(Mutex::new(SlowAgent { secs: 300 }));
        let result = run_single_case(
            agent,
            Arc::new(NeverCalled),
            None,
            case("slow-300s"),
            RunnerConfig::default(),
        )
        .await;
        assert!(
            result.passed,
            "一例跑了 300 秒就判成没过：钟又回来了（error = {:?}）",
            result.error
        );
        assert_eq!(result.error, None);
    }

    /// 一个永远不返回的例：它不被掐断（这是有意的），但它**必须**在读数面上
    /// 看得见。没有这一半，删掉钟就等于把「挂住」变成一件没人知道的事。
    struct GatedAgent {
        entered: tokio::sync::mpsc::Sender<()>,
        gate: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait]
    impl cog_core::AgentRuntime for GatedAgent {
        async fn run(
            &mut self,
            _input: serde_json::Value,
            _llm: &dyn cog_core::LlmClient,
        ) -> SFResult<serde_json::Value> {
            let _ = self.entered.send(()).await;
            let _permit = self.gate.acquire().await.expect("gate never closed");
            Ok(serde_json::json!({"answer": "released"}))
        }
        fn agent_id(&self) -> &str {
            "gated"
        }
        fn role(&self) -> &str {
            "eval"
        }
    }

    #[tokio::test]
    async fn a_case_that_never_returns_is_visible_in_flight_and_is_never_cut() {
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::channel(1);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let agent: Arc<Mutex<dyn cog_core::AgentRuntime>> = Arc::new(Mutex::new(GatedAgent {
            entered: entered_tx,
            gate: gate.clone(),
        }));

        let run = tokio::spawn(run_single_case(
            agent,
            Arc::new(NeverCalled),
            None,
            case("held"),
            RunnerConfig::default(),
        ));
        entered_rx.recv().await.expect("the case never started");

        let flying: Vec<InFlightCase> = in_flight_cases()
            .await
            .into_iter()
            .filter(|c| c.case_id == "held")
            .collect();
        assert_eq!(
            flying.len(),
            1,
            "在飞的例没有出现在读数里：删掉钟之后它就是隐形的"
        );
        assert_eq!(flying[0].attempt, 0);
        assert!(!run.is_finished(), "还没跑完的例已经被判完了");

        gate.add_permits(1);
        let result = run.await.expect("the case panicked");
        assert!(result.passed);
        assert!(
            in_flight_cases()
                .await
                .into_iter()
                .all(|c| c.case_id != "held"),
            "跑完的例还留在在飞读数里"
        );
    }
}

// ---------------------------------------------------------------------------
// 花销读数缺失时不许出分
// ---------------------------------------------------------------------------

#[cfg(test)]
mod spend_readings_are_readings_tests {
    use super::*;
    use crate::dataset::EvalCase;
    use crate::metric::EvalMetric;
    use async_trait::async_trait;
    use cog_core::{ChatOptions, ChatResponse, Message, SFResult};

    /// 这几条路径都不该碰上游：判的是「读数缺了怎么处理」，不是模型答得如何。
    struct NeverCalled;

    #[async_trait]
    impl cog_core::LlmClient for NeverCalled {
        async fn chat_stream(
            &self,
            _m: &[Message],
            _o: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn complete_stream(
            &self,
            _p: &str,
            _o: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
            unreachable!()
        }
        async fn health_check(&self) -> bool {
            false
        }
    }

    /// 交得出一份答案，但**不交用量**——真实运行时就是这个形状：
    /// `AgentRuntime::run` 的返回值里没有 usage 帧。
    struct AnswerOnly;

    #[async_trait]
    impl AgentRuntime for AnswerOnly {
        async fn run(
            &mut self,
            _input: serde_json::Value,
            _llm: &dyn cog_core::LlmClient,
        ) -> SFResult<serde_json::Value> {
            Ok(serde_json::json!({ "answer": "ok" }))
        }
        fn agent_id(&self) -> &str {
            "a"
        }
        fn role(&self) -> &str {
            "r"
        }
    }

    fn cost_case() -> EvalCase {
        EvalCase {
            id: "c1".into(),
            name: "c1".into(),
            input: serde_json::json!("q"),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![
                EvalMetric::CostPerTask,
                EvalMetric::TokenEfficiency,
                EvalMetric::CostPerStep { threshold: 0.002 },
                EvalMetric::TokenPerStep { threshold: 25.0 },
            ],
            metadata: Default::default(),
        }
    }

    /// 没有 usage 帧时，花销与用量是 `None`，不是 0；四条依赖它们的指标一条都不出。
    ///
    /// 这条测试盯的是原先那种形状：`(answer.len() / 4)` 当 token 数、再乘一个
    /// 写死的 $0.002/1K 当钱——于是 `CostPerTask` 的 `cost <= 0.01` 由算式本身
    /// 保证通过，谁也没真的量过。把估算删掉之后，这里读到的是「没量到」。
    #[tokio::test]
    async fn a_run_that_reported_no_usage_produces_no_spend_metric() {
        let case = cost_case();
        let result = run_single_case(
            Arc::new(Mutex::new(AnswerOnly)),
            Arc::new(NeverCalled),
            None,
            case,
            RunnerConfig::default(),
        )
        .await;

        assert_eq!(result.cost, None, "没量到钱就不能写一个数进去");
        assert_eq!(result.token_usage, None, "没量到用量就不能写一个数进去");

        let names: Vec<&str> = result.metrics.iter().map(|m| m.metric.as_str()).collect();
        for absent in [
            "cost_usd",
            "token_efficiency",
            "cost_per_step",
            "token_per_step",
        ] {
            assert!(
                !names.contains(&absent),
                "{absent} 没有读数却出现在结果里：{names:?}"
            );
        }
        assert!(result.passed, "指标缺席不等于这条 case 判红：它只是没被测");
    }

    /// 反过来：真拿到读数时这四条指标照样出，证明上面那条不是「一律跳过」。
    #[tokio::test]
    async fn a_measured_reading_still_produces_its_metric() {
        let case = cost_case();
        let result = EvalResult {
            case_id: "c1".into(),
            passed: true,
            metrics: vec![],
            agent_output: serde_json::json!("ok"),
            duration_ms: 10,
            token_usage: Some(40),
            cost: Some(0.004),
            error: None,
            steps: vec![],
            trace_json: None,
        };

        let mut metrics = vec![];
        for metric in &case.metrics {
            let Some((value, passed)) =
                compute_metric_value(metric, &case, &result, &None, Arc::new(NeverCalled), false)
                    .await
            else {
                continue;
            };
            metrics.push((metric.name(), value, passed));
        }
        let names: Vec<&str> = metrics.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(names.contains(&"cost_usd"), "{names:?}");
        assert!(names.contains(&"token_efficiency"), "{names:?}");
        assert!(names.contains(&"cost_per_step"), "{names:?}");
        assert!(names.contains(&"token_per_step"), "{names:?}");
    }
}
