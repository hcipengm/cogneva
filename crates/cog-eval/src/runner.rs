//! 评估执行器 — 批量运行 agent + 结果收集 + metric 计算。

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::{EvalMetric, EvalResult, MetricValue, StepRecord};
use cog_core::{cosine_similarity, AgentRuntime, EmbeddingProvider};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Runner 配置。
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub max_concurrency: usize,
    pub timeout_seconds: u64,
    pub retry_count: u32,
    /// 是否启用 LLM-as-a-Judge 对主观指标进行评分。
    pub judge_enabled: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            timeout_seconds: 120,
            retry_count: 1,
            judge_enabled: false,
        }
    }
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

    for _ in 0..=config.retry_count {
        match tokio::time::timeout(
            std::time::Duration::from_secs(config.timeout_seconds),
            async {
                let mut agent = agent.lock().await;
                agent.run(case.input.clone(), &*llm).await
            },
        )
        .await
        {
            Ok(Ok(result)) => {
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
            Ok(Err(e)) => {
                last_error = Some(e.to_string());
            }
            Err(_) => {
                last_error = Some("Timeout".into());
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    let output_str = output.to_string();
    let token_estimate = (output_str.len() / 4) as u64; // Rough estimate: 4 chars per token

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
        token_usage: token_estimate,
        cost: estimate_cost(token_estimate),
        error: last_error,
        steps,
        trace_json: None,
    };

    // Compute metrics based on case configuration
    let mut metrics = vec![];
    let mut passed = result.passed;

    for metric in &case.metrics {
        let metric_name = metric.name();
        let (value, metric_passed) = compute_metric_value(
            metric,
            &case,
            &result,
            &embedder,
            llm.clone(),
            config.judge_enabled,
        )
        .await;

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

/// 估算成本（USD/1K tokens，默认 $0.002）。
fn estimate_cost(token_count: u64) -> f64 {
    let per_1k = 0.002;
    (token_count as f64 / 1000.0) * per_1k
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
) -> (f64, bool) {
    match metric {
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
            let output_str = result.agent_output.to_string();
            let output_len = output_str.len().max(1) as f64;
            let efficiency = result.token_usage as f64 / output_len;
            (efficiency, efficiency <= 0.5)
        }
        EvalMetric::CostPerTask => (result.cost, result.cost <= 0.01),
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
    }
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
