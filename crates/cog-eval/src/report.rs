//! 评估报告生成。
//! 在原有 EvalReport 基础上扩展：
//! - 时延的 P50/P95/P99
//! - per-metric 聚合（mean/median/p95/p99/stddev/pass_rate）
//! - 按维度（D1-D9）聚合的 DimensionSummary
//! - per-case 详情（PerCaseResult）
//! - 增强 Markdown / JSON / HTML 渲染（comfy-table 表格）

use crate::metric::{EvalResult, MetricValue};
use chrono::{DateTime, Utc};
use comfy_table::{ContentArrangement, Table};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// 报告格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReportFormat {
    Markdown,
    Json,
    Html,
}

/// 评估报告。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalReport {
    pub dataset_name: String,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub pass_rate: f64,
    pub avg_duration_ms: f64,
    pub p50_duration_ms: f64,
    pub p95_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub avg_token_usage: f64,
    pub total_cost: f64,
    pub metric_aggregates: HashMap<String, MetricAggregate>,
    pub dimension_summaries: HashMap<String, DimensionSummary>,
    pub failures: Vec<FailureDetail>,
    pub per_case_results: Vec<PerCaseResult>,
    pub generated_at: DateTime<Utc>,
}

/// 单个指标在数据集层面的聚合统计。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricAggregate {
    pub metric: String,
    pub mean: f64,
    pub median: f64,
    pub p95: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
    pub stddev: f64,
    pub pass_count: usize,
    pub fail_count: usize,
    pub pass_rate: f64,
}

/// 单个维度（D1-D9）的聚合摘要。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionSummary {
    pub dimension: String,
    pub metric_count: usize,
    pub overall_pass_rate: f64,
    pub metrics: Vec<String>,
}

/// 单条 case 的完整结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerCaseResult {
    pub case_id: String,
    pub case_name: String,
    pub passed: bool,
    pub duration_ms: u64,
    pub token_usage: u64,
    pub cost: f64,
    pub metric_values: Vec<MetricValue>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetail {
    pub case_id: String,
    pub case_name: String,
    pub error: String,
    pub duration_ms: u64,
}

impl EvalReport {
    /// 从原始 `EvalResult` 列表生成报告。
    /// 计算分位数、标准差、按维度聚合，并保留 per-case 详情。
    pub fn from_results(dataset_name: &str, results: &[EvalResult]) -> Self {
        Self::from_results_internal(dataset_name, results)
    }

    /// 从原始 `EvalResult` 列表生成报告，并补充多个 `Observable` 系统级指标。
    /// 在基础聚合之上，拉取 D5/D8/D9 维度的原始数据并合并到报告。
    pub async fn from_results_with_observables(
        dataset_name: &str,
        results: &[EvalResult],
        observables: &[Arc<dyn cog_core::Observable>],
    ) -> Self {
        let mut report = Self::from_results_internal(dataset_name, results);

        for dimension in ["D5", "D8", "D9"] {
            let mut raw_metrics = Vec::new();
            for observable in observables {
                if let Ok(mut m) = observable.collect_metrics(dimension).await {
                    raw_metrics.append(&mut m);
                }
            }

            let mut buckets: HashMap<String, Vec<f64>> = HashMap::new();
            for m in raw_metrics {
                buckets.entry(m.name).or_default().push(m.value);
            }
            for (name, values) in buckets {
                if values.is_empty() {
                    continue;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let agg = MetricAggregate {
                    metric: name.clone(),
                    mean,
                    median: mean,
                    p95: mean,
                    p99: mean,
                    min: mean,
                    max: mean,
                    stddev: 0.0,
                    pass_count: values.len(),
                    fail_count: 0,
                    pass_rate: 1.0,
                };
                report.metric_aggregates.insert(name.clone(), agg);
                report
                    .dimension_summaries
                    .entry(dimension.into())
                    .and_modify(|d| {
                        d.metric_count += 1;
                        d.metrics.push(name.clone());
                        d.metrics.sort();
                        d.metrics.dedup();
                    })
                    .or_insert(DimensionSummary {
                        dimension: dimension.into(),
                        metric_count: 1,
                        overall_pass_rate: 1.0,
                        metrics: vec![name.clone()],
                    });
            }
        }

        report
    }

    fn from_results_internal(dataset_name: &str, results: &[EvalResult]) -> Self {
        let total = results.len();
        let passed = results.iter().filter(|r| r.passed).count();
        let failed = total - passed;
        let denom = total.max(1) as f64;
        let pass_rate = passed as f64 / denom;

        // Duration quantiles.
        let mut durations: Vec<u64> = results.iter().map(|r| r.duration_ms).collect();
        durations.sort_unstable();
        let avg_duration = if total == 0 {
            0.0
        } else {
            durations.iter().sum::<u64>() as f64 / denom
        };
        let p50_duration = percentile_u64(&durations, 0.50);
        let p95_duration = percentile_u64(&durations, 0.95);
        let p99_duration = percentile_u64(&durations, 0.99);

        let avg_tokens = results.iter().map(|r| r.token_usage).sum::<u64>() as f64 / denom;
        let total_cost = results.iter().map(|r| r.cost).sum::<f64>();

        // Per-metric aggregates.
        let metric_aggregates = build_metric_aggregates(results);

        // Dimension summaries (D1-D9 grouping).
        let dimension_summaries = build_dimension_summaries(&metric_aggregates);

        // Per-case results.
        let per_case_results: Vec<PerCaseResult> = results
            .iter()
            .map(|r| PerCaseResult {
                case_id: r.case_id.clone(),
                case_name: r.case_id.clone(),
                passed: r.passed,
                duration_ms: r.duration_ms,
                token_usage: r.token_usage,
                cost: r.cost,
                metric_values: r.metrics.clone(),
                error: r.error.clone(),
            })
            .collect();

        let failures = results
            .iter()
            .filter(|r| !r.passed)
            .map(|r| FailureDetail {
                case_id: r.case_id.clone(),
                case_name: r.case_id.clone(),
                error: r.error.clone().unwrap_or_default(),
                duration_ms: r.duration_ms,
            })
            .collect();

        Self {
            dataset_name: dataset_name.into(),
            total_cases: total,
            passed_cases: passed,
            failed_cases: failed,
            pass_rate,
            avg_duration_ms: avg_duration,
            p50_duration_ms: p50_duration,
            p95_duration_ms: p95_duration,
            p99_duration_ms: p99_duration,
            avg_token_usage: avg_tokens,
            total_cost,
            metric_aggregates,
            dimension_summaries,
            failures,
            per_case_results,
            generated_at: Utc::now(),
        }
    }

    /// 渲染为指定格式。
    pub fn render(&self, format: ReportFormat) -> String {
        match format {
            ReportFormat::Json => serde_json::to_string_pretty(self).unwrap_or_default(),
            ReportFormat::Markdown => self.render_markdown(),
            ReportFormat::Html => self.render_html(),
        }
    }

    /// Markdown 渲染（含 comfy-table 表格）。
    fn render_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str(&format!("# Eval Report: {}\n\n", self.dataset_name));
        md.push_str(&format!(
            "_Generated at {}_\n\n",
            self.generated_at.to_rfc3339()
        ));

        md.push_str("## Summary\n\n");
        md.push_str(&format!("- **Total**: {}\n", self.total_cases));
        md.push_str(&format!(
            "- **Passed**: {} ({:.1}%)\n",
            self.passed_cases,
            self.pass_rate * 100.0
        ));
        md.push_str(&format!("- **Failed**: {}\n", self.failed_cases));
        md.push_str(&format!(
            "- **Duration**: avg {:.0} ms, p50 {:.0} ms, p95 {:.0} ms, p99 {:.0} ms\n",
            self.avg_duration_ms, self.p50_duration_ms, self.p95_duration_ms, self.p99_duration_ms,
        ));
        md.push_str(&format!("- **Tokens**: avg {:.0}\n", self.avg_token_usage));
        md.push_str(&format!("- **Total Cost**: ${:.4}\n\n", self.total_cost));

        // Dimension summary table.
        if !self.dimension_summaries.is_empty() {
            md.push_str("## Dimension Summary\n\n");
            let mut table = new_table();
            table.set_header(vec!["Dimension", "Metric Count", "Pass Rate", "Metrics"]);
            // Sort by dimension name for stable output.
            let mut entries: Vec<&DimensionSummary> = self.dimension_summaries.values().collect();
            entries.sort_by(|a, b| a.dimension.cmp(&b.dimension));
            for d in entries {
                table.add_row(vec![
                    d.dimension.clone(),
                    d.metric_count.to_string(),
                    format!("{:.1}%", d.overall_pass_rate * 100.0),
                    d.metrics.join(", "),
                ]);
            }
            md.push_str(&table.to_string());
            md.push_str("\n\n");
        }

        // Metric aggregates table.
        if !self.metric_aggregates.is_empty() {
            md.push_str("## Metric Aggregates\n\n");
            let mut table = new_table();
            table.set_header(vec![
                "Metric",
                "Mean",
                "Median",
                "P95",
                "P99",
                "Min",
                "Max",
                "Stddev",
                "Pass Rate",
            ]);
            let mut entries: Vec<&MetricAggregate> = self.metric_aggregates.values().collect();
            entries.sort_by(|a, b| a.metric.cmp(&b.metric));
            for m in entries {
                table.add_row(vec![
                    m.metric.clone(),
                    format!("{:.4}", m.mean),
                    format!("{:.4}", m.median),
                    format!("{:.4}", m.p95),
                    format!("{:.4}", m.p99),
                    format!("{:.4}", m.min),
                    format!("{:.4}", m.max),
                    format!("{:.4}", m.stddev),
                    format!("{:.1}%", m.pass_rate * 100.0),
                ]);
            }
            md.push_str(&table.to_string());
            md.push_str("\n\n");
        }

        if !self.failures.is_empty() {
            md.push_str("## Failures\n\n");
            let mut table = new_table();
            table.set_header(vec!["Case ID", "Case Name", "Duration (ms)", "Error"]);
            for f in &self.failures {
                table.add_row(vec![
                    f.case_id.clone(),
                    f.case_name.clone(),
                    f.duration_ms.to_string(),
                    truncate(&f.error, 200),
                ]);
            }
            md.push_str(&table.to_string());
            md.push_str("\n\n");
        }

        md
    }

    /// HTML 渲染（使用表格展示各维度指标）。
    fn render_html(&self) -> String {
        let mut html = String::new();
        html.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
        html.push_str(&format!(
            "<title>Eval Report: {}</title>",
            html_escape(&self.dataset_name)
        ));
        html.push_str("<style>");
        html.push_str(
            "body{font-family:-apple-system,sans-serif;margin:2rem;color:#222;}\
             table{border-collapse:collapse;margin:1rem 0;width:100%;}\
             th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;font-size:0.9rem;}\
             th{background:#f4f4f4;}\
             tr.fail td{background:#fef2f2;}\
             tr.pass td{background:#f0fdf4;}\
             code{background:#f5f5f5;padding:1px 4px;border-radius:3px;}",
        );
        html.push_str("</style></head><body>");

        html.push_str(&format!(
            "<h1>Eval Report: {}</h1>",
            html_escape(&self.dataset_name)
        ));
        html.push_str(&format!(
            "<p><em>Generated at {}</em></p>",
            self.generated_at.to_rfc3339()
        ));

        html.push_str("<h2>Summary</h2><ul>");
        html.push_str(&format!("<li>Total: {}</li>", self.total_cases));
        html.push_str(&format!(
            "<li>Passed: {} ({:.1}%)</li>",
            self.passed_cases,
            self.pass_rate * 100.0
        ));
        html.push_str(&format!("<li>Failed: {}</li>", self.failed_cases));
        html.push_str(&format!(
            "<li>Duration: avg {:.0} ms, p50 {:.0} ms, p95 {:.0} ms, p99 {:.0} ms</li>",
            self.avg_duration_ms, self.p50_duration_ms, self.p95_duration_ms, self.p99_duration_ms,
        ));
        html.push_str(&format!("<li>Tokens: avg {:.0}</li>", self.avg_token_usage));
        html.push_str(&format!("<li>Total cost: ${:.4}</li>", self.total_cost));
        html.push_str("</ul>");

        if !self.dimension_summaries.is_empty() {
            html.push_str("<h2>Dimension Summary</h2>");
            html.push_str(
                "<table><thead><tr><th>Dimension</th><th>Metric Count</th>\
                 <th>Pass Rate</th><th>Metrics</th></tr></thead><tbody>",
            );
            let mut entries: Vec<&DimensionSummary> = self.dimension_summaries.values().collect();
            entries.sort_by(|a, b| a.dimension.cmp(&b.dimension));
            for d in entries {
                html.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{:.1}%</td><td>{}</td></tr>",
                    html_escape(&d.dimension),
                    d.metric_count,
                    d.overall_pass_rate * 100.0,
                    html_escape(&d.metrics.join(", ")),
                ));
            }
            html.push_str("</tbody></table>");
        }

        if !self.metric_aggregates.is_empty() {
            html.push_str("<h2>Metric Aggregates</h2>");
            html.push_str(
                "<table><thead><tr><th>Metric</th><th>Mean</th><th>Median</th>\
                 <th>P95</th><th>P99</th><th>Min</th><th>Max</th><th>Stddev</th>\
                 <th>Pass Rate</th></tr></thead><tbody>",
            );
            let mut entries: Vec<&MetricAggregate> = self.metric_aggregates.values().collect();
            entries.sort_by(|a, b| a.metric.cmp(&b.metric));
            for m in entries {
                html.push_str(&format!(
                    "<tr><td>{}</td><td>{:.4}</td><td>{:.4}</td><td>{:.4}</td>\
                     <td>{:.4}</td><td>{:.4}</td><td>{:.4}</td><td>{:.4}</td>\
                     <td>{:.1}%</td></tr>",
                    html_escape(&m.metric),
                    m.mean,
                    m.median,
                    m.p95,
                    m.p99,
                    m.min,
                    m.max,
                    m.stddev,
                    m.pass_rate * 100.0,
                ));
            }
            html.push_str("</tbody></table>");
        }

        if !self.failures.is_empty() {
            html.push_str("<h2>Failures</h2>");
            html.push_str(
                "<table><thead><tr><th>Case ID</th><th>Case Name</th>\
                 <th>Duration (ms)</th><th>Error</th></tr></thead><tbody>",
            );
            for f in &self.failures {
                html.push_str(&format!(
                    "<tr class=\"fail\"><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    html_escape(&f.case_id),
                    html_escape(&f.case_name),
                    f.duration_ms,
                    html_escape(&truncate(&f.error, 400)),
                ));
            }
            html.push_str("</tbody></table>");
        }

        html.push_str("</body></html>");
        html
    }
}

// ---------------------------------------------------------------------------
// Aggregation helpers
// ---------------------------------------------------------------------------

fn build_metric_aggregates(results: &[EvalResult]) -> HashMap<String, MetricAggregate> {
    // Collect values + pass counts per metric name.
    let mut buckets: BTreeMap<String, Vec<&MetricValue>> = BTreeMap::new();
    for r in results {
        for m in &r.metrics {
            buckets.entry(m.metric.clone()).or_default().push(m);
        }
    }

    let mut out = HashMap::new();
    for (name, items) in buckets {
        let mut values: Vec<f64> = items.iter().map(|m| m.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = values.len().max(1) as f64;
        let sum: f64 = values.iter().copied().sum();
        let mean = sum / n;
        let variance: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        let stddev = variance.sqrt();
        let median = percentile_f64(&values, 0.50);
        let p95 = percentile_f64(&values, 0.95);
        let p99 = percentile_f64(&values, 0.99);
        let min = *values.first().unwrap_or(&0.0);
        let max = *values.last().unwrap_or(&0.0);
        let pass_count = items.iter().filter(|m| m.passed).count();
        let fail_count = items.len().saturating_sub(pass_count);
        let pass_rate = pass_count as f64 / items.len().max(1) as f64;
        out.insert(
            name.clone(),
            MetricAggregate {
                metric: name,
                mean,
                median,
                p95,
                p99,
                min,
                max,
                stddev,
                pass_count,
                fail_count,
                pass_rate,
            },
        );
    }
    out
}

fn build_dimension_summaries(
    aggregates: &HashMap<String, MetricAggregate>,
) -> HashMap<String, DimensionSummary> {
    let mut grouped: BTreeMap<String, Vec<&MetricAggregate>> = BTreeMap::new();
    for agg in aggregates.values() {
        let dim = dimension_for_metric(&agg.metric);
        grouped.entry(dim).or_default().push(agg);
    }

    let mut out = HashMap::new();
    for (dim, items) in grouped {
        let metric_count = items.len();
        let total_pass: usize = items.iter().map(|m| m.pass_count).sum();
        let total: usize = items.iter().map(|m| m.pass_count + m.fail_count).sum();
        let overall_pass_rate = total_pass as f64 / total.max(1) as f64;
        let mut metrics: Vec<String> = items.iter().map(|m| m.metric.clone()).collect();
        metrics.sort();
        out.insert(
            dim.clone(),
            DimensionSummary {
                dimension: dim,
                metric_count,
                overall_pass_rate,
                metrics,
            },
        );
    }
    out
}

/// 把 metric 名映射到 9 个评估维度（D1-D9）。
/// 命名约定（前缀匹配，与完备版评估指标体系对齐）：
/// - D1 任务完成与结果质量: exact_match, semantic_similarity, task_success_rate, step_accuracy, click_accuracy, type_accuracy, scroll_accuracy, navigate_accuracy
/// - D2 规划与推理质量: plan_quality, plan_adherence, logical_consistency
/// - D3 工具使用质量: tool_call_accuracy, tool_selection, tool_calling
/// - D4 执行效率: step_ratio, time_per_step, latency_*, explore_metric, backtracking_*
/// - D5 可观测性与调试: snapshot_*, state_coverage, event_stream_*, debuggability_index, context_overflow_rate
/// - D6 安全与对齐: harmful_output_rate, correct_refusal_rate, policy_compliance_rate
/// - D7 鲁棒性与可靠性: output_consistency, hallucination_rate, context_retention_score
/// - D8 多智能体协作: task_assignment_accuracy, collaboration_success_rate, stance_convergence
/// - D9 成本与资源效率: cost_*, token_efficiency, token_per_step, inference_latency
pub fn dimension_for_metric(name: &str) -> String {
    let n = name.to_ascii_lowercase();

    // D1 — 任务完成与结果质量
    if matches!(
        n.as_str(),
        "exact_match"
            | "semantic_similarity"
            | "output_quality"
            | "task_success_rate"
            | "strict_success_rate"
            | "partial_success_rate"
            | "pass_at_k"
            | "resolve_rate"
            | "goal_fulfillment"
            | "step_accuracy"
            | "element_accuracy"
            | "operation_f1"
            | "click_accuracy"
            | "type_accuracy"
            | "scroll_accuracy"
            | "navigate_accuracy"
    ) || n.starts_with("answer_")
        || n.starts_with("match_")
        || n.starts_with("task_success")
        || n.starts_with("strict_success")
        || n.starts_with("partial_success")
        || n.starts_with("resolve")
        || n.starts_with("goal_fulfillment")
        || n.starts_with("click_")
        || n.starts_with("type_")
        || n.starts_with("scroll_")
        || n.starts_with("navigate_")
        || n.starts_with("element_")
        || n.starts_with("operation_")
    {
        return "D1".into();
    }

    // D2 — 规划与推理质量
    if n.starts_with("plan_")
        || n == "plan_quality"
        || n == "plan_adherence"
        || n == "logical_consistency"
    {
        return "D2".into();
    }

    // D3 — 工具使用质量
    if n.starts_with("tool_") || n == "tool_call_accuracy" {
        return "D3".into();
    }

    // D4 — 执行效率
    if n.starts_with("step_ratio")
        || n.starts_with("repetitiveness")
        || n.starts_with("time_per_step")
        || n.starts_with("first_action_latency")
        || n.starts_with("execution_efficiency")
        || n.starts_with("step_success")
        || n.starts_with("recovery_rate")
        || n == "explore_metric"
        || n.starts_with("backtracking")
        || n.starts_with("latency_")
        || n.ends_with("_duration_ms")
        || n == "latency_ms"
    {
        return "D4".into();
    }

    // D5 — 可观测性与调试
    if n.starts_with("snapshot_")
        || n == "state_coverage"
        || n.starts_with("compression_ratio")
        || n.starts_with("storage_efficiency")
        || n.starts_with("backtrace_time")
        || n.starts_with("event_stream_")
        || n.starts_with("streaming_smoothness")
        || n.starts_with("ui_rendering_latency")
        || n.starts_with("debuggability_index")
        || n.starts_with("context_overflow")
        || n.starts_with("information_retention")
        || n.starts_with("memory_task_proficiency")
        || n.starts_with("summarization_distortion")
        || n.starts_with("layer_switch_latency")
    {
        return "D5".into();
    }

    // D6 — 安全与对齐
    if n.starts_with("harmful_output")
        || n.starts_with("correct_refusal")
        || n.starts_with("over_refusal")
        || n.starts_with("policy_compliance")
        || n.starts_with("adversarial_success")
        || n.starts_with("instruction_following")
        || n.starts_with("fairness_bias")
        || n.starts_with("transparency")
        || n.starts_with("safety_")
        || n.starts_with("guardrail_")
        || n.starts_with("refusal_")
    {
        return "D6".into();
    }

    // D7 — 鲁棒性与可靠性
    if n.starts_with("output_consistency")
        || n.starts_with("trajectory_consistency")
        || n.starts_with("score_consistency")
        || n.starts_with("tool_failure_recovery")
        || n.starts_with("environment_adaptation")
        || n.starts_with("hallucination")
        || n.starts_with("context_retention")
        || n.starts_with("long_tail")
        || n.starts_with("high_load")
        || n.starts_with("noisy_input")
    {
        return "D7".into();
    }

    // D8 — 多智能体协作
    if n.starts_with("task_assignment")
        || n.starts_with("information_flow")
        || n.starts_with("stance_")
        || n.starts_with("total_stance")
        || n.starts_with("semantic_diversity")
        || n.starts_with("consensus_efficiency")
        || n.starts_with("collaboration_success")
        || n.starts_with("group_reflection")
        || n.starts_with("role_conflict")
        || n.starts_with("self_organization")
        || n.starts_with("reasoning_")
        || n == "chain_of_thought"
    {
        return "D8".into();
    }

    // D9 — 成本与资源效率
    if n.starts_with("cost_")
        || n == "cost_usd"
        || n == "cost_per_task"
        || n.starts_with("token_efficiency")
        || n.starts_with("token_per_step")
        || n.starts_with("inference_latency")
        || n.starts_with("time_to_first_token")
    {
        return "D9".into();
    }

    // D10 — 系统部署与运维
    if n.starts_with("deployment_")
        || n.starts_with("first_time_success")
        || n.starts_with("mttr_")
        || n.starts_with("scale_elasticity")
        || n.starts_with("resource_overhead")
        || n.starts_with("gateway_latency")
        || n.starts_with("stability_sla")
    {
        return "D10".into();
    }

    // D11 — 代码质量与架构
    if n.starts_with("clippy_")
        || n.starts_with("cargo_test_")
        || n.starts_with("regression_rate")
        || n.starts_with("cargo_deny_")
        || n.starts_with("crate_dependency")
        || n.starts_with("fault_isolation")
        || n.starts_with("plugin_hotplug")
        || n.starts_with("architecture_drift")
        || n.starts_with("new_plugin_integration")
    {
        return "D11".into();
    }

    // D12 — 消融实验与进化
    if n.starts_with("ablation_delta")
        || n.starts_with("evolution_")
        || n.starts_with("dual_track_")
        || n.starts_with("cross_system_")
    {
        return "D12".into();
    }

    // D13 — 安全纵深防御
    if n.starts_with("credential_leak_")
        || n.starts_with("sandbox_escape_")
        || n.starts_with("attack_surface_")
        || n.starts_with("bootstrapper_credential_")
        || n.starts_with("gateway_proxy_latency")
    {
        return "D13".into();
    }

    "D9".into()
}

fn percentile_u64(sorted: &[u64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = (pct * (n as f64 - 1.0)).clamp(0.0, (n - 1) as f64);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo] as f64
    } else {
        let frac = rank - lo as f64;
        sorted[lo] as f64 + frac * (sorted[hi] as f64 - sorted[lo] as f64)
    }
}

fn percentile_f64(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = (pct * (n as f64 - 1.0)).clamp(0.0, (n - 1) as f64);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] + frac * (sorted[hi] - sorted[lo])
    }
}

fn new_table() -> Table {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table
}

fn truncate(s: &str, limit: usize) -> String {
    if s.chars().count() <= limit {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(limit).collect();
        format!("{}…", truncated)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::{EvalResult, MetricValue};

    fn mk_result(
        id: &str,
        passed: bool,
        duration: u64,
        metrics: Vec<(&str, f64, bool)>,
    ) -> EvalResult {
        EvalResult {
            case_id: id.into(),
            passed,
            metrics: metrics
                .into_iter()
                .map(|(m, v, p)| MetricValue {
                    metric: m.into(),
                    value: v,
                    passed: p,
                    threshold: None,
                })
                .collect(),
            agent_output: serde_json::Value::Null,
            duration_ms: duration,
            token_usage: 100,
            cost: 0.001,
            error: None,
            steps: vec![],
            trace_json: None,
        }
    }

    #[test]
    fn from_results_computes_quantiles_and_dimensions() {
        let results = vec![
            mk_result(
                "a",
                true,
                100,
                vec![("exact_match", 1.0, true), ("plan_quality", 0.9, true)],
            ),
            mk_result(
                "b",
                true,
                200,
                vec![("exact_match", 1.0, true), ("plan_quality", 0.7, false)],
            ),
            mk_result(
                "c",
                false,
                1000,
                vec![("exact_match", 0.0, false), ("plan_quality", 0.6, false)],
            ),
        ];
        let report = EvalReport::from_results("demo", &results);
        assert_eq!(report.total_cases, 3);
        assert_eq!(report.passed_cases, 2);
        assert_eq!(report.failed_cases, 1);
        assert!(report.p99_duration_ms >= report.p95_duration_ms);
        assert!(report.p95_duration_ms >= report.p50_duration_ms);
        assert!(report.metric_aggregates.contains_key("exact_match"));
        assert!(report.dimension_summaries.contains_key("D1"));
        assert!(report.dimension_summaries.contains_key("D2"));
        assert_eq!(report.per_case_results.len(), 3);
    }

    #[test]
    fn dimension_mapping_for_known_metrics() {
        assert_eq!(dimension_for_metric("exact_match"), "D1");
        assert_eq!(dimension_for_metric("plan_quality"), "D2");
        assert_eq!(dimension_for_metric("tool_call_accuracy"), "D3");
        assert_eq!(dimension_for_metric("latency_ms"), "D4");
        assert_eq!(dimension_for_metric("snapshot_reproducibility"), "D5");
        assert_eq!(dimension_for_metric("guardrail_violation"), "D6");
        assert_eq!(dimension_for_metric("output_consistency"), "D7");
        assert_eq!(dimension_for_metric("collaboration_success_rate"), "D8");
        assert_eq!(dimension_for_metric("cost_usd"), "D9");
        assert_eq!(dimension_for_metric("token_efficiency"), "D9");
    }

    #[test]
    fn render_markdown_includes_sections() {
        let results = vec![mk_result("a", true, 100, vec![("exact_match", 1.0, true)])];
        let report = EvalReport::from_results("demo", &results);
        let md = report.render(ReportFormat::Markdown);
        assert!(md.contains("# Eval Report: demo"));
        assert!(md.contains("Dimension Summary"));
        assert!(md.contains("Metric Aggregates"));
    }

    #[test]
    fn render_html_includes_tables() {
        let results = vec![mk_result("a", true, 100, vec![("exact_match", 1.0, true)])];
        let report = EvalReport::from_results("demo", &results);
        let html = report.render(ReportFormat::Html);
        assert!(html.contains("<title>Eval Report: demo</title>"));
        assert!(html.contains("<h2>Dimension Summary</h2>"));
        assert!(html.contains("<h2>Metric Aggregates</h2>"));
    }
}
