//! Eval Gateway API — HTTP 端点定义。
//! 提供 `/api/v1/eval/*` 端点，用于触发评估运行和对比。
//! 当前为数据结构 + 路由定义骨架，完整 HTTP 服务需在 cog-gateway 或独立服务中集成。

use crate::metric::EvalResult;
use serde::{Deserialize, Serialize};

/// POST /api/v1/eval/run 请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvalRequest {
    pub dataset_path: String,
    pub max_concurrency: Option<usize>,
    pub metrics: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
}

/// POST /api/v1/eval/run 响应体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvalResponse {
    pub run_id: String,
    pub dataset_name: String,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub pass_rate: f64,
    pub duration_ms: u64,
    pub results: Vec<EvalResult>,
}

/// POST /api/v1/eval/compare 请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareEvalRequest {
    pub baseline_name: String,
    pub challenger_name: String,
    pub baseline_results: Vec<EvalResult>,
    pub challenger_results: Vec<EvalResult>,
}

/// POST /api/v1/eval/compare 响应体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareEvalResponse {
    pub baseline_name: String,
    pub challenger_name: String,
    pub winner: String,
    pub delta_percent: f64,
    pub p_value: f64,
    pub statistically_significant: bool,
    pub recommendation: String,
}

/// GET /api/v1/eval/datasets 响应体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatasetsResponse {
    pub datasets: Vec<DatasetInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub name: String,
    pub path: String,
    pub case_count: usize,
    pub tags: Vec<String>,
}

/// GET /api/v1/eval/report/:report_id 响应体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReportResponse {
    pub run_id: String,
    pub dataset_name: String,
    pub report_markdown: String,
    pub report_json: String,
}

/// Gateway 路由注册（骨架）。
/// 实际集成示例（使用 axum）：
/// ```ignore
/// use axum::{Router, routing::{post, get}};
/// let app = Router::new()
///     .route("/api/v1/eval/run", post(run_eval_handler))
///     .route("/api/v1/eval/compare", post(compare_eval_handler))
///     .route("/api/v1/eval/datasets", get(list_datasets_handler))
///     .route("/api/v1/eval/report/:report_id", get(get_report_handler));
/// ```
pub struct EvalGateway;

impl EvalGateway {
    pub fn api_prefix() -> &'static str {
        "/api/v1/eval"
    }

    pub fn routes() -> Vec<&'static str> {
        vec![
            "POST /run",
            "POST /compare",
            "GET /datasets",
            "GET /report/:report_id",
        ]
    }
}
