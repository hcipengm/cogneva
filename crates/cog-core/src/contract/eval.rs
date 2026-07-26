//!Eval service trait and gateway DTOs.
//!Keeps `cog-gateway` decoupled from `cog-eval` concrete types.

use serde::{Deserialize, Serialize};

/// Dataset metadata returned by list endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDatasetInfo {
    pub name: String,
    pub path: String,
    pub case_count: usize,
    pub tags: Vec<String>,
}

/// Response for `GET /api/v1/eval/datasets`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListDatasetsResponse {
    pub datasets: Vec<EvalDatasetInfo>,
}

/// Response for `GET /api/v1/eval/report/:report_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalGetReportResponse {
    pub run_id: String,
    pub dataset_name: String,
    pub report_markdown: String,
    pub report_json: String,
}

/// Trait abstracting the eval framework so the gateway never depends on `cog-eval`.
#[async_trait::async_trait]
pub trait EvalService: Send + Sync {
    /// Run an eval dataset and return the report as JSON.
    async fn run_eval(&self, dataset_path: &str) -> crate::SFResult<serde_json::Value>;

    /// Compare two result arrays (baseline vs challenger) and return comparison report as JSON.
    async fn compare_eval(
        &self,
        baseline: serde_json::Value,
        challenger: serde_json::Value,
        baseline_name: &str,
        challenger_name: &str,
    ) -> crate::SFResult<serde_json::Value>;

    /// Render a report JSON string into the requested format (`markdown`, `json`, `html`).
    async fn render_report(&self, report_json: &str, format: &str) -> crate::SFResult<String>;
}
