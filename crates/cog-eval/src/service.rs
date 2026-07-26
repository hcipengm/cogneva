//! [`cog_core::EvalService`] implementation — wraps [`crate::EvalRunner`] so
//! the gateway never depends on `cog-eval` concrete types.

use std::sync::Arc;

/// Wrapper that implements [`cog_core::EvalService`] for the concrete [`crate::EvalRunner`].
pub struct EvalServiceImpl {
    runner: Arc<tokio::sync::Mutex<crate::EvalRunner>>,
    observables: Vec<Arc<dyn cog_core::Observable>>,
}

impl EvalServiceImpl {
    pub fn new(
        runner: Arc<tokio::sync::Mutex<crate::EvalRunner>>,
        observables: Vec<Arc<dyn cog_core::Observable>>,
    ) -> Self {
        Self {
            runner,
            observables,
        }
    }
}

#[async_trait::async_trait]
impl cog_core::EvalService for EvalServiceImpl {
    async fn run_eval(&self, dataset_path: &str) -> cog_core::SFResult<serde_json::Value> {
        let runner = self.runner.lock().await;
        let dataset = crate::EvalDataset::load_from_jsonl(std::path::Path::new(dataset_path))
            .map_err(|e| cog_core::SFError::Agent(format!("dataset load failed: {}", e)))?;
        let results = runner.run_dataset(&dataset).await;
        let report = crate::EvalReport::from_results_with_observables(
            &dataset.name,
            &results,
            &self.observables,
        )
        .await;
        serde_json::to_value(report)
            .map_err(|e| cog_core::SFError::Agent(format!("report serialization failed: {}", e)))
    }

    async fn compare_eval(
        &self,
        baseline: serde_json::Value,
        challenger: serde_json::Value,
        baseline_name: &str,
        challenger_name: &str,
    ) -> cog_core::SFResult<serde_json::Value> {
        let baseline: Vec<crate::EvalResult> = serde_json::from_value(baseline)
            .map_err(|e| cog_core::SFError::Agent(format!("invalid baseline: {}", e)))?;
        let challenger: Vec<crate::EvalResult> = serde_json::from_value(challenger)
            .map_err(|e| cog_core::SFError::Agent(format!("invalid challenger: {}", e)))?;
        let comparator = crate::AbComparator::new();
        let report = comparator.compare(&baseline, &challenger, baseline_name, challenger_name);
        serde_json::to_value(report)
            .map_err(|e| cog_core::SFError::Agent(format!("report serialization failed: {}", e)))
    }

    async fn render_report(&self, report_json: &str, format: &str) -> cog_core::SFResult<String> {
        let report: crate::EvalReport = serde_json::from_str(report_json)
            .map_err(|e| cog_core::SFError::Agent(format!("invalid report: {}", e)))?;
        let fmt = match format {
            "markdown" | "md" => crate::ReportFormat::Markdown,
            "html" => crate::ReportFormat::Html,
            _ => crate::ReportFormat::Json,
        };
        Ok(report.render(fmt))
    }
}
